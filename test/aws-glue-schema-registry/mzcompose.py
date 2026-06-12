# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""End-to-end tests for the AWS Glue Schema Registry source path.

Two workflows are provided:

* `default` — runs against `motoserver/moto` inside the compose network.
  Wired into the `test` pipeline under the Kafka group. LocalStack Community
  does not implement the Glue API (it's a Pro service), so moto is the
  open-source mock we use.
* `aws` — runs against real AWS Glue using credentials from the ambient
  environment (env vars, `AWS_PROFILE`, SSO session, instance role, ...).
  Wired into the `nightly` pipeline under the AWS group via the
  `scratch-aws-access` plugin. Catches API/IAM drift that moto papers over.
"""

import json
import os
import uuid
from collections.abc import Iterator

import boto3

from materialize.mzcompose import DEFAULT_CLOUD_REGION
from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.moto import Moto
from materialize.mzcompose.services.testdrive import Testdrive

# Moto-mode constants. workflow_aws derives its own per-run names.
MOTO_AWS_ACCESS_KEY_ID = "test"
MOTO_AWS_SECRET_ACCESS_KEY = "test"
MOTO_AWS_ENDPOINT_URL = "http://moto:5000"

REGISTRY_NAME_BASE = "mz-test-registry"
OTHER_REGISTRY_NAME_BASE = "mz-other-registry"
# Glue's implicit default registry is addressed by the literal name
# "default-registry". It's an account-wide singleton — we suffix only the
# *schema* name inside it, never the registry itself.
DEFAULT_REGISTRY_NAME = "default-registry"
SIMPLE_SCHEMA_NAME_BASE = "mz-test-schema"
NESTED_SCHEMA_NAME_BASE = "mz-nested-schema"
EVOLVING_SCHEMA_NAME_BASE = "mz-evolving-schema"
DEFAULT_SCHEMA_NAME_BASE = "mz-default-schema"
# A JSON-format schema we deliberately register to prove that purification
# rejects non-AVRO data formats with a clear SQL error before any decode
# attempt.
JSON_SCHEMA_NAME_BASE = "mz-json-schema"

SIMPLE_SCHEMA = {
    "type": "record",
    "name": "row",
    "fields": [
        {"name": "a", "type": "long"},
        {"name": "b", "type": "string"},
    ],
}

# Exercises nested records, intra-document named-type references, arrays,
# and nullable unions. `inner_t` is defined once and reused by name in a
# later field — the canonical Avro intra-document reference pattern.
NESTED_SCHEMA = {
    "type": "record",
    "name": "outer",
    "fields": [
        {"name": "id", "type": "long"},
        {
            "name": "inner",
            "type": {
                "type": "record",
                "name": "inner_t",
                "fields": [
                    {"name": "x", "type": "int"},
                    {"name": "y", "type": "string"},
                ],
            },
        },
        {"name": "inner2", "type": "inner_t"},
        {"name": "tags", "type": {"type": "array", "items": "string"}},
        {"name": "note", "type": ["null", "string"], "default": None},
    ],
}

EVOLVING_SCHEMA_V1 = {
    "type": "record",
    "name": "evolving",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "value", "type": "string"},
    ],
}

EVOLVING_SCHEMA_V2 = {
    "type": "record",
    "name": "evolving",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "value", "type": "string"},
        {"name": "extra", "type": ["null", "string"], "default": None},
    ],
}

OTHER_SAMENAME_SCHEMA = {
    "type": "record",
    "name": "row",
    "fields": [
        {"name": "a", "type": "long"},
        {"name": "b", "type": "string"},
        {"name": "c", "type": "string"},
    ],
}

DEFAULT_REGISTRY_SCHEMA = {
    "type": "record",
    "name": "default_row",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "label", "type": "string"},
    ],
}

# Trivial JSON Schema document — the contents are irrelevant because
# purification's data-format check fires before the body is parsed.
JSON_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "properties": {"id": {"type": "integer"}},
}

SERVICES = [
    Moto(),
    Kafka(),
    Materialized(
        depends_on=["moto"],
        environment_extra=[
            f"AWS_REGION={DEFAULT_CLOUD_REGION}",
            f"AWS_ENDPOINT_URL={MOTO_AWS_ENDPOINT_URL}",
            f"AWS_ACCESS_KEY_ID={MOTO_AWS_ACCESS_KEY_ID}",
            f"AWS_SECRET_ACCESS_KEY={MOTO_AWS_SECRET_ACCESS_KEY}",
        ],
    ),
    Testdrive(
        default_timeout="30s",
        no_reset=True,
    ),
]


def _create_schemas(
    glue,
    *,
    registry_name: str,
    other_registry_name: str,
    simple_schema_name: str,
    nested_schema_name: str,
    evolving_schema_name: str,
    default_schema_name: str,
    json_schema_name: str,
) -> dict[str, str]:
    """Create the registries + schemas and return their version UUIDs."""
    glue.create_registry(RegistryName=registry_name)
    glue.create_registry(RegistryName=other_registry_name)

    def create_schema(registry: str, name: str, definition: dict) -> str:
        resp = glue.create_schema(
            RegistryId={"RegistryName": registry},
            SchemaName=name,
            DataFormat="AVRO",
            Compatibility="BACKWARD",
            SchemaDefinition=json.dumps(definition),
        )
        return resp["SchemaVersionId"]

    simple_version_id = create_schema(registry_name, simple_schema_name, SIMPLE_SCHEMA)
    nested_version_id = create_schema(registry_name, nested_schema_name, NESTED_SCHEMA)
    evolving_v1_id = create_schema(
        registry_name, evolving_schema_name, EVOLVING_SCHEMA_V1
    )
    evolving_v2_id = glue.register_schema_version(
        SchemaId={"RegistryName": registry_name, "SchemaName": evolving_schema_name},
        SchemaDefinition=json.dumps(EVOLVING_SCHEMA_V2),
    )["SchemaVersionId"]

    other_samename_version_id = create_schema(
        other_registry_name, simple_schema_name, OTHER_SAMENAME_SCHEMA
    )

    # No RegistryId → lands in the implicit default registry.
    default_version_id = glue.create_schema(
        SchemaName=default_schema_name,
        DataFormat="AVRO",
        Compatibility="BACKWARD",
        SchemaDefinition=json.dumps(DEFAULT_REGISTRY_SCHEMA),
    )["SchemaVersionId"]

    # JSON-format schema in the primary registry. Used by the negative
    # "unsupported data format" test — purification must reject this before
    # ever attempting to parse the body.
    json_version_id = glue.create_schema(
        RegistryId={"RegistryName": registry_name},
        SchemaName=json_schema_name,
        DataFormat="JSON",
        Compatibility="NONE",
        SchemaDefinition=json.dumps(JSON_SCHEMA),
    )["SchemaVersionId"]

    return {
        "simple": simple_version_id,
        "nested": nested_version_id,
        "evolving_v1": evolving_v1_id,
        "evolving_v2": evolving_v2_id,
        "other_samename": other_samename_version_id,
        "default": default_version_id,
        "json": json_version_id,
    }


def _cleanup_glue(
    glue,
    *,
    registries: Iterator[str],
    default_schema_name: str | None,
) -> None:
    """Best-effort cleanup. Never raises on missing resources."""
    for reg in registries:
        try:
            glue.delete_registry(RegistryId={"RegistryName": reg})
        except glue.exceptions.EntityNotFoundException:
            pass
        except Exception as e:
            print(f"warning: failed to delete registry {reg!r}: {e}")
    if default_schema_name is not None:
        try:
            glue.delete_schema(
                SchemaId={
                    "RegistryName": DEFAULT_REGISTRY_NAME,
                    "SchemaName": default_schema_name,
                }
            )
        except glue.exceptions.EntityNotFoundException:
            pass
        except Exception as e:
            print(f"warning: failed to delete default-registry schema: {e}")


def _run_testdrive(
    c: Composition,
    *,
    aws_conn_sql: str,
    version_ids: dict[str, str],
    registry_name: str,
    other_registry_name: str,
    nonexistent_registry_name: str,
    simple_schema_name: str,
    nested_schema_name: str,
    evolving_schema_name: str,
    default_schema_name: str,
    json_schema_name: str,
) -> None:
    # Bring up the AWS connection out-of-band so we can include SESSION TOKEN
    # only when present.
    c.testdrive(input=aws_conn_sql)
    c.run_testdrive_files(
        f"--var=simple-version-id={version_ids['simple']}",
        f"--var=nested-version-id={version_ids['nested']}",
        f"--var=evolving-v1-id={version_ids['evolving_v1']}",
        f"--var=evolving-v2-id={version_ids['evolving_v2']}",
        f"--var=other-samename-version-id={version_ids['other_samename']}",
        f"--var=default-version-id={version_ids['default']}",
        f"--var=registry-name={registry_name}",
        f"--var=other-registry-name={other_registry_name}",
        f"--var=default-registry-name={DEFAULT_REGISTRY_NAME}",
        f"--var=nonexistent-registry-name={nonexistent_registry_name}",
        f"--var=simple-schema-name={simple_schema_name}",
        f"--var=nested-schema-name={nested_schema_name}",
        f"--var=evolving-schema-name={evolving_schema_name}",
        f"--var=default-schema-name={default_schema_name}",
        f"--var=json-schema-name={json_schema_name}",
        "glue-schema-registry.td",
    )


def workflow_default(c: Composition) -> None:
    """CI workflow: run against motoserver/moto."""
    c.up("moto")

    aws_endpoint_url = f"http://localhost:{c.port('moto', 5000)}"
    glue = boto3.client(
        "glue",
        endpoint_url=aws_endpoint_url,
        region_name=DEFAULT_CLOUD_REGION,
        aws_access_key_id=MOTO_AWS_ACCESS_KEY_ID,
        aws_secret_access_key=MOTO_AWS_SECRET_ACCESS_KEY,
    )

    # Moto starts empty, but re-running against the same instance shouldn't
    # blow up. Fixed names are fine here — moto is per-compose-network.
    _cleanup_glue(
        glue,
        registries=iter([REGISTRY_NAME_BASE, OTHER_REGISTRY_NAME_BASE]),
        default_schema_name=DEFAULT_SCHEMA_NAME_BASE,
    )

    version_ids = _create_schemas(
        glue,
        registry_name=REGISTRY_NAME_BASE,
        other_registry_name=OTHER_REGISTRY_NAME_BASE,
        simple_schema_name=SIMPLE_SCHEMA_NAME_BASE,
        nested_schema_name=NESTED_SCHEMA_NAME_BASE,
        evolving_schema_name=EVOLVING_SCHEMA_NAME_BASE,
        default_schema_name=DEFAULT_SCHEMA_NAME_BASE,
        json_schema_name=JSON_SCHEMA_NAME_BASE,
    )

    c.up("kafka", "materialized")

    aws_conn_sql = f"""
> CREATE SECRET aws_secret_access_key AS '{MOTO_AWS_SECRET_ACCESS_KEY}'

> CREATE CONNECTION aws_conn TO AWS (
    ACCESS KEY ID = '{MOTO_AWS_ACCESS_KEY_ID}',
    SECRET ACCESS KEY = SECRET aws_secret_access_key
  )
"""

    _run_testdrive(
        c,
        aws_conn_sql=aws_conn_sql,
        version_ids=version_ids,
        registry_name=REGISTRY_NAME_BASE,
        other_registry_name=OTHER_REGISTRY_NAME_BASE,
        nonexistent_registry_name="no-such-registry",
        simple_schema_name=SIMPLE_SCHEMA_NAME_BASE,
        nested_schema_name=NESTED_SCHEMA_NAME_BASE,
        evolving_schema_name=EVOLVING_SCHEMA_NAME_BASE,
        default_schema_name=DEFAULT_SCHEMA_NAME_BASE,
        json_schema_name=JSON_SCHEMA_NAME_BASE,
    )


def workflow_aws(c: Composition) -> None:
    """Run the same test suite against real AWS Glue.

    In CI, the `scratch-aws-access` Buildkite plugin assumes the scratch
    role and exports `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` /
    `AWS_SESSION_TOKEN` before this workflow runs; locally, any credential
    source the default boto3 chain resolves (env vars / AWS_PROFILE / SSO
    session / instance role) works the same way. The resolved credentials
    are forwarded to Materialize so its in-process Glue client uses the
    same identity as the test harness. Region is read from `AWS_REGION` /
    `AWS_DEFAULT_REGION`, falling back to `us-east-1`.

    Registry and schema names are suffixed with a per-run UUID so concurrent
    runs and prior leaks do not collide. A try/finally guarantees teardown.
    """
    region = (
        os.environ.get("AWS_REGION")
        or os.environ.get("AWS_DEFAULT_REGION")
        or "us-east-1"
    )
    session = boto3.Session(region_name=region)
    creds = session.get_credentials()
    if creds is None:
        raise RuntimeError(
            "no AWS credentials resolvable; configure AWS_PROFILE / env vars / "
            "SSO before running workflow_aws"
        )
    frozen = creds.get_frozen_credentials()
    if not frozen.access_key or not frozen.secret_key:
        raise RuntimeError("resolved AWS credentials are missing access/secret key")

    run_id = uuid.uuid4().hex[:8]
    registry_name = f"{REGISTRY_NAME_BASE}-{run_id}"
    other_registry_name = f"{OTHER_REGISTRY_NAME_BASE}-{run_id}"
    simple_schema_name = f"{SIMPLE_SCHEMA_NAME_BASE}-{run_id}"
    nested_schema_name = f"{NESTED_SCHEMA_NAME_BASE}-{run_id}"
    evolving_schema_name = f"{EVOLVING_SCHEMA_NAME_BASE}-{run_id}"
    default_schema_name = f"{DEFAULT_SCHEMA_NAME_BASE}-{run_id}"
    json_schema_name = f"{JSON_SCHEMA_NAME_BASE}-{run_id}"
    # Use the same run-id; a registry by this name in another account
    # could exist but won't be visible under our credentials.
    nonexistent_registry_name = f"mz-no-such-registry-{uuid.uuid4().hex}"

    glue = session.client("glue")

    # Re-point Materialize at real AWS by dropping the moto endpoint override
    # and feeding it the resolved credentials. Region must match.
    materialized_env = [
        f"AWS_REGION={region}",
        f"AWS_ACCESS_KEY_ID={frozen.access_key}",
        f"AWS_SECRET_ACCESS_KEY={frozen.secret_key}",
    ]
    if frozen.token:
        materialized_env.append(f"AWS_SESSION_TOKEN={frozen.token}")

    aws_conn_sql_lines = [
        f"> CREATE SECRET aws_secret_access_key AS '{frozen.secret_key}'",
    ]
    if frozen.token:
        aws_conn_sql_lines.append(
            f"> CREATE SECRET aws_session_token AS '{frozen.token}'"
        )
    conn_body = [
        f"    ACCESS KEY ID = '{frozen.access_key}'",
        "    SECRET ACCESS KEY = SECRET aws_secret_access_key",
        f"    REGION = '{region}'",
    ]
    if frozen.token:
        conn_body.append("    SESSION TOKEN = SECRET aws_session_token")
    aws_conn_sql_lines.append("> CREATE CONNECTION aws_conn TO AWS (")
    aws_conn_sql_lines.append(",\n".join(conn_body))
    aws_conn_sql_lines.append("  )")
    aws_conn_sql = "\n".join(aws_conn_sql_lines) + "\n"

    print(f"workflow_aws: region={region} run_id={run_id}")
    print(f"workflow_aws: registry={registry_name}")
    print(f"workflow_aws: other-registry={other_registry_name}")
    print(f"workflow_aws: default-registry-schema={default_schema_name}")

    try:
        # Best-effort precleanup in case a prior run with this exact run_id
        # ever happened (collision is astronomically unlikely but cheap).
        _cleanup_glue(
            glue,
            registries=iter([registry_name, other_registry_name]),
            default_schema_name=default_schema_name,
        )

        version_ids = _create_schemas(
            glue,
            registry_name=registry_name,
            other_registry_name=other_registry_name,
            simple_schema_name=simple_schema_name,
            nested_schema_name=nested_schema_name,
            evolving_schema_name=evolving_schema_name,
            default_schema_name=default_schema_name,
            json_schema_name=json_schema_name,
        )

        with c.override(
            Materialized(depends_on=[], environment_extra=materialized_env),
        ):
            c.up("kafka", "materialized")
            _run_testdrive(
                c,
                aws_conn_sql=aws_conn_sql,
                version_ids=version_ids,
                registry_name=registry_name,
                other_registry_name=other_registry_name,
                nonexistent_registry_name=nonexistent_registry_name,
                simple_schema_name=simple_schema_name,
                nested_schema_name=nested_schema_name,
                evolving_schema_name=evolving_schema_name,
                default_schema_name=default_schema_name,
                json_schema_name=json_schema_name,
            )
    finally:
        _cleanup_glue(
            glue,
            registries=iter([registry_name, other_registry_name]),
            default_schema_name=default_schema_name,
        )
