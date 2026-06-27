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
from materialize.mzcompose.services.toxiproxy import Toxiproxy

# Moto-mode constants. workflow_aws derives its own per-run names.
MOTO_AWS_ACCESS_KEY_ID = "test"
MOTO_AWS_SECRET_ACCESS_KEY = "test"
# testdrive's AWS client talks to moto directly. Materialize goes through
# toxiproxy so a test can sever its link to the registry without disturbing
# testdrive's schema registration. With no toxic set toxiproxy is a transparent
# passthrough, so the happy-path scripts are unaffected.
MOTO_AWS_ENDPOINT_URL = "http://moto:5000"
GLUE_PROXY_ENDPOINT_URL = "http://toxiproxy:5000"

REGISTRY_NAME_BASE = "mz-test-registry"
OTHER_REGISTRY_NAME_BASE = "mz-other-registry"
# Glue's implicit default registry is addressed by the literal name
# "default-registry". It's an account-wide singleton — we suffix only the
# *schema* name inside it, never the registry itself.
DEFAULT_REGISTRY_NAME = "default-registry"

# Schemas that live inside a per-run registry need no per-run-unique name (the
# registry already isolates them), so their names and definitions both live in
# glue-schema-registry.td. The only schema name owned here is the one in the
# shared default registry, which *does* need a run-id suffix and which the
# cleanup hook deletes by name (the default registry can't be dropped).
DEFAULT_SCHEMA_NAME_BASE = "mz-default-schema"

SERVICES = [
    Moto(),
    Toxiproxy(),
    Kafka(),
    Materialized(
        depends_on=["moto", "toxiproxy"],
        environment_extra=[
            f"AWS_REGION={DEFAULT_CLOUD_REGION}",
            f"AWS_ENDPOINT_URL={GLUE_PROXY_ENDPOINT_URL}",
            f"AWS_ACCESS_KEY_ID={MOTO_AWS_ACCESS_KEY_ID}",
            f"AWS_SECRET_ACCESS_KEY={MOTO_AWS_SECRET_ACCESS_KEY}",
        ],
    ),
    Testdrive(
        default_timeout="30s",
        no_reset=True,
        # Pin the seed so topic names stay stable across the several testdrive
        # invocations a single run makes (main script, post-restart script,
        # ...). The restart script references topics the main script created.
        consistent_seed=True,
        # Point testdrive's AWS client at moto so the `glue-create-schema`
        # action registers schemas in the same backend Materialize reads from.
        # workflow_aws overrides this to talk to real AWS.
        aws_endpoint=MOTO_AWS_ENDPOINT_URL,
        aws_access_key_id=MOTO_AWS_ACCESS_KEY_ID,
        aws_secret_access_key=MOTO_AWS_SECRET_ACCESS_KEY,
    ),
]


def _create_registries(
    glue,
    *,
    registry_name: str,
    other_registry_name: str,
) -> None:
    """Create the named registries.

    The schemas inside them are registered by the testdrive script via the
    `glue-create-schema` action; here we only create the registries those
    schemas land in (the implicit default registry needs no creation). Deleting
    a registry cascades to its schemas, so this is all the cleanup hook below
    needs to undo for the named registries.
    """
    glue.create_registry(RegistryName=registry_name)
    glue.create_registry(RegistryName=other_registry_name)


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


def _testdrive_var_args(
    *,
    registry_name: str,
    other_registry_name: str,
    nonexistent_registry_name: str,
    default_schema_name: str,
) -> list[str]:
    """Build the `--var` args shared by every testdrive file in a run.

    Only the resources whose names must be unique per run and are managed here,
    the registries and the shared default-registry schema, are passed in.
    In-registry schema names are constants defined in the scripts, and every
    schema body + version UUID is produced there by `glue-create-schema`.
    """
    return [
        f"--var=registry-name={registry_name}",
        f"--var=other-registry-name={other_registry_name}",
        f"--var=default-registry-name={DEFAULT_REGISTRY_NAME}",
        f"--var=nonexistent-registry-name={nonexistent_registry_name}",
        f"--var=default-schema-name={default_schema_name}",
    ]


def _configure_glue_proxy(c: Composition) -> None:
    """Create the toxiproxy proxy that fronts moto for Materialize."""
    c.testdrive(input="""
$ http-request method=POST url=http://toxiproxy:8474/proxies content-type=application/json
{
  "name": "glue",
  "listen": "0.0.0.0:5000",
  "upstream": "moto:5000",
  "enabled": true
}
""")


def _run_testdrive(
    c: Composition,
    *,
    aws_conn_sql: str,
    registry_name: str,
    other_registry_name: str,
    nonexistent_registry_name: str,
    default_schema_name: str,
) -> None:
    var_args = _testdrive_var_args(
        registry_name=registry_name,
        other_registry_name=other_registry_name,
        nonexistent_registry_name=nonexistent_registry_name,
        default_schema_name=default_schema_name,
    )

    # Bring up the AWS connection out-of-band so we can include SESSION TOKEN
    # only when present.
    c.testdrive(input=aws_conn_sql)
    c.run_testdrive_files(*var_args, "glue-schema-registry.td")


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

    _create_registries(
        glue,
        registry_name=REGISTRY_NAME_BASE,
        other_registry_name=OTHER_REGISTRY_NAME_BASE,
    )

    c.up("toxiproxy", "kafka", "materialized")

    # Insert toxiproxy between Materialize and moto. No toxic is set, so it is a
    # transparent passthrough until the runtime-errors script cuts it. Created
    # before any Materialize Glue call (the first is glue_conn's validate()).
    _configure_glue_proxy(c)

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
        registry_name=REGISTRY_NAME_BASE,
        other_registry_name=OTHER_REGISTRY_NAME_BASE,
        nonexistent_registry_name="no-such-registry",
        default_schema_name=DEFAULT_SCHEMA_NAME_BASE,
    )

    # The scenarios below need container-lifecycle or connectivity control, so
    # they run here rather than inside the shared testdrive script. Moto-only:
    # we can't restart against or take down real AWS, and all exercise
    # Materialize-internal behavior moto reproduces faithfully.
    var_args = _testdrive_var_args(
        registry_name=REGISTRY_NAME_BASE,
        other_registry_name=OTHER_REGISTRY_NAME_BASE,
        nonexistent_registry_name="no-such-registry",
        default_schema_name=DEFAULT_SCHEMA_NAME_BASE,
    )

    # Restart Materialize and confirm the Glue connection and sources rehydrate
    # from the catalog and keep resolving + decoding against the registry. The
    # pinned seed keeps the post-restart script pointed at the same topics.
    c.kill("materialized")
    c.up("materialized")
    c.run_testdrive_files(*var_args, "glue-schema-registry-restart.td")

    # Runtime (decode-time) error paths: a record with an unknown UUID is a
    # permanent error, while a record arriving during a registry outage is
    # transient and recovers. The script toggles the toxiproxy proxy itself.
    c.run_testdrive_files(*var_args, "glue-schema-registry-runtime-errors.td")

    # Registry-unreachable: kill the Glue backend, then confirm connection
    # validation fails cleanly rather than hanging or panicking.
    c.kill("moto")
    c.run_testdrive_files(*var_args, "glue-schema-registry-unreachable.td")


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
    # The default registry is account-wide shared, so its schema needs a unique
    # name; the per-run registries above isolate every other schema.
    default_schema_name = f"{DEFAULT_SCHEMA_NAME_BASE}-{run_id}"
    nonexistent_registry_name = f"mz-no-such-registry-{uuid.uuid4().hex}"

    glue = session.client("glue")

    # Re-point Materialize at real AWS by dropping the moto endpoint override
    # and feeding it the resolved credentials. Region must match.
    #
    # SSL_CERT_FILE: Materialize's AWS client uses vendored OpenSSL, whose
    # compiled-in trust-store dir doesn't match where the image's
    # `ca-certificates` lives, so TLS to real AWS Glue fails to verify without
    # this. (moto runs over plaintext HTTP, so the `default` workflow doesn't
    # need it.)
    materialized_env = [
        f"AWS_REGION={region}",
        f"AWS_ACCESS_KEY_ID={frozen.access_key}",
        f"AWS_SECRET_ACCESS_KEY={frozen.secret_key}",
        "SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt",
    ]
    if frozen.token:
        materialized_env.append(f"AWS_SESSION_TOKEN={frozen.token}")

    # The Testdrive service forwards AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY /
    # AWS_SESSION_TOKEN from this process's environment into the container, where
    # testdrive's `glue-create-schema` action resolves them via the default AWS
    # provider chain. Set them here so the container gets the same identity even
    # when our own credentials came from an SSO profile rather than env vars.
    os.environ["AWS_ACCESS_KEY_ID"] = frozen.access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = frozen.secret_key
    if frozen.token:
        os.environ["AWS_SESSION_TOKEN"] = frozen.token

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

        _create_registries(
            glue,
            registry_name=registry_name,
            other_registry_name=other_registry_name,
        )

        with c.override(
            Materialized(depends_on=[], environment_extra=materialized_env),
            # Point testdrive at real AWS (region, no moto endpoint); credentials
            # are resolved from the environment set above.
            Testdrive(
                default_timeout="30s",
                no_reset=True,
                consistent_seed=True,
                aws_region=region,
                aws_endpoint=None,
                aws_access_key_id=None,
                aws_secret_access_key=None,
            ),
        ):
            c.up("kafka", "materialized")
            _run_testdrive(
                c,
                aws_conn_sql=aws_conn_sql,
                registry_name=registry_name,
                other_registry_name=other_registry_name,
                nonexistent_registry_name=nonexistent_registry_name,
                default_schema_name=default_schema_name,
            )
    finally:
        _cleanup_glue(
            glue,
            registries=iter([registry_name, other_registry_name]),
            default_schema_name=default_schema_name,
        )
