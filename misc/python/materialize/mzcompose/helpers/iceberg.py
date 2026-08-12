# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Shared utilities for setting up Iceberg sinks with Polaris catalog and MinIO storage.
"""

import json
import random
import string
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from materialize.mzcompose.composition import Composition

from materialize.mzcompose.services.polaris import Polaris

ALLOW_ALL_ACL = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": ["s3:*"],
            "Resource": ["arn:aws:s3:::*"],
        }
    ],
}


def make_random_key(n: int) -> str:
    return "".join(
        random.SystemRandom().choice(string.ascii_uppercase + string.digits)
        for _ in range(n)
    )


def create_minio_user(name: str, minio_alias: str, c: "Composition") -> str:
    key = make_random_key(10)
    c.exec("mc", "mc", "admin", "user", "add", minio_alias, name, key)
    c.exec("mc", "cp", "/dev/stdin", f"/tmp/{name}", stdin=json.dumps(ALLOW_ALL_ACL))
    c.exec("mc", "mc", "admin", "policy", "create", minio_alias, name, f"/tmp/{name}")
    c.exec("mc", "mc", "admin", "policy", "attach", minio_alias, name, "--user", name)
    return key


def setup_minio_alias(
    c: "Composition",
    alias: str = "s3test",
    endpoint: str = "http://minio:9000/",
    access_key: str = "minioadmin",
    secret_key: str = "minioadmin",
) -> None:
    c.exec("mc", "mc", "alias", "set", alias, endpoint, access_key, secret_key)


def create_minio_bucket(
    c: "Composition", alias: str, bucket_name: str, ignore_existing: bool = True
) -> None:
    args = ["mc", "mc", "mb"]
    if ignore_existing:
        args.append("--ignore-existing")
    args.append(f"{alias}/{bucket_name}")
    c.exec(*args)


def get_polaris_access_token(c: "Composition") -> str:
    token_resp = c.exec(
        "polaris",
        "curl",
        "-s",
        "-X",
        "POST",
        "http://localhost:8181/api/catalog/v1/oauth/tokens",
        "-d",
        "grant_type=client_credentials&client_id=root&client_secret=root&scope=PRINCIPAL_ROLE:ALL",
        capture=True,
    )
    try:
        return json.loads(token_resp.stdout)["access_token"]
    except Exception as e:
        raise RuntimeError(
            f"Failed to parse Polaris access token: {token_resp.stdout}: {e}"
        )


def create_polaris_catalog(
    c: "Composition",
    access_token: str,
    catalog_name: str = "default_catalog",
    bucket_name: str = "test-bucket",
    username: str = "tduser",
    secret_key: str = "",
    endpoint: str = "http://minio:9000",
    region: str = "minio",
    static_credentials: bool = True,
) -> None:
    """Create a Polaris catalog backed by `bucket_name` in MinIO.

    Catalog properties are returned to clients verbatim on `loadTable`, so the
    `s3.access-key-id`/`s3.secret-access-key` written here become the credentials
    every client uses. Pass `static_credentials=False` to leave them out, which
    makes credential vending the only way a client can reach the bucket. Polaris
    itself still reaches MinIO through the credentials in its environment.
    """
    properties = {
        "default-base-location": f"s3://{bucket_name}/",
        "s3.endpoint": endpoint,
        "s3.path-style-access": "true",
        "s3.region": region,
    }
    if static_credentials:
        properties["s3.access-key-id"] = username
        properties["s3.secret-access-key"] = secret_key

    catalog_payload = {
        "name": catalog_name,
        "type": "INTERNAL",
        "properties": properties,
        "storageConfigInfo": {
            "storageType": "S3",
            "allowedLocations": [f"s3://{bucket_name}/*"],
            "endpoint": endpoint,
            "endpointInternal": endpoint,
            "pathStyleAccess": True,
        },
    }

    c.exec(
        "polaris",
        "curl",
        "-sS",
        "-i",
        "-X",
        "POST",
        "-H",
        f"Authorization: Bearer {access_token}",
        "-H",
        "Content-Type: application/json",
        "http://localhost:8181/api/management/v1/catalogs",
        "--data-binary",
        json.dumps(catalog_payload),
    )


def create_polaris_namespace(
    c: "Composition",
    access_token: str,
    namespace: str = "default_namespace",
    catalog_name: str = "default_catalog",
) -> None:
    namespace_payload = {"namespace": [namespace]}
    c.exec(
        "polaris",
        "curl",
        "-sS",
        "-i",
        "-X",
        "POST",
        "-H",
        f"Authorization: Bearer {access_token}",
        "-H",
        "Content-Type: application/json",
        f"http://localhost:8181/api/catalog/v1/{catalog_name}/namespaces",
        "--data-binary",
        json.dumps(namespace_payload),
    )


def grant_catalog_role_privilege(
    c: "Composition",
    access_token: str,
    privilege: str,
    catalog_name: str = "default_catalog",
    catalog_role: str = "catalog_admin",
) -> None:
    """Grant a catalog-level privilege to a catalog role."""
    c.exec(
        "polaris",
        "curl",
        "-sS",
        "--fail-with-body",
        "-X",
        "PUT",
        "-H",
        f"Authorization: Bearer {access_token}",
        "-H",
        "Content-Type: application/json",
        f"http://localhost:8181/api/management/v1/catalogs/{catalog_name}/catalog-roles/{catalog_role}/grants",
        "-d",
        json.dumps({"type": "catalog", "privilege": privilege}),
    )


def load_polaris_vended_credentials(
    c: "Composition",
    table: str,
    namespace: str = "default_namespace",
    catalog_name: str = "default_catalog",
) -> dict[str, str]:
    """Load a table through the REST catalog requesting credential vending, and
    return the vended storage config (the `config` map, which contains the
    temporary `s3.access-key-id`, `s3.secret-access-key`, and `s3.session-token`).

    Requires the catalog to have been set up with `vended=True` so the principal
    is authorized for `LOAD_TABLE_WITH_READ_DELEGATION`.
    """
    access_token = get_polaris_access_token(c)
    resp = c.exec(
        "polaris",
        "curl",
        "-sS",
        "--fail-with-body",
        "-X",
        "GET",
        "-H",
        f"Authorization: Bearer {access_token}",
        "-H",
        "X-Iceberg-Access-Delegation: vended-credentials",
        f"http://localhost:8181/api/catalog/v1/{catalog_name}/namespaces/{namespace}/tables/{table}",
        capture=True,
    )
    return json.loads(resp.stdout)["config"]


def setup_polaris_for_iceberg(
    c: "Composition",
    bucket_name: str = "test-bucket",
    minio_alias: str = "s3test",
    username: str = "tduser",
    catalog_name: str = "default_catalog",
    namespace: str = "default_namespace",
    vended: bool = False,
    static_credentials: bool = True,
) -> tuple[str, str]:
    """
    Set up Polaris catalog with MinIO for Iceberg sink usage.

    This is the main entry point that orchestrates:
    1. Setting up MinIO alias and bucket
    2. Creating a MinIO user with S3 permissions
    3. Starting Polaris with the user's credentials
    4. Creating a catalog and namespace in Polaris

    With `vended=True`, also grant the catalog role the `TABLE_READ_DATA` and
    `TABLE_WRITE_DATA` privileges. These authorize credential vending, so a
    client sending the `X-Iceberg-Access-Delegation: vended-credentials` header
    on `loadTable`/`commit` receives temporary, table-scoped MinIO STS
    credentials instead of using its own static ones. Polaris mints them via
    AssumeRole against MinIO using the credentials passed to it below.

    With `static_credentials=False`, the catalog withholds the long-lived S3
    credentials it would otherwise hand every client, so vending becomes the only
    path to the bucket. Combine with `vended=True` to require vending.
    """
    from materialize.mzcompose.composition import Service

    c.up("minio", Service("mc", idle=True))

    setup_minio_alias(c, alias=minio_alias)

    create_minio_bucket(c, minio_alias, bucket_name)

    key = create_minio_user(username, minio_alias, c)

    # Create a dedicated database for Polaris so its internal tables don't
    # interfere with FOR ALL TABLES publications in PG CDC tests.
    c.up("postgres")
    c.exec("postgres", "psql", "-U", "postgres", "-c", "CREATE DATABASE polaris")

    with c.override(
        Polaris(
            extra_environment=[
                f"AWS_ACCESS_KEY_ID={username}",
                f"AWS_SECRET_ACCESS_KEY={key}",
            ],
        )
    ):
        c.up("polaris")

    access_token = get_polaris_access_token(c)

    create_polaris_catalog(
        c,
        access_token,
        catalog_name=catalog_name,
        bucket_name=bucket_name,
        username=username,
        secret_key=key,
        static_credentials=static_credentials,
    )

    create_polaris_namespace(
        c,
        access_token,
        namespace=namespace,
        catalog_name=catalog_name,
    )

    if vended:
        for privilege in ("TABLE_READ_DATA", "TABLE_WRITE_DATA"):
            grant_catalog_role_privilege(
                c, access_token, privilege, catalog_name=catalog_name
            )

    return (username, key)
