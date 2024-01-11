# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import json
import ssl
import uuid

from pg8000 import Cursor
from pg8000.dbapi import ProgrammingError
from pg8000.exceptions import InterfaceError

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.balancerd import Balancerd
from materialize.mzcompose.services.frontegg import FronteggMock
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.test_certs import TestCerts

TENANT_ID = str(uuid.uuid4())
ADMIN_USER = "u1@example.com"
OTHER_USER = "u2@example.com"
ADMIN_ROLE = "MaterializePlatformAdmin"
OTHER_ROLE = "MaterializePlatform"
USERS = {
    ADMIN_USER: {
        "email": ADMIN_USER,
        "password": str(uuid.uuid4()),
        "tenant_id": TENANT_ID,
        "initial_api_tokens": [
            {
                "client_id": str(uuid.uuid4()),
                "secret": str(uuid.uuid4()),
            }
        ],
        "roles": [OTHER_ROLE, ADMIN_ROLE],
    },
    OTHER_USER: {
        "email": OTHER_USER,
        "password": str(uuid.uuid4()),
        "tenant_id": TENANT_ID,
        "initial_api_tokens": [
            {
                "client_id": str(uuid.uuid4()),
                "secret": str(uuid.uuid4()),
            }
        ],
        "roles": [OTHER_ROLE],
    },
}
FRONTEGG_URL = "http://frontegg-mock:6880"

SERVICES = [
    TestCerts(),
    Balancerd(
        command=[
            "service",
            "--pgwire-listen-addr=0.0.0.0:6875",
            "--https-listen-addr=0.0.0.0:6876",
            "--internal-http-listen-addr=0.0.0.0:6878",
            "--frontegg-resolver-template=materialized:6880",
            "--frontegg-jwk-file=/secrets/frontegg-mock.crt",
            f"--frontegg-api-token-url={FRONTEGG_URL}/identity/resources/auth/v1/api-token",
            f"--frontegg-admin-role={ADMIN_ROLE}",
            "--https-resolver-template=materialized:6881",
            "--tls-key=/secrets/balancerd.key",
            "--tls-cert=/secrets/balancerd.crt",
        ],
        depends_on=["test-certs"],
        volumes=[
            "secrets:/secrets",
        ],
    ),
    FronteggMock(
        issuer=FRONTEGG_URL,
        encoding_key_file="/secrets/frontegg-mock.key",
        decoding_key_file="/secrets/frontegg-mock.crt",
        users=json.dumps(list(USERS.values())),
        depends_on=["test-certs"],
        volumes=[
            "secrets:/secrets",
        ],
    ),
    Materialized(
        options=[
            # Enable TLS on the public port to verify that balancerd is connecting to the balancerd
            # port.
            "--tls-mode=require",
            "--tls-key=/secrets/materialized.key",
            "--tls-cert=/secrets/materialized.crt",
            f"--frontegg-tenant={TENANT_ID}",
            "--frontegg-jwk-file=/secrets/frontegg-mock.crt",
            f"--frontegg-api-token-url={FRONTEGG_URL}/identity/resources/auth/v1/api-token",
            f"--frontegg-admin-role={ADMIN_ROLE}",
        ],
        # We do not do anything interesting on the Mz side
        # to justify the extra restarts
        sanity_restart=False,
        depends_on=["test-certs"],
        volumes_extra=[
            "secrets:/secrets",
        ],
    ),
]


def app_password(email: str) -> str:
    api_token = USERS[email]["initial_api_tokens"][0]
    password = f"mzp_{api_token['client_id']}{api_token['secret']}".replace("-", "")
    return password


def sql_cursor(c: Composition, service="balancerd", email="u1@example.com") -> Cursor:
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    return c.sql_cursor(
        service=service,
        user=email,
        password=app_password(email),
        ssl_context=ssl_context,
    )


def workflow_default(c: Composition) -> None:
    c.down(destroy_volumes=True)

    for i, name in enumerate(c.workflows):
        if name == "default":
            continue
        with c.test_case(name):
            c.workflow(name)


def workflow_http(c: Composition) -> None:
    """Test http endpoint"""
    c.up("balancerd", "frontegg-mock", "materialized")
    result = c.exec(
        "materialized",
        "curl",
        "https://balancerd:6876/api/sql",
        "-k",
        "-s",
        "--header",
        "Content-Type: application/json",
        "--user",
        f"{OTHER_USER}:{app_password(OTHER_USER)}",
        "--data",
        '{"query": "SELECT 123"}',
        capture=True,
    )
    assert json.loads(result.stdout)["results"][0]["rows"][0][0] == "123"


def workflow_wide_result(c: Composition) -> None:
    """Test passthrough of wide rows"""
    c.up("balancerd", "frontegg-mock", "materialized")

    cursor = sql_cursor(c)
    cursor.execute("SELECT 'ABC' || REPEAT('x', 1024 * 1024 * 96) || 'XYZ'")
    rows = cursor.fetchall()
    assert len(rows) == 1
    cols = rows[0]
    assert len(cols) == 1
    col = cols[0]
    assert len(col) == (1024 * 1024 * 96) + (2 * 3)
    assert col.startswith("ABCx")
    assert col.endswith("xXYZ")


def workflow_long_result(c: Composition) -> None:
    """Test passthrough of long results"""
    c.up("balancerd", "frontegg-mock", "materialized")

    cursor = sql_cursor(c)
    cursor.execute(
        "SELECT 'ABC', generate_series, 'XYZ' FROM generate_series(1, 10 * 1024 * 1024)"
    )
    cnt = 0
    for row in cursor.fetchall():
        cnt = cnt + 1
        assert len(row) == 3
        assert row[0] == "ABC"
        assert row[2] == "XYZ"
    assert cnt == 10 * 1024 * 1024


def workflow_long_query(c: Composition) -> None:
    """Test passthrough of a long SQL query."""
    c.up("balancerd", "frontegg-mock", "materialized")

    cursor = sql_cursor(c)
    small_pad_size = 512 * 1024
    small_pad = "x" * small_pad_size
    cursor.execute(f"SELECT 'ABC{small_pad}XYZ';")
    rows = cursor.fetchall()
    assert len(rows) == 1
    cols = rows[0]
    assert len(cols) == 1
    col = cols[0]
    assert len(col) == small_pad_size + (2 * 3)
    assert col.startswith("ABCx")
    assert col.endswith("xXYZ")

    medium_pad_size = 1 * 1024 * 1024
    medium_pad = "x" * medium_pad_size
    try:
        cursor.execute(f"SELECT 'ABC{medium_pad}XYZ';")
        assert False, "execute() expected to fail"
    except ProgrammingError as e:
        assert "statement batch size cannot exceed 1000.0 KB" in str(e)
    except:
        assert False, "execute() threw an unexpected exception"

    large_pad_size = 512 * 1024 * 1024
    large_pad = "x" * large_pad_size
    try:
        cursor.execute(f"SELECT 'ABC{large_pad}XYZ';")
        assert False, "execute() expected to fail"
    except InterfaceError as e:
        assert "network error" in str(e)
    except:
        assert False, "execute() threw an unexpected exception"

    # Confirm that balancerd remains up
    cursor = sql_cursor(c)
    cursor.execute("SELECT 1;")


def workflow_mz_restarted(c: Composition) -> None:
    """Existing connections should fail if Mz is restarted.
    This protects against the client not being informed
    that their transaction has been aborted on the Mz side
    """
    c.up("balancerd", "frontegg-mock", "materialized")

    cursor = sql_cursor(c)

    cursor.execute("CREATE TABLE restart_mz (f1 INTEGER)")
    cursor.execute("START TRANSACTION")
    cursor.execute("INSERT INTO restart_mz VALUES (1)")
    c.kill("materialized")
    c.up("materialized")
    try:
        cursor.execute("INSERT INTO restart_mz VALUES (2)")
        assert False, "execute() expected to fail"
    except InterfaceError as e:
        assert "network error" in str(e)
    except:
        assert False, "execute() threw an unexpected exception"

    # Future connections work
    sql_cursor(c)


def workflow_balancerd_restarted(c: Composition) -> None:
    """Existing connections should fail if balancerd is restarted"""
    c.up("balancerd", "frontegg-mock", "materialized")

    cursor = sql_cursor(c)

    cursor.execute("CREATE TABLE restart_balancerd (f1 INTEGER)")
    cursor.execute("START TRANSACTION")
    cursor.execute("INSERT INTO restart_balancerd VALUES (1)")
    c.kill("balancerd")
    c.up("balancerd")
    try:
        cursor.execute("INSERT INTO restart_balancerd VALUES (2)")
        assert False, "execute() expected to fail"
    except InterfaceError as e:
        assert "network error" in str(e)
    except:
        assert False, "execute() threw an unexpected exception"

    # Future connections work
    sql_cursor(c)


def workflow_mz_not_running(c: Composition) -> None:
    """New connections should fail if Mz is down"""
    c.up("balancerd", "frontegg-mock", "materialized")
    c.kill("materialized")
    try:
        sql_cursor(c)
        assert False, "connect() expected to fail"
    except ProgrammingError as e:
        assert any(
            expected in str(e)
            for expected in [
                "No route to host",
                "Connection timed out",
                "failure in name resolution",
                "failed to lookup address information",
                "Name or service not known",
            ]
        )
    except:
        assert False, "connect() threw an unexpected exception"

    # Things should work now
    c.up("materialized")
    sql_cursor(c)


def workflow_user(c: Composition) -> None:
    """Test that the user is passed all the way to Mz itself."""
    c.up("balancerd", "frontegg-mock", "materialized")

    # Non-admin user.
    cursor = sql_cursor(c, email=OTHER_USER)

    try:
        cursor.execute("DROP DATABASE materialize CASCADE")
        assert False, "execute() expected to fail"
    except ProgrammingError as e:
        assert "must be owner of DATABASE materialize" in str(e)
    except:
        assert False, "execute() threw an unexpected exception"

    cursor.execute("SELECT current_user()")
    assert OTHER_USER in str(cursor.fetchall())


def workflow_many_connections(c: Composition) -> None:
    c.up("balancerd", "frontegg-mock", "materialized")

    cursors = []
    connections = 1000 - 10  #  Go almost to the limit, but not above
    print(f"Opening {connections} connections.")
    for i in range(connections):
        cursor = sql_cursor(c)
        cursors.append(cursor)

    for cursor in cursors:
        cursor.execute("SELECT 'abc'")
        data = cursor.fetchall()
        assert len(data) == 1
        row = data[0]
        assert len(row) == 1
        col = row[0]
        assert col == "abc"
