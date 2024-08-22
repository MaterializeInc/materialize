# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Tests using the balancerd service instead of connecting to materialized directly.
Uses the frontegg-mock instead of a real frontend backend.
"""

import contextlib
import json
import socket
import ssl
import struct
import uuid
from collections.abc import Callable
from textwrap import dedent
from typing import Any
from urllib.parse import quote

import requests
from pg8000 import Cursor
from pg8000.dbapi import ProgrammingError
from pg8000.exceptions import InterfaceError

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.balancerd import Balancerd
from materialize.mzcompose.services.frontegg import FronteggMock
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.test_certs import TestCerts
from materialize.mzcompose.services.testdrive import Testdrive

TENANT_ID = str(uuid.uuid4())
ADMIN_USER = "u1@example.com"
OTHER_USER = "u2@example.com"
ADMIN_ROLE = "MaterializePlatformAdmin"
OTHER_ROLE = "MaterializePlatform"
USERS = {
    ADMIN_USER: {
        "email": ADMIN_USER,
        "password": str(uuid.uuid4()),
        "id": str(uuid.uuid4()),
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
        "id": str(uuid.uuid4()),
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


def app_password(email: str) -> str:
    api_token = USERS[email]["initial_api_tokens"][0]
    password = f"mzp_{api_token['client_id']}{api_token['secret']}".replace("-", "")
    return password


SERVICES = [
    TestCerts(),
    Testdrive(
        materialize_url=f"postgres://{quote(ADMIN_USER)}:{app_password(ADMIN_USER)}@balancerd:6875?sslmode=require",
        materialize_use_https=True,
        no_reset=True,
    ),
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
            "--default-config=balancerd_inject_proxy_protocol_header_http=true",
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


# Assert that contains is present in balancer metrics.
def assert_metrics(c: Composition, contains: str):
    result = c.exec(
        "materialized",
        "curl",
        "http://balancerd:6878/metrics",
        "-s",
        capture=True,
    )
    assert contains in result.stdout


def sql_cursor(
    c: Composition, service="balancerd", email="u1@example.com", init_params={}
) -> Cursor:
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    return c.sql_cursor(
        service=service,
        user=email,
        password=app_password(email),
        ssl_context=ssl_context,
        init_params=init_params,
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
    # TODO: We can't assert metrics for `mz_balancer_tenant_connection_active{source="https"` here
    # because there's no CNAME. Does docker-compose support this somehow?


def workflow_ip_forwarding(c: Composition) -> None:
    """Test that forwarding the client IP through the balancer works over both HTTP and SQL."""
    c.up("balancerd", "frontegg-mock", "materialized")
    # balancer is going to be running with https
    # in this scenario we should validate that connections
    # via the balancer come from the current ip
    # and that we can use proxy_protocol when talking to
    # envd directly.
    balancer_port = c.port("balancerd", 6876)
    # mz internal (unencrypted port)
    materialize_port = c.port("materialized", 6878)

    # We want to make sure the request we're making through the balancer does not use the balancers
    # ip for the sessions.
    # https://stackoverflow.com/questions/5281341/get-local-network-interface-addresses-using-only-proc
    balancer_ip = [
        ip
        for ip in c.exec(
            "balancerd",
            "awk",
            r"/32 host/ { print i } {i=$2}",
            "/proc/net/fib_trie",
            capture=True,
        ).stdout.split("\n")
        if ip != "127.0.0.1"
    ][0]

    r = requests.post(
        f"https://localhost:{balancer_port}/api/sql",
        headers={},
        auth=(OTHER_USER, app_password(OTHER_USER)),
        json={
            "query": "select client_ip from mz_internal.mz_sessions where connection_id = pg_backend_pid();"
        },
        verify=False,
    )
    print(f"response {r.text}")
    session_ip = json.loads(r.text)["results"][0]["rows"][0][0]
    assert (
        session_ip != balancer_ip
    ), f"requests from ({session_ip}) proxied by balancer should not use balancer ip ({balancer_ip}) in session"

    # Also assert psql connections don't use the balancer ip
    cursor = sql_cursor(c)
    cursor.execute(
        "select client_ip from mz_internal.mz_sessions where connection_id = pg_backend_pid();"
    )
    rows = cursor.fetchall()
    session_ip = rows[0][0]
    assert (
        session_ip != balancer_ip
    ), f"requests from ({session_ip}) proxied by balancer should not use balancer ip ({balancer_ip}) in session"

    def create_proxy_protocol_v2_header(
        client_ip: str, client_port: int, server_ip: str, server_port: int
    ):
        # Signature for Proxy Protocol v2
        signature = b"\r\n\r\n\x00\r\nQUIT\n"
        # Version and command (0x21 means version 2, PROXY command)
        version_and_command = 0x21
        # Address family and protocol (0x11 means INET (IPv4) + STREAM (TCP))
        family_and_protocol = 0x11
        # Source and destination address are sent as bytes.
        src_addr = socket.inet_aton(client_ip)
        dst_addr = socket.inet_aton(server_ip)
        # Pack ports into 2-byte unsigned integers
        src_port = struct.pack("!H", client_port)
        dst_port = struct.pack("!H", server_port)
        # Length of the address information (IPv4(4*2) + ports(1*2) = 12 bytes)
        addr_len = struct.pack("!H", 12)
        # Construct the final Proxy Protocol v2 header
        header = (
            signature
            + struct.pack("!BB", version_and_command, family_and_protocol)
            + addr_len
            + src_addr
            + dst_addr
            + src_port
            + dst_port
        )

        return header

    with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.connect(("127.0.0.1", materialize_port))

        # Pick an ip we couldn't normal connect from and trick envd into
        # thinking we're connecting with
        proxy_header = create_proxy_protocol_v2_header(
            "1.1.1.1", 1111, "127.0.0.1", 1111
        )
        # Make an http request over the socket

        json_data = {
            "query": "select client_ip from mz_internal.mz_sessions where connection_id = pg_backend_pid();"
        }
        json_data = json.dumps(json_data)
        content_length = len(json_data.encode("utf-8"))
        http_sql_query_request = dedent(
            f"""\
            POST /api/sql HTTP/1.1\r
            Host: 127.0.0.1:{materialize_port}\r
            Authorization: Basic {OTHER_USER}:{app_password(OTHER_USER)}\r
            Content-Type: application/json\r
            Content-Length: {content_length}\r
            \r
            {json_data}"""
        )
        sock.sendall(proxy_header + http_sql_query_request.encode("utf-8"))

        # read and parse the response
        body_separator = "\r\n\r\n"
        tcp_resp = sock.recv(8192)
        resp_split = tcp_resp.split(body_separator.encode("utf-8"))
        assert (
            len(resp_split) > 1
        ), f"expected response with header and body, found: {resp_split}"
        body = resp_split[1]
        # assert that we tricked environmentd
        assert json.loads(body)["results"][0]["rows"][0][0] == "1.1.1.1"


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
        raise RuntimeError("execute() expected to fail")
    except ProgrammingError as e:
        assert "statement batch size cannot exceed 1000.0 KB" in str(e)
    except:
        raise RuntimeError("execute() threw an unexpected exception")

    large_pad_size = 512 * 1024 * 1024
    large_pad = "x" * large_pad_size
    try:
        cursor.execute(f"SELECT 'ABC{large_pad}XYZ';")
        raise RuntimeError("execute() expected to fail")
    except InterfaceError as e:
        assert "network error" in str(e)
    except:
        raise RuntimeError("execute() threw an unexpected exception")

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
        raise RuntimeError("execute() expected to fail")
    except InterfaceError as e:
        assert "network error" in str(e)
    except:
        raise RuntimeError("execute() threw an unexpected exception")

    # Future connections work
    sql_cursor(c)


def workflow_pgwire_param_rejection(c: Composition) -> None:
    """Existing connections should fail if balancerd is restarted"""
    c.up("balancerd", "frontegg-mock", "materialized")

    def check_error(message: str, f: Callable[..., Any], expected_error: Any):
        try:
            f()
        except Exception as e:
            assert isinstance(e, expected_error)
            return
        raise AssertionError(f"Expected {message} to raise {expected_error}")

    check_error(
        "connect with x_forwarded_for param",
        lambda: sql_cursor(c, init_params={"mz_forwarded_for": "1.1.1.1"}),
        InterfaceError,
    )

    check_error(
        "connect with mz_connection_id param",
        lambda: sql_cursor(c, init_params={"mz_connection_uuid": "123456"}),
        InterfaceError,
    )


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
        raise RuntimeError("execute() expected to fail")
    except InterfaceError as e:
        assert "network error" in str(e)
    except:
        raise RuntimeError("execute() threw an unexpected exception")

    # Future connections work
    sql_cursor(c)


def workflow_mz_not_running(c: Composition) -> None:
    """New connections should fail if Mz is down"""
    c.up("balancerd", "frontegg-mock", "materialized")
    c.kill("materialized")
    try:
        sql_cursor(c)
        raise RuntimeError("connect() expected to fail")
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
        raise RuntimeError("connect() threw an unexpected exception")

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
        raise RuntimeError("execute() expected to fail")
    except ProgrammingError as e:
        assert "must be owner of DATABASE materialize" in str(e)
    except:
        raise RuntimeError("execute() threw an unexpected exception")

    cursor.execute("SELECT current_user()")
    assert OTHER_USER in str(cursor.fetchall())

    assert_metrics(c, 'mz_balancer_tenant_connection_active{source="pgwire"')
    assert_metrics(c, 'mz_balancer_tenant_connection_rx{source="pgwire"')


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


def workflow_webhook(c: Composition) -> None:
    c.up("balancerd", "frontegg-mock", "materialized")
    c.up("testdrive", persistent=True)

    c.testdrive(
        dedent(
            """
        > CREATE SOURCE wh FROM WEBHOOK BODY FORMAT TEXT
        $ webhook-append database=materialize schema=public name=wh
        a
        > SELECT * FROM wh
        a
    """
        )
    )
