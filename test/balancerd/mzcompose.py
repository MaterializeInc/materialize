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
import datetime
import json
import socket
import ssl
import struct
import time
import uuid
from collections.abc import Callable
from textwrap import dedent
from typing import Any
from urllib.parse import quote

import pg8000
import requests
from pg8000.exceptions import InterfaceError
from psycopg import Cursor
from psycopg.errors import OperationalError, ProgramLimitExceeded, ProgrammingError

from materialize import MZ_ROOT
from materialize.mzcompose.composition import Composition, Service
from materialize.mzcompose.services.balancerd import Balancerd
from materialize.mzcompose.services.dnsmasq import Dnsmasq, DnsmasqEntry
from materialize.mzcompose.services.frontegg import FronteggMock
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mz import Mz
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
STATIC_IPS = {
    "dnsmasq": "10.10.128.2",
    "balancerd": "10.10.128.3",
    "materialized": "10.10.128.4",
    "frontegg-mock": "10.10.128.5",
    "testdrive": "10.10.128.6",
}


def app_password(email: str) -> str:
    api_token = USERS[email]["initial_api_tokens"][0]
    password = f"mzp_{api_token['client_id']}{api_token['secret']}".replace("-", "")
    return password


NETWORKS = {"balancerd": {"ipam": {"config": [{"subnet": "10.10.128.0/27"}]}}}

SERVICES = [
    TestCerts(),
    Testdrive(
        materialize_url=f"postgres://{quote(ADMIN_USER)}:{app_password(ADMIN_USER)}@balancerd:6875?sslmode=require",
        materialize_use_https=True,
        no_reset=True,
        networks={"balancerd": {"ipv4_address": STATIC_IPS["testdrive"]}},
    ),
    Dnsmasq(
        dns_overrides=[
            # Docker compose will insert into all service pods an arec for
            # materialized. But what we need is a cname that points to an arec
            # with the tenant id in the name and the value should point to the
            # ip of materailized. We're going to use a new network for this
            # to ensure that we can get a unique ip space and we're going to
            # use explicit IPs for dnsmasq to set it it as the dns server for
            # balancerd.
            DnsmasqEntry(
                type="cname",
                key="materialized",
                value="environmentd.environment-58cd23ff-a4d7-4bd0-ad85-a6ff29cc86c3-0.svc.cluster.local",
            ),
            DnsmasqEntry(
                type="address",
                key="environmentd.environment-58cd23ff-a4d7-4bd0-ad85-a6ff29cc86c3-0.svc.cluster.local",
                value=STATIC_IPS["materialized"],
            ),
        ],
        networks={"balancerd": {"ipv4_address": STATIC_IPS["dnsmasq"]}},
    ),
    Balancerd(
        command=[
            "--startup-log-filter=debug",
            "service",
            "--pgwire-listen-addr=0.0.0.0:6875",
            "--https-listen-addr=0.0.0.0:6876",
            "--internal-http-listen-addr=0.0.0.0:6878",
            "--frontegg-resolver-template=materialized:6875",
            "--frontegg-jwk-file=/secrets/frontegg-mock.crt",
            f"--frontegg-api-token-url={FRONTEGG_URL}/identity/resources/auth/v1/api-token",
            f"--frontegg-admin-role={ADMIN_ROLE}",
            "--https-sni-resolver-template=materialized:6876",
            "--pgwire-sni-resolver-template=materialized:6875",
            "--tls-key=/secrets/balancerd.key",
            "--tls-cert=/secrets/balancerd.crt",
            "--internal-tls",
            "--tls-mode=require",
            "--default-config=balancerd_inject_proxy_protocol_header_http=true",
            # Nonsensical but we don't need cancellations here
            "--cancellation-resolver-dir=/secrets/",
        ],
        depends_on=["test-certs"],
        volumes=[
            "secrets:/secrets",
        ],
        # Points to DNSMasq which has an explicit ip
        dns=[STATIC_IPS["dnsmasq"]],
        networks={"balancerd": {"ipv4_address": STATIC_IPS["balancerd"]}},
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
        networks={"balancerd": {"ipv4_address": STATIC_IPS["frontegg-mock"]}},
    ),
    Mz(app_password=""),
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
        listeners_config_path=f"{MZ_ROOT}/src/materialized/ci/listener_configs/no_auth_https.json",
        networks={"balancerd": {"ipv4_address": STATIC_IPS["materialized"]}},
    ),
]


def grant_all_admin_user(c: Composition):
    # Connect once just to force the user to exist
    sql_cursor(c)
    mz_system_cursor = c.sql_cursor(service="materialized", port=6877, user="mz_system")
    mz_system_cursor.execute(
        f'GRANT ALL PRIVILEGES ON SCHEMA public TO "{ADMIN_USER}";'
    )
    mz_system_cursor.execute(
        f'GRANT ALL PRIVILEGES ON CLUSTER quickstart TO "{ADMIN_USER}";'
    )


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
    c: Composition, service="balancerd", email="u1@example.com", startup_params={}
) -> Cursor:
    return c.sql_cursor(
        service=service,
        user=email,
        password=app_password(email),
        sslmode="require",
        startup_params=startup_params,
    )


def pg8000_sql_cursor(
    c: Composition, service="balancerd", email="u1@example.com", startup_params={}
) -> pg8000.Cursor:
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    conn = pg8000.connect(
        host="127.0.0.1",
        port=c.default_port(service),
        user=email,
        password=app_password(email),
        ssl_context=ssl_context,
        startup_params=startup_params,
    )
    return conn.cursor()


def workflow_default(c: Composition) -> None:
    c.down(destroy_volumes=True)

    def process(name: str) -> None:
        if name in ["default", "plaintext"]:
            return
        with c.test_case(name):
            c.workflow(name)

    c.test_parts(list(c.workflows.keys()), process)

    with c.test_case("plaintext"):
        c.workflow("plaintext")


def workflow_plaintext(c: Composition) -> None:
    """Test plaintext internal connections"""
    c.down(destroy_volumes=True)
    with c.override(
        Materialized(
            options=[
                # Enable TLS on the public port to verify that balancerd is connecting to the balancerd
                # port.
                "--tls-mode=disable",
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
            networks={"balancerd": {"ipv4_address": STATIC_IPS["materialized"]}},
        ),
        Balancerd(
            command=[
                "service",
                "--pgwire-listen-addr=0.0.0.0:6875",
                "--https-listen-addr=0.0.0.0:6876",
                "--internal-http-listen-addr=0.0.0.0:6878",
                "--frontegg-resolver-template=materialized:6875",
                "--frontegg-jwk-file=/secrets/frontegg-mock.crt",
                f"--frontegg-api-token-url={FRONTEGG_URL}/identity/resources/auth/v1/api-token",
                f"--frontegg-admin-role={ADMIN_ROLE}",
                "--https-resolver-template=materialized:6876",
                "--tls-key=/secrets/balancerd.key",
                "--tls-cert=/secrets/balancerd.crt",
                "--default-config=balancerd_inject_proxy_protocol_header_http=true",
                # Nonsensical but we don't need cancellations here
                "--cancellation-resolver-dir=/secrets/",
            ],
            depends_on=["test-certs"],
            volumes=[
                "secrets:/secrets",
            ],
            networks={"balancerd": {"ipv4_address": STATIC_IPS["balancerd"]}},
        ),
    ):
        with c.test_case("plaintext_http"):
            c.workflow("http")
        with c.test_case("plaintext_wide_result"):
            c.workflow("wide-result")


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
        content_length = len(json_data.encode())
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
        sock.sendall(proxy_header + http_sql_query_request.encode())

        # read and parse the response
        body_separator = "\r\n\r\n"
        tcp_resp = sock.recv(8192)
        resp_split = tcp_resp.split(body_separator.encode())
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
    except ProgramLimitExceeded as e:
        assert "statement batch size cannot exceed 976.6 KiB" in str(e)
    except:
        raise RuntimeError("execute() threw an unexpected exception")

    large_pad_size = 512 * 1024 * 1024
    large_pad = "x" * large_pad_size
    try:
        cursor.execute(f"SELECT 'ABC{large_pad}XYZ';")
        raise RuntimeError("execute() expected to fail")
    except OperationalError as e:
        msg = str(e)
        assert (
            "server closed the connection unexpectedly" in msg
            or "EOF detected" in msg
            or "frame size too big" in msg
        )
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

    grant_all_admin_user(c)

    cursor = sql_cursor(c)

    cursor.execute("CREATE TABLE restart_mz (f1 INTEGER)")
    cursor.execute("START TRANSACTION")
    cursor.execute("INSERT INTO restart_mz VALUES (1)")
    c.kill("materialized")
    c.up("materialized")
    try:
        cursor.execute("INSERT INTO restart_mz VALUES (2)")
        raise RuntimeError("execute() expected to fail")
    except OperationalError as e:
        assert "SSL connection has been closed unexpectedly" in str(e)
    except:
        raise RuntimeError("execute() threw an unexpected exception")

    # Future connections work
    sql_cursor(c)


def workflow_pgwire_param_rejection(c: Composition) -> None:
    """Parameters should be rejected"""
    c.up("balancerd", "frontegg-mock", "materialized")

    def check_error(
        message: str, f: Callable[..., Any], ExpectedError: type[Exception]
    ):
        try:
            f()
        except ExpectedError:
            return
        raise AssertionError(f"Expected {message} to raise {ExpectedError}")

    # Uses pg8000, because with psycopg/libpq only a notice is printed, and
    # catching it during the connection process is not easy:
    # NOTICE:  startup setting mz_forwarded_for not set: unrecognized configuration parameter "mz_forwarded_for"
    check_error(
        "connect with mz_forwarded_for param",
        lambda: pg8000_sql_cursor(c, startup_params={"mz_forwarded_for": "1.1.1.1"}),
        InterfaceError,
    )

    check_error(
        "connect with mz_connection_uuid param",
        lambda: pg8000_sql_cursor(c, startup_params={"mz_connection_uuid": "123456"}),
        InterfaceError,
    )


def workflow_balancerd_restarted(c: Composition) -> None:
    """Existing connections should fail if balancerd is restarted"""
    c.up("balancerd", "frontegg-mock", "materialized")

    grant_all_admin_user(c)

    cursor = sql_cursor(c)

    cursor.execute("CREATE TABLE restart_balancerd (f1 INTEGER)")
    cursor.execute("START TRANSACTION")
    cursor.execute("INSERT INTO restart_balancerd VALUES (1)")
    c.kill("balancerd")
    c.up("balancerd")
    try:
        cursor.execute("INSERT INTO restart_balancerd VALUES (2)")
        raise RuntimeError("execute() expected to fail")
    except OperationalError as e:
        msg = str(e)
        assert (
            "EOF detected" in msg
            or "failed to lookup address information: Name or service not known" in msg
        )
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
    except OperationalError as e:
        assert any(
            expected in str(e)
            for expected in [
                "No route to host",
                "Connection timed out",
                "failure in name resolution",
                "failed to lookup address information",
                "Name or service not known",
                "SSL connection has been closed unexpectedly",
            ]
        )
    except:
        raise RuntimeError("connect() threw an unexpected exception")

    # Things should work now
    c.up("materialized")
    sql_cursor(c)


def workflow_user(c: Composition) -> None:
    """Test that the user is passed all the way to Mz itself."""
    with c.override(
        Balancerd(
            command=[
                "--startup-log-filter=debug",
                "service",
                "--pgwire-listen-addr=0.0.0.0:6875",
                "--https-listen-addr=0.0.0.0:6876",
                "--internal-http-listen-addr=0.0.0.0:6878",
                "--frontegg-resolver-template=materialized:6875",
                "--frontegg-jwk-file=/secrets/frontegg-mock.crt",
                f"--frontegg-api-token-url={FRONTEGG_URL}/identity/resources/auth/v1/api-token",
                f"--frontegg-admin-role={ADMIN_ROLE}",
                "--https-sni-resolver-template=materialized:6876",
                # We want to use the frontegg resolver in this
                "--pgwire-sni-resolver-template=materialized:6875",
                "--tls-key=/secrets/balancerd.key",
                "--tls-cert=/secrets/balancerd.crt",
                "--internal-tls",
                "--tls-mode=require",
                "--default-config=balancerd_inject_proxy_protocol_header_http=true",
                # Nonsensical but we don't need cancellations here
                "--cancellation-resolver-dir=/secrets/",
            ],
            depends_on=["test-certs"],
            volumes=[
                "secrets:/secrets",
            ],
            networks={"balancerd": {"ipv4_address": STATIC_IPS["balancerd"]}},
            dns=[STATIC_IPS["dnsmasq"]],
        ),
    ):
        c.up("balancerd", "dnsmasq", "frontegg-mock", "materialized")
        # Metrics aren't recorded until the connection has closed
        # Non-admin user.
        with contextlib.closing(sql_cursor(c, email=OTHER_USER)) as cursor:
            try:
                cursor.execute("DROP DATABASE materialize CASCADE")
                raise RuntimeError("execute() expected to fail")
            except ProgrammingError as e:
                assert "must be owner of DATABASE materialize" in str(e)
            except:
                raise RuntimeError("execute() threw an unexpected exception")

            cursor.execute("SELECT current_user()")
            assert OTHER_USER in str(cursor.fetchall())
            cursor.close()

        assert_metrics(c, 'mz_balancer_tenant_connection_active{source="pgwire"')
        assert_metrics(c, 'mz_balancer_tenant_connection_rx{source="pgwire"')


def workflow_many_connections(c: Composition) -> None:
    c.up("balancerd", "dnsmasq", "frontegg-mock", "materialized")
    cursors = []
    connections = 1000 - 10  #  Go almost to the limit, but not above
    print(f"Opening {connections} connections.")
    start = time.time()
    for _ in range(connections):
        cursor = sql_cursor(c)
        cursors.append(cursor)
    duration = time.time() - start
    print(
        f"{connections} connections opened in {duration} seconds, {duration/float(connections)} avg connection time"
    )

    for cursor in cursors:
        cursor.execute("SELECT 'abc'")
        data = cursor.fetchall()
        assert len(data) == 1
        row = data[0]
        assert len(row) == 1
        col = row[0]
        assert col == "abc"


def workflow_webhook(c: Composition) -> None:
    c.up(
        "balancerd",
        "dnsmasq",
        "frontegg-mock",
        "materialized",
        Service("testdrive", idle=True),
    )

    grant_all_admin_user(c)
    # This could be done in testdrive, but that doesn't seem to play
    # well with non default networks.
    cursor = sql_cursor(c)
    cursor.execute("DROP SOURCE IF EXISTS wh;")
    cursor.execute("CREATE SOURCE IF NOT EXISTS wh FROM WEBHOOK BODY FORMAT JSON;")
    balancer_port = c.port("balancerd", 6876)
    r = requests.post(
        f"https://localhost:{balancer_port}/api/webhook/materialize/public/wh",
        headers={},
        auth=(OTHER_USER, app_password(OTHER_USER)),
        json={"k": "v"},
        verify=False,
    )
    assert r.status_code == 200
    time.sleep(1.1)  # wait for webhook consistency I think
    cursor.execute("SELECT * FROM wh;")
    rows = cursor.fetchall()
    assert rows[0][0] == {"k": "v"}


def workflow_pgwire_with_sni(c: Composition) -> None:
    c.up(
        "balancerd",
        "dnsmasq",
        "materialized",
        Service("testdrive", idle=True),
    )
    # We're going to run this using ssl and, notably, without frontegg mock.
    # This should mean that we need to rely on SNI to do tenant resolution
    cursor = sql_cursor(c)
    cursor.execute("select 1;")


def retry(fn: Callable, timeout: int) -> None:
    end_time = (
        datetime.datetime.now() + datetime.timedelta(seconds=timeout)
    ).timestamp()
    while time.time() < end_time:
        try:
            fn()
            return
        except:
            pass
        time.sleep(1)
    fn()
