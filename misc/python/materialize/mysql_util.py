# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from dataclasses import dataclass

from materialize.mzcompose.composition import Composition


@dataclass
class MySqlSslContext:
    ca: str
    client_cert: str
    client_key: str


def retrieve_ssl_context_for_mysql(c: Composition) -> MySqlSslContext:
    # MySQL generates self-signed certificates for SSL connections on startup,
    # for both the server and client:
    # https://dev.mysql.com/doc/refman/8.3/en/creating-ssl-rsa-files-using-mysql.html
    # Grab the correct Server CA and Client Key and Cert from the MySQL container
    # (and strip the trailing null byte):
    ssl_ca = c.exec("mysql", "cat", "/var/lib/mysql/ca.pem", capture=True).stdout.split(
        "\x00", 1
    )[0]
    ssl_client_cert = c.exec(
        "mysql", "cat", "/var/lib/mysql/client-cert.pem", capture=True
    ).stdout.split("\x00", 1)[0]
    ssl_client_key = c.exec(
        "mysql", "cat", "/var/lib/mysql/client-key.pem", capture=True
    ).stdout.split("\x00", 1)[0]

    return MySqlSslContext(ssl_ca, ssl_client_cert, ssl_client_key)


def retrieve_invalid_ssl_context_for_mysql(c: Composition) -> MySqlSslContext:
    # Use the TestCert service to obtain a wrong CA and client cert/key:
    c.up({"name": "test-certs", "persistent": True})
    ssl_wrong_ca = c.run("test-certs", "cat", "/secrets/ca.crt", capture=True).stdout
    ssl_wrong_client_cert = c.run(
        "test-certs", "cat", "/secrets/certuser.crt", capture=True
    ).stdout
    ssl_wrong_client_key = c.run(
        "test-certs", "cat", "/secrets/certuser.key", capture=True
    ).stdout
    return MySqlSslContext(ssl_wrong_ca, ssl_wrong_client_cert, ssl_wrong_client_key)
