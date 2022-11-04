# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose import Composition, WorkflowArgumentParser
from materialize.mzcompose.services import Materialized, Postgres, TestCerts, Testdrive

SERVICES = [
    Materialized(volumes_extra=["secrets:/share/secrets"]),
    Testdrive(),
    TestCerts(),
    Postgres(),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "filter",
        nargs="*",
        default=["*.td"],
        help="limit to only the files matching filter",
    )
    args = parser.parse_args()

    ssl_ca = c.run("test-certs", "cat", "/secrets/ca.crt", capture=True).stdout
    ssl_cert = c.run("test-certs", "cat", "/secrets/certuser.crt", capture=True).stdout
    ssl_key = c.run("test-certs", "cat", "/secrets/certuser.key", capture=True).stdout
    ssl_wrong_cert = c.run(
        "test-certs", "cat", "/secrets/postgres.crt", capture=True
    ).stdout
    ssl_wrong_key = c.run(
        "test-certs", "cat", "/secrets/postgres.key", capture=True
    ).stdout

    c.up("materialized", "test-certs", "testdrive", "postgres")
    c.wait_for_materialized()
    c.wait_for_postgres()
    c.run(
        "testdrive",
        f"--var=ssl-ca={ssl_ca}",
        f"--var=ssl-cert={ssl_cert}",
        f"--var=ssl-key={ssl_key}",
        f"--var=ssl-wrong-cert={ssl_wrong_cert}",
        f"--var=ssl-wrong-key={ssl_wrong_key}",
        *args.filter,
    )
