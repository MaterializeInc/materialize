# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Dict

from materialize.mzcompose import Composition
from materialize.mzcompose.services import Materialized, Service, TestCerts

SERVICES = [
    TestCerts(),
    Materialized(),
    Materialized(
        name="materialized-tls",
        options=[
            "--tls-mode=verify-ca",
            "--tls-cert=/secrets/materialized.crt",
            "--tls-key=/secrets/materialized.key",
            "--tls-ca=/secrets/ca.crt",
        ],
        depends_on=["test-certs"],
        volumes=["secrets:/secrets"],
    ),
    Service(
        "dbt-test",
        {
            "mzbuild": "dbt-materialize",
            "depends_on": ["test-certs"],
            "volumes": ["secrets:/secrets"],
        },
    ),
]


def workflow_ci(c: Composition) -> None:
    """Runs the dbt adapter test suite against Materialize with and without TLS."""
    workflow_no_tls(c)
    workflow_tls(c)


def workflow_no_tls(c: Composition) -> None:
    """Runs the dbt adapter test suite against Materialize with TLS disabled."""
    run_test(c, "materialized", {"DBT_HOST": "materialized"})


def workflow_tls(c: Composition) -> None:
    """Runs the dbt adapter test suite against Materialize with TLS enabled."""
    run_test(
        c,
        "materialized-tls",
        {
            "DBT_HOST": "materialized-tls",
            "DBT_SSLCERT": "/secrets/materialized.crt",
            "DBT_SSLKEY": "/secrets/materialized.key",
            "DBT_SSLROOTCERT": "/secrets/ca.crt",
        },
    )


def run_test(c: Composition, materialized: str, env: Dict[str, str]) -> None:
    c.start_services(services=[materialized])
    c.wait_for_tcp(host=materialized, port=6875)
    c.run_service(
        service="dbt-test",
        command=["pytest", "dbt-materialize/test"],
        env=env,
    )
