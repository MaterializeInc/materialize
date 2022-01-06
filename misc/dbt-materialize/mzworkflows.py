# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Dict

from materialize.mzcompose import Workflow
from materialize.mzcompose.services import Materialized, Service, TestCerts

services = [
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


def workflow_ci(w: Workflow):
    """Runs the dbt adapter test suite against Materialize with and without TLS."""
    workflow_no_tls(w)
    workflow_tls(w)


def workflow_no_tls(w: Workflow):
    """Runs the dbt adapter test suite against Materialize with TLS disabled."""
    run_test(w, "materialized", {"DBT_HOST": "materialized"})


def workflow_tls(w: Workflow):
    """Runs the dbt adapter test suite against Materialize with TLS enabled."""
    run_test(
        w,
        "materialized-tls",
        {
            "DBT_HOST": "materialized-tls",
            "DBT_SSLCERT": "/secrets/materialized.crt",
            "DBT_SSLKEY": "/secrets/materialized.key",
            "DBT_SSLROOTCERT": "/secrets/ca.crt",
        },
    )


def run_test(w: Workflow, materialized: str, env: Dict[str, str]):
    w.start_services(services=[materialized])
    w.wait_for_tcp(host=materialized, port=6875)
    w.run_service(
        service="dbt-test",
        command=["pytest", "dbt-materialize/test"],
        env=env,
    )
