# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.test_certs import TestCerts
from materialize.mzcompose.services.testdrive import Testdrive

SERVICES = [
    Materialized(volumes_extra=["secrets:/share/secrets"]),
    Testdrive(),
    TestCerts(),
    # Set the max slot WAL keep size to 10MB
    Postgres(extra_command=["-c", "max_slot_wal_keep_size=10"]),
]

# Test that ceased statuses persist across restart
def workflow_ceased_status(c: Composition, parser: WorkflowArgumentParser) -> None:
    with c.override(Testdrive(no_reset=True)):
        c.up("materialized", "postgres")
        c.run("testdrive", "ceased/before-mz-restart.td")

        # Restart mz
        c.kill("materialized")
        c.up("materialized")

        c.run("testdrive", "ceased/after-mz-restart.td")


def workflow_replication_slots(c: Composition, parser: WorkflowArgumentParser) -> None:
    with c.override(Postgres(extra_command=["-c", "max_replication_slots=2"])):
        c.up("materialized", "postgres")
        c.run("testdrive", "override/replication-slots.td")


def workflow_wal_level(c: Composition, parser: WorkflowArgumentParser) -> None:
    for wal_level in ["replica", "minimal"]:
        with c.override(
            Postgres(
                extra_command=[
                    "-c",
                    "max_wal_senders=0",
                    "-c",
                    f"wal_level={wal_level}",
                ]
            )
        ):
            c.up("materialized", "postgres")
            c.run("testdrive", "override/insufficient-wal-level.td")


def workflow_replication_disabled(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    with c.override(Postgres(extra_command=["-c", "max_wal_senders=0"])):
        c.up("materialized", "postgres")
        c.run("testdrive", "override/replication-disabled.td")


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

    c.up("materialized", "test-certs", "postgres")
    c.run(
        "testdrive",
        f"--var=ssl-ca={ssl_ca}",
        f"--var=ssl-cert={ssl_cert}",
        f"--var=ssl-key={ssl_key}",
        f"--var=ssl-wrong-cert={ssl_wrong_cert}",
        f"--var=ssl-wrong-key={ssl_wrong_key}",
        f"--var=default-replica-size={Materialized.Size.DEFAULT_SIZE}-{Materialized.Size.DEFAULT_SIZE}",
        f"--var=default-storage-size={Materialized.Size.DEFAULT_SIZE}-1",
        *args.filter,
    )
