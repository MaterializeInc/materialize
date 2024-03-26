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
from materialize.mzcompose.services.toxiproxy import Toxiproxy

# Set the max slot WAL keep size to 10MB
DEFAULT_PG_EXTRA_COMMAND = ["-c", "max_slot_wal_keep_size=10"]


def create_postgres(
    pg_version: str | None, extra_command: list[str] = DEFAULT_PG_EXTRA_COMMAND
) -> Postgres:
    if pg_version is None:
        image = None
    else:
        image = f"postgres:{pg_version}"

    return Postgres(image=image, extra_command=extra_command)


SERVICES = [
    Materialized(volumes_extra=["secrets:/share/secrets"]),
    Testdrive(),
    TestCerts(),
    Toxiproxy(),
    create_postgres(pg_version=None),
]


def get_targeted_pg_version(parser: WorkflowArgumentParser) -> str | None:
    parser.add_argument(
        "--pg-version",
        type=str,
    )

    args, _ = parser.parse_known_args()
    pg_version = args.pg_version

    if pg_version is not None:
        print(f"Running with Postgres version {pg_version}")

    return pg_version


# TODO: redesign ceased status #25768
# Test that how subsource statuses work across a variety of scenarios
# def workflow_statuses(c: Composition, parser: WorkflowArgumentParser) -> None:
#     c.up("materialized", "postgres", "toxiproxy")
#     c.run_testdrive_files("status/01-setup.td")

#     with c.override(Testdrive(no_reset=True)):
#         # Restart mz
#         c.kill("materialized")
#         c.up("materialized")

#         c.run_testdrive_files(
#             "status/02-after-mz-restart.td",
#             "status/03-toxiproxy-interrupt.td",
#             "status/04-drop-publication.td",
#         )


def workflow_replication_slots(c: Composition, parser: WorkflowArgumentParser) -> None:
    pg_version = get_targeted_pg_version(parser)
    with c.override(
        create_postgres(
            pg_version=pg_version, extra_command=["-c", "max_replication_slots=3"]
        )
    ):
        c.up("materialized", "postgres")
        c.run_testdrive_files("override/replication-slots.td")


def workflow_wal_level(c: Composition, parser: WorkflowArgumentParser) -> None:
    pg_version = get_targeted_pg_version(parser)
    for wal_level in ["replica", "minimal"]:
        with c.override(
            create_postgres(
                pg_version=pg_version,
                extra_command=[
                    "-c",
                    "max_wal_senders=0",
                    "-c",
                    f"wal_level={wal_level}",
                ],
            )
        ):
            c.up("materialized", "postgres")
            c.run_testdrive_files("override/insufficient-wal-level.td")


def workflow_replication_disabled(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    pg_version = get_targeted_pg_version(parser)
    with c.override(
        create_postgres(
            pg_version=pg_version, extra_command=["-c", "max_wal_senders=0"]
        )
    ):
        c.up("materialized", "postgres")
        c.run_testdrive_files("override/replication-disabled.td")


def workflow_cdc(c: Composition, parser: WorkflowArgumentParser) -> None:
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

    pg_version = get_targeted_pg_version(parser)
    with c.override(create_postgres(pg_version=pg_version)):
        c.up("materialized", "test-certs", "postgres")
        c.run_testdrive_files(
            f"--var=ssl-ca={ssl_ca}",
            f"--var=ssl-cert={ssl_cert}",
            f"--var=ssl-key={ssl_key}",
            f"--var=ssl-wrong-cert={ssl_wrong_cert}",
            f"--var=ssl-wrong-key={ssl_wrong_key}",
            f"--var=default-replica-size={Materialized.Size.DEFAULT_SIZE}-{Materialized.Size.DEFAULT_SIZE}",
            f"--var=default-storage-size={Materialized.Size.DEFAULT_SIZE}-1",
            *args.filter,
        )


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    remaining_args = [arg for arg in parser.args if not arg.startswith("--pg-version")]

    # If args were passed then we are running the main CDC workflow
    if remaining_args:
        workflow_cdc(c, parser)
    else:
        # Otherwise we are running all workflows
        for name in c.workflows:
            # clear postgres to avoid issues with special arguments conflicting with existing state
            c.kill("postgres")
            c.rm("postgres")

            if name == "default":
                continue

            # TODO: Flaky, reenable when #25479 is fixed
            if name == "statuses":
                continue

            with c.test_case(name):
                c.workflow(name)
