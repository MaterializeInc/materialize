# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Testdrive is the basic framework and language for defining product tests under
the expected-result/actual-result (aka golden testing) paradigm. A query is
retried until it produces the desired result.
"""

from pathlib import Path

from prompt_toolkit import PromptSession
from prompt_toolkit.lexers import PygmentsLexer
from pygments.lexers.sql import SqlLexer
from prompt_toolkit import PromptSession
from prompt_toolkit.completion import Completer, WordCompleter, Completion
from prompt_toolkit.document import Document
from prompt_toolkit.lexers import PygmentsLexer
from pygments.lexers.sql import SqlLexer
from prompt_toolkit.shortcuts import CompleteStyle

from materialize import ci_util
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.fivetran_destination import FivetranDestination
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Minio
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.redpanda import Redpanda
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.zookeeper import Zookeeper

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Redpanda(),
    Postgres(),
    MySql(),
    Minio(setup_materialize=True, additional_directories=["copytos3"]),
    Materialized(external_minio=True),
    FivetranDestination(volumes_extra=["tmp:/share/tmp"]),
    Testdrive(external_minio=True),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Run testdrive."""
    parser.add_argument(
        "--redpanda",
        action="store_true",
        help="run against Redpanda instead of the Confluent Platform",
    )
    parser.add_argument(
        "--aws-region",
        help="run against the specified AWS region instead of localstack",
    )
    parser.add_argument(
        "--kafka-default-partitions",
        type=int,
        metavar="N",
        help="set the default number of kafka partitions per topic",
    )
    parser.add_argument(
        "--default-size",
        type=int,
        default=Materialized.Size.DEFAULT_SIZE,
        help="Use SIZE 'N-N' for replicas and SIZE 'N' for sources",
    )
    parser.add_argument(
        "--system-param",
        type=str,
        action="append",
        nargs="*",
        help="System parameters to set in Materialize, i.e. what you would set with `ALTER SYSTEM SET`",
    )

    parser.add_argument("--replicas", type=int, default=1, help="use multiple replicas")

    parser.add_argument(
        "--default-timeout",
        type=str,
        help="set the default timeout for Testdrive",
    )

    parser.add_argument(
        "--rewrite-results",
        action="store_true",
        help="Rewrite results, disables junit reports",
    )

    parser.add_argument(
        "files",
        nargs="*",
        default=["*.td"],
        help="run against the specified files",
    )
    (args, passthrough_args) = parser.parse_known_args()

    dependencies = [
        "fivetran-destination",
        "minio",
        "materialized",
        "postgres",
        "mysql",
    ]
    if args.redpanda:
        dependencies += ["redpanda"]
    else:
        dependencies += ["zookeeper", "kafka", "schema-registry"]

    testdrive = Testdrive(
        forward_buildkite_shard=True,
        kafka_default_partitions=args.kafka_default_partitions,
        aws_region=args.aws_region,
        validate_catalog_store=True,
        default_timeout=args.default_timeout,
        volumes_extra=["mzdata:/mzdata"],
        external_minio=True,
        fivetran_destination=True,
        fivetran_destination_files_path="/share/tmp",
        entrypoint_extra=[f"--var=uses-redpanda={args.redpanda}"],
    )

    sysparams = args.system_param
    if not args.system_param:
        sysparams = []

    additional_system_parameter_defaults = {}
    for val in sysparams:
        x = val[0].split("=", maxsplit=1)
        assert len(x) == 2, f"--system-param '{val}' should be the format <key>=<val>"
        key = x[0]
        val = x[1]

        additional_system_parameter_defaults[key] = val

    materialized = Materialized(
        default_size=args.default_size,
        external_minio=True,
        additional_system_parameter_defaults=additional_system_parameter_defaults,
    )

    with c.override(testdrive, materialized):
        c.up(*dependencies)

        c.sql(
            "ALTER SYSTEM SET max_clusters = 50;",
            port=6877,
            user="mz_system",
        )

        non_default_testdrive_vars = []

        if args.replicas > 1:
            c.sql("DROP CLUSTER quickstart CASCADE", user="mz_system", port=6877)
            # Make sure a replica named 'r1' always exists
            replica_names = [
                "r1" if replica_id == 0 else f"replica{replica_id}"
                for replica_id in range(0, args.replicas)
            ]
            replica_string = ",".join(
                f"{replica_name} (SIZE '{materialized.default_replica_size}')"
                for replica_name in replica_names
            )
            c.sql(
                f"CREATE CLUSTER quickstart REPLICAS ({replica_string})",
                user="mz_system",
                port=6877,
            )

            # Note that any command that outputs SHOW CLUSTERS will have output
            # that depends on the number of replicas testdrive has. This means
            # it might be easier to skip certain tests if the number of replicas
            # is > 1.
            c.sql(
                f"""
                CREATE CLUSTER testdrive_single_replica_cluster SIZE = '{materialized.default_replica_size}';
                GRANT ALL PRIVILEGES ON CLUSTER testdrive_single_replica_cluster TO materialize;
                """,
                user="mz_system",
                port=6877,
            )

            non_default_testdrive_vars.append(f"--var=replicas={args.replicas}")
            non_default_testdrive_vars.append(
                "--var=single-replica-cluster=testdrive_single_replica_cluster"
            )

        if args.default_size != 1:
            non_default_testdrive_vars.append(
                f"--var=default-replica-size={materialized.default_replica_size}"
            )
            non_default_testdrive_vars.append(
                f"--var=default-storage-size={materialized.default_storage_size}"
            )

        junit_report = ci_util.junit_report_filename(c.name)

        try:
            junit_report = ci_util.junit_report_filename(c.name)
            print(f"Passing through arguments to testdrive {passthrough_args}\n")
            # do not set default args, they should be set in the td file using set-arg-default to easen the execution
            # without mzcompose
            for file in args.files:
                c.run_testdrive_files(
                    (
                        "--rewrite-results"
                        if args.rewrite_results
                        else f"--junit-report={junit_report}"
                    ),
                    *non_default_testdrive_vars,
                    *passthrough_args,
                    file,
                )
                c.sanity_restart_mz()
        finally:
            ci_util.upload_junit_report(
                "testdrive", Path(__file__).parent / junit_report
            )

class TdCompleter(Completer):
    def get_completions(self, document: Document, complete_event):
        text = document.text_before_cursor
        words = text.split()

        # Define available commands and parameters
        commands = {
            ">": {"description": "Run a SQL query"},
            "$ kafka-ingest": {
                "description": "Sends the data provided to a kafka topic",
                "params": {
                    "topic=": "The topic to publish the data to",
                    "schema=": "The schema to use",
                    "format=": "The format in which the data is provided (avro|bytes)",
                    "key-format=": "For data that includes a key, the format of the key (avro|bytes)",
                    "key-schema=": "For data that contains a key, the schema of the key",
                    "key-terminator=": "Separator between key and data (bytes format)",
                    "repeat=": "Send the same data N times to Kafka",
                    "start-iteration=": "Set starting value of the ${kafka-ingest.iteration}",
                    "partition=": "Send the data to a specified partition",
                },
                "param_values": {
                    "format=": ["avro", "bytes"],
                    "key-format=": ["avro", "bytes"]
                }
            },
            "$ kafka-add-partitions": {"description": "Add partitions to an existing topic"},
        }

        # Determine the current context
        if not words or text.startswith("$"):
            # We're at the root level (no command or starting with "$"), suggest commands
            for command in commands.keys():
                if command.startswith(text):
                    yield Completion(command, start_position=-len(text), display_meta=commands[command]["description"])
        elif words[0] == "$" and len(words) > 1 and words[1] in commands:
            # The first command is entered, suggest parameters
            command = commands[words[1]]
            params = command["params"]

            # Suggest parameters if we are typing a new one
            if len(words) == 2 or (len(words) > 2 and "=" not in words[-1]):
                for param, description in params.items():
                    if param.startswith(words[-1]):
                        yield Completion(param, start_position=-len(words[-1]), display_meta=description)
            else:
                # Suggest values for specific parameters
                param = words[-2]
                if param in command["param_values"]:
                    for value in command["param_values"][param]:
                        if value.startswith(words[-1]):
                            yield Completion(value, start_position=-len(words[-1]), display_meta="Value")

def workflow_interactive(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Interactively write a new testdrive file"""
    parser.add_argument(
        "file",
        type=str,
        default=None,
        nargs="?",
        help="start with the specified file and extend it",
    )
    parser.parse_args()

    dependencies = [
        "fivetran-destination",
        "minio",
        "materialized",
        "postgres",
        "mysql",
        "zookeeper",
        "kafka",
        "schema-registry",
    ]

    with c.override(
        Testdrive(seed=1)):
        c.up(*dependencies)
        c.up("testdrive", persistent=True)

        c.sql(
            "ALTER SYSTEM SET max_clusters = 50;",
            port=6877,
            user="mz_system",
        )

        # TODO: args.file

        text = None

        session = PromptSession(lexer=PygmentsLexer(SqlLexer))

        while True:
            try:
                session = PromptSession(completer=TdCompleter())
                return session.prompt("td# ")
                #text = session.prompt(f"{first_char} ")
                #if not text:
                #    text = prompt("td| ")
                #if text.startswith(">"):
                #    result = c.sql_query(text[1:])
                #    for row in result:
                #        print(" ".join(row))
                #    action = button_dialog(
                #        title="Query Action",
                #        text="Choices",
                #        buttons=[
                #            ("Rerun query", "rerun"),
                #            ("Edit query", "edit"),
                #            ("Store result", "store"),
                #            ("Exit", "exit"),
                #        ],
                #    ).run()

                #    if action == "rerun":
                #        continue
                #    elif action == "edit":
                #        text = prompt("Edit query: ", default=text)
                #    elif action == "store":
                #        pass
                #    elif action == "exit":
                #        break
                #    else:
                #        raise RuntimeError(f"Unknown action {action}")

                #print("Result:")
                #print(result)
            except (KeyboardInterrupt, EOFError):
                return
