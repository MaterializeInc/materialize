# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Shared code for running SQLancer and SQLancer++ against Materialize.
"""

import argparse
import random
import re
import shutil
import subprocess
from collections.abc import Callable
from pathlib import Path
from threading import Thread

from materialize import buildkite, spawn
from materialize.mzcompose.composition import (
    Composition,
    WorkflowArgumentParser,
)
from materialize.mzcompose.service import Service
from materialize.mzcompose.services.materialized import Materialized

IGNORED_ERROR_PATTERNS = [
    r"^(?!.*panic).*not yet supported",
    r"does not exist",
    r"must have",
    r"overflow",
    r"invalid input",
    r"cannot be matched",
    r"implicitly casting",
    r"is not unique",
    r"invalid digit",
    r"is not defined",
    r"is defined",
    r"unterminated",
    r"Expected ",
    r"is out of range",
    r"of a negative",
    r"does not support",
    r"ANALYZE",
    r"is of type",
    r"must appear in the",
    r"negative substring length",
    r"is ambiguous",
    r"out of range",
    r"division by zero",
    r"is only defined for finite",
    r"cannot canonicalize predicates that are not of type",
    r"unexpected character in input",
    r"could not determine polymorphic type",
    r"cannot reference pseudo type",
    r"string is not a valid identifier",
    r"invalid regular expression",
    r" violates not-null constraint",
    r"could not convert type",
    r"are not allowed",
    r"requires a record",
    r"unrecognized privilege type",
    r"cannot cast",
    r"requires an OVER",
    r"value too long",
    r"may only refer to user-defined",
    r"cannot materialize call to",
    r"invalid hash algorithm",
    r"expected id",
    r"expected exactly one statement",
    r"regex parse error",
    r"invalid encoding name",
    r"must specify at least one capture group",
    r"requires a string literal",
    r"too large for encoding",
    r"must be a positive integer",
    r"bound must be less than",
    r"not recognized",
    r"invalid escape string",
    r"canceling statement due to statement timeout",
    r"invalid time zone",
    r"result exceeds max size",
    r"calls to mz_now in write statements",
    r"out of valid range",
    r"more than one record produced in subquery",
    r"input of anonymous composite types is not implemented",
    r"lists must all be the same length",
    r"invalid IANA Time Zone Database identifier",
    r"unknown schema",
    r"octal escapes are not supported",
    r"attempt to create relation with too many columns",
    r"column notation applied to type text",
    r"Unexpected EOF",
    r"missing required exponent",
    r"invalid unicode escape",
    r"dimension array or low bound array must not be null",
    r"LIKE pattern exceeds maximum length",
    r"cannot return complex numbers",
    r"must be greater than zero",
    r"null character not permitted",
    r"invalid datepart",
    r"must use value within",
    r"null character not permitted",
    r"expressions must appear in select list",
    r"expressions must match initial",
    r"invalid selection: operation may only",
    r"array size exceeds the maximum allowed",
    r"does not allow subqueries",
    r"must use value within",
    r"zero raised to a negative power is undefined",
    r"range type over",
]


def check_query_errors(logs_dir: Path) -> list[str]:
    unexpected_errors = []
    log_files = list(logs_dir.glob("*.log"))

    for log_file in log_files:
        try:
            content = log_file.read_text()
        except Exception:
            continue

        for line in content.splitlines():
            if "ERROR" not in line:
                continue

            is_ignored = False
            for pattern in IGNORED_ERROR_PATTERNS:
                if re.search(pattern, line):
                    is_ignored = True
                    break

            if not is_ignored:
                unexpected_errors.append(f"{log_file.name}: {line}")

    return unexpected_errors


def create_services(service_name: str) -> list[Service | Materialized]:
    """Create the services needed for SQLancer or SQLancer++ tests."""
    return [
        # Auto-restart so we can keep testing even after we ran into a panic
        Materialized(
            restart="on-failure",
            default_replication_factor=1,
            additional_system_parameter_defaults={
                "enable_alter_table_add_column": "true",
                "enable_statement_lifecycle_logging": "false",
                "enable_internal_statement_logging": "false",
                "statement_logging_default_sample_rate": "0",
                "statement_logging_max_sample_rate": "0",
                "enable_repeat_row": "true",
                "enable_list_length_max": "true",
                "enable_list_n_layers": "true",
                "enable_time_at_time_zone": "true",
                "enable_date_bin_hopping": "true",
            },
        ),
        Service(
            service_name,
            {
                "mzbuild": service_name,
            },
        ),
    ]


def _print_logs(container_id: str) -> None:
    spawn.runv(["docker", "logs", "-f", container_id])


def run_sqlancer(
    c: Composition,
    parser: WorkflowArgumentParser,
    *,
    service_name: str,
    default_oracle: str,
    build_run_args: Callable[[argparse.Namespace, int], list[str]],
    docker_logs_path: str,
    log_prefix: str,
) -> None:
    parser.add_argument("--runtime", default=600, type=int)
    parser.add_argument("--num-tries", default=100000, type=int)
    parser.add_argument("--num-threads", default=16, type=int)
    parser.add_argument("--seed", default=None, type=int)
    parser.add_argument("--qpg", default=True, action=argparse.BooleanOptionalAction)
    parser.add_argument("--oracle", default=default_oracle, type=str)
    args = parser.parse_args()

    c.up("materialized")

    c.sql(
        "ALTER SYSTEM SET max_databases TO 1000",
        user="mz_system",
        port=6877,
    )
    c.sql(
        "ALTER SYSTEM SET max_tables TO 1000",
        user="mz_system",
        port=6877,
    )
    c.sql(
        "ALTER SYSTEM SET max_materialized_views TO 1000",
        user="mz_system",
        port=6877,
    )

    seed = args.seed or random.randint(0, 2**31)

    run_args = build_run_args(args, seed)

    print("--- Run in progress")
    result = c.run(
        service_name,
        *run_args,
        check=False,
        detach=True,
        capture=True,
    )
    container_id = result.stdout.strip()

    # Print logs in a background thread so that we get immediate output in CI,
    # and also when running SQLancer locally
    thread = Thread(target=_print_logs, args=(container_id,))
    thread.start()
    # At the same time capture the logs to analyze for finding new issues
    stdout = spawn.capture(
        ["docker", "logs", "-f", container_id], stderr=subprocess.STDOUT
    )

    in_assertion = False
    for line in stdout.splitlines():
        if "OutOfMemoryError" in line or "IgnoreMeException" in line:
            continue
        if line.startswith("--java.lang."):
            in_assertion = True
            print(f"--- [{log_prefix}] {line.removeprefix('--java.lang.')}")
        elif line == "":
            in_assertion = False
        elif in_assertion:
            print(line)
    print(f"--- {result.stdout.splitlines()[-1]}")

    # Check for unexpected query errors in the logs
    logs = Path("logs")
    if logs.exists() and logs.is_dir():
        shutil.rmtree(logs)
    spawn.runv(
        [
            "docker",
            "cp",
            f"{container_id}:{docker_logs_path}",
            str(logs),
        ]
    )
    spawn.runv(["tar", "cfz", "logs.tar.gz", str(logs)])
    buildkite.upload_artifact("logs.tar.gz")
    unexpected_errors = check_query_errors(logs)
    if unexpected_errors:
        print("--- Unexpected query errors found:")
        for error in unexpected_errors:
            print(error)
        raise Exception(
            f"Found {len(unexpected_errors)} unexpected query error(s) in logs"
        )
