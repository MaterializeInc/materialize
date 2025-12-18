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

import os
import pathlib
import posixpath
import re
from typing import Any

import yaml

from materialize import MZ_ROOT, buildkite, ci_util
from materialize.mzcompose.composition import (
    Composition,
    Service,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.azurite import Azurite
from materialize.mzcompose.services.fivetran_destination import FivetranDestination
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Minio
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.redpanda import Redpanda
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.sql_server import SqlServer
from materialize.mzcompose.services.ssh_bastion_host import SshBastionHost
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.zookeeper import Zookeeper

SERVICES = [
    SshBastionHost(allow_any_key=True),
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Redpanda(),
    Postgres(),
    MySql(),
    Azurite(),
    Mz(app_password=""),
    Minio(setup_materialize=False, additional_directories=["copytos3"]),
    Materialized(),
    FivetranDestination(volumes_extra=["tmp:/share/tmp"]),
    Testdrive(),
    SqlServer(),
]


LOCATION = MZ_ROOT / "doc" / "user" / "data" / "examples"
INCLUDE_RE = re.compile(
    r"""\{\{\s*[%<]\s*include-md      # {{% include-md  or {{< include-md
        [\s\S]*?file\s*=\s*"([^"]+)"  # file="path"
        [\s\S]*?[%>]\s*\}\}           # %}} or >}}
    """,
    re.VERBOSE,
)


def md_include(text: str) -> str:
    while True:
        new = INCLUDE_RE.sub(
            lambda m: (pathlib.Path("doc/user") / m.group(1)).read_text(), text
        )
        if new == text:
            return new
        text = new


def td_query(
    sql: str, results: list[str] | str | None, replacements: dict[Any, Any]
) -> list[str]:
    result: list[str] = []

    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    sql = re.sub(r"--.*?$", "", sql, flags=re.MULTILINE)
    for old, new in replacements.items():
        sql = sql.replace(str(old), str(new))

    for i, query in enumerate(sql.split(";")):
        lines = query.strip().splitlines()
        if not lines:
            continue
        result.append(f"> {lines[0]}\n" + "\n".join(f"  {line}" for line in lines[1:]))
        if isinstance(results, list):
            result.append(td_result(results[i], replacements))

    if isinstance(results, str):
        result.append(td_result(results, replacements))

    return result


def td_result(raw: str, replacements: dict[Any, Any]) -> str:
    result = []
    in_block = False
    text = md_include(raw)
    is_open_table = False
    for line in text.splitlines():
        if line.startswith("```"):
            if in_block:
                break
            else:
                in_block = True
                result = []
        elif in_block:
            if set(line) == {"+", "-"}:
                is_open_table = True
                # Skip header
                result = []
            elif set(line) == {"|", " ", "-"}:
                is_open_table = False
                # Skip header
                result = []
            else:
                parts = re.split(r"\s*\|\s*", line)
                parts = [
                    p if p and p != "null" else ("<null>" if p == "null" else '""')
                    for p in parts
                ]
                if line.startswith("|") and not is_open_table:
                    parts = parts[1:]
                if line.endswith("|") and not is_open_table:
                    parts = parts[:-1]
                result.append(" ".join(parts))
        else:
            result.append(line)
    text = "\n".join(result)
    for old, new in replacements.items():
        text = text.replace(str(old), str(new))
    return text


def td_snippet(file: pathlib.Path) -> str:
    with open(file) as f:
        examples = yaml.load(f, Loader=yaml.Loader)
    snippet: list[str] = []
    for example in examples:
        if "test_depends_on" in example:
            snippet.append(td_snippet(LOCATION / example["test_depends_on"]))
        if (
            not "code" in example
            or not example.get("testable", True)
            or "syntax" in example.get("name", "")
        ):
            continue
        if "test_setup" in example:
            snippet.append(example["test_setup"])
        snippet.extend(
            td_query(
                example["code"],
                (
                    (example.get("test_results") or example.get("results"))
                    if example.get("testable_results", True)
                    else None
                ),
                example.get("test_replacements", {}),
            )
        )
    return "\n".join(snippet).strip()


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "files",
        nargs="*",
        default=["*.yml"],
        help="run against the specified files",
    )
    args = parser.parse_args()

    c.up(
        "fivetran-destination",
        "materialized",
        "postgres",
        "mysql",
        "minio",
        "zookeeper",
        "kafka",
        "schema-registry",
        "ssh-bastion-host",
        "sql-server",
        Service("testdrive", idle=True),
    )

    def process(file: pathlib.Path) -> None:
        snippet = td_snippet(file)
        if not snippet:
            return
        print(f"--- {posixpath.relpath(file, LOCATION)}")
        junit_report = ci_util.junit_report_filename(str(file).replace("/", "_"))
        c.testdrive(
            snippet,
            silent=True,
            args=[f"--junit-report={junit_report}"],
            caller=f"{file}:1",
        )
        # Uploading successful junit files wastes time and contains no useful information
        os.remove(MZ_ROOT / "test" / "doc-examples" / junit_report)

    files_unsharded: list[pathlib.Path] = []
    for file in args.files:
        files_unsharded.extend(LOCATION.rglob(file))
    files: list[pathlib.Path] = buildkite.shard_list(
        sorted(files_unsharded),
        lambda file: str(file),
    )
    c.test_parts(files, process)
