# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Simulates workloads captured via `bin/mz-workload-capture` in a local run using
Docker Compose.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime
import json
import math
import os
import pathlib
import posixpath
import random
import re
import string
import subprocess
import threading
import time
import urllib.parse
import uuid
from collections import Counter, defaultdict
from functools import cache
from pathlib import Path
from textwrap import dedent
from typing import Any
from typing import Literal as TypeLiteral

import aiohttp
import confluent_kafka  # type: ignore
import matplotlib.pyplot as plt
import numpy
import psycopg
import pymysql
import pymysql.cursors
import yaml
from confluent_kafka.admin import AdminClient  # type: ignore
from confluent_kafka.schema_registry import SchemaRegistryClient  # type: ignore
from confluent_kafka.schema_registry.avro import AvroSerializer  # type: ignore
from confluent_kafka.serialization import (  # type: ignore
    MessageField,
    SerializationContext,
)
from pg8000.native import literal
from psycopg.sql import SQL, Identifier, Literal

from materialize import MZ_ROOT, buildkite, spawn, ui
from materialize.docker import image_registry
from materialize.mzcompose.composition import (
    Composition,
    Service,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.azurite import Azurite
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.redpanda import Redpanda
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.sql_server import SqlServer
from materialize.mzcompose.services.ssh_bastion_host import SshBastionHost
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.zookeeper import Zookeeper
from materialize.mzcompose.test_result import (
    FailedTestExecutionError,
    TestFailureDetails,
)
from materialize.util import PropagatingThread
from materialize.version_list import resolve_ancestor_image_tag

LOCATION = MZ_ROOT / "test" / "workload-replay" / "captured-workloads"
WORKLOAD_REPLAY_VERSION = "1.0.0"  # Used for uploading test analytics results
SEED_RANGE = 1_000_000

cluster_replica_sizes = {
    "bootstrap": {
        "cpu_exclusive": False,
        "cpu_limit": 0.5,
        "credits_per_hour": "0.25",
        "disk_limit": "7762MiB",
        "memory_limit": "3881MiB",
        "scale": 1,
        "workers": 1,
    },
    # M.1 cluster replica sizes
    "M.1-nano": {
        "cpu_exclusive": False,
        "cpu_limit": 0.5,
        "credits_per_hour": "0.75",
        "disk_limit": "26624MiB",
        "memory_limit": "3881MiB",
        "scale": 1,
        "workers": 1,
    },
    "M.1-micro": {
        "cpu_exclusive": True,
        "cpu_limit": 1,
        "credits_per_hour": "1.5",
        "disk_limit": "54272MiB",
        "memory_limit": "7762MiB",
        "scale": 1,
        "workers": 1,
    },
    "M.1-xsmall": {
        "cpu_exclusive": True,
        "cpu_limit": 2,
        "credits_per_hour": "3",
        "disk_limit": "108544MiB",
        "memory_limit": "15525MiB",
        "scale": 1,
        "workers": 2,
    },
    "M.1-small": {
        "cpu_exclusive": True,
        "cpu_limit": 4,
        "credits_per_hour": "6",
        "disk_limit": "217088MiB",
        "memory_limit": "31050MiB",
        "scale": 1,
        "workers": 4,
    },
    "M.1-medium": {
        "cpu_exclusive": True,
        "cpu_limit": 6,
        "credits_per_hour": "9",
        "disk_limit": "325632MiB",
        "memory_limit": "46575MiB",
        "scale": 1,
        "workers": 6,
    },
    "M.1-large": {
        "cpu_exclusive": True,
        "cpu_limit": 8,
        "credits_per_hour": "12",
        "disk_limit": "434176MiB",
        "memory_limit": "62100MiB",
        "scale": 1,
        "workers": 8,
    },
    "M.1-1.5xlarge": {
        "cpu_exclusive": True,
        "cpu_limit": 12,
        "credits_per_hour": "18",
        "disk_limit": "651264MiB",
        "memory_limit": "93150MiB",
        "scale": 1,
        "workers": 12,
    },
    "M.1-2xlarge": {
        "cpu_exclusive": True,
        "cpu_limit": 16,
        "credits_per_hour": "24",
        "disk_limit": "869376MiB",
        "memory_limit": "124201MiB",
        "scale": 1,
        "workers": 16,
    },
    "M.1-3xlarge": {
        "cpu_exclusive": True,
        "cpu_limit": 24,
        "credits_per_hour": "36",
        "disk_limit": "1303552MiB",
        "memory_limit": "186301MiB",
        "scale": 1,
        "workers": 24,
    },
    "M.1-4xlarge": {
        "cpu_exclusive": True,
        "cpu_limit": 31,
        "credits_per_hour": "48",
        "disk_limit": "1684480MiB",
        "memory_limit": "240640MiB",
        "scale": 1,
        "workers": 31,
    },
    "M.1-8xlarge": {
        "cpu_exclusive": True,
        "cpu_limit": 62,
        "credits_per_hour": "96",
        "disk_limit": "3368960MiB",
        "memory_limit": "481280MiB",
        "scale": 1,
        "workers": 62,
    },
    # Fake, just to be able to create clusters
    "M.1-16xlarge": {
        "cpu_exclusive": True,
        "cpu_limit": 62,
        "credits_per_hour": "192",
        "disk_limit": "6737920MiB",
        "memory_limit": "481280MiB",
        "scale": 1,
        "workers": 62,
    },
    "M.1-32xlarge": {
        "cpu_exclusive": True,
        "cpu_limit": 62,
        "credits_per_hour": "384",
        "disk_limit": "13475840MiB",
        "memory_limit": "481280MiB",
        "scale": 1,
        "workers": 62,
    },
    "M.1-64xlarge": {
        "cpu_exclusive": True,
        "cpu_limit": 62,
        "credits_per_hour": "768",
        "disk_limit": "26951680MiB",
        "memory_limit": "481280MiB",
        "scale": 1,
        "workers": 62,
    },
    "M.1-128xlarge": {
        "cpu_exclusive": True,
        "cpu_limit": 62,
        "credits_per_hour": "1536",
        "disk_limit": "53719040",
        "memory_limit": "481280MiB",
        "scale": 1,
        "workers": 62,
    },
    # cc cluster replica sizes
    "25cc": {
        "cpu_exclusive": False,
        "cpu_limit": 0.5,
        "credits_per_hour": "0.25",
        "disk_limit": "7762MiB",
        "memory_limit": "3881MiB",
        "scale": 1,
        "workers": 1,
    },
    "50cc": {
        "cpu_exclusive": True,
        "cpu_limit": 1,
        "credits_per_hour": "0.5",
        "disk_limit": "15525MiB",
        "memory_limit": "7762MiB",
        "scale": 1,
        "workers": 1,
    },
    "100cc": {
        "cpu_exclusive": True,
        "cpu_limit": 2,
        "credits_per_hour": "1",
        "disk_limit": "31050MiB",
        "memory_limit": "15525MiB",
        "scale": 1,
        "workers": 2,
    },
    "200cc": {
        "cpu_exclusive": True,
        "cpu_limit": 4,
        "credits_per_hour": "2",
        "disk_limit": "62100MiB",
        "memory_limit": "31050MiB",
        "scale": 1,
        "workers": 4,
    },
    "300cc": {
        "cpu_exclusive": True,
        "cpu_limit": 6,
        "credits_per_hour": "3",
        "disk_limit": "93150MiB",
        "memory_limit": "46575MiB",
        "scale": 1,
        "workers": 6,
    },
    "400cc": {
        "cpu_exclusive": True,
        "cpu_limit": 8,
        "credits_per_hour": "4",
        "disk_limit": "124201MiB",
        "memory_limit": "62100MiB",
        "scale": 1,
        "workers": 8,
    },
    "600cc": {
        "cpu_exclusive": True,
        "cpu_limit": 12,
        "credits_per_hour": "6",
        "disk_limit": "186301MiB",
        "memory_limit": "93150MiB",
        "scale": 1,
        "workers": 12,
    },
    "800cc": {
        "cpu_exclusive": True,
        "cpu_limit": 16,
        "credits_per_hour": "8",
        "disk_limit": "248402MiB",
        "memory_limit": "124201MiB",
        "scale": 1,
        "workers": 16,
    },
    "1200cc": {
        "cpu_exclusive": True,
        "cpu_limit": 24,
        "credits_per_hour": "12",
        "disk_limit": "372603MiB",
        "memory_limit": "186301MiB",
        "scale": 1,
        "workers": 24,
    },
    "1600cc": {
        "cpu_exclusive": True,
        "cpu_limit": 31,
        "credits_per_hour": "16",
        "disk_limit": "481280MiB",
        "memory_limit": "240640MiB",
        "scale": 1,
        "workers": 31,
    },
    "3200cc": {
        "cpu_exclusive": True,
        "cpu_limit": 62,
        "credits_per_hour": "32",
        "disk_limit": "962560MiB",
        "memory_limit": "481280MiB",
        "scale": 1,
        "workers": 62,
    },
    # Fake, just to be able to create clusters
    "6400cc": {
        "cpu_exclusive": True,
        "cpu_limit": 62,
        "credits_per_hour": "32",
        "disk_limit": "962560MiB",
        "memory_limit": "481280MiB",
        "scale": 1,
        "workers": 62,
    },
    "128C": {
        "cpu_exclusive": True,
        "cpu_limit": 62,
        "credits_per_hour": "32",
        "disk_limit": "962560MiB",
        "memory_limit": "481280MiB",
        "scale": 1,
        "workers": 62,
    },
    "256C": {
        "cpu_exclusive": True,
        "cpu_limit": 62,
        "credits_per_hour": "32",
        "disk_limit": "962560MiB",
        "memory_limit": "481280MiB",
        "scale": 1,
        "workers": 62,
    },
    "512C": {
        "cpu_exclusive": True,
        "cpu_limit": 62,
        "credits_per_hour": "32",
        "disk_limit": "962560MiB",
        "memory_limit": "481280MiB",
        "scale": 1,
        "workers": 62,
    },
    # Internal test sizes
    "scale=1,workers=1": {
        "cpu_exclusive": True,
        "cpu_limit": 1,
        "credits_per_hour": "0.5",
        "disk_limit": "15525MiB",
        "memory_limit": "7762MiB",
        "scale": 1,
        "workers": 1,
    },
    "scale=1,workers=2": {
        "cpu_exclusive": True,
        "cpu_limit": 2,
        "credits_per_hour": "1",
        "disk_limit": "31050MiB",
        "memory_limit": "15525MiB",
        "scale": 1,
        "workers": 2,
    },
    "scale=1,workers=4": {
        "cpu_exclusive": True,
        "cpu_limit": 4,
        "credits_per_hour": "2",
        "disk_limit": "62100MiB",
        "memory_limit": "31050MiB",
        "scale": 1,
        "workers": 4,
    },
    "scale=1,workers=6": {
        "cpu_exclusive": True,
        "cpu_limit": 6,
        "credits_per_hour": "3",
        "disk_limit": "93150MiB",
        "memory_limit": "46575MiB",
        "scale": 1,
        "workers": 6,
    },
    "scale=1,workers=8": {
        "cpu_exclusive": True,
        "cpu_limit": 8,
        "credits_per_hour": "4",
        "disk_limit": "124201MiB",
        "memory_limit": "62100MiB",
        "scale": 1,
        "workers": 8,
    },
    "scale=1,workers=12": {
        "cpu_exclusive": True,
        "cpu_limit": 12,
        "credits_per_hour": "6",
        "disk_limit": "186301MiB",
        "memory_limit": "93150MiB",
        "scale": 1,
        "workers": 12,
    },
    "scale=1,workers=16": {
        "cpu_exclusive": True,
        "cpu_limit": 16,
        "credits_per_hour": "8",
        "disk_limit": "248402MiB",
        "memory_limit": "124201MiB",
        "scale": 1,
        "workers": 16,
    },
    "scale=1,workers=24": {
        "cpu_exclusive": True,
        "cpu_limit": 24,
        "credits_per_hour": "12",
        "disk_limit": "372603MiB",
        "memory_limit": "186301MiB",
        "scale": 1,
        "workers": 24,
    },
    "scale=1,workers=31": {
        "cpu_exclusive": True,
        "cpu_limit": 31,
        "credits_per_hour": "16",
        "disk_limit": "481280MiB",
        "memory_limit": "240640MiB",
        "scale": 1,
        "workers": 31,
    },
    "scale=1,workers=62": {
        "cpu_exclusive": True,
        "cpu_limit": 62,
        "credits_per_hour": "32",
        "disk_limit": "962560MiB",
        "memory_limit": "481280MiB",
        "scale": 1,
        "workers": 62,
    },
    "scale=2,workers=1": {
        "cpu_exclusive": True,
        "cpu_limit": 1,
        "credits_per_hour": "0.5",
        "disk_limit": "15525MiB",
        "memory_limit": "7762MiB",
        "scale": 2,
        "workers": 1,
    },
    "scale=4,workers=1": {
        "cpu_exclusive": True,
        "cpu_limit": 1,
        "credits_per_hour": "0.5",
        "disk_limit": "15525MiB",
        "memory_limit": "7762MiB",
        "scale": 4,
        "workers": 1,
    },
}

SERVICES = [
    SshBastionHost(allow_any_key=True),
    Zookeeper(),
    Kafka(
        auto_create_topics=False,
        ports=["30123:30123"],
        allow_host_ports=True,
        environment_extra=[
            "KAFKA_ADVERTISED_LISTENERS=HOST://127.0.0.1:30123,PLAINTEXT://kafka:9092",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=HOST:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        ],
    ),
    SchemaRegistry(),
    Redpanda(),
    Postgres(),
    MySql(),
    Azurite(),
    Mz(app_password=""),
    Materialized(
        cluster_replica_size=cluster_replica_sizes,
        ports=[6875, 6874, 6876, 6877, 6878, 6880, 6881, 26257],
        environment_extra=["MZ_NO_BUILTIN_CONSOLE=0"],
        additional_system_parameter_defaults={"enable_rbac_checks": "false"},
    ),
    Testdrive(
        seed=1,
        no_reset=True,
        entrypoint_extra=[
            f"--var=default-storage-size={Materialized.Size.DEFAULT_SIZE}-1",
            f"--var=mysql-root-password={MySql.DEFAULT_ROOT_PASSWORD}",
            f"--var=default-sql-server-user={SqlServer.DEFAULT_USER}",
            f"--var=default-sql-server-password={SqlServer.DEFAULT_SA_PASSWORD}",
        ],
    ),
    SqlServer(),
]


def p(label, value):
    if isinstance(value, str):
        s = value
    elif isinstance(value, float):
        s = f"{value:,.2f}"
    else:
        s = f"{value:,}"
    print(f"  {label:<{17}} {s:>{12}}")


def print_workload_stats(file: pathlib.Path, workload: dict[str, Any]) -> None:
    filename = posixpath.relpath(file, LOCATION)
    print(filename)
    p("size", f"{os.path.getsize(file) / (1024 * 1024):.1f} MiB")

    p("clusters", len(workload["clusters"]))
    p("databases", len(workload["databases"]))
    p(
        "schemas",
        sum(len(schemas) for schemas in workload["databases"].values()),
    )
    p(
        "data types",
        sum(
            len(items["types"])
            for schemas in workload["databases"].values()
            for items in schemas.values()
        ),
    )
    p(
        "tables",
        sum(
            len(items["tables"])
            for schemas in workload["databases"].values()
            for items in schemas.values()
        ),
    )
    p(
        "connections",
        sum(
            len(items["connections"])
            for schemas in workload["databases"].values()
            for items in schemas.values()
        ),
    )
    p(
        "sources",
        sum(
            len(items["sources"])
            for schemas in workload["databases"].values()
            for items in schemas.values()
        ),
    )
    source_types = Counter(
        source["type"]
        for schemas in workload["databases"].values()
        for items in schemas.values()
        for source in items["sources"].values()
    )
    for t, n in sorted(source_types.items()):
        p(f"  {t}", n)
    p(
        "subsources",
        sum(
            len(source.get("children", {}))
            for schemas in workload["databases"].values()
            for items in schemas.values()
            for source in items["sources"].values()
        ),
    )
    p(
        "views",
        sum(
            len(items["views"])
            for schemas in workload["databases"].values()
            for items in schemas.values()
        ),
    )
    p(
        "mat. views",
        sum(
            len(items["materialized_views"])
            for schemas in workload["databases"].values()
            for items in schemas.values()
        ),
    )
    p(
        "indexes",
        sum(
            len(items["indexes"])
            for schemas in workload["databases"].values()
            for items in schemas.values()
        ),
    )
    p(
        "sinks",
        sum(
            len(items["sinks"])
            for schemas in workload["databases"].values()
            for items in schemas.values()
        ),
    )
    sink_types = Counter(
        sink["type"]
        for schemas in workload["databases"].values()
        for items in schemas.values()
        for sink in items["sinks"].values()
    )
    for t, n in sorted(sink_types.items()):
        p(f"  {t}", n)

    rows = (
        sum(
            table["rows"]
            for schemas in workload["databases"].values()
            for items in schemas.values()
            for table in items["tables"].values()
        )
        + sum(
            source["messages_total"]
            for schemas in workload["databases"].values()
            for items in schemas.values()
            for source in items["sources"].values()
        )
        + sum(
            child["messages_total"]
            for schemas in workload["databases"].values()
            for items in schemas.values()
            for source in items["sources"].values()
            for child in source.get("children", {}).values()
        )
    )
    p("rows", rows)

    rows_s = sum(
        source.get("messages_second", 0.0)
        for schemas in workload["databases"].values()
        for items in schemas.values()
        for source in items["sources"].values()
    ) + sum(
        child.get("messages_second", 0.0)
        for schemas in workload["databases"].values()
        for items in schemas.values()
        for source in items["sources"].values()
        for child in source.get("children", {}).values()
    )
    p("  /s", rows_s)

    p("queries", len(workload["queries"]))
    if workload["queries"]:
        dur_s = (
            workload["queries"][-1]["began_at"] - workload["queries"][0]["began_at"]
        ).total_seconds()
        p("  span", f"{int(round(dur_s / 60))}min")
        p("  last", workload["queries"][-1]["began_at"].date().isoformat())


def resolve_tag(tag: str) -> str | None:
    if tag == "common-ancestor":
        # TODO: We probably will need overrides too
        return resolve_ancestor_image_tag({})
    return tag


def update_captured_workloads_repo() -> None:
    path = pathlib.Path(MZ_ROOT / "test" / "workload-replay" / "captured-workloads")
    if (path / ".git").is_dir():
        if ui.env_is_truthy("CI"):
            spawn.runv(["git", "-C", str(path), "pull"])
        else:
            spawn.runv(["git", "-C", str(path), "fetch"])
            local = spawn.capture(["git", "-C", str(path), "rev-parse", "@"])
            remote = spawn.capture(["git", "-C", str(path), "rev-parse", "@{upstream}"])
            if local != remote:
                spawn.runv(["git", "-C", str(path), "pull"])
    else:
        path.mkdir(exist_ok=True)
        if ui.env_is_truthy("CI"):
            github_token = (
                os.getenv("GITHUB_TOKEN")
                or os.environ["GITHUB_CI_ISSUE_REFERENCE_CHECKER_TOKEN"]
            )
            subprocess.run(
                [
                    "git",
                    "clone",
                    f"https://materializebot:{urllib.parse.quote(github_token, safe='')}@github.com/MaterializeInc/captured-workloads",
                    str(path),
                ]
            )
        else:
            try:
                spawn.runv(
                    [
                        "git",
                        "clone",
                        "https://github.com/MaterializeInc/captured-workloads",
                        str(path),
                    ]
                )
            except:
                spawn.runv(
                    [
                        "git",
                        "clone",
                        "git@github.com:MaterializeInc/captured-workloads",
                        str(path),
                    ]
                )


def get_kafka_topic(source: dict[str, Any]) -> str:
    match = re.search(
        r"TOPIC\s*=\s*'([^']+)'",
        source["create_sql"],
        re.IGNORECASE | re.DOTALL,
    )
    assert match, f"No topic found: {source['create_sql']}"
    return match.group(1)


def delivery_report(err: str, msg: Any) -> None:
    assert err is None, f"Delivery failed for user record {msg.key()}: {err}"


def get_postgres_reference_db_schema_table(
    child: dict[str, Any]
) -> tuple[str, str, str]:
    if child["type"] == "table":
        match = re.search(
            r"REFERENCE\s*=\s*([a-zA-Z0-9_.]+)",
            child["create_sql"],
        )
        assert match, f"Couldn't find REFERENCE in {child['create_sql']}"
    elif child["type"] == "subsource":
        match = re.search(
            r"EXTERNAL\s+REFERENCE\s*=\s*([a-zA-Z0-9_.]+)",
            child["create_sql"],
        )
        assert match, f"Couldn't find EXTERNAL REFERENCE in {child['create_sql']}"
    else:
        raise ValueError(f"Unhandled child type {child['type']}")
    db, schema, table = match.group(1).split(".", 2)
    return (db, schema, table)


def get_mysql_reference_db_table(child: dict[str, Any]) -> tuple[str, str]:
    if child["type"] == "table":
        match = re.search(
            r"REFERENCE\s*=\s*([a-zA-Z0-9_.]+)",
            child["create_sql"],
        )
        assert match, f"Couldn't find REFERENCE in {child['create_sql']}"
    elif child["type"] == "subsource":
        match = re.search(
            r"EXTERNAL\s+REFERENCE\s*=\s*([a-zA-Z0-9_.]+)",
            child["create_sql"],
        )
        assert match, f"Couldn't find EXTERNAL REFERENCE in {child['create_sql']}"
    else:
        raise ValueError(f"Unhandled child type {child['type']}")
    db, table = match.group(1).split(".", 1)
    return db, table


def get_sql_server_reference_db_schema_table(
    child: dict[str, Any]
) -> tuple[str, str, str]:
    if child["type"] == "table":
        match = re.search(
            r"REFERENCE\s*=\s*([a-zA-Z0-9_.]+)",
            child["create_sql"],
        )
        assert match, f"Couldn't find REFERENCE in {child['create_sql']}"
    elif child["type"] == "subsource":
        match = re.search(
            r"EXTERNAL\s+REFERENCE\s*=\s*([a-zA-Z0-9_.]+)",
            child["create_sql"],
        )
        assert match, f"Couldn't find EXTERNAL REFERENCE in {child['create_sql']}"
    else:
        raise ValueError(f"Unhandled child type {child['type']}")
    db, schema, table = match.group(1).split(".", 2)
    return (db, schema, table)


def to_sql_server_data_type(typ: str) -> str:
    if typ == "timestamp without time zone":
        return "datetime"
    return typ


def long_tail_rank(n: int, a: float, rng: random.Random) -> int:
    x = int((rng.paretovariate(a) - 1.0) * 3) + 1
    return max(1, min(n, x))


def long_tail_int(lo: int, hi: int, rng: random.Random) -> int:
    if lo > hi:
        lo, hi = hi, lo

    r = long_tail_rank(n=100_000, a=1.25, rng=rng)
    mag = int(math.log2(r + 1) ** 4)

    if rng.random() < 0.80:
        hot = [0, 1, -1, 2, -2, 10, -10, 100, -100]
        val = rng.choice(hot)
    else:
        val = mag
        if lo < 0 and hi > 0:
            val *= rng.choice([-1, 1])

    return max(lo, min(hi, val))


def long_tail_float(lo: float, hi: float, rng: random.Random) -> float:
    if lo > hi:
        lo, hi = hi, lo

    if rng.random() < 0.85:
        base = rng.gauss(0.0, 1.0)
    else:
        r = long_tail_rank(n=1_000_000, a=1.15, rng=rng)
        base = (math.log(r + 1) ** 3) * rng.choice([-1.0, 1.0])

    span = hi - lo
    x = base / (abs(base) + 10.0)
    val = (x + 1.0) / 2.0 * span + lo
    return max(lo, min(hi, val))


def long_tail_choice(values: list[Any], hot_prob: float, rng: random.Random) -> Any:
    if not values:
        raise ValueError("empty values")
    if rng.random() < hot_prob:
        return rng.choice(values[: min(8, len(values))])
    return rng.choice(values)


def long_tail_text(
    chars: str, max_len: int, hot_pool: list[str], rng: random.Random
) -> str:
    if rng.random() < 0.90 and hot_pool:
        return rng.choice(hot_pool)

    length = min(max_len, max(1, long_tail_rank(n=max_len, a=1.3, rng=rng)))
    return "".join(rng.choice(chars) for _ in range(length))


class Column:
    def __init__(
        self, name: str, typ: str, nullable: bool, default: Any, data_shape: str | None
    ):
        self.name = name
        self.typ = typ
        self.nullable = nullable
        self.default = default
        self.chars = string.ascii_letters + string.digits
        self.data_shape = data_shape
        if data_shape:
            assert typ in ("text", "bytea"), f"Can't create text shape for type {typ}"

        self._hot_strings = [
            f"{name}_a",
            f"{name}_b",
            f"{name}_c",
            "foo",
            "bar",
            "baz",
            "0",
            "1",
            "NULL",
        ]

    def avro_type(self) -> str | list[str]:
        result = self.typ
        if self.typ in ("text", "bytea", "character", "character varying"):
            result = "string"
        elif self.typ in ("smallint", "integer", "uint2", "uint4"):
            result = "int"
        elif self.typ in ("bigint", "uint8"):
            result = "long"
        elif self.typ in ("double precision", "numeric"):
            result = "double"
        elif self.typ in ("timestamp with time zone", "timestamp without time zone"):
            result = "long"
        return ["null", result] if self.nullable else result

    def kafka_value(self, rng: random.Random) -> Any:
        if self.default and rng.randrange(10) == 0 and self.default != "NULL":
            return str(self.default)
        if self.nullable and rng.randrange(10) == 0:
            return None

        if self.typ == "boolean":
            return rng.random() < 0.2

        elif self.typ == "smallint":
            return long_tail_int(-32768, 32767, rng=rng)
        elif self.typ == "integer":
            return long_tail_int(-2147483648, 2147483647, rng=rng)
        elif self.typ == "bigint":
            return long_tail_int(-9223372036854775808, 9223372036854775807, rng=rng)

        elif self.typ == "uint2":
            return long_tail_int(0, 65535, rng=rng)
        elif self.typ == "uint4":
            return long_tail_int(0, 4294967295, rng=rng)
        elif self.typ == "uint8":
            return long_tail_int(0, 18446744073709551615, rng=rng)

        elif self.typ in ("float", "double precision", "numeric"):
            return long_tail_float(-1_000_000_000.0, 1_000_000_000.0, rng=rng)

        elif self.typ in ("text", "bytea"):
            if self.data_shape == "datetime":
                year = long_tail_choice(
                    [2023, 2024, 2025, 2022, 2021, 2020, 2019], hot_prob=0.9, rng=rng
                )
                return literal(
                    f"{year}-{rng.randrange(1, 13):02}-{rng.randrange(1, 29):02}T{rng.randrange(0, 23):02}:{rng.randrange(0, 59):02}:{rng.randrange(0, 59):02}Z"
                )
            elif self.data_shape:
                raise ValueError(f"Unhandled text shape {self.data_shape}")
            return literal(long_tail_text(self.chars, 100, self._hot_strings, rng=rng))

        elif self.typ in ("character", "character varying"):
            return literal(long_tail_text(self.chars, 10, self._hot_strings, rng=rng))

        elif self.typ == "uuid":
            return str(uuid.UUID(int=rng.getrandbits(128), version=4))

        elif self.typ == "jsonb":
            result = {
                f"key{key}": str(long_tail_int(-100, 100, rng=rng)) for key in range(20)
            }
            return json.dumps(result)

        elif self.typ in ("timestamp with time zone", "timestamp without time zone"):
            now = 1700000000000  # doesn't need to be exact
            if rng.random() < 0.9:
                return now + long_tail_int(-86_400_000, 86_400_000, rng=rng)
            else:
                return rng.randrange(0, 9223372036854775807)

        elif self.typ == "mz_timestamp":
            year = long_tail_choice(
                [2023, 2024, 2025, 2022, 2021, 2020, 2019], hot_prob=0.9, rng=rng
            )
            return literal(f"{year}-{rng.randrange(1, 13)}-{rng.randrange(1, 29)}")

        elif self.typ == "date":
            year = long_tail_choice(
                [2023, 2024, 2025, 2022, 2021, 2020, 2019], hot_prob=0.9, rng=rng
            )
            return literal(f"{year}-{rng.randrange(1, 13)}-{rng.randrange(1, 29)}")

        elif self.typ == "time":
            if rng.random() < 0.8:
                common = ["00:00:00.000000", "12:00:00.000000", "23:59:59.000000"]
                return literal(rng.choice(common))
            return literal(
                f"{rng.randrange(0, 24)}:{rng.randrange(0, 60)}:{rng.randrange(0, 60)}.{rng.randrange(0, 1000000)}"
            )

        elif self.typ == "int2range":
            a = str(long_tail_int(-32768, 32767, rng=rng))
            b = str(long_tail_int(-32768, 32767, rng=rng))
            return literal(f"[{a},{b})")

        elif self.typ == "int4range":
            a = str(long_tail_int(-2147483648, 2147483647, rng=rng))
            b = str(long_tail_int(-2147483648, 2147483647, rng=rng))
            return literal(f"[{a},{b})")

        elif self.typ == "int8range":
            a = str(long_tail_int(-9223372036854775808, 9223372036854775807, rng=rng))
            b = str(long_tail_int(-9223372036854775808, 9223372036854775807, rng=rng))
            return literal(f"[{a},{b})")

        elif self.typ == "map":
            return {
                str(i): str(long_tail_int(-100, 100, rng=rng)) for i in range(0, 20)
            }

        elif self.typ == "text[]":
            values = [
                literal(long_tail_text(self.chars, 100, self._hot_strings, rng=rng))
                for _ in range(5)
            ]
            return literal(f"{{{', '.join(values)}}}")

        else:
            raise ValueError(f"Unhandled data type {self.typ}")

    def value(self, rng: random.Random, in_query: bool = True) -> Any:
        if self.default and rng.randrange(10) == 0 and self.default != "NULL":
            return str(self.default) if in_query else self.default

        if self.nullable and rng.randrange(10) == 0:
            return "NULL" if in_query else None

        if self.typ == "boolean":
            val = rng.random() < 0.2
            return ("true" if val else "false") if in_query else val

        elif self.typ == "smallint":
            val = long_tail_int(-32768, 32767, rng=rng)
            return str(val) if in_query else val

        elif self.typ == "integer":
            val = long_tail_int(-2147483648, 2147483647, rng=rng)
            return str(val) if in_query else val

        elif self.typ == "bigint":
            val = long_tail_int(-9223372036854775808, 9223372036854775807, rng=rng)
            return str(val) if in_query else val

        elif self.typ == "uint2":
            val = long_tail_int(0, 65535, rng=rng)
            return str(val) if in_query else val

        elif self.typ == "uint4":
            val = long_tail_int(0, 4294967295, rng=rng)
            return str(val) if in_query else val

        elif self.typ == "uint8":
            val = long_tail_int(0, 18446744073709551615, rng=rng)
            return str(val) if in_query else val

        elif self.typ in ("float", "double precision", "numeric"):
            val = long_tail_float(-1_000_000_000.0, 1_000_000_000.0, rng=rng)
            return str(val) if in_query else val

        elif self.typ in ("text", "bytea"):
            if self.data_shape == "datetime":
                year = long_tail_choice(
                    [2023, 2024, 2025, 2022, 2021, 2020, 2019], hot_prob=0.9, rng=rng
                )
                s = (
                    f"{year}-{rng.randrange(1, 13):02}-{rng.randrange(1, 29):02}"
                    f"T{rng.randrange(0, 23):02}:{rng.randrange(0, 59):02}:{rng.randrange(0, 59):02}Z"
                )
                return literal(s) if in_query else s

            elif self.data_shape:
                raise ValueError(f"Unhandled text shape {self.data_shape}")

            s = long_tail_text(self.chars, 100, self._hot_strings, rng=rng)
            return literal(s) if in_query else s

        elif self.typ in ("character", "character varying"):
            s = long_tail_text(self.chars, 10, self._hot_strings, rng=rng)
            return literal(s) if in_query else s

        elif self.typ == "uuid":
            u = uuid.UUID(int=rng.getrandbits(128), version=4)
            return str(u) if in_query else u

        elif self.typ == "jsonb":
            obj = {
                f"key{key}": str(long_tail_int(-100, 100, rng=rng)) for key in range(20)
            }
            if in_query:
                return f"'{json.dumps(obj)}'::jsonb"
            else:
                return json.dumps(obj)

        elif self.typ in ("timestamp with time zone", "timestamp without time zone"):
            year = long_tail_choice(
                [2023, 2024, 2025, 2022, 2021, 2020, 2019], hot_prob=0.9, rng=rng
            )
            s = f"{year}-{rng.randrange(1, 13)}-{rng.randrange(1, 29)}"
            return literal(s) if in_query else s

        elif self.typ == "mz_timestamp":
            year = long_tail_choice(
                [2023, 2024, 2025, 2022, 2021, 2020, 2019], hot_prob=0.9, rng=rng
            )
            s = f"{year}-{rng.randrange(1, 13)}-{rng.randrange(1, 29)}"
            return literal(s) if in_query else s

        elif self.typ == "date":
            year = long_tail_choice(
                [2023, 2024, 2025, 2022, 2021, 2020, 2019], hot_prob=0.9, rng=rng
            )
            s = f"{year}-{rng.randrange(1, 13)}-{rng.randrange(1, 29)}"
            return literal(s) if in_query else s

        elif self.typ == "time":
            if rng.random() < 0.8:
                s = rng.choice(
                    ["00:00:00.000000", "12:00:00.000000", "23:59:59.000000"]
                )
                return literal(s) if in_query else s

            s = (
                f"{rng.randrange(0, 24)}:{rng.randrange(0, 60)}:{rng.randrange(0, 60)}"
                f".{rng.randrange(0, 1000000)}"
            )
            return literal(s) if in_query else s

        elif self.typ == "int2range":
            a = str(long_tail_int(-32768, 32767, rng=rng))
            b = str(long_tail_int(-32768, 32767, rng=rng))
            s = f"[{a},{b})"
            return literal(s) if in_query else s

        elif self.typ == "int4range":
            a = str(long_tail_int(-2147483648, 2147483647, rng=rng))
            b = str(long_tail_int(-2147483648, 2147483647, rng=rng))
            s = f"[{a},{b})"
            return literal(s) if in_query else s

        elif self.typ == "int8range":
            a = str(long_tail_int(-9223372036854775808, 9223372036854775807, rng=rng))
            b = str(long_tail_int(-9223372036854775808, 9223372036854775807, rng=rng))
            s = f"[{a},{b})"
            return literal(s) if in_query else s

        elif self.typ == "map":
            if in_query:
                values = [
                    f"'{i}' => {str(long_tail_int(-100, 100, rng=rng))}"
                    for i in range(0, 20)
                ]
                return literal(f"{{{', '.join(values)}}}")
            else:
                # COPY text input for map expects the literal form too
                values = [
                    f'"{i}"=>"{str(long_tail_int(-100, 100, rng=rng))}"'
                    for i in range(0, 20)
                ]
                return "{" + ",".join(values) + "}"

        elif self.typ == "text[]":
            if in_query:
                values = [
                    literal(long_tail_text(self.chars, 100, self._hot_strings, rng=rng))
                    for _ in range(5)
                ]
                return literal(f"{{{', '.join(values)}}}")
            else:
                return [
                    long_tail_text(self.chars, 100, self._hot_strings, rng=rng)
                    for _ in range(5)
                ]

        else:
            raise ValueError(f"Unhandled data type {self.typ}")


async def ingest_webhook(
    c: Composition,
    db: str,
    schema: str,
    name: str,
    source: dict[str, Any],
    num_rows: int,
    print_progress: bool = False,
) -> None:
    url = (
        f"http://127.0.0.1:{c.port('materialized', 6876)}/api/webhook/"
        f"{urllib.parse.quote(db, safe='')}/"
        f"{urllib.parse.quote(schema, safe='')}/"
        f"{urllib.parse.quote(name, safe='')}"
    )

    body_column = None
    headers_column = None
    for column in source["columns"]:
        if column["name"] == "body":
            body_column = Column(
                column["name"],
                column["type"],
                column["nullable"],
                column["default"],
                column.get("data_shape"),
            )
        elif column["name"] == "headers":
            headers_column = Column(
                column["name"],
                column["type"],
                column["nullable"],
                column["default"],
                column.get("data_shape"),
            )
    assert body_column

    connector = aiohttp.TCPConnector(limit=5000)
    timeout = aiohttp.ClientTimeout(total=None)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        sem = asyncio.Semaphore(5000)

        progress = 0
        progress_lock = asyncio.Lock()

        async def report_progress() -> None:
            nonlocal progress
            async with progress_lock:
                progress += 1
                if progress % 10000 == 0 or progress == num_rows:
                    print(
                        f"{progress}/{num_rows} ({progress / num_rows:.1%})",
                        end="\r",
                        flush=True,
                    )

        async def send_one(seed: int):
            rng = random.Random(seed)
            backoff = 0.01

            while True:
                async with sem:
                    async with session.post(
                        url,
                        data=body_column.kafka_value(rng),
                        headers=(
                            headers_column.kafka_value(rng) if headers_column else None
                        ),
                    ) as resp:
                        if resp.status == 200:
                            if print_progress:
                                await report_progress()
                            return
                        elif resp.status == 429:
                            await asyncio.sleep(backoff)
                            backoff = min(backoff * 2, 1.0)
                        else:
                            text = await resp.text()
                            raise RuntimeError(
                                f"Webhook ingestion failed: {resp.status}: {text}"
                            )

        await asyncio.gather(
            *(send_one(random.randrange(SEED_RANGE)) for _ in range(num_rows))
        )
        print()


@cache
def get_kafka_objects(
    topic: str,
    columns: tuple,
    debezium: bool,
    schema_registry_port: int,
    kafka_port: int,
):
    """
    Build and cache Kafka producer + Avro serializers for a topic/schema combo.
    `columns` MUST be a tuple so this function can be cached.
    """

    registry = SchemaRegistryClient({"url": f"http://127.0.0.1:{schema_registry_port}"})

    producer = confluent_kafka.Producer(
        {
            "bootstrap.servers": f"127.0.0.1:{kafka_port}",
            "linger.ms": 20,
            "batch.num.messages": 10000,
            "queue.buffering.max.kbytes": 1048576,
            "compression.type": "lz4",
            "acks": "1",
            "retries": 3,
        }
    )

    col_names = [c.name for c in columns]

    if debezium:
        value_record_schema = {
            "type": "record",
            "name": "Value",
            "fields": [
                {
                    "name": c.name,
                    "type": c.avro_type(),
                    **({"default": c.default} if c.default is not None else {}),
                }
                for c in columns
            ],
            "connect.name": f"{topic}.Value",
        }

        envelope_schema = {
            "type": "record",
            "name": "Envelope",
            "namespace": topic,
            "fields": [
                {
                    "name": "before",
                    "type": ["null", value_record_schema],
                    "default": None,
                },
                {"name": "after", "type": ["null", "Value"], "default": None},
                {
                    "name": "source",
                    "type": {
                        "type": "record",
                        "name": "Source",
                        "namespace": "io.debezium.connector.mysql",
                        "fields": [
                            {"name": "version", "type": "string"},
                            {"name": "connector", "type": "string"},
                            {"name": "name", "type": "string"},
                            {"name": "ts_ms", "type": "long"},
                            {
                                "name": "snapshot",
                                "type": ["null", "string"],
                                "default": None,
                            },
                            {"name": "db", "type": "string"},
                            {
                                "name": "sequence",
                                "type": ["null", "string"],
                                "default": None,
                            },
                            {
                                "name": "table",
                                "type": ["null", "string"],
                                "default": None,
                            },
                            {"name": "server_id", "type": "long"},
                            {
                                "name": "gtid",
                                "type": ["null", "string"],
                                "default": None,
                            },
                            {"name": "file", "type": "string"},
                            {"name": "pos", "type": "long"},
                            {"name": "row", "type": "int"},
                            {
                                "name": "thread",
                                "type": ["null", "long"],
                                "default": None,
                            },
                            {
                                "name": "query",
                                "type": ["null", "string"],
                                "default": None,
                            },
                        ],
                        "connect.name": "io.debezium.connector.mysql.Source",
                    },
                },
                {"name": "op", "type": "string"},
                {"name": "ts_ms", "type": ["null", "long"], "default": None},
                {"name": "transaction", "type": ["null", "string"], "default": None},
            ],
            "connect.name": f"{topic}.Envelope",
        }

        key_schema = {
            "type": "record",
            "name": "Key",
            "namespace": topic,
            "fields": [
                {
                    "name": c.name,
                    "type": c.avro_type(),
                    **({"default": c.default} if c.default is not None else {}),
                }
                for c in columns
            ],
            "connect.name": f"{topic}.Key",
        }

        value_serializer = AvroSerializer(
            registry,
            json.dumps(envelope_schema),
            lambda d, ctx: d,
        )

        key_serializer = AvroSerializer(
            registry,
            json.dumps(key_schema),
            lambda d, ctx: d,
        )

    else:
        key_col = columns[0]

        value_schema = {
            "type": "record",
            "name": topic,
            "namespace": "com.materialize",
            "fields": [
                {
                    "name": c.name,
                    "type": c.avro_type(),
                    **({"default": c.default} if c.default is not None else {}),
                }
                for c in columns[1:]
            ],
        }

        key_schema = {
            "type": "record",
            "name": f"{topic}_key",
            "namespace": "com.materialize",
            "fields": [
                {
                    "name": key_col.name,
                    "type": key_col.avro_type(),
                    **(
                        {"default": key_col.default}
                        if key_col.default is not None
                        else {}
                    ),
                }
            ],
        }

        value_serializer = AvroSerializer(
            registry,
            json.dumps(value_schema),
            lambda d, ctx: d,
        )

        key_serializer = AvroSerializer(
            registry,
            json.dumps(key_schema),
            lambda d, ctx: d,
        )

    value_ctx = SerializationContext(topic, MessageField.VALUE)
    key_ctx = SerializationContext(topic, MessageField.KEY)

    return producer, value_serializer, key_serializer, value_ctx, key_ctx, col_names


def ingest(
    c: Composition,
    child: dict[str, Any],
    source: dict[str, Any],
    columns: list[Column],
    num_rows: int,
    rng: random.Random,
) -> None:
    if source["type"] == "postgres":
        ref_database, ref_schema, ref_table = get_postgres_reference_db_schema_table(
            child
        )
        conn = psycopg.connect(
            host="127.0.0.1",
            port=c.default_port("postgres"),
            user="postgres",
            password="postgres",
            dbname=ref_database,
        )
        conn.autocommit = True

        col_names = [col.name for col in columns]

        with conn.cursor() as cur:
            copy_stmt = SQL("COPY {}.{} ({}) FROM STDIN").format(
                Identifier(ref_schema),
                Identifier(ref_table),
                SQL(", ").join(map(Identifier, col_names)),
            )

            with cur.copy(copy_stmt) as copy:
                for _ in range(num_rows):
                    row = [col.value(rng, in_query=False) for col in columns]
                    copy.write_row(row)

    elif source["type"] == "mysql":
        ref_database, ref_table = get_mysql_reference_db_table(child)

        conn = pymysql.connect(
            host="127.0.0.1",
            user="root",
            password=MySql.DEFAULT_ROOT_PASSWORD,
            database=ref_database,
            port=c.default_port("mysql"),
            autocommit=False,
        )

        value_funcs = [col.value for col in columns]
        rows_sql = []
        for _ in range(num_rows):
            row = [fn(rng) for fn in value_funcs]
            rows_sql.append("(" + ", ".join(row) + ")")

        stmt = f"INSERT INTO {ref_table} VALUES " + ", ".join(rows_sql)

        with conn.cursor() as cur:
            cur.execute(stmt)
        conn.close()

    elif source["type"] == "kafka":
        batch_values_kafka = []
        for _ in range(num_rows):
            row = [c.kafka_value(rng) for c in columns]
            batch_values_kafka.append(row)

        topic = get_kafka_topic(source)
        debezium = "ENVELOPE DEBEZIUM" in child["create_sql"]

        producer, serializer, key_serializer, sctx, ksctx, col_names = (
            get_kafka_objects(
                topic,
                tuple(columns),
                debezium,
                c.default_port("schema-registry"),
                c.default_port("kafka"),
            )
        )
        now_ms = int(time.time() * 1000)
        if debezium:
            source_struct = {
                "version": "0",
                "connector": "mysql",
                "name": "materialize-generator",
                "ts_ms": now_ms,
                "snapshot": None,
                "db": "db",
                "sequence": None,
                "table": topic.split(".")[-1],
                "server_id": 0,
                "gtid": None,
                "file": "binlog.000001",
                "pos": 0,
                "row": 0,
                "thread": None,
                "query": None,
            }
        producer.poll(0)
        for row in batch_values_kafka:
            while True:
                try:
                    if debezium:
                        after_value = dict(zip(col_names, row))

                        envelope_value = {
                            "before": None,
                            "after": after_value,
                            "source": source_struct,
                            "op": "c",
                            "ts_ms": now_ms,
                            "transaction": None,
                        }

                        key_value = after_value

                        producer.produce(
                            topic=topic,
                            key=key_serializer(key_value, ksctx),
                            value=serializer(envelope_value, sctx),
                            on_delivery=delivery_report,
                        )
                    else:
                        key_dict = {col_names[0]: row[0]}
                        value_dict = dict(zip(col_names[1:], row[1:]))

                        producer.produce(
                            topic=topic,
                            key=key_serializer(key_dict, ksctx),
                            value=serializer(value_dict, sctx),
                            on_delivery=delivery_report,
                        )

                    break

                except BufferError:
                    producer.poll(0.01)

        producer.poll(0)
    elif source["type"] == "sql-server":
        batch_values = []
        for _ in range(num_rows):
            row = [c.value(rng) for c in columns]
            batch_values.append(f"({', '.join(row)})")

        ref_database, ref_schema, ref_table = get_sql_server_reference_db_schema_table(
            child
        )
        c.testdrive(
            dedent(
                f"""
                $ sql-server-connect name=sql-server
                server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

                $ sql-server-execute name=sql-server
                USE {ref_database};
                INSERT INTO {ref_schema}.{ref_table} VALUES {', '.join(batch_values)}
            """
            ),
            quiet=True,
            silent=True,
        )
    elif source["type"] == "load-generator":
        pass
    else:
        raise ValueError(f"Unhandled source type {source['type']}")


PG_PARAM_RE = re.compile(r"\$(\d+)")


def pg_params_to_psycopg(sql: str, params: list[Any]) -> tuple[str, list[Any]]:
    out_params = []

    def replace(m):
        i = int(m.group(1)) - 1
        out_params.append(params[i])
        return "%s"

    return PG_PARAM_RE.sub(replace, sql.replace("%", "%%")), out_params


def run_create_objects_part_1(
    c: Composition, services: set[str], workload: dict[str, Any]
) -> None:
    c.sql("DROP CLUSTER IF EXISTS quickstart CASCADE", user="mz_system", port=6877)
    c.sql("DROP DATABASE IF EXISTS materialize CASCADE", user="mz_system", port=6877)
    c.sql(
        "ALTER SYSTEM SET max_schemas_per_database = 1000000",
        user="mz_system",
        port=6877,
    )
    c.sql("ALTER SYSTEM SET max_tables = 1000000", user="mz_system", port=6877)
    c.sql(
        "ALTER SYSTEM SET max_materialized_views = 1000000", user="mz_system", port=6877
    )
    c.sql("ALTER SYSTEM SET max_sources = 1000000", user="mz_system", port=6877)
    c.sql("ALTER SYSTEM SET max_sinks = 1000000", user="mz_system", port=6877)
    c.sql("ALTER SYSTEM SET max_roles = 1000000", user="mz_system", port=6877)
    c.sql("ALTER SYSTEM SET max_clusters = 1000000", user="mz_system", port=6877)
    c.sql(
        "ALTER SYSTEM SET max_replicas_per_cluster = 1000000",
        user="mz_system",
        port=6877,
    )
    c.sql("ALTER SYSTEM SET max_secrets = 1000000", user="mz_system", port=6877)
    c.sql(
        "ALTER SYSTEM SET webhook_concurrent_request_limit = 1000000",
        user="mz_system",
        port=6877,
    )

    print("Creating clusters")
    for name, cluster in workload["clusters"].items():
        if cluster["managed"]:
            # Need at least one replica for everything to hydrate
            create_sql = cluster["create_sql"].replace(
                "REPLICATION FACTOR = 0", "REPLICATION FACTOR = 1"
            )
            c.sql(create_sql, user="mz_system", port=6877)
        else:
            raise ValueError("Handle unmanaged clusters")

    print("Creating databases")
    for db in workload["databases"]:
        c.sql(
            SQL("CREATE DATABASE {}").format(Identifier(db)),
            user="mz_system",
            port=6877,
        )

    print("Creating schemas")
    for db, schemas in workload["databases"].items():
        for schema in schemas:
            if schema == "public":
                continue
            c.sql(
                SQL("CREATE SCHEMA {}.{}").format(Identifier(db), Identifier(schema)),
                user="mz_system",
                port=6877,
            )

    print("Creating types")
    for schemas in workload["databases"].values():
        for items in schemas.values():
            for typ in items["types"].values():
                c.sql(typ["create_sql"], user="mz_system", port=6877)

    print("Preparing sources")
    if "postgres" in services:
        c.testdrive(
            dedent(
                """
            $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
            CREATE DATABASE IF NOT EXISTS materialize;
            CREATE SCHEMA IF NOT EXISTS public;
            CREATE SECRET pgpass AS 'postgres'
            """
            )
        )

    if "mysql" in services:
        c.testdrive(
            dedent(
                """
            $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
            CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'

            $ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}
            $ mysql-execute name=mysql
            DROP DATABASE IF EXISTS public;
            CREATE DATABASE public;
            """
            )
        )

    if "sql-server" in services:
        c.testdrive(
            dedent(
                f"""
            $ postgres-execute connection=postgres://mz_system:materialize@${{testdrive.materialize-internal-sql-addr}}
            CREATE SECRET sql_server_pass AS '{SqlServer.DEFAULT_SA_PASSWORD}'
                """
            )
        )
        # with open(MZ_ROOT / "test" / "sql-server-cdc" / "setup" / "setup.td") as f:
        #     c.testdrive(
        #         f.read(),
        #         args=[
        #             "--max-errors=1",
        #             f"--var=default-sql-server-user={SqlServer.DEFAULT_USER}",
        #             f"--var=default-sql-server-password={SqlServer.DEFAULT_SA_PASSWORD}",
        #         ],
        #     )

    print("Creating connections")
    existing_dbs = {"postgres": {"postgres"}, "sql-server": set()}
    for db, schemas in workload["databases"].items():
        for schema, items in schemas.items():
            for name, connection in items["connections"].items():
                if connection["type"] == "postgres":
                    match = re.search(
                        r"DATABASE\s*=\s*('?)([a-zA-Z_][a-zA-Z0-9_]*|\w+)\1(?=\s*[,\)])",
                        connection["create_sql"],
                        re.IGNORECASE,
                    )
                    assert match, f"No database found in {connection['create_sql']}"
                    ref_database = match.group(2)
                    if ref_database not in existing_dbs["postgres"]:
                        c.testdrive(
                            dedent(
                                f"""
                                $ postgres-execute connection=postgres://postgres:postgres@postgres
                                CREATE DATABASE {ref_database};
                                """
                            )
                        )
                        existing_dbs["postgres"].add(ref_database)
                    c.sql(
                        SQL(
                            "CREATE CONNECTION {}.{}.{} TO POSTGRES (HOST postgres, DATABASE {}, PASSWORD SECRET pgpass, USER postgres)"
                        ).format(
                            Identifier(db),
                            Identifier(schema),
                            Identifier(name),
                            Literal(ref_database),
                        ),
                        user="mz_system",
                        port=6877,
                    )
                elif connection["type"] == "mysql":
                    c.sql(
                        SQL(
                            "CREATE CONNECTION {}.{}.{} TO MYSQL (HOST mysql, PASSWORD SECRET mysqlpass, USER root)"
                        ).format(Identifier(db), Identifier(schema), Identifier(name)),
                        user="mz_system",
                        port=6877,
                    )
                elif connection["type"] == "sql-server":
                    match = re.search(
                        r"DATABASE\s*=\s*('?)([a-zA-Z_][a-zA-Z0-9_]*|\w+)\1(?=\s*[,\)])",
                        connection["create_sql"],
                        re.IGNORECASE,
                    )
                    assert match, f"No database found in {connection['create_sql']}"
                    ref_database = match.group(2)
                    if ref_database not in existing_dbs["sql-server"]:
                        c.testdrive(
                            dedent(
                                f"""
                                $ sql-server-connect name=sql-server
                                server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}
                                $ sql-server-execute name=sql-server
                                CREATE DATABASE {ref_database};
                                USE test;
                                EXEC sys.sp_cdc_enable_db;
                                ALTER DATABASE {ref_database} SET ALLOW_SNAPSHOT_ISOLATION ON;
                                """
                            )
                        )
                        existing_dbs["sql-server"].add(ref_database)
                    c.sql(
                        SQL(
                            "CREATE CONNECTION {}.{}.{} TO SQL SERVER (HOST 'sql-server', PASSWORD SECRET sql_server_pass, USER {}, DATABASE {}, PORT 1433)"
                        ).format(
                            Identifier(db),
                            Identifier(schema),
                            Identifier(name),
                            Identifier(SqlServer.DEFAULT_USER),
                            Identifier(ref_database),
                        ),
                        user="mz_system",
                        port=6877,
                    )
                elif connection["type"] == "kafka":
                    c.sql(
                        SQL(
                            "CREATE CONNECTION {}.{}.{} TO KAFKA (BROKER 'kafka', SECURITY PROTOCOL PLAINTEXT)"
                        ).format(Identifier(db), Identifier(schema), Identifier(name)),
                        user="mz_system",
                        port=6877,
                    )
                elif connection["type"] == "confluent-schema-registry":
                    c.sql(
                        SQL(
                            "CREATE CONNECTION {}.{}.{} TO CONFLUENT SCHEMA REGISTRY (URL 'http://schema-registry:8081')"
                        ).format(Identifier(db), Identifier(schema), Identifier(name)),
                        user="mz_system",
                        port=6877,
                    )
                elif connection["type"] == "ssh-tunnel":
                    c.sql(
                        SQL(
                            "CREATE CONNECTION {}.{}.{} TO SSH TUNNEL (HOST 'ssh-bastion-host', USER 'mz', PORT 22)"
                        ).format(Identifier(db), Identifier(schema), Identifier(name)),
                        user="mz_system",
                        port=6877,
                    )
                elif connection["type"] in ("aws-privatelink", "aws"):
                    pass  # can't run outside of cloud
                else:
                    raise ValueError(f"Unhandled connection type {connection['type']}")

    print("Preparing sources")
    for schemas in workload["databases"].values():
        for items in schemas.values():
            for name, source in items["sources"].items():
                first = True
                for child in source.get("children", {}).values():
                    if source["type"] == "mysql":
                        ref_database, ref_table = get_mysql_reference_db_table(child)
                        columns = [
                            f"{column['name']} {column['type']} {'NULL' if column['nullable'] else 'NOT NULL'} {'' if column['default'] is None else 'DEFAULT ' + column['default']}"
                            for column in child["columns"]
                        ]
                        if first:
                            c.testdrive(
                                dedent(
                                    f"""
                                    $ mysql-connect name=mysql url=mysql://root@mysql password=${{arg.mysql-root-password}}
                                    $ mysql-execute name=mysql
                                    DROP DATABASE IF EXISTS `{ref_database}`;
                                    CREATE DATABASE `{ref_database}`;
                                    CREATE TABLE `{ref_database}`.dummy (f1 INTEGER);
                                    """
                                )
                            )
                            first = False
                        c.testdrive(
                            dedent(
                                f"""
                            $ mysql-connect name=mysql url=mysql://root@mysql password=${{arg.mysql-root-password}}
                            $ mysql-execute name=mysql
                            CREATE TABLE `{ref_database}`.`{ref_table}` ({", ".join(columns)});
                                """
                            )
                        )
                    elif source["type"] == "postgres":
                        ref_database, ref_schema, ref_table = (
                            get_postgres_reference_db_schema_table(child)
                        )
                        match = re.search(
                            r"\bPUBLICATION\s*=\s*'?(?P<pub>[A-Za-z_][A-Za-z0-9_]*)'?",
                            source["create_sql"],
                        )
                        assert match, f"Publication not found in {source}"
                        publication = match.group(1)
                        columns = [
                            f"{column['name']} {column['type']} {'NULL' if column['nullable'] else 'NOT NULL'} DEFAULT {'NULL' if column['default'] is None else column['default']}"
                            for column in child["columns"]
                        ]
                        if first:
                            c.testdrive(
                                dedent(
                                    f"""
                                    $ postgres-execute connection=postgres://postgres:postgres@postgres/{ref_database}
                                    ALTER USER postgres WITH replication;

                                    DROP PUBLICATION IF EXISTS "{publication}";
                                    CREATE PUBLICATION "{publication}" FOR ALL TABLES;
                                    """
                                )
                            )
                            first = False
                        c.testdrive(
                            dedent(
                                f"""
                            $ postgres-execute connection=postgres://postgres:postgres@postgres/{ref_database}
                            CREATE SCHEMA IF NOT EXISTS "{ref_schema}";
                            CREATE TABLE "{ref_schema}"."{ref_table}" ({", ".join(columns)});
                            ALTER TABLE "{ref_schema}"."{ref_table}" REPLICA IDENTITY FULL;
                                """
                            )
                        )
                    elif source["type"] == "sql-server":
                        ref_database, ref_schema, ref_table = (
                            get_sql_server_reference_db_schema_table(child)
                        )
                        columns = [
                            f"{column['name']} {to_sql_server_data_type(column['type'])} {'NULL' if column['nullable'] else 'NOT NULL'} DEFAULT {'NULL' if column['default'] is None else column['default']}"
                            for column in child["columns"]
                        ]
                        if first:
                            c.testdrive(
                                dedent(
                                    f"""
                                    $ sql-server-connect name=sql-server
                                    server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

                                    $ sql-server-execute name=sql-server
                                    IF DB_ID(N'{ref_database}') IS NULL BEGIN CREATE DATABASE {ref_database}; END
                                    USE {ref_database};
                                    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'{ref_schema}')BEGIN EXEC(N'CREATE SCHEMA {ref_schema}'); END
                                    """
                                )
                            )
                            first = False
                        c.testdrive(
                            dedent(
                                f"""
                            $ sql-server-connect name=sql-server
                            server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

                            $ sql-server-execute name=sql-server
                            USE {ref_database};
                            CREATE TABLE "{ref_schema}"."{ref_table}" ({", ".join(columns)});
                            EXEC sys.sp_cdc_enable_table @source_schema = '{ref_schema}', @source_name = '{ref_table}', @role_name = 'SA', @supports_net_changes = 0;
                                """
                            )
                        )

                if source["type"] == "kafka":
                    topic = get_kafka_topic(source)
                    kafka_conf = {
                        "bootstrap.servers": f"127.0.0.1:{c.default_port('kafka')}"
                    }
                    admin_client = AdminClient(kafka_conf)
                    admin_client.create_topics(
                        [
                            confluent_kafka.admin.NewTopic(  # type: ignore
                                topic, num_partitions=1, replication_factor=1
                            )
                        ]
                    )

                    # Have to wait for topics to be created before creating sources/tables using them
                    while True:
                        md = admin_client.list_topics(timeout=2)
                        if topic in md.topics and md.topics[topic].error is None:
                            break
                        print(f"Waiting for topic: {topic}")
                        time.sleep(1)

                if source["type"] == "webhook":
                    # Checking secrets makes ingestion into webhooks difficult, remove the check instead
                    source["create_sql"] = re.sub(
                        r"\s*CHECK\s*\(.*?\)\s*;",
                        "",
                        source["create_sql"],
                        flags=re.DOTALL | re.IGNORECASE,
                    )


def run_create_objects_part_2(
    c: Composition, services: set[str], workload: dict[str, Any]
) -> None:
    print("Creating sources")
    for schemas in workload["databases"].values():
        for items in schemas.values():
            for name, source in items["sources"].items():
                c.sql(source["create_sql"], user="mz_system", port=6877)

                for child in source.get("children", {}).values():
                    if child["type"] == "table":
                        # TODO: Remove when https://github.com/MaterializeInc/database-issues/issues/10034 is fixed
                        create_sql = re.sub(
                            r",?\s*DETAILS\s*=\s*'[^']*'",
                            "",
                            child["create_sql"],
                            flags=re.IGNORECASE,
                        )
                        create_sql = re.sub(
                            r"\sWITH \(\s*\)", "", create_sql, flags=re.IGNORECASE
                        )
                        if source["type"] == "load-generator":
                            # TODO: Remove when https://github.com/MaterializeInc/database-issues/issues/10010 is fixed
                            create_sql = re.sub(
                                r"mz_load_generators\.",
                                "",
                                create_sql,
                                flags=re.IGNORECASE,
                            )
                        create_sql = re.sub(
                            r"\s*\(.*\)\s+FROM\s+",
                            " FROM ",
                            create_sql,
                            flags=re.IGNORECASE | re.DOTALL,
                        )
                        c.sql(create_sql, user="mz_system", port=6877)

    print("Creating tables")
    for schemas in workload["databases"].values():
        for items in schemas.values():
            for table in items["tables"].values():
                c.sql(table["create_sql"], user="mz_system", port=6877)

    print("Creating view, materialized views, sinks")
    pending = set()
    for schemas in workload["databases"].values():
        for items in schemas.values():
            for view in items["views"].values():
                pending.add(view["create_sql"])
            for mv in items["materialized_views"].values():
                pending.add(mv["create_sql"])
            for sink in items["sinks"].values():
                pending.add(sink["create_sql"])

    # TODO: Handle sink -> source roundtrips: scan for topic names to create a dependency graph
    while pending:
        progress = False
        for create in pending.copy():
            try:
                c.sql(create, user="mz_system", port=6877)
            except psycopg.Error as e:
                if "unknown catalog item" in str(e):
                    continue
                raise
            pending.remove(create)
            progress = True
        if not progress:
            raise RuntimeError(f"No progress, remaining creates: {pending}")


def create_initial_data_requiring_mz(
    c: Composition,
    workload: dict[str, Any],
    factor_initial_data: float,
    rng: random.Random,
) -> bool:
    batch_size = 10000
    created_data = False

    conn = psycopg.connect(
        host="127.0.0.1",
        port=c.port("materialized", 6877),
        user="mz_system",
        password="materialize",
        dbname="materialize",
    )
    conn.autocommit = True

    for db, schemas in workload["databases"].items():
        for schema, items in schemas.items():
            for name, table in items["tables"].items():
                num_rows = int(table["rows"] * factor_initial_data)
                if not num_rows:
                    continue

                data_columns = [
                    Column(
                        col["name"],
                        col["type"],
                        col["nullable"],
                        col["default"],
                        col.get("data_shape"),
                    )
                    for col in table["columns"]
                ]

                print(f"Creating {num_rows} rows for {db}.{schema}.{name}:")

                col_names = [col.name for col in data_columns]

                with conn.cursor() as cur:
                    for start in range(0, num_rows, batch_size):
                        progress = min(start + batch_size, num_rows)
                        print(
                            f"{progress}/{num_rows} ({progress / num_rows:.1%})",
                            end="\r",
                            flush=True,
                        )

                        copy_stmt = SQL("COPY {}.{}.{} ({}) FROM STDIN").format(
                            Identifier(db),
                            Identifier(schema),
                            Identifier(name),
                            SQL(", ").join(map(Identifier, col_names)),
                        )

                        with cur.copy(copy_stmt) as copy:
                            batch_rows = min(batch_size, num_rows - start)
                            for _ in range(batch_rows):
                                row = [
                                    col.value(rng, in_query=False)
                                    for col in data_columns
                                ]
                                copy.write_row(row)

            for name, source in items["sources"].items():
                if source["type"] == "webhook":
                    num_rows = int(source["messages_total"] * factor_initial_data)
                    if not num_rows:
                        continue
                    print(f"Creating {num_rows} rows for {db}.{schema}.{name}:")
                    asyncio.run(
                        ingest_webhook(
                            c,
                            db,
                            schema,
                            name,
                            source,
                            num_rows,
                            print_progress=True,
                        )
                    )
                    created_data = True
    conn.close()
    return created_data


def create_initial_data_external(
    c: Composition,
    workload: dict[str, Any],
    factor_initial_data: float,
    rng: random.Random,
) -> bool:
    batch_size = 10000
    created_data = False
    for db, schemas in workload["databases"].items():
        for schema, items in schemas.items():
            for name, source in items["sources"].items():
                if source["type"] != "webhook" and not source.get("children", {}):
                    num_rows = int(source["messages_total"] * factor_initial_data)
                    if not num_rows:
                        continue
                    data_columns = [
                        Column(
                            c["name"],
                            c["type"],
                            c["nullable"],
                            c["default"],
                            c.get("data_shape"),
                        )
                        for c in source["columns"]
                    ]
                    print(f"Creating {num_rows} rows for {db}.{schema}.{name}:")
                    for start in range(0, num_rows, batch_size):
                        progress = min(start + batch_size, num_rows)
                        print(
                            f"{progress}/{num_rows} ({progress / num_rows:.1%})",
                            end="\r",
                            flush=True,
                        )
                        ingest(
                            c,
                            source,
                            source,
                            data_columns,
                            min(batch_size, num_rows - start),
                            rng,
                        )
                    created_data = True
                    print()
                else:
                    for child_name, child in source.get("children", {}).items():
                        num_rows = int(child["messages_total"] * factor_initial_data)
                        if not num_rows:
                            continue
                        data_columns = [
                            Column(
                                c["name"],
                                c["type"],
                                c["nullable"],
                                c["default"],
                                c.get("data_shape"),
                            )
                            for c in child["columns"]
                        ]
                        print(
                            f"Creating {num_rows} rows for {db}.{schema}.{name}->{child_name}:"
                        )
                        for start in range(0, num_rows, batch_size):
                            progress = min(start + batch_size, num_rows)
                            print(
                                f"{progress}/{num_rows} ({progress / num_rows:.1%})",
                                end="\r",
                                flush=True,
                            )
                            ingest(
                                c,
                                child,
                                source,
                                data_columns,
                                min(batch_size, num_rows - start),
                                rng,
                            )
                        created_data = True
                        print()
    return created_data


def create_ingestions(
    c: Composition,
    workload: dict[str, Any],
    stop_event: threading.Event,
    factor_ingestions: float,
    verbose: bool,
    stats: dict[str, int],
) -> list[threading.Thread]:
    threads = []
    for db, schemas in workload["databases"].items():
        for schema, items in schemas.items():
            for name, source in items["sources"].items():
                if source["type"] == "webhook":
                    if "messages_second" not in source:
                        continue
                    target_rps = source["messages_second"] * factor_ingestions
                    if target_rps <= 0:
                        continue

                    batch_size = 1
                    period_s = min(batch_size / target_rps, 60)

                    pretty_name = f"{db}.{schema}.{name}"
                    print(
                        f"{target_rps:.3f} ing./s for {pretty_name} ({batch_size} every {period_s:.2f}s)"
                    )

                    def continuous_ingestion_webhook(
                        db: str,
                        schema: str,
                        name: str,
                        source: dict[str, Any],
                        pretty_name: str,
                        batch_size: int,
                        period_s: float,
                        rng: random.Random,
                    ) -> None:
                        nonlocal stop_event
                        try:
                            next_time = time.time()

                            while not stop_event.is_set():
                                now = time.time()
                                if now < next_time:
                                    stop_event.wait(timeout=next_time - now)
                                    continue

                                next_time += period_s

                                if verbose:
                                    print(
                                        f"Ingesting {batch_size} rows for {pretty_name}"
                                    )

                                stats["total"] += 1
                                asyncio.run(
                                    ingest_webhook(
                                        c,
                                        db,
                                        schema,
                                        name,
                                        source,
                                        batch_size,
                                    )
                                )

                                after = time.time()
                                if after > next_time:
                                    stats["slow"] += 1
                                    next_time = after
                                    if verbose:
                                        print(f"Can't keep up: {pretty_name}")

                        except Exception as e:
                            stats["failed"] += 1
                            print(f"Failed: {pretty_name}")
                            print(e)
                            stop_event.set()
                            raise

                elif not source.get("children", {}):
                    if "messages_second" not in source:
                        continue
                    target_rps = source["messages_second"] * factor_ingestions
                    if target_rps <= 0:
                        continue

                    batch_size = 1
                    period_s = min(batch_size / target_rps, 60)

                    data_columns = [
                        Column(
                            c["name"],
                            c["type"],
                            c["nullable"],
                            c["default"],
                            c.get("data_shape"),
                        )
                        for c in source["columns"]
                    ]

                    pretty_name = f"{db}.{schema}.{name}"
                    print(
                        f"{target_rps:.3f} ing./s for {pretty_name} "
                        f"({batch_size} every {period_s:.2f}s)"
                    )

                    def continuous_ingestion_source(
                        source: dict[str, Any],
                        pretty_name: str,
                        data_columns: list[Column],
                        batch_size: int,
                        period_s: float,
                        rng: random.Random,
                    ) -> None:
                        nonlocal stop_event
                        try:
                            next_time = time.time()

                            while not stop_event.is_set():
                                now = time.time()
                                if now < next_time:
                                    stop_event.wait(timeout=next_time - now)
                                    continue

                                next_time += period_s

                                if verbose:
                                    print(
                                        f"Ingesting {batch_size} rows for {pretty_name}"
                                    )

                                stats["total"] += 1
                                ingest(
                                    c,
                                    source,
                                    source,
                                    data_columns,
                                    batch_size,
                                    rng,
                                )

                                after = time.time()
                                if after > next_time:
                                    stats["slow"] += 1
                                    next_time = after
                                    if verbose:
                                        print(f"Can't keep up: {pretty_name}")

                        except Exception as e:
                            stats["failed"] += 1
                            print(f"Failed: {pretty_name}")
                            print(e)
                            stop_event.set()
                            raise

                    threads.append(
                        PropagatingThread(
                            target=continuous_ingestion_source,
                            name=f"ingest-{pretty_name}",
                            args=(
                                source,
                                pretty_name,
                                data_columns,
                                batch_size,
                                period_s,
                                random.Random(random.randrange(SEED_RANGE)),
                            ),
                        )
                    )
                else:
                    for child_name, child in source.get("children", {}).items():
                        if "messages_second" not in child:
                            continue
                        target_rps = child["messages_second"] * factor_ingestions
                        if target_rps <= 0:
                            continue

                        batch_size = 1
                        period_s = min(batch_size / target_rps, 60)

                        data_columns = [
                            Column(
                                c["name"],
                                c["type"],
                                c["nullable"],
                                c["default"],
                                c.get("data_shape"),
                            )
                            for c in child["columns"]
                        ]

                        pretty_name = f"{db}.{schema}.{name}->{child_name}"
                        print(
                            f"{target_rps:.3f} ing./s for {pretty_name} "
                            f"({batch_size} every {period_s:.2f}s)"
                        )

                        def continuous_ingestion_child(
                            source: dict[str, Any],
                            child: dict[str, Any],
                            pretty_name: str,
                            data_columns: list[Column],
                            batch_size: int,
                            period_s: float,
                            rng: random.Random,
                        ) -> None:
                            nonlocal stop_event
                            try:
                                next_time = time.time()

                                while not stop_event.is_set():
                                    now = time.time()
                                    if now < next_time:
                                        stop_event.wait(timeout=next_time - now)
                                        continue

                                    next_time += period_s

                                    if verbose:
                                        print(
                                            f"Ingesting {batch_size} rows for {pretty_name}"
                                        )

                                    stats["total"] += 1
                                    ingest(
                                        c,
                                        child,
                                        source,
                                        data_columns,
                                        batch_size,
                                        rng,
                                    )

                                    after = time.time()
                                    if after > next_time:
                                        stats["slow"] += 1
                                        next_time = after
                                        if verbose:
                                            print(f"Can't keep up: {pretty_name}")

                            except Exception as e:
                                stats["failed"] += 1
                                print(f"Failed: {pretty_name}")
                                print(e)
                                stop_event.set()
                                raise

                        threads.append(
                            PropagatingThread(
                                target=continuous_ingestion_child,
                                name=f"ingest-{pretty_name}",
                                args=(
                                    source,
                                    child,
                                    pretty_name,
                                    data_columns,
                                    batch_size,
                                    period_s,
                                    random.Random(random.randrange(SEED_RANGE)),
                                ),
                            )
                        )

    return threads


def run_query(
    c: Composition, query: dict[str, Any], stats: dict[str, Any], verbose: bool
) -> None:
    conn = c.sql_connection(user="mz_system", port=6877)
    with conn.cursor() as cur:
        cur.execute(
            SQL("SET transaction_isolation = {}").format(
                Literal(query["transaction_isolation"])
            )
        )
        cur.execute(SQL("SET cluster = {}").format(Literal(query["cluster"])))
        cur.execute(SQL("SET database = {}").format(Literal(query["database"])))
        cur.execute(f"SET search_path = {','.join(query['search_path'])}".encode())
        stats["total"] += 1
        try:
            sql, params = pg_params_to_psycopg(query["sql"], query["params"])
            # TODO: Better repacements for <REDACTED>, but requires parsing the SQL, figuring out the column name, object name, looking up the data type, etc.
            sql = sql.replace("'<REDACTED'>", "NULL")
            start_time = time.time()
            cur.execute(sql.encode(), params)
            end_time = time.time()
            stats["timings"].append((sql, end_time - start_time))
            if verbose:
                print(f"Success: {sql} (params: {params})")
        except psycopg.Error as e:
            stats["failed"] += 1
            stats["errors"][f"{e.sqlstate}: {e}"].append(sql)
            if query["finished_status"] == "success":
                if "unknown catalog item" not in str(e):
                    print(f"Failed: {sql} (params: {params})")
                    print(f"{e.sqlstate}: {e}")
            elif verbose:
                print(f"Failed expectedly: {sql} (params: {params})")
                print(f"{e.sqlstate}: {e}")
        # SELECT mz_indexes.name, schema_name, database FROM mz_indexes JOIN mz_internal.mz_object_fully_qualified_names AS ofqn ON on_id = ofqn.id WHERE schema_name NOT IN ('mz_catalog', 'mz_internal', 'mz_introspection')


def continuous_queries(
    c: Composition,
    workload: dict[str, Any],
    stop_event: threading.Event,
    factor_queries: float,
    verbose: bool,
    stats: dict[str, int],
    rng: random.Random,
) -> None:
    if not workload["queries"]:
        return
    i = 0
    try:
        while True:
            i += 1
            start = workload["queries"][0]["began_at"]
            replay_start = datetime.datetime.now(datetime.timezone.utc)
            for query in workload["queries"]:
                if stop_event.is_set():
                    return

                if query["statement_type"] in (
                    # TODO: Requires recreating transactions in which these have to be run
                    "start_transaction",
                    "set_transaction",
                    "commit",
                    "rollback",
                    "fetch",
                    # They will already exist statically, don't create
                    "create_connection",
                    "create_webhook",
                    "create_source",
                    "create_subsource",
                    "create_sink",
                    "create_table_from_source",
                ):
                    continue

                offset = (query["began_at"] - start) / factor_queries
                scheduled = replay_start + offset
                sleep_seconds = (
                    scheduled - datetime.datetime.now(datetime.timezone.utc)
                ).total_seconds()
                if sleep_seconds > 0:
                    stop_event.wait(timeout=sleep_seconds)
                    if stop_event.is_set():
                        return
                elif sleep_seconds < 0:
                    stats["slow"] += 1
                    if verbose:
                        print(f"Can't keep up: {query}")
                thread = PropagatingThread(
                    target=run_query,
                    name=f"query-{i}",
                    args=(
                        c,
                        query,
                        stats,
                        verbose,
                    ),
                )
                thread.start()
    except Exception as e:
        print(f"Failed: {query['sql']}")
        print(e)
        stop_event.set()
        raise


_SI_UNITS = {
    "b": 1,
    "kb": 10**3,
    "mb": 10**6,
    "gb": 10**9,
    "tb": 10**12,
    "pb": 10**15,
}

_IEC_UNITS = {
    "kib": 2**10,
    "mib": 2**20,
    "gib": 2**30,
    "tib": 2**40,
    "pib": 2**50,
}


def parse_bytes(s: str) -> int:
    """
    Parses strings like:
      "76.5MB" -> 76500000
      "3.202GiB" -> 3438115842
      "1e+03kB" -> 1000000
      "0B" -> 0
    """
    s = s.strip()
    # number: decimal or scientific notation, e.g. 1e+03, 3.2E-1, .5
    m = re.fullmatch(
        r"([+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*([A-Za-z]+)", s
    )
    if not m:
        raise ValueError(f"Cannot parse bytes: {s!r}")

    val = float(m.group(1))
    unit_raw = m.group(2)

    unit = unit_raw.lower()

    if unit in _SI_UNITS:
        mult = _SI_UNITS[unit]
    elif unit in _IEC_UNITS:
        mult = _IEC_UNITS[unit]
    else:
        raise ValueError(f"Unknown unit {unit_raw!r} in {s!r}")

    return int(val * mult)


def parse_two_bytes(s: str) -> tuple[int, int]:
    a, b = (x.strip() for x in s.split("/", 1))
    return parse_bytes(a), parse_bytes(b)


def parse_percent(s: str) -> float:
    return float(s.strip().rstrip("%"))


def docker_stats(
    stats: list[tuple[int, dict[str, dict[str, Any]]]], stop_event: threading.Event
) -> None:
    while not stop_event.is_set():
        result = {}
        timestamp = int(time.time())
        for line in subprocess.check_output(
            ["docker", "stats", "--format", "json", "--no-trunc", "--no-stream"],
            text=True,
        ).splitlines():
            obj = json.loads(line)
            if not obj["Name"].startswith("workload-replay-"):
                continue
            # net_rx, net_tx = parse_two_bytes(obj["NetIO"])
            # blk_read, blk_write = parse_two_bytes(obj["BlockIO"])
            result[obj["Name"].removeprefix("workload-replay-").removesuffix("-1")] = {
                "cpu_percent": parse_percent(obj["CPUPerc"]),
                "mem_percent": parse_percent(obj["MemPerc"]),
                # "net_rx_bytes": net_rx,
                # "net_tx_bytes": net_tx,
                # "block_read_bytes": blk_read,
                # "block_write_bytes": blk_write,
            }
        stats.append((timestamp, result))


def print_replay_stats(stats: dict[str, Any]) -> None:
    print("Queries:")
    print(f"   Total: {stats['queries']['total']}")
    failed = (
        100.0 * stats["queries"]["failed"] / stats["queries"]["total"]
        if stats["queries"]["total"]
        else 0
    )
    print(f"  Failed: {stats['queries']['failed']} ({failed:.0f}%)")
    slow = (
        100.0 * stats["queries"]["slow"] / stats["queries"]["total"]
        if stats["queries"]["total"]
        else 0
    )
    print(f"    Slow: {stats['queries']['slow']} ({slow:.0f}%)")
    # print(f" Timings: {stats['queries']['timings']}")
    print("Ingestions:")
    print(f"   Total: {stats['ingestions']['total']}")
    failed = (
        100.0 * stats["ingestions"]["failed"] / stats["ingestions"]["total"]
        if stats["ingestions"]["total"]
        else 0
    )
    print(f"  Failed: {stats['ingestions']['failed']} ({failed:.0f}%)")
    slow = (
        100.0 * stats["ingestions"]["slow"] / stats["ingestions"]["total"]
        if stats["ingestions"]["total"]
        else 0
    )
    print(f"    Slow: {stats['ingestions']['slow']} ({slow:.0f}%)")
    # print(f"Docker stats: {stats['docker']}")


def upload_plots(
    plot_paths: list[str],
    file: str,
):
    if not plot_paths:
        return
    if buildkite.is_in_buildkite():
        for plot_path in plot_paths:
            buildkite.upload_artifact(plot_path, cwd=MZ_ROOT, quiet=True)
        print(f"+++ Plots for {file}")
        for plot_path in plot_paths:
            print(
                buildkite.inline_image(f"artifact://{plot_path}", f"Plots for {file}")
            )
    else:
        print(f"Saving plots to {plot_paths}")


class DockerSeries:
    def __init__(
        self,
        *,
        t: list[int],
        cpu_percent: dict[str, list[float]],
        mem_percent: dict[str, list[float]],
    ) -> None:
        self.t = t
        self.cpu_percent = cpu_percent
        self.mem_percent = mem_percent


def extract_docker_series(
    docker_stats: list[tuple[int, dict[str, dict[str, Any]]]]
) -> DockerSeries:
    t0 = docker_stats[0][0]
    times: list[int] = [ts - t0 for (ts, _snapshot) in docker_stats]

    containers: set[str] = set()
    for _ts, snapshot in docker_stats:
        containers |= set(snapshot.keys())

    def init_float() -> dict[str, list[float]]:
        return {c: [] for c in sorted(containers)}

    def init_int() -> dict[str, list[int]]:
        return {c: [] for c in sorted(containers)}

    cpu_percent = init_float()
    mem_percent = init_float()

    for _ts, snapshot in docker_stats:
        for c in sorted(containers):
            m = snapshot.get(c)

            def last_or(lst: list[Any], default: Any):
                return default if not lst else lst[-1]

            if m is None:
                cpu_percent[c].append(last_or(cpu_percent[c], 0.0))
                mem_percent[c].append(last_or(mem_percent[c], 0.0))
                continue

            cpu_percent[c].append(float(m["cpu_percent"]))
            mem_percent[c].append(float(m["mem_percent"]))

    return DockerSeries(
        t=times,
        cpu_percent=cpu_percent,
        mem_percent=mem_percent,
    )


YScale = TypeLiteral["linear", "log", "symlog", "logit"]


def plot_timeseries_compare(
    *,
    t_old: list[int],
    ys_old: dict[str, list[float]],
    t_new: list[int],
    ys_new: dict[str, list[float]],
    title: str,
    ylabel: str,
    out_path: Path,
    yscale: YScale | None = None,
):
    plt.figure(figsize=(10, 6))

    containers = sorted(set(ys_old.keys()) | set(ys_new.keys()))

    for c in containers:
        if c in ys_old:
            plt.plot(t_old, ys_old[c], linestyle="--", label=f"{c} (old)")
        if c in ys_new:
            plt.plot(t_new, ys_new[c], linestyle="-", label=f"{c} (new)")

    plt.xlabel("time [s]")
    plt.ylabel(ylabel)
    if yscale is not None:
        plt.yscale(yscale)

    plt.title(title)
    plt.legend(loc="best")  # type: ignore
    plt.grid(True)
    plt.ylim(bottom=0)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, dpi=300)
    plt.close()


def plot_docker_stats_compare(
    *,
    stats_old: dict[str, Any],
    stats_new: dict[str, Any],
    file: str,
    old_version: str,
    new_version: str,
):
    plot_paths = []

    if "initial_data" in stats_old:
        old = extract_docker_series(stats_old["initial_data"]["docker"])
        new = extract_docker_series(stats_new["initial_data"]["docker"])

        plot_path = Path("plots") / f"{file}_initial_cpu.png"
        plot_timeseries_compare(
            t_old=old.t,
            ys_old=old.cpu_percent,
            t_new=new.t,
            ys_new=new.cpu_percent,
            title=f"{file} - Initial Data Phase CPU\n{old_version} [old] vs {new_version} [new]",
            ylabel="CPU [%]",
            out_path=plot_path,
        )
        plot_paths.append(plot_path)

        plot_path = Path("plots") / f"{file}_initial_mem.png"
        plot_timeseries_compare(
            t_old=old.t,
            ys_old=old.mem_percent,
            t_new=new.t,
            ys_new=new.mem_percent,
            title=f"{file} - Initial Data Phase Memory\n{old_version} [old] vs {new_version} [new]",
            ylabel="Memory [%]",
            out_path=plot_path,
        )
        plot_paths.append(plot_path)

    if "docker" in stats_old:
        old = extract_docker_series(stats_old["docker"])
        new = extract_docker_series(stats_new["docker"])

        plot_path = Path("plots") / f"{file}_continuous_cpu.png"
        plot_timeseries_compare(
            t_old=old.t,
            ys_old=old.cpu_percent,
            t_new=new.t,
            ys_new=new.cpu_percent,
            title=f"{file} - Continuous Phase CPU\n{old_version} [old] vs {new_version} [new]",
            ylabel="CPU [%]",
            out_path=plot_path,
        )
        plot_paths.append(plot_path)

        plot_path = Path("plots") / f"{file}_continuous_mem.png"
        plot_timeseries_compare(
            t_old=old.t,
            ys_old=old.mem_percent,
            t_new=new.t,
            ys_new=new.mem_percent,
            title=f"{file} - Continuous Phase Memory\n{old_version} [old] vs {new_version} [new]",
            ylabel="Memory [%]",
            out_path=plot_path,
        )
        plot_paths.append(plot_path)

    upload_plots(plot_paths, file)


def average_cpu_mem_for_container(
    docker_stats: list[tuple[int, dict[str, dict[str, Any]]]],
    container: str,
) -> tuple[float, float]:
    cpu_sum = 0.0
    mem_sum = 0.0
    n = 0

    for _ts, snapshot in docker_stats:
        m = snapshot.get(container)
        if m is None:
            continue
        cpu_sum += float(m["cpu_percent"])
        mem_sum += float(m["mem_percent"])
        n += 1

    if n == 0:
        raise ValueError(f"container {container!r} not found in docker_stats")

    return (cpu_sum / n, mem_sum / n)


def query_timing_stats(stats: dict[str, Any]) -> dict[str, float]:
    durations = sorted(float(t) for (_sql, t) in stats["queries"]["timings"])
    return {
        "avg": float(numpy.mean(durations)),
        "min": min(durations),
        "max": max(durations),
        "p50": float(numpy.median(durations)),
        "p95": float(numpy.percentile(durations, 95)),
        "p99": float(numpy.percentile(durations, 99)),
        # "p99_9": float(numpy.percentile(durations, 99.9)),
        # "p99_99": float(numpy.percentile(durations, 99.99)),
        # "p99_999": float(numpy.percentile(durations, 99.999)),
        # "p99_9999": float(numpy.percentile(durations, 99.9999)),
        # "p99_99999": float(numpy.percentile(durations, 99.99999)),
        # "p99_999999": float(numpy.percentile(durations, 99.999999)),
        "std": float(numpy.std(durations, ddof=1)),
    }


def pct_change(old: float, new: float) -> float:
    if old == 0:
        return float("inf") if new != 0 else 0.0
    return (new - old) / old * 100.0


def fmt_pct(delta: float) -> str:
    if delta == float("inf"):
        return "inf"
    return f"{delta:+.1f}%"


def compare_table(
    filename: str, stats_old: dict[str, Any], stats_new: dict[str, Any]
) -> list[TestFailureDetails]:
    rows = []
    if "object_creation" in stats_old:
        rows.append(
            (
                "Object creation (s)",
                stats_old["object_creation"],
                stats_new["object_creation"],
                1.2,
            )
        )

    if "initial_data" in stats_old:
        old_avg_cpu_initial_data, old_avg_mem_initial_data = (
            average_cpu_mem_for_container(
                stats_old["initial_data"]["docker"], "materialized"
            )
        )
        new_avg_cpu_initial_data, new_avg_mem_initial_data = (
            average_cpu_mem_for_container(
                stats_new["initial_data"]["docker"], "materialized"
            )
        )
        rows.extend(
            [
                (
                    "Data ingestion CPU (sum)",
                    old_avg_cpu_initial_data * stats_old["initial_data"]["time"],
                    new_avg_cpu_initial_data * stats_new["initial_data"]["time"],
                    1.2,
                ),
                (
                    "Data ingestion Mem (sum)",
                    old_avg_mem_initial_data * stats_old["initial_data"]["time"],
                    new_avg_mem_initial_data * stats_new["initial_data"]["time"],
                    1.2,
                ),
            ]
        )

    if "docker" in stats_old:
        old_avg_cpu, old_avg_mem = average_cpu_mem_for_container(
            stats_old["docker"], "materialized"
        )
        new_avg_cpu, new_avg_mem = average_cpu_mem_for_container(
            stats_new["docker"], "materialized"
        )
        rows.extend(
            [
                ("CPU avg (%)", old_avg_cpu, new_avg_cpu, 1.2),
                ("Mem avg (%)", old_avg_mem, new_avg_mem, 1.2),
            ]
        )

    if "timings" in stats_old["queries"]:
        old_q = query_timing_stats(stats_old)
        new_q = query_timing_stats(stats_new)
        rows.extend(
            [
                ("Query max (ms)", old_q["max"] * 1000, new_q["max"] * 1000, None),
                ("Query min (ms)", old_q["min"] * 1000, new_q["min"] * 1000, None),
                # TODO: Why is query avg/p50 not stable enough?
                (
                    "Query avg (ms)",
                    old_q["avg"] * 1000,
                    new_q["avg"] * 1000,
                    None,
                ),
                (
                    "Query p50 (ms)",
                    old_q["p50"] * 1000,
                    new_q["p50"] * 1000,
                    None,
                ),
                ("Query p95 (ms)", old_q["p95"] * 1000, new_q["p95"] * 1000, None),
                ("Query p99 (ms)", old_q["p99"] * 1000, new_q["p99"] * 1000, None),
                ("Query std (ms)", old_q["std"] * 1000, new_q["std"] * 1000, None),
            ]
        )

    failures: list[TestFailureDetails] = []

    output_lines = [
        f"{'METRIC':<24} | {'OLD':^12} | {'NEW':^12} | {'CHANGE':^9} | {'THRESHOLD':^9} | {'REGRESSION?':^12}",
        "-" * 93,
    ]

    regressed = False
    for name, old, new, threshold in rows:
        delta = pct_change(old, new)

        if threshold is None:
            flag = ""
        elif new > old * threshold:
            regressed = True
            flag = "!!YES!!"
        else:
            flag = "no"
        threshold_field = f"{threshold:>9.3f}" if threshold is not None else ""
        output_lines.append(
            f"{name:<24} | "
            f"{old:>12.3f} | "
            f"{new:>12.3f} | "
            f"{fmt_pct(delta):>9} | "
            f"{threshold_field:>9} | "
            f"{flag:^12}"
        )
    if regressed:
        failures.append(
            TestFailureDetails(
                message=f"Workload {filename} regressed",
                details="\n".join(output_lines),
                test_class_name_override=filename,
            )
        )

    print("\n".join(output_lines))
    return failures


def benchmark(
    c: Composition,
    file: pathlib.Path,
    compare_against: str,
    factor_initial_data: float,
    factor_ingestions: float,
    factor_queries: float,
    runtime: int,
    verbose: bool,
    seed: str,
    early_initial_data: bool,
) -> None:
    services = [
        "materialized",
        "postgres",
        "mysql",
        "sql-server",
        "kafka",
        "schema-registry",
        "zookeeper",
        "ssh-bastion-host",
        "testdrive",
    ]

    with open(file) as f:
        workload = yaml.load(f, Loader=yaml.CSafeLoader)

    print_workload_stats(file, workload)

    tag = resolve_tag(compare_against)
    print(f"-- Running against materialized:{tag} (reference)")
    random.seed(seed)
    with c.override(
        Materialized(
            image=f"{image_registry()}/materialized:{tag}",
            cluster_replica_size=cluster_replica_sizes,
            ports=[6875, 6874, 6876, 6877, 6878, 6880, 6881, 26257],
            environment_extra=["MZ_NO_BUILTIN_CONSOLE=0"],
            additional_system_parameter_defaults={"enable_rbac_checks": "false"},
        )
    ):
        stats_old = test(
            c,
            workload,
            file,
            factor_initial_data,
            factor_ingestions,
            factor_queries,
            runtime,
            verbose,
            True,
            True,
            early_initial_data,
            True,
            True,
        )
        old_version = c.query_mz_version()
    try:
        c.kill(*services)
    except:
        pass
    c.rm(*services, destroy_volumes=True)
    c.rm_volumes("mzdata")
    print("-- Running against current materialized")
    random.seed(seed)
    with c.override(
        Materialized(
            image=None,
            cluster_replica_size=cluster_replica_sizes,
            ports=[6875, 6874, 6876, 6877, 6878, 6880, 6881, 26257],
            environment_extra=["MZ_NO_BUILTIN_CONSOLE=0"],
            additional_system_parameter_defaults={"enable_rbac_checks": "false"},
        )
    ):
        stats_new = test(
            c,
            workload,
            file,
            factor_initial_data,
            factor_ingestions,
            factor_queries,
            runtime,
            verbose,
            True,
            True,
            early_initial_data,
            True,
            True,
        )
        new_version = c.query_mz_version()
    try:
        c.kill(*services)
    except:
        pass
    c.rm(*services, destroy_volumes=True)
    c.rm_volumes("mzdata")
    filename = posixpath.relpath(file, LOCATION)

    print(f"-- Comparing {old_version} against {new_version}")
    plot_docker_stats_compare(
        stats_old=stats_old,
        stats_new=stats_new,
        file=filename,
        old_version=old_version,
        new_version=new_version,
    )
    failures: list[TestFailureDetails] = []
    failures.extend(compare_table(filename, stats_old, stats_new))

    if "errors" in stats_old["queries"]:
        new_errors = []
        for error, occurrences in stats_new["queries"]["errors"].items():
            if error in stats_old["queries"]["errors"]:
                continue
            # XX000: Evaluation error: invalid input syntax for type uuid: invalid character: expected an optional prefix of `urn:uuid:` followed by [0-9a-fA-F-], found `V` at 4: "005V"
            if "invalid input syntax for type uuid" in error:
                continue
            new_errors.append(f"{error} in queries: {occurrences}")
        if new_errors:
            failures.append(
                TestFailureDetails(
                    message=f"Workload {filename} has new errors",
                    details="\n".join(new_errors),
                    test_class_name_override=filename,
                )
            )

    if failures:
        raise FailedTestExecutionError(errors=failures)


def test(
    c: Composition,
    workload: dict[str, Any],
    file: pathlib.Path,
    factor_initial_data: float,
    factor_ingestions: float,
    factor_queries: float,
    runtime: int,
    verbose: bool,
    create_objects: bool,
    initial_data: bool,
    early_initial_data: bool,
    run_ingestions: bool,
    run_queries: bool,
) -> dict[str, Any]:
    print(f"--- {posixpath.relpath(file, LOCATION)}")
    services = set()

    for schemas in workload["databases"].values():
        for objs in schemas.values():
            for connection in objs["connections"].values():
                if connection["type"] == "postgres":
                    services.add("postgres")
                elif connection["type"] == "mysql":
                    services.add("mysql")
                elif connection["type"] == "sql-server":
                    services.add("sql-server")
                elif connection["type"] in ("kafka", "confluent-schema-registry"):
                    services.update(["kafka", "schema-registry", "zookeeper"])
                elif connection["type"] == "ssh-tunnel":
                    services.add("ssh-bastion-host")
                elif connection["type"] in ("aws-privatelink", "aws"):
                    pass  # can't run outside of cloud
                else:
                    raise ValueError(f"Unhandled connection type {connection['type']}")

    print(f"Required services for connections: {services}")

    c.up(
        "materialized",
        *services,
        Service("testdrive", idle=True),
    )
    print(f"Console available: http://127.0.0.1:{c.port('materialized', 6874)}")

    threads = []
    stop_event = threading.Event()
    stats: dict[str, Any] = {
        "queries": {"total": 0, "failed": 0, "slow": 0},
        "ingestions": {"total": 0, "failed": 0, "slow": 0},
    }
    if create_objects:
        start_time = time.time()
        run_create_objects_part_1(c, services, workload)
        if not early_initial_data:
            run_create_objects_part_2(c, services, workload)
        stats["object_creation"] = time.time() - start_time
    created_data = False
    try:
        if initial_data:
            print("Creating initial data")
            stats["initial_data"] = {"docker": [], "time": 0.0}
            stats_thread = PropagatingThread(
                target=docker_stats,
                name="docker-stats",
                args=(stats["initial_data"]["docker"], stop_event),
            )
            stats_thread.start()
            start_time = time.time()
            created_data = create_initial_data_external(
                c,
                workload,
                factor_initial_data,
                random.Random(random.randrange(SEED_RANGE)),
            )
        if early_initial_data:
            start_time = time.time()
            run_create_objects_part_2(c, services, workload)
            stats["object_creation"] += time.time() - start_time
        if initial_data:
            created_data = created_data or create_initial_data_requiring_mz(
                c,
                workload,
                factor_initial_data,
                random.Random(random.randrange(SEED_RANGE)),
            )
            stats["initial_data"]["time"] = time.time() - start_time
            if not created_data:
                del stats["initial_data"]
            while True:
                not_hydrated: list[str] = [
                    entry[0]
                    for entry in c.sql_query(
                        """
                    SELECT DISTINCT name
                        FROM (
                          SELECT o.name
                          FROM mz_objects o
                          JOIN mz_internal.mz_hydration_statuses h
                            ON o.id = h.object_id
                          WHERE NOT h.hydrated

                          UNION ALL

                          SELECT o.name
                          FROM mz_objects o
                          JOIN mz_internal.mz_compute_hydration_statuses h
                            ON o.id = h.object_id
                          WHERE NOT h.hydrated
                        ) x
                        ORDER BY 1;"""
                    )
                    if not entry[0].startswith("mz_")
                ]
                if not_hydrated:
                    print(f"Waiting to hydrate: {', '.join(not_hydrated)}")
                    time.sleep(1)
                else:
                    break
    finally:
        stop_event.set()
        stats_thread.join()
        stop_event.clear()
    if run_ingestions:
        print("Starting continuous ingestions")
        threads.extend(
            create_ingestions(
                c, workload, stop_event, factor_ingestions, verbose, stats["ingestions"]
            )
        )
    if run_queries and workload["queries"]:
        print("Starting continuous queries")
        stats["queries"]["timings"] = []
        stats["queries"]["errors"] = defaultdict(list)
        threads.append(
            PropagatingThread(
                target=continuous_queries,
                name="queries",
                args=(
                    c,
                    workload,
                    stop_event,
                    factor_queries,
                    verbose,
                    stats["queries"],
                    random.Random(random.randrange(SEED_RANGE)),
                ),
            )
        )
    if threads:
        stats["docker"] = []
        threads.append(
            PropagatingThread(
                target=docker_stats,
                name="docker-stats",
                args=(stats["docker"], stop_event),
            )
        )
        for thread in threads:
            thread.start()

        try:
            stop_event.wait(timeout=runtime)
        finally:
            stop_event.set()
            for thread in threads:
                thread.join()
            print_replay_stats(stats)
    else:
        print("No continuous ingestions or queries defined, skipping phase")

    return stats


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--factor-initial-data",
        type=float,
        default=1,
        help="scale factor for initial data generation",
    )
    parser.add_argument(
        "--factor-ingestions",
        type=float,
        default=1,
        help="scale factor for runtime data ingestion rate",
    )
    parser.add_argument(
        "--factor-queries",
        type=float,
        default=1,
        help="scale factor for runtime queries",
    )
    parser.add_argument(
        "--runtime",
        type=int,
        default=1200,
        help="runtime for continuous ingestion/query period, in seconds",
    )
    parser.add_argument(
        "--seed",
        metavar="SEED",
        type=str,
        default=str(int(time.time())),
        help="factor for initial data generation",
    )
    parser.add_argument(
        "files",
        nargs="*",
        default=["*.yml"],
        help="run against the specified files",
    )
    parser.add_argument("--verbose", action=argparse.BooleanOptionalAction)
    parser.add_argument(
        "--create-objects", action=argparse.BooleanOptionalAction, default=True
    )
    parser.add_argument(
        "--initial-data", action=argparse.BooleanOptionalAction, default=True
    )
    parser.add_argument(
        "--early-initial-data",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Run the initial data creation before creating sources in Materialize (except for webhooks)",
    )
    parser.add_argument(
        "--run-ingestions", action=argparse.BooleanOptionalAction, default=True
    )
    parser.add_argument(
        "--run-queries", action=argparse.BooleanOptionalAction, default=True
    )
    args = parser.parse_args()

    print(f"-- Random seed is {args.seed}")
    random.seed(args.seed)
    update_captured_workloads_repo()

    files_unsharded: list[pathlib.Path] = []
    for file in args.files:
        files_unsharded.extend(LOCATION.rglob(file))
    files: list[pathlib.Path] = buildkite.shard_list(
        sorted(files_unsharded),
        lambda file: str(file),
    )

    def run(file: pathlib.Path) -> None:
        with open(file) as f:
            workload = yaml.load(f, Loader=yaml.CSafeLoader)
        print_workload_stats(file, workload)
        test(
            c,
            workload,
            file,
            args.factor_initial_data,
            args.factor_ingestions,
            args.factor_queries,
            args.runtime,
            args.verbose,
            args.create_objects,
            args.initial_data,
            args.early_initial_data,
            args.run_ingestions,
            args.run_queries,
        )

    c.test_parts(files, run)


def workflow_benchmark(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--factor-initial-data",
        type=float,
        default=1,
        help="scale factor for initial data generation",
    )
    parser.add_argument(
        "--factor-ingestions",
        type=float,
        default=1,
        help="scale factor for runtime data ingestion rate",
    )
    parser.add_argument(
        "--factor-queries",
        type=float,
        default=1,
        help="scale factor for runtime queries",
    )
    parser.add_argument(
        "--runtime",
        type=int,
        default=1200,
        help="runtime for continuous ingestion/query period, in seconds",
    )
    parser.add_argument(
        "--seed",
        metavar="SEED",
        type=str,
        default=str(int(time.time())),
        help="factor for initial data generation",
    )
    parser.add_argument(
        "files",
        nargs="*",
        default=["*.yml"],
        help="run against the specified files",
    )
    parser.add_argument("--verbose", action=argparse.BooleanOptionalAction)
    parser.add_argument(
        "--compare-against",
        type=str,
        default=None,
        help="compare performance and errors against another Materialize tag",
    )
    parser.add_argument(
        "--early-initial-data",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Run the initial data creation before creating sources in Materialize (except for webhooks)",
    )
    args = parser.parse_args()

    print(f"-- Random seed is {args.seed}")
    update_captured_workloads_repo()

    files_unsharded: list[pathlib.Path] = []
    for file in args.files:
        files_unsharded.extend(LOCATION.rglob(file))
    files: list[pathlib.Path] = buildkite.shard_list(
        sorted(files_unsharded),
        lambda file: str(file),
    )
    c.test_parts(
        files,
        lambda file: benchmark(
            c,
            file,
            args.compare_against,
            args.factor_initial_data,
            args.factor_ingestions,
            args.factor_queries,
            args.runtime,
            args.verbose,
            args.seed,
            args.early_initial_data,
        ),
    )


def workflow_stats(c: Composition, parser: WorkflowArgumentParser) -> None:
    with c.override(Materialized(sanity_restart=False)):
        parser.add_argument(
            "files",
            nargs="*",
            default=["*.yml"],
            help="run against the specified files",
        )
        args = parser.parse_args()
        update_captured_workloads_repo()

        files: list[pathlib.Path] = []
        for file in args.files:
            files.extend(LOCATION.rglob(file))
        files.sort()

        for file in files:
            with open(file) as f:
                workload = yaml.load(f, Loader=yaml.CSafeLoader)
            print()
            print_workload_stats(file, workload)
        print()
