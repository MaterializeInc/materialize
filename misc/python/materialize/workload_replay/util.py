# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Utility functions for workload replay.
"""

from __future__ import annotations

import math
import os
import pathlib
import posixpath
import random
import re
import subprocess
import urllib.parse
from collections import Counter
from typing import Any

from materialize import MZ_ROOT, spawn, ui
from materialize.version_list import resolve_ancestor_image_tag
from materialize.workload_replay.config import LOCATION


def p(label: str, value: Any) -> None:
    """Print a formatted label-value pair."""
    if isinstance(value, str):
        s = value
    elif isinstance(value, float):
        s = f"{value:,.2f}"
    else:
        s = f"{value:,}"
    print(f"  {label:<{17}} {s:>{12}}")


def print_workload_stats(file: pathlib.Path, workload: dict[str, Any]) -> None:
    """Print statistics about a workload file."""
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
    """Resolve a tag name to an actual image tag."""
    if tag == "common-ancestor":
        # TODO: We probably will need overrides too
        return resolve_ancestor_image_tag({})
    return tag


def update_captured_workloads_repo() -> None:
    """Clone or update the captured-workloads repository."""
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
            github_token = os.environ.get(
                "GITHUB_CI_ISSUE_REFERENCE_CHECKER_TOKEN"
            ) or os.getenv("GITHUB_TOKEN")
            assert (
                github_token
            ), "GITHUB_CI_ISSUE_REFERENCE_CHECKER_TOKEN or GITHUB_TOKEN must be set in CI"
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


def get_paths(globs: list[str]) -> list[pathlib.Path]:
    """Get paths matching the given glob patterns."""
    paths = []
    for glob in globs:
        new_paths = list(LOCATION.rglob(glob))
        if not new_paths:
            known = "\n  ".join(
                [posixpath.relpath(file, LOCATION) for file in LOCATION.rglob("*.yml")]
            )
            raise ValueError(
                f'No workload files found matching "{glob}", known:\n  {known}'
            )
        paths.extend(new_paths)
    paths.sort()
    return paths


# Reference parsing helpers


def get_kafka_topic(source: dict[str, Any]) -> str:
    """Extract the Kafka topic name from a source's CREATE SQL."""
    match = re.search(
        r"TOPIC\s*=\s*'([^']+)'",
        source["create_sql"],
        re.IGNORECASE | re.DOTALL,
    )
    assert match, f"No topic found: {source['create_sql']}"
    return match.group(1)


def get_postgres_reference_db_schema_table(
    child: dict[str, Any]
) -> tuple[str, str, str]:
    """Extract database, schema, and table from a Postgres source child."""
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
    """Extract database and table from a MySQL source child."""
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
    """Extract database, schema, and table from a SQL Server source child."""
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
    """Convert a Materialize type to SQL Server type."""
    if typ == "timestamp without time zone":
        return "datetime"
    return typ


# Random generation helpers


def long_tail_rank(n: int, a: float, rng: random.Random) -> int:
    """Generate a rank with long-tail distribution."""
    x = int((rng.paretovariate(a) - 1.0) * 3) + 1
    return max(1, min(n, x))


def long_tail_int(lo: int, hi: int, rng: random.Random) -> int:
    """Generate an integer with long-tail distribution."""
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
    """Generate a float with long-tail distribution."""
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
    """Choose a value with bias towards the first few values."""
    if not values:
        raise ValueError("empty values")
    if rng.random() < hot_prob:
        return rng.choice(values[: min(8, len(values))])
    return rng.choice(values)


def long_tail_text(
    chars: str, max_len: int, hot_pool: list[str], rng: random.Random
) -> str:
    """Generate text with long-tail distribution on length."""
    if rng.random() < 0.90 and hot_pool:
        return rng.choice(hot_pool)

    length = min(max_len, max(1, long_tail_rank(n=max_len, a=1.3, rng=rng)))
    return "".join(rng.choice(chars) for _ in range(length))
