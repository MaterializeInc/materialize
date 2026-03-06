# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Performance tests for standing queries.

Measures EXECUTE STANDING QUERY latency and throughput under varying
concurrency levels using dbbench.
"""

import re
import shlex

from materialize.mzcompose.composition import (
    Composition,
    WorkflowArgumentParser,
)
from materialize.mzcompose.service import Service as MzComposeService
from materialize.mzcompose.services.materialized import Materialized

SERVICES = [
    Materialized(propagate_crashes=True),
    MzComposeService(
        "dbbench",
        {"mzbuild": "dbbench"},
    ),
]

NUM_ROWS = 100_000


def setup_standing_query(c: Composition) -> None:
    """Create a table with test data and a standing query."""
    c.sql(
        """
        ALTER SYSTEM SET max_result_size = '10GB';
        ALTER SYSTEM SET default_timestamp_interval = '100ms';
        """,
        port=6877,
        user="mz_system",
    )
    c.sql(
        f"""
        DROP STANDING QUERY IF EXISTS orders_by_customer;
        DROP TABLE IF EXISTS orders CASCADE;
        CREATE TABLE orders (id INT, customer_id INT, amount INT);
        INSERT INTO orders
            SELECT g, g % 100, g * 10
            FROM generate_series(1, {NUM_ROWS}) AS g;
        CREATE STANDING QUERY orders_by_customer (cid INT)
            AS SELECT id, customer_id, amount
            FROM orders
            WHERE customer_id = cid;
        """,
        port=6875,
    )
    # Wait for the standing query dataflow to hydrate.
    c.sql("SELECT 1", port=6875)


def run_dbbench(
    c: Composition,
    *,
    name: str,
    query: str,
    duration: str = "30s",
    concurrency: int | None = None,
    rate: float | None = None,
    batch_size: int | None = None,
) -> dict:
    """Run dbbench and return parsed results.

    Returns a dict with keys: qps, tps, latency_mean, latency_ci.
    """
    lines: list[str] = [f"duration={duration}", ""]

    lines.append("[loadtest]")
    # Escape newlines for INI format.
    lines.append(f"query={query.replace(chr(10), ' ').strip()}")
    if concurrency is not None:
        lines.append(f"concurrency={concurrency}")
    if rate is not None:
        lines.append(f"rate={rate}")
    if batch_size is not None:
        lines.append(f"batch-size={batch_size}")

    ini_text = "\n".join(lines) + "\n"

    flags = [
        "-driver",
        "postgres",
        "-host",
        "materialized",
        "-port",
        "6875",
        "-username",
        "materialize",
        "-database",
        "materialize",
    ]
    quoted_flags = " ".join(shlex.quote(x) for x in flags)
    script = (
        'tmp="$(mktemp -t dbbench.XXXXXX)"; '
        'cat > "$tmp"; '
        f'exec dbbench {quoted_flags} -intermediate-stats=false "$tmp"'
    )

    print(f"--- dbbench: {name}")
    result = c.run(
        "dbbench",
        "-lc",
        script,
        entrypoint="sh",
        rm=True,
        capture_and_print=True,
        stdin=ini_text,
    )

    combined = f"{result.stderr or ''}\n{result.stdout or ''}".strip()
    print(combined)

    parsed = {}

    # Parse QPS
    qps_matches = re.findall(r"([0-9]+(?:\.[0-9]+)?)\s*QPS", combined)
    if qps_matches:
        parsed["qps"] = float(qps_matches[-1])

    # Parse TPS
    tps_matches = re.findall(r"([0-9]+(?:\.[0-9]+)?)\s*TPS", combined)
    if tps_matches:
        parsed["tps"] = float(tps_matches[-1])

    # Parse latency: "latency 796.907µs±63.671µs"
    lat_matches = re.findall(
        r"latency\s+([0-9.]+(?:µs|ms|s|ns))±([0-9.]+(?:µs|ms|s|ns))",
        combined,
    )
    if lat_matches:
        parsed["latency_mean"] = lat_matches[-1][0]
        parsed["latency_ci"] = lat_matches[-1][1]

    return parsed


def parse_duration_ms(s: str) -> float:
    """Parse a Go-style duration string to milliseconds."""
    if s.endswith("µs"):
        return float(s[:-2]) / 1000.0
    elif s.endswith("ns"):
        return float(s[:-2]) / 1_000_000.0
    elif s.endswith("ms"):
        return float(s[:-2])
    elif s.endswith("s"):
        return float(s[:-1]) * 1000.0
    raise ValueError(f"cannot parse duration: {s}")


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Run all standing query performance workflows."""
    for workflow in ["throughput", "target-qps", "target-qps-single-row"]:
        with c.test_case(workflow):
            c.workflow(workflow)


def workflow_throughput(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Measure max throughput at increasing concurrency levels."""
    c.up("materialized")

    setup_standing_query(c)

    concurrency_levels = [1, 4, 16, 64]

    for conc in concurrency_levels:
        stats = run_dbbench(
            c,
            name=f"throughput_c{conc}",
            query="EXECUTE STANDING QUERY orders_by_customer (42)",
            duration="15s",
            concurrency=conc,
        )
        qps = stats.get("qps", 0)
        latency = stats.get("latency_mean", "N/A")
        print(f"  concurrency={conc}: {qps:.1f} QPS, latency={latency}")

    c.kill("materialized")
    c.rm("materialized")
    c.rm_volumes("mzdata")


def workflow_target_qps(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Verify standing queries meet a target QPS with acceptable latency.

    Uses dbbench's `rate` mode to send requests at a fixed rate and checks
    that mean latency stays below a threshold. This validates that the system
    can sustain the target throughput without queuing.
    """
    c.up("materialized")

    setup_standing_query(c)

    # Target QPS levels with latency budgets (ms).
    targets = [
        {"rate": 256, "max_latency_ms": 500},
        {"rate": 512, "max_latency_ms": 500},
        {"rate": 1024, "max_latency_ms": 1000},
        {"rate": 2048, "max_latency_ms": 1000},
        {"rate": 4096, "max_latency_ms": 2000},
    ]

    for target in targets:
        rate = target["rate"]
        max_lat = target["max_latency_ms"]

        stats = run_dbbench(
            c,
            name=f"target_qps_{rate}",
            query="EXECUTE STANDING QUERY orders_by_customer (42)",
            duration="20s",
            rate=float(rate),
        )

        latency_str = stats.get("latency_mean")
        if latency_str is None:
            raise RuntimeError(f"rate={rate}: dbbench did not report latency")

        latency_ms = parse_duration_ms(latency_str)
        qps = stats.get("qps", 0)
        print(
            f"  rate={rate}: achieved {qps:.1f} QPS, latency={latency_str} ({latency_ms:.1f}ms)"
        )

        if latency_ms > max_lat:
            raise RuntimeError(
                f"rate={rate}: latency {latency_ms:.1f}ms exceeds budget {max_lat}ms"
            )

    c.kill("materialized")
    c.rm("materialized")
    c.rm_volumes("mzdata")


def workflow_target_qps_single_row(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """Like target-qps but each execute returns exactly 1 row (filter on unique id)."""
    c.up("materialized")

    c.sql(
        """
        ALTER SYSTEM SET max_result_size = '10GB';
        ALTER SYSTEM SET default_timestamp_interval = '100ms';
        """,
        port=6877,
        user="mz_system",
    )
    c.sql(
        f"""
        DROP STANDING QUERY IF EXISTS order_by_id;
        DROP TABLE IF EXISTS orders CASCADE;
        CREATE TABLE orders (id INT, customer_id INT, amount INT);
        INSERT INTO orders
            SELECT g, g % 100, g * 10
            FROM generate_series(1, {NUM_ROWS}) AS g;
        CREATE STANDING QUERY order_by_id (oid INT)
            AS SELECT id, customer_id, amount
            FROM orders
            WHERE id = oid;
        """,
        port=6875,
    )
    c.sql("SELECT 1", port=6875)

    # Target QPS levels with latency budgets (ms).
    targets = [
        {"rate": 256, "max_latency_ms": 500},
        {"rate": 512, "max_latency_ms": 500},
        {"rate": 1024, "max_latency_ms": 1000},
        {"rate": 2048, "max_latency_ms": 1000},
        {"rate": 4096, "max_latency_ms": 2000},
    ]

    for target in targets:
        rate = target["rate"]
        max_lat = target["max_latency_ms"]

        stats = run_dbbench(
            c,
            name=f"target_qps_single_row_{rate}",
            query="EXECUTE STANDING QUERY order_by_id (42)",
            duration="20s",
            rate=float(rate),
        )

        latency_str = stats.get("latency_mean")
        if latency_str is None:
            raise RuntimeError(f"rate={rate}: dbbench did not report latency")

        latency_ms = parse_duration_ms(latency_str)
        qps = stats.get("qps", 0)
        print(
            f"  rate={rate}: achieved {qps:.1f} QPS, latency={latency_str} ({latency_ms:.1f}ms)"
        )

        if latency_ms > max_lat:
            raise RuntimeError(
                f"rate={rate}: latency {latency_ms:.1f}ms exceeds budget {max_lat}ms"
            )

    c.kill("materialized")
    c.rm("materialized")
    c.rm_volumes("mzdata")
