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

Measures EXECUTE STANDING QUERY against the equivalent index SELECT. The
open-loop workflows offer fixed request rates with pgbench and report the
latency distribution. The throughput workflows run a closed loop with dbbench
at fixed concurrency.
"""

import re
import shlex

from materialize.mzcompose.composition import (
    Composition,
    WorkflowArgumentParser,
)
from materialize.mzcompose.service import Service as MzComposeService
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.postgres import Postgres

SERVICES = [
    Materialized(propagate_crashes=True),
    MzComposeService(
        "dbbench",
        {"mzbuild": "dbbench"},
    ),
    # Only its pgbench is used, as the open-loop load generator.
    Postgres(),
]

NUM_ROWS = 100_000


def setup(c: Composition) -> None:
    """Create a table with test data, indexes, and standing queries."""
    c.sql(
        """
        ALTER SYSTEM SET max_result_size = '10GB';
        ALTER SYSTEM SET max_connections = 65536;
        ALTER SYSTEM SET enable_standing_queries = true;
        """,
        port=6877,
        user="mz_system",
    )
    # Strictly serializable executions of a standing query wait for the param
    # shard to reach the oracle's read timestamp, about a second. Measure both
    # standing queries and their index SELECT baselines as serializable.
    c.sql(
        "ALTER ROLE materialize SET transaction_isolation = 'serializable'",
        port=6877,
        user="mz_system",
    )
    c.sql(
        f"""
        DROP STANDING QUERY IF EXISTS orders_by_customer;
        DROP STANDING QUERY IF EXISTS order_by_id;
        DROP TABLE IF EXISTS orders CASCADE;
        CREATE TABLE orders (id INT, customer_id INT, amount INT);
        INSERT INTO orders
            SELECT g, g % 100, g * 10
            FROM generate_series(1, {NUM_ROWS}) AS g;
        CREATE INDEX orders_by_customer_idx ON orders (customer_id);
        CREATE INDEX orders_by_id_idx ON orders (id);
        """,
        port=6875,
    )
    recreate_standing_queries(c)


def recreate_standing_queries(c: Composition) -> None:
    """Drop and recreate standing queries to reset subscribe state.

    Standing query subscribes accumulate arrangement state over time.
    Recreating between test runs prevents max_result_size errors.
    """
    c.sql(
        """
        DROP STANDING QUERY IF EXISTS orders_by_customer;
        DROP STANDING QUERY IF EXISTS order_by_id;
        CREATE STANDING QUERY orders_by_customer (cid INT)
            AS SELECT id, customer_id, amount
            FROM orders
            WHERE customer_id = cid;
        CREATE STANDING QUERY order_by_id (oid INT)
            AS SELECT id, customer_id, amount
            FROM orders
            WHERE id = oid;
        """,
        port=6875,
    )
    # Wait for the standing query dataflows to hydrate.
    c.sql("SELECT 1", port=6875)


# Distinct parameter values the load cycles through. The standing query's join
# partitions requests by their parameter, so a single value would send every
# request to one worker. Each value selects 1000 rows of `orders_by_customer`
# and one row of `order_by_id`.
KEYS = list(range(1, 17))

# One line of dbbench's final per-job summary, for example
# `job_3: 30720 transactions (255.985 TPS), latency 11.45ms±217.34µs; ...`.
SUMMARY_RE = re.compile(
    r"(\w+): (\d+) transactions \(([0-9.]+) TPS\), "
    r"latency ([0-9.]+(?:µs|ms|s|ns))±([0-9.]+(?:µs|ms|s|ns))"
)


def run_dbbench(
    c: Composition,
    *,
    name: str,
    query: str,
    keys: list[int] = KEYS,
    duration: str = "120s",
    concurrency: int | None = None,
    rate: float | None = None,
    batch_size: int | None = None,
) -> dict:
    """Run dbbench and return parsed results.

    `query` is a template with a `{key}` placeholder. Each key gets its own
    dbbench job, and the jobs split `concurrency` and `rate` between them.
    Returns a dict with keys: qps, tps, latency_mean, summed or averaged
    over the jobs weighted by their transactions.
    """
    num_jobs = min(len(keys), concurrency) if concurrency is not None else len(keys)
    lines: list[str] = [f"duration={duration}", ""]
    for i, key in enumerate(keys[:num_jobs]):
        lines.append(f"[job_{i}]")
        # Escape newlines for INI format.
        job_query = query.format(key=key).replace(chr(10), " ").strip()
        lines.append(f"query={job_query}")
        if concurrency is not None:
            job_concurrency = concurrency // num_jobs + (
                1 if i < concurrency % num_jobs else 0
            )
            lines.append(f"concurrency={job_concurrency}")
        if rate is not None:
            lines.append(f"rate={rate / num_jobs}")
        if batch_size is not None:
            lines.append(f"batch-size={batch_size}")
        lines.append("")

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

    # dbbench can print a job's summary more than once, so keep one per job.
    jobs = {}
    for job, transactions, tps, latency, _ci in SUMMARY_RE.findall(combined):
        jobs[job] = (int(transactions), float(tps), parse_duration_ms(latency))

    parsed = {}
    if jobs:
        total = sum(transactions for transactions, _, _ in jobs.values())
        tps = sum(tps for _, tps, _ in jobs.values())
        # Each transaction runs one query.
        parsed["tps"] = tps
        parsed["qps"] = tps
        if total > 0:
            mean_ms = (
                sum(transactions * ms for transactions, _, ms in jobs.values()) / total
            )
            parsed["latency_mean"] = f"{mean_ms:.3f}ms"

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
    for name in c.workflows:
        if name == "default":
            continue

        with c.test_case(name):
            c.workflow(name)


def workflow_throughput(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Measure max throughput at increasing concurrency levels."""
    c.up("materialized")
    setup(c)

    concurrency_levels = [1, 4, 8, 16, 32, 64, 128, 256, 512, 1024]

    for conc in concurrency_levels:
        recreate_standing_queries(c)
        stats = run_dbbench(
            c,
            name=f"standing_query_c{conc}",
            query="EXECUTE STANDING QUERY orders_by_customer ({key})",
            concurrency=conc,
        )
        qps = stats.get("qps", 0)
        latency = stats.get("latency_mean", "N/A")
        print(f"  standing_query concurrency={conc}: {qps:.1f} QPS, latency={latency}")

    for conc in concurrency_levels:
        stats = run_dbbench(
            c,
            name=f"index_select_c{conc}",
            query="SELECT id, customer_id, amount FROM orders WHERE customer_id = {key}",
            concurrency=conc,
        )
        qps = stats.get("qps", 0)
        latency = stats.get("latency_mean", "N/A")
        print(f"  index_select concurrency={conc}: {qps:.1f} QPS, latency={latency}")

    c.kill("materialized")
    c.rm("materialized")
    c.rm_volumes("mzdata")


def workflow_throughput_single_row(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """Measure max throughput at increasing concurrency (1 row per execute)."""
    c.up("materialized")
    setup(c)

    concurrency_levels = [1, 4, 8, 16, 32, 64, 128, 256, 512, 1024]

    for conc in concurrency_levels:
        recreate_standing_queries(c)
        stats = run_dbbench(
            c,
            name=f"standing_query_single_row_c{conc}",
            query="EXECUTE STANDING QUERY order_by_id ({key})",
            concurrency=conc,
        )
        qps = stats.get("qps", 0)
        latency = stats.get("latency_mean", "N/A")
        print(f"  standing_query concurrency={conc}: {qps:.1f} QPS, latency={latency}")

    for conc in concurrency_levels:
        stats = run_dbbench(
            c,
            name=f"index_select_single_row_c{conc}",
            query="SELECT id, customer_id, amount FROM orders WHERE id = {key}",
            concurrency=conc,
        )
        qps = stats.get("qps", 0)
        latency = stats.get("latency_mean", "N/A")
        print(f"  index_select concurrency={conc}: {qps:.1f} QPS, latency={latency}")

    c.kill("materialized")
    c.rm("materialized")
    c.rm_volumes("mzdata")


# Latency quantiles the open-loop workflows report, besides the maximum.
QUANTILES = [0.5, 0.9, 0.99, 0.999, 0.9999]

# Seconds at the start of each open-loop run whose transactions are dropped as
# warmup, and the run's total duration.
WARMUP_SECONDS = 5
OPEN_LOOP_SECONDS = 60

# Connections pgbench spreads the offered load over. Requests that find every
# connection busy wait for one, and that wait counts towards their latency,
# so the pool bounds concurrency without hiding queueing.
OPEN_LOOP_CONNECTIONS = 512


def run_pgbench_open_loop(c: Composition, *, name: str, script: str, rate: int) -> dict:
    """Offer `rate` transactions per second of `script` and return the
    achieved rate and the latency distribution in milliseconds.

    Latency is measured from each transaction's scheduled start, so time a
    request waits because earlier ones are slow counts, which a closed loop
    hides.
    """
    print(f"--- pgbench: {name}")
    quantile_args = " ".join(str(q) for q in QUANTILES)
    # The per-transaction log's columns are `client_id transaction_no time
    # script_no time_epoch time_us schedule_lag`, with `time` measured from
    # the actual start, so the latency from the scheduled start is
    # `time + schedule_lag`, in microseconds.
    shell = f"""
set -eu
dir="$(mktemp -d)"
cd "$dir"
cat > script.sql
pgbench -h materialized -p 6875 -U materialize -n -M prepared \\
    -c {OPEN_LOOP_CONNECTIONS} -j 8 -R {rate} -T {OPEN_LOOP_SECONDS} \\
    --log --log-prefix=txn -f script.sql materialize > summary.txt 2>&1 || {{
    cat summary.txt
    exit 1
}}
grep -E '^(tps|number of failed transactions|latency average)' summary.txt
start="$(cat txn.* | awk 'NR == 1 || $5 < min {{ min = $5 }} END {{ print min }}')"
cat txn.* \\
    | awk -v start="$start" -v warmup={WARMUP_SECONDS} '$5 >= start + warmup {{ print $3 + $7 }}' \\
    | sort -n \\
    | awk -v qs="{quantile_args}" '
        {{ v[NR] = $1 }}
        END {{
            n = split(qs, q, " ")
            printf "samples %d\\n", NR
            for (i = 1; i <= n; i++) {{
                idx = int(q[i] * NR)
                if (idx < 1) idx = 1
                printf "q%s %.3f\\n", q[i], v[idx] / 1000
            }}
            printf "max %.3f\\n", v[NR] / 1000
        }}'
"""
    result = c.exec(
        "postgres",
        "bash",
        "-c",
        shell,
        capture=True,
        stdin=script,
        check=False,
    )
    output = result.stdout or ""
    print(output)
    if result.returncode != 0:
        raise RuntimeError(f"pgbench failed for {name}")

    parsed: dict = {"quantiles": {}}
    for line in output.splitlines():
        if line.startswith("tps = "):
            parsed["achieved"] = float(line.split()[2])
        elif line.startswith("number of failed transactions"):
            parsed["failed"] = int(line.split(":")[1].split()[0])
        elif line.startswith("samples "):
            parsed["samples"] = int(line.split()[1])
        elif line.startswith("q"):
            q, ms = line[1:].split()
            parsed["quantiles"][float(q)] = float(ms)
        elif line.startswith("max "):
            parsed["max"] = float(line.split()[1])
    return parsed


def open_loop(
    c: Composition, *, label: str, rates: list[int], variants: list[tuple[str, str]]
) -> None:
    """Run each `(name, script)` variant at every offered rate and print a
    latency table per variant."""
    c.up("materialized", "postgres")
    setup(c)

    for name, script in variants:
        rows = []
        for rate in rates:
            recreate_standing_queries(c)
            stats = run_pgbench_open_loop(
                c, name=f"{label}_{name}_{rate}", script=script, rate=rate
            )
            if stats.get("failed", 0) > 0:
                raise RuntimeError(
                    f"{name} at {rate}/s: {stats['failed']} transactions failed"
                )
            rows.append((rate, stats))

        header = " ".join(f"p{q * 100:g}".rjust(9) for q in QUANTILES)
        print(f"  {label} {name}: offered achieved {header}       max (ms)")
        for rate, stats in rows:
            quantiles = " ".join(
                f"{stats['quantiles'].get(q, float('nan')):9.2f}" for q in QUANTILES
            )
            print(
                f"  {label} {name}: {rate:7d} {stats.get('achieved', 0):8.0f} "
                f"{quantiles} {stats.get('max', float('nan')):9.2f}"
            )

    c.kill("materialized")
    c.rm("materialized")
    c.rm_volumes("mzdata")


def workflow_open_loop(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Latency distribution of 1000-row lookups at fixed offered rates."""
    key = f"\\set key random(1, {len(KEYS)})\n"
    open_loop(
        c,
        label="open_loop",
        rates=[250, 500, 1000, 2000, 4000],
        variants=[
            (
                "standing_query",
                key + "EXECUTE STANDING QUERY orders_by_customer (:key);\n",
            ),
            (
                "index_select",
                key
                + "SELECT id, customer_id, amount FROM orders WHERE customer_id = :key;\n",
            ),
        ],
    )


def workflow_open_loop_single_row(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """Latency distribution of single-row lookups at fixed offered rates."""
    key = f"\\set key random(1, {NUM_ROWS})\n"
    open_loop(
        c,
        label="open_loop_single_row",
        rates=[1000, 2000, 4000, 8000, 16000, 24000, 32000],
        variants=[
            (
                "standing_query",
                key + "EXECUTE STANDING QUERY order_by_id (:key);\n",
            ),
            (
                "index_select",
                key + "SELECT id, customer_id, amount FROM orders WHERE id = :key;\n",
            ),
        ],
    )
