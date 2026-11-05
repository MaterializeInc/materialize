# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Reduce idle windows, retaining timing uncertainty and incomplete attribution."""

import argparse
import gzip
import json
from collections import defaultdict
from pathlib import Path

from prometheus_client.parser import text_string_to_metric_families

SHARD_COUNTERS = {
    "mz_persist_shard_diff_size_bytes",
    "mz_persist_shard_blob_gets",
    "mz_persist_shard_blob_sets",
    "mz_persist_shard_compaction_applied",
    "mz_persist_shard_gc_finished",
    "mz_persist_cmd_cas_mismatch_count",
}
POLLING_COUNTERS = {
    "mz_persist_cmd_fetch_upper_count",
    "mz_persist_state_update_state_empty_path",
    "mz_persist_state_update_state_fast_path",
    "mz_persist_state_update_state_noop_path",
    "mz_persist_state_update_state_slow_path",
}
OP_SUFFIXES = {
    "started_count",
    "succeeded_count",
    "failed_count",
    "seconds",
    "bytes_count",
}
TABLE_COUNTERS = {
    "seq_scan",
    "seq_tup_read",
    "idx_scan",
    "idx_tup_fetch",
    "n_tup_ins",
    "n_tup_upd",
    "n_tup_del",
    "n_tup_hot_upd",
    "vacuum_count",
    "autovacuum_count",
    "analyze_count",
    "autoanalyze_count",
    "heap_blks_read",
    "heap_blks_hit",
    "idx_blks_read",
    "idx_blks_hit",
    "toast_blks_read",
    "toast_blks_hit",
    "tidx_blks_read",
    "tidx_blks_hit",
}


def load(path):
    return json.loads(path.read_text())


def metrics(path):
    with gzip.open(path, "rt") as f:
        return {
            (
                (s.name.removesuffix("_total") if family.type == "counter" else s.name),
                tuple(sorted(s.labels.items())),
            ): s.value
            for family in text_string_to_metric_families(f.read())
            for s in family.samples
        }


def fields(text):
    return {k: int(v) for k, v in (line.split() for line in text.splitlines())}


def cpu(row):
    return row["cpu"]["user"] + row["cpu"]["system"]


def describe(key, **extra):
    return {"metric": key[0], "labels": dict(key[1]), **extra}


def operation_rates(before, after, prefix, elapsed, coverage):
    rates = defaultdict(dict)
    keys = {
        key
        for key in before.keys() | after.keys()
        if key[0].startswith(prefix) and key[0][len(prefix) :] in OP_SUFFIXES
    }
    for key in keys:
        if key not in before:
            coverage["added"].append(describe(key))
            continue
        if key not in after:
            coverage["removed"].append(describe(key))
            continue
        delta = after[key] - before[key]
        if delta < 0:
            raise ValueError(
                ("process-lifetime counter reset", key, before[key], after[key])
            )
        rates[dict(key[1]).get("op", "unlabeled")][key[0][len(prefix) :]] = (
            delta / elapsed
        )
    for values in rates.values():
        completed = values.get("succeeded_count", 0) + values.get("failed_count", 0)
        values["mean_latency_ms"] = (
            1000 * values["seconds"] / completed
            if completed and "seconds" in values
            else None
        )
    return dict(rates)


def pg_span(run, previous, label, before, after, category):
    if "timing" in before and "timing" in after:
        seconds = after["timing"][category]["end"] - before["timing"][category]["end"]
        source = "monotonic query completion"
    else:
        # The initial witness lacked query timestamps. Preserve that limitation
        # rather than substitute the CPU window or silently imply exact timing.
        seconds = (run / (label + "-postgres.json")).stat().st_mtime - (
            run / (previous + "-postgres.json")
        ).stat().st_mtime
        source = "original snapshot file mtime, approximate completion proxy"
    assert seconds > 0
    return {"seconds": seconds, "source": source}


def pg_rates(before, after, elapsed, keys):
    rates = {}
    for key in keys:
        if before.get(key) is None or after.get(key) is None:
            continue
        delta = float(after[key]) - float(before[key])
        if delta < 0:
            raise ValueError(
                ("Postgres counter decreased", key, before[key], after[key])
            )
        rates[key] = delta / elapsed
    return rates


def analyze(run):
    result = load(run / "result.json")
    measurement_status = result.get("measurement_status", result["status"])
    if measurement_status != "passed":
        # Older witnesses only marked a global status. A teardown-only failure
        # follows completed measurement/profile work, but remains a failed run.
        if "cleanup_error" in result and "error" not in result:
            measurement_status = "completed_before_cleanup_failure"
        else:
            raise RuntimeError(("measurements incomplete", run, result))
    samples = [
        json.loads(line) for line in (run / "usage.jsonl").read_text().splitlines()
    ]
    reports = []
    for window in range(1, result["arguments"]["windows"] + 1):
        label = f"steady-{window}"
        previous = f"steady-{window-1}"
        points = [s for s in samples if s["phase"] == label]
        span = points[-1]["time"] - points[0]["time"]
        assert span > 0
        first = {p["role"]: p for p in points[0]["processes"]}
        last = {p["role"]: p for p in points[-1]["processes"]}
        assert first.keys() == last.keys()
        start_t = load(run / (previous + "-metrics-timing.json"))
        end_t = load(run / (label + "-metrics-timing.json"))
        memory_start = {
            p["role"]: p["memory"] for p in load(run / (previous + "-memory.json"))
        }
        memory_end = {
            p["role"]: p["memory"] for p in load(run / (label + "-memory.json"))
        }
        roles = {}
        for role, initial in first.items():
            final = last[role]
            assert initial["pid"] == final["pid"]
            before = metrics(run / f"{previous}-{role}.prom.gz")
            after = metrics(run / f"{label}-{role}.prom.gz")
            minimum = end_t[role]["start"] - start_t[role]["end"]
            maximum = end_t[role]["end"] - start_t[role]["start"]
            elapsed = (minimum + maximum) / 2
            assert minimum > 0
            coverage = {"added": [], "removed": [], "reset_shard_counters": []}
            external = operation_rates(
                before, after, "mz_persist_external_", elapsed, coverage
            )
            oracle = operation_rates(before, after, "mz_ts_oracle_", elapsed, coverage)
            polling = {}
            for name in POLLING_COUNTERS:
                key = (name, ())
                if key in before and key in after:
                    delta = after[key] - before[key]
                    if delta < 0:
                        raise ValueError(("process-lifetime counter reset", key))
                    polling[name] = delta / elapsed
                elif key in before:
                    coverage["removed"].append(describe(key))
                elif key in after:
                    coverage["added"].append(describe(key))
            activity = []
            for key in before.keys() | after.keys():
                if key[0] not in SHARD_COUNTERS:
                    continue
                if key not in before:
                    coverage["added"].append(describe(key))
                    continue
                if key not in after:
                    coverage["removed"].append(describe(key))
                    continue
                delta = after[key] - before[key]
                if delta < 0:
                    coverage["reset_shard_counters"].append(
                        describe(key, before=before[key], after=after[key])
                    )
                    continue
                if delta:
                    activity.append(describe(key, per_second=delta / elapsed))
            c0 = fields(initial["cgroup"].get("cpu.stat", ""))
            c1 = fields(final["cgroup"].get("cpu.stat", ""))
            roles[role] = {
                "pid": initial["pid"],
                "cores": (cpu(final) - cpu(initial)) / span,
                "rss_mean_bytes": sum(
                    next(
                        p["memory"]["rss"] for p in s["processes"] if p["role"] == role
                    )
                    for s in points
                )
                / len(points),
                "rss_end_bytes": final["memory"]["rss"],
                "pss_start_bytes": memory_start[role]["pss"],
                "pss_end_bytes": memory_end[role]["pss"],
                "uss_end_bytes": memory_end[role]["uss"],
                "threads_end": final["threads"],
                "polling_counters_per_second": polling,
                "allocator_and_cache_gauges_end": [
                    describe(key, value=value)
                    for key, value in after.items()
                    if key[0].startswith(("jemalloc_", "mz_persist_blob_cache_size_"))
                ],
                "io_per_second": {
                    k: (final["io"][k] - initial["io"][k]) / span for k in initial["io"]
                },
                "cpu_throttled_usec": c1.get("throttled_usec", 0)
                - c0.get("throttled_usec", 0),
                "external_per_second": external,
                "oracle_per_second": oracle,
                "metric_elapsed_seconds": elapsed,
                "metric_elapsed_bounds_seconds": [minimum, maximum],
                "counter_coverage": coverage,
                "shard_activity": sorted(
                    activity, key=lambda r: r["per_second"], reverse=True
                ),
            }
        pg0 = fields(points[0]["postgres_cgroup"]["cpu.stat"])
        pg1 = fields(points[-1]["postgres_cgroup"]["cpu.stat"])
        pga = load(run / (previous + "-postgres.json"))
        pgb = load(run / (label + "-postgres.json"))
        for category in ["database", "wal"]:
            assert pga[category][0]["stats_reset"] == pgb[category][0]["stats_reset"], (
                "Postgres stats reset",
                category,
            )
        timing = {
            category: pg_span(run, previous, label, pga, pgb, category)
            for category in ["database", "wal", "tables", "table_io"]
        }
        database_keys = {
            k
            for k, v in pga["database"][0].items()
            if isinstance(v, (int, float)) and k not in ["datid", "numbackends"]
        }
        database = pg_rates(
            pga["database"][0],
            pgb["database"][0],
            timing["database"]["seconds"],
            database_keys,
        )
        wal = pg_rates(
            pga["wal"][0],
            pgb["wal"][0],
            timing["wal"]["seconds"],
            ["wal_records", "wal_fpi", "wal_bytes"],
        )
        tables = {}
        for category in ["tables", "table_io"]:
            rows0 = {(r["schemaname"], r["relname"]): r for r in pga[category]}
            rows1 = {(r["schemaname"], r["relname"]): r for r in pgb[category]}
            assert rows0.keys() == rows1.keys(), ("table inventory changed", category)
            tables[category] = {
                ".".join(key): pg_rates(
                    row,
                    rows1[key],
                    timing[category]["seconds"],
                    TABLE_COUNTERS & row.keys(),
                )
                for key, row in rows0.items()
            }
        reports.append(
            {
                "phase": label,
                "cpu_sample_seconds": span,
                "roles": roles,
                "total_materialize_cores": sum(r["cores"] for r in roles.values()),
                "total_materialize_pss_end_bytes": sum(
                    r["pss_end_bytes"] for r in roles.values()
                ),
                "postgres_cores": (pg1["usage_usec"] - pg0["usage_usec"]) / 1e6 / span,
                "postgres_cgroup_memory_end_bytes": int(
                    points[-1]["postgres_cgroup"]["memory.current"]
                ),
                "postgres_snapshot_timing": timing,
                "postgres_database_per_second": database,
                "postgres_wal_per_second": wal,
                "postgres_tables_per_second": tables,
                "minimum_available_memory_bytes": min(
                    s["available_memory"] for s in points
                ),
            }
        )
    output = {
        "run": str(run),
        "kind": result["kind"],
        "processes": load(run / "processes.json"),
        "windows": reports,
        "measurement_status": measurement_status,
        "overall_run_status": result["status"],
    }
    (run / "analysis.json").write_text(json.dumps(output, indent=2))
    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("runs", type=Path, nargs="+")
    for run in parser.parse_args().runs:
        for report in analyze(run)["windows"]:
            print(
                run.name,
                report["phase"],
                "cores",
                round(report["total_materialize_cores"], 3),
                "PSS MiB",
                round(report["total_materialize_pss_end_bytes"] / 1024**2, 1),
                "postgres cores",
                round(report["postgres_cores"], 3),
            )
            for role, r in report["roles"].items():
                print(
                    " ",
                    role,
                    round(r["cores"], 3),
                    "cores",
                    round(r["pss_end_bytes"] / 1024**2, 1),
                    "MiB PSS",
                )
