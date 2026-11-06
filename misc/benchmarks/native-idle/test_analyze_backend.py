# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Contracts for the diagnostic backend ledger and scrape reconciliation."""

import contextlib
import datetime
import gzip
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from analyze_backend import (
    analyze,
    counters,
    deltas,
    parse,
    reconcile,
    resolve,
    summarize,
    totals,
)

ID = "00000000-0000-0000-0000-000000000001"
SHARD = "s" + ID
READER = "r" + ID
WRITER = "w" + ID
BATCH = f"{SHARD}/n0026000/p{ID}"
ROLLUP = f"{SHARD}/v7/r{ID}"
EPOCH = 1700000000


def log(
    fields,
    context="",
    record="BACKEND_IO",
    role="cluster-s2-replica-s2-gen-0",
    time=EPOCH,
):
    stamp = (
        datetime.datetime.fromtimestamp(time, datetime.UTC)
        .isoformat()
        .replace("+00:00", "Z")
    )
    return f"{role + ': ' if role else ''}{stamp}  INFO {context}mz_persist_client::backend_audit: {record} {fields}\n"


def operation(op="blob_get", outcome="ok", size=10, key=BATCH, **kwargs):
    return log(f'op="{op}" key="{key}" outcome="{outcome}" bytes={size}', **kwargs)


def mapping(purpose, reader=READER, shard=SHARD, **kwargs):
    return log(
        f"reader={reader} shard={shard} purpose={json.dumps(purpose)}",
        record="BACKEND_READER",
        **kwargs,
    )


def events(*lines):
    return resolve([parse(line) for line in lines])


def metrics(path, values, total=False):
    with gzip.open(path, "wt") as stream:
        for (op, suffix), number in values.items():
            stream.write(
                f'mz_persist_external_{suffix}{"_total" if total else ""}{{op="{op}"}} {number}\n'
            )


def counts(success, failed, size, op="blob_get"):
    return {
        (op, "succeeded_count"): success,
        (op, "failed_count"): failed,
        (op, "bytes_count"): size,
    }


class Parsing(unittest.TestCase):
    def test_append_context_escaped_purpose_and_actual_attempt(self):
        purpose = 'commit txns {quoted "owner"} \\ path\n reader=fake audit_gc{shard=evil} mz_persist_client::backend_audit: BACKEND_IO'
        context = (
            f"audit_append{{writer={WRITER} purpose=Some({json.dumps(purpose)}) "
            "updates=0 lower=Antichain { elements: [1] } upper=Antichain { elements: [2] }}:"
            'audit_cas{command="compare_and_append" payload_len=123}:'
            f'consensus::compare_and_set{{shard="{SHARD}" size_bytes=123}}: '
        )
        line = log(
            f'op="consensus_cas" key="{SHARD}" seqno=8 outcome="mismatch" bytes=0 attempted_bytes=123',
            context=context,
        )
        event = events(line)[0]
        self.assertEqual(event["role"], "replica-s2-s2")
        self.assertEqual(event["process"], "cluster-s2-replica-s2-gen-0")
        self.assertEqual(event["purpose"], purpose)
        self.assertEqual(event["writer"], WRITER)
        self.assertEqual(event["command"], "compare_and_append")
        self.assertEqual(event["updates"], 0)
        self.assertEqual(event["work_class"], "append empty")
        self.assertEqual(event["append_kind"], "progress")
        heartbeat = events(
            line.replace(
                "upper=Antichain { elements: [2] }", "upper=Antichain { elements: [1] }"
            )
        )[0]
        self.assertEqual(heartbeat["append_kind"], "heartbeat")
        self.assertEqual(event["context"], context)
        self.assertEqual(event["seqno"], 8)
        self.assertEqual(event["attempted_bytes"], 123)
        self.assertEqual(event["conflicts"], {})
        data = events(line.replace("updates=0", "updates=42"))[0]
        self.assertEqual(data["command_category"], "append data")
        self.assertEqual(data["updates"], 42)
        self.assertEqual(data["append_kind"], "data")
        unknown = events(line.replace("updates=0 ", ""))[0]
        self.assertEqual(unknown["command_category"], "append unknown")
        self.assertIsNone(unknown["updates"])

    def test_rust_debug_unicode_and_unknown_fields_survive(self):
        context = (
            r'audit_acquire{purpose="a\u{1f600}\0\\u{41}" custom=Some({x: [1, 2]})}: '
        )
        event = events(operation(context=context))[0]
        self.assertEqual(event["purpose"], "a\U0001f600\0\\u{41}")
        self.assertEqual(event["spans"][0]["fields"][1]["raw"], "Some({x: [1, 2]})")
        self.assertEqual(event["work_class"], "acquisition")
        purpose = 'snapshot \U0001f600 {"quoted"}'
        event = events(
            operation(context=f"audit_acquire{{purpose={json.dumps(purpose)}}}: ")
        )[0]
        self.assertEqual(event["purpose"], purpose)
        self.assertIsNone(parse("unrelated BACKEND_IO text"))
        with self.assertRaisesRegex(ValueError, "truncated"):
            parse(operation(context='audit_acquire{purpose="unfinished}: '))

    def test_work_is_separate_from_key_and_implementation(self):
        cases = [
            ("audit_gc:audit_rollup_get: ", ROLLUP, "gc", "rollup"),
            (
                "audit_compaction:audit_part_set{payload_len=10}: ",
                BATCH,
                "compaction",
                "batch",
            ),
            (
                f"audit_snapshot_cursor{{reader={READER}}}:audit_prefetch: ",
                BATCH,
                "snapshot cursor",
                "batch",
            ),
            ("audit_prefetch:compaction::prefetch: ", BATCH, "unknown", "batch"),
            ("audit_consolidate: ", BATCH, "compaction", "batch"),
            (
                "audit_row_fetch{filter=Snapshot { as_of: Antichain { elements: [1] } }}: ",
                BATCH,
                "row fetch",
                "batch",
            ),
            ("audit_upload:audit_part_set{payload_len=10}: ", BATCH, "upload", "batch"),
            (
                "audit_compaction:audit_compaction_discard: ",
                BATCH,
                "compaction cleanup",
                "batch",
            ),
            (
                "audit_rollup_maintenance:audit_rollup_get: ",
                ROLLUP,
                "rollup maintenance",
                "rollup",
            ),
            ("some_new_caller: ", "unrecognized/key", "unknown", "unknown"),
            ("", "LIVENESS", "liveness", "liveness"),
        ]
        for context, key, work, kind in cases:
            with self.subTest(context=context):
                event = events(operation(context=context, key=key))[0]
                self.assertEqual((event["work_class"], event["key_kind"]), (work, kind))
                self.assertEqual(event["key"], key)
                self.assertIsNone(event["owner"])
        modern = events(operation())[0]
        self.assertEqual(modern["key_version"], "0026000")
        self.assertIsNone(modern["writer"])
        self.assertIsNone(modern["key_writer"])
        old = events(operation(key=f"{SHARD}/{WRITER}/p{ID}"))[0]
        self.assertEqual(old["key_writer"], WRITER)
        self.assertIsNone(old["writer"])  # Key producer is not this operation's owner.

    def test_global_reader_join_and_explicit_conflicts(self):
        fetch = operation(
            context=f"audit_source_fetch{{reader={READER} filter=Listen {{ lower: [1] }} transient=false}}: "
        )
        purpose = 'shard_source(test {"quoted"}) pid=123 worker=0 operator=4'
        # Mapping can be emitted later in this file and in another process.
        event = events(fetch, mapping(purpose, role="environmentd"))[0]
        self.assertEqual(event["reader_resolution"], "resolved")
        self.assertEqual(event["purpose"], purpose)
        self.assertEqual(event["reader_mappings"][0]["role"], "environmentd")
        self.assertEqual(event["work_class"], "source fetch")
        duplicate = events(
            fetch, mapping(purpose), mapping(purpose, role="environmentd")
        )[0]
        self.assertEqual(duplicate["reader_resolution"], "resolved")
        ambiguous = events(
            fetch, mapping(purpose), mapping("another owner", role="environmentd")
        )[0]
        self.assertEqual(ambiguous["reader_resolution"], "ambiguous")
        self.assertIsNone(ambiguous["owner"])
        self.assertIn("purpose", ambiguous["conflicts"])
        mismatch = events(
            fetch, mapping(purpose, shard="s00000000-0000-0000-0000-000000000002")
        )[0]
        self.assertEqual(mismatch["reader_resolution"], "shard mismatch")
        self.assertIsNone(mismatch["purpose"])
        reused = events(
            fetch,
            mapping(purpose),
            mapping("other shard", shard="s00000000-0000-0000-0000-000000000002"),
        )[0]
        self.assertEqual(reused["reader_resolution"], "resolved")
        self.assertEqual(reused["purpose"], purpose)
        self.assertEqual(len(reused["reader_mappings"]), 1)
        unknown = events(fetch)[0]
        self.assertEqual(unknown["reader_resolution"], "unknown")
        self.assertIsNone(unknown["purpose"])
        context = f'audit_acquire{{purpose="outer"}}:audit_reader{{reader={READER}}}:audit_cas{{command="downgrade_since"}}: '
        conflict = events(
            operation(
                op="consensus_cas", key=SHARD, outcome="committed", context=context
            ),
            mapping(purpose),
        )[0]
        self.assertIsNone(conflict["purpose"])
        self.assertEqual(conflict["owner_kind"], "conflict")
        self.assertEqual(conflict["command_category"], "reader maintenance")

    def test_bounded_groups_and_context_conflicts(self):
        rows = []
        for i in range(20):
            rows.extend(
                events(
                    operation(
                        context=f'audit_source_fetch{{reader=r{i} purpose="shard_source(x) worker={i}"}}: '
                    )
                )
            )
        groups = summarize(rows)
        self.assertEqual(len(groups), 1)
        self.assertEqual(groups[0]["events"], 20)
        self.assertEqual(groups[0]["owner_kind"], "shard_source")
        self.assertIn("context", groups[0]["example"])
        other_shard = "s00000000-0000-0000-0000-000000000002"
        other = events(operation(key=BATCH.replace(SHARD, other_shard)))
        by_shard = summarize([rows[0], *other])
        self.assertEqual({g["shard"] for g in by_shard}, {SHARD, other_shard})
        self.assertEqual(sum(g["events"] for g in by_shard), 2)
        event = events(
            operation(
                context="audit_source_fetch{reader=r1}:audit_row_fetch{reader=r2}: "
            )
        )[0]
        self.assertIsNone(event["reader"])
        self.assertIn("reader", event["conflicts"])
        self.assertEqual(event["reader_resolution"], "ambiguous")


class Reconciliation(unittest.TestCase):
    def test_cas_mismatch_and_blob_missing_are_successes_not_bytes(self):
        rows = events(
            *[
                log(
                    f'op="consensus_cas" key="{SHARD}" outcome="{outcome}" bytes={size} attempted_bytes=100'
                )
                for outcome, size in [("committed", 100), ("mismatch", 0), ("error", 0)]
            ]
        )
        observed = totals(rows)
        self.assertEqual(
            (
                observed["succeeded_count"],
                observed["failed_count"],
                observed["bytes_count"],
            ),
            (2, 1, 100),
        )
        self.assertEqual(observed["attempted_bytes"], 300)
        report, _ = reconcile(
            rows,
            counts(0, 0, 0, op="consensus_cas"),
            counts(2, 1, 100, op="consensus_cas"),
            dict(start=-2, end=-1),
            dict(start=1, end=2),
            EPOCH,
            0,
        )
        self.assertTrue(
            all(
                check["reconciled"]
                for check in report["operations"]["consensus_cas"][
                    "reconciliation"
                ].values()
            )
        )
        for op in ("blob_get", "blob_delete"):
            observed = totals(
                events(
                    *[
                        operation(op=op, outcome=outcome, size=size)
                        for outcome, size in [("ok", 100), ("missing", 0), ("error", 0)]
                    ]
                )
            )
            self.assertEqual(
                (
                    observed["events"],
                    observed["succeeded_count"],
                    observed["failed_count"],
                    observed["bytes_count"],
                ),
                (3, 2, 1, 100),
            )
        unknown = totals(events(operation(outcome="new outcome")))
        self.assertIsNone(unknown["succeeded_count"])
        self.assertIsNone(unknown["attempted_bytes"])
        self.assertEqual(unknown["events"], 1)

    def test_missing_metrics_are_unavailable_and_integers_are_exact(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "role.prom.gz"
            self.assertEqual(counters(path), {})
            big = 2**53 + 1
            metrics(path, {("blob_get", "bytes_count"): big}, total=True)
            before = counters(path)
            metrics(path, {("blob_get", "bytes_count"): big + 2}, total=False)
            observed = deltas(before, counters(path), "blob_get")
            self.assertEqual(observed["bytes_count"]["delta"], 2)
            self.assertIsNone(observed["events"]["delta"])
            self.assertIsNone(observed["succeeded_count"]["delta"])
            self.assertIsNone(
                deltas({}, counts(0, 0, 0), "blob_get")["events"]["delta"]
            )
            self.assertIsNone(
                deltas(counts(0, 0, 0), {}, "blob_get")["events"]["delta"]
            )
            self.assertEqual(
                deltas(counts(0, 0, 0), counts(0, 0, 0), "blob_get")["events"]["delta"],
                0,
            )

    def test_each_counter_reset_is_rejected_even_if_total_rises(self):
        for suffix in ("succeeded_count", "failed_count", "bytes_count"):
            with self.subTest(suffix=suffix):
                after = counts(30, 30, 30)
                after["blob_get", suffix] = 9
                with self.assertRaisesRegex(ValueError, "counter reset"):
                    deltas(counts(10, 10, 10), after, "blob_get")

    def test_strict_loose_count_failure_and_byte_reconciliation(self):
        rows = events(
            *[operation(time=EPOCH + t, size=10) for t in (0.05, 1, 9, 10.05)]
        )
        lo, hi = dict(start=0, end=0.1), dict(start=10, end=10.1)

        def report(success=3, size=30, jitter=0):
            result, _ = reconcile(
                rows,
                counts(5, 0, 50),
                counts(5 + success, 0, 50 + size),
                lo,
                hi,
                EPOCH,
                jitter,
            )
            return result["operations"]["blob_get"]

        checks = report()["reconciliation"]
        self.assertEqual(
            (checks["events"]["strict"], checks["events"]["loose"]), (2, 4)
        )
        self.assertEqual(
            (checks["bytes_count"]["strict"], checks["bytes_count"]["loose"]), (20, 40)
        )
        self.assertTrue(all(check["reconciled"] for check in checks.values()))
        self.assertFalse(report(success=5)["reconciliation"]["events"]["reconciled"])
        self.assertFalse(report(size=41)["reconciliation"]["bytes_count"]["reconciled"])
        self.assertEqual(report(jitter=1)["strict"]["events"], 0)
        result, _ = reconcile(rows, {}, {}, lo, hi, EPOCH, 0)
        self.assertIsNone(
            result["operations"]["blob_get"]["reconciliation"]["events"]["reconciled"]
        )

    def test_run_outputs_ledger_role_windows_and_numeric_boundary_order(self):
        with tempfile.TemporaryDirectory() as directory:
            run = Path(directory)
            (run / "usage.jsonl").write_text(
                "".join(
                    json.dumps(dict(time=t, wall_time=EPOCH + t + j)) + "\n"
                    for t, j in [(0, -0.01), (1, 0), (2, 0.01)]
                )
            )
            (run / "runtime.log").write_text(
                mapping("read txns", role="environmentd")
                + operation(
                    time=EPOCH + 5, context=f"audit_source_fetch{{reader={READER}}}: "
                )
                + operation(
                    time=EPOCH + 15, role="", key="LIVENESS", outcome="missing", size=0
                )
            )
            for number, start, count in [(2, 0, 5), (9, 10, 6), (10, 20, 6)]:
                (run / f"steady-{number}-metrics-timing.json").write_text(
                    json.dumps({"replica-s2-s2": dict(start=start, end=start + 0.1)})
                )
                metrics(
                    run / f"steady-{number}-replica-s2-s2.prom.gz",
                    counts(count, 0, count * 10),
                )
            with contextlib.redirect_stdout(io.StringIO()):
                result = analyze(run)
            saved = json.loads((run / "backend-analysis.json").read_text())
            self.assertEqual(result, saved)
            self.assertEqual(result["events"], 2)
            self.assertEqual(result["reader_records"], 1)
            self.assertAlmostEqual(result["clock_spread_seconds"], 0.01, places=6)
            self.assertEqual(
                [w["after"] for w in result["windows"]],
                ["steady-9-metrics-timing.json", "steady-10-metrics-timing.json"],
            )
            window = result["windows"][0]
            self.assertTrue(
                window["roles"]["replica-s2-s2"]["operations"]["blob_get"][
                    "reconciliation"
                ]["bytes_count"]["reconciled"]
            )
            self.assertEqual(window["unmeasured_event_roles"], ["pubsubd"])
            with gzip.open(run / "backend-events.jsonl.gz", "rt") as stream:
                ledger = [json.loads(line) for line in stream]
            self.assertEqual(ledger[1]["purpose"], "read txns")
            self.assertEqual(ledger[1]["key"], BATCH)
            self.assertEqual(ledger[2]["role"], "pubsubd")
            self.assertEqual(ledger[1]["line"], 2)
            cli = subprocess.run(
                [
                    sys.executable,
                    str(Path(__file__).with_name("analyze_backend.py")),
                    str(run),
                ],
                text=True,
                capture_output=True,
                check=True,
            )
            self.assertEqual(json.loads(cli.stdout)["events"], 2)


if __name__ == "__main__":
    unittest.main()
