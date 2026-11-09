# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import contextlib
import gzip
import io
import json
import tempfile
import unittest
from pathlib import Path

import analyze_profile_components as profiles
import analyze_publications as publications


class AttributionTest(unittest.TestCase):
    def test_overlapping_prefixes_do_not_double_count_or_cover_unknown_commits(self):
        with tempfile.TemporaryDirectory() as directory:
            run = Path(directory)
            (run / "publication-result.json").write_text(
                json.dumps({"status": "passed", "start_wall": 0, "end_wall": 100})
            )
            (run / "logical-backend.json").write_text(
                json.dumps({"dataset_identities": {"s1": "mz_internal.mz_catalog_raw"}})
            )
            with gzip.open(run / "backend-events.jsonl.gz", "wt") as f:
                for upper in [10, 20, 30]:
                    event = {
                        "op": "consensus_cas",
                        "shard": "s1",
                        "outcome": "committed",
                        "append_kind": "data",
                        "updates": 2,
                        "role": "adapterd",
                        "time": upper,
                        "line": upper,
                        "context_fields": {
                            "upper": [{"value": f"Antichain {{ elements: [{upper}] }}"}]
                        },
                    }
                    f.write(json.dumps(event) + "\n")
            lines = []
            for upper, count in [(20, 4), (10, 2), (20, 4)]:
                lines.append(
                    f'catalog committed prefix applied from_position=Some(CatalogPosition {{ upper: 0 }}) upper={upper} update_kinds={{("ClientReadRequirement", Addition): {count}}} grant_incarnations=[]\n'
                )
            (run / "runtime.log").write_text("".join(lines))
            with contextlib.redirect_stdout(io.StringIO()):
                publications.reduce(run)
            result = json.loads((run / "publication-coverage.json").read_text())
            self.assertEqual(result["selected_rows"], 6)
            self.assertEqual(result["covered_commits"], 2)
            self.assertEqual(result["solved_commits"], 2)
            self.assertEqual(
                result["solved_kinds_by_role"],
                {"adapterd": {"ClientReadRequirement": 4}},
            )
            self.assertEqual(len(result["uncovered"]), 1)

    def test_profile_weight_mismatch_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            run = Path(directory)
            (run / "adapterd-cpu-raw.txt").write_text(
                "Samples:\ncpu/nanoseconds\n 3: 1\nLocations\n 1: 0x123 M=1\nMappings\n1: start end abc binary\n"
            )
            (run / "adapterd-cpu-all-symbols.txt").write_text(
                "unknown_function\nsource:1\n"
            )
            meta = {
                "build_id": "abc",
                "sample_type": "cpu/nanoseconds",
                "total_weight": 3,
            }
            path = run / "adapterd-cpu-analysis.json"
            path.write_text(json.dumps(meta))
            _, samples = profiles.samples(run, "adapterd", "cpu")
            self.assertEqual(samples, [(3, ["unknown_function"])])
            self.assertEqual(
                profiles.classify(samples[0][1], "cpu"), "runtime / other / unmapped"
            )
            meta["total_weight"] = 4
            path.write_text(json.dumps(meta))
            with self.assertRaises(AssertionError):
                profiles.samples(run, "adapterd", "cpu")


if __name__ == "__main__":
    unittest.main()
