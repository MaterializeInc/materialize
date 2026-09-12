# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Movements derived from the cluster spec sheet's CSV artifacts.

Unlike the benchmark steps, the spec sheet measures one build in isolation and uploads
raw per-repetition rows. The baseline therefore has to be reconstructed here, from the
builds that preceded the one being reported on.

Schemas are those written by test/cluster-spec-sheet/mzcompose.py: the cluster streams
carry `cluster_size`, `size_bytes` and `time_ms`, the environmentd stream carries
`envd_cpus` and `qps`.
"""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from statistics import median

from materialize.buildkite_insights.perf_movements.movement import (
    BaselineKind,
    Direction,
    JobRef,
    Movement,
)

CLUSTER_DIMENSION = "cluster_size"
ENVD_DIMENSION = "envd_cpus"

DIRECTION_BY_METRIC = {
    "time_ms": Direction.LOWER_IS_BETTER,
    "size_bytes": Direction.LOWER_IS_BETTER,
    "qps": Direction.HIGHER_IS_BETTER,
}

UNIT_BY_METRIC = {"time_ms": "ms", "size_bytes": "bytes", "qps": "qps"}

# `.cluster_object_limits.csv` is deliberately absent: its headline number is a maximum
# healthy object count derived from the `healthy` and `failure_mode` columns, which is a
# different aggregation than the median-of-repetitions used here.
RESULT_FILE_SUFFIXES = (
    ".cluster.csv",
    ".envd_objects_scalability.csv",
    ".envd.csv",
)


@dataclass(frozen=True)
class SampleKey:
    """Identifies one measured point across builds."""

    scenario: str
    scale: str
    mode: str
    category: str
    test_name: str
    dimension_name: str
    dimension: str

    def describe(self) -> str:
        return (
            f"{self.scenario}/{self.category}/{self.test_name}"
            f" [{self.dimension_name}={self.dimension}, scale={self.scale}, mode={self.mode}]"
        )


def parse_results_csv(text: str) -> dict[tuple[SampleKey, str], list[float]]:
    """Group one CSV artifact's rows into per-key, per-metric repetition lists."""
    reader = csv.DictReader(io.StringIO(text))
    if reader.fieldnames is None:
        return {}

    fieldnames = set(reader.fieldnames)
    dimension_name = ENVD_DIMENSION if ENVD_DIMENSION in fieldnames else CLUSTER_DIMENSION
    metrics = [name for name in DIRECTION_BY_METRIC if name in fieldnames]

    samples: dict[tuple[SampleKey, str], list[float]] = {}
    for row in reader:
        key = SampleKey(
            scenario=(row.get("scenario") or "").strip(),
            scale=(row.get("scale") or "").strip(),
            mode=(row.get("mode") or "").strip(),
            category=(row.get("category") or "").strip(),
            test_name=(row.get("test_name") or "").strip(),
            dimension_name=dimension_name,
            dimension=(row.get(dimension_name) or "").strip(),
        )
        for metric in metrics:
            raw = (row.get(metric) or "").strip()
            if not raw:
                continue
            try:
                value = float(raw)
            except ValueError:
                continue
            samples.setdefault((key, metric), []).append(value)

    return samples


def median_per_key(
    samples: dict[tuple[SampleKey, str], list[float]]
) -> dict[tuple[SampleKey, str], float]:
    """Collapse each key's repetitions to their median."""
    return {key: median(values) for key, values in samples.items() if values}


def merge_medians(
    per_file: list[dict[tuple[SampleKey, str], float]]
) -> dict[tuple[SampleKey, str], float]:
    """Combine the medians of every result file belonging to a single build.

    A key is measured by exactly one stream, so a collision means two shards of the same
    build measured it; their medians are averaged rather than one silently winning.
    """
    collected: dict[tuple[SampleKey, str], list[float]] = {}
    for medians in per_file:
        for key, value in medians.items():
            collected.setdefault(key, []).append(value)
    return {key: median(values) for key, values in collected.items()}


def compare_to_window(
    latest: dict[tuple[SampleKey, str], float],
    history: list[dict[tuple[SampleKey, str], float]],
    job: JobRef,
    min_history: int = 3,
) -> list[Movement]:
    """Compare the newest build against the median of the preceding builds.

    A key needs `min_history` earlier observations before it is reported, so that a newly
    added scenario does not surface as an infinite movement on its first run.
    """
    movements = []

    for (key, metric), this in latest.items():
        earlier = [
            build[(key, metric)] for build in history if (key, metric) in build
        ]
        if len(earlier) < min_history:
            continue

        movements.append(
            Movement(
                job=job,
                scenario=key.describe(),
                metric=metric,
                this=this,
                baseline=median(earlier),
                direction=DIRECTION_BY_METRIC[metric],
                baseline_kind=BaselineKind.WINDOW_MEDIAN,
                unit=UNIT_BY_METRIC.get(metric),
            )
        )

    return movements
