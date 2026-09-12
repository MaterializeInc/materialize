# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Fetches build data from Buildkite and turns it into movements.

Every fetch goes through the buildkite_insights cache, so repeated runs over the same
window cost nothing beyond the newest build.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from materialize.buildkite_insights.cache import artifacts_cache, builds_cache
from materialize.buildkite_insights.cache.cache_constants import FetchMode
from materialize.buildkite_insights.cache.logs_cache import get_or_download_log
from materialize.buildkite_insights.perf_movements import spec_sheet
from materialize.buildkite_insights.perf_movements.log_parsers import (
    parse_feature_benchmark_log,
    parse_parallel_benchmark_log,
    parse_scalability_log,
)
from materialize.buildkite_insights.perf_movements.movement import JobRef, Movement

LogParser = Callable[[str, JobRef], list[Movement]]

LOG_PARSER_BY_STEP_KEY: dict[str, LogParser] = {
    "feature-benchmark": parse_feature_benchmark_log,
    "feature-benchmark-scale-plus-one": parse_feature_benchmark_log,
    "parallel-benchmark": parse_parallel_benchmark_log,
    "long-parallel-benchmark": parse_parallel_benchmark_log,
    "scalability-benchmark-dml-dql": parse_scalability_log,
    "scalability-benchmark-ddl": parse_scalability_log,
    "scalability-benchmark-connection": parse_scalability_log,
}

SPEC_SHEET_STEP_KEYS = frozenset(
    {
        "cluster-spec-sheet-cluster",
        "cluster-spec-sheet-source-ingestion",
        "cluster-spec-sheet-staging",
    }
)

# Steps whose output carries no per-metric comparison and no per-repetition CSV:
# `limits`, `limits-instance-size`, `bounded-memory`, `bounded-memory-search`,
# `orchestratord-rolling-upgrade-downtime` and `cargo-bench`. They are not collected.
PIPELINES = ("nightly", "release-qualification", "spec-sheet")


def _jobs_with_step_keys(build: Any, step_keys: frozenset[str]) -> list[dict[str, Any]]:
    return [
        job
        for job in build.get("jobs", [])
        if job.get("step_key") in step_keys and job.get("id")
    ]


def _job_ref(pipeline_slug: str, build: Any, job: dict[str, Any]) -> JobRef:
    return JobRef(
        pipeline_slug=pipeline_slug,
        build_number=build["number"],
        job_id=job["id"],
        step_key=job["step_key"],
        commit_hash=build.get("commit"),
    )


def fetch_builds(
    pipeline_slug: str,
    branch: str,
    fetch_mode: FetchMode,
    max_builds: int,
) -> list[Any]:
    """Newest builds first, restricted to `branch`."""
    builds = builds_cache.get_or_query_builds(
        pipeline_slug=pipeline_slug,
        fetch_mode=fetch_mode,
        max_fetches=1,
        branch=branch,
        build_states=None,
    )
    return builds[:max_builds]


def collect_log_movements(
    pipeline_slug: str,
    build: Any,
    fetch_mode: FetchMode,
) -> list[Movement]:
    """Movements from every self-comparing step of a single build."""
    movements = []

    for job in _jobs_with_step_keys(build, frozenset(LOG_PARSER_BY_STEP_KEY)):
        parser = LOG_PARSER_BY_STEP_KEY[job["step_key"]]
        log = get_or_download_log(
            pipeline_slug=pipeline_slug,
            fetch_mode=fetch_mode,
            build_number=build["number"],
            job_id=job["id"],
        )
        movements.extend(parser(log, _job_ref(pipeline_slug, build, job)))

    return movements


def _collect_spec_sheet_medians(
    pipeline_slug: str,
    build: Any,
    fetch_mode: FetchMode,
) -> dict[tuple[spec_sheet.SampleKey, str], float]:
    per_file = []

    for job in _jobs_with_step_keys(build, SPEC_SHEET_STEP_KEYS):
        artifacts = artifacts_cache.get_or_query_job_artifact_list(
            pipeline_slug=pipeline_slug,
            fetch_mode=fetch_mode,
            build_number=build["number"],
            job_id=job["id"],
        )
        for artifact in artifacts:
            filename = artifact.get("filename", "")
            if not filename.endswith(spec_sheet.RESULT_FILE_SUFFIXES):
                continue
            content = artifacts_cache.get_or_download_artifact(
                pipeline_slug=pipeline_slug,
                fetch_mode=fetch_mode,
                build_number=build["number"],
                job_id=job["id"],
                artifact_id=artifact["id"],
                is_zst_compressed=False,
            )
            per_file.append(
                spec_sheet.median_per_key(spec_sheet.parse_results_csv(content))
            )

    return spec_sheet.merge_medians(per_file)


def collect_spec_sheet_movements(
    pipeline_slug: str,
    builds: list[Any],
    fetch_mode: FetchMode,
    min_history: int,
) -> list[Movement]:
    """Compare the newest spec sheet build against the builds behind it.

    `builds` must be newest first. The reported job reference points at the first spec
    sheet job of the newest build, since the movement is a property of that whole build
    rather than of any single shard.
    """
    if not builds:
        return []

    latest_build = builds[0]
    latest_jobs = _jobs_with_step_keys(latest_build, SPEC_SHEET_STEP_KEYS)
    if not latest_jobs:
        return []

    latest = _collect_spec_sheet_medians(pipeline_slug, latest_build, fetch_mode)
    history = [
        _collect_spec_sheet_medians(pipeline_slug, build, fetch_mode)
        for build in builds[1:]
    ]

    return spec_sheet.compare_to_window(
        latest=latest,
        history=history,
        job=_job_ref(pipeline_slug, latest_build, latest_jobs[0]),
        min_history=min_history,
    )


def collect_pipeline_movements(
    pipeline_slug: str,
    branch: str,
    fetch_mode: FetchMode,
    max_builds: int,
    min_history: int,
) -> list[Movement]:
    """All movements for one pipeline's newest build on `branch`."""
    builds = fetch_builds(pipeline_slug, branch, fetch_mode, max_builds)
    if not builds:
        return []

    if pipeline_slug == "spec-sheet":
        return collect_spec_sheet_movements(
            pipeline_slug, builds, fetch_mode, min_history
        )

    return collect_log_movements(pipeline_slug, builds[0], fetch_mode)
