# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# ci_logged_errors_detect.py - Detect errors in log files during CI and find
# associated open GitHub issues in Materialize repository.

import argparse
import mmap
import os
import re
import sys
from typing import Any

import requests

from materialize import ci_util, spawn, ui

CI_RE = re.compile("ci-regexp: (.*)")
CI_APPLY_TO = re.compile("ci-apply-to: (.*)")

# Unexpected failures, report them
ERROR_RE = re.compile(
    rb"""
    ^ .*
    ( segfault\ at
    | trap\ invalid\ opcode
    | general\ protection
    | has\ overflowed\ its\ stack
    | Attempting\ to\ connect
    | Applying\ configuration\ update
    | internal\ error:
    | \*\ FATAL:
    | [Oo]ut\ [Oo]f\ [Mm]emory
    | cannot\ migrate\ from\ catalog
    | halting\ process: # Rust unwrap
    | fatal runtime error: # stack overflow
    | \[SQLsmith\] # Unknown errors are logged
    | \[SQLancer\] # Unknown errors are logged
    # From src/testdrive/src/action/sql.rs
    | column\ name\ mismatch
    | non-matching\ rows:
    | do\ not\ match
    | wrong\ row\ count:
    | wrong\ hash\ value:
    | expected\ one\ statement
    | query\ succeeded,\ but\ expected
    | expected\ .*,\ got\ .*
    | expected\ .*,\ but\ found\ none
    | unsupported\ SQL\ type\ in\ testdrive:
    | environmentd:\ fatal: # startup failure
    | clusterd:\ fatal: # startup failure
    | error:\ Found\ argument\ '.*'\ which\ wasn't\ expected,\ or\ isn't\ valid\ in\ this\ context
    | environmentd .* unrecognized\ configuration\ parameter
    | cannot\ load\ unknown\ system\ parameter\ from\ catalog\ storage
    )
    .* $
    """,
    re.VERBOSE | re.MULTILINE,
)

# Panics are multiline and our log lines of multiple services are interleaved,
# making them complex to handle in regular expressions, thus handle them
# separately.
PANIC_START_RE = re.compile(rb"^(?P<service>[^ ]*) *\| thread '.*' panicked at ")
SERVICES_LOG_LINE_RE = re.compile(rb"^(?P<service>[^ ]*) *\| (?P<msg>.*)$")

# Expected failures, don't report them
IGNORE_RE = re.compile(
    rb"""
    # Expected in restart test
    ( restart-materialized-1\ \ \|\ thread\ 'coordinator'\ panicked\ at\ 'can't\ persist\ timestamp
    # Expected in restart test
    | restart-materialized-1\ *|\ thread\ 'coordinator'\ panicked\ at\ 'external\ operation\ .*\ failed\ unrecoverably.*
    # Expected in cluster test
    | cluster-clusterd[12]-1\ .*\ halting\ process:\ new\ timely\ configuration\ does\ not\ match\ existing\ timely\ configuration
    # Emitted by tests employing explicit mz_panic()
    | forced\ panic
    # Emitted by broken_statements.slt in order to stop panic propagation, as 'forced panic' will unwantedly panic the `environmentd` thread.
    | forced\ optimizer\ panic
    # Expected once compute cluster has panicked, brings no new information
    | timely\ communication\ error:
    # Expected once compute cluster has panicked, only happens in CI
    | aborting\ because\ propagate_crashes\ is\ enabled
    # Expected when CRDB is corrupted
    | restart-materialized-1\ .*relation\ \\"fence\\"\ does\ not\ exist
    # Expected when CRDB is corrupted
    | restart-materialized-1\ .*relation\ "consensus"\ does\ not\ exist
    # Will print a separate panic line which will be handled and contains the relevant information (new style)
    | internal\ error:\ unexpected\ panic\ during\ query\ optimization
    # redpanda INFO logging
    | larger\ sizes\ prevent\ running\ out\ of\ memory
    # Old versions won't support new parameters
    | (platform-checks|legacy-upgrade|upgrade-matrix|feature-benchmark)-materialized-.* \| .*cannot\ load\ unknown\ system\ parameter\ from\ catalog\ storage
    # Fencing warnings are OK in fencing tests
    | persist-txn-fencing-mz_first-.* \| .*unexpected\ fence\ epoch
    | persist-txn-fencing-mz_first-.* \| .*fenced\ by\ new\ catalog\ upper
    | platform-checks-mz_txn_tables.* \| .*unexpected\ fence\ epoch
    # For platform-checks upgrade tests
    | platform-checks-clusterd.* \| .* received\ persist\ state\ from\ the\ future
    | cannot\ load\ unknown\ system\ parameter\ from\ catalog\ storage(\ to\ set\ (default|configured)\ parameter)?
    | internal\ error:\ no\ AWS\ external\ ID\ prefix\ configured
    # For persist-catalog-migration ignore failpoint panics
    | persist-catalog-migration-materialized.* \| .* failpoint\ .* panic
    )
    """,
    re.VERBOSE | re.MULTILINE,
)

WALL_OF_TEXT = """
platform-checks-materialized-1      | assertion `left == right` failed: stash and persist result variant do not match. stash: Err(Durable(IncompatibleDataVersion { found_version: 46, min_catalog_version: 42, catalog_version: 45 })). persist: Ok(PersistCatalogState { mode: Writable, since_handle: SinceHandle { machine: Machine { applier: Applier { cfg: PersistConfig { build_version: Version { major: 0, minor: 86, patch: 0, pre: Prerelease("dev") }, hostname: "materialized", now: <now_fn>, configs: {"crdb_connect_timeout": Duration(RwLock { data: 5s, poisoned: false, .. }), "crdb_tcp_user_timeout": Duration(RwLock { data: 30s, poisoned: false, .. }), "persist_batch_delete_enabled": Bool(false), "persist_blob_cache_mem_limit_bytes": Usize(1048576), "persist_blob_target_size": Usize(134217728), "persist_compaction_minimum_timeout": Duration(RwLock { data: 90s, poisoned: false, .. }), "persist_consensus_connection_pool_ttl": Duration(RwLock { data: 300s, poisoned: false, .. }), "persist_consensus_connection_pool_ttl_stagger": Duration(RwLock { data: 6s, poisoned: false, .. }), "persist_next_listen_batch_retryer_clamp": Duration(RwLock { data: 100ms, poisoned: false, .. }), "persist_next_listen_batch_retryer_initial_backoff": Duration(RwLock { data: 1.2s, poisoned: false, .. }), "persist_next_listen_batch_retryer_multiplier": U32(2), "persist_pubsub_client_enabled": Bool(true), "persist_pubsub_push_diff_enabled": Bool(true), "persist_reader_lease_duration": Duration(RwLock { data: 900s, poisoned: false, .. }), "persist_rollup_threshold": Usize(128), "persist_sink_minimum_batch_updates": Usize(0), "persist_stats_audit_percent": Usize(0), "persist_stats_budget_bytes": Usize(1024), "persist_stats_collection_enabled": Bool(true), "persist_stats_filter_enabled": Bool(true), "persist_stats_untrimmable_columns_equals": String(RwLock { data: "err,ts,receivedat,createdat,_fivetran_deleted,", poisoned: false, .. }), "persist_stats_untrimmable_columns_prefix": String(RwLock { data: "last_,", poisoned: false, .. }), "persist_stats_untrimmable_columns_suffix": String(RwLock { data: "timestamp,time,_at,_tstamp,", poisoned: false, .. }), "persist_streaming_compaction_enabled": Bool(false), "persist_streaming_snapshot_and_fetch_enabled": Bool(false), "persist_txns_data_shard_retryer_clamp": Duration(RwLock { data: 16s, poisoned: false, .. }), "persist_txns_data_shard_retryer_initial_backoff": Duration(RwLock { data: 1.024s, poisoned: false, .. }), "persist_txns_data_shard_retryer_multiplier": U32(2), "storage_persist_sink_minimum_batch_updates": Usize(1024), "storage_source_decode_fuel": Usize(1000000)}, dynamic: DynamicConfig { batch_builder_max_outstanding_parts: 2, compaction_heuristic_min_inputs: 8, compaction_heuristic_min_parts: 8, compaction_heuristic_min_updates: 1024, compaction_memory_bound_bytes: 1073741824, gc_blob_delete_concurrency_limit: 32, state_versions_recent_live_diffs_limit: 3840, usage_state_fetch_concurrency_limit: 8 }, compaction_enabled: true, compaction_concurrency_limit: 5, compaction_queue_size: 20, compaction_yield_after_n_updates: 100000, consensus_connection_pool_max_size: 50, consensus_connection_pool_max_wait: Some(60s), writer_lease_duration: 3600s, critical_downgrade_interval: 30s, pubsub_connect_attempt_timeout: 5s, pubsub_request_timeout: 5s, pubsub_connect_max_backoff: 60s, pubsub_client_sender_channel_size: 25, pubsub_client_receiver_channel_size: 25, pubsub_server_connection_channel_size: 25, pubsub_state_cache_shard_ref_channel_size: 25, pubsub_reconnect_backoff: 5s }, metrics: Metrics { .. }, shard_metrics: ShardMetrics { shard_id: ShardId(86892608-7ff3-82f8-1090-7584b4945aaa), since: DeleteOnDropGauge { inner: GenericGauge { v: Value { desc: Desc { fq_name: "mz_persist_shard_since", help: "since by shard", const_label_pairs: [], variable_labels: ["shard", "name"], id: 15579663059153072772, dim_hash: 13414171350431820200 }, val: AtomicI64 { inner: 1688 }, val_type: Gauge, label_pairs: [LabelPair { name: "name", value: "catalog" }, LabelPair { name: "shard", value: "s86892608-7ff3-82f8-1090-7584b4945aaa" }] } }, labels: ["s86892608-7ff3-82f8-1090-7584b4945aaa", "catalog"], vec: MetricVec, _phantom: PhantomData<()> }, upper: DeleteOnDropGauge { inner: GenericGauge { v: Value { desc: Desc { fq_name: "mz_persist_shard_upper", help: "upper by shard", const_label_pairs: [], variable_labels: ["shard", "name"], id: 4092965619647097614, dim_hash: 14717199528981430922 }, val: AtomicI64 { inner: 2826 }, val_type: Gauge, label_pairs: [LabelPair { name: "name", value: "catalog" }, LabelPair { name: "shard", value: "s86892608-7ff3-82f8-1090-7584b4945aaa" }] } }, labels: ["s86892608-7ff3-82f8-1090-7584b4945aaa", "catalog"], vec: MetricVec, _phantom: PhantomData<()> }, largest_batch_size: DeleteOnDropGauge { inner: GenericGauge { v: Value { desc: Desc { fq_name: "mz_persist_shard_largest_batch_size", help: "largest encoded batch size by shard", const_label_pairs: [], variable_labels: ["shard", "name"], id: 15668483818539198391, dim_hash: 762317046610672469 }, val: AtomicU64 { inner: 1207754 }, val_type: Gauge, label_pairs: [LabelPair { name: "name", value: "catalog" }, LabelPair { name: "shard", value: "s86892608-7ff3-82f8-1090-7584b4945aaa" }] } }, labels: ["s86892608-7ff3-82f8-1090-7584b4945aaa", "catalog"], vec: MetricVec, _phantom: PhantomData<()> }, latest_rollup_size: DeleteOnDropGauge { inner: GenericGauge { v: Value { desc: Desc { fq_name: "mz_persist_shard_rollup_size_bytes", help: "total encoded rollup size by shard", const_label_pairs: [], variable_labels: ["shard", "name"], id: 9414154508029371128, dim_hash: 4833994461610112783 }, val: AtomicU64 { inner: 0 }, val_type: Gauge, label_pairs: [LabelPair { name: "name", value: "catalog" }, LabelPair { name: "shard", value: "s86892608-7ff3-82f8-1090-7584b4945aaa" }] } }, labels: ["s86892608-7ff3-82f8-1090-7584b4945aaa", "catalog"], vec: MetricVec, _phantom: PhantomData<()> }, encoded_diff_size: DeleteOnDropCounter { inner: GenericCounter { v: Value { desc: Desc { fq_name: "mz_persist_shard_diff_size_bytes", help: "total encoded diff size by shard", const_label_pairs: [], variable_labels: ["shard", "name"], id: 14502594083303268037, dim_hash: 1365161818160524766 }, val: AtomicU64 { inner: 1600 }, val_type: Counter, label_pairs: [LabelPair { name: "name", value: "catalog" }, LabelPair { name: "shard", value: "s86892608-7ff3-82f8-1090-7584b4945aaa" }] } }, labels: ["s86892608-7ff3-82f8-1090-7584b4945aaa", "catalog"], vec: MetricVec, _phantom: PhantomData<()> }, hollow_batch_count: DeleteOnDropGauge { inner: GenericGauge { v: Value { desc: Desc { fq_name: "mz_persist_shard_hollow_batch_count", help: "count of hollow batches by shard", const_label_pairs: [], variable_labels: ["shard", "name"], id: 14684318548278768644, dim_hash: 75193424216019475 }, val: AtomicU64 { inner: 17 }, val_type: Gauge, label_pairs: [LabelPair { name: "name", value: "catalog" }, LabelPair { name: "shard", value: "s86892608-7ff3-82f8-1090-7584b4945aaa" }] } }, labels: ["s86892608-7ff3-82f8-1090-7584b4945aaa", "catalog"], vec: MetricVec, _phantom: PhantomData<()> }, spine_batch_count: DeleteOnDropGauge { inner: GenericGauge { v: Value { desc: Desc { fq_name: "mz_persist_shard_spine_batch_count", help: "count of spine batches by shard", const_label_pairs: [], variable_labels: ["shard", "name"], id: 1834377188952327142, dim_hash: 6921476859480867789 }, val: AtomicU64 { inner: 11 }
""".strip()


class KnownIssue:
    def __init__(self, regex: re.Pattern[Any], apply_to: str | None, info: Any):
        self.regex = regex
        self.apply_to = apply_to
        self.info = info


class ErrorLog:
    def __init__(self, match: bytes, file: str):
        self.match = match
        self.file = file


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="ci-logged-errors-detect",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="""
ci-logged-errors-detect detects errors in log files during CI and finds
associated open GitHub issues in Materialize repository.""",
    )

    parser.add_argument("log_files", nargs="+", help="log files to search in")
    args = parser.parse_args()

    return annotate_logged_errors(args.log_files)


def annotate_errors(errors: list[str], title: str, style: str) -> None:
    if not errors:
        return

    errors = group_identical_errors(errors)
    suite_name = os.getenv("BUILDKITE_LABEL") or "Logged Errors"

    error_str = "\n".join(f"* {error}" for error in errors)
    # 400 Bad Request: The annotation body must be less than 1 MB
    if len(error_str) > 900_000:
        error_str = error_str[:900_000] + "..."

    if style == "info":
        markdown = f"""<details><summary>{suite_name}: {title}</summary>

{error_str}
</details>"""
    else:
        markdown = f"""{suite_name}: {title}

{error_str}"""

    spawn.runv(
        [
            "buildkite-agent",
            "annotate",
            f"--style={style}",
            f"--context={os.environ['BUILDKITE_JOB_ID']}-{style}",
        ],
        stdin=markdown.encode(),
    )


def group_identical_errors(errors: list[str]) -> list[str]:
    errors_with_counts: dict[str, int] = {}

    for error in errors:
        errors_with_counts[error] = 1 + errors_with_counts.get(error, 0)

    consolidated_errors = []

    for error, count in errors_with_counts.items():
        consolidated_errors.append(
            error if count == 1 else f"{error}\n({count} occurrences)"
        )

    return consolidated_errors


def annotate_logged_errors(log_files: list[str]) -> int:
    """
    Returns the number of unknown errors, 0 when all errors are known or there
    were no errors logged. This will be used to fail a test even if the test
    itself succeeded, as long as it had any unknown error logs.
    """

    error_logs = get_error_logs(log_files)

    if not error_logs:
        error_logs = []

    step_key = os.getenv("BUILDKITE_STEP_KEY", "")
    os.getenv("BUILDKITE_LABEL", "")

    (known_issues, unknown_errors) = get_known_issues_from_github()

    artifacts = ci_util.get_artifacts()
    job = os.getenv("BUILDKITE_JOB_ID")

    known_errors: list[str] = []

    # Keep track of known errors so we log each only once

    if step_key == "cloudtest":
        unknown_errors.append(
            "Unknown error starting with backtick 1:  \n```\n`error_message`\n```"
        )
        unknown_errors.append(
            "Unknown error starting with backtick 2:  \n```\n`error_message` happened\n```"
        )
    elif step_key == "cluster-isolation":
        error_message = sanitize_text(WALL_OF_TEXT)
        formatted_error_message = f"```\n{error_message}\n```"

        unknown_errors.append(
            f"Fat error: {formatted_error_message}"
        )


    for error in error_logs:
        error_message = sanitize_text(error.match.decode("utf-8"))
        formatted_error_message = f"```\n{error_message}\n```"

        for artifact in artifacts:
            if artifact["job_id"] == job and artifact["path"] == error.file:
                org = os.environ["BUILDKITE_ORGANIZATION_SLUG"]
                pipeline = os.environ["BUILDKITE_PIPELINE_SLUG"]
                build = os.environ["BUILDKITE_BUILD_NUMBER"]
                linked_file = f'[{error.file}](https://buildkite.com/organizations/{org}/pipelines/{pipeline}/builds/{build}/jobs/{artifact["job_id"]}/artifacts/{artifact["id"]})'
                break
        else:
            linked_file = error.file

        unknown_errors.append(
            f"Unknown error in {linked_file}:  \n{formatted_error_message}"
        )

    annotate_errors(
        unknown_errors,
        "Unknown errors and regressions in logs (see [ci-regexp](https://github.com/MaterializeInc/materialize/blob/main/doc/developer/ci-regexp.md))",
        "error",
    )
    annotate_errors(known_errors, "Known errors in logs, ignoring", "info")

    if unknown_errors:
        print(
            f"+++ Failing test because of {len(unknown_errors)} unknown error(s) in logs:"
        )
        print(unknown_errors)

    return len(unknown_errors)


def get_error_logs(log_files: list[str]) -> list[ErrorLog]:
    error_logs = []
    for log_file in log_files:
        with open(log_file, "r+") as f:
            try:
                data = mmap.mmap(f.fileno(), 0)
            except ValueError:
                # empty file, ignore
                continue
            for match in ERROR_RE.finditer(data):
                if IGNORE_RE.search(match.group(0)):
                    continue
                # environmentd segfaults during normal shutdown in coverage builds, see #20016
                # Ignoring this in regular ways would still be quite spammy.
                if (
                    b"environmentd" in match.group(0)
                    and b"segfault at" in match.group(0)
                    and ui.env_is_truthy("CI_COVERAGE_ENABLED")
                ):
                    continue
                error_logs.append(ErrorLog(match.group(0), log_file))
            open_panics = {}
            for line in iter(data.readline, b""):
                line = line.rstrip(b"\n")
                if match := PANIC_START_RE.match(line):
                    service = match.group("service")
                    assert (
                        service not in open_panics
                    ), f"Two panics of same service {service} interleaving: {line}"
                    open_panics[service] = line
                elif open_panics:
                    if match := SERVICES_LOG_LINE_RE.match(line):
                        # Handling every services.log line here, filter to
                        # handle only the ones which are currently in a panic
                        # handler:
                        if panic_start := open_panics.get(match.group("service")):
                            del open_panics[match.group("service")]
                            if IGNORE_RE.search(match.group(0)):
                                continue
                            error_logs.append(
                                ErrorLog(
                                    panic_start + b" " + match.group("msg"), log_file
                                )
                            )
            assert not open_panics, f"Panic log never finished: {open_panics}"

    # TODO: Only report multiple errors once?
    return error_logs


def sanitize_text(text: str, max_length: int = 4000) -> str:
    if len(text) > max_length:
        text = text[:max_length]

    text = text.replace("```", r"\`\`\`")

    return text


def get_known_issues_from_github_page(page: int = 1) -> Any:
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if token := os.getenv("GITHUB_TOKEN"):
        headers["Authorization"] = f"Bearer {token}"

    response = requests.get(
        f'https://api.github.com/search/issues?q=repo:MaterializeInc/materialize%20type:issue%20in:body%20"ci-regexp%3A"&per_page=100&page={page}',
        headers=headers,
    )

    if response.status_code != 200:
        raise ValueError(f"Bad return code from GitHub: {response.status_code}")

    issues_json = response.json()
    assert issues_json["incomplete_results"] == False
    return issues_json


def get_known_issues_from_github() -> tuple[list[KnownIssue], list[str]]:
    page = 1
    issues_json = get_known_issues_from_github_page(page)
    while issues_json["total_count"] > len(issues_json["items"]):
        page += 1
        next_page_json = get_known_issues_from_github_page(page)
        if not next_page_json["items"]:
            break
        issues_json["items"].extend(next_page_json["items"])

    unknown_errors = []
    known_issues = []

    for issue in issues_json["items"]:
        matches = CI_RE.findall(issue["body"])
        matches_apply_to = CI_APPLY_TO.findall(issue["body"])
        for match in matches:
            try:
                regex_pattern = re.compile(match.strip().encode("utf-8"))
            except:
                unknown_errors.append(
                    f"[{issue.info['title']} (#{issue.info['number']})]({issue.info['html_url']}): Invalid regex in ci-regexp: {match.strip()}, ignoring"
                )
                continue

            if matches_apply_to:
                for match_apply_to in matches_apply_to:
                    known_issues.append(
                        KnownIssue(regex_pattern, match_apply_to.strip().lower(), issue)
                    )
            else:
                known_issues.append(KnownIssue(regex_pattern, None, issue))

    return (known_issues, unknown_errors)


if __name__ == "__main__":
    sys.exit(main())
