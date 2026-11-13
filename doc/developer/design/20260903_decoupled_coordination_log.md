# Decoupled coordination: current handoff

## Current objective and next action

Finish native-only ownership, execution and harness cleanup. Preserve adapter
policy, validated catalog transactions, durable protection, compatible full
writers and explicit retirement. Assess remaining architectural load separately,
without changing Persist or the 60s publication default.

The current two-version local `bin/controllerd` walkthrough and complementary
`0dt ddl` workload passed, including sink authority and singleton CDC handover.
The current debug bounded-memory upsert-index test still OOMs at its original
limit in both current runs. The independent launch audit found no demonstrated
conversion omission. Effective runtime settings, heap attribution and a matched
build comparison remain the diagnostic options. Do not raise limits or shrink
the workload.

Then reconcile remaining native-only acceptance gaps. The owned runtime is
cleaned up. Proposed residual-load work is ranked in the follow-up inventory,
separately from implementation. Do not turn passing targeted checks into a claim
of full CI or arbitrary-release compatibility.

## Verified outcomes

Controllerd owns processes, replica followers own maintained execution, and
adapters retain policy, transactions, protection and transient queries. Mandatory
native startup, transport ownership removal, storage restart fencing and native
profiling inventory are committed and pass targeted checks. Temporary objects
have execution ownership while retaining session visibility. Subscription IDs
are execution-qualified through observations, logging and dependencies.

Replay-history cloning, compaction, implementation tests and both history gauges
are removed (`8c09eb6c`). Independent review found no execution/recovery consumer.
All-target checks, compute tests and real current-binary replica-metrics/HTTP-proxy
workflows pass. Other affected library tests pass except the exact-size failure
below. The regenerated metrics catalog also removes previously retired entries.
No memory improvement has been measured.

The repaired upsert load-test runs all original scenarios, including the two
formerly combined configurations, with unchanged workloads/assertions and fixture
caps (`4a6f23e5`). The launcher regression traverses all configurations and fails
on the original malformed tuple. The first runtime attempt hit a harness guard
that mistook `cargo metadata` for compilation. Its exact retry passed unchanged.

Default-development 26.46 and 26.47 package builds share implementation and plan
encoding. The version-only build changes only environmentd/clusterd package
versions and their two lock entries, then restores original sources. The local
walkthrough verifies both versions serving SQL/DDL/table writes before and after
promotion, warmed execution and relay preservation, explicit retirement,
rejected old writes/reopen, survivor writes and a held snapshot, and clean stop.
It retains the configured ten-minute replica-stability requirement.

Successful receipt: `native-history-lifecycle/run-1794572603/result.json`.
The log is `native-history-lifecycle-process-snapshot-reproduce.log`. Evidence
was copied locally in `native-history-lifecycle-complete-evidence.tar.gz`.
The user-facing walkthrough is in `doc/developer/controllerd.md` (`8a996a03`).
This is an actual local launcher run, not merely container acceptance or green CI.

Launcher shutdown now handles exited non-child zombies without weakening live
process checks (`8ca234c0`). The regression's negative control, launcher tests,
independent review and actual walkthrough pass. Failed harness runs remain in the
evidence logs. Use `native_history_lifecycle_snapshot_safe.py` for any future
walkthrough to avoid the separately reproduced psutil cache race.

The unchanged full `0dt ddl` workflow passed with these two versions before and
after promotion, then the survivor after retirement. It exercises source/sink
workloads and singleton handover. Receipt: `aj-native-current-317b08de/result.json`,
with `sql-versions.json` recording actual versions. Cleanup completed and no new
OOM events occurred. This is compatible-prototype coverage, not arbitrary-release
or builtin-schema-migration coverage.

Earlier current-binary full multi-process smoke passed in both Timely topologies,
profiling HTTP passed, and same-build two-writer subscription promotion passed.
Those receipts remain useful regression evidence, not substitutes for the current
distinct-version walkthrough and sink/CDC run.

## Approved deferrals and live gaps

- Positive native builtin-schema migration is deferred beyond M3. Keep the
  rejection guard. Current-native Kubernetes/operator/Terraform integration is
  also deferred. Commit `1380f0ba` exposes unsupported coverage while preserving
  historical-only and unrelated workflows. Optional final preflight skips unknown
  or unsupported provenance before stopping processes.
- `ComputeCommand` is 48 bytes, but its existing size test expects 40. The exact
  full test command reproduces this. The approved subscription-incarnation field
  in `8c9e18f0d7` explains the growth. Asked whether to accept the eight bytes and
  update the expectation. It remains unchanged pending that decision. The remainder
  run explicitly skipped only that recorded failure. Do not claim a full pass.
- Bounded-memory upsert-index hydration reproducibly OOMs at its original 3.5-GiB
  replica limit, including with current debug binaries. The current first run
  and exact retry record clusterd exit 137 and OOMKilled, with about 60 GB host
  RAM available. `native-bounded-launch-audit.md` records the source comparison.
  `native-bounded-offline-findings.md` confirms active upsert backpressure and
  predominantly anonymous memory growth during ingestion/index startup. It does
  not identify the retaining operator or the actual compaction frontier.
  Cause remains unproven. This does not establish a regression against optimized
  upstream. Full upsert stress also exceeded an earlier local cap. Do not shrink
  workloads or raise limits to hide either failure.
- Data-ingest/parallel stress, randomized zippy/restore and bumped-version platform
  acceptance remain incomplete. Earlier index-publication and slow source-status
  cases remain unexplained. Adapter-loss acceptance needs a Persist CLI/jobs artifact.
- Broad CI is not green. Last reviewed broad evidence is
  [Build141419](https://buildkite.com/materialize/test/builds/141419). Registry/image
  gates, merge skew, Persist JSON-order checking and stale hydration expectations
  remain unresolved. Full bin/fmt reports unchanged long catalog-file lines.
  Changed-file checks pass. The PR stays draft.

## Worker, resources and evidence

Evidence root: `~/.local/share/aj/m3-evidence/native-only/`. Filenames above are
relative to `native-driver-scratch/` unless noted otherwise.
Owned worker: `i-064fa517ec1d8bbdb`, now terminated after copying evidence
and candidates locally. `worker-termination.json` records the request and
`worker-final-state.json` confirms termination. No access rules
changed. `remote.py` points to that released worker, not a reusable live host.

Keep heavy work on scratch, sequentially. Container/build aggregate budget:
40 GiB, no swap, eight CPUs. Builds use one Cargo job, 32 GiB and four CPUs.
Preserve 30 GiB disk and 2 GiB available RAM. The local-process walkthrough used
disjoint hard caps totalling 36 GiB/eight CPUs, verified actual membership, then
stopped and removed its user slice and restored the standard system-slice budget.
No native processes or containers remained after the final memory-test retry
either. See `native-history-post-bounded-resources.txt`.

Both versioned candidates are copied locally and hashes verified. The old version
is in `native-history-candidate/`, the new in
`native-history-compatible/artifacts/26.47.0-dev.0/`. Source/build/version manifests
are in `native-history-compatible/`. Superseded binaries and redundant archives
were removed. Local disk headroom was 37 GiB at last check.

The completed DDL fixture uses pinned mounted binaries and the existing dependency
images. SQL Server uses the same upstream CU21 base as the repository image,
without its preinitialized startup cache. The workload/assertions are unchanged.
This is not rebuilt-image CI. Its guard records the reviewed launcher-only source
override separately from the unchanged Rust build inputs.

Essential receipts:
- Replay checks: `native-history-check.log`, `native-history-reproduce.log`,
  `native-history-remainder.log`, `native-history-final-format-delta.txt`.
- Metrics/proxy: `native-history-metrics-evidence.tar.gz`.
- Upsert: `native-history-upsert-evidence.tar.gz` and
  `native-history-upsert-reproduce.log`.
- Launcher: `native-stop-zombie-tests.log` and the lifecycle evidence above.
- Distinct-version DDL: `native-history-compatible-ddl-evidence.tar.gz` and
  `native-history-compatible-ddl.log`.
- Current bounded-memory failure: `native-history-bounded-first-evidence.tar.gz`
  (`aj-native-cluster-cc9269fe`) and `native-history-bounded-metadata-fixed.log`.
  Exact retry: `native-history-bounded-oom-reproduce.log` and
  `native-history-bounded-repeat-evidence.tar.gz` (`aj-native-cluster-f7277a96`).
- Deferred boundaries: `native-bounded-harness-tests.log` and
  `native-boundary-review-{negative,fixed}.log`.
- Earlier outcomes and pointers: `native-handoff-before-current-version-acceptance.md`,
  `native-handoff-before-approved-deferrals.md` and the
  [archive](20260903_decoupled_coordination_log_archive.md).

Remaining architectural opportunities and unresolved cases are in the
[follow-up inventory](20260903_decoupled_coordination_followups.md).
`native-introspection-current-audit.md` distinguishes native relays from removed
legacy ownership. Do not remove their SQL inputs without replacement.
The [publication comparison](20260903_decoupled_coordination_publication_interval.md)
and `../logical-component-load/` remain the performance baseline, not measured
benefits from this cleanup.

Preserve the unrelated
`tmp_dec1fdd35343b4a99fe5b65d9e20e2b4b7085ecc719a42c7915575284b56c512.toml`.
