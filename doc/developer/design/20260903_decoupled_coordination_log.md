# Decoupled coordination: current handoff

## Current objective

Finish native-only ownership, execution and harness cleanup. Preserve adapter
policy, validated catalog transactions, durable protection, compatible full
writers and explicit retirement. Keep shared query, table and subscription
machinery. Assess architecture-generated load separately, without changing
Persist or the 60s publication default.

## Next concrete action

Finish the running distinct-version local `bin/controllerd` walkthrough. The
approved replay removal, metrics/proxy acceptance and full repaired upsert load-test
are complete. Keep the command-size failure below explicit. After the walkthrough,
verify remaining current-version sink/CDC coverage and document executed commands.

Commit `8c09eb6c` removes the replay buffer, cloning/compaction, implementation
tests and two exported history gauges. Execution uses no history reader.
Independent review found no blocking issue. Cluster coverage now checks live
collections and command processing, with existing hydration/drop assertions
preserved. The regenerated metric catalog also removes stale entries from prior
native-only removals. No memory improvement has been measured.

All-target checks and compute library tests pass. The compute-client suite fails
its existing exact-size assertion: `ComputeCommand` is 48 bytes rather than 40.
The exact full command reproduced this. The accepted subscription-incarnation
field in `8c9e18f0d7` explains the growth. Asked whether to accept the eight bytes
and update the expectation. The assertion is unchanged pending that decision.
Do not claim the full suite passes.

The remaining library cases passed under `aj-native-history-remainder.service`,
using the same package/feature union and explicitly skipping only that recorded
failure. An initial storage-only invocation was stopped because changing the
package union triggered unnecessary dependency recompilation. Evidence:
`native-history-check.log`, `native-history-reproduce.log`,
`native-history-storage-tests.log` and `native-history-remainder.log`.
Source: `native-history-source-sha256.json`. A later local Rust edit only removes
an empty comment heading from compute metrics.

Commit `4a6f23e5` repairs the upsert fixture. It now
has two separate scenarios with unchanged settings, workloads and assertions.
The existing launcher test now traverses all five configurations. Its negative
control reproduces the malformed tuple in memory without editing the worktree.
The full original runtime workload passed, including both repaired scenarios.

The current default-dev build passed with a matching source manifest and unchanged
Cargo.lock. Peak build memory was 28.2 GiB. Inputs: `native-history-final-source.tar.gz`,
`native-history-runtime-source.tar.gz`, `native-history-build-source-sha256.json`,
`native-history-build.sh` and `native_history_runtime_verify.py`. The runner supports metrics, HTTP proxy, full upsert load-test and prior smoke/
subscription workflows. It uses `native-history-candidate/`, preserving the
previous verified candidate. Replica metrics and HTTP proxy workflows passed, with cleanup complete and no
aggregate OOM increase. Receipts: `native-history-metrics-runtime.log`,
`native-history-proxy-runtime.log`, `aj-native-current-f0be52f2` and
`aj-native-current-785104f2`. Evidence is being copied locally in
`native-history-metrics-evidence.tar.gz`. The sole post-build source difference
is removal of one blank line, recorded in `native-history-final-format-delta.txt`.
The first upsert run was interrupted by a guard mistaking `cargo metadata` for
compilation. Its exact retry passed all original scenarios without a code or
resource change. Logs: `native-history-upsert-runtime.log` and
`native-history-upsert-reproduce.log`. Both runs' receipts are copied locally in
`native-history-upsert-evidence.tar.gz`. A precise read-only metadata exception
has been prepared and checked separately for future runners.
The current candidate is copied locally with all four hashes verified. Superseded
candidate binaries and redundant archives were removed, retaining provenance.
Local disk headroom is 40 GiB.

The version-only 26.47 build passed. Only the environmentd/clusterd package versions
and their two Cargo.lock entries changed. Original sources were restored and the
26.46 artifacts stayed unchanged. Evidence: `native-history-next-build-fixed.log`
and `native-history-compatible/`. An earlier harness assumption incorrectly
required a Git-clean scratch overlay, reproduced before replacing it with exact
verified-manifest validation. Both artifacts are being preserved locally.

`native_history_lifecycle.py` adapts the retained local walkthrough to this pair.
Current catalog schemas and synchronous retirement retractions were reviewed.
It retains both-writer DDL/table probes, warmed execution, promotion, separate
retirement, rejected old writes and surviving held snapshots. Resource and cleanup
review findings were fixed before launch. Its current run is
`native-history-lifecycle/run-1794569533`, under
`aj-native-history-lifecycle.service`. Log: `native-history-lifecycle-runtime.log`.
The first controller is initializing. Runtime acceptance is not established yet.

This local-process fixture has disjoint hard caps: system slice 8 GiB/2 CPU for
supervisor and metadata, user `ajnativeruntime.slice` 28 GiB/6 CPU for native child
scopes. It checks actual process/container ancestry and limits, records OOM deltas,
and uses a private metadata container/state directory. The shell wrapper restores
the usual system slice 40-GiB/8-CPU limits on exit and stops/removes its user slice.
No builds overlap. Do not run another fixture or build until it finishes.
The walkthrough lacks external sink/CDC workloads, so complementary current-version
coverage remains required. Do not count same-build subscription coverage instead.

## Approved scope and verified outcomes

Positive native builtin-schema migrations are deferred beyond M3. Keep the
rejection guard. Kubernetes, operator and Terraform current-native integration
are also deferred under local-only M3. These are explicit capability gaps, not
claims of passing coverage. Historical-only and unrelated workflows remain enabled.

Commit `1380f0ba` adds visible unsupported results and bounds optional final
preflight before process mutation. It checks running, target, rollback and
restored-definition provenance. Unknown CI/development artifacts do not count as
historical releases. Operator image-copy ordering uses the real modifiers.
Independent review found both edge cases, and negative-control tests reproduced
them before repair. Combined Python tests and changed-file Black/Ruff pass.
Evidence: `native-bounded-harness-tests.log`, `native-boundary-review-negative.log`
and `native-boundary-review-fixed.log`. Docker boundaries are mocked here, not
runtime deployment acceptance.

Controllerd owns processes, replica followers own maintained execution, and
adapters retain policy, transactions, protection and transient queries. Mandatory
native startup, transport ownership removal, storage restart fencing and native
profiling inventory are committed and pass targeted checks. Temporary objects
have execution ownership while retaining session visibility. Subscription IDs
are execution-qualified through observations, logging and dependencies.

The last verified default-development candidate passes full multi-process smoke
in both topologies, profiling HTTP, and same-build two-writer promotion with
surviving sessions/subscriptions, independent cleanup and explicit retirement.
These are mounted-binary outcomes, not rebuilt-image CI. Current replay-history
removal postdates those binaries. Evidence: `native-current-build.log`,
`native-current-runtime-evidence.tar.gz`, `native-current-subscriptions-success.tar.gz`
and `native-current-subscriptions-metadata-fixed.log`.

Two subscription attempts OOMed metadata PostgreSQL because the runner omitted
its established 500-connection setting. Exact retry reproduced the failure.
Restoring that setting passed without changing workloads or memory caps. Failure
receipts remain in `aj-native-current-1abefc00` and `aj-native-current-4781f0b7`.
This does not establish rejection of a retired INSERT or distinct-version compatibility.

## Live blockers and unverified outcomes

- Distinct-version acceptance after the latest removals is incomplete. Earlier
  26.46/26.47 prototype evidence shares implementation and plan encoding, not
  arbitrary release compatibility.
- Bounded-memory upsert-index hydration reproducibly OOMs at its original
  3.5-GiB limit. Cause remains unproven. Do not shrink workloads or raise limits
  to hide it. Receipts: `bounded-memory-evidence/aj-native-cluster-6ed29d27/`
  and `aj-native-cluster-c74e55f5/`.
- Full upsert stress previously exceeded the local memory cap. Current
  data-ingest/parallel stress, randomized zippy/restore and bumped-version
  platform acceptance remain incomplete. Earlier index-publication and slow
  source-status cases remain unexplained.
- Adapter-loss acceptance needs a Persist CLI/jobs artifact not present among
  the retained local scratch images.
- Broad CI is not green. Last reviewed broad evidence is
  [Build141419](https://buildkite.com/materialize/test/builds/141419), including
  the unpublished pubsubd image gate. Registry visibility, merge skew, Persist
  JSON-order checking and stale hydration expectations remain unresolved.
  Full bin/fmt also reports unchanged long lines in catalog files. The PR stays draft.

## Worker and essential evidence

Evidence root: `~/.local/share/aj/m3-evidence/native-only/`. Runtime and check
filenames above are relative to `native-driver-scratch/`.
Owned worker: `i-064fa517ec1d8bbdb`, expiry recorded in that directory's
`worker.json`. Use its instance-specific `remote.py`. Check expiry before long work.
All completed runtime fixtures are removed. The last verified candidate was
copied locally and all four hashes matched. Superseded binaries were removed,
with manifests/evidence retained. Local disk headroom was 39 GiB at last check.

Keep heavy work sequential on scratch, never the local VM. Aggregate slice:
40 GiB, no swap, eight CPUs. Build: one Cargo job, 32 GiB, four CPUs. Runtime
containers must stay in the guarded slice. Preserve 30 GiB disk and 2 GiB RAM
headroom. Do not overlap builds and runtime fixtures. Clean owned resources when
finished. Preserve the unrelated `tmp_dec1fdd35343b4a99fe5b65d9e20e2b4b7085ecc719a42c7915575284b56c512.toml`.

Earlier detailed evidence pointers: `native-handoff-before-approved-deferrals.md`
and the [archive](20260903_decoupled_coordination_log_archive.md). Remaining
architectural opportunities and unresolved cases are in the
[follow-up inventory](20260903_decoupled_coordination_followups.md).
`native-introspection-current-audit.md` distinguishes native relays from removed
legacy ownership. Do not remove their SQL inputs without replacement.
The [publication comparison](20260903_decoupled_coordination_publication_interval.md)
and `../logical-component-load/` remain the performance baseline, not measured
benefits from this cleanup.
