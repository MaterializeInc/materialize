# Decoupled coordination: current handoff

## Current objective

Finish native-only ownership, execution and harness cleanup. Preserve adapter
policy, validated catalog transactions, durable protection, compatible full
writers and explicit retirement. Keep shared query, table and subscription
machinery. Assess architecture-generated load separately, without changing
Persist or the 60s publication default.

## Next concrete action

Finish verification of the approved compute replay-history removal, then run
current-binary metrics/proxy acceptance and the repaired upsert load-test. Return
to distinct-version compatible-upgrade acceptance after this bounded cleanup.
Do not present same-build promotion as distinct-version or migration coverage.

The replay buffer, cloning/compaction, implementation tests and two exported
history gauges are removed in the worktree. Execution uses no history reader.
Independent review found no blocking issue. Cluster coverage now checks live
collections and command processing, with existing hydration/drop assertions
preserved. The regenerated metric catalog also removes stale entries from prior
native-only removals. No memory improvement has been measured.

Scratch task `aj-native-history-check.service` is running the affected library
suites after successful all-target checks. Evidence under
`native-driver-scratch/`: `native-history-check.log`, `native-history-check.sh`,
`native-history-source-sha256.json` and `native-history-metrics.yml`.
A later local edit only removes an empty comment heading from compute metrics.
Rust removal and upsert fixture changes are not committed yet. The fixture now
has two separate scenarios with unchanged settings, workloads and assertions.
Its launcher unit check passes. Both repaired runtime scenarios still need to run.

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
