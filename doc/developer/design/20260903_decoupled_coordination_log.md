# Decoupled coordination: current handoff

## Objective and next action

Finish native-only ownership, execution and harness cleanup. Preserve adapter
policy, validated transactions, durable read protection, compatible full writers
and explicit retirement. Keep Persist and the 60s publication default unchanged.

Next: inspect delete-heavy ingest upgrade run `aj-native-parallel-6d903002`.
It uses `DeleteDataAtEndOfDay0dtDeploys`, seed42, default 400s duration and replica
sizes, PostgreSQL/MinIO, and pinned debug binaries. Then cover remaining ingestion
workloads, randomized zippy/restore, and the latest compatible-version pair.
Do not reduce workloads, assertions, cadence or retry limits to obtain passes.

## Current outcome

Controllerd owns processes, replica followers own maintained execution, and
adapters retain policy, transactions, protection and transient queries. Native
startup is mandatory. Temporary objects retain execution ownership and session
visibility. Subscription identities remain execution-qualified through observations,
logging and dependencies. Bounded audits found no reachable legacy alternative in
the inspected startup and command-ingress paths. Obsolete replay-history work and
its diagnostics are removed. No overall memory saving is established.

Combined protection publications derive against the current transaction, including
proposed client changes. Frontier-only retries preserve consolidated effects,
incarnation fencing and pending-publication barriers. Targeted checks pass.
Unprofiled normal-INFO runs twice passed the unchanged four-index case at original
limits. Earlier intermittent failures are not declared resolved.

Targeted DEBUG diagnostics inflated application CPU through tracing bookkeeping.
Removing only that diagnostic target reduced measured inner application residence
from 4.54s to 0.66s with similar call counts. Batches were not perfectly matched.
An isolated reproduction confirms rejected spans can still acquire registry IDs.
Do not redesign catalog maps based on the instrumented timings or expand into a
tracing fix without scope agreement. Whole-deployment CPU remains high.

The default-duration single-sensor ingestion upgrade case passed final data
comparison after repeated promotions and explicit retirements. This is same-build
native coverage with mounted binaries, not the full ingestion suite or green CI.
Earlier lifecycle/DDL coverage used distinct 26.46/26.47 package versions sharing
implementation and plan encoding. It is not latest-binary, arbitrary-release or
builtin-migration coverage. Earlier checks also cover both Timely topologies,
surviving subscriptions, source lifecycle/status, metrics and HTTP proxy behavior.

## Live gaps and approved deferrals

- Complete remaining data-ingest workloads, seed42 default-length zippy scenarios
  and actual backup/restore. PostgreSQL CDC deliberately excludes restore.
  Missing zippy fixtures include balancerd, ssh-bastion and compatible Persist CLI/jobs.
- Rebuild and verify the latest compatible-version pair. Adapter-loss coverage also
  needs compatible Persist CLI/jobs. Do not substitute old candidate artifacts.
- Native parallel stress has a repaired one-cycle pass. Earlier timeout,
  two-cycle retirement and bootstrap failures are retained in the evidence.
  Broader current-binary repeat coverage remains incomplete.
- Original-limit debug upsert hydration OOM reproduces on both native and pinned
  upstream base. Decoded rows and processing buffers dominate sampled heaps.
  The retaining queue is not established. No native-specific repair is evidenced.
  Do not raise memory limits or shrink the workload. General input-memory bounds
  are separate scope.
- Positive native builtin-schema migrations, pre-native conversion and current-native
  Kubernetes/operator/Terraform integration are unsupported within M3. Keep guards
  and visible unsupported outcomes without disabling historical/unrelated tests.
- Broad CI is not green. Last reviewed evidence is
  [Build141419](https://buildkite.com/materialize/test/builds/141419). Registry/image
  gates, merge skew, Persist JSON-order checking and stale hydration expectations
  remain unresolved. Full bin/fmt reports unchanged long catalog-file lines.

## Worker and evidence

Evidence root: `/home/ubuntu/.local/share/aj/m3-evidence/native-only/`.
Current investigation directory: `upsert-oom-investigation/` below that root,
mirrored on worker `i-0b3699392b71bd7d2` (16 CPUs, 61 GiB). Its recorded expiry is
2026-11-15 23:00 UTC. Credentials were restored without access-rule changes.
Use `remote_with_cli_credentials.py` in the investigation directory.

Keep heavy work sequential and off the local VM. Runtime aggregate limits are
40 GiB, eight CPUs and no swap. Deployments use four CPUs and 6 GiB each. Builds
use one job, 32 GiB and four CPUs. Preserve 30 GiB disk and 2 GiB available RAM.
Do not overlap builds and runtimes. Cleanup is supervised, including after SIGKILL.
Commits remain local and unpushed. Preserve pinned binaries and source manifests.

Essential pointers in `upsert-oom-investigation/`:
- Current binaries: `native-publication-projection-candidate/` and
  `native-publication-projection-binaries.json`. Build/check receipts:
  `projection-publication-build-evidence.tar.gz`.
- Four-index verification: `index-four-info-unprofiled{,-repeat}-evidence.tar.gz`.
- Ingestion upgrade pass: `data-ingest-0dt-evidence.tar.gz`. Active delete-heavy
  case: `data-ingest-delete-0dt-launch.json`, `native_data_ingest_delete_0dt.py`.
- Observer-cost evidence: `publication-tracing-findings.md`,
  `publication-tracing-control.json`, `publication-leaf-v2-evidence.tar.gz`,
  `publication-info-evidence.tar.gz`, and `tracing-filter-control/`.
  `leaf-calibration-findings.md` preserves the failed first sampler and its repair.
- Upsert/native-baseline evidence and independent review: `findings.md` and
  `comparison-review.md`. Native stress repair: `parallel-retirement-policy-evidence.tar.gz`.
- Zippy prerequisites and restore contract: `zippy-acceptance-audit.md`.
- Earlier receipts, failed runs and detailed pointers are preserved in
  `handoff-before-ingest-verification.md`. Do not treat historical passes as
  acceptance of the latest binary.

Earlier compatible-version lifecycle evidence is in `native-driver-scratch/`,
including `native-history-lifecycle-complete-evidence.tar.gz` and
`native-history-compatible-ddl-evidence.tar.gz`.

The [follow-up inventory](20260903_decoupled_coordination_followups.md) keeps
architectural opportunities separate from this cleanup. The
[publication comparison](20260903_decoupled_coordination_publication_interval.md)
and `logical-component-load/` remain the performance baseline, not measured
benefits of the removals. Preserve native observation SQL inputs until replaced.

Preserve the unrelated
`tmp_dec1fdd35343b4a99fe5b65d9e20e2b4b7085ecc719a42c7915575284b56c512.toml`.
