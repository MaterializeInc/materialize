# Decoupled coordination: current handoff

## Objective and next action

Finish native-only ownership, execution and harness cleanup. Preserve adapter
policy, validated transactions, durable read protection, compatible full writers
and explicit retirement. Keep Persist and the 60s publication default unchanged.

Next run bounded current-binary parallel-workload promotion acceptance with the
strict native error policy. Then reconcile the remaining index-publication and
source-status cases before claiming broader acceptance. The error-filter unit
checks and shutdown negative control pass, but are not a full stress run.

The upsert investigation reproduced the original-limit debug OOM on pinned
upstream base `d9a2c5dc`, twice. Both native and baseline sampled heaps are dominated
by decoded upsert rows and processing buffers. Native ownership is not required
to reproduce the failure. Startup timing and progress differ, so this does not
exclude a native contribution to severity. No native-specific repair is evidenced.
A general upsert input-memory bound is separate work, not part of this cleanup.
See `upsert-oom-investigation/findings.md` under the evidence root below.

## Verified outcome

Controllerd owns processes, replica followers own maintained execution, and
adapters retain policy, transactions, protection and transient queries. Native
startup is mandatory. Temporary objects carry execution ownership while retaining
session visibility. Subscription identities remain execution-qualified through
observations, logging and dependencies. Bounded independent audits found no
reachable legacy alternative in the inspected startup and command-ingress paths.

Replay-history cloning, compaction and both obsolete gauges are removed. Affected
all-target/library checks, current-binary metrics and HTTP-proxy workflows pass.
The accepted command-size expectation passes the complete compute-client library suite.
The repaired upsert load-test runs its unchanged original scenarios successfully.
No performance or memory saving from cleanup has been measured.

The current local `bin/controllerd` walkthrough and full `0dt ddl` workflow pass
with 26.46/26.47 package versions sharing implementation and plan encoding. They
cover concurrent SQL/DDL/table writers around promotion, warmed execution,
protection, explicit retirement, rejected retired writers, sink authority and
singleton CDC handover. Earlier checks cover both Timely topologies and surviving
subscriptions. This is compatible-prototype coverage using mounted binaries,
not arbitrary-release, builtin-migration or rebuilt-image CI coverage.

## Live gaps and approved deferrals

- Original-limit bounded-memory upsert-index hydration still fails. Native and
  both baseline runs report clusterd OOMKilled/exit137 at 3.5 GiB with ample host
  RAM. The 256-MiB control bounds Persist feedback, not decoded input. Heap stacks
  do not identify the retaining queue. Periodic jemalloc gauges are not exact
  simultaneous accounting. Do not raise limits or shrink workloads to hide this.
- Data-ingest/parallel stress, randomized zippy/restore and bumped-version platform
  acceptance remain incomplete. Earlier index-publication and slow source-status
  cases remain unexplained. Adapter-loss acceptance needs a Persist CLI/jobs artifact.
- Positive native builtin-schema migrations, pre-native conversion and current-native
  Kubernetes/operator/Terraform integration are unsupported within M3. Keep guards
  and visible unsupported outcomes without disabling historical/unrelated tests.
- Broad CI is not green. Last reviewed evidence is
  [Build141419](https://buildkite.com/materialize/test/builds/141419). Registry/image
  gates, merge skew, Persist JSON-order checking and stale hydration expectations
  remain unresolved. Full bin/fmt reports unchanged long catalog-file lines.
  Changed-file checks pass. The PR stays draft.

## Worker and essential evidence

Evidence root: `~/.local/share/aj/m3-evidence/native-only/`.

Owned worker `i-0b3699392b71bd7d2` expires at
`2026-11-13T20:31:57.921677+00:00`. Its helper and receipts are in
`upsert-oom-investigation/`. No test runtime or build remains. The baseline build
succeeded with one Cargo job and unchanged lockfile. SSO refresh expired, but
`remote_with_cli_credentials.py` uses the same role's valid CLI credentials only
in memory, expiring at 20:23 UTC. No access rules changed. The prior worker
`i-064fa517ec1d8bbdb` is terminated and its old helper is not reusable.

Keep heavy work sequential and off the local VM. Worker aggregate limits are
40 GiB, eight CPUs and no swap. Builds use one job, 32 GiB and four CPUs.
Preserve 30 GiB disk and 2 GiB available RAM. All investigation containers were
removed and raw evidence copied locally. Do not overlap builds with test runtimes.

Essential pointers, relative to the evidence root:
- Upsert diagnosis, controls and independent review:
  `upsert-oom-investigation/findings.md` and `comparison-review.md` there.
  Its native/baseline profile archives, repeated-baseline archive, function-level
  reports and build/binary manifests preserve the detailed measurements.
- Local lifecycle: `native-driver-scratch/native-history-lifecycle-complete-evidence.tar.gz`.
- Distinct-version DDL/sink/CDC:
  `native-driver-scratch/native-history-compatible-ddl-evidence.tar.gz`.
- Metrics/proxy and repaired upsert load-test: `native-driver-scratch/` archives
  `native-history-metrics-evidence.tar.gz` and `native-history-upsert-evidence.tar.gz`.
- Runtime/error-policy reviews and detailed earlier outcomes:
  `native-driver-scratch/native-handoff-before-upsert-baseline.md`.
  Both versioned native candidates and their manifests remain under that directory.

The [follow-up inventory](20260903_decoupled_coordination_followups.md) separates
remaining architectural opportunities from this cleanup. The
[publication comparison](20260903_decoupled_coordination_publication_interval.md)
and `logical-component-load/` remain the performance baseline, not measured
benefits of the removals. Preserve native observation SQL inputs until replaced.

Preserve the unrelated
`tmp_dec1fdd35343b4a99fe5b65d9e20e2b4b7085ecc719a42c7915575284b56c512.toml`.
