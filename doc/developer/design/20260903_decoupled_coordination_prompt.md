# Decoupled coordination: implementer session prompt

```text
Work on doc/developer/design/20260903_decoupled_coordination.md.

Working PR: https://github.com/MaterializeInc/materialize/pull/38696
Bookmark: decoupled-coordination
Remote: origin, pointing to the contributor fork, not upstream

Read the design and doc/developer/design/20260903_decoupled_coordination_log.md,
then inspect the current code, worktree, and remote bookmark. Preserve existing
work and account for progress from other sessions. Use the latest handoff and
current code to identify the active milestone in the design's Implementation and
verification section. Choose the next coherent piece toward its observable outcome,
briefly explain that choice, then implement and verify it. Historical proposals
and next steps are context, not a cumulative task list.

Current steering

Check which of these review findings remain unresolved, then choose one coherent
change. Remove resolved steering from this prompt. These are implementation
priorities, not additional design requirements.

Milestone 2 is active. The lifecycle-service scope question is decided in the
design's Lifecycle placement and Query client decisions and the re-cut milestone.
Order the work so each step lands and is verified in-process before the process
boundary moves:

1. Query client inside the adapter, replacing the adapter's direct use of
   controller frontiers and holds: storage frontiers from persist, compute
   frontiers from fast-protocol `Frontiers` responses, peeks and query-local
   dataflows through it, durable client protection as its only protection. The
   client records and reclamation rule are agreed. No remote controller access API.
2. Cluster-side connection split: a cluster accepts one lifecycle connection and
   query connections at once. A new query connection must not replace desired
   state or reset maintained dataflows.
3. Cooperating catalog writers: adapter DDL and lifecycle publication commit
   independently using the agreed protected-generation admission and CAS model.
   Do not route DDL through the lifecycle process.
4. Move the controller bundle out: MV and metric-sink compute installation and
   sink alteration from committed state, then the bundle in its own process with
   catalog following, enactment, and publication as its interface. Table appends
   stay with the adapter.

Milestone 1's performance scope is 100 and 1,000 generated objects, retaining the
shared-view index topology and diagnostics. Larger-scale work and the known
quadratic identical-index notice/dependency costs are deferred. Baseline catalog
snapshot and storage-metadata cloning costs remain. Keep payload, Persist metadata,
and current-state batch footprint distinct in reports, and do not treat observer
timings as isolated subscriber latency or current-state bytes as total disk usage.

Keep the draft PR description accurate about what is implemented and what remains,
with validation status in the PR rather than the design log.

Standing rules

- No production trait methods, coordinator commands, or system-variable semantics
  whose only consumer is a test harness. Bring the need to Aljoscha first.
- A durable record is added only when it carries information that cannot be
  derived from existing catalog state or durable progress. A record whose value
  is constant at birth is a signal to look again.
- Milestone 2 builds the query client with durable client protection for one
  adapter. Multiplicity and isolation of clients stay in milestone 3. Do not
  build a bridge that the design's shape will replace.

Prefer changes that remove a dependency on the originating adapter and demonstrate
that through a production path. Preparatory work is appropriate when it unblocks
that path. Temporary interfaces may be replaced as integration clarifies the
boundary. Do not add parallel machinery merely to preserve them or keep milestone
boundaries tidy. Keep maintained requirements object-owned and client protection
incarnation-scoped, without durable per-query or per-SQL-session bookkeeping.

Treat the design as the agreed boundaries, not a prescribed mechanism. Prefer
the smallest coherent solution that preserves the full capability. Incremental
progress is fine, but do not mistake an intermediate step for completion.
Implementation choices within the agreed boundaries do not require renewed
design approval. Catalog fields, leases, protocol messages, planning placement,
and process topology remain implementation choices.

Bring discoveries, consequential tradeoffs, and scope growth to me, Aljoscha.
Pause affected work when guidance is needed rather than silently narrowing
scope, adding machinery, or changing an agreed boundary. Bring disproportionate
implementation cost to me even when the implementation conforms to the design.

Follow repository instructions and skills. Verify at the changed boundaries,
and seek independent review when the risk warrants it.

Use the draft PR's CI as the default test loop instead of running test suites
locally. You may push changes and iterate on CI failures. Keep cheap local
formatting and checks, and run targeted local tests when useful, not as a
prerequisite for pushing. Follow the mz-debug-ci skill when investigating CI.
Report pending or failed checks explicitly rather than treating a push as
successful validation.

Use draft PR CI for Docker/mzcompose demos and performance workloads. Avoid
running them locally when CI can run them. Local verification should focus on
formatting, compilation, and targeted Rust tests.

Locally, `bin/fmt`, `cargo check`, and the Rust parts of `bin/lint`
(check-cargo, check-formatting, check-python-docs) work. `bin/lint` also runs
checks whose tools are not installed here (npm, helm-docs, trufflehog, zizmor);
CI is the authority for those. Do not record local tooling gaps anywhere.

Focus on regular PR CI for now. Nightly intentionally does not run on this
draft PR, so do not treat its absence as a blocker or try to enable it. We will
start nightly validation once we have a working implementation.

Normally finish the session with one coherent change. Give it a clear commit
message and change description explaining the outcome, rationale, and validation
status, not the chronology of attempts. If blocked, report the blocker rather
than claiming completion.

Append only a minimal dated handoff to
doc/developer/design/20260903_decoupled_coordination_log.md: consequential findings
or decisions, unresolved questions, and the next useful step. Do not record
validation status there at all: CI results, pending checks, formatting or
compile checks, tool availability, and review outcomes are reconstructible from
the PR and are noise in the log. Distinguish proposals from decisions we
reviewed together. Do not rewrite earlier entries. Keep the main design focused
on design, and change its agreed boundaries only after discussing them with me.

You may commit and push progress to this bookmark without asking again.
Prefer jj. In-progress commits and pushes are allowed while iterating. Before
finishing, squash your session's intermediate commits, including fixes and log
updates, into one coherent commit.
You may rewrite and repush your own session's work-in-progress commits for
this purpose. Check for remote changes before pushing. Do not overwrite
others' work, and ask before rewriting other sessions' commits or commits
that others have built on. Report validation failures honestly.

Keep the existing PR as a draft. Do not merge, mark it ready, or push to
upstream or other branches without asking.
```

## Code navigation

- [Catalog implications](../../../src/adapter/src/coord/catalog_implications.rs)
  derive effects from committed changes. MV and metric-sink compute installation
  still have sequencer-side paths.
- [Compute protocol](../../../src/compute-client/src/protocol/command.rs) carries
  separate lifecycle and query connections through
  [transport](../../../src/service/src/transport.rs).
- [StorageCollections](../../../src/storage-client/src/storage_collections.rs)
  owns storage capability accounting and critical since handles. Protected
  handles follow committed bounds. Unprotected handles retain local capability
  accounting and epoch fencing.

Re-read this prompt when compaction happens!
