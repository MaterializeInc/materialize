# Decoupled coordination: implementer session prompt

```text
Work on doc/developer/design/20260903_decoupled_coordination.md.

Working PR: https://github.com/MaterializeInc/materialize/pull/38696
Bookmark: decoupled-coordination
Remote: origin, pointing to the contributor fork, not upstream

Read the design and the latest handoff in
doc/developer/design/20260903_decoupled_coordination_log.md, consulting earlier
entries for relevant decisions. Inspect the current code, worktree, and remote
bookmark. Preserve existing work and account for progress from other sessions.
Use the handoff and code to identify the active milestone in the design's
Implementation and verification section. Choose the next coherent piece toward
its observable outcome, briefly explain that choice, then implement and verify it.
Historical proposals and next steps are context, not a cumulative task list.

Current steering

Recheck these priorities against the code and latest handoff. Flag resolved
steering for removal rather than accumulating a checklist.

Milestone 2 is active. The ownership cutover is enabled: clusterd replicas enact
compute and storage from the shared committed catalog. Keep the architecture stable
and the native gate enabled. Adapter inventory and SQL observations are passive,
not a second installer. Table/WAL work, webhook ticking, introspection writers and
shard finalization remain adapter-owned and may pause during adapter absence.

Finish the cluster/adapter-loss demonstration and the bounded multi-replica
measurements. Fix demonstrated regressions at their owning boundaries. Review the
complete acceptance setup against actual service configuration and batch related
plumbing corrections. Prefer targeted acceptance runs against available binaries
where practical. Passing unit tests and preparatory cleanup do not replace the
remaining end-to-end proof.

Show compute and storage reconstruction and progress during adapter absence,
including progress by the restarted replica rather than only its surviving sibling.
Preserve physical compaction, successive autonomous curated metrics, resumed queries,
and zero-replica historical recovery assertions. Independent reader holds still
constrain advancement. Compute logs and indexes over them retain replica-local
history semantics, not shutdown persistence. Unavailable diagnostics may remain
unknown, but must not be invented or become execution prerequisites.

EXPLAIN can describe declared indexes before their traces are installed. Keep its
planning candidates separate from actual read admission without weakening read
protection. Surface consequential transaction behavior changes separately.

Distinguish the agreed five-minute reclamation grace from leaks. Observe abandoned
incarnations being reclaimed, then assert release. Do not shorten production grace
or broadly reclaim clients to make tests pass. Pending installation defers bounds
and reclamation, so check its interaction with Kafka takeover against the restart
proof. Bring concrete stalls before proposing coordination machinery. Kafka keeps
its incarnation-based eligibility, local pre-open admission, transactional fencing
and versioned progress, without another lease. Iceberg retains its guarded commit
and fail-closed missing-progress boundaries.

Current-status recovery and live observations are the immediate target. Do not add
durable outage-status history or redesign replica failure detection. Prove the
native path first, then integrate upstream in a separate review boundary unless an
upstream change actually blocks acceptance.

Keep per-build selection keys, but defer cross-build follower repair, version
upgrades, and prewarming-owned selections. Retired-build cleanup is also deferred.
After a generation is fenced, its survivor may remove older builds' selections
and entries. An unfenced owner's entries are off limits.

Milestone 1's performance scope is 100 and 1,000 generated objects, retaining the
shared-view index topology and diagnostics. Larger-scale work and the known
quadratic identical-index notice/dependency costs are deferred. Baseline catalog
snapshot and storage-metadata cloning costs remain. Keep payload, Persist metadata,
and current-state batch footprint distinct in reports, and do not treat observer
timings as isolated subscriber latency or current-state bytes as total disk usage.
At replica cutover, measure follower and publication costs with multiple replicas
at those same bounded sizes. Single-publisher measurements are not that evidence.

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
boundaries tidy. Keep maintained recovery and retention requirements object-owned,
and additional client and execution protection incarnation-scoped, without durable
per-query or per-SQL-session bookkeeping.

Treat the design as the agreed boundaries, not a prescribed mechanism. Prefer
the smallest coherent solution that preserves the full capability. Incremental
progress is fine, but do not mistake an intermediate step for completion.
Implementation choices within the agreed boundaries do not require renewed
design approval. Writer-owned planning and replica-side enactment are decided.
Internal factoring and mechanisms not fixed by the agreed decisions are yours
to choose.

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

Land coherent, reviewable changes. Separate commits are appropriate for distinct
implementation or review boundaries, especially mechanical relocation versus
behavior changes. An intermediate commit is not a reason to stop the work. Commit
messages and change descriptions explain the outcome, rationale, and validation,
not the chronology of attempts. If blocked, report the blocker.

Append only a minimal dated handoff to
doc/developer/design/20260903_decoupled_coordination_log.md: consequential findings
or decisions, unresolved questions, and the next useful step. Do not record
validation status there at all: CI results, pending checks, formatting or
compile checks, tool availability, and review outcomes are reconstructible from
the PR and are noise in the log. Distinguish proposals from decisions we
reviewed together. Do not rewrite earlier entries. Design and prompt bodies are
designer-owned. Propose changes rather than editing them unless Aljoscha explicitly
assigns documentation work.

You may commit and push progress to this bookmark without asking again.
Prefer jj. In-progress commits and pushes are allowed while iterating. Before
finishing, fold fixups and log updates into the relevant coherent commits without
collapsing useful review boundaries merely because they share a session.
You may rewrite and repush your own session's work-in-progress commits for
this purpose. Check for remote changes before pushing. Do not overwrite
others' work, and ask before rewriting other sessions' commits or commits
that others have built on. Report validation failures honestly.

Keep the existing PR as a draft. Do not merge, mark it ready, or push to
upstream or other branches without asking.
```

## Code navigation

- [Catalog implications](../../../src/adapter/src/coord/catalog_implications.rs)
  derive effects from committed changes, including maintained compute installation
  and sink alteration.
- [Compute protocol](../../../src/compute-client/src/protocol/command.rs) carries
  separate lifecycle and query connections through
  [transport](../../../src/service/src/transport.rs).
- [StorageCollections](../../../src/storage-client/src/storage_collections.rs)
  owns storage capability accounting and critical since handles. Protected
  handles follow committed bounds. Unprotected handles retain local capability
  accounting and epoch fencing.

Re-read this prompt when compaction happens!
