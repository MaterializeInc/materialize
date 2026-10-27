# Session catalog snapshot cache

- Associated:
  [database-issues#9593](https://github.com/MaterializeInc/database-issues/issues/9593),
  [materialize#37533](https://github.com/MaterializeInc/materialize/pull/37533)

## The Problem

Every statement handled by a session task fetched a fresh catalog snapshot
via a `Command::CatalogSnapshot` round-trip to the Coordinator, and fast path
peeks did two of them (one in `declare` or `Bind`, one in the frontend peek
sequencing). The handler is just an `Arc::clone`, so the cost is pure message
volume on the Coordinator main loop. On query-heavy environments,
`command-catalog_snapshot` was by far the largest consumer of main loop time,
capping QPS and adding queueing delay for all other messages.

## The Design

Sessions cache the latest catalog snapshot and reuse it as long as the
catalog is unchanged. The pieces:

- The catalog's transient revision is mirrored in a shared atomic,
  `Catalog::shared_transient_revision`, which all clones of a catalog share.
  The store happens inside `Catalog::transact`, adjacent to the
  `transient_revision` increment.
- `PeekClient` holds a `Weak<Catalog>`, seeded at connection startup from the
  `StartupResponse` catalog. A statement gets a cache hit when the `Weak`
  upgrades and the snapshot reports itself current
  (`Catalog::transient_revision_is_current`, which compares the snapshot's
  own revision against the shared atomic the snapshot itself carries, so the
  atomic needs no plumbing). Anything else falls back to the
  `Command::CatalogSnapshot` round-trip and re-populates the cache.

## Correctness

The invariant that makes the cache correct: the atomic is bumped inside
`Catalog::transact`, *before* `transact` returns. Everything that can reveal
a transaction's effects (responses, notices, builtin table writes) happens
after `transact` returns. So no client can observe evidence of a catalog
change before the bump, and a session that has observed such evidence is
guaranteed to see the bump on its next atomic load (delivery of the evidence
provides the happens-before edge). A cached snapshot with a matching revision
is therefore bit-identical to what a fresh `Command::CatalogSnapshot` would
return. The bump must not move to a later point (for example the end of
message handling): responses are sent mid-message, and a bump after the
response reopens a read-your-writes race.

On a mismatch, sessions refresh via the ordinary `Command::CatalogSnapshot`
round-trip, which is processed between Coordinator messages and therefore
never observes a mid-message intermediate state. This is also why the catalog
is *pulled* on miss rather than *pushed* into a shared slot on change: a push
would have to prove consistency at every publish point.

In-place mutations of the Coordinator's catalog that do not bump the revision
must remain invisible to sessions (see the corresponding entry in
`guide-adapter.md`). The known ones at the time of writing: plan side-cache
fills (`set_optimized_plan` / `set_physical_plan` / `set_dataflow_metainfo`,
read only by Coordinator-side paths like `EXPLAIN <existing object>` and
bootstrap), `EXPLAIN REPLAN`'s save-and-restore of `system_configuration`
(net-zero), bootstrap (before any session exists), and
`drop_temporary_schema` on connection terminate (connection-scoped,
unreachable from other sessions).

A failed `Weak::upgrade` is an expected path, not a bug: any in-place
mutation of the Coordinator's catalog (including revision-preserving ones)
moves it to a new allocation via `Arc::make_mut`, killing outstanding
`Weak`s. Both a revision mismatch and a failed upgrade route to the same
refetch.

The cache does not violate the adapter's "no local-only assumptions"
invariant any more than `Command::CatalogSnapshot` itself does: both reflect
only this process's catalog. Read-only 0dt instances are safe because they
halt and reboot on external catalog changes and on promotion. If catalog
changes ever start being applied from other writers (multi-`environmentd`),
that application path must bump the shared revision too.

## Alternatives

- **Strong `Arc` in the session instead of `Weak`**: rejected because idle
  sessions would pin superseded catalog versions. `imbl::OrdMap` structural
  sharing bounds each pinned version's marginal footprint to the path-copied
  nodes (roughly tens of KB per catalog transaction), but with tens of
  thousands of sessions and DDL churn this still adds up to a slow,
  hard-to-diagnose memory leak shape. With `Weak`, reclamation is prompt and
  deterministic, and memory plots stay legible.
- **Pushing snapshots into a shared slot (`ArcSwap`/watch) instead of
  pulling on miss**: rejected, see Correctness above.
- **Interior mutability so `catalog_snapshot` takes `&self`**: `Cell` does
  not compile (pgwire's `Send` futures hold `&SessionClient` across await
  points, which requires `Sync`), a `Mutex` works but a survey of call sites
  found that every caller either already had `&mut` or trivially could, so
  plain `&mut self` won.

## Performance characteristics

- Steady-state hit: one `Weak::upgrade` plus one atomic load, no Coordinator
  interaction.
- Misses scale with `active_sessions x catalog mutation rate`. Every
  revision bump invalidates every session's cache. Temporary-object DDL goes
  through `transact` and therefore also bumps. If production data (the
  `mz_catalog_snapshot_cache` miss counter) shows temp-DDL churn defeating
  the cache, a possible follow-up is to exempt connection-local temp-item
  transactions from the global bump. Other sessions cannot see those items,
  but the owning session must then still be invalidated through a
  session-local mechanism.
- The design is never worse than the pre-cache behavior: the worst case
  degenerates to a round-trip per statement, which was the status quo.

## Observability

- `mz_catalog_snapshot_cache{context, result}`: cache hits and misses.
- `mz_catalog_arc_strong_count` / `mz_catalog_arc_weak_count`: sampled by
  the catalog info metrics task; in-flight snapshot users, and how many
  sessions cache the current catalog version.
- `mz_catalog_snapshot_seconds` now only observes misses.
- Rollout acceptance signal: the `command-catalog_snapshot` message kind in
  `mz_slow_message_handling_sum` drops to a periodic trickle (system
  parameter sync and the info metrics task remain on the round-trip).
