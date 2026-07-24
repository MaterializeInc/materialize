# Durable Temporary Objects (SQL-150)

- Associated: SQL-118, database-issues #9973, #9974, #9975, #9976, PR #35807

## The Problem

Temporary views and tables live only in per-envd in-memory state. See `temporary_schemas` at `src/adapter/src/catalog/state.rs` never reach the catalog shard.

This blocks converting `mz_views` and `mz_tables` into materialized views over `mz_catalog_raw`, because such a view would silently drop every temp item. PR #35807 attempted the conversion and was closed for this reason.

It also causes bugs. `minimal_qualification` at `src/adapter/src/catalog.rs` is documented as broken for temp objects, with a workaround. Related bugs: database-issues #9973, #9974, #9975, #9976.

In a multi-envd world (one of the motivations for SQL-118), the problem compounds. If sessions can hop between envds, per-envd in-memory temp state cannot follow.

## Success Criteria

- Temporary views and tables are durable in the catalog shard.
- `mz_views` and `mz_tables` can be converted to `BuiltinMaterializedView`s over `mz_catalog_raw`.
- The design works with N envds running concurrently.
- Both graceful session close and envd crash lead to eventual cleanup of orphaned temp items.
- Session connect and disconnect do not write the catalog shard, so connection churn neither blocks on nor contends with DDL.
- dbt-adapter and `pg_views` behave as they do today.

## Out of Scope

- Temporary indexes and materialized views. Not supported today (database-issues #1017).
- Cross-envd session hop with temp objects following the session.

## Solution Proposal

Temp items become durable catalog items tagged with their owner session. Sessions stay in the `mz_sessions` builtin table, which gains a `deploy_generation` column.

(1) An ephemeral-owner field on items: `ephemeral_owner_session: Option<Uuid>` on `ItemValue` and `SchemaValue` in `src/catalog-protos`. `None` means a normal durable item. `Some(uuid)` means a temp item, visible only to the session with that UUID.

(2) `mz_sessions` stays a `BuiltinTable`, written through group commit as today: a fire-and-forget insert on connect and a retraction on close, off the connect path and off the catalog shard. A new `deploy_generation` column records which envd incarnation owns the session.

Only one envd serves an environment at a time today, fenced by `deploy_generation: u64` (see `FenceToken` at `src/catalog/src/durable/objects.rs`). So `deploy_generation` is enough to identify which envd incarnation owns a given session. When multi-envd (SQL-118) lands and multiple envds serve one environment at once, we'll add a per-process envd identifier next to `deploy_generation`. `EnvironmentId` (`src/sql/src/catalog.rs`, exposed as `mz_environment_id()`) is not that identifier. It names the environment, which every envd serving that environment shares.

Read-your-writes on `mz_sessions` is preserved by the existing `REQUIRED_BUILTIN_TABLES` machinery (`src/adapter/src/coord/appends.rs`): a query depending on `mz_sessions` is deferred until the session's own outstanding builtin-table writes land. Connect itself never blocks on the write.

### Cost

Temp DDL writes zero persist bytes today (all temp state is in memory) but will cost one catalog write per create/drop, and now contends with real DDL for the catalog shard's single writer. This should be small in practice, since a session issues far fewer temp DDLs than queries. Worth benchmarking though.

Session lifecycle costs are unchanged from today: one builtin-table row on connect, one retraction on close, both batched through group commit.

### Write path

`sequence_create_table` at `src/adapter/src/coord/sequencer/inner.rs` already branches on `table.temporary`. Also read `session.uuid()` and pass it to `Op::CreateItem`. Same shape for `sequence_create_view` and `create_temporary_schema`. Existing `Catalog::transact` handles atomicity.

### Read path

`resolve()` at `src/adapter/src/catalog/state.rs` uses one uniform rule. `ephemeral_owner_session = None` is visible to everyone. `Some(uuid) = session.uuid()` is visible to that session. Otherwise the item is hidden. The current `SchemaSpecifier::Temporary` branching goes away.

### `mz_views` / `mz_tables` MV shape

The MV shows every item, regardless of `ephemeral_owner_session`. Session-scoped visibility lives in name resolution, not in the MV filter. This isn't new. `mz_views` today already includes temp views from every session (see `pack_view_update` at `src/adapter/src/catalog/builtin_table_updates.rs`), and per-session visibility is enforced only in `resolve()`. Filtering the MV on `ephemeral_owner_session IS NULL` would silently change what `mz_views`, `pg_views`, and downstream readers (dbt-adapter, catalog introspection) see. So we keep the current shape.

### GC

Graceful close: the session-close hook at `src/adapter/src/coord/command_handler.rs` issues one `Catalog::transact` that drops every item with `ephemeral_owner_session = session.uuid()`. The UUID comes from the in-memory connection metadata, no durable session record is needed. The `mz_sessions` retraction is a separate builtin-table write. Items drop first, so a crash between the two leaves a session row without items rather than orphaned items.

An envd becomes the live owner of the catalog on promotion, not on startup. At that moment the newly-promoted envd drops every ephemeral item and schema from the catalog. Any session that owned them is necessarily dead by then, guaranteed by the deploy-generation fence alone. Stale `mz_sessions` rows from prior incarnations are removed by the existing bootstrap blanket retraction of non-retained system tables (`bootstrap_tables` at `src/adapter/src/coord.rs`).

Multi-envd cleanup is follow-up work. The `deploy_generation` column is the first piece of it. With several envds serving one environment, the boot-time blanket retraction of `mz_sessions` must be replaced by retracting only rows whose generation is older than the current one, which the fence makes safe. Cleaning up after a crashed peer of the same generation additionally needs a per-envd identifier and a durable heartbeat.

Inventory-driven GC (dropping catalog temp items whose owner is absent from `mz_sessions`) also needs an ordering rule to be sound: a session's row must be durable before the session may issue its first temp DDL. Then a cleaner that snapshots the catalog and afterwards reads `mz_sessions` at a fresh oracle read timestamp is guaranteed to see the row of any live owner. Deferring the first temp DDL on the session's builtin-table write notify gives this cheaply, in the same shape as the `REQUIRED_BUILTIN_TABLES` deferral.

### Multi-writer `mz_sessions`

With N envds, several processes append to the `mz_sessions` shard concurrently. The txn-wal protocol already supports this: commits are compare-and-appends against the txns shard, and a conflicting committer retries at a fresh timestamp (see the `conflicting_writes` test at `src/txn-wal/src/txn_write.rs`, which runs many independent handles against one txns shard). Session inserts are keyed by session UUID, so concurrent writers never conflict logically, only on the shard upper, and the standard retry with a fresh oracle write timestamp is safe. Note the retried timestamp must still come from the shared oracle. The `connected_at` value in the row is payload, not a persist timestamp.

What assumes a single writer is the layer above txn-wal, and relaxing it is follow-up work alongside heartbeats:

- Group commit treats sustained compare-and-append conflict as an illegitimate second writer and halts after a bounded number of attempts (`write_to_txns` at `src/adapter/src/coord/appends.rs`). Genuine multi-writer needs this relaxed to plain optimistic retry.
- Each writer aggressively compacts the txns shard (`compact_to`, `compare_and_downgrade_since`). Two uncoordinated writers can race the since token, or advance the since past a peer's committed-but-unapplied writes, which is a reader correctness violation. Compaction needs coordination.
- Writers must share the timestamp oracle. This already holds, the oracle is durable and shared across processes.

## Alternatives

- Durable session records in the catalog shard: a `StateUpdateKind::Session` written on connect and deleted on close, with `mz_sessions` as an MV over `mz_catalog_raw`. This was implemented and then rejected for performance. Connect could not complete before the session record's catalog compare-and-append was durable, and connection churn contended with real DDL for the catalog shard's single writer. Batching session ops into shared catalog commits and bounding the flush rate reduced the commit count but kept both couplings. The records also bought nothing for single-envd GC, since promotion-time cleanup deletes all ephemeral rows justified by the deploy-generation fence alone, without consulting a session inventory.
- Keep temp objects in memory and expose them via a per-envd runtime side-channel. This breaks multi-envd from the start: sessions can only see one envd's temp items.
- Pin sessions to a single envd so they cannot hop. Rules out cross-envd session hop as a future option.
- Use `ConnectionId` as the durable owner key. Does not work: `ConnectionId` is a per-envd `u32` (`src/adapter-types/src/connection.rs`), not durable, and can be reused.
- Introduce a `StateUpdateKind::TemporaryItem` variant distinct from `Item`. Duplicates all Item machinery. An optional field on `ItemValue` achieves the same result with less proto churn.
- Inject builtin items into the catalog shard. Jan considered and rejected this in PR #35807: it adds state that must be migrated across zero-downtime upgrades.

## References

- SQL-150 tracking issue: https://linear.app/materializeinc/issue/SQL-150/add-temporary-objects-to-the-catalog
