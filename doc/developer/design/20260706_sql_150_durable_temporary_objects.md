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
- dbt-adapter and `pg_views` behave as they do today.

## Out of Scope

- Temporary indexes and materialized views. Not supported today (database-issues #1017).
- Cross-envd session hop with temp objects following the session.

## Solution Proposal

Two additions to the durable catalog.

(1) Durable session records: a new `StateUpdateKind::Session { uuid, deploy_generation, connection_id, role_id, client_ip, connected_at }`. The envd owning a session writes this on connect and deletes it on graceful close. This lets `mz_sessions` become a catalog-derived MV, and gives GC the durable session inventory it needs.

Only one envd serves an environment at a time today, fenced by `deploy_generation: u64` (see `FenceToken` at `src/catalog/src/durable/objects.rs:1319`). So `deploy_generation` is enough to identify which envd incarnation owns a given session. When multi-envd (SQL-118) lands and multiple envds serve one environment at once, we'll add a per-process envd identifier next to `deploy_generation`. `EnvironmentId` (`src/sql/src/catalog.rs:1278`, exposed as `mz_environment_id()`) is not that identifier. It names the environment, which every envd serving that environment shares.

(2) An ephemeral-owner field on items: `ephemeral_owner_session: Option<Uuid>` on `ItemValue` and `SchemaValue` in `src/catalog-protos`. `None` means a normal durable item. `Some(uuid)` means a temp item, visible only to the session with that UUID.

### Cost

Session writes move shards, they don't multiply. Today, opening a session writes one row into the builtin `mz_sessions` shard, and closing it writes a delete. With this change the same two writes go to the catalog shard instead, and the `mz_sessions` shard disappears once it becomes an MV over the catalog. Total persist writes per session lifecycle are the same.

Two second-order costs to flag. First, temp DDL writes zero persist bytes today (all temp state is in memory) but will cost one catalog write per create/drop. Second, the catalog shard has a single writer, so temp DDL now contends with real DDL for that writer. Both should be small in practice, since a session issues far fewer temp DDLs than queries. Worth benchmarking though.

### Write path

`sequence_create_table` at `src/adapter/src/coord/sequencer/inner.rs:990` already branches on `table.temporary`. Also read `session.uuid()` and pass it to `Op::CreateItem`. Same shape for `sequence_create_view` and `create_temporary_schema`. Existing `Catalog::transact` handles atomicity.

### Read path

`resolve()` at `src/adapter/src/catalog/state.rs:2131` uses one uniform rule. `ephemeral_owner_session = None` is visible to everyone. `Some(uuid) = session.uuid()` is visible to that session. Otherwise the item is hidden. The current `SchemaSpecifier::Temporary` branching goes away.

### `mz_views` / `mz_tables` MV shape

The MV shows every item, regardless of `ephemeral_owner_session`. Session-scoped visibility lives in name resolution, not in the MV filter. This isn't new. `mz_views` today already includes temp views from every session (see `pack_view_update` at `src/adapter/src/catalog/builtin_table_updates.rs:363`), and per-session visibility is enforced only in `resolve()`. Filtering the MV on `ephemeral_owner_session IS NULL` would silently change what `mz_views`, `pg_views`, and downstream readers (dbt-adapter, catalog introspection) see. So we keep the current shape.

### GC

Graceful close: the session-close hook at `src/adapter/src/coord/command_handler.rs:1988` issues one `Catalog::transact` that drops the session record and every item with `ephemeral_owner_session = session.uuid()`.

An envd becomes the live owner of the catalog on promotion, not on startup. At that moment the newly-promoted envd can drop every temp-object entry from the catalog. Any session that owned them is necessarily dead by then.

Multi-envd cleanup is follow-up work. Once several envds can run concurrently, we will need a durable envd-heartbeat table so any envd can identify dead peers and drop temp items owned by their sessions.

## Alternatives

- Keep temp objects in memory and expose them via a per-envd runtime side-channel. This breaks multi-envd from the start: sessions can only see one envd's temp items.
- Pin sessions to a single envd so they cannot hop. Rules out cross-envd session hop as a future option.
- Use `ConnectionId` as the durable owner key. Does not work: `ConnectionId` is a per-envd `u32` (`src/adapter-types/src/connection.rs:18`), not durable, and can be reused.
- Introduce a `StateUpdateKind::TemporaryItem` variant distinct from `Item`. Duplicates all Item machinery. An optional field on `ItemValue` achieves the same result with less proto churn.
- Inject builtin items into the catalog shard. Jan considered and rejected this in PR #35807: it adds state that must be migrated across zero-downtime upgrades.

## References

- SQL-150 tracking issue: https://linear.app/materializeinc/issue/SQL-150/add-temporary-objects-to-the-catalog
