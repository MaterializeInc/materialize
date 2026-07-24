# Durable Temporary Objects (SQL-150)

- Associated: SQL-118, database-issues #9973, #9974, #9975, #9976, PR #35807

## The Problem

Temporary views and tables live only in per-envd in-memory state. See `temporary_schemas` at `src/adapter/src/catalog/state.rs` never reach the catalog shard.

This blocks converting `mz_views` and `mz_tables` into materialized views over `mz_catalog_raw`, because such a view would silently drop every temp item. PR #35807 attempted the conversion and was closed for this reason.

It also causes bugs. `minimal_qualification` at `src/adapter/src/catalog.rs` is documented as broken for temp objects, with a workaround. Related bugs: database-issues #9973, #9974, #9975, #9976. This change does not fix that bug family. Its root cause is name resolution, not storage. A temp qualified name carries `(Ambient, Temporary)` qualifiers that do not identify the owning session, so humanizing another session's temp object cannot resolve it (the repro in `temporary_objects.slt`, where one connection's DROP error must name another connection's temp view), and two sessions' same-named temp objects compare equal as `QualifiedItemName`s. The follow-up section describes the structural fix.

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


An ephemeral-owner field on items: `ephemeral_owner_session: Option<Uuid>` on `ItemValue` in `src/catalog-protos`. `None` means a normal durable item. `Some(uuid)` means a temp item, visible only to the session with that UUID.

Durable temp items all reference the shared sentinel schema id `SchemaId::User(0)`, the id `SchemaSpecifier::Temporary` maps to. Because every session's temp items live under that one schema id, durable item name uniqueness is scoped by `(schema_id, name, ephemeral_owner_session)` rather than `(schema_id, name)`.

Temp schemas themselves stay per-connection in-memory objects (`temporary_schemas` at `src/adapter/src/catalog/state.rs`) and get no durable record. A temp schema is derived state. Its name is always `mz_temp`, its owner is the session's role, and its contents are exactly the durable items tagged with the session's UUID, so any envd can synthesize it on demand. Durably recording it would buy a self-describing shard (no sentinel), which the follow-up section takes up.

### Cost

Temp DDL writes zero persist bytes today (all temp state is in memory) but will cost one catalog write per create/drop, and now contends with real DDL for the catalog shard's single writer. This should be small in practice, since a session issues far fewer temp DDLs than queries.


### Write path

`sequence_create_table` at `src/adapter/src/coord/sequencer/inner.rs:990` already branches on `table.temporary`. Also read `session.uuid()` and pass it to `Op::CreateItem`. Same shape for `sequence_create_view`. Existing `Catalog::transact` handles atomicity. Temporary schemas will still be in memory and created lazily on creation of the first temporary item in a session.

### Read path

`resolve()` at `src/adapter/src/catalog/state.rs:2131` applies one rule to items. `ephemeral_owner_session = None` is visible to everyone. `Some(uuid) = session.uuid()` is visible to that session. Otherwise the item is hidden. Schema resolution keeps the current `SchemaSpecifier::Temporary` branching, since temp schemas remain in-memory per-connection objects.

### `mz_views` / `mz_tables` MV shape

The MV shows every item, regardless of `ephemeral_owner_session`. Session-scoped visibility lives in name resolution, not in the MV filter. This isn't new. `mz_views` today already includes temp views from every session (see `pack_view_update` at `src/adapter/src/catalog/builtin_table_updates.rs:363`), and per-session visibility is enforced only in `resolve()`. Filtering the MV on `ephemeral_owner_session IS NULL` would silently change what `mz_views`, `pg_views`, and downstream readers (dbt-adapter, catalog introspection) see. So we keep the current shape.

### GC

Graceful close: the session-close hook at `src/adapter/src/coord/command_handler.rs:1988` issues one `Catalog::transact` that drops every item with `ephemeral_owner_session = session.uuid()`.

The UUID comes from the in-memory connection metadata, no durable session record is needed.

An envd becomes the live owner of the catalog on promotion, not on startup. At that moment the newly-promoted envd can drop every temp-object entry from the catalog. Any session that owned them is necessarily dead by then.

Multi-envd cleanup is follow-up work. Once several envds can run concurrently, we will need a durable envd-heartbeat table so any envd can identify dead peers and drop temp items owned by their sessions.

### Follow-up: Multi-writer `mz_sessions`

With N envds, several processes append to the `mz_sessions` shard concurrently. Concurrent inserts are fine because the txn-wal protocol already supports this: commits are compare-and-appends against the txns shard, and a conflicting committer retries at a fresh timestamp.

Each session record will need to be partitioned by a per-envd identifier and need a durable envd-heartbeat table. This is to identify rows left by dead processes for cleanup. We need a durable heartbeat to tell a crashed peer from a live one.

## Follow-up: durable per-session temporary schemas

The plan is to adopt Postgres's model, where each backend's temp
namespace is a real catalog object with a distinct name (`pg_temp_<N>` in
Postgres, with `pg_temp` as an alias resolving to your own). Adapted here:

- A session's first temp DDL durably creates its temp schema with a real
  allocated `SchemaId` and OID and a per-session-unique name (`mz_temp_<N>`).
  `mz_temp` becomes a resolution alias for the session's own schema. The
  choice of `N` is open. Deriving it from the session UUID needs no
  allocation and lets any envd reconstruct the name from a durable item's
  owner field, but produces long names in error messages. An allocated
  counter reads better but adds a per-session allocation.
- That migration adds `ephemeral_owner_session` on `SchemaValue`, together
  with the code that reads it: owner-scoped GC (graceful session close),
  resolve-time visibility, and the `mz_schemas` shape decision. Postgres
  shows `pg_temp_N` rows in `pg_namespace`. Filtering `mz_schemas` on
  `ephemeral_owner_session IS NULL` instead preserves its current temp-free
  contents.
- `SchemaSpecifier::Temporary` and the `SchemaId::User(0)` sentinel retire in
  favor of the real ids (the TODO at `src/sql/src/names.rs` anticipates
  this). That deletes the `minimal_qualification` workaround, since
  cross-session names like `mz_temp_<N>.v1` become resolvable, mirroring
  Postgres's `pg_temp_3.v1` error output. Item name uniqueness can then drop
  its `ephemeral_owner_session` term, because per-session schema ids already
  isolate names.
- It also needs an access-denial rule for other sessions' temp objects once
  they become resolvable (today privacy is enforced by unresolvability), and
  an audit of literal `mz_temp` assumptions: the DDL special cases in
  `src/sql/src/plan/statement/ddl.rs`, normalized `create_sql`, SHOW output,
  `pg_views`, dbt, and `find_temp_schema` at
  `src/adapter/src/catalog/state.rs`.

## Alternatives

- Durable session records in the catalog shard: a `StateUpdateKind::Session` written on connect and deleted on close, with `mz_sessions` as an MV over `mz_catalog_raw`. This was implemented and then rejected for performance. Connect could not complete before the session record's catalog compare-and-append was durable, and connection churn contended with real DDL for the catalog shard's single writer.
- Keep temp objects in memory and expose them via a per-envd runtime side-channel. This breaks multi-envd from the start: sessions can only see one envd's temp items.
- Pin sessions to a single envd so they cannot hop. Rules out cross-envd session hop as a future option.
- Use `ConnectionId` as the durable owner key. Does not work: `ConnectionId` is a per-envd `u32` (`src/adapter-types/src/connection.rs:18`), not durable, and can be reused.
- Introduce a `StateUpdateKind::TemporaryItem` variant distinct from `Item`. Duplicates all Item machinery. An optional field on `ItemValue` achieves the same result with less proto churn.
- Inject builtin items into the catalog shard. Jan considered and rejected this in PR #35807: it adds state that must be migrated across zero-downtime upgrades.

## References

- SQL-150 tracking issue: https://linear.app/materializeinc/issue/SQL-150/add-temporary-objects-to-the-catalog
