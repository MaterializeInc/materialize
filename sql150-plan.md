# SQL-150 Durable Temporary Objects — Implementation Plan

Design doc: doc/developer/design/20260706_sql_150_durable_temporary_objects.md
Working copy: jj change wmzuwkkq on top of main (a60edac7).

## Facts from exploration (verified by explore agents)

### Durable catalog / catalog-protos
- catalog-protos are HAND-WRITTEN serde Rust structs serialized as JSON (Jsonb), NOT .proto.
  `objects.rs` = current; `objects_vNN.rs` = frozen snapshots. CATALOG_VERSION = 90
  (src/catalog-protos/src/lib.rs:38). build.rs hashes into objects_hashes.json.
  Test: objects.rs must byte-equal objects_v{CURRENT}.rs.
- Version bump steps (90→91): lib.rs bump + `pub mod objects_v91;`; edit objects.rs; copy to
  objects_v91.rs; update objects_hashes.json; upgrade.rs: add v91 to objects! macro list,
  `mod v90_to_v91;`, dispatch arm `90 => run_versioned_upgrade(..., v90_to_v91::upgrade)`;
  create src/catalog/src/durable/upgrade/v90_to_v91.rs; generate encoding snapshot via
  `cargo test ... generate_missing_encodings -- --ignored` (snapshots dir under durable/upgrade/snapshots/).
- StateUpdateKind (durable) at src/catalog/src/durable/objects/state_update.rs:234 — tuple
  (protoKey, protoValue) variants. Adding `Session` variant touches:
  - catalog-protos objects.rs: SessionKey/SessionValue structs + wrapper `Session {key,value}` +
    proto enum StateUpdateKind variant (pattern: NetworkPolicy at objects.rs:320/326, wrapper objects_v90.rs:2121, enum objects_v90.rs:1981)
  - state_update.rs: variant (:234), collection_type() (:278), try_into memory (:456 — or None arm :562),
    into_proto_owned (:618), from_proto (:712)
  - durable/objects.rs: high-level Session struct + DurableType impl (pattern NetworkPolicy :253/:262),
    in-crate SessionKey/SessionValue near :1568
  - serialization.rs: RustType impls (pattern :703/:717)
  - debug.rs: CollectionType enum :55, collection_impl! :205, Trace struct :348 + Trace::new :376
  - persist.rs: snapshot apply match :792, Trace::from_snapshot :2083
  - durable/objects.rs Snapshot struct :1303 + Snapshot::iter()/from_batch state_update.rs:162-224
  - transaction.rs: TableTransaction field :84, new() :155/:234, into_parts :2634,
    TransactionBatch :2911, is_empty() :2976, commit_op destructure :2450 + chain :2481,
    from_txn_batch state_update.rs:134; plus insert_session/remove_sessions/get_sessions methods
    (pattern: insert_network_policy :671, remove_items :1409 delete_by_keys, get_network_policies :2321)
- ItemValue proto at catalog-protos objects.rs:231 (schema_id, name, definition, owner_id,
  privileges, oid, global_id, extra_versions), key ItemKey{gid}. SchemaValue at :205
  (database_id: Option, name, owner_id, privileges, oid), key SchemaKey{id}.
- In-crate ItemValue durable/objects.rs:1493 (create_sql not definition), SchemaValue :1477.
  RustType: serialization.rs:545 (Item), :509 (Schema). Constructors: Item::into_key_value :625,
  from_key_value :638; Schema::into_key_value :145; transaction.rs insert_item :753/:767,
  insert_schema :353/:376.
- No Uuid anywhere in catalog-protos (no uuid dep). Decision needed: Option<Uuid> w/ serde vs Option<String>.
  Check proptest Arbitrary derives on proto structs before deciding.
- Migration: new Option field on Item/Schema needs explicit backfill migration (v89_to_v90 pattern:
  json_compatible! for unchanged types, MigrationAction::Update rewriting each changed record).
  Item/Schema are migratable (not always-deserializable).
- FenceToken { deploy_generation: u64, epoch } at durable/objects.rs:1349.

### Adapter temp-object lifecycle
- temporary_schemas: imbl::OrdMap<ConnectionId, Schema> at adapter/src/catalog/state.rs:170.
  Readers/writers: state.rs 827(get_temp_items) 840(has_temporary_schema) 1698(resolve_schema_in_database
  mz_temp) 1726(try_get_schema (Ambient,Temporary)) 1762(find_temp_schema) 1842(create_temporary_schema
  — id=SchemaSpecifier::Temporary, oid=INVALID_OID, name=MZ_TEMP_SCHEMA);
  catalog.rs 1064(item_exists_in_temp_schemas) 1073(drop_temporary_schema) 1924(get_temporary_oids);
  apply.rs 1179 & 2050 (lazy create on TemporaryItem apply), 1741(get_schema_mut).
- resolve() at state.rs:2161; temp fast path 2186-2196; loop uses try_get_schema 2199-2209.
  effective_search_path state.rs:2092-2123 pushes (Ambient, Temporary) first when include_temp_schema.
- Temp items get USER CatalogItemIds (allocate_user_id at inner.rs:1007 table / create_view.rs:299).
  Op::CreateItem transact.rs:165. Temp = item.conn_id().is_some() (memory/objects.rs:2035, conn_id 1974 —
  only View/Index/Table).
- THE BYPASS: transact.rs:1733 in transact_op — is_temporary → allocate_oid only, push
  TemporaryItem into temporary_item_updates (in-memory), else tx.insert_user_item (durable).
  Re-entry: transact_inner 953-993 wraps as memory StateUpdateKind::TemporaryItem, applies to state.
  should_audit_log_item = !is_temporary (transact.rs:429).
  Non-temp item depending on temp item is rejected (transact.rs:1768-1780).
  temporary_ids pre-scan transact.rs:631-664.
- Session close: handle_terminate command_handler.rs:1969-2016: clear_connection, drop_temp_items
  (ddl.rs:927 — one Op::DropObjects via object_dependents), drop_temporary_schema, retract
  pack_session_update Diff::MINUS_ONE (deferred, 2009-2015).
- Session start: handle_startup command_handler.rs ~819-861; ConnMeta built 835-848 (uuid, conn_id,
  client_ip, authenticated_role, connected_at = self.now()); pack_session_update(conn, Diff::ONE) :849;
  active_conns insert :852. mz_sessions in REQUIRED_BUILTIN_TABLES (appends.rs).
- session uuid: Session.uuid (adapter/src/session.rs:77, accessor :279), ConnMeta.uuid (coord.rs:1312,
  accessor :1355). Uuid::new_v4 or SessionConfig. Command::Startup carries it (client.rs:260).
- minimal_qualification workaround: adapter/src/catalog.rs:2295-2357 (temp → bare name; issues 9973/9974).
  Test: test_minimal_qualification catalog.rs:2431. temporary_objects.slt regression file.

### Builtins / conversion machinery
- MZ_CATALOG_RAW: BuiltinSource, mz_internal, DataSourceDesc::Catalog, single JSONB col `data`,
  builtin/mz_internal.rs:40-51; persist_desc() durable.rs:423; wired to catalog shard open.rs:647-657.
  Rows: {kind, key, value}. access: vec![] (system only).
- MZ_TABLES BuiltinTable builtin/mz_catalog.rs:934-953 (id, oid, schema_id, name, owner_id,
  privileges, create_sql?, redacted_create_sql?, source_id?; is_retained_metrics_object: true).
- MZ_VIEWS BuiltinTable mz_catalog.rs:1270-1289 (id, oid, schema_id, name, definition, owner_id,
  privileges, create_sql, redacted_create_sql).
- MZ_SESSIONS BuiltinTable mz_internal.rs:3097-3110 (id uuid, connection_id uint4, role_id text,
  client_ip text?, connected_at timestamptz).
- BuiltinMaterializedView struct builtin.rs:596-623; sql = "IN CLUSTER mz_catalog_server WITH (ASSERT
  NOT NULL ...) AS SELECT ...". Helpers: mz_internal.parse_catalog_id, parse_catalog_privileges,
  parse_catalog_create_sql. Example MZ_DATABASES mz_catalog.rs:375-439; MZ_POSTGRES_SOURCES mz_internal.rs:52-118.
  Item create_sql path: data->'value'->'definition'->'V1'->>'create_sql'; item id: data->'key'->'gid'.
- Migration: src/adapter/src/catalog/open/builtin_schema_migration.rs. MIGRATIONS list :84-305;
  MigrationStep::replacement(version, CatalogItemType::MaterializedView, schema, name).
  Latest entries "26.34.0-dev.0" (mz_postgres_sources/mz_kafka_sources :288-303). Need current version.
  Replacement → new shard, old shard IDs → txn.insert_unfinalized_shards → storage finalize_shards_task
  (storage_collections.rs:1300,525). Fingerprint guard update_fingerprints :1060 panics on unlisted change.
  mz_catalog_raw itself excluded :641. Entry point run() from open.rs:492; cleanup_action awaited open.rs:520.
- pack_table_update builtin_table_updates.rs:494-549, pack_view_update :812-857, pack_session_update
  :1446-1463. Dispatch generate_builtin_table_update :176-178 (Table), :352-353 (View).
- Fingerprint: BuiltinTable/Source = desc shape; BMV = full create_sql. Changing any converted MV's SQL
  (e.g. mz_schemas filter) needs a new replacement step.
- Past conversions: 9693291dd4 (mz_sources), d777d0f6da (mz_indexes), 584bb9030c (mz_audit_events),
  34813f0a3f (mz_postgres_sources/mz_kafka_sources) — use as diff templates.

## Decisions (working)
1. Owner key: ephemeral_owner_session as Uuid in JSON (string form). Proto layer: check
   proptest derives; prefer Option<Uuid> with uuid serde; fall back to Option<String>.
2. SessionValue = { deploy_generation: u64, connection_id: u32, role_id: RoleId, client_ip:
   Option<String>, connected_at: u64 (EpochMillis) }; SessionKey = { uuid }.
3. Temp schemas become REAL durable schemas: real SchemaId + real OID, name "mz_temp",
   ephemeral_owner_session = Some(session uuid). NOT in ambient by-name map (name collides across
   sessions). In-memory placement + per-session index: decide when reading apply.rs/state.rs
   (Option A: keep temporary_schemas map keyed by ConnectionId, translate uuid→conn via durable
   Session records at apply time; Option B: store in ambient_schemas_by_id + index uuid→SchemaId,
   thread session uuid into ConnCatalog). Bias: whichever keeps resolve() call sites stable.
4. Durable temp item apply: create_sql re-parse at apply time is a hazard (mz_temp resolution
   without conn ctx). Check how apply builds CatalogItem for in-process transacts vs boot. Boot
   (writable): GC temp rows BEFORE item parse. Read-only/savepoint catalog: filter out ephemeral
   items+schemas+sessions when applying.
5. GC on promotion = at writable durable-catalog open (deploy_generation fence = ownership):
   delete all Session rows + all ephemeral items/schemas in the open/migration transaction, before
   adapter parses items.
6. Graceful close: single Catalog::transact that removes session record + temp items + temp schema.
   Extend handle_terminate/drop_temp_items; new Op (e.g. Op::SessionEnd or fold into DropObjects).
   Startup: durable insert of Session record (replaces pack_session_update ONE). Accept coordinator
   catalog-transact on connect per design "writes move shards".
7. mz_sessions MV over mz_catalog_raw kind='Session'; connected_at via to_timestamp(ms/1000.0).
   mz_tables/mz_views MVs show ALL items incl. ephemeral (design: visibility lives in resolve()).
   mz_schemas MV: check current handling; likely needs filter ephemeral IS NULL to preserve behavior
   → new replacement step for mz_schemas too. Verify what schema_id temp rows show today in
   mz_views (pack uses SchemaSpecifier::Temporary → ?) and pg_views join behavior; preserve.
8. Builtin migration steps at next dev version (check current version in MIGRATIONS, likely
   "26.35.0-dev.0" or later — read src/MZ_VERSION or Cargo.toml environmentd version):
   mz_sessions (mz_internal), mz_tables, mz_views (mz_catalog), maybe mz_schemas replacement.
   Old shards auto-finalized via unfinalized_shards (user's step 5 = these steps).

## Stages
1. **Durable layer** (no behavior change): catalog-protos v91 (Session kinds + ephemeral fields),
   durable wiring, Transaction APIs (insert_session, remove_sessions, get_sessions,
   item/schema insert with ephemeral owner, remove_ephemeral_* helpers), v90_to_v91 migration,
   encoding snapshots. cargo test -p mz-catalog-protos, cargo check -p mz-catalog.
2. **Session records live**: startup insert / terminate delete; boot GC of stale sessions;
   mz_sessions → BMV + migration step + drop pack_session_update + remove from REQUIRED_BUILTIN_TABLES.
3. **Durable temp objects**: transact.rs temp branch → durable inserts (+ lazy durable temp schema
   in same tx); apply/state/resolve rework; session-close single-transact GC; boot GC of ephemeral
   items/schemas; keep minimal_qualification workaround (revisit after).
4. **mz_tables/mz_views → BMVs**: builtin statics, migration steps, remove pack dispatch,
   mz_schemas filter if needed.
5. **Tests/polish**: temporary_objects.slt, catalog upgrade test snapshots, builtin migration list
   comments, bin/fmt, bin/lint, cargo check full, targeted cargo tests, sqllogictests.

## Progress log
- [x] Exploration (3 agents) — reports summarized above.
- [x] Stage 1 (durable layer, jj change wmzuwkkq):
  - catalog-protos v91: SessionKey{uuid}/SessionValue{deploy_generation,connection_id,role_id,
    client_ip,connected_at}/Session wrapper + StateUpdateKind::Session; ephemeral_owner_session
    on ItemValue+SchemaValue; uuid dep (serde feature); any_uuid() proptest helper in objects.rs;
    objects_v91.rs snapshot; hashes updated. CATALOG_VERSION=91.
  - catalog durable: variant wired through state_update.rs (memory conversion = None arm /
    "not exposed"), objects.rs (high-level Session + DurableType, in-crate KVs, Snapshot.sessions),
    serialization.rs RustType impls, debug.rs (CollectionType::Session, SessionCollection,
    Trace.sessions), persist.rs (2 matches), transaction.rs:
    * Transaction.sessions TableTransaction; insert_session/remove_sessions(&BTreeSet<Uuid>)/
      get_sessions; insert_temporary_schema(owner_session,...) allocates user SchemaId+oid,
      name mz_temp; insert_schema/insert_item/insert_user_item take ephemeral_owner_session param;
      schema_unique_fn scoped by ephemeral owner; SessionValue in impl_no_unique_name.
    * get_op_updates: sessions in "Not representable as StateUpdate" group (memory doesn't track).
  - memory/objects.rs: Schema.ephemeral_owner_session field + conversions;
    From<CatalogEntry> for durable::Item sets None (Stage 3: carry owner through CatalogEntry).
  - upgrade v90_to_v91: backfill None on Item/Schema; json_compatible impls; unit test;
    encoding snapshot objects_v91.txt generated.
  - catalog-debug: for_collection! + dump wiring.
  - VERIFIED: cargo check --workspace clean; nextest -p mz-catalog --lib 247 passed;
    -p mz-catalog-protos 11 passed. Cargo.lock diff = uuid edge only. bin/fmt run
    (buf failure = tool not installed, unrelated).
- [ ] Stage 2 (in progress): startup/terminate durable session writes (new Op variants in
  adapter transact.rs), boot GC of stale sessions at writable open, mz_sessions → BMV
  ("26.35.0-dev.0" replacement step), remove pack_session_update + REQUIRED_BUILTIN_TABLES entry.
- [x] Stage 3 DONE (jj change wumquzuu) — as designed below, EXCEPT: no durable temp schema rows
  (sentinel SchemaId::User(0) kept; SchemaValue.ephemeral_owner_session exists but unpopulated);
  TemporaryItem machinery + BootstrapStateUpdateKind deleted; sessions exposed to memory
  (session_conns_by_uuid / session_uuids_by_conn on CatalogState, fed via get_op_updates fix in
  durable transaction.rs — sessions must be in get_op_updates or in-process transacts never
  populate the maps, was a boot-crash bug); state.durable_item(entry) replaces
  From<CatalogEntry> for durable::Item; handle_terminate single transact (DropObjects+DropSession);
  boot GC extended with remove_ephemeral_items(); item uniqueness scoped by owner.
  Verified: temp lifecycle, same-name cross-session, kill -9 GC, DISCARD, temporary_objects.slt.
- [x] Stage 4 DONE (jj change wnvwzvvn): mz_tables/mz_views → BMVs over mz_catalog_raw
  (user arm + builtin arm via NEW generated views mz_internal.mz_builtin_tables/mz_builtin_views;
  mz_builtin_views excludes itself → mz_builtin_views absent from mz_views/mz_objects, documented);
  temp rows keep schema_id sentinel "0" via CASE; parse_catalog_create_sql extended
  (view 'definition', table 'source_id'); pack_table_update/pack_view_update deleted;
  replacement steps at 26.35.0-dev.0 for mz_sessions/mz_tables/mz_views; oids 17107-17111;
  test expectations updated (oid.slt, information_schema_tables.slt, catalog.td,
  show_create_system_objects.slt, mz_catalog_server_index_accounting.slt rewritten for s-id shifts).
  NO mz_schemas change needed (no durable temp schema rows). Old shards auto-finalized via
  unfinalized_shards. Clippy clean; bin/lint blocked by missing local tools (shellcheck/gnu-sed/buf).
- Stage 3 original design notes:
  * Durable: transact.rs temp branch (adapter transact.rs ~1733 "is_temporary") now ALSO writes
    durable rows: lazily tx.insert_temporary_schema(owner uuid) on first temp DDL per session
    (record real SchemaId in state map), tx.insert_user_item(..., Some(uuid)) with schema_id =
    that real id. Session uuid must be threaded into Op::CreateItem or resolved from conn via
    state session map (Op::CreateItem gets no new field if state maps conn→uuid).
  * Memory stays as today: temporary_schemas keyed by ConnectionId, SchemaSpecifier::Temporary,
    resolution untouched, minimal_qualification workaround stays (flag as follow-up).
  * NEW memory state: CatalogState.active_sessions BTreeMap<Uuid, u32(conn)> (+ reverse index)
    maintained by exposing StateUpdateKind::Session to memory layer (undo the "not exposed" choice);
    plus temp_schemas_by_owner: BTreeMap<Uuid, SchemaId> (real durable ids) for round-trip.
  * Apply: durable Item updates with ephemeral_owner_session=Some route into the
    apply_temporary_item_update flow (translate uuid→conn, patch conn_id — create_sql replan
    already works there today: apply.rs:1153-1282). Durable Schema updates with Some(owner) do NOT
    create a normal schema; they only update temp_schemas_by_owner (memory temp schema stays
    lazily created per conn). Read-only mode applies these fine (sessions map from old gen).
  * Old TemporaryItem in-memory update machinery: delete (durable rows now flow through apply).
    should_audit_log_item stays !temporary.
  * Session close: extend handle_terminate to one catalog_transact: Op::DropObjects(temp items)
    + drop schema + Op::DropSession. Boot GC in persist.rs open_inner extends to remove all
    ephemeral items+schemas (same txn as session GC), BEFORE adapter parses snapshot.
  * From<CatalogEntry> for durable::Item can't know uuid/schema_id (bare From) — fix up at the
    call sites that update temp items (transact update_item), using state maps.
- [ ] Stage 4 — mz_tables/mz_views → BMVs showing ALL rows (incl. ephemeral; matches today).
  ALSO: mz_schemas MV must gain filter `data->'value'->'ephemeral_owner_session' IS NULL`
  (+ replacement step) or temp schemas would newly appear in mz_schemas/pg_namespace → behavior
  change vs today. Check pg_views/pg_class joins. Temp rows in mz_tables/mz_views will show the
  REAL durable schema_id (u123) instead of today's Temporary-spec sentinel — check what
  pack_table_update prints today for temp schema_id and flag the diff.
- [ ] Stage 5
### Stage 2 detail (done pending smoke test)
- durable GC of ALL sessions at open_inner (persist.rs, mode != Readonly), deploy_generation()
  sync accessor on ReadOnlyDurableCatalogState.
- adapter: Op::CreateSession/DropSession (transact.rs enum + transact_op arms + ddl.rs
  validate_resource_limits no-op arm), handle_startup does catalog_transact(None, CreateSession)
  skipped when controller.read_only() (read-only envd reboots at promotion; today's code also
  only buffers and never flushes), handle_terminate ditto with warn on error.
  pack_session_update deleted; REQUIRED_BUILTIN_TABLES now empty (machinery kept).
- builtins: MZ_SESSIONS → BuiltinMaterializedView (oid MV_MZ_SESSIONS_OID=17107, old
  TABLE_MZ_SESSIONS_OID=16763 const removed), SQL over mz_catalog_raw kind='Session',
  to_timestamp(millis/1000.0) pattern, parse_catalog_id for role_id; Builtin::MaterializedView
  registration; MigrationStep::replacement("26.35.0-dev.0", MV, MZ_INTERNAL_SCHEMA, mz_sessions).
- tests updated: oid.slt (16763 removed, 17107 added), information_schema_tables.slt
  (BASE TABLE→MATERIALIZED VIEW), testdrive/catalog.td (mz_sessions moved tables→MVs list).
- envd boot smoke test running in background (alternate ports 1687x, log at scratchpad/envd.log).
