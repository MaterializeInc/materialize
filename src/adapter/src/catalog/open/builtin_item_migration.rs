// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Migrations for builtin items.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::FutureExt;
use mz_catalog::builtin::{BuiltinTable, Fingerprint, BUILTINS};
use mz_catalog::config::BuiltinItemMigrationConfig;
use mz_catalog::durable::objects::SystemObjectUniqueIdentifier;
use mz_catalog::durable::{
    DurableCatalogError, FenceError, SystemObjectDescription, SystemObjectMapping, Transaction,
};
use mz_catalog::memory::error::{Error, ErrorKind};
use mz_catalog::memory::objects::CatalogItem;
use mz_catalog::SYSTEM_CONN_ID;
use mz_ore::collections::CollectionExt;
use mz_ore::{halt, soft_assert_or_log, soft_panic_or_log};
use mz_persist_client::cfg::USE_CRITICAL_SINCE_CATALOG;
use mz_persist_client::critical::SinceHandle;
use mz_persist_client::read::ReadHandle;
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Diagnostics, PersistClient};
use mz_persist_types::codec_impls::ShardIdSchema;
use mz_persist_types::ShardId;
use mz_repr::{CatalogItemId, Diff, GlobalId, Timestamp};
use mz_sql::catalog::CatalogItem as _;
use mz_storage_client::controller::StorageTxn;
use timely::progress::{Antichain, Timestamp as TimelyTimestamp};
use tracing::{debug, error};

use crate::catalog::open::builtin_item_migration::persist_schema::{TableKey, TableKeySchema};
use crate::catalog::state::LocalExpressionCache;
use crate::catalog::{BuiltinTableUpdate, CatalogState};

/// The results of a builtin item migration.
pub(crate) struct BuiltinItemMigrationResult {
    /// A vec of updates to apply to the builtin tables.
    pub(crate) builtin_table_updates: Vec<BuiltinTableUpdate<&'static BuiltinTable>>,
    /// A set of new shards that may need to be initialized.
    pub(crate) migrated_storage_collections_0dt: BTreeSet<CatalogItemId>,
    /// Some cleanup action to take once the migration has been made durable.
    pub(crate) cleanup_action: BoxFuture<'static, ()>,
}

/// Perform migrations for any builtin items that may have changed between versions.
pub(crate) async fn migrate_builtin_items(
    state: &mut CatalogState,
    txn: &mut Transaction<'_>,
    local_expr_cache: &mut LocalExpressionCache,
    migrated_builtins: Vec<CatalogItemId>,
    BuiltinItemMigrationConfig {
        persist_client,
        read_only,
    }: BuiltinItemMigrationConfig,
) -> Result<BuiltinItemMigrationResult, Error> {
    migrate_builtin_items_0dt(
        state,
        txn,
        local_expr_cache,
        persist_client,
        migrated_builtins,
        read_only,
    )
    .await
}

/// An implementation of builtin item migrations that is compatible with zero down-time upgrades.
/// The issue with the legacy approach is that it mints new global IDs for each migrated item and
/// its descendents, without durably writing those IDs down in the catalog. As a result, the
/// previous Materialize version, which is still running, may allocate the same global IDs. This
/// would cause confusion for the current version when it's promoted to the leader because its
/// definition of global IDs will no longer be valid. At best, the current version would have to
/// rehydrate all objects that depend on migrated items. At worst, it would panic.
///
/// The high level description of this approach is that we create new shards for each migrated
/// builtin table with the new table schema, without changing the global ID. Dependent objects are
/// not modified but now read from the new shards.
///
/// A detailed description of this approach follows. It's important that all of these steps are
/// idempotent, so that we can safely crash at any point and non-upgrades turn into a no-op.
///
///    1. Each environment has a dedicated persist shard, called the migration shard, that allows
///       environments to durably write down metadata while in read-only mode. The shard is a
///       mapping of `(GlobalId, build_version)` to `ShardId`.
///    2. Collect the `GlobalId` of all migrated tables for the current build version.
///    3. Read in the current contents of the migration shard.
///    4. Collect all the `ShardId`s from the migration shard that are not at the current
///       `build_version` or are not in the set of migrated tables.
///       a. If they ARE NOT mapped to a `GlobalId` in the storage metadata then they are shards
///          from an incomplete migration. Finalize them and remove them from the migration shard.
///          Note: care must be taken to not remove the shard from the migration shard until we are
///          sure that they will be finalized, otherwise the shard will leak.
///       b. If they ARE mapped to a `GlobalId` in the storage metadata then they are shards from a
///       complete migration. Remove them from the migration shard.
///    5. Collect all the `GlobalId`s of tables that are migrated, but not in the migration shard
///       for the current build version. Generate new `ShardId`s and add them to the migration
///       shard.
///    6. At this point the migration shard should only logically contain a mapping of migrated
///       table `GlobalId`s to new `ShardId`s for the current build version. For each of these
///       `GlobalId`s such that the `ShardId` isn't already in the storage metadata:
///       a. Remove the current `GlobalId` to `ShardId` mapping from the storage metadata.
///       b. Finalize the removed `ShardId`s.
///       c. Insert the new `GlobalId` to `ShardId` mapping into the storage metadata.
///
/// This approach breaks the abstraction boundary between the catalog and the storage metadata, but
/// these types of rare, but extremely useful, abstraction breaks is the exact reason they are
/// co-located.
///
/// Since the new shards are created in read-only mode, they will be left empty and all dependent
/// items will fail to hydrate.
/// TODO(jkosh44) Back-fill these tables in read-only mode so they can properly hydrate.
///
/// While in read-only mode we write the migration changes to `txn`, which will update the
/// in-memory catalog, which will cause the new shards to be created in storage. However, we don't
/// have to worry about the catalog changes becoming durable because the `txn` is in savepoint
/// mode. When we re-execute this migration as the leader (i.e. outside of read-only mode), `txn`
/// will be writable and the migration will be made durable in the catalog. We always write
/// directly to the migration shard, regardless of read-only mode. So we have to be careful not to
/// remove anything from the migration shard until we're sure that its results have been made
/// durable elsewhere.
async fn migrate_builtin_items_0dt(
    state: &mut CatalogState,
    txn: &mut Transaction<'_>,
    local_expr_cache: &mut LocalExpressionCache,
    persist_client: PersistClient,
    migrated_builtins: Vec<CatalogItemId>,
    read_only: bool,
) -> Result<BuiltinItemMigrationResult, Error> {
    assert_eq!(
        read_only,
        txn.is_savepoint(),
        "txn must be in savepoint mode when read_only is true, and in writable mode when read_only is false"
    );

    let build_version = state.config.build_info.semver_version();

    // 0. Update durably stored fingerprints.
    let id_fingerprint_map: BTreeMap<_, _> = BUILTINS::iter(&state.config().builtins_cfg)
        .map(|builtin| {
            let id = state.resolve_builtin_object(builtin);
            let fingerprint = builtin.fingerprint();
            (id, fingerprint)
        })
        .collect();
    let mut migrated_system_object_mappings = BTreeMap::new();
    for item_id in &migrated_builtins {
        let fingerprint = id_fingerprint_map
            .get(item_id)
            .expect("missing fingerprint");
        let entry = state.get_entry(item_id);
        let schema_name = state
            .get_schema(
                &entry.name().qualifiers.database_spec,
                &entry.name().qualifiers.schema_spec,
                entry.conn_id().unwrap_or(&SYSTEM_CONN_ID),
            )
            .name
            .schema
            .as_str();
        // Builtin Items can only be referenced by a single GlobalId.
        let global_id = state.get_entry(item_id).global_ids().into_element();

        migrated_system_object_mappings.insert(
            *item_id,
            SystemObjectMapping {
                description: SystemObjectDescription {
                    schema_name: schema_name.to_string(),
                    object_type: entry.item_type(),
                    object_name: entry.name().item.clone(),
                },
                unique_identifier: SystemObjectUniqueIdentifier {
                    catalog_id: *item_id,
                    global_id,
                    fingerprint: fingerprint.clone(),
                },
            },
        );
    }
    txn.update_system_object_mappings(migrated_system_object_mappings)?;

    // 1. Open migration shard.
    let organization_id = state.config.environment_id.organization_id();
    let shard_id = txn
        .get_builtin_migration_shard()
        .expect("builtin migration shard should exist for opened catalogs");
    let diagnostics = Diagnostics {
        shard_name: "builtin_migration".to_string(),
        handle_purpose: format!(
            "builtin table migration shard for org {organization_id:?} version {build_version:?}"
        ),
    };
    let mut since_handle: SinceHandle<TableKey, ShardId, Timestamp, Diff, i64> = persist_client
        .open_critical_since(
            shard_id,
            // TODO: We may need to use a different critical reader
            // id for this if we want to be able to introspect it via SQL.
            PersistClient::CONTROLLER_CRITICAL_SINCE,
            diagnostics.clone(),
        )
        .await
        .expect("invalid usage");
    let (mut write_handle, mut read_handle): (
        WriteHandle<TableKey, ShardId, Timestamp, Diff>,
        ReadHandle<TableKey, ShardId, Timestamp, Diff>,
    ) = persist_client
        .open(
            shard_id,
            Arc::new(TableKeySchema),
            Arc::new(ShardIdSchema),
            diagnostics,
            USE_CRITICAL_SINCE_CATALOG.get(persist_client.dyncfgs()),
        )
        .await
        .expect("invalid usage");
    // Commit an empty write at the minimum timestamp so the shard is always readable.
    const EMPTY_UPDATES: &[((TableKey, ShardId), Timestamp, Diff)] = &[];
    let res = write_handle
        .compare_and_append(
            EMPTY_UPDATES,
            Antichain::from_elem(Timestamp::minimum()),
            Antichain::from_elem(Timestamp::minimum().step_forward()),
        )
        .await
        .expect("invalid usage");
    if let Err(e) = res {
        debug!("migration shard already initialized: {e:?}");
    }

    // 2. Get the `GlobalId` of all migrated storage collections.
    let migrated_storage_collections: BTreeSet<_> = migrated_builtins
        .into_iter()
        .filter_map(|item_id| {
            let gid = match state.get_entry(&item_id).item() {
                CatalogItem::Table(table) => {
                    let mut ids: Vec<_> = table.global_ids().collect();
                    assert_eq!(ids.len(), 1, "{ids:?}");
                    ids.pop().expect("checked length")
                }
                CatalogItem::Source(source) => source.global_id(),
                CatalogItem::MaterializedView(mv) => mv.global_id(),
                CatalogItem::ContinualTask(ct) => ct.global_id(),
                CatalogItem::Log(_)
                | CatalogItem::Sink(_)
                | CatalogItem::View(_)
                | CatalogItem::Index(_)
                | CatalogItem::Type(_)
                | CatalogItem::Func(_)
                | CatalogItem::Secret(_)
                | CatalogItem::Connection(_) => return None,
            };
            let GlobalId::System(raw_gid) = gid else {
                unreachable!(
                    "builtin objects must have system ID, found: {item_id:?} with {gid:?}"
                );
            };
            Some(raw_gid)
        })
        .collect();

    // 3. Read in the current contents of the migration shard.
    // We intentionally fetch the upper AFTER opening the read handle to address races between
    // the upper and since moving forward in some other process.
    let upper = fetch_upper(&mut write_handle).await;
    // The empty write above should ensure that the upper is at least 1.
    let as_of = upper.checked_sub(1).ok_or_else(|| {
        Error::new(ErrorKind::Internal(format!(
            "builtin migration failed, unexpected upper: {upper:?}"
        )))
    })?;
    let since = read_handle.since();
    assert!(
        since.less_equal(&as_of),
        "since={since:?}, as_of={as_of:?}; since must be less than or equal to as_of"
    );
    let as_of = Antichain::from_elem(as_of);
    let snapshot = read_handle
        .snapshot_and_fetch(as_of)
        .await
        .expect("we have advanced the as_of by the since");
    soft_assert_or_log!(
        snapshot.iter().all(|(_, _, diff)| *diff == 1),
        "snapshot_and_fetch guarantees a consolidated result: {snapshot:?}"
    );
    let mut global_id_shards: BTreeMap<_, _> = snapshot
        .into_iter()
        .map(|((key, value), _ts, _diff)| {
            let table_key = key.expect("persist decoding error");
            let shard_id = value.expect("persist decoding error");
            (table_key, shard_id)
        })
        .collect();

    // 4. Clean up contents of migration shard.
    let mut migrated_shard_updates: Vec<((TableKey, ShardId), Timestamp, Diff)> = Vec::new();
    let mut migration_shards_to_finalize = BTreeSet::new();
    let storage_collection_metadata = {
        let txn: &mut dyn StorageTxn<Timestamp> = txn;
        txn.get_collection_metadata()
    };
    for (table_key, shard_id) in global_id_shards.clone() {
        if table_key.build_version > build_version {
            halt!(
                "saw build version {}, which is greater than current build version {}",
                table_key.build_version,
                build_version
            );
        }

        if !migrated_storage_collections.contains(&table_key.global_id)
            || table_key.build_version < build_version
        {
            global_id_shards.remove(&table_key);
            if storage_collection_metadata.get(&GlobalId::System(table_key.global_id))
                == Some(&shard_id)
            {
                migrated_shard_updates.push(((table_key, shard_id.clone()), upper, -1));
            } else {
                migration_shards_to_finalize.insert((table_key, shard_id));
            }
        }
    }

    // 5. Add migrated tables to migration shard for current build version.
    let mut global_id_shards: BTreeMap<_, _> = global_id_shards
        .into_iter()
        .map(|(table_key, shard_id)| (table_key.global_id, shard_id))
        .collect();
    for global_id in migrated_storage_collections {
        if !global_id_shards.contains_key(&global_id) {
            let shard_id = ShardId::new();
            global_id_shards.insert(global_id, shard_id);
            let table_key = TableKey {
                global_id,
                build_version: build_version.clone(),
            };
            migrated_shard_updates.push(((table_key, shard_id), upper, 1));
        }
    }

    // It's very important that we use the same `upper` that was used to read in a snapshot of the
    // shard. If someone updated the shard after we read then this write will fail.
    let upper = if !migrated_shard_updates.is_empty() {
        write_to_migration_shard(
            migrated_shard_updates,
            upper,
            &mut write_handle,
            &mut since_handle,
        )
        .await?
    } else {
        upper
    };

    // 6. Update `GlobalId` to `ShardId` mapping and register old `ShardId`s for finalization. We don't do the finalization here and instead rely on the background finalization task.
    let migrated_storage_collections_0dt = {
        let txn: &mut dyn StorageTxn<Timestamp> = txn;
        let storage_collection_metadata = txn.get_collection_metadata();
        let global_id_shards: BTreeMap<_, _> = global_id_shards
            .into_iter()
            .map(|(global_id, shard_id)| (GlobalId::System(global_id), shard_id))
            .filter(|(global_id, shard_id)| {
                storage_collection_metadata.get(global_id) != Some(shard_id)
            })
            .collect();
        let global_ids: BTreeSet<_> = global_id_shards.keys().cloned().collect();
        let mut old_shard_ids: BTreeSet<_> = txn
            .delete_collection_metadata(global_ids.clone())
            .into_iter()
            .map(|(_, shard_id)| shard_id)
            .collect();
        old_shard_ids.extend(
            migration_shards_to_finalize
                .iter()
                .map(|(_, shard_id)| shard_id),
        );
        txn.insert_unfinalized_shards(old_shard_ids).map_err(|e| {
            Error::new(ErrorKind::Internal(format!(
                "builtin migration failed: {e}"
            )))
        })?;
        txn.insert_collection_metadata(global_id_shards)
            .map_err(|e| {
                Error::new(ErrorKind::Internal(format!(
                    "builtin migration failed: {e}"
                )))
            })?;
        global_ids
    };

    // 7. Map the migrated `GlobalId`s to their corresponding `CatalogItemId`.
    let migrated_storage_collections_0dt = migrated_storage_collections_0dt
        .into_iter()
        .map(|gid| state.get_entry_by_global_id(&gid).id())
        .collect();

    let updates = txn.get_and_commit_op_updates();
    let builtin_table_updates = state
        .apply_updates_for_bootstrap(updates, local_expr_cache)
        .await;

    let cleanup_action = async move {
        if !read_only {
            let updates: Vec<_> = migration_shards_to_finalize
                .into_iter()
                .map(|(table_key, shard_id)| ((table_key, shard_id), upper, -1))
                .collect();
            if !updates.is_empty() {
                // Ignore any errors, these shards will get cleaned up in the next upgrade.
                // It's important to use `upper` here. If there was another concurrent write at
                // `upper`, then `updates` are no longer valid.
                let res =
                    write_to_migration_shard(updates, upper, &mut write_handle, &mut since_handle)
                        .await;
                if let Err(e) = res {
                    error!("Unable to remove old entries from migration shard: {e:?}");
                }
            }
        }
    }
    .boxed();

    Ok(BuiltinItemMigrationResult {
        builtin_table_updates,
        migrated_storage_collections_0dt,
        cleanup_action,
    })
}

async fn fetch_upper(
    write_handle: &mut WriteHandle<TableKey, ShardId, Timestamp, Diff>,
) -> Timestamp {
    write_handle
        .fetch_recent_upper()
        .await
        .as_option()
        .cloned()
        .expect("we use a totally ordered time and never finalize the shard")
}

async fn write_to_migration_shard(
    updates: Vec<((TableKey, ShardId), Timestamp, Diff)>,
    upper: Timestamp,
    write_handle: &mut WriteHandle<TableKey, ShardId, Timestamp, Diff>,
    since_handle: &mut SinceHandle<TableKey, ShardId, Timestamp, Diff, i64>,
) -> Result<Timestamp, Error> {
    let next_upper = upper.step_forward();
    // Lag the shard's upper by 1 to keep it readable.
    let downgrade_to = Antichain::from_elem(next_upper.saturating_sub(1));
    let next_upper_antichain = Antichain::from_elem(next_upper);

    if let Err(err) = write_handle
        .compare_and_append(updates, Antichain::from_elem(upper), next_upper_antichain)
        .await
        .expect("invalid usage")
    {
        return Err(Error::new(ErrorKind::Durable(DurableCatalogError::Fence(
            FenceError::migration(err),
        ))));
    }

    // The since handle gives us the ability to fence out other downgraders using an opaque token.
    // (See the method documentation for details.)
    // That's not needed here, so we use the since handle's opaque token to avoid any comparison
    // failures.
    let opaque = *since_handle.opaque();
    let downgrade = since_handle
        .maybe_compare_and_downgrade_since(&opaque, (&opaque, &downgrade_to))
        .await;
    match downgrade {
        None => {}
        Some(Err(e)) => soft_panic_or_log!("found opaque value {e}, but expected {opaque}"),
        Some(Ok(updated)) => soft_assert_or_log!(
            updated == downgrade_to,
            "updated bound ({updated:?}) should match expected ({downgrade_to:?})"
        ),
    }

    Ok(next_upper)
}

mod persist_schema {
    use std::num::ParseIntError;

    use arrow::array::{StringArray, StringBuilder};
    use bytes::{BufMut, Bytes};
    use mz_persist_types::codec_impls::{
        SimpleColumnarData, SimpleColumnarDecoder, SimpleColumnarEncoder,
    };
    use mz_persist_types::columnar::Schema2;
    use mz_persist_types::stats::NoneStats;
    use mz_persist_types::Codec;

    #[derive(Debug, Clone, Eq, Ord, PartialEq, PartialOrd)]
    pub(super) struct TableKey {
        pub(super) global_id: u64,
        pub(super) build_version: semver::Version,
    }

    impl std::fmt::Display for TableKey {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}-{}", self.global_id, self.build_version)
        }
    }

    impl std::str::FromStr for TableKey {
        type Err = String;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            let parts: Vec<_> = s.splitn(2, '-').collect();
            let &[global_id, build_version] = parts.as_slice() else {
                return Err(format!("invalid TableKey '{s}'"));
            };
            let global_id = global_id
                .parse()
                .map_err(|e: ParseIntError| e.to_string())?;
            let build_version = build_version
                .parse()
                .map_err(|e: semver::Error| e.to_string())?;
            Ok(TableKey {
                global_id,
                build_version,
            })
        }
    }

    impl From<TableKey> for String {
        fn from(table_key: TableKey) -> Self {
            table_key.to_string()
        }
    }

    impl TryFrom<String> for TableKey {
        type Error = String;

        fn try_from(s: String) -> Result<Self, Self::Error> {
            s.parse()
        }
    }

    impl Default for TableKey {
        fn default() -> Self {
            Self {
                global_id: Default::default(),
                build_version: semver::Version::new(0, 0, 0),
            }
        }
    }

    impl Codec for TableKey {
        type Storage = ();
        type Schema = TableKeySchema;
        fn codec_name() -> String {
            "TableKey".into()
        }
        fn encode<B: BufMut>(&self, buf: &mut B) {
            buf.put(self.to_string().as_bytes())
        }
        fn decode<'a>(buf: &'a [u8], _schema: &TableKeySchema) -> Result<Self, String> {
            let table_key = String::from_utf8(buf.to_owned()).map_err(|err| err.to_string())?;
            table_key.parse()
        }
        fn encode_schema(_schema: &Self::Schema) -> Bytes {
            Bytes::new()
        }
        fn decode_schema(buf: &Bytes) -> Self::Schema {
            assert_eq!(*buf, Bytes::new());
            TableKeySchema
        }
    }

    impl SimpleColumnarData for TableKey {
        type ArrowBuilder = StringBuilder;
        type ArrowColumn = StringArray;

        fn goodbytes(builder: &Self::ArrowBuilder) -> usize {
            builder.values_slice().len()
        }

        fn push(&self, builder: &mut Self::ArrowBuilder) {
            builder.append_value(&self.to_string());
        }
        fn push_null(builder: &mut Self::ArrowBuilder) {
            builder.append_null();
        }
        fn read(&mut self, idx: usize, column: &Self::ArrowColumn) {
            *self = column.value(idx).parse().expect("should be valid TableKey");
        }
    }

    /// An implementation of [Schema2] for [TableKey].
    #[derive(Debug, PartialEq)]
    pub(super) struct TableKeySchema;

    impl Schema2<TableKey> for TableKeySchema {
        type ArrowColumn = StringArray;
        type Statistics = NoneStats;

        type Decoder = SimpleColumnarDecoder<TableKey>;
        type Encoder = SimpleColumnarEncoder<TableKey>;

        fn encoder(&self) -> Result<Self::Encoder, anyhow::Error> {
            Ok(SimpleColumnarEncoder::default())
        }

        fn decoder(&self, col: Self::ArrowColumn) -> Result<Self::Decoder, anyhow::Error> {
            Ok(SimpleColumnarDecoder::new(col))
        }
    }
}
