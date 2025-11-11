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

use futures::FutureExt;
use futures::future::BoxFuture;
use mz_adapter_types::dyncfgs::ENABLE_BUILTIN_MIGRATION_SCHEMA_EVOLUTION;
use mz_catalog::SYSTEM_CONN_ID;
use mz_catalog::builtin::{BUILTINS, BuiltinTable, Fingerprint};
use mz_catalog::config::BuiltinItemMigrationConfig;
use mz_catalog::durable::objects::SystemObjectUniqueIdentifier;
use mz_catalog::durable::{
    DurableCatalogError, FenceError, SystemObjectDescription, SystemObjectMapping, Transaction,
};
use mz_catalog::memory::error::{Error, ErrorKind};
use mz_catalog::memory::objects::CatalogItem;
use mz_ore::collections::CollectionExt;
use mz_ore::{halt, soft_assert_or_log, soft_panic_or_log};
use mz_persist_client::cfg::USE_CRITICAL_SINCE_CATALOG;
use mz_persist_client::critical::SinceHandle;
use mz_persist_client::read::ReadHandle;
use mz_persist_client::schema::CaESchema;
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Diagnostics, PersistClient};
use mz_persist_types::ShardId;
use mz_persist_types::codec_impls::{ShardIdSchema, UnitSchema};
use mz_repr::{CatalogItemId, GlobalId, Timestamp};
use mz_sql::catalog::CatalogItem as _;
use mz_storage_client::controller::StorageTxn;
use mz_storage_types::StorageDiff;
use mz_storage_types::sources::SourceData;
use timely::progress::{Antichain, Timestamp as TimelyTimestamp};
use tracing::{debug, error, info, warn};

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
///
/// We only need to do anything for items that have an associated storage collection. Others
/// (views, indexes) don't have any durable state that requires migration.
///
/// We have the ability to handle some backward-compatible schema changes through persist schema
/// evolution, and we do so when possible. For changes that schema evolution doesn't support, we
/// instead "migrate" the affected storage collections by creating new persist shards with the new
/// schemas and dropping the old ones. See [`migrate_builtin_collections_incompatible`] for
/// details.
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
    assert_eq!(
        read_only,
        txn.is_savepoint(),
        "txn must be in savepoint mode when read_only is true, and in writable mode otherwise",
    );

    update_catalog_fingerprints(state, txn, &migrated_builtins)?;

    // Collect GlobalIds of storage collections we need to migrate.
    let mut collections_to_migrate: Vec<_> = migrated_builtins
        .into_iter()
        .filter_map(|id| {
            use CatalogItem::*;
            match &state.get_entry(&id).item() {
                Table(table) => Some(table.global_ids().into_element()),
                Source(source) => Some(source.global_id()),
                MaterializedView(mv) => Some(mv.global_id()),
                ContinualTask(ct) => Some(ct.global_id()),
                Log(_) | Sink(_) | View(_) | Index(_) | Type(_) | Func(_) | Secret(_)
                | Connection(_) => None,
            }
        })
        .collect();

    // Attempt to perform schema evolution.
    //
    // If we run into an unexpected error while trying to perform schema evolution, we abort the
    // migration process, rather than automatically falling back to replacing the persist shards.
    // We do this to avoid accidentally losing data due to a bug. This gives us the option to
    // decide if we'd rather fix the bug or skip schema evolution using the dyncfg flag.
    if ENABLE_BUILTIN_MIGRATION_SCHEMA_EVOLUTION.get(state.system_config().dyncfgs()) {
        collections_to_migrate =
            try_evolve_persist_schemas(state, txn, collections_to_migrate, &persist_client).await?;
    } else {
        info!("skipping builtin migration by schema evolution");
    }

    // For collections whose schemas we couldn't evolve, perform the replacement process.
    // Note that we need to invoke this process even if `collections_to_migrate` is empty because
    // it also cleans up any leftovers of previous migrations from the migration shard.
    migrate_builtin_collections_incompatible(
        state,
        txn,
        local_expr_cache,
        persist_client,
        collections_to_migrate,
        read_only,
    )
    .await
}

/// Update the durably stored fingerprints of `migrated_builtins`.
fn update_catalog_fingerprints(
    state: &CatalogState,
    txn: &mut Transaction<'_>,
    migrated_builtins: &[CatalogItemId],
) -> Result<(), Error> {
    let id_fingerprint_map: BTreeMap<_, _> = BUILTINS::iter(&state.config().builtins_cfg)
        .map(|builtin| {
            let id = state.resolve_builtin_object(builtin);
            let fingerprint = builtin.fingerprint();
            (id, fingerprint)
        })
        .collect();
    let mut migrated_system_object_mappings = BTreeMap::new();
    for item_id in migrated_builtins {
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

    Ok(())
}

/// Attempt to migrate the given builtin collections using persist schema evolution.
///
/// Returns the IDs of collections for which schema evolution did not succeed, due to an "expected"
/// error. At the moment, there are two expected reasons for schema evolution to fail: (a) the
/// existing shard doesn't have a schema and (b) the new schema is incompatible with the existing
/// schema.
///
/// If this method encounters an unexpected error, it returns an `Error` instead. The caller is
/// expected to abort the migration process in response.
async fn try_evolve_persist_schemas(
    state: &CatalogState,
    txn: &Transaction<'_>,
    migrated_storage_collections: Vec<GlobalId>,
    persist_client: &PersistClient,
) -> Result<Vec<GlobalId>, Error> {
    let collection_metadata = txn.get_collection_metadata();

    let mut failed = Vec::new();
    for id in migrated_storage_collections {
        let Some(&shard_id) = collection_metadata.get(&id) else {
            return Err(Error::new(ErrorKind::Internal(format!(
                "builtin migration: missing metadata for builtin collection {id}"
            ))));
        };

        let diagnostics = Diagnostics {
            shard_name: id.to_string(),
            handle_purpose: "migrate builtin schema".to_string(),
        };
        let Some((old_schema_id, old_schema, _)) = persist_client
            .latest_schema::<SourceData, (), Timestamp, StorageDiff>(shard_id, diagnostics.clone())
            .await
            .expect("invalid usage")
        else {
            // There are two known cases where it is expected to not find a latest schema:
            //   * The existing shard has never been written to previously.
            //   * We are running inside an upgrade check, with an in-memory `persist_client`.
            info!(%id, "builtin schema evolution failed: missing latest schema");
            failed.push(id);
            continue;
        };

        let entry = state.get_entry_by_global_id(&id);
        let Some(new_schema) = entry.desc_opt_latest() else {
            return Err(Error::new(ErrorKind::Internal(format!(
                "builtin migration: missing new schema for builtin collection {id}"
            ))));
        };

        info!(%id, ?old_schema, ?new_schema, "attempting builtin schema evolution");

        let result = persist_client
            .compare_and_evolve_schema::<SourceData, (), Timestamp, StorageDiff>(
                shard_id,
                old_schema_id,
                &new_schema,
                &UnitSchema,
                diagnostics,
            )
            .await
            .expect("invalid usage");

        match result {
            CaESchema::Ok(_) => {
                info!("builtin schema evolution succeeded");
            }
            CaESchema::Incompatible => {
                info!("builtin schema evolution failed: incompatible");
                failed.push(id);
            }
            CaESchema::ExpectedMismatch { schema_id, .. } => {
                return Err(Error::new(ErrorKind::Internal(format!(
                    "builtin migration: unexpected schema mismatch ({} != {})",
                    schema_id, old_schema_id,
                ))));
            }
        }
    }

    Ok(failed)
}

/// Migrate builtin collections that are not supported by persist schema evolution.
///
/// The high level description of this approach is that we create new shards for each migrated
/// builtin collection with the new schema, without changing the global ID. Dependent objects are
/// not modified but now read from the new shards.
///
/// A detailed description of this approach follows. It's important that all of these steps are
/// idempotent, so that we can safely crash at any point and non-upgrades turn into a no-op.
///
///    1. Each environment has a dedicated persist shard, called the migration shard, that allows
///       environments to durably write down metadata while in read-only mode. The shard is a
///       mapping of `(GlobalId, build_version)` to `ShardId`.
///    2. Read in the current contents of the migration shard.
///    3. Collect all the `ShardId`s from the migration shard that are not at the current
///       `build_version` or are not in the set of migrated collections.
///       a. If they ARE NOT mapped to a `GlobalId` in the storage metadata then they are shards
///          from an incomplete migration. Finalize them and remove them from the migration shard.
///          Note: care must be taken to not remove the shard from the migration shard until we are
///          sure that they will be finalized, otherwise the shard will leak.
///       b. If they ARE mapped to a `GlobalId` in the storage metadata then they are shards from a
///          complete migration. Remove them from the migration shard.
///    4. Collect all the `GlobalId`s of collections that are migrated, but not in the migration
///       shard for the current build version. Generate new `ShardId`s and add them to the
///       migration shard.
///    5. At this point the migration shard should only logically contain a mapping of migrated
///       collection `GlobalId`s to new `ShardId`s for the current build version. For each of these
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
///
/// While in read-only mode we write the migration changes to `txn`, which will update the
/// in-memory catalog, which will cause the new shards to be created in storage. However, we don't
/// have to worry about the catalog changes becoming durable because the `txn` is in savepoint
/// mode. When we re-execute this migration as the leader (i.e. outside of read-only mode), `txn`
/// will be writable and the migration will be made durable in the catalog. We always write
/// directly to the migration shard, regardless of read-only mode. So we have to be careful not to
/// remove anything from the migration shard until we're sure that its results have been made
/// durable elsewhere.
async fn migrate_builtin_collections_incompatible(
    state: &mut CatalogState,
    txn: &mut Transaction<'_>,
    local_expr_cache: &mut LocalExpressionCache,
    persist_client: PersistClient,
    migrated_storage_collections: Vec<GlobalId>,
    read_only: bool,
) -> Result<BuiltinItemMigrationResult, Error> {
    let build_version = state.config.build_info.semver_version();

    // The migration shard only stores raw GlobalIds, so it's more convenient to keep the list of
    // migrated collections in that form.
    let migrated_storage_collections: Vec<_> = migrated_storage_collections
        .into_iter()
        .map(|gid| match gid {
            GlobalId::System(raw) => raw,
            _ => panic!("builtins must have system IDs"),
        })
        .collect();

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
    let mut since_handle: SinceHandle<TableKey, ShardId, Timestamp, StorageDiff, i64> =
        persist_client
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
        WriteHandle<TableKey, ShardId, Timestamp, StorageDiff>,
        ReadHandle<TableKey, ShardId, Timestamp, StorageDiff>,
    ) = persist_client
        .open(
            shard_id,
            Arc::new(TableKeySchema),
            Arc::new(ShardIdSchema),
            diagnostics.clone(),
            USE_CRITICAL_SINCE_CATALOG.get(persist_client.dyncfgs()),
        )
        .await
        .expect("invalid usage");
    // Commit an empty write at the minimum timestamp so the shard is always readable.
    const EMPTY_UPDATES: &[((TableKey, ShardId), Timestamp, StorageDiff)] = &[];
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

    // 2. Read in the current contents of the migration shard.
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
        .filter_map(|(data, _ts, _diff)| {
            if let (Ok(table_key), Ok(shard_id)) = data {
                Some((table_key, shard_id))
            } else {
                // If we can't decode the data, it has likely been written by a newer version, so
                // we ignore it.
                warn!("skipping unreadable migration shard entry: {data:?}");
                None
            }
        })
        .collect();

    // 4. Clean up contents of migration shard.
    let mut migrated_shard_updates: Vec<((TableKey, ShardId), Timestamp, StorageDiff)> = Vec::new();
    let mut migration_shards_to_finalize = BTreeSet::new();
    let storage_collection_metadata = txn.get_collection_metadata();
    for (table_key, shard_id) in global_id_shards.clone() {
        if table_key.build_version > build_version {
            if read_only {
                halt!(
                    "saw build version {}, which is greater than current build version {}",
                    table_key.build_version,
                    build_version
                );
            } else {
                // If we are in leader mode, and a newer (read-only) version has started a
                // migration, we must not allow ourselves to get fenced out! Continuing here might
                // confuse any read-only process running the migrations concurrently, but it's
                // better for the read-only env to crash than the leader.
                // TODO(#9755): handle this in a more principled way
                warn!(
                    %table_key.build_version, %build_version,
                    "saw build version which is greater than current build version",
                );
                global_id_shards.remove(&table_key);
                continue;
            }
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
            persist_client
                .upgrade_version::<TableKey, ShardId, Timestamp, StorageDiff>(shard_id, diagnostics)
                .await
                .expect("valid usage");
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
    write_handle: &mut WriteHandle<TableKey, ShardId, Timestamp, StorageDiff>,
) -> Timestamp {
    write_handle
        .fetch_recent_upper()
        .await
        .as_option()
        .cloned()
        .expect("we use a totally ordered time and never finalize the shard")
}

async fn write_to_migration_shard(
    updates: Vec<((TableKey, ShardId), Timestamp, StorageDiff)>,
    upper: Timestamp,
    write_handle: &mut WriteHandle<TableKey, ShardId, Timestamp, StorageDiff>,
    since_handle: &mut SinceHandle<TableKey, ShardId, Timestamp, StorageDiff, i64>,
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
    use mz_persist_types::Codec;
    use mz_persist_types::codec_impls::{
        SimpleColumnarData, SimpleColumnarDecoder, SimpleColumnarEncoder,
    };
    use mz_persist_types::columnar::Schema;
    use mz_persist_types::stats::NoneStats;

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

    /// An implementation of [Schema] for [TableKey].
    #[derive(Debug, PartialEq)]
    pub(super) struct TableKeySchema;

    impl Schema<TableKey> for TableKeySchema {
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
