// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Support for migrating the schemas of builtin storage collections.
//!
//! If a version upgrade changes the schema of a builtin collection that's made durable in persist,
//! that persist shard's schema must be migrated accordingly. The migration must happen in a way
//! that's compatible with 0dt upgrades: Read-only environments need to be able to read the
//! collections with the new schema, without interfering with the leader environment's continued
//! use of the old schema.
//!
//! Two migration mechanisms are provided:
//!
//!  * [`Mechanism::Evolution`] uses persist's schema evolution support to evolve the persist
//!    shard's schema in-place. Only works for backward-compatible changes.
//!  * [`Mechanism::Replacement`] creates a new shard to serve the builtin collection in the new
//!    version. Works for all schema changes but discards existing data.
//!
//! Which mechanism to use is selected through entries in the `MIGRATIONS` list. In general, the
//! `Evolution` mechanism should be used when possible, as it avoids data loss.
//!
//! For more context and details on the implementation, see
//! `doc/developer/design/20251015_builtin_schema_migration.md`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use anyhow::bail;
use futures::FutureExt;
use futures::future::BoxFuture;
use mz_build_info::BuildInfo;
use mz_catalog::builtin::{
    BUILTINS_STATIC, Builtin, Fingerprint, MZ_STORAGE_USAGE_BY_SHARD_DESCRIPTION,
    RUNTIME_ALTERABLE_FINGERPRINT_SENTINEL,
};
use mz_catalog::config::BuiltinItemMigrationConfig;
use mz_catalog::durable::objects::SystemObjectUniqueIdentifier;
use mz_catalog::durable::{SystemObjectDescription, SystemObjectMapping, Transaction};
use mz_catalog::memory::error::{Error, ErrorKind};
use mz_ore::soft_assert_or_log;
use mz_persist_client::cfg::USE_CRITICAL_SINCE_CATALOG;
use mz_persist_client::critical::SinceHandle;
use mz_persist_client::read::ReadHandle;
use mz_persist_client::schema::CaESchema;
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Diagnostics, PersistClient};
use mz_persist_types::ShardId;
use mz_persist_types::codec_impls::{ShardIdSchema, UnitSchema};
use mz_persist_types::schema::backward_compatible;
use mz_repr::namespaces::{MZ_CATALOG_SCHEMA, MZ_INTERNAL_SCHEMA};
use mz_repr::{CatalogItemId, GlobalId, Timestamp};
use mz_sql::catalog::{CatalogItemType, NameReference};
use mz_storage_client::controller::StorageTxn;
use mz_storage_types::StorageDiff;
use mz_storage_types::sources::SourceData;
use semver::Version;
use timely::progress::Antichain;
use tracing::{debug, info};

use crate::catalog::migrate::get_migration_version;

/// Builtin schema migrations required to upgrade to the current build version.
///
/// Migration steps for old versions must be retained around according to the upgrade policy. For
/// example, if we support upgrading one major version at a time, the release of version `N.0.0`
/// can delete all migration steps with versions before `(N-1).0.0`.
///
/// Smallest supported version: 0.147.0
const MIGRATIONS: &[MigrationStep] = &[
    MigrationStep {
        version: Version::new(0, 149, 0),
        object: Object {
            type_: CatalogItemType::Source,
            schema: MZ_INTERNAL_SCHEMA,
            name: "mz_sink_statistics_raw",
        },
        mechanism: Mechanism::Replacement,
    },
    MigrationStep {
        version: Version::new(0, 149, 0),
        object: Object {
            type_: CatalogItemType::Source,
            schema: MZ_INTERNAL_SCHEMA,
            name: "mz_source_statistics_raw",
        },
        mechanism: Mechanism::Replacement,
    },
    MigrationStep {
        version: Version::new(0, 159, 0),
        object: Object {
            type_: CatalogItemType::Source,
            schema: MZ_INTERNAL_SCHEMA,
            name: "mz_cluster_replica_metrics_history",
        },
        mechanism: Mechanism::Evolution,
    },
    MigrationStep {
        version: Version::new(0, 160, 0),
        object: Object {
            type_: CatalogItemType::Table,
            schema: MZ_CATALOG_SCHEMA,
            name: "mz_roles",
        },
        mechanism: Mechanism::Replacement,
    },
    MigrationStep {
        version: Version::new(0, 160, 0),
        object: Object {
            type_: CatalogItemType::Table,
            schema: MZ_CATALOG_SCHEMA,
            name: "mz_sinks",
        },
        mechanism: Mechanism::Replacement,
    },
];

/// A migration required to upgrade past a specific version.
#[derive(Clone, Debug)]
struct MigrationStep {
    /// The build version that requires this migration.
    version: Version,
    /// The object that requires migration.
    object: Object,
    /// The migration mechanism to be used.
    mechanism: Mechanism,
}

/// The mechanism to use to migrate the schema of a builtin collection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[allow(dead_code)]
enum Mechanism {
    /// Persist schema evolution.
    ///
    /// Keeps existing contents but only works for schema changes that are backward compatible
    /// according to [`backward_compatible`].
    Evolution,
    /// Shard replacement.
    ///
    /// Works for arbitrary schema changes but loses existing contents.
    Replacement,
}

/// The object of a migration.
///
/// This has the same information as [`SystemObjectDescription`] but can be constructed in `const`
/// contexts, like the `MIGRATIONS` list.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Object {
    type_: CatalogItemType,
    schema: &'static str,
    name: &'static str,
}

impl From<Object> for SystemObjectDescription {
    fn from(object: Object) -> Self {
        SystemObjectDescription {
            schema_name: object.schema.into(),
            object_type: object.type_,
            object_name: object.name.into(),
        }
    }
}

/// The result of a builtin schema migration.
pub(super) struct MigrationResult {
    /// IDs of items whose shards have been replaced using the `Replacement` mechanism.
    pub replaced_items: BTreeSet<CatalogItemId>,
    /// A cleanup action to take once the migration has been made durable.
    pub cleanup_action: BoxFuture<'static, ()>,
}

impl Default for MigrationResult {
    fn default() -> Self {
        Self {
            replaced_items: Default::default(),
            cleanup_action: async {}.boxed(),
        }
    }
}

/// Run builtin schema migrations.
///
/// This is the entry point used by adapter when opening the catalog. It uses the hardcoded
/// `BUILTINS_STATIC` and `MIGRATIONS` lists to initialize the lists of available builtins and
/// required migrations, respectively.
pub(super) async fn run(
    build_info: &BuildInfo,
    txn: &mut Transaction<'_>,
    config: BuiltinItemMigrationConfig,
) -> Result<MigrationResult, Error> {
    // Sanity check to ensure we're not touching durable state in read-only mode.
    assert_eq!(config.read_only, txn.is_savepoint());

    let Some(durable_version) = get_migration_version(txn) else {
        // New catalog; nothing to do.
        return Ok(MigrationResult::default());
    };
    let build_version = build_info.semver_version();

    let builtins = BUILTINS_STATIC
        .iter()
        .map(|builtin| {
            let object = SystemObjectDescription {
                schema_name: builtin.schema().to_string(),
                object_type: builtin.catalog_item_type(),
                object_name: builtin.name().to_string(),
            };
            (object, builtin)
        })
        .collect();

    let migration = Migration::new(
        durable_version.clone(),
        build_version.clone(),
        txn,
        builtins,
        config,
    );

    let result = migration.run(MIGRATIONS).await.map_err(|e| {
        Error::new(ErrorKind::FailedBuiltinSchemaMigration {
            last_seen_version: durable_version.to_string(),
            this_version: build_version.to_string(),
            cause: e.to_string(),
        })
    })?;

    Ok(result)
}

/// Context of a builtin schema migration.
struct Migration<'a, 'b> {
    /// The version we are migrating from.
    ///
    /// Same as the build version of the most recent leader process that successfully performed
    /// migrations.
    source_version: Version,
    /// The version we are migration to.
    ///
    /// Same as the build version of this process.
    target_version: Version,
    txn: &'a mut Transaction<'b>,
    builtins: BTreeMap<SystemObjectDescription, &'static Builtin<NameReference>>,
    object_ids: BTreeMap<SystemObjectDescription, SystemObjectUniqueIdentifier>,
    config: BuiltinItemMigrationConfig,
}

impl<'a, 'b> Migration<'a, 'b> {
    fn new(
        source_version: Version,
        target_version: Version,
        txn: &'a mut Transaction<'b>,
        builtins: BTreeMap<SystemObjectDescription, &'static Builtin<NameReference>>,
        config: BuiltinItemMigrationConfig,
    ) -> Self {
        let object_ids = txn
            .get_system_object_mappings()
            .map(|m| (m.description, m.unique_identifier))
            .collect();

        Self {
            source_version,
            target_version,
            txn,
            builtins,
            object_ids,
            config,
        }
    }

    async fn run(mut self, steps: &[MigrationStep]) -> anyhow::Result<MigrationResult> {
        info!(
            "running builtin schema migration: {} -> {}",
            self.source_version, self.target_version
        );

        self.validate_migration_steps(steps);

        if self.source_version == self.target_version {
            info!("skipping migration: already at target version");
            return Ok(Default::default());
        } else if self.source_version > self.target_version {
            bail!("downgrade not supported");
        }

        let plan = self.plan_migration(steps);
        info!("executing migration plan: {plan:?}");

        self.migrate_evolve(&plan.evolve).await?;
        self.migrate_replace(&plan.replace).await?;

        let mut migrated_items = BTreeSet::new();
        let mut replaced_items = BTreeSet::new();
        for object in &plan.evolve {
            let id = self.object_ids[object].catalog_id;
            migrated_items.insert(id);
        }
        for object in &plan.replace {
            let id = self.object_ids[object].catalog_id;
            migrated_items.insert(id);
            replaced_items.insert(id);
        }

        self.update_fingerprints(&migrated_items)?;

        let cleanup_action = self.cleanup().await?;

        Ok(MigrationResult {
            replaced_items,
            cleanup_action,
        })
    }

    /// Sanity check the given migration steps.
    ///
    /// If any of these checks fail, that's a bug in Materialize, and we panic immediately.
    fn validate_migration_steps(&self, steps: &[MigrationStep]) {
        for step in steps {
            assert!(
                step.version <= self.target_version,
                "migration step version greater than target version: {} > {}",
                step.version,
                self.target_version,
            );

            let object = SystemObjectDescription::from(step.object.clone());

            // `mz_storage_usage_by_shard` cannot be migrated for multiple reasons. Firstly, it would
            // cause the table to be truncated because the contents are not also stored in the durable
            // catalog. Secondly, we prune `mz_storage_usage_by_shard` of old events in the background
            // on startup. The correctness of that pruning relies on there being no other retractions
            // to `mz_storage_usage_by_shard`.
            //
            // TODO: Confirm the above reasoning, it might be outdated?
            assert_ne!(
                *MZ_STORAGE_USAGE_BY_SHARD_DESCRIPTION, object,
                "mz_storage_usage_by_shard cannot be migrated or else the table will be truncated"
            );

            let Some(builtin) = self.builtins.get(&object) else {
                panic!("migration step for non-existent builtin: {object:?}");
            };

            use Builtin::*;
            assert!(
                matches!(builtin, Table(..) | Source(..) | ContinualTask(..)),
                "schema migration not supported for builtin: {builtin:?}",
            );
        }
    }

    /// Select for each object to migrate the appropriate migration mechanism.
    fn plan_migration(&self, steps: &[MigrationStep]) -> Plan {
        // Ignore any steps at versions before `source_version`.
        let steps = steps.iter().filter(|s| s.version > self.source_version);

        // Select a mechanism for each object, according to the requested migrations:
        //  * If any `Replacement` was requested, use `Replacement`.
        //  * Otherwise, (i.e. only `Evolution` was requested), use `Evolution`.
        let mut by_object = BTreeMap::new();
        for step in steps {
            if let Some(entry) = by_object.get_mut(&step.object) {
                *entry = match (step.mechanism, *entry) {
                    (Mechanism::Evolution, Mechanism::Evolution) => Mechanism::Evolution,
                    (Mechanism::Replacement, _) | (_, Mechanism::Replacement) => {
                        Mechanism::Replacement
                    }
                };
            } else {
                by_object.insert(step.object.clone(), step.mechanism);
            }
        }

        let mut plan = Plan::default();
        for (object, mechanism) in by_object {
            match mechanism {
                Mechanism::Evolution => plan.evolve.push(object.into()),
                Mechanism::Replacement => plan.replace.push(object.into()),
            }
        }

        plan
    }

    /// Migrate the given objects using the `Evolution` mechanism.
    async fn migrate_evolve(&self, objects: &[SystemObjectDescription]) -> anyhow::Result<()> {
        for object in objects {
            self.migrate_evolve_one(object).await?;
        }
        Ok(())
    }

    async fn migrate_evolve_one(&self, object: &SystemObjectDescription) -> anyhow::Result<()> {
        let collection_metadata = self.txn.get_collection_metadata();
        let persist = &self.config.persist_client;

        let Some(builtin) = self.builtins.get(object) else {
            bail!("missing builtin {object:?}");
        };
        let Some(id) = self.object_ids.get(object).map(|i| i.global_id) else {
            bail!("missing id for builtin {object:?}");
        };
        let Some(&shard_id) = collection_metadata.get(&id) else {
            // No shard is registered for this builtin. In leader mode, this is fine, we'll
            // register the shard during bootstrap. In read-only mode, we might be racing with the
            // leader to register the shard and it's unclear what sort of confusion can arise from
            // that -- better to bail out in this case.
            if self.config.read_only {
                bail!("missing collection metadata for builtin {object:?} ({id})");
            } else {
                return Ok(());
            }
        };

        let target_desc = match builtin {
            Builtin::Table(table) => &table.desc,
            Builtin::Source(source) => &source.desc,
            Builtin::ContinualTask(ct) => &ct.desc,
            _ => bail!("not a storage collection: {builtin:?}"),
        };

        let diagnostics = Diagnostics {
            shard_name: id.to_string(),
            handle_purpose: format!("builtin schema migration @ {}", self.target_version),
        };
        let source_schema = persist
            .latest_schema::<SourceData, (), Timestamp, StorageDiff>(shard_id, diagnostics.clone())
            .await
            .expect("valid usage");

        info!(?object, %id, %shard_id, ?source_schema, ?target_desc, "migrating by evolution");

        if self.config.read_only {
            // In read-only mode, only check that the new schema is backward compatible.
            // We'll register it when/if we restart in leader mode.
            if let Some((_, source_desc, _)) = &source_schema {
                let old = mz_persist_types::columnar::data_type::<SourceData>(source_desc)?;
                let new = mz_persist_types::columnar::data_type::<SourceData>(target_desc)?;
                if backward_compatible(&old, &new).is_none() {
                    bail!(
                        "incompatible schema evolution for {object:?}: \
                         {source_desc:?} -> {target_desc:?}"
                    );
                }
            }

            return Ok(());
        }

        let (mut schema_id, mut source_desc) = match source_schema {
            Some((schema_id, source_desc, _)) => (schema_id, source_desc),
            None => {
                // If no schema was previously registered, simply try to register the new one. This
                // might fail due to a concurrent registration, in which case we'll fall back to
                // `compare_and_evolve_schema`.

                debug!(%id, %shard_id, "no previous schema found; registering initial one");
                let schema_id = persist
                    .register_schema::<SourceData, (), Timestamp, StorageDiff>(
                        shard_id,
                        target_desc,
                        &UnitSchema,
                        diagnostics.clone(),
                    )
                    .await
                    .expect("valid usage");
                if schema_id.is_some() {
                    return Ok(());
                }

                debug!(%id, %shard_id, "schema registration failed; falling back to CaES");
                let (schema_id, source_desc, _) = persist
                    .latest_schema::<SourceData, (), Timestamp, StorageDiff>(
                        shard_id,
                        diagnostics.clone(),
                    )
                    .await
                    .expect("valid usage")
                    .expect("known to exist");

                (schema_id, source_desc)
            }
        };

        loop {
            // Evolving the schema might fail if another process evolved the schema concurrently,
            // in which case we need to retry. Most likely the other process evolved the schema to
            // our own target schema and the second try is a no-op.

            debug!(%id, %shard_id, %schema_id, ?source_desc, ?target_desc, "attempting CaES");
            let result = persist
                .compare_and_evolve_schema::<SourceData, (), Timestamp, StorageDiff>(
                    shard_id,
                    schema_id,
                    target_desc,
                    &UnitSchema,
                    diagnostics.clone(),
                )
                .await
                .expect("valid usage");

            match result {
                CaESchema::Ok(schema_id) => {
                    debug!(%id, %shard_id, %schema_id, "schema evolved successfully");
                    break;
                }
                CaESchema::Incompatible => bail!(
                    "incompatible schema evolution for {object:?}: \
                     {source_desc:?} -> {target_desc:?}"
                ),
                CaESchema::ExpectedMismatch {
                    schema_id: new_id,
                    key,
                    val: UnitSchema,
                } => {
                    schema_id = new_id;
                    source_desc = key;
                }
            }
        }

        Ok(())
    }

    /// Migrate the given objects using the `Replacement` mechanism.
    async fn migrate_replace(&mut self, objects: &[SystemObjectDescription]) -> anyhow::Result<()> {
        if objects.is_empty() {
            return Ok(Default::default());
        }

        let diagnostics = Diagnostics {
            shard_name: "builtin_migration".to_string(),
            handle_purpose: format!("builtin schema migration @ {}", self.target_version),
        };
        let (mut persist_write, mut persist_read) =
            self.open_migration_shard(diagnostics.clone()).await;

        let mut ids_to_replace = BTreeSet::new();
        for object in objects {
            if let Some(ids) = self.object_ids.get(object) {
                ids_to_replace.insert(ids.global_id);
            } else {
                bail!("missing id for builtin {object:?}");
            }
        }

        info!(?objects, ?ids_to_replace, "migrating by replacement");

        // Fetch replacement shard IDs from the migration shard, or insert new ones if none exist.
        // This can fail due to writes by concurrent processes, so we need to retry.
        let replaced_shards = loop {
            if let Some(shards) = self
                .try_get_or_insert_replacement_shards(
                    &ids_to_replace,
                    &mut persist_write,
                    &mut persist_read,
                )
                .await?
            {
                break shards;
            }
        };

        // Update the collection metadata in the transaction and enqueue old shards for
        // finalization.
        let old = self.txn.delete_collection_metadata(ids_to_replace);
        let old_shards = old.into_iter().map(|(_, shard_id)| shard_id).collect();
        self.txn.insert_unfinalized_shards(old_shards)?;
        self.txn.insert_collection_metadata(replaced_shards)?;

        Ok(())
    }

    /// Try to get or insert replacement shards for the given IDs into the migration shard, at
    /// `target_version`.
    ///
    /// This method looks for existing entries in the migration shards and returns those if they
    /// are present. Otherwise it generates new shard IDs and tries to insert them.
    ///
    /// The result of this call is `None` if no existing entries were found and inserting new ones
    /// failed because of a concurrent write to the migration shard. In this case, the caller is
    /// expected to retry.
    async fn try_get_or_insert_replacement_shards(
        &self,
        ids_to_replace: &BTreeSet<GlobalId>,
        persist_write: &mut WriteHandle<migration_shard::Key, ShardId, Timestamp, StorageDiff>,
        persist_read: &mut ReadHandle<migration_shard::Key, ShardId, Timestamp, StorageDiff>,
    ) -> anyhow::Result<Option<BTreeMap<GlobalId, ShardId>>> {
        let upper = persist_write.fetch_recent_upper().await;
        let write_ts = *upper.as_option().expect("migration shard not sealed");

        // Another process might already have done a shard replacement at our version, in which
        // case we can directly reuse the replacement shards.
        //
        // TODO: We should check for deploy generation as well as build version. Storing the deploy
        // generation as well requires changing the key schema of the migration shard.
        if let Some(read_ts) = write_ts.step_back() {
            let pred = |key: &migration_shard::Key| key.build_version == self.target_version;
            if let Some(entries) = read_migration_shard(persist_read, read_ts, pred).await {
                let replaced_shards: BTreeMap<_, _> = entries
                    .into_iter()
                    .map(|(key, shard_id)| (GlobalId::System(key.global_id), shard_id))
                    .collect();

                // Processes at the same build version are expected to migrate the same collections.
                let replaced_ids: BTreeSet<_> = replaced_shards.keys().copied().collect();
                if replaced_ids != *ids_to_replace {
                    bail!("replaced ids mismatch: {replaced_ids:?} != {ids_to_replace:?}");
                }

                debug!(
                    %read_ts, ?replaced_shards,
                    "found existing entries in migration shard",
                );
                return Ok(Some(replaced_shards));
            }
        }

        // Generate new shard IDs and attempt to insert them into the migration shard. If we get a
        // CaA failure at `write_ts` that means a concurrent process has inserted in the meantime
        // and we need to re-check the migration shard contents.
        let mut replaced_shards = BTreeMap::new();
        let mut updates = Vec::new();
        for &id in ids_to_replace {
            let shard_id = ShardId::new();
            replaced_shards.insert(id, shard_id);

            let GlobalId::System(global_id) = id else {
                bail!("attempt to migrate a non-system collection: {id}");
            };
            let key = migration_shard::Key {
                global_id,
                build_version: self.target_version.clone(),
            };
            updates.push(((key, shard_id), write_ts, 1));
        }

        let upper = Antichain::from_elem(write_ts);
        let new_upper = Antichain::from_elem(write_ts.step_forward());
        debug!(%write_ts, "attempting insert into migration shard");
        let result = persist_write
            .compare_and_append(updates, upper, new_upper)
            .await
            .expect("valid usage");

        match result {
            Ok(()) => {
                debug!(
                    %write_ts, ?replaced_shards,
                    "successfully inserted into migration shard"
                );
                Ok(Some(replaced_shards))
            }
            Err(_mismatch) => Ok(None),
        }
    }

    /// Open writer and reader for the migration shard.
    async fn open_migration_shard(
        &self,
        diagnostics: Diagnostics,
    ) -> (
        WriteHandle<migration_shard::Key, ShardId, Timestamp, StorageDiff>,
        ReadHandle<migration_shard::Key, ShardId, Timestamp, StorageDiff>,
    ) {
        let persist = &self.config.persist_client;
        let shard_id = self.txn.get_builtin_migration_shard().expect("must exist");

        persist
            .open(
                shard_id,
                Arc::new(migration_shard::KeySchema),
                Arc::new(ShardIdSchema),
                diagnostics,
                USE_CRITICAL_SINCE_CATALOG.get(persist.dyncfgs()),
            )
            .await
            .expect("valid usage")
    }

    /// Open a [`SinceHandle`] for the migration shard.
    async fn open_migration_shard_since(
        &self,
        diagnostics: Diagnostics,
    ) -> SinceHandle<migration_shard::Key, ShardId, Timestamp, StorageDiff, i64> {
        let persist = &self.config.persist_client;
        let shard_id = self.txn.get_builtin_migration_shard().expect("must exist");

        persist
            .open_critical_since(
                shard_id,
                // TODO: We may need to use a different critical reader
                // id for this if we want to be able to introspect it via SQL.
                PersistClient::CONTROLLER_CRITICAL_SINCE,
                diagnostics.clone(),
            )
            .await
            .expect("valid usage")
    }

    /// Update the fingerprints stored for `migrated_items` in the catalog.
    ///
    /// Also asserts that the stored fingerprints of all other system items match their builtin
    /// definitions.
    fn update_fingerprints(
        &mut self,
        migrated_items: &BTreeSet<CatalogItemId>,
    ) -> anyhow::Result<()> {
        let mut updates = BTreeMap::new();
        for (object, ids) in &self.object_ids {
            let Some(builtin) = self.builtins.get(object) else {
                bail!("missing builtin {object:?}");
            };

            let id = ids.catalog_id;
            let fingerprint = builtin.fingerprint();
            if fingerprint == ids.fingerprint {
                continue; // fingerprint unchanged, nothing to do
            }

            // Fingerprint mismatch is expected for a migrated item.
            let migrated = migrated_items.contains(&id);
            // Some builtin types have schemas but no durable state. No migration needed for those.
            let ephemeral = matches!(
                builtin,
                Builtin::Log(_) | Builtin::View(_) | Builtin::Index(_),
            );

            if migrated || ephemeral {
                let new_mapping = SystemObjectMapping {
                    description: object.clone(),
                    unique_identifier: SystemObjectUniqueIdentifier {
                        catalog_id: ids.catalog_id,
                        global_id: ids.global_id,
                        fingerprint,
                    },
                };
                updates.insert(id, new_mapping);
            } else if builtin.runtime_alterable() {
                // Runtime alterable builtins have no meaningful builtin fingerprint, and a
                // sentinel value stored in the catalog.
                assert_eq!(
                    ids.fingerprint, RUNTIME_ALTERABLE_FINGERPRINT_SENTINEL,
                    "fingerprint mismatch for runtime-alterable builtin {builtin:?} ({id})",
                );
            } else {
                panic!(
                    "fingerprint mismatch for builtin {builtin:?} ({id}): {} != {}",
                    fingerprint, ids.fingerprint,
                );
            }
        }

        self.txn.update_system_object_mappings(updates)?;

        Ok(())
    }

    /// Perform cleanup of migration state, i.e. the migration shard.
    ///
    /// Returns a `Future` that must be run after the `txn` has been successfully committed to
    /// durable state. This is used to remove entries from the migration shard only after we know
    /// the respective shards will be finalized. Removing entries immediately would risk leaking
    /// the shards.
    ///
    /// We only perform cleanup in leader mode, to keep the durable state changes made by read-only
    /// processes a minimal as possible. Given that Materialize doesn't support version downgrades,
    /// it is safe to assume that any state for versions below the `target_version` is not needed
    /// anymore and can be cleaned up.
    ///
    /// Note that it is fine for cleanup to sometimes fail or be skipped. The size of the migration
    /// shard should always be pretty small, so keeping migration state around for longer isn't a
    /// concern. As a result, we can keep the logic simple here and skip doing cleanup in response
    /// to transient failures, instead of retrying.
    async fn cleanup(&mut self) -> anyhow::Result<BoxFuture<'static, ()>> {
        let noop_action = async {}.boxed();

        if self.config.read_only {
            return Ok(noop_action);
        }

        let collection_metadata = self.txn.get_collection_metadata();
        let diagnostics = Diagnostics {
            shard_name: "builtin_migration".to_string(),
            handle_purpose: "builtin schema migration cleanup".into(),
        };
        let (mut persist_write, mut persist_read) =
            self.open_migration_shard(diagnostics.clone()).await;
        let mut persist_since = self.open_migration_shard_since(diagnostics.clone()).await;

        let upper = persist_write.fetch_recent_upper().await.clone();
        let write_ts = *upper.as_option().expect("migration shard not sealed");
        let Some(read_ts) = write_ts.step_back() else {
            return Ok(noop_action);
        };

        // Collect old entries to remove.
        let pred = |key: &migration_shard::Key| key.build_version < self.target_version;
        let Some(stale_entries) = read_migration_shard(&mut persist_read, read_ts, pred).await
        else {
            return Ok(noop_action);
        };

        debug!(
            ?stale_entries,
            "cleaning migration shard up to version {}", self.target_version,
        );

        let mut unfinalized_shards = BTreeSet::new();
        let mut retractions = Vec::new();
        for (key, shard_id) in stale_entries {
            // The migration shard contains both shards created during aborted upgrades and shards
            // created during successful upgrades. The latter may still be in use, so we have to
            // check and only finalize those that aren't anymore.
            let gid = GlobalId::System(key.global_id);
            if collection_metadata.get(&gid) != Some(&shard_id) {
                unfinalized_shards.insert(shard_id);
            }

            retractions.push(((key, shard_id), write_ts, -1));
        }

        // Arrange for shard finalization and subsequent removal of the migration shard entries.
        self.txn.insert_unfinalized_shards(unfinalized_shards)?;
        let cleanup_action = async move {
            if !retractions.is_empty() {
                let new_upper = Antichain::from_elem(write_ts.step_forward());
                let result = persist_write
                    .compare_and_append(retractions, upper, new_upper)
                    .await
                    .expect("valid usage");
                match result {
                    Ok(()) => debug!("cleaned up migration shard"),
                    Err(mismatch) => debug!(?mismatch, "migration shard cleanup failed"),
                }
            }
        }
        .boxed();

        // Downgrade the since, to enable some compaction.
        let o = *persist_since.opaque();
        let new_since = Antichain::from_elem(read_ts);
        let result = persist_since
            .maybe_compare_and_downgrade_since(&o, (&o, &new_since))
            .await;
        soft_assert_or_log!(result.is_none_or(|r| r.is_ok()), "opaque mismatch");

        Ok(cleanup_action)
    }
}

/// Read the migration shard at the given timestamp, returning all entries that match the given
/// predicate.
///
/// Returns `None` if the migration shard contains no matching entries, or if it isn't readable at
/// `read_ts`.
async fn read_migration_shard<P>(
    persist_read: &mut ReadHandle<migration_shard::Key, ShardId, Timestamp, StorageDiff>,
    read_ts: Timestamp,
    predicate: P,
) -> Option<Vec<(migration_shard::Key, ShardId)>>
where
    P: for<'a> Fn(&migration_shard::Key) -> bool,
{
    let as_of = Antichain::from_elem(read_ts);
    let updates = persist_read.snapshot_and_fetch(as_of).await.ok()?;

    assert!(
        updates.iter().all(|(_, _, diff)| *diff == 1),
        "migration shard contains invalid diffs: {updates:?}",
    );

    let entries: Vec<_> = updates
        .into_iter()
        .filter_map(|(data, _, _)| {
            if let (Ok(key), Ok(val)) = data {
                Some((key, val))
            } else {
                // If we can't decode the data, it has likely been written by a newer version, so
                // we ignore it.
                info!("skipping unreadable migration shard entry: {data:?}");
                None
            }
        })
        .filter(move |(key, _)| predicate(key))
        .collect();

    (!entries.is_empty()).then_some(entries)
}

/// A plan to migrate between two versions.
#[derive(Debug, Default)]
struct Plan {
    /// Objects to migrate using the `Evolution` mechanism.
    evolve: Vec<SystemObjectDescription>,
    /// Objects to migrate using the `Replacement` mechanism.
    replace: Vec<SystemObjectDescription>,
}

/// Types and persist codec impls for the migration shard used by the `Replacement` mechanism.
mod migration_shard {
    use std::fmt;
    use std::str::FromStr;

    use arrow::array::{StringArray, StringBuilder};
    use bytes::{BufMut, Bytes};
    use mz_persist_types::Codec;
    use mz_persist_types::codec_impls::{
        SimpleColumnarData, SimpleColumnarDecoder, SimpleColumnarEncoder,
    };
    use mz_persist_types::columnar::Schema;
    use mz_persist_types::stats::NoneStats;
    use semver::Version;

    #[derive(Debug, Clone, Eq, Ord, PartialEq, PartialOrd)]
    pub(super) struct Key {
        pub(super) global_id: u64,
        pub(super) build_version: Version,
    }

    impl fmt::Display for Key {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}-{}", self.global_id, self.build_version)
        }
    }

    impl FromStr for Key {
        type Err = String;

        fn from_str(s: &str) -> Result<Self, String> {
            let parts: Vec<_> = s.splitn(2, '-').collect();
            let &[global_id, build_version] = parts.as_slice() else {
                return Err(format!("invalid Key '{s}'"));
            };
            let global_id = global_id.parse::<u64>().map_err(|e| e.to_string())?;
            let build_version = build_version
                .parse::<Version>()
                .map_err(|e| e.to_string())?;
            Ok(Key {
                global_id,
                build_version,
            })
        }
    }

    impl Default for Key {
        fn default() -> Self {
            Self {
                global_id: Default::default(),
                build_version: Version::new(0, 0, 0),
            }
        }
    }

    impl Codec for Key {
        type Schema = KeySchema;
        type Storage = ();

        fn codec_name() -> String {
            "TableKey".into()
        }

        fn encode<B: BufMut>(&self, buf: &mut B) {
            buf.put(self.to_string().as_bytes())
        }

        fn decode<'a>(buf: &'a [u8], _schema: &KeySchema) -> Result<Self, String> {
            let s = str::from_utf8(buf).map_err(|e| e.to_string())?;
            s.parse()
        }

        fn encode_schema(_schema: &KeySchema) -> Bytes {
            Bytes::new()
        }

        fn decode_schema(buf: &Bytes) -> Self::Schema {
            assert_eq!(*buf, Bytes::new());
            KeySchema
        }
    }

    impl SimpleColumnarData for Key {
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
            *self = column.value(idx).parse().expect("valid Key");
        }
    }

    #[derive(Debug, PartialEq)]
    pub(super) struct KeySchema;

    impl Schema<Key> for KeySchema {
        type ArrowColumn = StringArray;
        type Statistics = NoneStats;
        type Decoder = SimpleColumnarDecoder<Key>;
        type Encoder = SimpleColumnarEncoder<Key>;

        fn encoder(&self) -> anyhow::Result<SimpleColumnarEncoder<Key>> {
            Ok(SimpleColumnarEncoder::default())
        }

        fn decoder(&self, col: StringArray) -> anyhow::Result<SimpleColumnarDecoder<Key>> {
            Ok(SimpleColumnarDecoder::new(col))
        }
    }
}
