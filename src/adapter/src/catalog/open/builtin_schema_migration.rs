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
use mz_build_info::{BuildInfo, DUMMY_BUILD_INFO};
use mz_catalog::builtin::{
    BUILTIN_LOOKUP, Builtin, Fingerprint, MZ_STORAGE_USAGE_BY_SHARD_DESCRIPTION,
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
/// `BUILTINS` and `MIGRATIONS` lists to initialize the lists of available builtins and required
/// migrations, respectively.
pub(super) async fn run(
    build_info: &BuildInfo,
    deploy_generation: u64,
    txn: &mut Transaction<'_>,
    config: BuiltinItemMigrationConfig,
) -> Result<MigrationResult, Error> {
    // Sanity check to ensure we're not touching durable state in read-only mode.
    assert_eq!(config.read_only, txn.is_savepoint());

    // Tests may provide a dummy build info that confuses the migration step selection logic. Skip
    // migrations if we observe this build info.
    if *build_info == DUMMY_BUILD_INFO {
        return Ok(MigrationResult::default());
    }

    let Some(durable_version) = get_migration_version(txn) else {
        // New catalog; nothing to do.
        return Ok(MigrationResult::default());
    };
    let build_version = build_info.semver_version();

    let collection_metadata = txn.get_collection_metadata();
    let system_objects = txn
        .get_system_object_mappings()
        .map(|m| {
            let object = m.description;
            let global_id = m.unique_identifier.global_id;
            let shard_id = collection_metadata.get(&global_id).copied();
            let Some((_, builtin)) = BUILTIN_LOOKUP.get(&object) else {
                panic!("missing builtin {object:?}");
            };
            let info = ObjectInfo {
                global_id,
                shard_id,
                builtin,
                fingerprint: m.unique_identifier.fingerprint,
            };
            (object, info)
        })
        .collect();

    let migration_shard = txn.get_builtin_migration_shard().expect("must exist");

    let migration = Migration {
        source_version: durable_version.clone(),
        target_version: build_version.clone(),
        deploy_generation,
        system_objects,
        migration_shard,
        config,
    };

    let result = migration.run(MIGRATIONS).await.map_err(|e| {
        Error::new(ErrorKind::FailedBuiltinSchemaMigration {
            last_seen_version: durable_version.to_string(),
            this_version: build_version.to_string(),
            cause: e.to_string(),
        })
    })?;

    result.apply(txn);

    let replaced_items = txn
        .get_system_object_mappings()
        .map(|m| m.unique_identifier)
        .filter(|ids| result.new_shards.contains_key(&ids.global_id))
        .map(|ids| ids.catalog_id)
        .collect();

    Ok(MigrationResult {
        replaced_items,
        cleanup_action: result.cleanup_action,
    })
}

/// Result produced by `Migration::run`.
struct MigrationRunResult {
    new_shards: BTreeMap<GlobalId, ShardId>,
    new_fingerprints: BTreeMap<SystemObjectDescription, String>,
    shards_to_finalize: BTreeSet<ShardId>,
    cleanup_action: BoxFuture<'static, ()>,
}

impl Default for MigrationRunResult {
    fn default() -> Self {
        Self {
            new_shards: BTreeMap::new(),
            new_fingerprints: BTreeMap::new(),
            shards_to_finalize: BTreeSet::new(),
            cleanup_action: async {}.boxed(),
        }
    }
}

impl MigrationRunResult {
    /// Apply this migration result to the given transaction.
    fn apply(&self, txn: &mut Transaction<'_>) {
        // Update collection metadata.
        let replaced_ids = self.new_shards.keys().copied().collect();
        let old_metadata = txn.delete_collection_metadata(replaced_ids);
        txn.insert_collection_metadata(self.new_shards.clone())
            .expect("inserting unique shards IDs after deleting existing entries");

        // Register shards for finalization.
        let mut unfinalized_shards: BTreeSet<_> =
            old_metadata.into_iter().map(|(_, sid)| sid).collect();
        unfinalized_shards.extend(self.shards_to_finalize.iter().copied());
        txn.insert_unfinalized_shards(unfinalized_shards)
            .expect("cannot fail");

        // Update fingerprints.
        let mappings = txn
            .get_system_object_mappings()
            .filter_map(|m| {
                let fingerprint = self.new_fingerprints.get(&m.description)?;
                Some(SystemObjectMapping {
                    description: m.description,
                    unique_identifier: SystemObjectUniqueIdentifier {
                        catalog_id: m.unique_identifier.catalog_id,
                        global_id: m.unique_identifier.global_id,
                        fingerprint: fingerprint.clone(),
                    },
                })
            })
            .collect();
        txn.set_system_object_mappings(mappings)
            .expect("filtered existing mappings remain unique");
    }
}

/// Information about a system object required to run a `Migration`.
struct ObjectInfo {
    global_id: GlobalId,
    shard_id: Option<ShardId>,
    builtin: &'static Builtin<NameReference>,
    fingerprint: String,
}

/// Context of a builtin schema migration.
struct Migration {
    /// The version we are migrating from.
    ///
    /// Same as the build version of the most recent leader process that successfully performed
    /// migrations.
    source_version: Version,
    /// The version we are migration to.
    ///
    /// Same as the build version of this process.
    target_version: Version,
    /// The deploy generation of this process.
    deploy_generation: u64,
    /// Information about all objects in the system.
    system_objects: BTreeMap<SystemObjectDescription, ObjectInfo>,
    /// The ID of the migration shard.
    migration_shard: ShardId,
    /// Additional configuration.
    config: BuiltinItemMigrationConfig,
}

impl Migration {
    async fn run(self, steps: &[MigrationStep]) -> anyhow::Result<MigrationRunResult> {
        info!(
            deploy_generation = %self.deploy_generation,
            "running builtin schema migration: {} -> {}",
            self.source_version, self.target_version
        );

        self.validate_migration_steps(steps);

        let (force, plan) = match self.config.force_migration.as_deref() {
            None => (false, self.plan_migration(steps)),
            Some("evolution") => (true, self.plan_forced_migration(Mechanism::Evolution)),
            Some("replacement") => (true, self.plan_forced_migration(Mechanism::Replacement)),
            Some(other) => panic!("unknown force migration mechanism: {other}"),
        };

        if self.source_version == self.target_version && !force {
            info!("skipping migration: already at target version");
            return Ok(MigrationRunResult::default());
        } else if self.source_version > self.target_version {
            bail!("downgrade not supported");
        }

        // In leader mode, upgrade the version of the migration shard to the target version.
        // This fences out any readers at lower versions.
        if !self.config.read_only {
            self.upgrade_migration_shard_version().await;
        }

        info!("executing migration plan: {plan:?}");

        self.migrate_evolve(&plan.evolve).await?;
        let new_shards = self.migrate_replace(&plan.replace).await?;

        let mut migrated_objects = BTreeSet::new();
        migrated_objects.extend(plan.evolve);
        migrated_objects.extend(plan.replace);

        let new_fingerprints = self.update_fingerprints(&migrated_objects)?;

        let (shards_to_finalize, cleanup_action) = self.cleanup().await?;

        Ok(MigrationRunResult {
            new_shards,
            new_fingerprints,
            shards_to_finalize,
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

            let Some(object_info) = self.system_objects.get(&object) else {
                panic!("migration step for non-existent builtin: {object:?}");
            };

            let builtin = object_info.builtin;
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

    /// Plan a forced migration of all objects using the given mechanism.
    fn plan_forced_migration(&self, mechanism: Mechanism) -> Plan {
        let objects = self
            .system_objects
            .iter()
            .filter(|(_, info)| matches!(info.builtin, Builtin::Table(..) | Builtin::Source(..)))
            .map(|(object, _)| object.clone())
            .collect();

        let mut plan = Plan::default();
        match mechanism {
            Mechanism::Evolution => plan.evolve = objects,
            Mechanism::Replacement => plan.replace = objects,
        }

        plan
    }

    /// Upgrade the migration shard to the target version.
    async fn upgrade_migration_shard_version(&self) {
        let persist = &self.config.persist_client;
        let diagnostics = Diagnostics {
            shard_name: "builtin_migration".to_string(),
            handle_purpose: format!("migration shard upgrade @ {}", self.target_version),
        };

        persist
            .upgrade_version::<migration_shard::Key, ShardId, Timestamp, StorageDiff>(
                self.migration_shard,
                diagnostics,
            )
            .await
            .expect("valid usage");
    }

    /// Migrate the given objects using the `Evolution` mechanism.
    async fn migrate_evolve(&self, objects: &[SystemObjectDescription]) -> anyhow::Result<()> {
        for object in objects {
            self.migrate_evolve_one(object).await?;
        }
        Ok(())
    }

    async fn migrate_evolve_one(&self, object: &SystemObjectDescription) -> anyhow::Result<()> {
        let persist = &self.config.persist_client;

        let Some(object_info) = self.system_objects.get(object) else {
            bail!("missing builtin {object:?}");
        };
        let id = object_info.global_id;

        let Some(shard_id) = object_info.shard_id else {
            // No shard is registered for this builtin. In leader mode, this is fine, we'll
            // register the shard during bootstrap. In read-only mode, we might be racing with the
            // leader to register the shard and it's unclear what sort of confusion can arise from
            // that -- better to bail out in this case.
            if self.config.read_only {
                bail!("missing shard ID for builtin {object:?} ({id})");
            } else {
                return Ok(());
            }
        };

        let target_desc = match object_info.builtin {
            Builtin::Table(table) => &table.desc,
            Builtin::Source(source) => &source.desc,
            Builtin::ContinualTask(ct) => &ct.desc,
            _ => bail!("not a storage collection: {object:?}"),
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
    async fn migrate_replace(
        &self,
        objects: &[SystemObjectDescription],
    ) -> anyhow::Result<BTreeMap<GlobalId, ShardId>> {
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
            if let Some(info) = self.system_objects.get(object) {
                ids_to_replace.insert(info.global_id);
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

        Ok(replaced_shards)
    }

    /// Try to get or insert replacement shards for the given IDs into the migration shard, at
    /// `target_version` and `deploy_generation`.
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

        let mut ids_to_replace = ids_to_replace.clone();
        let mut replaced_shards = BTreeMap::new();

        // Another process might already have done a shard replacement at our version and
        // generation, in which case we can directly reuse the replacement shards.
        //
        // Note that we can't assume that the previous process had the same `ids_to_replace` as we
        // do. The set of migrations to run depends on both the source and the target version, and
        // the migration shard is not keyed by source version. The previous writer might have seen
        // a different source version, if there was a concurrent migration by a leader process.
        if let Some(read_ts) = write_ts.step_back() {
            let pred = |key: &migration_shard::Key| {
                key.build_version == self.target_version
                    && key.deploy_generation == Some(self.deploy_generation)
            };
            if let Some(entries) = read_migration_shard(persist_read, read_ts, pred).await {
                for (key, shard_id) in entries {
                    let id = GlobalId::System(key.global_id);
                    if ids_to_replace.remove(&id) {
                        replaced_shards.insert(id, shard_id);
                    }
                }

                debug!(
                    %read_ts, ?replaced_shards, ?ids_to_replace,
                    "found existing entries in migration shard",
                );
            }

            if ids_to_replace.is_empty() {
                return Ok(Some(replaced_shards));
            }
        }

        // Generate new shard IDs and attempt to insert them into the migration shard. If we get a
        // CaA failure at `write_ts` that means a concurrent process has inserted in the meantime
        // and we need to re-check the migration shard contents.
        let mut updates = Vec::new();
        for id in ids_to_replace {
            let shard_id = ShardId::new();
            replaced_shards.insert(id, shard_id);

            let GlobalId::System(global_id) = id else {
                bail!("attempt to migrate a non-system collection: {id}");
            };
            let key = migration_shard::Key {
                global_id,
                build_version: self.target_version.clone(),
                deploy_generation: Some(self.deploy_generation),
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

        persist
            .open(
                self.migration_shard,
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
        self.config
            .persist_client
            .open_critical_since(
                self.migration_shard,
                // TODO: We may need to use a different critical reader
                // id for this if we want to be able to introspect it via SQL.
                PersistClient::CONTROLLER_CRITICAL_SINCE,
                diagnostics.clone(),
            )
            .await
            .expect("valid usage")
    }

    /// Update the fingerprints for `migrated_items`.
    ///
    /// Returns the new fingerprints. Also asserts that the current fingerprints of all other
    /// system items match their builtin definitions.
    fn update_fingerprints(
        &self,
        migrated_items: &BTreeSet<SystemObjectDescription>,
    ) -> anyhow::Result<BTreeMap<SystemObjectDescription, String>> {
        let mut new_fingerprints = BTreeMap::new();
        for (object, object_info) in &self.system_objects {
            let id = object_info.global_id;
            let builtin = object_info.builtin;

            let fingerprint = builtin.fingerprint();
            if fingerprint == object_info.fingerprint {
                continue; // fingerprint unchanged, nothing to do
            }

            // Fingerprint mismatch is expected for a migrated item.
            let migrated = migrated_items.contains(object);
            // Some builtin types have schemas but no durable state. No migration needed for those.
            let ephemeral = matches!(
                builtin,
                Builtin::Log(_) | Builtin::View(_) | Builtin::Index(_),
            );

            if migrated || ephemeral {
                new_fingerprints.insert(object.clone(), fingerprint);
            } else if builtin.runtime_alterable() {
                // Runtime alterable builtins have no meaningful builtin fingerprint, and a
                // sentinel value stored in the catalog.
                assert_eq!(
                    object_info.fingerprint, RUNTIME_ALTERABLE_FINGERPRINT_SENTINEL,
                    "fingerprint mismatch for runtime-alterable builtin {object:?} ({id})",
                );
            } else {
                panic!(
                    "fingerprint mismatch for builtin {builtin:?} ({id}): {} != {}",
                    fingerprint, object_info.fingerprint,
                );
            }
        }

        Ok(new_fingerprints)
    }

    /// Perform cleanup of migration state, i.e. the migration shard.
    ///
    /// Returns a list of shards to finalize, and a `Future` that must be run after the shard
    /// finalization has been durably enqueued. The `Future` is used to remove entries from the
    /// migration shard only after we know the respective shards will be finalized. Removing
    /// entries immediately would risk leaking the shards.
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
    async fn cleanup(&self) -> anyhow::Result<(BTreeSet<ShardId>, BoxFuture<'static, ()>)> {
        let noop_action = async {}.boxed();
        let noop_result = (BTreeSet::new(), noop_action);

        if self.config.read_only {
            return Ok(noop_result);
        }

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
            return Ok(noop_result);
        };

        // Collect old entries to remove.
        let pred = |key: &migration_shard::Key| key.build_version < self.target_version;
        let Some(stale_entries) = read_migration_shard(&mut persist_read, read_ts, pred).await
        else {
            return Ok(noop_result);
        };

        debug!(
            ?stale_entries,
            "cleaning migration shard up to version {}", self.target_version,
        );

        let current_shards: BTreeMap<_, _> = self
            .system_objects
            .values()
            .filter_map(|o| o.shard_id.map(|shard_id| (o.global_id, shard_id)))
            .collect();

        let mut shards_to_finalize = BTreeSet::new();
        let mut retractions = Vec::new();
        for (key, shard_id) in stale_entries {
            // The migration shard contains both shards created during aborted upgrades and shards
            // created during successful upgrades. The latter may still be in use, so we have to
            // check and only finalize those that aren't anymore.
            let gid = GlobalId::System(key.global_id);
            if current_shards.get(&gid) != Some(&shard_id) {
                shards_to_finalize.insert(shard_id);
            }

            retractions.push(((key, shard_id), write_ts, -1));
        }

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

        Ok((shards_to_finalize, cleanup_action))
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
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
    pub(super) struct Key {
        pub(super) global_id: u64,
        pub(super) build_version: Version,
        // Versions < 26.0 didn't include the deploy generation. As long as we still might
        // encounter migration shard entries that don't have it, we need to keep this an `Option`
        // and keep supporting both key formats.
        pub(super) deploy_generation: Option<u64>,
    }

    impl fmt::Display for Key {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            if self.deploy_generation.is_some() {
                // current format
                let s = serde_json::to_string(self).expect("JSON serializable");
                f.write_str(&s)
            } else {
                // pre-26.0 format
                write!(f, "{}-{}", self.global_id, self.build_version)
            }
        }
    }

    impl FromStr for Key {
        type Err = String;

        fn from_str(s: &str) -> Result<Self, String> {
            // current format
            if let Ok(key) = serde_json::from_str(s) {
                return Ok(key);
            };

            // pre-26.0 format
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
                deploy_generation: None,
            })
        }
    }

    impl Default for Key {
        fn default() -> Self {
            Self {
                global_id: Default::default(),
                build_version: Version::new(0, 0, 0),
                deploy_generation: Some(0),
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
