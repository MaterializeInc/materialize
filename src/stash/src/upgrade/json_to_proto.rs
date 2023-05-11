// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Write;
use std::time::Duration;
use std::{collections::BTreeMap, num::NonZeroI64};

use anyhow::Context;
use bytes::Bytes;
use fail::fail_point;
use futures::Future;
use serde::Deserialize;
use timely::progress::Antichain;
use tokio::time::sleep;
use tokio_postgres::{types::ToSql, Client, Row};
use tracing::error;

use mz_ore::cast::CastFrom;

use super::legacy_types::{
    AclMode, AlterSourceSinkV1, AuditLogKey, CatalogItemType, ClusterId,
    ClusterIntrospectionSourceIndexKey, ClusterIntrospectionSourceIndexValue, ClusterKey,
    ClusterReplicaKey, ClusterReplicaValue, ClusterValue, ConfigValue, CreateClusterReplicaV1,
    CreateSourceSinkV1, CreateSourceSinkV2, DatabaseKey, DatabaseNamespace, DatabaseValue,
    DropClusterReplicaV1, DurableCollectionMetadata, DurableExportMetadata, EventDetails,
    EventType, EventV1, FullNameV1, GidMappingKey, GidMappingValue, GlobalId, GrantRoleV1,
    GrantRoleV2, IdAllocKey, IdAllocValue, IdFullNameV1, IdNameV1, ItemKey, ItemValue, MzAclItem,
    MzTimestamp, ObjectType, RenameItemV1, RevokeRoleV1, RevokeRoleV2, RoleAttributes, RoleId,
    RoleKey, RoleMembership, RoleValue, SchemaKey, SchemaNamespace, SchemaV1, SchemaV2,
    SchemaValue, SerializedCatalogItem, SerializedReplicaConfig, SerializedReplicaLocation,
    SerializedReplicaLogging, ServerConfigurationKey, ServerConfigurationValue, SettingKey,
    SettingValue, ShardId, SinkAsOf, StorageUsageKey, StorageUsageV1, TimestampKey, TimestampValue,
    VersionedEvent, VersionedStorageUsage,
};

pub mod objects_v15 {
    include!(concat!(env!("OUT_DIR"), "/objects_v15.rs"));
}

const COLLECTION_CONFIG_NAME: &str = "config";
const COLLECTION_SETTING_NAME: &str = "setting";
const COLLECTION_ID_ALLOC_NAME: &str = "id_alloc";
const COLLECTION_SYSTEM_GID_MAPPING_NAME: &str = "system_gid_mapping";
const COLLECTION_CLUSTERS_NAME: &str = "compute_instance";
const COLLECTION_CLUSTER_INTROSPECTION_SOURCE_INDEX_NAME: &str =
    "compute_introspection_source_index";
const COLLECTION_CLUSTER_REPLICAS_NAME: &str = "compute_replicas";
const COLLECTION_DATABASE_NAME: &str = "database";
const COLLECTION_SCHEMA_NAME: &str = "schema";
const COLLECTION_ITEM_NAME: &str = "item";
const COLLECTION_ROLE_NAME: &str = "role";
const COLLECTION_TIMESTAMP_NAME: &str = "timestamp";
const COLLECTION_SYSTEM_CONFIGURATION_NAME: &str = "system_configuration";
const COLLECTION_AUDIT_LOG_NAME: &str = "audit_log";
const COLLECTION_STORAGE_USAGE_NAME: &str = "storage_usage";

const COLLECTION_STORAGE_METADATA: &str = "storage-collection-metadata";
const COLLECTION_STORAGE_METADATA_EXPORT: &str = "storage-export-metadata-u64";
const COLLECTION_STORAGE_SHARD_FINALIZATION: &str = "storage-shards-to-finalize";

/// CockroachDB has a max request size, which has a default of 16MiB. We stay way below that just
/// to be safe.
const INSERT_BATCH_MAX_SIZE: usize = 2 * 1024 * 1024;

/// [`tokio_postgres`] has a maximum number of allowed arguments to be provided with a query. We
/// stay way below that just to be safe.
const MAX_NUMBER_OF_ARGS: u16 = u16::MAX / 8;

const DATA_PROTO_DROP_TABLE_QUERY: &str = "DROP TABLE IF EXISTS data_proto";
const DATA_PROTO_TABLE_QUERY: &str = "
CREATE TABLE data_proto (
    collection_id bigint NOT NULL REFERENCES collections (collection_id),
    key bytea NOT NULL,
    value bytea NOT NULL,
    time bigint NOT NULL,
    diff bigint NOT NULL
);

CREATE INDEX data_proto_time_idx ON data_proto (collection_id, time);
";

/// A __special__ migration that migrates all of the types in the `data` table from JSON to
/// protobuf.
pub async fn migrate_json_to_proto(
    client: &mut Client,
    epoch: NonZeroI64,
) -> Result<(), anyhow::Error> {
    /// Inner functionality that does the migration, we'll retry this loop a few times on failure.
    async fn inner(client: &mut Client, epoch: NonZeroI64) -> Result<(), anyhow::Error> {
        // Create a new table for the protobufs.
        //
        // Note: Cockroach does not support selecting or inserting into a table that you created in the
        // same transaction _if_ that table references another, e.g. how data_proto references the
        // collections table. Because of this, we create the data_proto table outside of the transaction.
        client
            .execute(DATA_PROTO_DROP_TABLE_QUERY, &[])
            .await
            .context("dropping data_proto, if exists")?;
        client
            .batch_execute(DATA_PROTO_TABLE_QUERY)
            .await
            .context("creating data_proto")?;

        fail_point!("stash_proto_create_table", |_| {
            Err(anyhow::anyhow!("Failpoint stash_proto_create_table"))
        });

        // Get all of our current collections.
        let rows = client
            .query("SELECT name, collection_id FROM collections", &[])
            .await
            .context("get all collections")?;
        let collections: BTreeMap<i64, String> = rows
            .into_iter()
            .map(|row| (row.get("collection_id"), row.get("name")))
            .collect();

        // Migrate all of the collections.
        migrate_collections(client, collections.clone(), epoch)
            .await
            .context("migrating collections")?;

        // Check if the Stash is valid.
        validate_collections(client, collections)
            .await
            .context("validating Stash")?;

        // Validate no other environmentd's have connected to the Stash.
        check_fence(|stmt| client.query_one(stmt, &[]), epoch)
            .await
            .context("checking fence, step 1")?;

        fail_point!("stash_proto_migrate", |_| {
            Err(anyhow::anyhow!("Failpoint stash_proto_migrate"))
        });

        // Start a new transaction for renaming data_proto to data.
        let tx = client
            .build_transaction()
            .start()
            .await
            .context("starting rename Transaction")?;

        // Swap the JSON data table and the proto data table, we'll drop this table in a future version.
        tx.execute("ALTER TABLE data RENAME TO data_json", &[])
            .await
            .context("renaming JSON table")?;

        // Rename the new table into place.
        tx.execute("ALTER TABLE data_proto RENAME TO data", &[])
            .await
            .context("renaming proto table")?;

        // Validate no other environmentd's have connected to the Stash.
        check_fence(|stmt| tx.query_one(stmt, &[]), epoch)
            .await
            .context("checking fence, step 2")?;

        fail_point!("stash_proto_swap_table", |_| {
            Err(anyhow::anyhow!("Failpoint stash_proto_swap_table"))
        });

        // End the transaction.
        tx.commit().await.context("commiting rename Transaction")?;

        Ok(())
    }

    // Retry the migration, reporting to Sentry for failures.
    //
    // Note: We use a loop here instead of `mz_ore::retry` because `inner(...)` takes a mutable
    // reference to Client, which FnMut (and thus Retry) doesn't support.
    let mut attempt = 0;
    loop {
        attempt += 1;
        let result = inner(client, epoch).await;
        match result {
            Err(e) if attempt >= 5 => {
                tracing::error!("Failure to migrate stash to proto after {attempt} attempts!");
                return Err(e);
            }
            Err(e) => {
                tracing::error!(
                    "Failed to migrate from JSON to proto, retrying. Attempt: {attempt}, Err: {e:?}",
                );
                sleep(Duration::from_secs(attempt * 7)).await;
            }
            Ok(_) => return Ok(()),
        }
    }
}

/// Migrates a collections from JSON to Proto based on their name.
async fn migrate_collections(
    client: &'_ mut Client,
    collections: BTreeMap<i64, String>,
    epoch: NonZeroI64,
) -> Result<(), anyhow::Error> {
    for (id, name) in collections {
        match name.as_str() {
            //
            // Adapter Collections
            //
            COLLECTION_CONFIG_NAME => migrate_collection::<
                String,
                ConfigValue,
                objects_v15::ConfigKey,
                objects_v15::ConfigValue,
            >(client, id, epoch)
            .await
            .context("migrating config")?,
            COLLECTION_SETTING_NAME => migrate_collection::<
                SettingKey,
                SettingValue,
                objects_v15::SettingKey,
                objects_v15::SettingValue,
            >(client, id, epoch)
            .await
            .context("migrating setting")?,
            COLLECTION_ID_ALLOC_NAME => migrate_collection::<
                IdAllocKey,
                IdAllocValue,
                objects_v15::IdAllocKey,
                objects_v15::IdAllocValue,
            >(client, id, epoch)
            .await
            .context("migrating id_alloc")?,
            COLLECTION_SYSTEM_GID_MAPPING_NAME => migrate_collection::<
                GidMappingKey,
                GidMappingValue,
                objects_v15::GidMappingKey,
                objects_v15::GidMappingValue,
            >(client, id, epoch)
            .await
            .context("migrating system_gid_mapping")?,
            COLLECTION_CLUSTERS_NAME => migrate_collection::<
                ClusterKey,
                ClusterValue,
                objects_v15::ClusterKey,
                objects_v15::ClusterValue,
            >(client, id, epoch)
            .await
            .context("migrating compute_instance")?,
            COLLECTION_CLUSTER_INTROSPECTION_SOURCE_INDEX_NAME => migrate_collection::<
                ClusterIntrospectionSourceIndexKey,
                ClusterIntrospectionSourceIndexValue,
                objects_v15::ClusterIntrospectionSourceIndexKey,
                objects_v15::ClusterIntrospectionSourceIndexValue,
            >(client, id, epoch)
            .await
            .context("migrating compute_introspection_source_index")?,
            COLLECTION_CLUSTER_REPLICAS_NAME => migrate_collection::<
                ClusterReplicaKey,
                ClusterReplicaValue,
                objects_v15::ClusterReplicaKey,
                objects_v15::ClusterReplicaValue,
            >(client, id, epoch)
            .await
            .context("migrating compute_replicas")?,
            COLLECTION_DATABASE_NAME => migrate_collection::<
                DatabaseKey,
                DatabaseValue,
                objects_v15::DatabaseKey,
                objects_v15::DatabaseValue,
            >(client, id, epoch)
            .await
            .context("migrating database")?,
            COLLECTION_SCHEMA_NAME => migrate_collection::<
                SchemaKey,
                SchemaValue,
                objects_v15::SchemaKey,
                objects_v15::SchemaValue,
            >(client, id, epoch)
            .await
            .context("migrating schema")?,
            COLLECTION_ITEM_NAME => migrate_collection::<
                ItemKey,
                ItemValue,
                objects_v15::ItemKey,
                objects_v15::ItemValue,
            >(client, id, epoch)
            .await
            .context("migrating item")?,
            COLLECTION_ROLE_NAME => migrate_collection::<
                RoleKey,
                RoleValue,
                objects_v15::RoleKey,
                objects_v15::RoleValue,
            >(client, id, epoch)
            .await
            .context("migrating role")?,
            COLLECTION_TIMESTAMP_NAME => migrate_collection::<
                TimestampKey,
                TimestampValue,
                objects_v15::TimestampKey,
                objects_v15::TimestampValue,
            >(client, id, epoch)
            .await
            .context("migrating timestamp")?,
            COLLECTION_SYSTEM_CONFIGURATION_NAME => migrate_collection::<
                ServerConfigurationKey,
                ServerConfigurationValue,
                objects_v15::ServerConfigurationKey,
                objects_v15::ServerConfigurationValue,
            >(client, id, epoch)
            .await
            .context("migrating system_configuration")?,
            COLLECTION_AUDIT_LOG_NAME => {
                migrate_collection::<AuditLogKey, (), objects_v15::AuditLogKey, ()>(
                    client, id, epoch,
                )
                .await
                .context("migrating audit_log")?
            }
            COLLECTION_STORAGE_USAGE_NAME => {
                migrate_collection::<StorageUsageKey, (), objects_v15::StorageUsageKey, ()>(
                    client, id, epoch,
                )
                .await
                .context("migrating storage_usage")?
            }
            //
            // Storage Client Collections
            //
            COLLECTION_STORAGE_METADATA => {
                migrate_collection::<
                    GlobalId,
                    DurableCollectionMetadata,
                    objects_v15::GlobalId,
                    objects_v15::DurableCollectionMetadata,
                >(client, id, epoch)
                .await
                .context("migrating storage-collection-metadata")?;
            }
            COLLECTION_STORAGE_METADATA_EXPORT => {
                migrate_collection::<
                    GlobalId,
                    DurableExportMetadata,
                    objects_v15::GlobalId,
                    objects_v15::DurableExportMetadata,
                >(client, id, epoch)
                .await
                .context("migrating storage-export-metadata-u64")?;
            }
            COLLECTION_STORAGE_SHARD_FINALIZATION => {
                migrate_collection::<ShardId, (), objects_v15::ShardId, ()>(client, id, epoch)
                    .await
                    .context("migrating storage-shards-to-finalize")?;
            }
            collection_name => {
                // Complain loudly, but don't error out the entire migration. All of the
                // collections listed above are the ones we know about, and are currently using.
                // If there exists more it doesn't really matter since nothing in code references
                // them today, but we want to know if there are.
                tracing::error!("Unrecognized collection name {collection_name}");
            }
        }
        fail_point!("stash_proto_migrate_collections", |_| {
            Err(anyhow::anyhow!("Failpoint stash_proto_migrate_collections"))
        });
    }

    Ok(())
}

/// Migrates a single collection, based on an `id` from the JSON types `(KJ, VJ)` to the protobuf
/// types `(KP, VP)`.
async fn migrate_collection<KJ, VJ, KP, VP>(
    client: &'_ mut Client,
    collection_id: i64,
    epoch: NonZeroI64,
) -> Result<(), anyhow::Error>
where
    KJ: for<'de> Deserialize<'de>,
    VJ: for<'de> Deserialize<'de>,
    KP: ::prost::Message + From<KJ>,
    VP: ::prost::Message + From<VJ>,
{
    // Get all of the rows for a single collection and convert them to proto.
    let mut proto_rows = client
        .query(
            "SELECT key, value, time, diff FROM data WHERE collection_id = $1",
            &[&collection_id],
        )
        .await
        .context("getting rows")?
        .into_iter()
        .map(|row| {
            let key: serde_json::Value = row.get("key");
            let val: serde_json::Value = row.get("value");
            let time: i64 = row.get("time");
            let diff: i64 = row.get("diff");

            let key_json: KJ = serde_json::from_value(key).expect("must deserialize");
            let val_json: VJ = serde_json::from_value(val).expect("must deserialize");

            let key_proto = KP::from(key_json);
            let val_proto = VP::from(val_json);

            ((key_proto, val_proto), time, diff)
        });

    tracing::info!("Read in our rows");

    // Insert all of the converted protos into the new table.
    loop {
        let mut batch = Vec::new();
        let mut rows = 0;
        let mut serialized_size = 0;

        // Keep popping rows into the batch.
        while let Some(((key, val), time, diff)) = proto_rows.next() {
            serialized_size += key.encoded_len();
            serialized_size += val.encoded_len();

            batch.push(((key.encode_to_vec(), val.encode_to_vec()), time, diff));
            rows += 1;

            // Once our serialized size is greater than our max, break to run an insert.
            if serialized_size > INSERT_BATCH_MAX_SIZE {
                break;
            }

            // Or our number of rows has passed the max number of dynamic args allowed.
            if rows * 4 > CastFrom::cast_from(MAX_NUMBER_OF_ARGS) {
                break;
            }
        }

        // We must have removed all of the rows, exit!
        if rows == 0 {
            return Ok(());
        }

        // Prepare our insert statement.
        let mut stmt =
            String::from("INSERT INTO data_proto (collection_id, key, value, time, diff) VALUES");
        let mut sep = ' ';
        for i in 0..rows {
            let idx = 1 + i * 4;
            write!(
                &mut stmt,
                "{}($1, ${}, ${}, ${}, ${})",
                sep,
                idx + 1,
                idx + 2,
                idx + 3,
                idx + 4
            )
            .unwrap();
            sep = ',';
        }
        let tx = client
            .build_transaction()
            .start()
            .await
            .context("creating insert transaction")?;
        let stmt = tx.prepare(&stmt).await.context("preparing insert")?;

        // Prepare our args.
        let mut args: Vec<&'_ (dyn ToSql + Sync)> = vec![&collection_id];
        for ((key, val), time, diff) in &batch {
            args.push(key);
            args.push(val);
            args.push(time);
            args.push(diff);
        }

        // Validate our epoch hasn't changed.
        check_fence(|stmt| tx.query_one(stmt, &[]), epoch)
            .await
            .context("checking epoch before batch")?;

        tracing::info!("Migrating {rows} rows for collection {collection_id}");

        // Insert the rows.
        tx.execute(&stmt, &args).await.context("executing insert")?;
        tx.commit().await.context("committing insert transaction")?;

        fail_point!("stash_proto_migrate_collection_batch", |_| {
            Err(anyhow::anyhow!(
                "Failpoint stash_proto_migrate_collection_batch"
            ))
        });
    }
}

async fn validate_collections(
    client: &'_ Client,
    migrated_collections: BTreeMap<i64, String>,
) -> Result<(), anyhow::Error> {
    // Validate each collection was migrated correctly.
    for (id, name) in migrated_collections {
        match name.as_str() {
            COLLECTION_CONFIG_NAME => {
                validate::<objects_v15::ConfigKey, objects_v15::ConfigValue>(client, id).await?
            }
            COLLECTION_SETTING_NAME => {
                validate::<objects_v15::SettingKey, objects_v15::SettingValue>(client, id).await?
            }
            COLLECTION_ID_ALLOC_NAME => {
                validate::<objects_v15::IdAllocKey, objects_v15::IdAllocValue>(client, id).await?
            }
            COLLECTION_SYSTEM_GID_MAPPING_NAME => {
                validate::<objects_v15::GidMappingKey, objects_v15::GidMappingValue>(client, id)
                    .await?
            }
            COLLECTION_CLUSTERS_NAME => {
                validate::<objects_v15::ClusterKey, objects_v15::ClusterValue>(client, id).await?
            }
            COLLECTION_CLUSTER_INTROSPECTION_SOURCE_INDEX_NAME => {
                validate::<
                    objects_v15::ClusterIntrospectionSourceIndexKey,
                    objects_v15::ClusterIntrospectionSourceIndexValue,
                >(client, id)
                .await?
            }
            COLLECTION_CLUSTER_REPLICAS_NAME => {
                validate::<objects_v15::ClusterReplicaKey, objects_v15::ClusterReplicaValue>(
                    client, id,
                )
                .await?
            }
            COLLECTION_DATABASE_NAME => {
                validate::<objects_v15::DatabaseKey, objects_v15::DatabaseValue>(client, id).await?
            }
            COLLECTION_SCHEMA_NAME => {
                validate::<objects_v15::SchemaKey, objects_v15::SchemaValue>(client, id).await?
            }
            COLLECTION_ITEM_NAME => {
                validate::<objects_v15::ItemKey, objects_v15::ItemValue>(client, id).await?
            }
            COLLECTION_ROLE_NAME => {
                validate::<objects_v15::RoleKey, objects_v15::RoleValue>(client, id).await?
            }
            COLLECTION_TIMESTAMP_NAME => {
                validate::<objects_v15::TimestampKey, objects_v15::TimestampValue>(client, id)
                    .await?
            }
            COLLECTION_SYSTEM_CONFIGURATION_NAME => validate::<
                objects_v15::ServerConfigurationKey,
                objects_v15::ServerConfigurationValue,
            >(client, id)
            .await?,
            COLLECTION_AUDIT_LOG_NAME => {
                validate::<objects_v15::AuditLogKey, ()>(client, id).await?
            }
            COLLECTION_STORAGE_USAGE_NAME => {
                validate::<objects_v15::StorageUsageKey, ()>(client, id).await?
            }
            _ => (),
        }
    }

    /// Deserialize all of the rows for a colletion, returning if we hit an error.
    async fn validate<KP, VP>(client: &'_ Client, id: i64) -> Result<(), anyhow::Error>
    where
        KP: ::prost::Message + Default,
        VP: ::prost::Message + Default,
    {
        // Get all of our rows from the data_proto table.
        let rows = client
            .query(
                "SELECT key, value, time, diff FROM data_proto WHERE collection_id = $1",
                &[&id],
            )
            .await?;

        // "Check" a row by making sure we can deserialize it.
        let check_row = |row: &Row| {
            let key: Vec<u8> = row.try_get("key").context("key")?;
            let val: Vec<u8> = row.try_get("value").context("val")?;
            let _: i64 = row.try_get("time").context("time")?;
            let _: i64 = row.try_get("diff").context("diff")?;

            let _ = KP::decode(&key[..]).context("proto key")?;
            let _ = VP::decode(&val[..]).context("proto val")?;

            Ok::<_, anyhow::Error>(())
        };

        // Check each row, tracing to Sentry before returning the error.
        for row in rows {
            if let Err(e) = check_row(&row) {
                error!("Failed to validate {row:?}");
                return Err(e);
            }
        }

        Ok(())
    }

    Ok(())
}

/// Checks the fence (i.e. epoch and nonce) to make sure no one else has connected to CockroachDB
/// and could be editting the Stash simultaneously. Returns an error if we detect this race.
async fn check_fence<Q, F>(query_one: Q, epoch: NonZeroI64) -> Result<(), anyhow::Error>
where
    F: Future<Output = Result<Row, tokio_postgres::Error>>,
    Q: FnOnce(&'static str) -> F,
{
    let epoch_ck = query_one("SELECT epoch FROM fence")
        .await
        .map(|row| {
            let epoch: i64 = row.get("epoch");
            NonZeroI64::new(epoch).unwrap()
        })
        .context("SELECT epoch")?;

    if epoch != epoch_ck {
        Err(anyhow::anyhow!(
            "Race detected, epoch changed!, {epoch} != {epoch_ck}"
        ))
    } else {
        Ok(())
    }
}

impl From<String> for objects_v15::ConfigKey {
    fn from(json: String) -> Self {
        objects_v15::ConfigKey { key: json }
    }
}

impl From<ConfigValue> for objects_v15::ConfigValue {
    fn from(json: ConfigValue) -> objects_v15::ConfigValue {
        objects_v15::ConfigValue { value: json.value }
    }
}

impl From<SettingKey> for objects_v15::SettingKey {
    fn from(json: SettingKey) -> objects_v15::SettingKey {
        objects_v15::SettingKey { name: json.name }
    }
}

impl From<SettingValue> for objects_v15::SettingValue {
    fn from(json: SettingValue) -> objects_v15::SettingValue {
        objects_v15::SettingValue { value: json.value }
    }
}

impl From<IdAllocKey> for objects_v15::IdAllocKey {
    fn from(json: IdAllocKey) -> objects_v15::IdAllocKey {
        objects_v15::IdAllocKey { name: json.name }
    }
}

impl From<IdAllocValue> for objects_v15::IdAllocValue {
    fn from(json: IdAllocValue) -> objects_v15::IdAllocValue {
        objects_v15::IdAllocValue {
            next_id: json.next_id,
        }
    }
}

impl From<CatalogItemType> for objects_v15::CatalogItemType {
    fn from(json: CatalogItemType) -> Self {
        match json {
            CatalogItemType::Table => objects_v15::CatalogItemType::Table,
            CatalogItemType::Source => objects_v15::CatalogItemType::Source,
            CatalogItemType::Sink => objects_v15::CatalogItemType::Sink,
            CatalogItemType::View => objects_v15::CatalogItemType::View,
            CatalogItemType::MaterializedView => objects_v15::CatalogItemType::MaterializedView,
            CatalogItemType::Index => objects_v15::CatalogItemType::Index,
            CatalogItemType::Type => objects_v15::CatalogItemType::Type,
            CatalogItemType::Func => objects_v15::CatalogItemType::Func,
            CatalogItemType::Secret => objects_v15::CatalogItemType::Secret,
            CatalogItemType::Connection => objects_v15::CatalogItemType::Connection,
        }
    }
}

impl From<GidMappingKey> for objects_v15::GidMappingKey {
    fn from(json: GidMappingKey) -> Self {
        let object_type = objects_v15::CatalogItemType::from(json.object_type);
        objects_v15::GidMappingKey {
            schema_name: json.schema_name,
            object_type: object_type.into(),
            object_name: json.object_name,
        }
    }
}

impl From<GidMappingValue> for objects_v15::GidMappingValue {
    fn from(json: GidMappingValue) -> Self {
        objects_v15::GidMappingValue {
            id: json.id,
            fingerprint: json.fingerprint,
        }
    }
}

impl From<ClusterId> for objects_v15::ClusterId {
    fn from(json: ClusterId) -> objects_v15::ClusterId {
        let value = match json {
            ClusterId::User(id) => objects_v15::cluster_id::Value::User(id),
            ClusterId::System(id) => objects_v15::cluster_id::Value::System(id),
        };

        objects_v15::ClusterId { value: Some(value) }
    }
}

impl From<ClusterKey> for objects_v15::ClusterKey {
    fn from(json: ClusterKey) -> objects_v15::ClusterKey {
        objects_v15::ClusterKey {
            id: Some(json.id.into()),
        }
    }
}

impl From<ClusterValue> for objects_v15::ClusterValue {
    fn from(json: ClusterValue) -> objects_v15::ClusterValue {
        objects_v15::ClusterValue {
            name: json.name,
            linked_object_id: json.linked_object_id.map(objects_v15::GlobalId::from),
            owner_id: Some(json.owner_id.into()),
            privileges: json
                .privileges
                .unwrap_or_default()
                .into_iter()
                .map(objects_v15::MzAclItem::from)
                .collect(),
        }
    }
}

impl From<ClusterIntrospectionSourceIndexKey> for objects_v15::ClusterIntrospectionSourceIndexKey {
    fn from(json: ClusterIntrospectionSourceIndexKey) -> Self {
        objects_v15::ClusterIntrospectionSourceIndexKey {
            cluster_id: Some(json.cluster_id.into()),
            name: json.name,
        }
    }
}

impl From<ClusterIntrospectionSourceIndexValue>
    for objects_v15::ClusterIntrospectionSourceIndexValue
{
    fn from(json: ClusterIntrospectionSourceIndexValue) -> Self {
        objects_v15::ClusterIntrospectionSourceIndexValue {
            index_id: json.index_id,
        }
    }
}

impl From<DatabaseKey> for objects_v15::DatabaseKey {
    fn from(json: DatabaseKey) -> objects_v15::DatabaseKey {
        let value = match json.ns {
            None | Some(DatabaseNamespace::User) => objects_v15::database_id::Value::User(json.id),
            Some(DatabaseNamespace::System) => objects_v15::database_id::Value::System(json.id),
        };

        objects_v15::DatabaseKey {
            id: Some(objects_v15::DatabaseId { value: Some(value) }),
        }
    }
}

impl From<DatabaseValue> for objects_v15::DatabaseValue {
    fn from(json: DatabaseValue) -> objects_v15::DatabaseValue {
        objects_v15::DatabaseValue {
            name: json.name,
            owner_id: Some(json.owner_id.into()),
            privileges: json
                .privileges
                .unwrap_or_default()
                .into_iter()
                .map(objects_v15::MzAclItem::from)
                .collect(),
        }
    }
}

impl From<SchemaKey> for objects_v15::SchemaKey {
    fn from(json: SchemaKey) -> objects_v15::SchemaKey {
        let value = match json.ns {
            None | Some(SchemaNamespace::User) => objects_v15::schema_id::Value::User(json.id),
            Some(SchemaNamespace::System) => objects_v15::schema_id::Value::System(json.id),
        };

        objects_v15::SchemaKey {
            id: Some(objects_v15::SchemaId { value: Some(value) }),
        }
    }
}

impl From<SchemaValue> for objects_v15::SchemaValue {
    fn from(json: SchemaValue) -> objects_v15::SchemaValue {
        let database_id = match (json.database_id, json.database_ns) {
            (Some(id), Some(DatabaseNamespace::User)) | (Some(id), None) => {
                Some(objects_v15::database_id::Value::User(id))
            }
            (Some(id), Some(DatabaseNamespace::System)) => {
                Some(objects_v15::database_id::Value::System(id))
            }
            (None, _) => None,
        };

        objects_v15::SchemaValue {
            database_id: database_id.map(|id| objects_v15::DatabaseId { value: Some(id) }),
            name: json.name,
            owner_id: Some(json.owner_id.into()),
            privileges: json
                .privileges
                .unwrap_or_default()
                .into_iter()
                .map(objects_v15::MzAclItem::from)
                .collect(),
        }
    }
}

impl From<ClusterReplicaKey> for objects_v15::ClusterReplicaKey {
    fn from(json: ClusterReplicaKey) -> objects_v15::ClusterReplicaKey {
        objects_v15::ClusterReplicaKey {
            id: Some(objects_v15::ReplicaId { value: json.id }),
        }
    }
}

impl From<SerializedReplicaLocation> for objects_v15::replica_config::Location {
    fn from(json: SerializedReplicaLocation) -> objects_v15::replica_config::Location {
        match json {
            SerializedReplicaLocation::Managed {
                size,
                availability_zone,
                az_user_specified,
            } => objects_v15::replica_config::Location::Managed(
                objects_v15::replica_config::ManagedLocation {
                    size,
                    availability_zone,
                    az_user_specified,
                },
            ),
            SerializedReplicaLocation::Unmanaged {
                storagectl_addrs,
                storage_addrs,
                computectl_addrs,
                compute_addrs,
                workers,
            } => objects_v15::replica_config::Location::Unmanaged(
                objects_v15::replica_config::UnmanagedLocation {
                    storagectl_addrs,
                    storage_addrs,
                    computectl_addrs,
                    compute_addrs,
                    workers: u64::cast_from(workers),
                },
            ),
        }
    }
}

impl From<SerializedReplicaLogging> for objects_v15::replica_config::Logging {
    fn from(json: SerializedReplicaLogging) -> Self {
        objects_v15::replica_config::Logging {
            log_logging: json.log_logging,
            interval: json.interval.map(objects_v15::Duration::from),
        }
    }
}

impl From<SerializedReplicaConfig> for objects_v15::ReplicaConfig {
    fn from(json: SerializedReplicaConfig) -> objects_v15::ReplicaConfig {
        objects_v15::ReplicaConfig {
            logging: Some(json.logging.into()),
            location: Some(json.location.into()),
            idle_arrangement_merge_effort: json
                .idle_arrangement_merge_effort
                .map(|effort| objects_v15::replica_config::MergeEffort { effort }),
        }
    }
}

impl From<ClusterReplicaValue> for objects_v15::ClusterReplicaValue {
    fn from(json: ClusterReplicaValue) -> objects_v15::ClusterReplicaValue {
        objects_v15::ClusterReplicaValue {
            cluster_id: Some(json.cluster_id.into()),
            name: json.name,
            config: Some(json.config.into()),
            owner_id: Some(json.owner_id.into()),
        }
    }
}

impl From<TimestampKey> for objects_v15::TimestampKey {
    fn from(json: TimestampKey) -> Self {
        objects_v15::TimestampKey { id: json.id }
    }
}

impl From<MzTimestamp> for objects_v15::Timestamp {
    fn from(json: MzTimestamp) -> Self {
        objects_v15::Timestamp {
            internal: json.internal,
        }
    }
}

impl From<TimestampValue> for objects_v15::TimestampValue {
    fn from(json: TimestampValue) -> Self {
        objects_v15::TimestampValue {
            ts: Some(json.ts.into()),
        }
    }
}

impl From<ServerConfigurationKey> for objects_v15::ServerConfigurationKey {
    fn from(json: ServerConfigurationKey) -> Self {
        objects_v15::ServerConfigurationKey { name: json.name }
    }
}

impl From<ServerConfigurationValue> for objects_v15::ServerConfigurationValue {
    fn from(json: ServerConfigurationValue) -> Self {
        objects_v15::ServerConfigurationValue { value: json.value }
    }
}

impl From<ItemKey> for objects_v15::ItemKey {
    fn from(json: ItemKey) -> objects_v15::ItemKey {
        objects_v15::ItemKey {
            gid: Some(json.gid.into()),
        }
    }
}

impl From<SerializedCatalogItem> for objects_v15::CatalogItem {
    fn from(json: SerializedCatalogItem) -> objects_v15::CatalogItem {
        let value = match json {
            SerializedCatalogItem::V1 { create_sql } => {
                objects_v15::catalog_item::Value::V1(objects_v15::catalog_item::V1 { create_sql })
            }
        };

        objects_v15::CatalogItem { value: Some(value) }
    }
}

impl From<ItemValue> for objects_v15::ItemValue {
    fn from(json: ItemValue) -> objects_v15::ItemValue {
        let schema_id = match json.schema_ns {
            None | Some(SchemaNamespace::User) => {
                objects_v15::schema_id::Value::User(json.schema_id)
            }
            Some(SchemaNamespace::System) => objects_v15::schema_id::Value::System(json.schema_id),
        };

        objects_v15::ItemValue {
            schema_id: Some(objects_v15::SchemaId {
                value: Some(schema_id),
            }),
            name: json.name,
            definition: Some(json.definition.into()),
            owner_id: Some(json.owner_id.into()),
            privileges: json
                .privileges
                .unwrap_or_default()
                .into_iter()
                .map(objects_v15::MzAclItem::from)
                .collect(),
        }
    }
}

impl From<RoleKey> for objects_v15::RoleKey {
    fn from(json: RoleKey) -> objects_v15::RoleKey {
        objects_v15::RoleKey {
            id: Some(json.id.into()),
        }
    }
}

impl From<RoleAttributes> for objects_v15::RoleAttributes {
    fn from(json: RoleAttributes) -> objects_v15::RoleAttributes {
        objects_v15::RoleAttributes {
            inherit: json.inherit,
            create_role: json.create_role,
            create_db: json.create_db,
            create_cluster: json.create_cluster,
        }
    }
}

impl From<RoleMembership> for objects_v15::RoleMembership {
    fn from(json: RoleMembership) -> objects_v15::RoleMembership {
        objects_v15::RoleMembership {
            map: json
                .map
                .into_iter()
                .map(|(key, value)| objects_v15::role_membership::Entry {
                    key: Some(objects_v15::RoleId::from(key)),
                    value: Some(objects_v15::RoleId::from(value)),
                })
                .collect(),
        }
    }
}

impl From<RoleValue> for objects_v15::RoleValue {
    fn from(json: RoleValue) -> objects_v15::RoleValue {
        objects_v15::RoleValue {
            name: json.role.name,
            attributes: json.role.attributes.map(objects_v15::RoleAttributes::from),
            membership: json.role.membership.map(objects_v15::RoleMembership::from),
        }
    }
}

impl From<EventType> for objects_v15::audit_log_event_v1::EventType {
    fn from(json: EventType) -> Self {
        match json {
            EventType::Create => objects_v15::audit_log_event_v1::EventType::Create,
            EventType::Drop => objects_v15::audit_log_event_v1::EventType::Drop,
            EventType::Alter => objects_v15::audit_log_event_v1::EventType::Alter,
            EventType::Grant => objects_v15::audit_log_event_v1::EventType::Grant,
            EventType::Revoke => objects_v15::audit_log_event_v1::EventType::Revoke,
        }
    }
}

impl From<ObjectType> for objects_v15::audit_log_event_v1::ObjectType {
    fn from(json: ObjectType) -> Self {
        match json {
            ObjectType::Cluster => objects_v15::audit_log_event_v1::ObjectType::Cluster,
            ObjectType::ClusterReplica => {
                objects_v15::audit_log_event_v1::ObjectType::ClusterReplica
            }
            ObjectType::Connection => objects_v15::audit_log_event_v1::ObjectType::Connection,
            ObjectType::Database => objects_v15::audit_log_event_v1::ObjectType::Database,
            ObjectType::Func => objects_v15::audit_log_event_v1::ObjectType::Func,
            ObjectType::Index => objects_v15::audit_log_event_v1::ObjectType::Index,
            ObjectType::MaterializedView => {
                objects_v15::audit_log_event_v1::ObjectType::MaterializedView
            }
            ObjectType::Role => objects_v15::audit_log_event_v1::ObjectType::Role,
            ObjectType::Secret => objects_v15::audit_log_event_v1::ObjectType::Secret,
            ObjectType::Schema => objects_v15::audit_log_event_v1::ObjectType::Schema,
            ObjectType::Sink => objects_v15::audit_log_event_v1::ObjectType::Sink,
            ObjectType::Source => objects_v15::audit_log_event_v1::ObjectType::Source,
            ObjectType::Table => objects_v15::audit_log_event_v1::ObjectType::Table,
            ObjectType::Type => objects_v15::audit_log_event_v1::ObjectType::Type,
            ObjectType::View => objects_v15::audit_log_event_v1::ObjectType::View,
        }
    }
}

impl From<FullNameV1> for objects_v15::audit_log_event_v1::FullNameV1 {
    fn from(json: FullNameV1) -> Self {
        objects_v15::audit_log_event_v1::FullNameV1 {
            database: json.database,
            schema: json.schema,
            item: json.item,
        }
    }
}

impl From<IdNameV1> for objects_v15::audit_log_event_v1::IdNameV1 {
    fn from(json: IdNameV1) -> Self {
        objects_v15::audit_log_event_v1::IdNameV1 {
            id: json.id,
            name: json.name,
        }
    }
}

impl From<IdFullNameV1> for objects_v15::audit_log_event_v1::IdFullNameV1 {
    fn from(json: IdFullNameV1) -> Self {
        objects_v15::audit_log_event_v1::IdFullNameV1 {
            id: json.id,
            name: Some(json.name.into()),
        }
    }
}

impl From<CreateClusterReplicaV1> for objects_v15::audit_log_event_v1::CreateClusterReplicaV1 {
    fn from(json: CreateClusterReplicaV1) -> Self {
        objects_v15::audit_log_event_v1::CreateClusterReplicaV1 {
            cluster_id: json.cluster_id,
            cluser_name: json.cluster_name,
            replica_id: json.replica_id.map(|s| s.into()),
            replica_name: json.replica_name,
            logical_size: json.logical_size,
        }
    }
}

impl From<DropClusterReplicaV1> for objects_v15::audit_log_event_v1::DropClusterReplicaV1 {
    fn from(json: DropClusterReplicaV1) -> Self {
        objects_v15::audit_log_event_v1::DropClusterReplicaV1 {
            cluster_id: json.cluster_id,
            cluster_name: json.cluster_name,
            replica_id: json.replica_id.map(|s| s.into()),
            replica_name: json.replica_name,
        }
    }
}

impl From<CreateSourceSinkV1> for objects_v15::audit_log_event_v1::CreateSourceSinkV1 {
    fn from(json: CreateSourceSinkV1) -> Self {
        objects_v15::audit_log_event_v1::CreateSourceSinkV1 {
            id: json.id,
            name: Some(json.name.into()),
            size: json.size.map(|s| s.into()),
        }
    }
}

impl From<CreateSourceSinkV2> for objects_v15::audit_log_event_v1::CreateSourceSinkV2 {
    fn from(json: CreateSourceSinkV2) -> Self {
        objects_v15::audit_log_event_v1::CreateSourceSinkV2 {
            id: json.id,
            name: Some(json.name.into()),
            size: json.size.map(|s| s.into()),
            external_type: json.external_type,
        }
    }
}

impl From<AlterSourceSinkV1> for objects_v15::audit_log_event_v1::AlterSourceSinkV1 {
    fn from(json: AlterSourceSinkV1) -> Self {
        objects_v15::audit_log_event_v1::AlterSourceSinkV1 {
            id: json.id,
            name: Some(json.name.into()),
            old_size: json.old_size.map(|s| s.into()),
            new_size: json.new_size.map(|s| s.into()),
        }
    }
}

impl From<GrantRoleV1> for objects_v15::audit_log_event_v1::GrantRoleV1 {
    fn from(json: GrantRoleV1) -> Self {
        objects_v15::audit_log_event_v1::GrantRoleV1 {
            role_id: json.role_id,
            member_id: json.member_id,
            grantor_id: json.grantor_id,
        }
    }
}

impl From<GrantRoleV2> for objects_v15::audit_log_event_v1::GrantRoleV2 {
    fn from(json: GrantRoleV2) -> Self {
        objects_v15::audit_log_event_v1::GrantRoleV2 {
            role_id: json.role_id,
            member_id: json.member_id,
            grantor_id: json.grantor_id,
            executed_by: json.executed_by,
        }
    }
}

impl From<RevokeRoleV1> for objects_v15::audit_log_event_v1::RevokeRoleV1 {
    fn from(json: RevokeRoleV1) -> Self {
        objects_v15::audit_log_event_v1::RevokeRoleV1 {
            role_id: json.role_id,
            member_id: json.member_id,
        }
    }
}

impl From<RevokeRoleV2> for objects_v15::audit_log_event_v1::RevokeRoleV2 {
    fn from(json: RevokeRoleV2) -> Self {
        objects_v15::audit_log_event_v1::RevokeRoleV2 {
            role_id: json.role_id,
            member_id: json.member_id,
            grantor_id: json.grantor_id,
            executed_by: json.executed_by,
        }
    }
}

impl From<RenameItemV1> for objects_v15::audit_log_event_v1::RenameItemV1 {
    fn from(json: RenameItemV1) -> Self {
        objects_v15::audit_log_event_v1::RenameItemV1 {
            id: json.id,
            old_name: Some(json.old_name.into()),
            new_name: Some(json.new_name.into()),
        }
    }
}

impl From<SchemaV1> for objects_v15::audit_log_event_v1::SchemaV1 {
    fn from(json: SchemaV1) -> Self {
        objects_v15::audit_log_event_v1::SchemaV1 {
            id: json.id,
            name: json.name,
            database_name: json.database_name,
        }
    }
}

impl From<SchemaV2> for objects_v15::audit_log_event_v1::SchemaV2 {
    fn from(json: SchemaV2) -> Self {
        objects_v15::audit_log_event_v1::SchemaV2 {
            id: json.id,
            name: json.name,
            database_name: json.database_name.map(|s| s.into()),
        }
    }
}

impl From<EventDetails> for objects_v15::audit_log_event_v1::Details {
    fn from(json: EventDetails) -> Self {
        match json {
            EventDetails::CreateClusterReplicaV1(details) => {
                objects_v15::audit_log_event_v1::Details::CreateClusterReplicaV1(details.into())
            }
            EventDetails::DropClusterReplicaV1(details) => {
                objects_v15::audit_log_event_v1::Details::DropClusterReplicaV1(details.into())
            }
            EventDetails::CreateSourceSinkV1(details) => {
                objects_v15::audit_log_event_v1::Details::CreateSourceSinkV1(details.into())
            }
            EventDetails::CreateSourceSinkV2(details) => {
                objects_v15::audit_log_event_v1::Details::CreateSourceSinkV2(details.into())
            }
            EventDetails::AlterSourceSinkV1(details) => {
                objects_v15::audit_log_event_v1::Details::AlterSourceSinkV1(details.into())
            }
            EventDetails::GrantRoleV1(details) => {
                objects_v15::audit_log_event_v1::Details::GrantRoleV1(details.into())
            }
            EventDetails::GrantRoleV2(details) => {
                objects_v15::audit_log_event_v1::Details::GrantRoleV2(details.into())
            }
            EventDetails::RevokeRoleV1(details) => {
                objects_v15::audit_log_event_v1::Details::RevokeRoleV1(details.into())
            }
            EventDetails::RevokeRoleV2(details) => {
                objects_v15::audit_log_event_v1::Details::RevokeRoleV2(details.into())
            }
            EventDetails::IdFullNameV1(details) => {
                objects_v15::audit_log_event_v1::Details::IdFullNameV1(details.into())
            }
            EventDetails::RenameItemV1(details) => {
                objects_v15::audit_log_event_v1::Details::RenameItemV1(details.into())
            }
            EventDetails::IdNameV1(details) => {
                objects_v15::audit_log_event_v1::Details::IdNameV1(details.into())
            }
            EventDetails::SchemaV1(details) => {
                objects_v15::audit_log_event_v1::Details::SchemaV1(details.into())
            }
            EventDetails::SchemaV2(details) => {
                objects_v15::audit_log_event_v1::Details::SchemaV2(details.into())
            }
        }
    }
}

impl From<EventV1> for objects_v15::AuditLogEventV1 {
    fn from(value: EventV1) -> Self {
        objects_v15::AuditLogEventV1 {
            id: value.id,
            event_type: objects_v15::audit_log_event_v1::EventType::from(value.event_type).into(),
            object_type: objects_v15::audit_log_event_v1::ObjectType::from(value.object_type)
                .into(),
            user: value.user.map(|s| s.into()),
            occurred_at: Some(objects_v15::EpochMillis {
                millis: value.occurred_at,
            }),
            details: Some(value.details.into()),
        }
    }
}

impl From<VersionedEvent> for objects_v15::audit_log_key::Event {
    fn from(json: VersionedEvent) -> Self {
        match json {
            VersionedEvent::V1(event) => objects_v15::audit_log_key::Event::V1(event.into()),
        }
    }
}

impl From<AuditLogKey> for objects_v15::AuditLogKey {
    fn from(json: AuditLogKey) -> Self {
        objects_v15::AuditLogKey {
            event: Some(json.event.into()),
        }
    }
}

impl From<StorageUsageV1> for objects_v15::storage_usage_key::StorageUsageV1 {
    fn from(json: StorageUsageV1) -> Self {
        objects_v15::storage_usage_key::StorageUsageV1 {
            id: json.id,
            shard_id: json.shard_id.map(|s| s.into()),
            size_bytes: json.size_bytes,
            collection_timestamp: Some(objects_v15::EpochMillis {
                millis: json.collection_timestamp,
            }),
        }
    }
}

impl From<StorageUsageKey> for objects_v15::StorageUsageKey {
    fn from(json: StorageUsageKey) -> Self {
        let usage = match json.metric {
            VersionedStorageUsage::V1(usage) => {
                objects_v15::storage_usage_key::Usage::V1(usage.into())
            }
        };

        objects_v15::StorageUsageKey { usage: Some(usage) }
    }
}

impl From<RoleId> for objects_v15::RoleId {
    fn from(json: RoleId) -> objects_v15::RoleId {
        let value = match json {
            RoleId::User(id) => objects_v15::role_id::Value::User(id),
            RoleId::System(id) => objects_v15::role_id::Value::System(id),
            RoleId::Public => objects_v15::role_id::Value::Public(objects_v15::Empty::default()),
        };

        objects_v15::RoleId { value: Some(value) }
    }
}

impl From<GlobalId> for objects_v15::GlobalId {
    fn from(json: GlobalId) -> objects_v15::GlobalId {
        let value = match json {
            GlobalId::User(id) => objects_v15::global_id::Value::User(id),
            GlobalId::System(id) => objects_v15::global_id::Value::System(id),
            GlobalId::Transient(id) => objects_v15::global_id::Value::Transient(id),
            GlobalId::Explain => {
                objects_v15::global_id::Value::Explain(objects_v15::Empty::default())
            }
        };

        objects_v15::GlobalId { value: Some(value) }
    }
}

impl From<AclMode> for objects_v15::AclMode {
    fn from(json: AclMode) -> Self {
        objects_v15::AclMode {
            bitflags: json.bits(),
        }
    }
}

impl From<MzAclItem> for objects_v15::MzAclItem {
    fn from(json: MzAclItem) -> objects_v15::MzAclItem {
        objects_v15::MzAclItem {
            grantee: Some(json.grantee.into()),
            grantor: Some(json.grantor.into()),
            acl_mode: Some(json.acl_mode.into()),
        }
    }
}

impl From<DurableCollectionMetadata> for objects_v15::DurableCollectionMetadata {
    fn from(json: DurableCollectionMetadata) -> Self {
        objects_v15::DurableCollectionMetadata {
            remap_shard: json.remap_shard.map(objects_v15::ShardId::from),
            data_shard: Some(json.data_shard.into()),
        }
    }
}

impl From<DurableExportMetadata> for objects_v15::DurableExportMetadata {
    fn from(json: DurableExportMetadata) -> Self {
        objects_v15::DurableExportMetadata {
            initial_as_of: Some(json.initial_as_of.into()),
        }
    }
}

impl From<SinkAsOf> for objects_v15::SinkAsOf {
    fn from(json: SinkAsOf) -> Self {
        objects_v15::SinkAsOf {
            frontier: Some(json.frontier.into()),
            strict: json.strict,
        }
    }
}

impl From<ShardId> for objects_v15::ShardId {
    fn from(json: ShardId) -> Self {
        objects_v15::ShardId {
            id: Bytes::from(json.0.to_vec()),
        }
    }
}

impl From<Antichain<MzTimestamp>> for objects_v15::TimestampAntichain {
    fn from(json: Antichain<MzTimestamp>) -> Self {
        objects_v15::TimestampAntichain {
            elements: json
                .elements()
                .into_iter()
                .map(|e| objects_v15::Timestamp::from(*e))
                .collect(),
        }
    }
}

impl From<std::time::Duration> for objects_v15::Duration {
    fn from(value: std::time::Duration) -> Self {
        objects_v15::Duration {
            secs: value.as_secs(),
            nanos: value.subsec_nanos(),
        }
    }
}

impl From<String> for objects_v15::StringWrapper {
    fn from(value: String) -> Self {
        objects_v15::StringWrapper { inner: value }
    }
}

#[cfg(test)]
mod tests {
    use proptest::arbitrary::any;
    use proptest::strategy::{Strategy, ValueTree};
    use proptest::test_runner::TestRunner;
    use rand::Rng;
    use tokio_postgres::Config;

    use mz_ore::metrics::MetricsRegistry;

    use super::{migrate_json_to_proto, objects_v15};
    use crate::upgrade::json_to_proto::test_helpers::{
        insert_collections, insert_stash, ArbitraryStash, Collection,
    };
    use crate::upgrade::legacy_types::{
        ConfigValue, SettingKey, SettingValue, StorageUsageKey, StorageUsageV1,
        VersionedStorageUsage,
    };
    use crate::StashFactory;

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn smoketest_migrate_json_to_proto() {
        let tls = mz_postgres_util::make_tls(&Config::new()).unwrap();
        let factory = StashFactory::new(&MetricsRegistry::new());

        // Connect to Cockroach.
        let connstr = std::env::var("COCKROACH_URL").expect("COCKROACH_URL must be set");
        let (mut client, connection) = tokio_postgres::connect(&connstr, tls.clone())
            .await
            .expect("able to connect");
        mz_ore::task::spawn(|| "tokio-postgres stash connection", async move {
            if let Err(e) = connection.await {
                tracing::error!("postgres stash connection error: {}", e);
            }
        });

        // Create a simple Stash we'll check against.
        let mut arbitrary_stash = ArbitraryStash::new();
        arbitrary_stash
            .config
            .items
            .push(("foo".to_string(), ConfigValue { value: 42 }, 1, 1));
        arbitrary_stash.setting.items.push((
            SettingKey {
                name: "version".to_string(),
            },
            SettingValue {
                value: "bar".to_string(),
            },
            2,
            -2,
        ));

        // Create a schema for our test.
        let seed: u32 = rand::thread_rng().gen();
        println!("Using Seed {seed}");
        let schema = format!("stash_test_{seed}");

        client
            .execute(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"), &[])
            .await
            .unwrap();
        client
            .execute(&format!("SET search_path TO {schema}"), &[])
            .await
            .unwrap();

        // Initialize the Stash.
        let stash = factory
            .open(connstr.to_string(), Some(schema), tls.clone())
            .await
            .unwrap();
        let epoch = stash.epoch().unwrap();

        insert_collections(&client, &arbitrary_stash).await;
        insert_stash(&client, &arbitrary_stash).await;

        // Migrate the Stash to protobuf.
        migrate_json_to_proto(&mut client, epoch)
            .await
            .expect("migration to succeed");

        let protos_config: Collection<objects_v15::ConfigKey, objects_v15::ConfigValue> =
            Collection::read(arbitrary_stash.config.id, &client)
                .await
                .expect("config");
        assert_eq!(protos_config.items[0].0.key, "foo");
        assert_eq!(protos_config.items[0].1.value, 42);
        assert_eq!(protos_config.items[0].2, 1);
        assert_eq!(protos_config.items[0].3, 1);

        let protos_setting: Collection<objects_v15::SettingKey, objects_v15::SettingValue> =
            Collection::read(arbitrary_stash.setting.id, &client)
                .await
                .expect("setting");
        assert_eq!(protos_setting.items[0].0.name, "version");
        assert_eq!(protos_setting.items[0].1.value, "bar");
        assert_eq!(protos_setting.items[0].2, 2);
        assert_eq!(protos_setting.items[0].3, -2);
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn test_unrecognized_collection() {
        mz_ore::test::init_logging();
        let tls = mz_postgres_util::make_tls(&Config::new()).unwrap();
        let factory = StashFactory::new(&MetricsRegistry::new());

        // Connect to Cockroach.
        let connstr = std::env::var("COCKROACH_URL").expect("COCKROACH_URL must be set");
        let (mut client, connection) = tokio_postgres::connect(&connstr, tls.clone())
            .await
            .expect("able to connect");
        mz_ore::task::spawn(|| "tokio-postgres stash connection", async move {
            if let Err(e) = connection.await {
                tracing::error!("postgres stash connection error: {}", e);
            }
        });

        // Create a schema for our test.
        let seed: u32 = rand::thread_rng().gen();
        println!("Using Seed {seed}");
        let schema = format!("stash_test_unrecognized_collection_{seed}");

        client
            .execute(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"), &[])
            .await
            .unwrap();
        client
            .execute(&format!("SET search_path TO {schema}"), &[])
            .await
            .unwrap();

        // Initialize the Stash.
        let stash = factory
            .open(connstr.to_string(), Some(schema), tls.clone())
            .await
            .unwrap();
        let epoch = stash.epoch().unwrap();

        // Insert a collection that is unknown.
        client
            .execute(
                "INSERT INTO collections (collection_id, name) VALUES ($1, $2)",
                &[&42i64, &"unknown-foo"],
            )
            .await
            .unwrap();

        // Migrate the Stash to protobuf.
        migrate_json_to_proto(&mut client, epoch)
            .await
            .expect("migration to succeed");
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn test_very_large_collections() {
        mz_ore::test::init_logging();
        let tls = mz_postgres_util::make_tls(&Config::new()).unwrap();
        let factory = StashFactory::new(&MetricsRegistry::new());

        // Connect to Cockroach.
        let connstr = std::env::var("COCKROACH_URL").expect("COCKROACH_URL must be set");
        let (mut client, connection) = tokio_postgres::connect(&connstr, tls.clone())
            .await
            .expect("able to connect");
        mz_ore::task::spawn(|| "tokio-postgres stash connection", async move {
            if let Err(e) = connection.await {
                tracing::error!("postgres stash connection error: {}", e);
            }
        });

        // Create a very large Stash we'll check against.
        let mut arbitrary_stash = ArbitraryStash::new();
        for i in 0..100_000 {
            arbitrary_stash.storage_usage.items.push((
                StorageUsageKey {
                    metric: VersionedStorageUsage::V1(StorageUsageV1 {
                        id: i,
                        shard_id: Some("i_am_a_shard_id_that_is_decently_large".to_string()),
                        size_bytes: i,
                        collection_timestamp: i,
                    }),
                },
                (),
                1,
                1,
            ));
        }

        // Create a schema for our test.
        let seed: u32 = rand::thread_rng().gen();
        println!("Using Seed {seed}");
        let schema = format!("stash_test_{seed}");

        client
            .execute(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"), &[])
            .await
            .unwrap();
        client
            .execute(&format!("SET search_path TO {schema}"), &[])
            .await
            .unwrap();

        // Initialize the Stash.
        let stash = factory
            .open(connstr.to_string(), Some(schema), tls.clone())
            .await
            .unwrap();
        let epoch = stash.epoch().unwrap();

        insert_collections(&client, &arbitrary_stash).await;
        insert_stash(&client, &arbitrary_stash).await;

        // Migrate the Stash to protobuf.
        migrate_json_to_proto(&mut client, epoch)
            .await
            .expect("migration to succeed");

        let protos_storage_usage: Collection<objects_v15::StorageUsageKey, ()> =
            Collection::read(arbitrary_stash.storage_usage.id, &client)
                .await
                .expect("storage_usage");
        assert_eq!(protos_storage_usage.items.len(), 100_000);
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn proptest_stash_migrate_json_to_proto() {
        let tls = mz_postgres_util::make_tls(&Config::new()).unwrap();
        let factory = StashFactory::new(&MetricsRegistry::new());

        // Connect to Cockroach.
        let connstr = std::env::var("COCKROACH_URL").expect("COCKROACH_URL must be set");
        let (mut client, connection) = tokio_postgres::connect(&connstr, tls.clone())
            .await
            .expect("able to connect");
        mz_ore::task::spawn(|| "tokio-postgres stash connection", async move {
            if let Err(e) = connection.await {
                tracing::error!("postgres stash connection error: {}", e);
            }
        });

        // Create a proptest Runner to generate random Stashes.
        let mut runner = TestRunner::deterministic();
        let strategy = any::<ArbitraryStash>();

        // These can take a little while to run, so lets always do 1/8 of PROPTEST_CASES.
        let cases = runner
            .config()
            .cases
            .checked_div(8)
            .unwrap_or_default()
            .max(1);
        for _ in 0..cases {
            // Generate an arbitrary Stash.
            let arbitrary_stash = strategy
                .new_tree(&mut runner)
                .expect("valid tree")
                .current();

            // Create a schema for our test.
            let seed: u32 = rand::thread_rng().gen();
            println!("Using Seed {seed}");
            let schema = format!("stash_proptest_{seed}");

            client
                .execute(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"), &[])
                .await
                .unwrap();
            client
                .execute(&format!("SET search_path TO {schema}"), &[])
                .await
                .unwrap();

            // Initialize the Stash.
            let stash = factory
                .open(connstr.to_string(), Some(schema), tls.clone())
                .await
                .unwrap();
            let epoch = stash.epoch().unwrap();

            insert_collections(&client, &arbitrary_stash).await;
            insert_stash(&client, &arbitrary_stash).await;

            // Migrate the Stash to protobuf.
            migrate_json_to_proto(&mut client, epoch)
                .await
                .expect("migration to succeed");
        }
    }
}

pub mod test_helpers {
    use itertools::Itertools;
    use proptest_derive::Arbitrary;
    use serde::Serialize;
    use tokio_postgres::types::ToSql;
    use tokio_postgres::Client;

    use crate::upgrade::legacy_types::{
        AuditLogKey, ClusterIntrospectionSourceIndexKey, ClusterIntrospectionSourceIndexValue,
        ClusterKey, ClusterReplicaKey, ClusterReplicaValue, ClusterValue, ConfigValue, DatabaseKey,
        DatabaseValue, DurableCollectionMetadata, DurableExportMetadata, GidMappingKey,
        GidMappingValue, GlobalId, IdAllocKey, IdAllocValue, ItemKey, ItemValue, RoleKey,
        RoleValue, SchemaKey, SchemaValue, ServerConfigurationKey, ServerConfigurationValue,
        SettingKey, SettingValue, ShardId, StorageUsageKey, TimestampKey, TimestampValue,
    };

    /// Insert all of the non-empty collections into CockroachDB.
    pub async fn insert_collections(client: &Client, stash: &ArbitraryStash) {
        let collections = stash.non_empty_collections();

        // Add all of the collections.
        let str_args: String = itertools::Itertools::intersperse(
            collections.iter().enumerate().map(|(idx, _)| {
                let idx = 1 + idx * 2;
                format!("(${}, ${})", idx, idx + 1)
            }),
            ", ".to_string(),
        )
        .collect();
        let stmt = format!("INSERT INTO collections (collection_id, name) VALUES {str_args}");
        let mut args: Vec<&'_ (dyn ToSql + Sync)> = vec![];

        for (collection, name) in &collections {
            let id = collection.id();

            args.push(id);
            args.push(name);
        }
        client.execute(&stmt, &args).await.unwrap();
    }

    /// Inserts all of the items for non-empty collections into CockroackDB.
    pub async fn insert_stash(client: &Client, stash: &ArbitraryStash) {
        for (collection, _name) in stash.non_empty_collections() {
            let id = collection.id();
            let items = collection.items();

            // Break into chunks so we don't go over the CockroachDB request size.
            for chunk in &items.into_iter().chunks(2048) {
                let chunk = chunk.collect_vec();

                // Generate an INSERT statement.
                let str_args: String = itertools::Itertools::intersperse(
                    chunk.iter().enumerate().map(|(idx, _)| {
                        let idx = 1 + idx * 5;
                        format!(
                            "(${}, ${}, ${}, ${}, ${})",
                            idx,
                            idx + 1,
                            idx + 2,
                            idx + 3,
                            idx + 4
                        )
                    }),
                    ", ".to_string(),
                )
                .collect();
                let stmt = format!(
                    "INSERT INTO data (collection_id, key, value, time, diff) VALUES {str_args}"
                );

                // Collect our arguments.
                let mut args: Vec<&'_ (dyn ToSql + Sync)> = vec![];
                for (key, val, ts, diff) in &chunk {
                    args.push(&id);
                    args.push(key);
                    args.push(val);
                    args.push(ts);
                    args.push(diff);
                }

                // Insert all of our data.
                client.execute(&stmt, &args).await.unwrap();
            }
        }
    }

    #[derive(Debug, Default, Clone, PartialEq, Eq, Arbitrary)]
    pub struct Collection<K, V> {
        pub id: i64,
        pub items: Vec<(K, V, Timestamp, Diff)>,
    }

    impl<K, V> Collection<K, V> {
        pub fn new(id: i64) -> Self {
            Collection {
                id,
                items: Vec::default(),
            }
        }
    }

    impl<K, V> Collection<K, V>
    where
        K: ::prost::Message + Default,
        V: ::prost::Message + Default,
    {
        pub async fn read(id: i64, client: &Client) -> Result<Self, anyhow::Error> {
            let rows = client
                .query(
                    "SELECT key, value, time, diff FROM data WHERE collection_id = $1",
                    &[&id],
                )
                .await?;

            let protos = rows
                .into_iter()
                .map(|row| {
                    let key: Vec<u8> = row.get("key");
                    let val: Vec<u8> = row.get("value");
                    let time: i64 = row.get("time");
                    let diff: i64 = row.get("diff");

                    let key = K::decode(&key[..]).expect("success");
                    let val = V::decode(&val[..]).expect("success");

                    (key, val, time, diff)
                })
                .collect();

            Ok(Collection { id, items: protos })
        }
    }

    impl<K: Serialize, V: Serialize> ArbitraryCollection for Collection<K, V> {
        fn id(&self) -> &i64 {
            &self.id
        }

        fn items(&self) -> Vec<(serde_json::Value, serde_json::Value, Timestamp, Diff)> {
            self.items
                .iter()
                .map(|(key, val, ts, diff)| {
                    let key = serde_json::to_value(key).unwrap();
                    let val = serde_json::to_value(val).unwrap();

                    (key, val, *ts, *diff)
                })
                .collect()
        }

        fn len(&self) -> usize {
            self.items.len()
        }
    }

    type Diff = i64;
    type Timestamp = i64;

    pub trait ArbitraryCollection {
        fn id(&self) -> &i64;
        fn items(&self) -> Vec<(serde_json::Value, serde_json::Value, Timestamp, Diff)>;
        fn len(&self) -> usize;
    }

    #[derive(Debug, Clone, Arbitrary)]
    pub struct ArbitraryStash {
        pub config: Collection<String, ConfigValue>,
        pub setting: Collection<SettingKey, SettingValue>,
        pub id_alloc: Collection<IdAllocKey, IdAllocValue>,
        pub system_gid_mapping: Collection<GidMappingKey, GidMappingValue>,
        pub compute_instance: Collection<ClusterKey, ClusterValue>,
        pub compute_introspection_source_index:
            Collection<ClusterIntrospectionSourceIndexKey, ClusterIntrospectionSourceIndexValue>,
        pub compute_replicas: Collection<ClusterReplicaKey, ClusterReplicaValue>,
        pub database: Collection<DatabaseKey, DatabaseValue>,
        pub schema: Collection<SchemaKey, SchemaValue>,
        pub item: Collection<ItemKey, ItemValue>,
        pub role: Collection<RoleKey, RoleValue>,
        pub timestamp: Collection<TimestampKey, TimestampValue>,
        pub system_configuration: Collection<ServerConfigurationKey, ServerConfigurationValue>,
        pub audit_log: Collection<AuditLogKey, ()>,
        pub storage_usage: Collection<StorageUsageKey, ()>,
        pub storage_collection_metadata: Collection<GlobalId, DurableCollectionMetadata>,
        pub storage_export_metadata_u64: Collection<GlobalId, DurableExportMetadata>,
        pub storage_shards_to_finalize: Collection<ShardId, ()>,
    }

    impl ArbitraryStash {
        // Create a new `ArbitraryStash` with some default collection IDs.
        pub fn new() -> Self {
            ArbitraryStash {
                config: Collection::new(1),
                setting: Collection::new(2),
                id_alloc: Collection::new(3),
                system_gid_mapping: Collection::new(4),
                compute_instance: Collection::new(5),
                compute_introspection_source_index: Collection::new(6),
                compute_replicas: Collection::new(7),
                database: Collection::new(8),
                schema: Collection::new(9),
                item: Collection::new(10),
                role: Collection::new(11),
                timestamp: Collection::new(12),
                system_configuration: Collection::new(13),
                audit_log: Collection::new(14),
                storage_usage: Collection::new(15),
                storage_collection_metadata: Collection::new(16),
                storage_export_metadata_u64: Collection::new(17),
                storage_shards_to_finalize: Collection::new(18),
            }
        }

        /// Returns all collections that contain at least one item.
        pub fn non_empty_collections(&self) -> Vec<(&dyn ArbitraryCollection, &'static str)> {
            let collections: Vec<(&dyn ArbitraryCollection, &'static str)> = vec![
                (&self.config, "config"),
                (&self.setting, "setting"),
                (&self.id_alloc, "id_alloc"),
                (&self.system_gid_mapping, "system_gid_mapping"),
                (&self.compute_instance, "compute_instance"),
                (
                    &self.compute_introspection_source_index,
                    "compute_introspection_source_index",
                ),
                (&self.compute_replicas, "compute_replicas"),
                (&self.database, "database"),
                (&self.schema, "schema"),
                (&self.item, "item"),
                (&self.role, "role"),
                (&self.timestamp, "timestamp"),
                (&self.system_configuration, "system_configuration"),
                (&self.audit_log, "audit_log"),
                (&self.storage_usage, "storage_usage"),
                (
                    &self.storage_collection_metadata,
                    "storage-collection-metadata",
                ),
                (
                    &self.storage_export_metadata_u64,
                    "storage-export-metadata-u64",
                ),
                (
                    &self.storage_shards_to_finalize,
                    "storage-shards-to-finalize",
                ),
            ];

            collections
                .into_iter()
                .filter(|(c, _name)| c.len() != 0)
                .collect()
        }
    }
}
