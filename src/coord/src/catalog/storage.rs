// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::hash::Hash;
use std::iter::once;

use bytes::BufMut;
use futures::future::BoxFuture;
use mz_repr::proto::{IntoRustIfSome, RustType};
use prost::{self, Message};
use uuid::Uuid;

use mz_dataflow_types::client::{
    ComputeInstanceId, ConcreteComputeInstanceReplicaConfig, ReplicaId,
};
use mz_ore::cast::CastFrom;
use mz_ore::collections::CollectionExt;
use mz_persist_types::Codec;
use mz_repr::global_id::ProtoGlobalId;
use mz_repr::GlobalId;
use mz_sql::catalog::CatalogError as SqlCatalogError;
use mz_sql::names::{
    DatabaseId, ObjectQualifiers, QualifiedObjectName, ResolvedDatabaseSpecifier, SchemaId,
    SchemaSpecifier,
};
use mz_sql::plan::ComputeInstanceIntrospectionConfig;
use mz_stash::{Append, AppendBatch, Stash, StashError, TableTransaction, TypedCollection};

use crate::catalog::builtin::BuiltinLog;
use crate::catalog::error::{Error, ErrorKind};

const USER_VERSION: &str = "user_version";

async fn migrate<S: Append>(stash: &mut S, version: u64) -> Result<(), StashError> {
    // Initial state.
    let migrations: &[fn(&mut S) -> BoxFuture<Result<(), StashError>>] = &[
        |stash| {
            Box::pin(async {
                // Bump uppers so peek works.
                COLLECTION_SETTING.upsert(stash, vec![]).await?;
                COLLECTION_SYSTEM_GID_MAPPING.upsert(stash, vec![]).await?;
                COLLECTION_COMPUTE_INTROSPECTION_SOURCE_INDEX
                    .upsert(stash, vec![])
                    .await?;
                COLLECTION_ITEM.upsert(stash, vec![]).await?;

                COLLECTION_GID_ALLOC
                    .upsert(
                        stash,
                        vec![
                            (
                                GidAllocKey {
                                    name: "user".into(),
                                },
                                GidAllocValue { next_gid: 1 },
                            ),
                            (
                                GidAllocKey {
                                    name: "system".into(),
                                },
                                GidAllocValue { next_gid: 1 },
                            ),
                        ],
                    )
                    .await?;
                COLLECTION_DATABASE
                    .upsert(
                        stash,
                        vec![(
                            DatabaseKey { id: 1 },
                            DatabaseValue {
                                name: "materialize".into(),
                            },
                        )],
                    )
                    .await?;
                COLLECTION_SCHEMA
                    .upsert(
                        stash,
                        vec![
                            (
                                SchemaKey { id: 1 },
                                SchemaValue {
                                    database_id: None,
                                    name: "mz_catalog".into(),
                                },
                            ),
                            (
                                SchemaKey { id: 2 },
                                SchemaValue {
                                    database_id: None,
                                    name: "pg_catalog".into(),
                                },
                            ),
                            (
                                SchemaKey { id: 3 },
                                SchemaValue {
                                    database_id: Some(1),
                                    name: "public".into(),
                                },
                            ),
                            (
                                SchemaKey { id: 4 },
                                SchemaValue {
                                    database_id: None,
                                    name: "mz_internal".into(),
                                },
                            ),
                            (
                                SchemaKey { id: 5 },
                                SchemaValue {
                                    database_id: None,
                                    name: "information_schema".into(),
                                },
                            ),
                        ],
                    )
                    .await?;
                COLLECTION_ROLE
                    .upsert(
                        stash,
                        vec![(
                            RoleKey { id: 1 },
                            RoleValue {
                                name: "materialize".into(),
                            },
                        )],
                    )
                    .await?;
                COLLECTION_COMPUTE_INSTANCES
                    .upsert(
                        stash,
                        vec![(
                    ComputeInstanceKey { id: 1 },
                    ComputeInstanceValue {
                        name: "default".into(),
                        config: Some(
                            "{\"debugging\":false,\"granularity\":{\"secs\":1,\"nanos\":0}}".into(),
                        ),
                    },
                )],
                    )
                    .await?;
                COLLECTION_COMPUTE_INSTANCE_REPLICAS.upsert(
                    stash,
                    vec![
                        (
                            ComputeInstanceReplicaKey { id: 1},
                            ComputeInstanceReplicaValue {
                                compute_instance_id: 1,
                                name: "default_replica".into(),
                                config: "{\"Managed\":{\"size_config\":{\"memory_limit\": null, \"cpu_limit\": null, \"scale\": 1, \"workers\": 1}}}".into(),
                            }
                        )
                    ]
                ).await?;
                Ok(())
            })
        },
        // Add new migrations here.
        //
        // Migrations should be preceded with a comment of the following form:
        //
        //     > Short summary of migration's purpose.
        //     >
        //     > Introduced in <VERSION>.
        //     >
        //     > Optional additional commentary about safety or approach.
        //
        // Please include @benesch on any code reviews that add or edit migrations.
        // Migrations must preserve backwards compatibility with all past releases
        // of materialized. Migrations can be edited up until they ship in a
        // release, after which they must never be removed, only patched by future
        // migrations. Migrations must be transactional or idempotent (in case of
        // midway failure).
    ];

    for (i, migration) in migrations
        .iter()
        .enumerate()
        .skip(usize::cast_from(version))
    {
        (migration)(stash).await?;
        COLLECTION_CONFIG
            .upsert_key(
                stash,
                &USER_VERSION.to_string(),
                &ConfigValue {
                    value: u64::cast_from(i),
                },
            )
            .await?;
    }
    Ok(())
}

#[derive(Debug)]
pub struct Connection<S> {
    stash: S,
    cluster_id: Uuid,
}

impl<S: Append> Connection<S> {
    pub async fn open(mut stash: S) -> Result<Connection<S>, Error> {
        // Run unapplied migrations. The `user_version` field stores the index
        // of the last migration that was run. If the upper is min, the config
        // collection is empty.
        let skip = if COLLECTION_CONFIG.upper(&mut stash).await?.elements()
            == [mz_stash::Timestamp::MIN]
        {
            0
        } else {
            // An advanced collection must have had its user version set, so the unwrap
            // must succeed.
            COLLECTION_CONFIG
                .peek_key_one(&mut stash, &USER_VERSION.to_string())
                .await?
                .expect("user_version must exist")
                .value
                + 1
        };
        migrate(&mut stash, skip).await?;

        let conn = Connection {
            cluster_id: Self::set_or_get_cluster_id(&mut stash).await?,
            stash,
        };

        Ok(conn)
    }
}

impl<S: Append> Connection<S> {
    async fn get_setting(&mut self, key: &str) -> Result<Option<String>, Error> {
        Self::get_setting_stash(&mut self.stash, key).await
    }

    async fn set_setting(&mut self, key: &str, value: &str) -> Result<(), Error> {
        Self::set_setting_stash(&mut self.stash, key, value).await
    }

    async fn get_setting_stash(stash: &mut impl Stash, key: &str) -> Result<Option<String>, Error> {
        let settings = COLLECTION_SETTING.get(stash).await?;
        let v = stash
            .peek_key_one(
                settings,
                &SettingKey {
                    name: key.to_string(),
                },
            )
            .await?;
        Ok(v.map(|v| v.value))
    }

    async fn set_setting_stash<V: Into<String> + std::fmt::Display>(
        stash: &mut impl Append,
        key: &str,
        value: V,
    ) -> Result<(), Error> {
        let key = SettingKey {
            name: key.to_string(),
        };
        let value = SettingValue {
            value: value.into(),
        };
        COLLECTION_SETTING
            .upsert(stash, once((key, value)))
            .await
            .map_err(|e| e.into())
    }

    /// Sets catalog's `cluster_id` setting on initialization or gets that value.
    async fn set_or_get_cluster_id(stash: &mut impl Append) -> Result<Uuid, Error> {
        let current_setting = Self::get_setting_stash(stash, "cluster_id").await?;
        match current_setting {
            // Server init
            None => {
                // Generate a new version 4 UUID. These are generated from random input.
                let cluster_id = Uuid::new_v4();
                Self::set_setting_stash(stash, "cluster_id", cluster_id.to_string()).await?;
                Ok(cluster_id)
            }
            // Server reboot
            Some(cs) => Ok(Uuid::parse_str(&cs)?),
        }
    }

    pub async fn get_catalog_content_version(&mut self) -> Result<Option<String>, Error> {
        self.get_setting("catalog_content_version").await
    }

    pub async fn set_catalog_content_version(&mut self, new_version: &str) -> Result<(), Error> {
        self.set_setting("catalog_content_version", new_version)
            .await
    }

    pub async fn load_databases(&mut self) -> Result<Vec<(DatabaseId, String)>, Error> {
        Ok(COLLECTION_DATABASE
            .peek_one(&mut self.stash)
            .await?
            .into_iter()
            .map(|(k, v)| (DatabaseId::new(k.id), v.name))
            .collect())
    }

    pub async fn load_schemas(
        &mut self,
    ) -> Result<Vec<(SchemaId, String, Option<DatabaseId>)>, Error> {
        Ok(COLLECTION_SCHEMA
            .peek_one(&mut self.stash)
            .await?
            .into_iter()
            .map(|(k, v)| {
                (
                    SchemaId::new(k.id),
                    v.name,
                    v.database_id.map(DatabaseId::new),
                )
            })
            .collect())
    }

    pub async fn load_roles(&mut self) -> Result<Vec<(i64, String)>, Error> {
        Ok(COLLECTION_ROLE
            .peek_one(&mut self.stash)
            .await?
            .into_iter()
            .map(|(k, v)| (k.id, v.name))
            .collect())
    }

    pub async fn load_compute_instances(
        &mut self,
    ) -> Result<
        Vec<(
            ComputeInstanceId,
            String,
            Option<ComputeInstanceIntrospectionConfig>,
        )>,
        Error,
    > {
        COLLECTION_COMPUTE_INSTANCES
            .peek_one(&mut self.stash)
            .await?
            .into_iter()
            .map(|(k, v)| {
                let config: Option<ComputeInstanceIntrospectionConfig> = match v.config {
                    None => None,
                    Some(config) => serde_json::from_str(&config)
                        .map_err(|err| Error::from(StashError::from(err.to_string())))?,
                };
                Ok((k.id, v.name, config))
            })
            .collect()
    }

    pub async fn load_compute_instance_replicas(
        &mut self,
    ) -> Result<
        Vec<(
            ComputeInstanceId,
            ReplicaId,
            String,
            ConcreteComputeInstanceReplicaConfig,
        )>,
        Error,
    > {
        COLLECTION_COMPUTE_INSTANCE_REPLICAS
            .peek_one(&mut self.stash)
            .await?
            .into_iter()
            .map(|(k, v)| {
                let config = serde_json::from_str(&v.config)
                    .map_err(|err| Error::from(StashError::from(err.to_string())))?;
                Ok((v.compute_instance_id, k.id, v.name, config))
            })
            .collect()
    }

    /// Load the persisted mapping of system object to global ID. Key is (schema-name, object-name).
    pub async fn load_system_gids(
        &mut self,
    ) -> Result<BTreeMap<(String, String), (GlobalId, u64)>, Error> {
        Ok(COLLECTION_SYSTEM_GID_MAPPING
            .peek_one(&mut self.stash)
            .await?
            .into_iter()
            .map(|(k, v)| {
                (
                    (k.schema_name, k.object_name),
                    (GlobalId::System(v.id), v.fingerprint),
                )
            })
            .collect())
    }

    pub async fn load_introspection_source_index_gids(
        &mut self,
        compute_id: ComputeInstanceId,
    ) -> Result<BTreeMap<String, GlobalId>, Error> {
        Ok(COLLECTION_COMPUTE_INTROSPECTION_SOURCE_INDEX
            .peek_one(&mut self.stash)
            .await?
            .into_iter()
            .filter_map(|(k, v)| {
                if k.compute_id == compute_id {
                    Some((k.name, GlobalId::System(v.index_id)))
                } else {
                    None
                }
            })
            .collect())
    }

    /// Persist mapping from system objects to global IDs. Each element of `mappings` should be
    /// (schema-name, object-name, global-id).
    ///
    /// Panics if provided id is not a system id
    pub async fn set_system_gids(
        &mut self,
        mappings: Vec<(&str, &str, GlobalId, u64)>,
    ) -> Result<(), Error> {
        if mappings.is_empty() {
            return Ok(());
        }

        let mappings = mappings
            .into_iter()
            .map(|(schema_name, object_name, id, fingerprint)| {
                let id = if let GlobalId::System(id) = id {
                    id
                } else {
                    panic!("non-system id provided")
                };
                (
                    GidMappingKey {
                        schema_name: schema_name.to_string(),
                        object_name: object_name.to_string(),
                    },
                    GidMappingValue { id, fingerprint },
                )
            });
        COLLECTION_SYSTEM_GID_MAPPING
            .upsert(&mut self.stash, mappings)
            .await
            .map_err(|e| e.into())
    }

    /// Panics if provided id is not a system id
    pub async fn set_introspection_source_index_gids(
        &mut self,
        mappings: Vec<(ComputeInstanceId, &str, GlobalId)>,
    ) -> Result<(), Error> {
        if mappings.is_empty() {
            return Ok(());
        }

        let mappings = mappings.into_iter().map(|(compute_id, name, index_id)| {
            let index_id = if let GlobalId::System(id) = index_id {
                id
            } else {
                panic!("non-system id provided")
            };
            (
                ComputeIntrospectionSourceIndexKey {
                    compute_id,
                    name: name.to_string(),
                },
                ComputeIntrospectionSourceIndexValue { index_id },
            )
        });
        COLLECTION_COMPUTE_INTROSPECTION_SOURCE_INDEX
            .upsert(&mut self.stash, mappings)
            .await
            .map_err(|e| e.into())
    }

    pub async fn allocate_system_ids(&mut self, amount: u64) -> Result<Vec<GlobalId>, Error> {
        let id = self.allocate_global_id("system", amount).await?;

        Ok(id.into_iter().map(GlobalId::System).collect())
    }

    pub async fn allocate_user_id(&mut self) -> Result<GlobalId, Error> {
        let id = self.allocate_global_id("user", 1).await?;
        let id = id.into_element();
        Ok(GlobalId::User(id))
    }

    async fn allocate_global_id(&mut self, id_type: &str, amount: u64) -> Result<Vec<u64>, Error> {
        let key = GidAllocKey {
            name: id_type.to_string(),
        };
        let prev = COLLECTION_GID_ALLOC
            .peek_key_one(&mut self.stash, &key)
            .await?;
        let id = prev.expect("must exist").next_gid;
        let next = match id.checked_add(amount) {
            Some(next_gid) => GidAllocValue { next_gid },
            None => return Err(Error::new(ErrorKind::IdExhaustion)),
        };
        COLLECTION_GID_ALLOC
            .upsert_key(&mut self.stash, &key, &next)
            .await?;
        Ok((id..next.next_gid).collect())
    }

    pub async fn transaction<'a>(&'a mut self) -> Result<Transaction<'a, S>, Error> {
        let databases = COLLECTION_DATABASE.peek_one(&mut self.stash).await?;
        let schemas = COLLECTION_SCHEMA.peek_one(&mut self.stash).await?;
        let roles = COLLECTION_ROLE.peek_one(&mut self.stash).await?;
        let items = COLLECTION_ITEM.peek_one(&mut self.stash).await?;
        let compute_instances = COLLECTION_COMPUTE_INSTANCES
            .peek_one(&mut self.stash)
            .await?;
        let compute_instance_replicas = COLLECTION_COMPUTE_INSTANCE_REPLICAS
            .peek_one(&mut self.stash)
            .await?;
        let introspection_sources = COLLECTION_COMPUTE_INTROSPECTION_SOURCE_INDEX
            .peek_one(&mut self.stash)
            .await?;

        Ok(Transaction {
            stash: &mut self.stash,
            databases: TableTransaction::new(databases, Some(|k| k.id), |a, b| a.name == b.name),
            schemas: TableTransaction::new(schemas, Some(|k| k.id), |a, b| {
                a.database_id == b.database_id && a.name == b.name
            }),
            items: TableTransaction::new(items, None, |a, b| {
                a.schema_id == b.schema_id && a.name == b.name
            }),
            roles: TableTransaction::new(roles, Some(|k| k.id), |a, b| a.name == b.name),
            compute_instances: TableTransaction::new(compute_instances, Some(|k| k.id), |a, b| {
                a.name == b.name
            }),
            compute_instance_replicas: TableTransaction::new(
                compute_instance_replicas,
                Some(|k| k.id),
                |a, b| a.compute_instance_id == b.compute_instance_id && a.name == b.name,
            ),
            introspection_sources: TableTransaction::new(introspection_sources, None, |_a, _b| {
                false
            }),
        })
    }

    pub fn cluster_id(&self) -> Uuid {
        self.cluster_id
    }
}

pub struct Transaction<'a, S> {
    stash: &'a mut S,
    databases: TableTransaction<DatabaseKey, DatabaseValue, i64>,
    schemas: TableTransaction<SchemaKey, SchemaValue, i64>,
    items: TableTransaction<ItemKey, ItemValue, i64>,
    roles: TableTransaction<RoleKey, RoleValue, i64>,
    compute_instances: TableTransaction<ComputeInstanceKey, ComputeInstanceValue, i64>,
    compute_instance_replicas:
        TableTransaction<ComputeInstanceReplicaKey, ComputeInstanceReplicaValue, i64>,
    introspection_sources: TableTransaction<
        ComputeIntrospectionSourceIndexKey,
        ComputeIntrospectionSourceIndexValue,
        i64,
    >,
}

impl<'a, S: Append> Transaction<'a, S> {
    pub fn loaded_items(&self) -> Vec<(GlobalId, QualifiedObjectName, Vec<u8>)> {
        let databases = self.databases.items();
        let schemas = self.schemas.items();
        let mut items = Vec::new();
        self.items.for_values(|k, v| {
            let schema = match schemas.get(&SchemaKey { id: v.schema_id }) {
                Some(schema) => schema,
                None => return,
            };
            let database_id = match schema.database_id {
                Some(id) => id,
                None => return,
            };
            let _database = match databases.get(&DatabaseKey { id: database_id }) {
                Some(database) => database,
                None => return,
            };
            items.push((
                k.gid,
                QualifiedObjectName {
                    qualifiers: ObjectQualifiers {
                        database_spec: ResolvedDatabaseSpecifier::from(database_id),
                        schema_spec: SchemaSpecifier::from(v.schema_id),
                    },
                    item: v.name.clone(),
                },
                v.definition.clone(),
            ));
        });
        items.sort_by_key(|(id, _, _)| *id);
        items
    }

    pub fn insert_database(&mut self, database_name: &str) -> Result<DatabaseId, Error> {
        match self.databases.insert(
            |id| DatabaseKey { id: id.unwrap() },
            DatabaseValue {
                name: database_name.to_string(),
            },
        ) {
            Ok(id) => Ok(DatabaseId::new(id.unwrap())),
            Err(()) => Err(Error::new(ErrorKind::DatabaseAlreadyExists(
                database_name.to_owned(),
            ))),
        }
    }

    pub fn insert_schema(
        &mut self,
        database_id: DatabaseId,
        schema_name: &str,
    ) -> Result<SchemaId, Error> {
        match self.schemas.insert(
            |id| SchemaKey { id: id.unwrap() },
            SchemaValue {
                database_id: Some(database_id.0),
                name: schema_name.to_string(),
            },
        ) {
            Ok(id) => Ok(SchemaId::new(id.unwrap())),
            Err(()) => Err(Error::new(ErrorKind::SchemaAlreadyExists(
                schema_name.to_owned(),
            ))),
        }
    }

    pub fn insert_role(&mut self, role_name: &str) -> Result<i64, Error> {
        match self.roles.insert(
            |id| RoleKey { id: id.unwrap() },
            RoleValue {
                name: role_name.to_string(),
            },
        ) {
            Ok(id) => Ok(id.unwrap()),
            Err(()) => Err(Error::new(ErrorKind::RoleAlreadyExists(
                role_name.to_owned(),
            ))),
        }
    }

    /// Panics if any introspection source id is not a system id
    pub fn insert_compute_instance(
        &mut self,
        cluster_name: &str,
        config: &Option<ComputeInstanceIntrospectionConfig>,
        introspection_sources: &Vec<(&'static BuiltinLog, GlobalId)>,
    ) -> Result<ComputeInstanceId, Error> {
        let config = serde_json::to_string(config)
            .map_err(|err| Error::from(StashError::from(err.to_string())))?;
        let id = match self.compute_instances.insert(
            |id| ComputeInstanceKey { id: id.unwrap() },
            ComputeInstanceValue {
                name: cluster_name.to_string(),
                config: Some(config),
            },
        ) {
            Ok(id) => id.unwrap(),
            Err(()) => {
                return Err(Error::new(ErrorKind::ClusterAlreadyExists(
                    cluster_name.to_owned(),
                )))
            }
        };

        for (builtin, index_id) in introspection_sources {
            let index_id = if let GlobalId::System(id) = index_id {
                *id
            } else {
                panic!("non-system id provided")
            };
            self.introspection_sources
                .insert(
                    |_| ComputeIntrospectionSourceIndexKey {
                        compute_id: id,
                        name: builtin.name.to_string(),
                    },
                    ComputeIntrospectionSourceIndexValue { index_id },
                )
                .expect("no uniqueness violation");
        }

        Ok(id)
    }

    pub fn insert_compute_instance_replica(
        &mut self,
        compute_name: &str,
        replica_name: &str,
        config: &ConcreteComputeInstanceReplicaConfig,
    ) -> Result<ReplicaId, Error> {
        let config = serde_json::to_string(config)
            .map_err(|err| Error::from(StashError::from(err.to_string())))?;
        let mut compute_instance_id = None;
        for (
            ComputeInstanceKey { id },
            ComputeInstanceValue {
                name,
                config: _config,
            },
        ) in self.compute_instances.items()
        {
            if &name == compute_name {
                compute_instance_id = Some(id);
                break;
            }
        }
        let id = match self.compute_instance_replicas.insert(
            |id| ComputeInstanceReplicaKey { id: id.unwrap() },
            ComputeInstanceReplicaValue {
                compute_instance_id: compute_instance_id.unwrap(),
                name: replica_name.into(),
                config,
            },
        ) {
            Ok(id) => id.unwrap(),
            Err(_) => {
                return Err(Error::new(ErrorKind::DuplicateReplica(
                    replica_name.to_string(),
                    compute_name.to_string(),
                )))
            }
        };
        Ok(id)
    }

    pub fn insert_item(
        &mut self,
        id: GlobalId,
        schema_id: SchemaId,
        item_name: &str,
        item: &[u8],
    ) -> Result<(), Error> {
        match self.items.insert(
            |_| ItemKey { gid: id },
            ItemValue {
                schema_id: schema_id.0,
                name: item_name.to_string(),
                definition: item.to_vec(),
            },
        ) {
            Ok(_) => Ok(()),
            Err(()) => Err(Error::new(ErrorKind::ItemAlreadyExists(
                item_name.to_owned(),
            ))),
        }
    }

    pub fn remove_database(&mut self, id: &DatabaseId) -> Result<(), Error> {
        let n = self.databases.delete(|k, _v| k.id == id.0).len();
        assert!(n <= 1);
        if n == 1 {
            Ok(())
        } else {
            Err(SqlCatalogError::UnknownDatabase(id.to_string()).into())
        }
    }

    pub fn remove_schema(
        &mut self,
        database_id: &DatabaseId,
        schema_id: &SchemaId,
    ) -> Result<(), Error> {
        let n = self.schemas.delete(|k, _v| k.id == schema_id.0).len();
        assert!(n <= 1);
        if n == 1 {
            Ok(())
        } else {
            Err(SqlCatalogError::UnknownSchema(format!("{}.{}", database_id.0, schema_id.0)).into())
        }
    }

    pub fn remove_role(&mut self, name: &str) -> Result<(), Error> {
        let n = self.roles.delete(|_k, v| v.name == name).len();
        assert!(n <= 1);
        if n == 1 {
            Ok(())
        } else {
            Err(SqlCatalogError::UnknownRole(name.to_owned()).into())
        }
    }

    pub fn remove_compute_instance(&mut self, name: &str) -> Result<Vec<GlobalId>, Error> {
        let deleted = self.compute_instances.delete(|_k, v| v.name == name);
        if deleted.is_empty() {
            Err(SqlCatalogError::UnknownComputeInstance(name.to_owned()).into())
        } else {
            assert_eq!(deleted.len(), 1);
            // Cascade delete introsepction sources and cluster replicas.
            let id = deleted.into_element().0.id;
            self.compute_instance_replicas
                .delete(|_k, v| v.compute_instance_id == id);
            let introspection_source_indexes = self
                .introspection_sources
                .delete(|k, _v| k.compute_id == id);
            Ok(introspection_source_indexes
                .into_iter()
                .map(|(_, v)| GlobalId::System(v.index_id))
                .collect())
        }
    }

    pub fn remove_compute_instance_replica(
        &mut self,
        name: &str,
        compute_id: ComputeInstanceId,
    ) -> Result<(), Error> {
        let deleted = self
            .compute_instance_replicas
            .delete(|_k, v| v.compute_instance_id == compute_id && v.name == name);
        if deleted.len() == 1 {
            Ok(())
        } else {
            assert!(deleted.is_empty());
            Err(SqlCatalogError::UnknownComputeInstanceReplica(name.to_owned()).into())
        }
    }

    pub fn remove_item(&mut self, id: GlobalId) -> Result<(), Error> {
        let n = self.items.delete(|k, _v| k.gid == id).len();
        assert!(n <= 1);
        if n == 1 {
            Ok(())
        } else {
            Err(SqlCatalogError::UnknownItem(id.to_string()).into())
        }
    }

    pub fn update_item(&mut self, id: GlobalId, item_name: &str, item: &[u8]) -> Result<(), Error> {
        let n = self.items.update(|k, v| {
            if k.gid == id {
                Some(ItemValue {
                    schema_id: v.schema_id,
                    name: item_name.to_string(),
                    definition: item.to_vec(),
                })
            } else {
                None
            }
        })?;
        assert!(n <= 1);
        if n == 1 {
            Ok(())
        } else {
            Err(SqlCatalogError::UnknownItem(id.to_string()).into())
        }
    }

    pub async fn commit(self) -> Result<(), Error> {
        let mut batches = Vec::new();
        async fn add_batch<K, V, S, I>(
            stash: &mut S,
            batches: &mut Vec<AppendBatch>,
            collection: &TypedCollection<K, V>,
            changes: I,
        ) -> Result<(), Error>
        where
            K: mz_stash::Data,
            V: mz_stash::Data,
            S: Append,
            I: IntoIterator<Item = (K, V, mz_stash::Diff)>,
        {
            let mut changes = changes.into_iter().peekable();
            if changes.peek().is_none() {
                return Ok(());
            }
            let collection = collection.get(stash).await?;
            let mut batch = collection.make_batch(stash).await?;
            for (k, v, diff) in changes {
                collection.append_to_batch(&mut batch, &k, &v, diff);
            }
            batches.push(batch);
            Ok(())
        }
        add_batch(
            self.stash,
            &mut batches,
            &COLLECTION_DATABASE,
            self.databases.pending(),
        )
        .await?;
        add_batch(
            self.stash,
            &mut batches,
            &COLLECTION_SCHEMA,
            self.schemas.pending(),
        )
        .await?;
        add_batch(
            self.stash,
            &mut batches,
            &COLLECTION_ITEM,
            self.items.pending(),
        )
        .await?;
        add_batch(
            self.stash,
            &mut batches,
            &COLLECTION_ROLE,
            self.roles.pending(),
        )
        .await?;
        add_batch(
            self.stash,
            &mut batches,
            &COLLECTION_COMPUTE_INSTANCES,
            self.compute_instances.pending(),
        )
        .await?;
        add_batch(
            self.stash,
            &mut batches,
            &COLLECTION_COMPUTE_INSTANCE_REPLICAS,
            self.compute_instance_replicas.pending(),
        )
        .await?;
        add_batch(
            self.stash,
            &mut batches,
            &COLLECTION_COMPUTE_INTROSPECTION_SOURCE_INDEX,
            self.introspection_sources.pending(),
        )
        .await?;
        if batches.is_empty() {
            return Ok(());
        }
        self.stash.append(batches).await.map_err(|e| e.into())
    }
}

macro_rules! impl_codec {
    ($ty:ty) => {
        impl Codec for $ty {
            fn codec_name() -> String {
                "protobuf[$ty]".into()
            }

            fn encode<B: BufMut>(&self, buf: &mut B) {
                Message::encode(self, buf).expect("provided buffer had sufficient capacity")
            }

            fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
                Message::decode(buf).map_err(|err| err.to_string())
            }
        }
    };
}

#[derive(Clone, Message, PartialOrd, PartialEq, Eq, Ord, Hash)]
struct SettingKey {
    #[prost(string)]
    name: String,
}
impl_codec!(SettingKey);

#[derive(Clone, Message, PartialOrd, PartialEq, Eq, Ord)]
struct SettingValue {
    #[prost(string)]
    value: String,
}
impl_codec!(SettingValue);

#[derive(Clone, Message, PartialOrd, PartialEq, Eq, Ord, Hash)]
struct GidAllocKey {
    #[prost(string)]
    name: String,
}
impl_codec!(GidAllocKey);

#[derive(Clone, Message, PartialOrd, PartialEq, Eq, Ord)]
struct GidAllocValue {
    #[prost(uint64)]
    next_gid: u64,
}
impl_codec!(GidAllocValue);

#[derive(Clone, Message, PartialOrd, PartialEq, Eq, Ord, Hash)]
struct GidMappingKey {
    #[prost(string)]
    schema_name: String,
    #[prost(string)]
    object_name: String,
}
impl_codec!(GidMappingKey);

#[derive(Clone, Message, PartialOrd, PartialEq, Eq, Ord)]
struct GidMappingValue {
    #[prost(uint64)]
    id: u64,
    #[prost(uint64)]
    fingerprint: u64,
}
impl_codec!(GidMappingValue);

#[derive(Clone, Message, PartialOrd, PartialEq, Eq, Ord, Hash)]
struct ComputeInstanceKey {
    #[prost(int64)]
    id: i64,
}
impl_codec!(ComputeInstanceKey);

#[derive(Clone, Message, PartialOrd, PartialEq, Eq, Ord)]
struct ComputeInstanceValue {
    #[prost(string)]
    name: String,
    #[prost(string, optional)]
    config: Option<String>,
}
impl_codec!(ComputeInstanceValue);

#[derive(Clone, Message, PartialOrd, PartialEq, Eq, Ord, Hash)]
struct ComputeIntrospectionSourceIndexKey {
    #[prost(int64)]
    compute_id: i64,
    #[prost(string)]
    name: String,
}
impl_codec!(ComputeIntrospectionSourceIndexKey);

#[derive(Clone, Message, PartialOrd, PartialEq, Eq, Ord)]
struct ComputeIntrospectionSourceIndexValue {
    #[prost(uint64)]
    index_id: u64,
}
impl_codec!(ComputeIntrospectionSourceIndexValue);

#[derive(Clone, Message, PartialOrd, PartialEq, Eq, Ord, Hash)]
struct DatabaseKey {
    #[prost(int64)]
    id: i64,
}
impl_codec!(DatabaseKey);

#[derive(Clone, Message, PartialOrd, PartialEq, Eq, Ord, Hash)]
struct ComputeInstanceReplicaKey {
    #[prost(int64)]
    id: ReplicaId,
}
impl_codec!(ComputeInstanceReplicaKey);

#[derive(Clone, Message, PartialOrd, PartialEq, Eq, Ord)]
struct ComputeInstanceReplicaValue {
    #[prost(int64)]
    compute_instance_id: ComputeInstanceId,
    #[prost(string)]
    name: String,
    #[prost(string)]
    config: String,
}
impl_codec!(ComputeInstanceReplicaValue);

#[derive(Clone, Message, PartialOrd, PartialEq, Eq, Ord)]
struct DatabaseValue {
    #[prost(string)]
    name: String,
}
impl_codec!(DatabaseValue);

#[derive(Clone, Message, PartialOrd, PartialEq, Eq, Ord, Hash)]
struct SchemaKey {
    #[prost(int64)]
    id: i64,
}
impl_codec!(SchemaKey);

#[derive(Clone, Message, PartialOrd, PartialEq, Eq, Ord)]
struct SchemaValue {
    #[prost(int64, optional)]
    database_id: Option<i64>,
    #[prost(string)]
    name: String,
}
impl_codec!(SchemaValue);

#[derive(Clone, Debug, PartialOrd, PartialEq, Eq, Ord, Hash)]
struct ItemKey {
    gid: GlobalId,
}

#[derive(Clone, Message)]
struct ProtoItemKey {
    #[prost(message)]
    gid: Option<ProtoGlobalId>,
}

// To pleasantly support GlobalId, use a custom impl.
// TODO: Is there a better way to do this?
impl Codec for ItemKey {
    fn codec_name() -> String {
        "protobuf[ItemKey]".into()
    }

    fn encode<B: BufMut>(&self, buf: &mut B) {
        let proto = ProtoItemKey {
            gid: Some(self.gid.into_proto()),
        };
        Message::encode(&proto, buf).expect("provided buffer had sufficient capacity")
    }

    fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
        let proto: ProtoItemKey = Message::decode(buf).map_err(|err| err.to_string())?;
        let gid = proto
            .gid
            .into_rust_if_some("ProtoItemKey.gid")
            .map_err(|e| e.to_string())?;
        Ok(Self { gid })
    }
}

#[derive(Clone, Message, PartialOrd, PartialEq, Eq, Ord)]
struct ItemValue {
    #[prost(int64)]
    schema_id: i64,
    #[prost(string)]
    name: String,
    #[prost(bytes)]
    definition: Vec<u8>,
}
impl_codec!(ItemValue);

#[derive(Clone, Message, PartialOrd, PartialEq, Eq, Ord, Hash)]
struct RoleKey {
    #[prost(int64)]
    id: i64,
}
impl_codec!(RoleKey);

#[derive(Clone, Message, PartialOrd, PartialEq, Eq, Ord)]
struct RoleValue {
    #[prost(string)]
    name: String,
}
impl_codec!(RoleValue);

#[derive(Clone, Message, PartialOrd, PartialEq, Eq, Ord, Hash)]
struct ConfigValue {
    #[prost(uint64)]
    value: u64,
}
impl_codec!(ConfigValue);

static COLLECTION_CONFIG: TypedCollection<String, ConfigValue> = TypedCollection::new("config");
static COLLECTION_SETTING: TypedCollection<SettingKey, SettingValue> =
    TypedCollection::new("setting");
static COLLECTION_GID_ALLOC: TypedCollection<GidAllocKey, GidAllocValue> =
    TypedCollection::new("gid_alloc");
static COLLECTION_SYSTEM_GID_MAPPING: TypedCollection<GidMappingKey, GidMappingValue> =
    TypedCollection::new("system_gid_mapping");
static COLLECTION_COMPUTE_INSTANCES: TypedCollection<ComputeInstanceKey, ComputeInstanceValue> =
    TypedCollection::new("compute_instance");
static COLLECTION_COMPUTE_INTROSPECTION_SOURCE_INDEX: TypedCollection<
    ComputeIntrospectionSourceIndexKey,
    ComputeIntrospectionSourceIndexValue,
> = TypedCollection::new("compute_introspection_source_index");
static COLLECTION_COMPUTE_INSTANCE_REPLICAS: TypedCollection<
    ComputeInstanceReplicaKey,
    ComputeInstanceReplicaValue,
> = TypedCollection::new("compute_instance_replicas");
static COLLECTION_DATABASE: TypedCollection<DatabaseKey, DatabaseValue> =
    TypedCollection::new("database");
static COLLECTION_SCHEMA: TypedCollection<SchemaKey, SchemaValue> = TypedCollection::new("schema");
static COLLECTION_ITEM: TypedCollection<ItemKey, ItemValue> = TypedCollection::new("item");
static COLLECTION_ROLE: TypedCollection<RoleKey, RoleValue> = TypedCollection::new("role");
