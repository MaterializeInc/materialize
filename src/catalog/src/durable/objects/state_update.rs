// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module contains various representations of a single catalog update and the logic necessary
//! for converting between representations.
//!
//! The general lifecycle of a single update when read from persist is as follows:
//!
//!   1. The update is stored in persist as a [`PersistStateUpdate`].
//!   2. After being read from persist the update is immediately converted into a
//!      [`StateUpdate<StateUpdateKindJson>`], which models the update as a JSON.
//!   3. The [`StateUpdateKindJson`] is converted into a protobuf message,
//!      [`proto::StateUpdateKind`].
//!   4. The update is then converted into a [`StateUpdate<StateUpdateKind>`], which is a strongly
//!      typed Rust object.
//!   5. Finally, the update is converted into an [`Option<memory::objects::StateUpdate>`], and
//!      `Some` variants are given to the in-memory catalog. The in-memory catalog is only
//!      interested in a subset of catalog updates which is why the [`Option`] is necessary.
//!
//! TLDR: [`PersistStateUpdate`] -> [`StateUpdate<StateUpdateKindJson>`] ->
//!       [`proto::StateUpdateKind`] -> [`StateUpdate<StateUpdateKind>`] ->
//!       [`Option<memory::objects::StateUpdate>`]
//!
//! The process of writing a catalog update to persist is the exact opposite.
//!
//! When running catalog protobuf upgrades/migrations we may need to take a detour and convert the
//! [`StateUpdateKindJson`] to some `proto::object_v{x}::StateUpdateKind` before applying specific
//! upgrades to get us to a valid [`proto::StateUpdateKind`].

use std::fmt::Debug;
use std::sync::LazyLock;

use mz_ore::collections::HashSet;
use mz_proto::{ProtoType, RustType, TryFromProtoError};
use mz_repr::adt::jsonb::Jsonb;
use mz_repr::adt::numeric::{Dec, Numeric};
use mz_repr::Diff;
use mz_storage_types::sources::SourceData;
use proptest_derive::Arbitrary;
use tracing::error;

use crate::durable::debug::CollectionType;
use crate::durable::objects::serialization::proto;
use crate::durable::objects::{DurableType, FenceToken};
use crate::durable::persist::Timestamp;
use crate::durable::transaction::TransactionBatch;
use crate::durable::{DurableCatalogError, Epoch};
use crate::memory;

/// Trait for objects that can be converted to/from a [`StateUpdateKindJson`].
pub trait IntoStateUpdateKindJson:
    Into<StateUpdateKindJson> + PartialEq + Eq + PartialOrd + Ord + Debug + Clone
{
    type Error: Debug;

    fn try_from(raw: StateUpdateKindJson) -> Result<Self, Self::Error>;
}
impl<
        T: Into<StateUpdateKindJson>
            + TryFrom<StateUpdateKindJson>
            + PartialEq
            + Eq
            + PartialOrd
            + Ord
            + Debug
            + Clone,
    > IntoStateUpdateKindJson for T
where
    T::Error: Debug,
{
    type Error = T::Error;

    fn try_from(raw: StateUpdateKindJson) -> Result<Self, Self::Error> {
        <T as TryFrom<StateUpdateKindJson>>::try_from(raw)
    }
}

/// Trait for objects that can be converted to/from a [`StateUpdateKind`].
pub(crate) trait TryIntoStateUpdateKind: IntoStateUpdateKindJson {
    type Error: Debug;

    fn try_into(self) -> Result<StateUpdateKind, <Self as TryIntoStateUpdateKind>::Error>;
}
impl<T: IntoStateUpdateKindJson + TryInto<StateUpdateKind>> TryIntoStateUpdateKind for T
where
    <T as TryInto<StateUpdateKind>>::Error: Debug,
{
    type Error = <T as TryInto<StateUpdateKind>>::Error;

    fn try_into(self) -> Result<StateUpdateKind, <T as TryInto<StateUpdateKind>>::Error> {
        <T as TryInto<StateUpdateKind>>::try_into(self)
    }
}

/// A single update to the catalog state.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct StateUpdate<T: IntoStateUpdateKindJson = StateUpdateKind> {
    /// They kind and contents of the state update.
    pub kind: T,
    /// The timestamp at which the update occurred.
    pub ts: Timestamp,
    /// Record count difference for the update.
    pub diff: Diff,
}

impl StateUpdate {
    /// Convert a [`TransactionBatch`] to a list of [`StateUpdate`]s at timestamp `ts`.
    pub(crate) fn from_txn_batch_ts(
        txn_batch: TransactionBatch,
        ts: Timestamp,
    ) -> impl Iterator<Item = StateUpdate> {
        Self::from_txn_batch(txn_batch).map(move |(kind, diff)| StateUpdate { kind, ts, diff })
    }

    /// Convert a [`TransactionBatch`] to a list of [`StateUpdate`]s and [`Diff`]s.
    pub(crate) fn from_txn_batch(
        txn_batch: TransactionBatch,
    ) -> impl Iterator<Item = (StateUpdateKind, Diff)> {
        fn from_batch<K, V>(
            batch: Vec<(K, V, Diff)>,
            kind: fn(K, V) -> StateUpdateKind,
        ) -> impl Iterator<Item = (StateUpdateKind, Diff)> {
            batch
                .into_iter()
                .map(move |(k, v, diff)| (kind(k, v), diff))
        }
        let TransactionBatch {
            databases,
            schemas,
            items,
            comments,
            roles,
            clusters,
            cluster_replicas,
            network_policies,
            introspection_sources,
            id_allocator,
            configs,
            settings,
            source_references,
            system_gid_mapping,
            system_configurations,
            default_privileges,
            system_privileges,
            storage_collection_metadata,
            unfinalized_shards,
            txn_wal_shard,
            audit_log_updates,
            upper: _,
        } = txn_batch;
        let databases = from_batch(databases, StateUpdateKind::Database);
        let schemas = from_batch(schemas, StateUpdateKind::Schema);
        let items = from_batch(items, StateUpdateKind::Item);
        let comments = from_batch(comments, StateUpdateKind::Comment);
        let roles = from_batch(roles, StateUpdateKind::Role);
        let clusters = from_batch(clusters, StateUpdateKind::Cluster);
        let cluster_replicas = from_batch(cluster_replicas, StateUpdateKind::ClusterReplica);
        let network_policies = from_batch(network_policies, StateUpdateKind::NetworkPolicy);
        let introspection_sources = from_batch(
            introspection_sources,
            StateUpdateKind::IntrospectionSourceIndex,
        );
        let id_allocators = from_batch(id_allocator, StateUpdateKind::IdAllocator);
        let configs = from_batch(configs, StateUpdateKind::Config);
        let settings = from_batch(settings, StateUpdateKind::Setting);
        let system_object_mappings =
            from_batch(system_gid_mapping, StateUpdateKind::SystemObjectMapping);
        let system_configurations =
            from_batch(system_configurations, StateUpdateKind::SystemConfiguration);
        let default_privileges = from_batch(default_privileges, StateUpdateKind::DefaultPrivilege);
        let source_references = from_batch(source_references, StateUpdateKind::SourceReferences);
        let system_privileges = from_batch(system_privileges, StateUpdateKind::SystemPrivilege);
        let storage_collection_metadata = from_batch(
            storage_collection_metadata,
            StateUpdateKind::StorageCollectionMetadata,
        );
        let unfinalized_shards = from_batch(unfinalized_shards, StateUpdateKind::UnfinalizedShard);
        let txn_wal_shard = from_batch(txn_wal_shard, StateUpdateKind::TxnWalShard);
        let audit_logs = from_batch(audit_log_updates, StateUpdateKind::AuditLog);

        databases
            .chain(schemas)
            .chain(items)
            .chain(comments)
            .chain(roles)
            .chain(clusters)
            .chain(cluster_replicas)
            .chain(network_policies)
            .chain(introspection_sources)
            .chain(id_allocators)
            .chain(configs)
            .chain(settings)
            .chain(source_references)
            .chain(system_object_mappings)
            .chain(system_configurations)
            .chain(default_privileges)
            .chain(system_privileges)
            .chain(storage_collection_metadata)
            .chain(unfinalized_shards)
            .chain(txn_wal_shard)
            .chain(audit_logs)
    }
}

/// The contents of a single state update.
///
/// The entire catalog is serialized as bytes and saved in a single persist shard. We use this
/// enum to determine what collection something in the catalog belongs to.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Arbitrary)]
pub enum StateUpdateKind {
    AuditLog(proto::AuditLogKey, ()),
    Cluster(proto::ClusterKey, proto::ClusterValue),
    ClusterReplica(proto::ClusterReplicaKey, proto::ClusterReplicaValue),
    Comment(proto::CommentKey, proto::CommentValue),
    Config(proto::ConfigKey, proto::ConfigValue),
    Database(proto::DatabaseKey, proto::DatabaseValue),
    DefaultPrivilege(proto::DefaultPrivilegesKey, proto::DefaultPrivilegesValue),
    FenceToken(FenceToken),
    IdAllocator(proto::IdAllocKey, proto::IdAllocValue),
    IntrospectionSourceIndex(
        proto::ClusterIntrospectionSourceIndexKey,
        proto::ClusterIntrospectionSourceIndexValue,
    ),
    Item(proto::ItemKey, proto::ItemValue),
    NetworkPolicy(proto::NetworkPolicyKey, proto::NetworkPolicyValue),
    Role(proto::RoleKey, proto::RoleValue),
    Schema(proto::SchemaKey, proto::SchemaValue),
    Setting(proto::SettingKey, proto::SettingValue),
    SourceReferences(proto::SourceReferencesKey, proto::SourceReferencesValue),
    SystemConfiguration(
        proto::ServerConfigurationKey,
        proto::ServerConfigurationValue,
    ),
    SystemObjectMapping(proto::GidMappingKey, proto::GidMappingValue),
    SystemPrivilege(proto::SystemPrivilegesKey, proto::SystemPrivilegesValue),
    StorageCollectionMetadata(
        proto::StorageCollectionMetadataKey,
        proto::StorageCollectionMetadataValue,
    ),
    UnfinalizedShard(proto::UnfinalizedShardKey, ()),
    TxnWalShard((), proto::TxnWalShardValue),
}

impl StateUpdateKind {
    pub(crate) fn collection_type(&self) -> Option<CollectionType> {
        match self {
            StateUpdateKind::AuditLog(_, _) => Some(CollectionType::AuditLog),
            StateUpdateKind::Cluster(_, _) => Some(CollectionType::ComputeInstance),
            StateUpdateKind::ClusterReplica(_, _) => Some(CollectionType::ComputeReplicas),
            StateUpdateKind::Comment(_, _) => Some(CollectionType::Comments),
            StateUpdateKind::Config(_, _) => Some(CollectionType::Config),
            StateUpdateKind::Database(_, _) => Some(CollectionType::Database),
            StateUpdateKind::DefaultPrivilege(_, _) => Some(CollectionType::DefaultPrivileges),
            StateUpdateKind::FenceToken(_) => None,
            StateUpdateKind::IdAllocator(_, _) => Some(CollectionType::IdAlloc),
            StateUpdateKind::IntrospectionSourceIndex(_, _) => {
                Some(CollectionType::ComputeIntrospectionSourceIndex)
            }
            StateUpdateKind::Item(_, _) => Some(CollectionType::Item),
            StateUpdateKind::NetworkPolicy(_, _) => Some(CollectionType::NetworkPolicy),
            StateUpdateKind::Role(_, _) => Some(CollectionType::Role),
            StateUpdateKind::Schema(_, _) => Some(CollectionType::Schema),
            StateUpdateKind::Setting(_, _) => Some(CollectionType::Setting),
            StateUpdateKind::SourceReferences(_, _) => Some(CollectionType::SourceReferences),
            StateUpdateKind::SystemConfiguration(_, _) => Some(CollectionType::SystemConfiguration),
            StateUpdateKind::SystemObjectMapping(_, _) => Some(CollectionType::SystemGidMapping),
            StateUpdateKind::SystemPrivilege(_, _) => Some(CollectionType::SystemPrivileges),
            StateUpdateKind::StorageCollectionMetadata(_, _) => {
                Some(CollectionType::StorageCollectionMetadata)
            }
            StateUpdateKind::UnfinalizedShard(_, _) => Some(CollectionType::UnfinalizedShard),
            StateUpdateKind::TxnWalShard(_, _) => Some(CollectionType::TxnWalShard),
        }
    }
}

/// Version of [`StateUpdateKind`] to allow reading/writing raw json from/to persist.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct StateUpdateKindJson(Jsonb);

impl StateUpdateKindJson {
    pub(crate) fn from_serde<S: serde::Serialize>(s: S) -> Self {
        let serde_value = serde_json::to_value(s).expect("valid json");
        let row = Jsonb::from_serde_json(serde_value).expect("valid json");
        StateUpdateKindJson(row)
    }

    pub(crate) fn to_serde<D: serde::de::DeserializeOwned>(&self) -> D {
        self.try_to_serde().expect("jsonb should roundtrip")
    }

    pub(crate) fn try_to_serde<D: serde::de::DeserializeOwned>(
        &self,
    ) -> Result<D, serde_json::error::Error> {
        let serde_value = self.0.as_ref().to_serde_json();
        serde_json::from_value::<D>(serde_value)
    }

    fn kind(&self) -> &str {
        let row = self.0.row();
        let mut iter = row.unpack_first().unwrap_map().iter();
        let datum = iter
            .find_map(|(field, datum)| if field == "kind" { Some(datum) } else { None })
            .expect("kind field must exist");
        datum.unwrap_str()
    }

    pub(crate) fn audit_log_id(&self) -> u64 {
        assert!(self.is_audit_log(), "unexpected update kind: {self:?}");
        let row = self.0.row();
        let mut iter = row.unpack_first().unwrap_map().iter();
        let key = iter
            .find_map(|(field, datum)| if field == "key" { Some(datum) } else { None })
            .expect("key field must exist")
            .unwrap_map();
        let event = key
            .iter()
            .find_map(|(field, datum)| if field == "event" { Some(datum) } else { None })
            .expect("event field must exist")
            .unwrap_map();
        let (event_version, versioned_datum) = event.iter().next().expect("event cannot be empty");
        match event_version {
            "V1" => {
                let versioned_map = versioned_datum.unwrap_map();
                let id = versioned_map
                    .iter()
                    .find_map(|(field, datum)| if field == "id" { Some(datum) } else { None })
                    .expect("event field must exist")
                    .unwrap_numeric();
                let mut cx = Numeric::context();
                cx.try_into_u64(id.into_inner()).expect("invalid id")
            }
            version => unimplemented!("unsupported event version: {version}"),
        }
    }

    /// Returns true if this is an update kind that is always deserializable, even before migrations. Otherwise, returns false.
    pub(crate) fn is_always_deserializable(&self) -> bool {
        // Construct some fake update kinds so we can extract exactly what the kind field will
        // serialize as.
        static DESERIALIZABLE_KINDS: LazyLock<HashSet<String>> = LazyLock::new(|| {
            [
                StateUpdateKind::FenceToken(FenceToken {
                    deploy_generation: 1,
                    epoch: Epoch::new(1).expect("non-zero"),
                }),
                StateUpdateKind::Config(
                    proto::ConfigKey { key: String::new() },
                    proto::ConfigValue { value: 1 },
                ),
                StateUpdateKind::Setting(
                    proto::SettingKey {
                        name: String::new(),
                    },
                    proto::SettingValue {
                        value: String::new(),
                    },
                ),
                StateUpdateKind::AuditLog(proto::AuditLogKey { event: None }, ()),
            ]
            .into_iter()
            .map(|kind| {
                let json_kind: StateUpdateKindJson = kind.into();
                json_kind.kind().to_string()
            })
            .collect()
        });
        DESERIALIZABLE_KINDS.contains(self.kind())
    }

    /// Returns true if this is an audit log update. Otherwise, returns false.
    pub(crate) fn is_audit_log(&self) -> bool {
        // Construct a fake audit log so we can extract exactly what the kind field will serialize
        // as.
        static AUDIT_LOG_KIND: LazyLock<String> = LazyLock::new(|| {
            let audit_log = StateUpdateKind::AuditLog(proto::AuditLogKey { event: None }, ());
            let json_kind: StateUpdateKindJson = audit_log.into();
            json_kind.kind().to_string()
        });
        &*AUDIT_LOG_KIND == self.kind()
    }
}

/// Version of [`StateUpdateKind`] that is stored directly in persist.
type PersistStateUpdate = (
    (Result<SourceData, String>, Result<(), String>),
    Timestamp,
    i64,
);

impl TryFrom<&StateUpdate<StateUpdateKind>> for Option<memory::objects::StateUpdate> {
    type Error = DurableCatalogError;

    fn try_from(
        StateUpdate { kind, ts, diff }: &StateUpdate<StateUpdateKind>,
    ) -> Result<Self, Self::Error> {
        let kind: Option<memory::objects::StateUpdateKind> = TryInto::try_into(kind)?;
        let update = kind.map(|kind| memory::objects::StateUpdate {
            kind,
            ts: ts.clone(),
            diff: diff.clone().try_into().expect("invalid diff"),
        });
        Ok(update)
    }
}

impl TryFrom<&StateUpdateKind> for Option<memory::objects::StateUpdateKind> {
    type Error = DurableCatalogError;

    fn try_from(kind: &StateUpdateKind) -> Result<Self, Self::Error> {
        fn into_durable<PK, PV, T>(key: &PK, value: &PV) -> Result<T, DurableCatalogError>
        where
            PK: ProtoType<T::Key> + Clone,
            PV: ProtoType<T::Value> + Clone,
            T: DurableType,
        {
            let key = key.clone().into_rust()?;
            let value = value.clone().into_rust()?;
            Ok(T::from_key_value(key, value))
        }

        Ok(match kind {
            StateUpdateKind::AuditLog(key, value) => {
                let audit_log = into_durable(key, value)?;
                Some(memory::objects::StateUpdateKind::AuditLog(audit_log))
            }
            StateUpdateKind::Cluster(key, value) => {
                let cluster = into_durable(key, value)?;
                Some(memory::objects::StateUpdateKind::Cluster(cluster))
            }
            StateUpdateKind::ClusterReplica(key, value) => {
                let cluster_replica = into_durable(key, value)?;
                Some(memory::objects::StateUpdateKind::ClusterReplica(
                    cluster_replica,
                ))
            }
            StateUpdateKind::Comment(key, value) => {
                let comment = into_durable(key, value)?;
                Some(memory::objects::StateUpdateKind::Comment(comment))
            }
            StateUpdateKind::Database(key, value) => {
                let database = into_durable(key, value)?;
                Some(memory::objects::StateUpdateKind::Database(database))
            }
            StateUpdateKind::DefaultPrivilege(key, value) => {
                let default_privilege = into_durable(key, value)?;
                Some(memory::objects::StateUpdateKind::DefaultPrivilege(
                    default_privilege,
                ))
            }
            StateUpdateKind::Item(key, value) => {
                let item = into_durable(key, value)?;
                Some(memory::objects::StateUpdateKind::Item(item))
            }
            StateUpdateKind::IntrospectionSourceIndex(key, value) => {
                let introspection_source_index = into_durable(key, value)?;
                Some(memory::objects::StateUpdateKind::IntrospectionSourceIndex(
                    introspection_source_index,
                ))
            }
            StateUpdateKind::NetworkPolicy(key, value) => {
                let policy = into_durable(key, value)?;
                Some(memory::objects::StateUpdateKind::NetworkPolicy(policy))
            }
            StateUpdateKind::Role(key, value) => {
                let role = into_durable(key, value)?;
                Some(memory::objects::StateUpdateKind::Role(role))
            }
            StateUpdateKind::Schema(key, value) => {
                let schema = into_durable(key, value)?;
                Some(memory::objects::StateUpdateKind::Schema(schema))
            }
            StateUpdateKind::SourceReferences(key, value) => {
                let source_references = into_durable(key, value)?;
                Some(memory::objects::StateUpdateKind::SourceReferences(
                    source_references,
                ))
            }
            StateUpdateKind::StorageCollectionMetadata(key, value) => {
                let storage_collection_metadata = into_durable(key, value)?;
                Some(memory::objects::StateUpdateKind::StorageCollectionMetadata(
                    storage_collection_metadata,
                ))
            }
            StateUpdateKind::SystemConfiguration(key, value) => {
                let system_configuration = into_durable(key, value)?;
                Some(memory::objects::StateUpdateKind::SystemConfiguration(
                    system_configuration,
                ))
            }
            StateUpdateKind::SystemObjectMapping(key, value) => {
                let system_object_mapping = into_durable(key, value)?;
                Some(memory::objects::StateUpdateKind::SystemObjectMapping(
                    system_object_mapping,
                ))
            }
            StateUpdateKind::SystemPrivilege(key, value) => {
                let system_privilege = into_durable(key, value)?;
                Some(memory::objects::StateUpdateKind::SystemPrivilege(
                    system_privilege,
                ))
            }
            StateUpdateKind::UnfinalizedShard(key, value) => {
                let unfinalized_shard = into_durable(key, value)?;
                Some(memory::objects::StateUpdateKind::UnfinalizedShard(
                    unfinalized_shard,
                ))
            }
            // Not exposed to higher layers.
            StateUpdateKind::Config(_, _)
            | StateUpdateKind::FenceToken(_)
            | StateUpdateKind::IdAllocator(_, _)
            | StateUpdateKind::Setting(_, _)
            | StateUpdateKind::TxnWalShard(_, _) => None,
        })
    }
}

impl TryFrom<StateUpdate<StateUpdateKindJson>> for StateUpdate<StateUpdateKind> {
    type Error = String;

    fn try_from(update: StateUpdate<StateUpdateKindJson>) -> Result<Self, Self::Error> {
        Ok(StateUpdate {
            kind: TryInto::try_into(update.kind)?,
            ts: update.ts,
            diff: update.diff,
        })
    }
}

impl TryFrom<StateUpdateKindJson> for StateUpdateKind {
    type Error = String;

    fn try_from(value: StateUpdateKindJson) -> Result<Self, Self::Error> {
        let kind: proto::state_update_kind::Kind =
            value.try_to_serde().map_err(|err| err.to_string())?;
        let kind = proto::StateUpdateKind { kind: Some(kind) };
        StateUpdateKind::from_proto(kind).map_err(|err| err.to_string())
    }
}

impl TryFrom<&StateUpdateKindJson> for StateUpdateKind {
    type Error = String;

    fn try_from(value: &StateUpdateKindJson) -> Result<Self, Self::Error> {
        let kind: proto::state_update_kind::Kind =
            value.try_to_serde().map_err(|err| err.to_string())?;
        let kind = proto::StateUpdateKind { kind: Some(kind) };
        StateUpdateKind::from_proto(kind).map_err(|err| err.to_string())
    }
}

impl From<StateUpdateKind> for StateUpdateKindJson {
    fn from(value: StateUpdateKind) -> Self {
        let kind = value.into_proto_owned();
        let kind = kind.kind.expect("kind should be set");
        StateUpdateKindJson::from_serde(kind)
    }
}

// Be very careful about changing these implementations. The default impl of `into_proto_owned`
// calls `into_proto`, and this impl of `into_proto` calls `into_proto_owned`. It would be very
// easy to accidentally cause infinite recursion.
impl RustType<proto::StateUpdateKind> for StateUpdateKind {
    fn into_proto(&self) -> proto::StateUpdateKind {
        error!("unexpected clone of catalog data");
        self.clone().into_proto_owned()
    }

    fn into_proto_owned(self) -> proto::StateUpdateKind {
        proto::StateUpdateKind {
            kind: Some(match self {
                StateUpdateKind::AuditLog(key, _value) => {
                    proto::state_update_kind::Kind::AuditLog(proto::state_update_kind::AuditLog {
                        key: Some(key),
                    })
                }
                StateUpdateKind::Cluster(key, value) => {
                    proto::state_update_kind::Kind::Cluster(proto::state_update_kind::Cluster {
                        key: Some(key),
                        value: Some(value),
                    })
                }
                StateUpdateKind::ClusterReplica(key, value) => {
                    proto::state_update_kind::Kind::ClusterReplica(
                        proto::state_update_kind::ClusterReplica {
                            key: Some(key),
                            value: Some(value),
                        },
                    )
                }
                StateUpdateKind::Comment(key, value) => {
                    proto::state_update_kind::Kind::Comment(proto::state_update_kind::Comment {
                        key: Some(key),
                        value: Some(value),
                    })
                }
                StateUpdateKind::Config(key, value) => {
                    proto::state_update_kind::Kind::Config(proto::state_update_kind::Config {
                        key: Some(key),
                        value: Some(value),
                    })
                }
                StateUpdateKind::Database(key, value) => {
                    proto::state_update_kind::Kind::Database(proto::state_update_kind::Database {
                        key: Some(key),
                        value: Some(value),
                    })
                }
                StateUpdateKind::DefaultPrivilege(key, value) => {
                    proto::state_update_kind::Kind::DefaultPrivileges(
                        proto::state_update_kind::DefaultPrivileges {
                            key: Some(key),
                            value: Some(value),
                        },
                    )
                }
                StateUpdateKind::FenceToken(fence_token) => {
                    proto::state_update_kind::Kind::FenceToken(
                        proto::state_update_kind::FenceToken {
                            deploy_generation: fence_token.deploy_generation,
                            epoch: fence_token.epoch.get(),
                        },
                    )
                }
                StateUpdateKind::IdAllocator(key, value) => {
                    proto::state_update_kind::Kind::IdAlloc(proto::state_update_kind::IdAlloc {
                        key: Some(key),
                        value: Some(value),
                    })
                }
                StateUpdateKind::IntrospectionSourceIndex(key, value) => {
                    proto::state_update_kind::Kind::ClusterIntrospectionSourceIndex(
                        proto::state_update_kind::ClusterIntrospectionSourceIndex {
                            key: Some(key),
                            value: Some(value),
                        },
                    )
                }
                StateUpdateKind::Item(key, value) => {
                    proto::state_update_kind::Kind::Item(proto::state_update_kind::Item {
                        key: Some(key),
                        value: Some(value),
                    })
                }
                StateUpdateKind::NetworkPolicy(key, value) => {
                    proto::state_update_kind::Kind::NetworkPolicy(
                        proto::state_update_kind::NetworkPolicy {
                            key: Some(key),
                            value: Some(value),
                        },
                    )
                }
                StateUpdateKind::Role(key, value) => {
                    proto::state_update_kind::Kind::Role(proto::state_update_kind::Role {
                        key: Some(key),
                        value: Some(value),
                    })
                }
                StateUpdateKind::Schema(key, value) => {
                    proto::state_update_kind::Kind::Schema(proto::state_update_kind::Schema {
                        key: Some(key),
                        value: Some(value),
                    })
                }
                StateUpdateKind::Setting(key, value) => {
                    proto::state_update_kind::Kind::Setting(proto::state_update_kind::Setting {
                        key: Some(key),
                        value: Some(value),
                    })
                }
                StateUpdateKind::SourceReferences(key, value) => {
                    proto::state_update_kind::Kind::SourceReferences(
                        proto::state_update_kind::SourceReferences {
                            key: Some(key),
                            value: Some(value),
                        },
                    )
                }
                StateUpdateKind::SystemConfiguration(key, value) => {
                    proto::state_update_kind::Kind::ServerConfiguration(
                        proto::state_update_kind::ServerConfiguration {
                            key: Some(key),
                            value: Some(value),
                        },
                    )
                }
                StateUpdateKind::SystemObjectMapping(key, value) => {
                    proto::state_update_kind::Kind::GidMapping(
                        proto::state_update_kind::GidMapping {
                            key: Some(key),
                            value: Some(value),
                        },
                    )
                }
                StateUpdateKind::SystemPrivilege(key, value) => {
                    proto::state_update_kind::Kind::SystemPrivileges(
                        proto::state_update_kind::SystemPrivileges {
                            key: Some(key),
                            value: Some(value),
                        },
                    )
                }
                StateUpdateKind::StorageCollectionMetadata(key, value) => {
                    proto::state_update_kind::Kind::StorageCollectionMetadata(
                        proto::state_update_kind::StorageCollectionMetadata {
                            key: Some(key),
                            value: Some(value),
                        },
                    )
                }
                StateUpdateKind::UnfinalizedShard(key, ()) => {
                    proto::state_update_kind::Kind::UnfinalizedShard(
                        proto::state_update_kind::UnfinalizedShard { key: Some(key) },
                    )
                }
                StateUpdateKind::TxnWalShard((), value) => {
                    proto::state_update_kind::Kind::TxnWalShard(
                        proto::state_update_kind::TxnWalShard { value: Some(value) },
                    )
                }
            }),
        }
    }

    fn from_proto(proto: proto::StateUpdateKind) -> Result<StateUpdateKind, TryFromProtoError> {
        Ok(
            match proto
                .kind
                .ok_or_else(|| TryFromProtoError::missing_field("StateUpdateKind::kind"))?
            {
                proto::state_update_kind::Kind::AuditLog(proto::state_update_kind::AuditLog {
                    key,
                }) => StateUpdateKind::AuditLog(
                    key.ok_or_else(|| {
                        TryFromProtoError::missing_field("state_update_kind::AuditLog::key")
                    })?,
                    (),
                ),
                proto::state_update_kind::Kind::Cluster(proto::state_update_kind::Cluster {
                    key,
                    value,
                }) => StateUpdateKind::Cluster(
                    key.ok_or_else(|| {
                        TryFromProtoError::missing_field("state_update_kind::Cluster::key")
                    })?,
                    value.ok_or_else(|| {
                        TryFromProtoError::missing_field("state_update_kind::Cluster::value")
                    })?,
                ),
                proto::state_update_kind::Kind::ClusterReplica(
                    proto::state_update_kind::ClusterReplica { key, value },
                ) => StateUpdateKind::ClusterReplica(
                    key.ok_or_else(|| {
                        TryFromProtoError::missing_field("state_update_kind::ClusterReplica::key")
                    })?,
                    value.ok_or_else(|| {
                        TryFromProtoError::missing_field("state_update_kind::ClusterReplica::value")
                    })?,
                ),
                proto::state_update_kind::Kind::Comment(proto::state_update_kind::Comment {
                    key,
                    value,
                }) => StateUpdateKind::Comment(
                    key.ok_or_else(|| {
                        TryFromProtoError::missing_field("state_update_kind::Comment::key")
                    })?,
                    value.ok_or_else(|| {
                        TryFromProtoError::missing_field("state_update_kind::Comment::value")
                    })?,
                ),
                proto::state_update_kind::Kind::Config(proto::state_update_kind::Config {
                    key,
                    value,
                }) => StateUpdateKind::Config(
                    key.ok_or_else(|| {
                        TryFromProtoError::missing_field("state_update_kind::Config::key")
                    })?,
                    value.ok_or_else(|| {
                        TryFromProtoError::missing_field("state_update_kind::Config::value")
                    })?,
                ),
                proto::state_update_kind::Kind::Database(proto::state_update_kind::Database {
                    key,
                    value,
                }) => StateUpdateKind::Database(
                    key.ok_or_else(|| {
                        TryFromProtoError::missing_field("state_update_kind::Database::key")
                    })?,
                    value.ok_or_else(|| {
                        TryFromProtoError::missing_field("state_update_kind::Database::value")
                    })?,
                ),
                proto::state_update_kind::Kind::DefaultPrivileges(
                    proto::state_update_kind::DefaultPrivileges { key, value },
                ) => StateUpdateKind::DefaultPrivilege(
                    key.ok_or_else(|| {
                        TryFromProtoError::missing_field(
                            "state_update_kind::DefaultPrivileges::key",
                        )
                    })?,
                    value.ok_or_else(|| {
                        TryFromProtoError::missing_field(
                            "state_update_kind::DefaultPrivileges::value",
                        )
                    })?,
                ),
                proto::state_update_kind::Kind::FenceToken(
                    proto::state_update_kind::FenceToken {
                        deploy_generation,
                        epoch,
                    },
                ) => StateUpdateKind::FenceToken(FenceToken {
                    deploy_generation,
                    epoch: Epoch::new(epoch).ok_or_else(|| {
                        TryFromProtoError::missing_field("state_update_kind::Epoch::epoch")
                    })?,
                }),
                proto::state_update_kind::Kind::IdAlloc(proto::state_update_kind::IdAlloc {
                    key,
                    value,
                }) => StateUpdateKind::IdAllocator(
                    key.ok_or_else(|| {
                        TryFromProtoError::missing_field("state_update_kind::IdAlloc::key")
                    })?,
                    value.ok_or_else(|| {
                        TryFromProtoError::missing_field("state_update_kind::IdAlloc::value")
                    })?,
                ),
                proto::state_update_kind::Kind::ClusterIntrospectionSourceIndex(
                    proto::state_update_kind::ClusterIntrospectionSourceIndex { key, value },
                ) => StateUpdateKind::IntrospectionSourceIndex(
                    key.ok_or_else(|| {
                        TryFromProtoError::missing_field(
                            "state_update_kind::ClusterIntrospectionSourceIndex::key",
                        )
                    })?,
                    value.ok_or_else(|| {
                        TryFromProtoError::missing_field(
                            "state_update_kind::ClusterIntrospectionSourceIndex::value",
                        )
                    })?,
                ),
                proto::state_update_kind::Kind::Item(proto::state_update_kind::Item {
                    key,
                    value,
                }) => StateUpdateKind::Item(
                    key.ok_or_else(|| {
                        TryFromProtoError::missing_field("state_update_kind::Item::key")
                    })?,
                    value.ok_or_else(|| {
                        TryFromProtoError::missing_field("state_update_kind::Item::value")
                    })?,
                ),
                proto::state_update_kind::Kind::Role(proto::state_update_kind::Role {
                    key,
                    value,
                }) => StateUpdateKind::Role(
                    key.ok_or_else(|| {
                        TryFromProtoError::missing_field("state_update_kind::Role::key")
                    })?,
                    value.ok_or_else(|| {
                        TryFromProtoError::missing_field("state_update_kind::Role::value")
                    })?,
                ),
                proto::state_update_kind::Kind::Schema(proto::state_update_kind::Schema {
                    key,
                    value,
                }) => StateUpdateKind::Schema(
                    key.ok_or_else(|| {
                        TryFromProtoError::missing_field("state_update_kind::Schema::key")
                    })?,
                    value.ok_or_else(|| {
                        TryFromProtoError::missing_field("state_update_kind::Schema::value")
                    })?,
                ),
                proto::state_update_kind::Kind::Setting(proto::state_update_kind::Setting {
                    key,
                    value,
                }) => StateUpdateKind::Setting(
                    key.ok_or_else(|| {
                        TryFromProtoError::missing_field("state_update_kind::Setting::key")
                    })?,
                    value.ok_or_else(|| {
                        TryFromProtoError::missing_field("state_update_kind::Setting::value")
                    })?,
                ),
                proto::state_update_kind::Kind::ServerConfiguration(
                    proto::state_update_kind::ServerConfiguration { key, value },
                ) => StateUpdateKind::SystemConfiguration(
                    key.ok_or_else(|| {
                        TryFromProtoError::missing_field(
                            "state_update_kind::ServerConfiguration::key",
                        )
                    })?,
                    value.ok_or_else(|| {
                        TryFromProtoError::missing_field(
                            "state_update_kind::ServerConfiguration::value",
                        )
                    })?,
                ),
                proto::state_update_kind::Kind::GidMapping(
                    proto::state_update_kind::GidMapping { key, value },
                ) => StateUpdateKind::SystemObjectMapping(
                    key.ok_or_else(|| {
                        TryFromProtoError::missing_field("state_update_kind::GidMapping::key")
                    })?,
                    value.ok_or_else(|| {
                        TryFromProtoError::missing_field("state_update_kind::GidMapping::value")
                    })?,
                ),
                proto::state_update_kind::Kind::SystemPrivileges(
                    proto::state_update_kind::SystemPrivileges { key, value },
                ) => StateUpdateKind::SystemPrivilege(
                    key.ok_or_else(|| {
                        TryFromProtoError::missing_field("state_update_kind::SystemPrivileges::key")
                    })?,
                    value.ok_or_else(|| {
                        TryFromProtoError::missing_field(
                            "state_update_kind::SystemPrivileges::value",
                        )
                    })?,
                ),
                proto::state_update_kind::Kind::StorageCollectionMetadata(
                    proto::state_update_kind::StorageCollectionMetadata { key, value },
                ) => StateUpdateKind::StorageCollectionMetadata(
                    key.ok_or_else(|| {
                        TryFromProtoError::missing_field(
                            "state_update_kind::StorageCollectionMetadata::key",
                        )
                    })?,
                    value.ok_or_else(|| {
                        TryFromProtoError::missing_field(
                            "state_update_kind::StorageCollectionMetadata::value",
                        )
                    })?,
                ),
                proto::state_update_kind::Kind::UnfinalizedShard(
                    proto::state_update_kind::UnfinalizedShard { key },
                ) => StateUpdateKind::UnfinalizedShard(
                    key.ok_or_else(|| {
                        TryFromProtoError::missing_field(
                            "state_update_kind::StorageCollectionMetadata::key",
                        )
                    })?,
                    (),
                ),
                proto::state_update_kind::Kind::TxnWalShard(
                    proto::state_update_kind::TxnWalShard { value },
                ) => StateUpdateKind::TxnWalShard(
                    (),
                    value.ok_or_else(|| {
                        TryFromProtoError::missing_field("state_update_kind::TxnWalShard::value")
                    })?,
                ),
                proto::state_update_kind::Kind::SourceReferences(
                    proto::state_update_kind::SourceReferences { key, value },
                ) => StateUpdateKind::SourceReferences(
                    key.ok_or_else(|| {
                        TryFromProtoError::missing_field("state_update_kind::SourceReferences::key")
                    })?,
                    value.ok_or_else(|| {
                        TryFromProtoError::missing_field(
                            "state_update_kind::SourceReferences::value",
                        )
                    })?,
                ),
                proto::state_update_kind::Kind::NetworkPolicy(
                    proto::state_update_kind::NetworkPolicy { key, value },
                ) => StateUpdateKind::NetworkPolicy(
                    key.ok_or_else(|| {
                        TryFromProtoError::missing_field("state_update_kind::NetworkPolicy::key")
                    })?,
                    value.ok_or_else(|| {
                        TryFromProtoError::missing_field("state_update_kind::NetworkPolicy::value")
                    })?,
                ),
            },
        )
    }
}

/// Decodes a [`StateUpdate<StateUpdateKindJson>`] from the `(key, value, ts,
/// diff)` tuple/update we store in persist.
impl From<PersistStateUpdate> for StateUpdate<StateUpdateKindJson> {
    fn from(kvtd: PersistStateUpdate) -> Self {
        let ((key, val), ts, diff) = kvtd;
        let (key, ()) = (
            key.expect("persist decoding error"),
            val.expect("persist decoding error"),
        );
        StateUpdate {
            kind: StateUpdateKindJson::from(key),
            ts,
            diff,
        }
    }
}

impl From<StateUpdateKindJson> for SourceData {
    fn from(value: StateUpdateKindJson) -> SourceData {
        let row = value.0.into_row();
        SourceData(Ok(row))
    }
}

impl From<SourceData> for StateUpdateKindJson {
    fn from(value: SourceData) -> Self {
        let row = value.0.expect("only Ok values stored in catalog shard");
        StateUpdateKindJson(Jsonb::from_row(row))
    }
}

#[cfg(test)]
mod tests {
    use mz_persist_types::Codec;
    use mz_repr::{RelationDesc, ScalarType};
    use mz_storage_types::sources::SourceData;
    use proptest::prelude::*;

    use crate::durable::objects::serialization::proto;
    use crate::durable::objects::state_update::{StateUpdateKind, StateUpdateKindJson};
    use crate::durable::objects::FenceToken;
    use crate::durable::Epoch;

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn kind_test() {
        let test_cases = [
            (
                StateUpdateKind::FenceToken(FenceToken {
                    deploy_generation: 1,
                    epoch: Epoch::new(1).expect("non-zero"),
                }),
                "FenceToken",
            ),
            (
                StateUpdateKind::Config(
                    proto::ConfigKey { key: String::new() },
                    proto::ConfigValue { value: 1 },
                ),
                "Config",
            ),
            (
                StateUpdateKind::Setting(
                    proto::SettingKey {
                        name: String::new(),
                    },
                    proto::SettingValue {
                        value: String::new(),
                    },
                ),
                "Setting",
            ),
            (
                StateUpdateKind::AuditLog(proto::AuditLogKey { event: None }, ()),
                "AuditLog",
            ),
        ];

        for (kind, expected) in test_cases {
            let json_kind: StateUpdateKindJson = kind.into();
            let kind = json_kind.kind().to_string();
            assert_eq!(expected, kind);
        }
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn audit_log_id_test() {
        let test_cases = [
            (
                StateUpdateKind::AuditLog(
                    proto::AuditLogKey {
                        event: Some(proto::audit_log_key::Event::V1(proto::AuditLogEventV1 {
                            id: 1,
                            event_type: 2,
                            object_type: 3,
                            user: None,
                            occurred_at: None,
                            details: None,
                        })),
                    },
                    (),
                ),
                1,
            ),
            (
                StateUpdateKind::AuditLog(
                    proto::AuditLogKey {
                        event: Some(proto::audit_log_key::Event::V1(proto::AuditLogEventV1 {
                            id: 4,
                            event_type: 5,
                            object_type: 6,
                            user: None,
                            occurred_at: None,
                            details: None,
                        })),
                    },
                    (),
                ),
                4,
            ),
        ];

        for (kind, expected) in test_cases {
            let json_kind: StateUpdateKindJson = kind.into();
            let id = json_kind.audit_log_id();
            assert_eq!(expected, id);
        }
    }

    proptest! {
        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // slow
        fn proptest_state_update_kind_roundtrip(kind: StateUpdateKind) {
            // Verify that we can map encode into the "raw" json format. This
            // validates things like contained integers fitting in f64.
            let raw = StateUpdateKindJson::from(kind.clone());
            let desc = RelationDesc::builder().with_column("a", ScalarType::Jsonb.nullable(false)).finish();

            // Verify that the raw roundtrips through the SourceData Codec impl.
            let source_data = SourceData::from(raw.clone());
            let mut encoded = Vec::new();
            source_data.encode(&mut encoded);
            let decoded = SourceData::decode(&encoded, &desc).expect("should be valid SourceData");
            prop_assert_eq!(&source_data, &decoded);
            let decoded = StateUpdateKindJson::from(decoded);
            prop_assert_eq!(&raw, &decoded);

            // Verify that the enum roundtrips.
            let decoded = StateUpdateKind::try_from(decoded).expect("should be valid StateUpdateKind");
            prop_assert_eq!(&kind, &decoded);
        }
    }
}
