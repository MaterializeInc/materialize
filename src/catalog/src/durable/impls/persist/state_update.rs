// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Debug;

use mz_persist_types::codec_impls::{SimpleDecoder, SimpleEncoder, SimpleSchema};
use mz_persist_types::dyn_struct::{ColumnsMut, ColumnsRef, DynStructCfg};
use mz_persist_types::Codec;
use mz_proto::{ProtoType, RustType, TryFromProtoError};
use mz_repr::Diff;
use proptest_derive::Arbitrary;
use prost::Message;

use crate::durable::impls::persist::Timestamp;
use crate::durable::objects::serialization::proto;
use crate::durable::transaction::TransactionBatch;
use crate::durable::Epoch;

/// Trait for objects that can be converted to/from a `StateUpdateKind` and can be used to
/// read/write to persist.
pub trait StateUpdateKindAlias:
    TryInto<StateUpdateKind>
    + From<StateUpdateKind>
    + PartialEq
    + Eq
    + PartialOrd
    + Ord
    + Debug
    + Clone
    + Codec
{
}
impl<
        T: TryInto<StateUpdateKind>
            + From<StateUpdateKind>
            + PartialEq
            + Eq
            + PartialOrd
            + Ord
            + Debug
            + Clone
            + Codec,
    > StateUpdateKindAlias for T
{
}

/// A single update to the catalog state.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct StateUpdate<T: StateUpdateKindAlias = StateUpdateKind> {
    /// They kind and contents of the state update.
    pub(super) kind: T,
    /// The timestamp at which the update occurred.
    pub(super) ts: Timestamp,
    /// Record count difference for the update.
    pub(super) diff: Diff,
}

impl StateUpdate {
    /// Convert a [`TransactionBatch`] to a list of [`StateUpdate`]s at timestamp `ts`.
    pub(super) fn from_txn_batch(txn_batch: TransactionBatch, ts: Timestamp) -> Vec<StateUpdate> {
        fn from_batch<K, V>(
            batch: Vec<(K, V, Diff)>,
            ts: Timestamp,
            kind: fn(K, V) -> StateUpdateKind,
        ) -> impl Iterator<Item = StateUpdate> {
            batch.into_iter().map(move |(k, v, diff)| StateUpdate {
                kind: kind(k, v),
                ts,
                diff,
            })
        }
        let TransactionBatch {
            databases,
            schemas,
            items,
            comments,
            roles,
            clusters,
            cluster_replicas,
            introspection_sources,
            id_allocator,
            configs,
            settings,
            timestamps,
            system_gid_mapping,
            system_configurations,
            default_privileges,
            system_privileges,
            audit_log_updates,
            storage_usage_updates,
            // Persist implementation does not use the connection timeout.
            connection_timeout: _,
        } = txn_batch;
        let databases = from_batch(databases, ts, StateUpdateKind::Database);
        let schemas = from_batch(schemas, ts, StateUpdateKind::Schema);
        let items = from_batch(items, ts, StateUpdateKind::Item);
        let comments = from_batch(comments, ts, StateUpdateKind::Comment);
        let roles = from_batch(roles, ts, StateUpdateKind::Role);
        let clusters = from_batch(clusters, ts, StateUpdateKind::Cluster);
        let cluster_replicas = from_batch(cluster_replicas, ts, StateUpdateKind::ClusterReplica);
        let introspection_sources = from_batch(
            introspection_sources,
            ts,
            StateUpdateKind::IntrospectionSourceIndex,
        );
        let id_allocators = from_batch(id_allocator, ts, StateUpdateKind::IdAllocator);
        let configs = from_batch(configs, ts, StateUpdateKind::Config);
        let settings = from_batch(settings, ts, StateUpdateKind::Setting);
        let timestamps = from_batch(timestamps, ts, StateUpdateKind::Timestamp);
        let system_object_mappings =
            from_batch(system_gid_mapping, ts, StateUpdateKind::SystemObjectMapping);
        let system_configurations = from_batch(
            system_configurations,
            ts,
            StateUpdateKind::SystemConfiguration,
        );
        let default_privileges =
            from_batch(default_privileges, ts, StateUpdateKind::DefaultPrivilege);
        let system_privileges = from_batch(system_privileges, ts, StateUpdateKind::SystemPrivilege);
        let audit_logs = from_batch(audit_log_updates, ts, StateUpdateKind::AuditLog);
        let storage_usage_updates =
            from_batch(storage_usage_updates, ts, StateUpdateKind::StorageUsage);

        databases
            .chain(schemas)
            .chain(items)
            .chain(comments)
            .chain(roles)
            .chain(clusters)
            .chain(cluster_replicas)
            .chain(introspection_sources)
            .chain(id_allocators)
            .chain(configs)
            .chain(settings)
            .chain(timestamps)
            .chain(system_object_mappings)
            .chain(system_configurations)
            .chain(default_privileges)
            .chain(system_privileges)
            .chain(audit_logs)
            .chain(storage_usage_updates)
            .collect()
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
    Epoch(Epoch),
    IdAllocator(proto::IdAllocKey, proto::IdAllocValue),
    IntrospectionSourceIndex(
        proto::ClusterIntrospectionSourceIndexKey,
        proto::ClusterIntrospectionSourceIndexValue,
    ),
    Item(proto::ItemKey, proto::ItemValue),
    Role(proto::RoleKey, proto::RoleValue),
    Schema(proto::SchemaKey, proto::SchemaValue),
    Setting(proto::SettingKey, proto::SettingValue),
    StorageUsage(proto::StorageUsageKey, ()),
    SystemConfiguration(
        proto::ServerConfigurationKey,
        proto::ServerConfigurationValue,
    ),
    SystemObjectMapping(proto::GidMappingKey, proto::GidMappingValue),
    SystemPrivilege(proto::SystemPrivilegesKey, proto::SystemPrivilegesValue),
    Timestamp(proto::TimestampKey, proto::TimestampValue),
}

impl RustType<proto::StateUpdateKind> for StateUpdateKind {
    fn into_proto(&self) -> proto::StateUpdateKind {
        proto::StateUpdateKind {
            kind: Some(match self {
                StateUpdateKind::AuditLog(key, _value) => {
                    proto::state_update_kind::Kind::AuditLog(proto::state_update_kind::AuditLog {
                        key: Some(key.clone()),
                    })
                }
                StateUpdateKind::Cluster(key, value) => {
                    proto::state_update_kind::Kind::Cluster(proto::state_update_kind::Cluster {
                        key: Some(key.clone()),
                        value: Some(value.clone()),
                    })
                }
                StateUpdateKind::ClusterReplica(key, value) => {
                    proto::state_update_kind::Kind::ClusterReplica(
                        proto::state_update_kind::ClusterReplica {
                            key: Some(key.clone()),
                            value: Some(value.clone()),
                        },
                    )
                }
                StateUpdateKind::Comment(key, value) => {
                    proto::state_update_kind::Kind::Comment(proto::state_update_kind::Comment {
                        key: Some(key.clone()),
                        value: Some(value.clone()),
                    })
                }
                StateUpdateKind::Config(key, value) => {
                    proto::state_update_kind::Kind::Config(proto::state_update_kind::Config {
                        key: Some(key.clone()),
                        value: Some(value.clone()),
                    })
                }
                StateUpdateKind::Database(key, value) => {
                    proto::state_update_kind::Kind::Database(proto::state_update_kind::Database {
                        key: Some(key.clone()),
                        value: Some(value.clone()),
                    })
                }
                StateUpdateKind::DefaultPrivilege(key, value) => {
                    proto::state_update_kind::Kind::DefaultPrivileges(
                        proto::state_update_kind::DefaultPrivileges {
                            key: Some(key.clone()),
                            value: Some(value.clone()),
                        },
                    )
                }
                StateUpdateKind::Epoch(epoch) => {
                    proto::state_update_kind::Kind::Epoch(proto::state_update_kind::Epoch {
                        epoch: epoch.get(),
                    })
                }
                StateUpdateKind::IdAllocator(key, value) => {
                    proto::state_update_kind::Kind::IdAlloc(proto::state_update_kind::IdAlloc {
                        key: Some(key.clone()),
                        value: Some(value.clone()),
                    })
                }
                StateUpdateKind::IntrospectionSourceIndex(key, value) => {
                    proto::state_update_kind::Kind::ClusterIntrospectionSourceIndex(
                        proto::state_update_kind::ClusterIntrospectionSourceIndex {
                            key: Some(key.clone()),
                            value: Some(value.clone()),
                        },
                    )
                }
                StateUpdateKind::Item(key, value) => {
                    proto::state_update_kind::Kind::Item(proto::state_update_kind::Item {
                        key: Some(key.clone()),
                        value: Some(value.clone()),
                    })
                }
                StateUpdateKind::Role(key, value) => {
                    proto::state_update_kind::Kind::Role(proto::state_update_kind::Role {
                        key: Some(key.clone()),
                        value: Some(value.clone()),
                    })
                }
                StateUpdateKind::Schema(key, value) => {
                    proto::state_update_kind::Kind::Schema(proto::state_update_kind::Schema {
                        key: Some(key.clone()),
                        value: Some(value.clone()),
                    })
                }
                StateUpdateKind::Setting(key, value) => {
                    proto::state_update_kind::Kind::Setting(proto::state_update_kind::Setting {
                        key: Some(key.clone()),
                        value: Some(value.clone()),
                    })
                }
                StateUpdateKind::StorageUsage(key, _value) => {
                    proto::state_update_kind::Kind::StorageUsage(
                        proto::state_update_kind::StorageUsage {
                            key: Some(key.clone()),
                        },
                    )
                }
                StateUpdateKind::SystemConfiguration(key, value) => {
                    proto::state_update_kind::Kind::ServerConfiguration(
                        proto::state_update_kind::ServerConfiguration {
                            key: Some(key.clone()),
                            value: Some(value.clone()),
                        },
                    )
                }
                StateUpdateKind::SystemObjectMapping(key, value) => {
                    proto::state_update_kind::Kind::GidMapping(
                        proto::state_update_kind::GidMapping {
                            key: Some(key.clone()),
                            value: Some(value.clone()),
                        },
                    )
                }
                StateUpdateKind::SystemPrivilege(key, value) => {
                    proto::state_update_kind::Kind::SystemPrivileges(
                        proto::state_update_kind::SystemPrivileges {
                            key: Some(key.clone()),
                            value: Some(value.clone()),
                        },
                    )
                }
                StateUpdateKind::Timestamp(key, value) => {
                    proto::state_update_kind::Kind::Timestamp(proto::state_update_kind::Timestamp {
                        key: Some(key.clone()),
                        value: Some(value.clone()),
                    })
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
                proto::state_update_kind::Kind::Epoch(proto::state_update_kind::Epoch {
                    epoch,
                }) => StateUpdateKind::Epoch(Epoch::new(epoch).ok_or_else(|| {
                    TryFromProtoError::missing_field("state_update_kind::Epoch::epoch")
                })?),
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
                proto::state_update_kind::Kind::StorageUsage(
                    proto::state_update_kind::StorageUsage { key },
                ) => StateUpdateKind::StorageUsage(
                    key.ok_or_else(|| {
                        TryFromProtoError::missing_field("state_update_kind::StorageUsage::key")
                    })?,
                    (),
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
                proto::state_update_kind::Kind::Timestamp(
                    proto::state_update_kind::Timestamp { key, value },
                ) => StateUpdateKind::Timestamp(
                    key.ok_or_else(|| {
                        TryFromProtoError::missing_field("state_update_kind::Timestamp::key")
                    })?,
                    value.ok_or_else(|| {
                        TryFromProtoError::missing_field("state_update_kind::Timestamp::value")
                    })?,
                ),
            },
        )
    }
}

#[derive(Debug, Clone, Default)]
pub struct StateUpdateKindSchema;

impl Codec for StateUpdateKind {
    type Schema = StateUpdateKindSchema;

    fn codec_name() -> String {
        "StateUpdateProto".to_string()
    }

    fn encode<B>(&self, buf: &mut B)
    where
        B: bytes::BufMut,
    {
        let bytes = self.into_proto().encode_to_vec();
        buf.put(bytes.as_slice());
    }

    fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
        let kind = proto::StateUpdateKind::decode(buf).map_err(|err| err.to_string())?;
        kind.into_rust().map_err(|err| err.to_string())
    }
}

impl mz_persist_types::columnar::Schema<StateUpdateKind> for StateUpdateKindSchema {
    type Encoder<'a> = SimpleEncoder<'a, StateUpdateKind, Vec<u8>>;

    type Decoder<'a> = SimpleDecoder<'a, StateUpdateKind, Vec<u8>>;

    fn columns(&self) -> DynStructCfg {
        SimpleSchema::<StateUpdateKind, Vec<u8>>::columns(&())
    }

    fn decoder<'a>(&self, cols: ColumnsRef<'a>) -> Result<Self::Decoder<'a>, String> {
        SimpleSchema::<StateUpdateKind, Vec<u8>>::decoder(cols, |val, ret| {
            *ret = StateUpdateKind::decode(val).expect("should be valid StateUpdateKind")
        })
    }

    fn encoder<'a>(&self, cols: ColumnsMut<'a>) -> Result<Self::Encoder<'a>, String> {
        SimpleSchema::<StateUpdateKind, Vec<u8>>::push_encoder(cols, |col, val| {
            let mut buf = Vec::new();
            StateUpdateKind::encode(val, &mut buf);
            mz_persist_types::columnar::ColumnPush::<Vec<u8>>::push(col, &buf)
        })
    }
}

/// Binary version of [`StateUpdateKind`] to allow reading/writing raw binary from/to persist.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Arbitrary)]
pub(crate) struct StateUpdateKindBinary(Vec<u8>);

impl From<StateUpdateKind> for StateUpdateKindBinary {
    fn from(value: StateUpdateKind) -> Self {
        Self(value.into_proto().encode_to_vec())
    }
}

impl TryFrom<StateUpdateKindBinary> for StateUpdateKind {
    type Error = String;

    fn try_from(value: StateUpdateKindBinary) -> Result<Self, Self::Error> {
        let kind =
            proto::StateUpdateKind::decode(value.0.as_slice()).map_err(|err| err.to_string())?;
        kind.into_rust().map_err(|err| err.to_string())
    }
}
impl Codec for StateUpdateKindBinary {
    type Schema = StateUpdateKindSchema;

    fn codec_name() -> String {
        "StateUpdateProto".to_string()
    }

    fn encode<B>(&self, buf: &mut B)
    where
        B: bytes::BufMut,
    {
        buf.put(self.0.as_slice());
    }

    fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
        Ok(Self(buf.to_vec()))
    }
}

impl mz_persist_types::columnar::Schema<StateUpdateKindBinary> for StateUpdateKindSchema {
    type Encoder<'a> = SimpleEncoder<'a, StateUpdateKindBinary, Vec<u8>>;

    type Decoder<'a> = SimpleDecoder<'a, StateUpdateKindBinary, Vec<u8>>;

    fn columns(&self) -> DynStructCfg {
        SimpleSchema::<StateUpdateKindBinary, Vec<u8>>::columns(&())
    }

    fn decoder<'a>(&self, cols: ColumnsRef<'a>) -> Result<Self::Decoder<'a>, String> {
        SimpleSchema::<StateUpdateKindBinary, Vec<u8>>::decoder(cols, |val, ret| {
            *ret =
                StateUpdateKindBinary::decode(val).expect("should be valid StateUpdateKindBinary")
        })
    }

    fn encoder<'a>(&self, cols: ColumnsMut<'a>) -> Result<Self::Encoder<'a>, String> {
        SimpleSchema::<StateUpdateKindBinary, Vec<u8>>::push_encoder(cols, |col, val| {
            let mut buf = Vec::new();
            StateUpdateKindBinary::encode(val, &mut buf);
            mz_persist_types::columnar::ColumnPush::<Vec<u8>>::push(col, &buf)
        })
    }
}

#[cfg(test)]
mod tests {
    use mz_persist_types::Codec;
    use proptest::prelude::*;

    use crate::durable::impls::persist::state_update::StateUpdateKindBinary;
    use crate::durable::impls::persist::StateUpdateKind;

    proptest! {
        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // slow
        fn proptest_state_update_kind_round_trip(kind: StateUpdateKind) {
            let mut binary = Vec::new();
            kind.encode(&mut binary);

            let decoded = StateUpdateKind::decode(&binary).expect("should be valid StateUpdateKind");

            prop_assert_eq!(kind, decoded);
        }

        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // slow
        fn proptest_state_update_kind_binary_round_trip(kind: StateUpdateKindBinary) {
            let mut binary = Vec::new();
            kind.encode(&mut binary);

            let decoded = StateUpdateKindBinary::decode(&binary).expect("should be valid StateUpdateKindBinary");

            prop_assert_eq!(kind, decoded);
        }

        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // slow
        fn proptest_state_update_kind_binary_equivalence(kind: StateUpdateKind) {
            let mut kind_encoded = Vec::new();
            kind.encode(&mut kind_encoded);

            let kind_binary: StateUpdateKindBinary = kind.into();
            let mut kind_binary_encoded = Vec::new();
            kind_binary.encode(&mut kind_binary_encoded);

            prop_assert_eq!(kind_encoded, kind_binary_encoded);
        }
    }
}
