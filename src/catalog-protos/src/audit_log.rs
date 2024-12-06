// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module is responsible for serializing objects from the
//! [`mz_audit_log`] crate into protobuf.
//!
//! The reason this module doesn't exist in the `mz_catalog` crate itself is
//! because of Rust's orphan rules.

use mz_audit_log::{
    AlterDefaultPrivilegeV1, AlterRetainHistoryV1, AlterSetClusterV1, AlterSourceSinkV1,
    CreateClusterReplicaV1, CreateClusterReplicaV2, CreateClusterReplicaV3, CreateIndexV1,
    CreateMaterializedViewV1, CreateOrDropClusterReplicaReasonV1, CreateSourceSinkV1,
    CreateSourceSinkV2, CreateSourceSinkV3, CreateSourceSinkV4, DropClusterReplicaV1,
    DropClusterReplicaV2, DropClusterReplicaV3, EventDetails, EventType, EventV1, FromPreviousIdV1,
    FullNameV1, GrantRoleV1, GrantRoleV2, IdFullNameV1, IdNameV1, RefreshDecisionWithReasonV1,
    RefreshDecisionWithReasonV2, RenameClusterReplicaV1, RenameClusterV1, RenameItemV1,
    RenameSchemaV1, RevokeRoleV1, RevokeRoleV2, RotateKeysV1, SchedulingDecisionV1,
    SchedulingDecisionsWithReasonsV1, SchedulingDecisionsWithReasonsV2, SchemaV1, SchemaV2, SetV1,
    ToNewIdV1, UpdateItemV1, UpdateOwnerV1, UpdatePrivilegeV1, VersionedEvent,
};
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};

use crate::objects::Empty;

impl RustType<crate::objects::audit_log_key::Event> for VersionedEvent {
    fn into_proto(&self) -> crate::objects::audit_log_key::Event {
        match self {
            VersionedEvent::V1(event) => {
                crate::objects::audit_log_key::Event::V1(event.into_proto())
            }
        }
    }

    fn from_proto(proto: crate::objects::audit_log_key::Event) -> Result<Self, TryFromProtoError> {
        match proto {
            crate::objects::audit_log_key::Event::V1(event) => {
                Ok(VersionedEvent::V1(event.into_rust()?))
            }
        }
    }
}

impl RustType<crate::objects::audit_log_event_v1::EventType> for EventType {
    fn into_proto(&self) -> crate::objects::audit_log_event_v1::EventType {
        match self {
            EventType::Create => crate::objects::audit_log_event_v1::EventType::Create,
            EventType::Drop => crate::objects::audit_log_event_v1::EventType::Drop,
            EventType::Alter => crate::objects::audit_log_event_v1::EventType::Alter,
            EventType::Grant => crate::objects::audit_log_event_v1::EventType::Grant,
            EventType::Revoke => crate::objects::audit_log_event_v1::EventType::Revoke,
            EventType::Comment => crate::objects::audit_log_event_v1::EventType::Comment,
        }
    }

    fn from_proto(
        proto: crate::objects::audit_log_event_v1::EventType,
    ) -> Result<Self, TryFromProtoError> {
        match proto {
            crate::objects::audit_log_event_v1::EventType::Create => Ok(EventType::Create),
            crate::objects::audit_log_event_v1::EventType::Drop => Ok(EventType::Drop),
            crate::objects::audit_log_event_v1::EventType::Alter => Ok(EventType::Alter),
            crate::objects::audit_log_event_v1::EventType::Grant => Ok(EventType::Grant),
            crate::objects::audit_log_event_v1::EventType::Revoke => Ok(EventType::Revoke),
            crate::objects::audit_log_event_v1::EventType::Comment => Ok(EventType::Comment),
            crate::objects::audit_log_event_v1::EventType::Unknown => Err(
                TryFromProtoError::unknown_enum_variant("EventType::Unknown"),
            ),
        }
    }
}

impl RustType<crate::objects::audit_log_event_v1::ObjectType> for mz_audit_log::ObjectType {
    fn into_proto(&self) -> crate::objects::audit_log_event_v1::ObjectType {
        match self {
            mz_audit_log::ObjectType::Cluster => {
                crate::objects::audit_log_event_v1::ObjectType::Cluster
            }
            mz_audit_log::ObjectType::ClusterReplica => {
                crate::objects::audit_log_event_v1::ObjectType::ClusterReplica
            }
            mz_audit_log::ObjectType::Connection => {
                crate::objects::audit_log_event_v1::ObjectType::Connection
            }
            mz_audit_log::ObjectType::ContinualTask => {
                crate::objects::audit_log_event_v1::ObjectType::ContinualTask
            }
            mz_audit_log::ObjectType::Database => {
                crate::objects::audit_log_event_v1::ObjectType::Database
            }
            mz_audit_log::ObjectType::Func => crate::objects::audit_log_event_v1::ObjectType::Func,
            mz_audit_log::ObjectType::Index => {
                crate::objects::audit_log_event_v1::ObjectType::Index
            }
            mz_audit_log::ObjectType::MaterializedView => {
                crate::objects::audit_log_event_v1::ObjectType::MaterializedView
            }
            mz_audit_log::ObjectType::NetworkPolicy => {
                crate::objects::audit_log_event_v1::ObjectType::NetworkPolicy
            }
            mz_audit_log::ObjectType::Role => crate::objects::audit_log_event_v1::ObjectType::Role,
            mz_audit_log::ObjectType::Secret => {
                crate::objects::audit_log_event_v1::ObjectType::Secret
            }
            mz_audit_log::ObjectType::Schema => {
                crate::objects::audit_log_event_v1::ObjectType::Schema
            }
            mz_audit_log::ObjectType::Sink => crate::objects::audit_log_event_v1::ObjectType::Sink,
            mz_audit_log::ObjectType::Source => {
                crate::objects::audit_log_event_v1::ObjectType::Source
            }
            mz_audit_log::ObjectType::System => {
                crate::objects::audit_log_event_v1::ObjectType::System
            }
            mz_audit_log::ObjectType::Table => {
                crate::objects::audit_log_event_v1::ObjectType::Table
            }
            mz_audit_log::ObjectType::Type => crate::objects::audit_log_event_v1::ObjectType::Type,
            mz_audit_log::ObjectType::View => crate::objects::audit_log_event_v1::ObjectType::View,
        }
    }

    fn from_proto(
        proto: crate::objects::audit_log_event_v1::ObjectType,
    ) -> Result<Self, TryFromProtoError> {
        match proto {
            crate::objects::audit_log_event_v1::ObjectType::Cluster => {
                Ok(mz_audit_log::ObjectType::Cluster)
            }
            crate::objects::audit_log_event_v1::ObjectType::ClusterReplica => {
                Ok(mz_audit_log::ObjectType::ClusterReplica)
            }
            crate::objects::audit_log_event_v1::ObjectType::Connection => {
                Ok(mz_audit_log::ObjectType::Connection)
            }
            crate::objects::audit_log_event_v1::ObjectType::ContinualTask => {
                Ok(mz_audit_log::ObjectType::ContinualTask)
            }
            crate::objects::audit_log_event_v1::ObjectType::Database => {
                Ok(mz_audit_log::ObjectType::Database)
            }
            crate::objects::audit_log_event_v1::ObjectType::Func => {
                Ok(mz_audit_log::ObjectType::Func)
            }
            crate::objects::audit_log_event_v1::ObjectType::Index => {
                Ok(mz_audit_log::ObjectType::Index)
            }
            crate::objects::audit_log_event_v1::ObjectType::MaterializedView => {
                Ok(mz_audit_log::ObjectType::MaterializedView)
            }
            crate::objects::audit_log_event_v1::ObjectType::NetworkPolicy => {
                Ok(mz_audit_log::ObjectType::NetworkPolicy)
            }
            crate::objects::audit_log_event_v1::ObjectType::Role => {
                Ok(mz_audit_log::ObjectType::Role)
            }
            crate::objects::audit_log_event_v1::ObjectType::Secret => {
                Ok(mz_audit_log::ObjectType::Secret)
            }
            crate::objects::audit_log_event_v1::ObjectType::Schema => {
                Ok(mz_audit_log::ObjectType::Schema)
            }
            crate::objects::audit_log_event_v1::ObjectType::Sink => {
                Ok(mz_audit_log::ObjectType::Sink)
            }
            crate::objects::audit_log_event_v1::ObjectType::Source => {
                Ok(mz_audit_log::ObjectType::Source)
            }
            crate::objects::audit_log_event_v1::ObjectType::System => {
                Ok(mz_audit_log::ObjectType::System)
            }
            crate::objects::audit_log_event_v1::ObjectType::Table => {
                Ok(mz_audit_log::ObjectType::Table)
            }
            crate::objects::audit_log_event_v1::ObjectType::Type => {
                Ok(mz_audit_log::ObjectType::Type)
            }
            crate::objects::audit_log_event_v1::ObjectType::View => {
                Ok(mz_audit_log::ObjectType::View)
            }
            crate::objects::audit_log_event_v1::ObjectType::Unknown => Err(
                TryFromProtoError::unknown_enum_variant("ObjectType::Unknown"),
            ),
        }
    }
}

impl RustType<crate::objects::audit_log_event_v1::IdFullNameV1> for IdFullNameV1 {
    fn into_proto(&self) -> crate::objects::audit_log_event_v1::IdFullNameV1 {
        crate::objects::audit_log_event_v1::IdFullNameV1 {
            id: self.id.to_string(),
            name: Some(self.name.into_proto()),
        }
    }

    fn from_proto(
        proto: crate::objects::audit_log_event_v1::IdFullNameV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(IdFullNameV1 {
            id: proto.id,
            name: proto.name.into_rust_if_some("IdFullNameV1::name")?,
        })
    }
}

impl RustType<crate::objects::audit_log_event_v1::FullNameV1> for FullNameV1 {
    fn into_proto(&self) -> crate::objects::audit_log_event_v1::FullNameV1 {
        crate::objects::audit_log_event_v1::FullNameV1 {
            database: self.database.to_string(),
            schema: self.schema.to_string(),
            item: self.item.to_string(),
        }
    }

    fn from_proto(
        proto: crate::objects::audit_log_event_v1::FullNameV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(FullNameV1 {
            database: proto.database,
            schema: proto.schema,
            item: proto.item,
        })
    }
}

impl RustType<crate::objects::audit_log_event_v1::IdNameV1> for IdNameV1 {
    fn into_proto(&self) -> crate::objects::audit_log_event_v1::IdNameV1 {
        crate::objects::audit_log_event_v1::IdNameV1 {
            id: self.id.to_string(),
            name: self.name.to_string(),
        }
    }

    fn from_proto(
        proto: crate::objects::audit_log_event_v1::IdNameV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(IdNameV1 {
            id: proto.id,
            name: proto.name,
        })
    }
}

impl RustType<crate::objects::audit_log_event_v1::RenameItemV1> for RenameItemV1 {
    fn into_proto(&self) -> crate::objects::audit_log_event_v1::RenameItemV1 {
        crate::objects::audit_log_event_v1::RenameItemV1 {
            id: self.id.to_string(),
            old_name: Some(self.old_name.into_proto()),
            new_name: Some(self.new_name.into_proto()),
        }
    }

    fn from_proto(
        proto: crate::objects::audit_log_event_v1::RenameItemV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(RenameItemV1 {
            id: proto.id,
            old_name: proto.old_name.into_rust_if_some("RenameItemV1::old_name")?,
            new_name: proto.new_name.into_rust_if_some("RenameItemV1::new_name")?,
        })
    }
}

impl RustType<crate::objects::audit_log_event_v1::RenameClusterV1> for RenameClusterV1 {
    fn into_proto(&self) -> crate::objects::audit_log_event_v1::RenameClusterV1 {
        crate::objects::audit_log_event_v1::RenameClusterV1 {
            id: self.id.to_string(),
            old_name: self.old_name.into_proto(),
            new_name: self.new_name.into_proto(),
        }
    }

    fn from_proto(
        proto: crate::objects::audit_log_event_v1::RenameClusterV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(RenameClusterV1 {
            id: proto.id,
            old_name: proto.old_name,
            new_name: proto.new_name,
        })
    }
}

impl RustType<crate::objects::audit_log_event_v1::RenameClusterReplicaV1>
    for RenameClusterReplicaV1
{
    fn into_proto(&self) -> crate::objects::audit_log_event_v1::RenameClusterReplicaV1 {
        crate::objects::audit_log_event_v1::RenameClusterReplicaV1 {
            cluster_id: self.cluster_id.to_string(),
            replica_id: self.replica_id.to_string(),
            old_name: self.old_name.into_proto(),
            new_name: self.new_name.into_proto(),
        }
    }

    fn from_proto(
        proto: crate::objects::audit_log_event_v1::RenameClusterReplicaV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(RenameClusterReplicaV1 {
            cluster_id: proto.cluster_id,
            replica_id: proto.replica_id,
            old_name: proto.old_name,
            new_name: proto.new_name,
        })
    }
}

impl RustType<crate::objects::audit_log_event_v1::DropClusterReplicaV1> for DropClusterReplicaV1 {
    fn into_proto(&self) -> crate::objects::audit_log_event_v1::DropClusterReplicaV1 {
        crate::objects::audit_log_event_v1::DropClusterReplicaV1 {
            cluster_id: self.cluster_id.to_string(),
            cluster_name: self.cluster_name.to_string(),
            replica_id: self
                .replica_id
                .as_ref()
                .map(|id| crate::objects::StringWrapper {
                    inner: id.to_string(),
                }),
            replica_name: self.replica_name.to_string(),
        }
    }

    fn from_proto(
        proto: crate::objects::audit_log_event_v1::DropClusterReplicaV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(DropClusterReplicaV1 {
            cluster_id: proto.cluster_id,
            cluster_name: proto.cluster_name,
            replica_id: proto.replica_id.map(|s| s.inner),
            replica_name: proto.replica_name,
        })
    }
}

impl RustType<crate::objects::audit_log_event_v1::DropClusterReplicaV2> for DropClusterReplicaV2 {
    fn into_proto(&self) -> crate::objects::audit_log_event_v1::DropClusterReplicaV2 {
        crate::objects::audit_log_event_v1::DropClusterReplicaV2 {
            cluster_id: self.cluster_id.to_string(),
            cluster_name: self.cluster_name.to_string(),
            replica_id: self
                .replica_id
                .as_ref()
                .map(|id| crate::objects::StringWrapper {
                    inner: id.to_string(),
                }),
            replica_name: self.replica_name.to_string(),
            reason: Some(self.reason.into_proto()),
            scheduling_policies: self.scheduling_policies.into_proto(),
        }
    }

    fn from_proto(
        proto: crate::objects::audit_log_event_v1::DropClusterReplicaV2,
    ) -> Result<Self, TryFromProtoError> {
        Ok(DropClusterReplicaV2 {
            cluster_id: proto.cluster_id,
            cluster_name: proto.cluster_name,
            replica_id: proto.replica_id.map(|s| s.inner),
            replica_name: proto.replica_name,
            reason: proto
                .reason
                .into_rust_if_some("DropClusterReplicaV2::reason")?,
            scheduling_policies: proto.scheduling_policies.into_rust()?,
        })
    }
}

impl RustType<crate::objects::audit_log_event_v1::DropClusterReplicaV3> for DropClusterReplicaV3 {
    fn into_proto(&self) -> crate::objects::audit_log_event_v1::DropClusterReplicaV3 {
        crate::objects::audit_log_event_v1::DropClusterReplicaV3 {
            cluster_id: self.cluster_id.to_string(),
            cluster_name: self.cluster_name.to_string(),
            replica_id: self
                .replica_id
                .as_ref()
                .map(|id| crate::objects::StringWrapper {
                    inner: id.to_string(),
                }),
            replica_name: self.replica_name.to_string(),
            reason: Some(self.reason.into_proto()),
            scheduling_policies: self.scheduling_policies.into_proto(),
        }
    }

    fn from_proto(
        proto: crate::objects::audit_log_event_v1::DropClusterReplicaV3,
    ) -> Result<Self, TryFromProtoError> {
        Ok(DropClusterReplicaV3 {
            cluster_id: proto.cluster_id,
            cluster_name: proto.cluster_name,
            replica_id: proto.replica_id.map(|s| s.inner),
            replica_name: proto.replica_name,
            reason: proto
                .reason
                .into_rust_if_some("DropClusterReplicaV3::reason")?,
            scheduling_policies: proto.scheduling_policies.into_rust()?,
        })
    }
}

impl RustType<crate::objects::audit_log_event_v1::CreateClusterReplicaV1>
    for CreateClusterReplicaV1
{
    fn into_proto(&self) -> crate::objects::audit_log_event_v1::CreateClusterReplicaV1 {
        crate::objects::audit_log_event_v1::CreateClusterReplicaV1 {
            cluster_id: self.cluster_id.to_string(),
            cluster_name: self.cluster_name.to_string(),
            replica_id: self
                .replica_id
                .as_ref()
                .map(|id| crate::objects::StringWrapper {
                    inner: id.to_string(),
                }),
            replica_name: self.replica_name.to_string(),
            logical_size: self.logical_size.to_string(),
            disk: self.disk,
            billed_as: self.billed_as.clone(),
            internal: self.internal,
        }
    }

    fn from_proto(
        proto: crate::objects::audit_log_event_v1::CreateClusterReplicaV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(CreateClusterReplicaV1 {
            cluster_id: proto.cluster_id,
            cluster_name: proto.cluster_name,
            replica_id: proto.replica_id.map(|id| id.inner),
            replica_name: proto.replica_name,
            logical_size: proto.logical_size,
            disk: proto.disk,
            billed_as: proto.billed_as,
            internal: proto.internal,
        })
    }
}

impl RustType<crate::objects::audit_log_event_v1::CreateClusterReplicaV2>
    for CreateClusterReplicaV2
{
    fn into_proto(&self) -> crate::objects::audit_log_event_v1::CreateClusterReplicaV2 {
        crate::objects::audit_log_event_v1::CreateClusterReplicaV2 {
            cluster_id: self.cluster_id.to_string(),
            cluster_name: self.cluster_name.to_string(),
            replica_id: self
                .replica_id
                .as_ref()
                .map(|id| crate::objects::StringWrapper {
                    inner: id.to_string(),
                }),
            replica_name: self.replica_name.to_string(),
            logical_size: self.logical_size.to_string(),
            disk: self.disk,
            billed_as: self.billed_as.clone(),
            internal: self.internal,
            reason: Some(self.reason.into_proto()),
            scheduling_policies: self.scheduling_policies.into_proto(),
        }
    }

    fn from_proto(
        proto: crate::objects::audit_log_event_v1::CreateClusterReplicaV2,
    ) -> Result<Self, TryFromProtoError> {
        Ok(CreateClusterReplicaV2 {
            cluster_id: proto.cluster_id,
            cluster_name: proto.cluster_name,
            replica_id: proto.replica_id.map(|id| id.inner),
            replica_name: proto.replica_name,
            logical_size: proto.logical_size,
            disk: proto.disk,
            billed_as: proto.billed_as,
            internal: proto.internal,
            reason: proto
                .reason
                .into_rust_if_some("DropClusterReplicaV2::reason")?,
            scheduling_policies: proto.scheduling_policies.into_rust()?,
        })
    }
}

impl RustType<crate::objects::audit_log_event_v1::CreateClusterReplicaV3>
    for CreateClusterReplicaV3
{
    fn into_proto(&self) -> crate::objects::audit_log_event_v1::CreateClusterReplicaV3 {
        crate::objects::audit_log_event_v1::CreateClusterReplicaV3 {
            cluster_id: self.cluster_id.to_string(),
            cluster_name: self.cluster_name.to_string(),
            replica_id: self
                .replica_id
                .as_ref()
                .map(|id| crate::objects::StringWrapper {
                    inner: id.to_string(),
                }),
            replica_name: self.replica_name.to_string(),
            logical_size: self.logical_size.to_string(),
            disk: self.disk,
            billed_as: self.billed_as.clone(),
            internal: self.internal,
            reason: Some(self.reason.into_proto()),
            scheduling_policies: self.scheduling_policies.into_proto(),
        }
    }

    fn from_proto(
        proto: crate::objects::audit_log_event_v1::CreateClusterReplicaV3,
    ) -> Result<Self, TryFromProtoError> {
        Ok(CreateClusterReplicaV3 {
            cluster_id: proto.cluster_id,
            cluster_name: proto.cluster_name,
            replica_id: proto.replica_id.map(|id| id.inner),
            replica_name: proto.replica_name,
            logical_size: proto.logical_size,
            disk: proto.disk,
            billed_as: proto.billed_as,
            internal: proto.internal,
            reason: proto
                .reason
                .into_rust_if_some("DropClusterReplicaV3::reason")?,
            scheduling_policies: proto.scheduling_policies.into_rust()?,
        })
    }
}

impl RustType<crate::objects::audit_log_event_v1::CreateOrDropClusterReplicaReasonV1>
    for CreateOrDropClusterReplicaReasonV1
{
    fn into_proto(&self) -> crate::objects::audit_log_event_v1::CreateOrDropClusterReplicaReasonV1 {
        match self {
            CreateOrDropClusterReplicaReasonV1::Manual => crate::objects::audit_log_event_v1::CreateOrDropClusterReplicaReasonV1 {
                reason: Some(crate::objects::audit_log_event_v1::create_or_drop_cluster_replica_reason_v1::Reason::Manual(Empty {}))
            },
            CreateOrDropClusterReplicaReasonV1::Schedule => crate::objects::audit_log_event_v1::CreateOrDropClusterReplicaReasonV1 {
                reason: Some(crate::objects::audit_log_event_v1::create_or_drop_cluster_replica_reason_v1::Reason::Schedule(Empty {}))
            },
            CreateOrDropClusterReplicaReasonV1::System => crate::objects::audit_log_event_v1::CreateOrDropClusterReplicaReasonV1 {
                reason: Some(crate::objects::audit_log_event_v1::create_or_drop_cluster_replica_reason_v1::Reason::System(Empty {}))
            },
        }
    }

    fn from_proto(
        proto: crate::objects::audit_log_event_v1::CreateOrDropClusterReplicaReasonV1,
    ) -> Result<Self, TryFromProtoError> {
        match proto.reason {
            None => Err(TryFromProtoError::missing_field("CreateOrDropClusterReplicaReasonV1::reason")),
            Some(crate::objects::audit_log_event_v1::create_or_drop_cluster_replica_reason_v1::Reason::Manual(Empty {})) => Ok(CreateOrDropClusterReplicaReasonV1::Manual),
            Some(crate::objects::audit_log_event_v1::create_or_drop_cluster_replica_reason_v1::Reason::Schedule(Empty {})) => Ok(CreateOrDropClusterReplicaReasonV1::Schedule),
            Some(crate::objects::audit_log_event_v1::create_or_drop_cluster_replica_reason_v1::Reason::System(Empty {})) => Ok(CreateOrDropClusterReplicaReasonV1::System),
        }
    }
}

impl RustType<crate::objects::audit_log_event_v1::SchedulingDecisionsWithReasonsV1>
    for SchedulingDecisionsWithReasonsV1
{
    fn into_proto(&self) -> crate::objects::audit_log_event_v1::SchedulingDecisionsWithReasonsV1 {
        crate::objects::audit_log_event_v1::SchedulingDecisionsWithReasonsV1 {
            on_refresh: Some(self.on_refresh.into_proto()),
        }
    }

    fn from_proto(
        proto: crate::objects::audit_log_event_v1::SchedulingDecisionsWithReasonsV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(SchedulingDecisionsWithReasonsV1 {
            on_refresh: proto
                .on_refresh
                .into_rust_if_some("SchedulingDecisionsWithReasonsV1::on_refresh")?,
        })
    }
}

impl RustType<crate::objects::audit_log_event_v1::SchedulingDecisionsWithReasonsV2>
    for SchedulingDecisionsWithReasonsV2
{
    fn into_proto(&self) -> crate::objects::audit_log_event_v1::SchedulingDecisionsWithReasonsV2 {
        crate::objects::audit_log_event_v1::SchedulingDecisionsWithReasonsV2 {
            on_refresh: Some(self.on_refresh.into_proto()),
        }
    }

    fn from_proto(
        proto: crate::objects::audit_log_event_v1::SchedulingDecisionsWithReasonsV2,
    ) -> Result<Self, TryFromProtoError> {
        Ok(SchedulingDecisionsWithReasonsV2 {
            on_refresh: proto
                .on_refresh
                .into_rust_if_some("SchedulingDecisionsWithReasonsV2::on_refresh")?,
        })
    }
}

impl RustType<crate::objects::audit_log_event_v1::RefreshDecisionWithReasonV1>
    for RefreshDecisionWithReasonV1
{
    fn into_proto(&self) -> crate::objects::audit_log_event_v1::RefreshDecisionWithReasonV1 {
        let decision = match &self.decision {
            SchedulingDecisionV1::On => {
                crate::objects::audit_log_event_v1::refresh_decision_with_reason_v1::Decision::On(
                    Empty {},
                )
            }
            SchedulingDecisionV1::Off => {
                crate::objects::audit_log_event_v1::refresh_decision_with_reason_v1::Decision::Off(
                    Empty {},
                )
            }
        };
        crate::objects::audit_log_event_v1::RefreshDecisionWithReasonV1 {
            decision: Some(decision),
            objects_needing_refresh: self.objects_needing_refresh.clone(),
            rehydration_time_estimate: self.hydration_time_estimate.clone(),
        }
    }

    fn from_proto(
        proto: crate::objects::audit_log_event_v1::RefreshDecisionWithReasonV1,
    ) -> Result<Self, TryFromProtoError> {
        let decision = match proto.decision {
            None => {
                return Err(TryFromProtoError::missing_field(
                    "CreateOrDropClusterReplicaReasonV1::reason",
                ));
            }
            Some(
                crate::objects::audit_log_event_v1::refresh_decision_with_reason_v1::Decision::On(
                    Empty {},
                ),
            ) => SchedulingDecisionV1::On,
            Some(
                crate::objects::audit_log_event_v1::refresh_decision_with_reason_v1::Decision::Off(
                    Empty {},
                ),
            ) => SchedulingDecisionV1::Off,
        };
        Ok(RefreshDecisionWithReasonV1 {
            decision,
            objects_needing_refresh: proto.objects_needing_refresh,
            hydration_time_estimate: proto.rehydration_time_estimate,
        })
    }
}

impl RustType<crate::objects::audit_log_event_v1::RefreshDecisionWithReasonV2>
    for RefreshDecisionWithReasonV2
{
    fn into_proto(&self) -> crate::objects::audit_log_event_v1::RefreshDecisionWithReasonV2 {
        let decision = match &self.decision {
            SchedulingDecisionV1::On => {
                crate::objects::audit_log_event_v1::refresh_decision_with_reason_v2::Decision::On(
                    Empty {},
                )
            }
            SchedulingDecisionV1::Off => {
                crate::objects::audit_log_event_v1::refresh_decision_with_reason_v2::Decision::Off(
                    Empty {},
                )
            }
        };
        crate::objects::audit_log_event_v1::RefreshDecisionWithReasonV2 {
            decision: Some(decision),
            objects_needing_refresh: self.objects_needing_refresh.clone(),
            objects_needing_compaction: self.objects_needing_compaction.clone(),
            rehydration_time_estimate: self.hydration_time_estimate.clone(),
        }
    }

    fn from_proto(
        proto: crate::objects::audit_log_event_v1::RefreshDecisionWithReasonV2,
    ) -> Result<Self, TryFromProtoError> {
        let decision = match proto.decision {
            None => {
                return Err(TryFromProtoError::missing_field(
                    "CreateOrDropClusterReplicaReasonV2::reason",
                ));
            }
            Some(
                crate::objects::audit_log_event_v1::refresh_decision_with_reason_v2::Decision::On(
                    Empty {},
                ),
            ) => SchedulingDecisionV1::On,
            Some(
                crate::objects::audit_log_event_v1::refresh_decision_with_reason_v2::Decision::Off(
                    Empty {},
                ),
            ) => SchedulingDecisionV1::Off,
        };
        Ok(RefreshDecisionWithReasonV2 {
            decision,
            objects_needing_refresh: proto.objects_needing_refresh,
            objects_needing_compaction: proto.objects_needing_compaction,
            hydration_time_estimate: proto.rehydration_time_estimate,
        })
    }
}

impl RustType<crate::objects::audit_log_event_v1::CreateSourceSinkV1> for CreateSourceSinkV1 {
    fn into_proto(&self) -> crate::objects::audit_log_event_v1::CreateSourceSinkV1 {
        crate::objects::audit_log_event_v1::CreateSourceSinkV1 {
            id: self.id.to_string(),
            name: Some(self.name.into_proto()),
            size: self.size.as_ref().map(|s| crate::objects::StringWrapper {
                inner: s.to_string(),
            }),
        }
    }

    fn from_proto(
        proto: crate::objects::audit_log_event_v1::CreateSourceSinkV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(CreateSourceSinkV1 {
            id: proto.id,
            name: proto.name.into_rust_if_some("CreateSourceSinkV1::name")?,
            size: proto.size.map(|s| s.inner),
        })
    }
}

impl RustType<crate::objects::audit_log_event_v1::CreateSourceSinkV2> for CreateSourceSinkV2 {
    fn into_proto(&self) -> crate::objects::audit_log_event_v1::CreateSourceSinkV2 {
        crate::objects::audit_log_event_v1::CreateSourceSinkV2 {
            id: self.id.to_string(),
            name: Some(self.name.into_proto()),
            size: self.size.as_ref().map(|s| crate::objects::StringWrapper {
                inner: s.to_string(),
            }),
            external_type: self.external_type.to_string(),
        }
    }

    fn from_proto(
        proto: crate::objects::audit_log_event_v1::CreateSourceSinkV2,
    ) -> Result<Self, TryFromProtoError> {
        Ok(CreateSourceSinkV2 {
            id: proto.id,
            name: proto.name.into_rust_if_some("CreateSourceSinkV2::name")?,
            size: proto.size.map(|s| s.inner),
            external_type: proto.external_type,
        })
    }
}

impl RustType<crate::objects::audit_log_event_v1::CreateSourceSinkV3> for CreateSourceSinkV3 {
    fn into_proto(&self) -> crate::objects::audit_log_event_v1::CreateSourceSinkV3 {
        crate::objects::audit_log_event_v1::CreateSourceSinkV3 {
            id: self.id.to_string(),
            name: Some(self.name.into_proto()),
            external_type: self.external_type.to_string(),
        }
    }

    fn from_proto(
        proto: crate::objects::audit_log_event_v1::CreateSourceSinkV3,
    ) -> Result<Self, TryFromProtoError> {
        Ok(CreateSourceSinkV3 {
            id: proto.id,
            name: proto.name.into_rust_if_some("CreateSourceSinkV2::name")?,
            external_type: proto.external_type,
        })
    }
}

impl RustType<crate::objects::audit_log_event_v1::CreateSourceSinkV4> for CreateSourceSinkV4 {
    fn into_proto(&self) -> crate::objects::audit_log_event_v1::CreateSourceSinkV4 {
        crate::objects::audit_log_event_v1::CreateSourceSinkV4 {
            id: self.id.to_string(),
            cluster_id: self
                .cluster_id
                .as_ref()
                .map(|id| crate::objects::StringWrapper {
                    inner: id.to_string(),
                }),
            name: Some(self.name.into_proto()),
            external_type: self.external_type.to_string(),
        }
    }

    fn from_proto(
        proto: crate::objects::audit_log_event_v1::CreateSourceSinkV4,
    ) -> Result<Self, TryFromProtoError> {
        Ok(CreateSourceSinkV4 {
            id: proto.id,
            cluster_id: proto.cluster_id.map(|s| s.inner),
            name: proto.name.into_rust_if_some("CreateSourceSinkV4::name")?,
            external_type: proto.external_type,
        })
    }
}

impl RustType<crate::objects::audit_log_event_v1::CreateIndexV1> for CreateIndexV1 {
    fn into_proto(&self) -> crate::objects::audit_log_event_v1::CreateIndexV1 {
        crate::objects::audit_log_event_v1::CreateIndexV1 {
            id: self.id.to_string(),
            cluster_id: self.cluster_id.to_string(),
            name: Some(self.name.into_proto()),
        }
    }

    fn from_proto(
        proto: crate::objects::audit_log_event_v1::CreateIndexV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(CreateIndexV1 {
            id: proto.id,
            cluster_id: proto.cluster_id,
            name: proto.name.into_rust_if_some("CreateIndexV1::name")?,
        })
    }
}

impl RustType<crate::objects::audit_log_event_v1::CreateMaterializedViewV1>
    for CreateMaterializedViewV1
{
    fn into_proto(&self) -> crate::objects::audit_log_event_v1::CreateMaterializedViewV1 {
        crate::objects::audit_log_event_v1::CreateMaterializedViewV1 {
            id: self.id.to_string(),
            cluster_id: self.cluster_id.to_string(),
            name: Some(self.name.into_proto()),
        }
    }

    fn from_proto(
        proto: crate::objects::audit_log_event_v1::CreateMaterializedViewV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(CreateMaterializedViewV1 {
            id: proto.id,
            cluster_id: proto.cluster_id,
            name: proto
                .name
                .into_rust_if_some("CreateMaterializedViewV1::name")?,
        })
    }
}

impl RustType<crate::objects::audit_log_event_v1::AlterSourceSinkV1> for AlterSourceSinkV1 {
    fn into_proto(&self) -> crate::objects::audit_log_event_v1::AlterSourceSinkV1 {
        crate::objects::audit_log_event_v1::AlterSourceSinkV1 {
            id: self.id.to_string(),
            name: Some(self.name.into_proto()),
            old_size: self
                .old_size
                .as_ref()
                .map(|s| crate::objects::StringWrapper {
                    inner: s.to_string(),
                }),
            new_size: self
                .new_size
                .as_ref()
                .map(|s| crate::objects::StringWrapper {
                    inner: s.to_string(),
                }),
        }
    }

    fn from_proto(
        proto: crate::objects::audit_log_event_v1::AlterSourceSinkV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(AlterSourceSinkV1 {
            id: proto.id,
            name: proto.name.into_rust_if_some("AlterSourceSinkV1::name")?,
            old_size: proto.old_size.map(|s| s.inner),
            new_size: proto.new_size.map(|s| s.inner),
        })
    }
}

impl RustType<crate::objects::audit_log_event_v1::AlterSetClusterV1> for AlterSetClusterV1 {
    fn into_proto(&self) -> crate::objects::audit_log_event_v1::AlterSetClusterV1 {
        crate::objects::audit_log_event_v1::AlterSetClusterV1 {
            id: self.id.to_string(),
            name: Some(self.name.into_proto()),
            old_cluster: self
                .old_cluster
                .as_ref()
                .map(|s| crate::objects::StringWrapper {
                    inner: s.to_string(),
                }),
            new_cluster: self
                .new_cluster
                .as_ref()
                .map(|s| crate::objects::StringWrapper {
                    inner: s.to_string(),
                }),
        }
    }

    fn from_proto(
        proto: crate::objects::audit_log_event_v1::AlterSetClusterV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            id: proto.id,
            name: proto.name.into_rust_if_some("AlterSetClusterV1::name")?,
            old_cluster: proto.old_cluster.map(|s| s.inner),
            new_cluster: proto.new_cluster.map(|s| s.inner),
        })
    }
}

impl RustType<crate::objects::audit_log_event_v1::GrantRoleV1> for GrantRoleV1 {
    fn into_proto(&self) -> crate::objects::audit_log_event_v1::GrantRoleV1 {
        crate::objects::audit_log_event_v1::GrantRoleV1 {
            role_id: self.role_id.to_string(),
            member_id: self.member_id.to_string(),
            grantor_id: self.grantor_id.to_string(),
        }
    }

    fn from_proto(
        proto: crate::objects::audit_log_event_v1::GrantRoleV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(GrantRoleV1 {
            role_id: proto.role_id,
            member_id: proto.member_id,
            grantor_id: proto.grantor_id,
        })
    }
}

impl RustType<crate::objects::audit_log_event_v1::GrantRoleV2> for GrantRoleV2 {
    fn into_proto(&self) -> crate::objects::audit_log_event_v1::GrantRoleV2 {
        crate::objects::audit_log_event_v1::GrantRoleV2 {
            role_id: self.role_id.to_string(),
            member_id: self.member_id.to_string(),
            grantor_id: self.grantor_id.to_string(),
            executed_by: self.executed_by.to_string(),
        }
    }

    fn from_proto(
        proto: crate::objects::audit_log_event_v1::GrantRoleV2,
    ) -> Result<Self, TryFromProtoError> {
        Ok(GrantRoleV2 {
            role_id: proto.role_id,
            member_id: proto.member_id,
            grantor_id: proto.grantor_id,
            executed_by: proto.executed_by,
        })
    }
}

impl RustType<crate::objects::audit_log_event_v1::RevokeRoleV1> for RevokeRoleV1 {
    fn into_proto(&self) -> crate::objects::audit_log_event_v1::RevokeRoleV1 {
        crate::objects::audit_log_event_v1::RevokeRoleV1 {
            role_id: self.role_id.to_string(),
            member_id: self.member_id.to_string(),
        }
    }

    fn from_proto(
        proto: crate::objects::audit_log_event_v1::RevokeRoleV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(RevokeRoleV1 {
            role_id: proto.role_id,
            member_id: proto.member_id,
        })
    }
}

impl RustType<crate::objects::audit_log_event_v1::RevokeRoleV2> for RevokeRoleV2 {
    fn into_proto(&self) -> crate::objects::audit_log_event_v1::RevokeRoleV2 {
        crate::objects::audit_log_event_v1::RevokeRoleV2 {
            role_id: self.role_id.to_string(),
            member_id: self.member_id.to_string(),
            grantor_id: self.grantor_id.to_string(),
            executed_by: self.executed_by.to_string(),
        }
    }

    fn from_proto(
        proto: crate::objects::audit_log_event_v1::RevokeRoleV2,
    ) -> Result<Self, TryFromProtoError> {
        Ok(RevokeRoleV2 {
            role_id: proto.role_id,
            member_id: proto.member_id,
            grantor_id: proto.grantor_id,
            executed_by: proto.executed_by,
        })
    }
}

impl RustType<crate::objects::audit_log_event_v1::UpdatePrivilegeV1> for UpdatePrivilegeV1 {
    fn into_proto(&self) -> crate::objects::audit_log_event_v1::UpdatePrivilegeV1 {
        crate::objects::audit_log_event_v1::UpdatePrivilegeV1 {
            object_id: self.object_id.to_string(),
            grantee_id: self.grantee_id.to_string(),
            grantor_id: self.grantor_id.to_string(),
            privileges: self.privileges.to_string(),
        }
    }

    fn from_proto(
        proto: crate::objects::audit_log_event_v1::UpdatePrivilegeV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(UpdatePrivilegeV1 {
            object_id: proto.object_id,
            grantee_id: proto.grantee_id,
            grantor_id: proto.grantor_id,
            privileges: proto.privileges,
        })
    }
}

impl RustType<crate::objects::audit_log_event_v1::AlterDefaultPrivilegeV1>
    for AlterDefaultPrivilegeV1
{
    fn into_proto(&self) -> crate::objects::audit_log_event_v1::AlterDefaultPrivilegeV1 {
        crate::objects::audit_log_event_v1::AlterDefaultPrivilegeV1 {
            role_id: self.role_id.to_string(),
            database_id: self
                .database_id
                .as_ref()
                .map(|id| crate::objects::StringWrapper {
                    inner: id.to_string(),
                }),
            schema_id: self
                .schema_id
                .as_ref()
                .map(|id| crate::objects::StringWrapper {
                    inner: id.to_string(),
                }),
            grantee_id: self.grantee_id.to_string(),
            privileges: self.privileges.to_string(),
        }
    }

    fn from_proto(
        proto: crate::objects::audit_log_event_v1::AlterDefaultPrivilegeV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(AlterDefaultPrivilegeV1 {
            role_id: proto.role_id,
            database_id: proto.database_id.map(|id| id.inner),
            schema_id: proto.schema_id.map(|id| id.inner),
            grantee_id: proto.grantee_id,
            privileges: proto.privileges,
        })
    }
}

impl RustType<crate::objects::audit_log_event_v1::UpdateOwnerV1> for UpdateOwnerV1 {
    fn into_proto(&self) -> crate::objects::audit_log_event_v1::UpdateOwnerV1 {
        crate::objects::audit_log_event_v1::UpdateOwnerV1 {
            object_id: self.object_id.to_string(),
            old_owner_id: self.old_owner_id.to_string(),
            new_owner_id: self.new_owner_id.to_string(),
        }
    }

    fn from_proto(
        proto: crate::objects::audit_log_event_v1::UpdateOwnerV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(UpdateOwnerV1 {
            object_id: proto.object_id,
            old_owner_id: proto.old_owner_id,
            new_owner_id: proto.new_owner_id,
        })
    }
}

impl RustType<crate::objects::audit_log_event_v1::SchemaV1> for SchemaV1 {
    fn into_proto(&self) -> crate::objects::audit_log_event_v1::SchemaV1 {
        crate::objects::audit_log_event_v1::SchemaV1 {
            id: self.id.to_string(),
            name: self.name.to_string(),
            database_name: self.database_name.to_string(),
        }
    }

    fn from_proto(
        proto: crate::objects::audit_log_event_v1::SchemaV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(SchemaV1 {
            id: proto.id,
            name: proto.name,
            database_name: proto.database_name,
        })
    }
}

impl RustType<crate::objects::audit_log_event_v1::SchemaV2> for SchemaV2 {
    fn into_proto(&self) -> crate::objects::audit_log_event_v1::SchemaV2 {
        crate::objects::audit_log_event_v1::SchemaV2 {
            id: self.id.to_string(),
            name: self.name.to_string(),
            database_name: self
                .database_name
                .as_ref()
                .map(|d| crate::objects::StringWrapper {
                    inner: d.to_string(),
                }),
        }
    }

    fn from_proto(
        proto: crate::objects::audit_log_event_v1::SchemaV2,
    ) -> Result<Self, TryFromProtoError> {
        Ok(SchemaV2 {
            id: proto.id,
            name: proto.name,
            database_name: proto.database_name.map(|d| d.inner),
        })
    }
}

impl RustType<crate::objects::audit_log_event_v1::RenameSchemaV1> for RenameSchemaV1 {
    fn into_proto(&self) -> crate::objects::audit_log_event_v1::RenameSchemaV1 {
        crate::objects::audit_log_event_v1::RenameSchemaV1 {
            id: self.id.to_string(),
            database_name: self.database_name.clone(),
            old_name: self.old_name.clone(),
            new_name: self.new_name.clone(),
        }
    }

    fn from_proto(
        proto: crate::objects::audit_log_event_v1::RenameSchemaV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(RenameSchemaV1 {
            id: proto.id,
            database_name: proto.database_name,
            old_name: proto.old_name,
            new_name: proto.new_name,
        })
    }
}

impl RustType<crate::objects::audit_log_event_v1::UpdateItemV1> for UpdateItemV1 {
    fn into_proto(&self) -> crate::objects::audit_log_event_v1::UpdateItemV1 {
        crate::objects::audit_log_event_v1::UpdateItemV1 {
            id: self.id.to_string(),
            name: Some(self.name.into_proto()),
        }
    }

    fn from_proto(
        proto: crate::objects::audit_log_event_v1::UpdateItemV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(UpdateItemV1 {
            id: proto.id,
            name: proto.name.into_rust_if_some("UpdateItemV1::name")?,
        })
    }
}

impl RustType<crate::objects::audit_log_event_v1::AlterRetainHistoryV1> for AlterRetainHistoryV1 {
    fn into_proto(&self) -> crate::objects::audit_log_event_v1::AlterRetainHistoryV1 {
        crate::objects::audit_log_event_v1::AlterRetainHistoryV1 {
            id: self.id.to_string(),
            old_history: self.old_history.clone(),
            new_history: self.new_history.clone(),
        }
    }

    fn from_proto(
        proto: crate::objects::audit_log_event_v1::AlterRetainHistoryV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(AlterRetainHistoryV1 {
            id: proto.id,
            old_history: proto.old_history,
            new_history: proto.new_history,
        })
    }
}

impl RustType<crate::objects::audit_log_event_v1::ToNewIdV1> for ToNewIdV1 {
    fn into_proto(&self) -> crate::objects::audit_log_event_v1::ToNewIdV1 {
        crate::objects::audit_log_event_v1::ToNewIdV1 {
            id: self.id.to_string(),
            new_id: self.new_id.to_string(),
        }
    }

    fn from_proto(
        proto: crate::objects::audit_log_event_v1::ToNewIdV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(ToNewIdV1 {
            id: proto.id,
            new_id: proto.new_id,
        })
    }
}

impl RustType<crate::objects::audit_log_event_v1::FromPreviousIdV1> for FromPreviousIdV1 {
    fn into_proto(&self) -> crate::objects::audit_log_event_v1::FromPreviousIdV1 {
        crate::objects::audit_log_event_v1::FromPreviousIdV1 {
            id: self.id.to_string(),
            previous_id: self.previous_id.to_string(),
        }
    }

    fn from_proto(
        proto: crate::objects::audit_log_event_v1::FromPreviousIdV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(FromPreviousIdV1 {
            id: proto.id,
            previous_id: proto.previous_id,
        })
    }
}

impl RustType<crate::objects::audit_log_event_v1::SetV1> for SetV1 {
    fn into_proto(&self) -> crate::objects::audit_log_event_v1::SetV1 {
        crate::objects::audit_log_event_v1::SetV1 {
            name: self.name.clone(),
            value: self.value.clone(),
        }
    }

    fn from_proto(
        proto: crate::objects::audit_log_event_v1::SetV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(SetV1 {
            name: proto.name,
            value: proto.value,
        })
    }
}

impl RustType<crate::objects::audit_log_event_v1::RotateKeysV1> for RotateKeysV1 {
    fn into_proto(&self) -> crate::objects::audit_log_event_v1::RotateKeysV1 {
        crate::objects::audit_log_event_v1::RotateKeysV1 {
            id: self.id.clone(),
            name: self.name.clone(),
        }
    }

    fn from_proto(
        proto: crate::objects::audit_log_event_v1::RotateKeysV1,
    ) -> Result<Self, TryFromProtoError> {
        Ok(RotateKeysV1 {
            id: proto.id,
            name: proto.name,
        })
    }
}

impl RustType<crate::objects::audit_log_event_v1::Details> for EventDetails {
    fn into_proto(&self) -> crate::objects::audit_log_event_v1::Details {
        use crate::objects::audit_log_event_v1::Details::*;

        match self {
            EventDetails::CreateClusterReplicaV1(details) => {
                CreateClusterReplicaV1(details.into_proto())
            }
            EventDetails::CreateClusterReplicaV2(details) => {
                CreateClusterReplicaV2(details.into_proto())
            }
            EventDetails::CreateClusterReplicaV3(details) => {
                CreateClusterReplicaV3(details.into_proto())
            }
            EventDetails::DropClusterReplicaV1(details) => {
                DropClusterReplicaV1(details.into_proto())
            }
            EventDetails::DropClusterReplicaV2(details) => {
                DropClusterReplicaV2(details.into_proto())
            }
            EventDetails::DropClusterReplicaV3(details) => {
                DropClusterReplicaV3(details.into_proto())
            }
            EventDetails::CreateSourceSinkV1(details) => CreateSourceSinkV1(details.into_proto()),
            EventDetails::CreateSourceSinkV2(details) => CreateSourceSinkV2(details.into_proto()),
            EventDetails::CreateSourceSinkV3(details) => CreateSourceSinkV3(details.into_proto()),
            EventDetails::CreateSourceSinkV4(details) => CreateSourceSinkV4(details.into_proto()),
            EventDetails::CreateIndexV1(details) => CreateIndexV1(details.into_proto()),
            EventDetails::CreateMaterializedViewV1(details) => {
                CreateMaterializedViewV1(details.into_proto())
            }
            EventDetails::AlterSourceSinkV1(details) => AlterSourceSinkV1(details.into_proto()),
            EventDetails::AlterSetClusterV1(details) => AlterSetClusterV1(details.into_proto()),
            EventDetails::GrantRoleV1(details) => GrantRoleV1(details.into_proto()),
            EventDetails::GrantRoleV2(details) => GrantRoleV2(details.into_proto()),
            EventDetails::RevokeRoleV1(details) => RevokeRoleV1(details.into_proto()),
            EventDetails::RevokeRoleV2(details) => RevokeRoleV2(details.into_proto()),
            EventDetails::UpdatePrivilegeV1(details) => UpdatePrivilegeV1(details.into_proto()),
            EventDetails::AlterDefaultPrivilegeV1(details) => {
                AlterDefaultPrivilegeV1(details.into_proto())
            }
            EventDetails::UpdateOwnerV1(details) => UpdateOwnerV1(details.into_proto()),
            EventDetails::IdFullNameV1(details) => IdFullNameV1(details.into_proto()),
            EventDetails::RenameClusterV1(details) => RenameClusterV1(details.into_proto()),
            EventDetails::RenameClusterReplicaV1(details) => {
                RenameClusterReplicaV1(details.into_proto())
            }
            EventDetails::RenameItemV1(details) => RenameItemV1(details.into_proto()),
            EventDetails::IdNameV1(details) => IdNameV1(details.into_proto()),
            EventDetails::SchemaV1(details) => SchemaV1(details.into_proto()),
            EventDetails::SchemaV2(details) => SchemaV2(details.into_proto()),
            EventDetails::RenameSchemaV1(details) => RenameSchemaV1(details.into_proto()),
            EventDetails::UpdateItemV1(details) => UpdateItemV1(details.into_proto()),
            EventDetails::AlterRetainHistoryV1(details) => {
                AlterRetainHistoryV1(details.into_proto())
            }
            EventDetails::ToNewIdV1(details) => ToNewIdV1(details.into_proto()),
            EventDetails::FromPreviousIdV1(details) => FromPreviousIdV1(details.into_proto()),
            EventDetails::SetV1(details) => SetV1(details.into_proto()),
            EventDetails::ResetAllV1 => ResetAllV1(Empty {}),
            EventDetails::RotateKeysV1(details) => RotateKeysV1(details.into_proto()),
        }
    }

    fn from_proto(
        proto: crate::objects::audit_log_event_v1::Details,
    ) -> Result<Self, TryFromProtoError> {
        use crate::objects::audit_log_event_v1::Details::*;

        match proto {
            CreateClusterReplicaV1(details) => {
                Ok(EventDetails::CreateClusterReplicaV1(details.into_rust()?))
            }
            CreateClusterReplicaV2(details) => {
                Ok(EventDetails::CreateClusterReplicaV2(details.into_rust()?))
            }
            CreateClusterReplicaV3(details) => {
                Ok(EventDetails::CreateClusterReplicaV3(details.into_rust()?))
            }
            DropClusterReplicaV1(details) => {
                Ok(EventDetails::DropClusterReplicaV1(details.into_rust()?))
            }
            DropClusterReplicaV2(details) => {
                Ok(EventDetails::DropClusterReplicaV2(details.into_rust()?))
            }
            DropClusterReplicaV3(details) => {
                Ok(EventDetails::DropClusterReplicaV3(details.into_rust()?))
            }
            CreateSourceSinkV1(details) => {
                Ok(EventDetails::CreateSourceSinkV1(details.into_rust()?))
            }
            CreateSourceSinkV2(details) => {
                Ok(EventDetails::CreateSourceSinkV2(details.into_rust()?))
            }
            CreateSourceSinkV3(details) => {
                Ok(EventDetails::CreateSourceSinkV3(details.into_rust()?))
            }
            CreateSourceSinkV4(details) => {
                Ok(EventDetails::CreateSourceSinkV4(details.into_rust()?))
            }
            CreateIndexV1(details) => Ok(EventDetails::CreateIndexV1(details.into_rust()?)),
            CreateMaterializedViewV1(details) => {
                Ok(EventDetails::CreateMaterializedViewV1(details.into_rust()?))
            }
            AlterSourceSinkV1(details) => Ok(EventDetails::AlterSourceSinkV1(details.into_rust()?)),
            AlterSetClusterV1(details) => Ok(EventDetails::AlterSetClusterV1(details.into_rust()?)),
            GrantRoleV1(details) => Ok(EventDetails::GrantRoleV1(details.into_rust()?)),
            GrantRoleV2(details) => Ok(EventDetails::GrantRoleV2(details.into_rust()?)),
            RevokeRoleV1(details) => Ok(EventDetails::RevokeRoleV1(details.into_rust()?)),
            RevokeRoleV2(details) => Ok(EventDetails::RevokeRoleV2(details.into_rust()?)),
            UpdatePrivilegeV1(details) => Ok(EventDetails::UpdatePrivilegeV1(details.into_rust()?)),
            AlterDefaultPrivilegeV1(details) => {
                Ok(EventDetails::AlterDefaultPrivilegeV1(details.into_rust()?))
            }
            UpdateOwnerV1(details) => Ok(EventDetails::UpdateOwnerV1(details.into_rust()?)),
            IdFullNameV1(details) => Ok(EventDetails::IdFullNameV1(details.into_rust()?)),
            RenameClusterV1(details) => Ok(EventDetails::RenameClusterV1(details.into_rust()?)),
            RenameClusterReplicaV1(details) => {
                Ok(EventDetails::RenameClusterReplicaV1(details.into_rust()?))
            }
            RenameItemV1(details) => Ok(EventDetails::RenameItemV1(details.into_rust()?)),
            IdNameV1(details) => Ok(EventDetails::IdNameV1(details.into_rust()?)),
            SchemaV1(details) => Ok(EventDetails::SchemaV1(details.into_rust()?)),
            SchemaV2(details) => Ok(EventDetails::SchemaV2(details.into_rust()?)),
            RenameSchemaV1(details) => Ok(EventDetails::RenameSchemaV1(details.into_rust()?)),
            UpdateItemV1(details) => Ok(EventDetails::UpdateItemV1(details.into_rust()?)),
            AlterRetainHistoryV1(details) => {
                Ok(EventDetails::AlterRetainHistoryV1(details.into_rust()?))
            }
            ToNewIdV1(details) => Ok(EventDetails::ToNewIdV1(details.into_rust()?)),
            FromPreviousIdV1(details) => Ok(EventDetails::FromPreviousIdV1(details.into_rust()?)),
            SetV1(details) => Ok(EventDetails::SetV1(details.into_rust()?)),
            ResetAllV1(Empty {}) => Ok(EventDetails::ResetAllV1),
            RotateKeysV1(details) => Ok(EventDetails::RotateKeysV1(details.into_rust()?)),
        }
    }
}

impl RustType<crate::objects::AuditLogEventV1> for EventV1 {
    fn into_proto(&self) -> crate::objects::AuditLogEventV1 {
        crate::objects::AuditLogEventV1 {
            id: self.id,
            event_type: self.event_type.into_proto().into(),
            object_type: self.object_type.into_proto().into(),
            user: self.user.as_ref().map(|u| crate::objects::StringWrapper {
                inner: u.to_string(),
            }),
            occurred_at: Some(crate::objects::EpochMillis {
                millis: self.occurred_at,
            }),
            details: Some(self.details.into_proto()),
        }
    }

    fn from_proto(proto: crate::objects::AuditLogEventV1) -> Result<Self, TryFromProtoError> {
        let event_type = crate::objects::audit_log_event_v1::EventType::try_from(proto.event_type)
            .map_err(|_| TryFromProtoError::unknown_enum_variant("EventType"))?;
        let object_type =
            crate::objects::audit_log_event_v1::ObjectType::try_from(proto.object_type)
                .map_err(|_| TryFromProtoError::unknown_enum_variant("ObjectType"))?;
        Ok(EventV1 {
            id: proto.id,
            event_type: event_type.into_rust()?,
            object_type: object_type.into_rust()?,
            details: proto
                .details
                .into_rust_if_some("AuditLogEventV1::details")?,
            user: proto.user.map(|u| u.inner),
            occurred_at: proto
                .occurred_at
                .into_rust_if_some("AuditLogEventV1::occurred_at")?,
        })
    }
}
