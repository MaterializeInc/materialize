// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(clippy::unwrap_used)]

use crate::durable::traits::UpgradeFrom;
use crate::durable::traits::UpgradeInto;
use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::json_compatible::JsonCompatible;
use crate::durable::upgrade::objects_v78 as v78;
use crate::durable::upgrade::objects_v79 as v79;
use crate::json_compatible;

json_compatible!(v78::AclMode with v79::AclMode);
json_compatible!(v78::CommentValue with v79::CommentValue);
json_compatible!(v78::ConfigKey with v79::ConfigKey);
json_compatible!(v78::ConfigValue with v79::ConfigValue);
json_compatible!(v78::Duration with v79::Duration);
json_compatible!(v78::EpochMillis with v79::EpochMillis);
json_compatible!(v78::IdAllocKey with v79::IdAllocatorKey);
json_compatible!(v78::IdAllocValue with v79::IdAllocatorValue);
json_compatible!(v78::OptimizerFeatureOverride with v79::OptimizerFeatureOverride);
json_compatible!(v78::RoleAttributes with v79::RoleAttributes);
json_compatible!(v78::ServerConfigurationKey with v79::SystemConfigurationKey);
json_compatible!(v78::ServerConfigurationValue with v79::SystemConfigurationValue);
json_compatible!(v78::SettingKey with v79::SettingKey);
json_compatible!(v78::SettingValue with v79::SettingValue);
json_compatible!(v78::SourceReference with v79::SourceReference);
json_compatible!(v78::StorageCollectionMetadataValue with v79::StorageCollectionMetadataValue);
json_compatible!(v78::TxnWalShardValue with v79::TxnWalShardValue);
json_compatible!(v78::UnfinalizedShardKey with v79::UnfinalizedShardKey);
json_compatible!(v78::Version with v79::Version);
json_compatible!(v78::audit_log_event_v1::FullNameV1 with v79::audit_log_event_v1::FullNameV1);
json_compatible!(v78::audit_log_event_v1::IdNameV1 with v79::audit_log_event_v1::IdNameV1);
json_compatible!(v78::audit_log_event_v1::RenameClusterV1 with v79::audit_log_event_v1::RenameClusterV1);
json_compatible!(v78::audit_log_event_v1::RenameClusterReplicaV1 with v79::audit_log_event_v1::RenameClusterReplicaV1);
json_compatible!(v78::audit_log_event_v1::RenameSchemaV1 with v79::audit_log_event_v1::RenameSchemaV1);
json_compatible!(v78::audit_log_event_v1::SchemaV1 with v79::audit_log_event_v1::SchemaV1);
json_compatible!(v78::audit_log_event_v1::GrantRoleV1 with v79::audit_log_event_v1::GrantRoleV1);
json_compatible!(v78::audit_log_event_v1::GrantRoleV2 with v79::audit_log_event_v1::GrantRoleV2);
json_compatible!(v78::audit_log_event_v1::RevokeRoleV1 with v79::audit_log_event_v1::RevokeRoleV1);
json_compatible!(v78::audit_log_event_v1::RevokeRoleV2 with v79::audit_log_event_v1::RevokeRoleV2);
json_compatible!(v78::audit_log_event_v1::UpdatePrivilegeV1 with v79::audit_log_event_v1::UpdatePrivilegeV1);
json_compatible!(v78::audit_log_event_v1::UpdateOwnerV1 with v79::audit_log_event_v1::UpdateOwnerV1);
json_compatible!(v78::audit_log_event_v1::AlterRetainHistoryV1 with v79::audit_log_event_v1::AlterRetainHistoryV1);
json_compatible!(v78::audit_log_event_v1::ToNewIdV1 with v79::audit_log_event_v1::ToNewIdV1);
json_compatible!(v78::audit_log_event_v1::FromPreviousIdV1 with v79::audit_log_event_v1::FromPreviousIdV1);
json_compatible!(v78::audit_log_event_v1::SetV1 with v79::audit_log_event_v1::SetV1);
json_compatible!(v78::audit_log_event_v1::RotateKeysV1 with v79::audit_log_event_v1::RotateKeysV1);
json_compatible!(v78::replica_config::UnmanagedLocation with v79::UnmanagedLocation);
json_compatible!(v78::replica_config::ManagedLocation with v79::ManagedLocation);
json_compatible!(v78::state_update_kind::FenceToken with v79::FenceToken);

pub fn upgrade(
    snapshot: Vec<v78::StateUpdateKind>,
) -> Vec<MigrationAction<v78::StateUpdateKind, v79::StateUpdateKind>> {
    snapshot
        .into_iter()
        .map(|old| {
            let new = old.clone().upgrade_into();
            MigrationAction::Update(old, new)
        })
        .collect()
}

impl UpgradeFrom<v78::StateUpdateKind> for v79::StateUpdateKind {
    fn upgrade_from(old: v78::StateUpdateKind) -> Self {
        use v78::state_update_kind::Kind::*;
        match old.kind.unwrap() {
            AuditLog(x) => Self::AuditLog(x.upgrade_into()),
            Cluster(x) => Self::Cluster(x.upgrade_into()),
            ClusterReplica(x) => Self::ClusterReplica(x.upgrade_into()),
            Comment(x) => Self::Comment(x.upgrade_into()),
            Config(x) => Self::Config(x.upgrade_into()),
            Database(x) => Self::Database(x.upgrade_into()),
            DefaultPrivileges(x) => Self::DefaultPrivilege(x.upgrade_into()),
            IdAlloc(x) => Self::IdAllocator(x.upgrade_into()),
            ClusterIntrospectionSourceIndex(x) => Self::IntrospectionSourceIndex(x.upgrade_into()),
            Item(x) => Self::Item(x.upgrade_into()),
            Role(x) => Self::Role(x.upgrade_into()),
            Schema(x) => Self::Schema(x.upgrade_into()),
            Setting(x) => Self::Setting(x.upgrade_into()),
            ServerConfiguration(x) => Self::SystemConfiguration(x.upgrade_into()),
            GidMapping(x) => Self::SystemObjectMapping(x.upgrade_into()),
            SystemPrivileges(x) => Self::SystemPrivilege(x.upgrade_into()),
            StorageCollectionMetadata(x) => Self::StorageCollectionMetadata(x.upgrade_into()),
            UnfinalizedShard(x) => Self::UnfinalizedShard(x.upgrade_into()),
            TxnWalShard(x) => Self::TxnWalShard(x.upgrade_into()),
            SourceReferences(x) => Self::SourceReferences(x.upgrade_into()),
            FenceToken(x) => Self::FenceToken(x.upgrade_into()),
            NetworkPolicy(x) => Self::NetworkPolicy(x.upgrade_into()),
            RoleAuth(x) => Self::RoleAuth(x.upgrade_into()),
        }
    }
}

impl UpgradeFrom<v78::state_update_kind::AuditLog> for v79::AuditLog {
    fn upgrade_from(old: v78::state_update_kind::AuditLog) -> Self {
        Self {
            key: old.key.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::AuditLogKey> for v79::AuditLogKey {
    fn upgrade_from(old: v78::AuditLogKey) -> Self {
        Self {
            event: old.event.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::audit_log_key::Event> for v79::AuditLogEvent {
    fn upgrade_from(old: v78::audit_log_key::Event) -> Self {
        use v78::audit_log_key::Event::*;
        match old {
            V1(x) => Self::V1(x.upgrade_into()),
        }
    }
}

impl UpgradeFrom<v78::AuditLogEventV1> for v79::AuditLogEventV1 {
    fn upgrade_from(old: v78::AuditLogEventV1) -> Self {
        Self {
            id: old.id,
            event_type: old.event_type.upgrade_into(),
            object_type: old.object_type.upgrade_into(),
            user: old.user.map(|x| x.inner),
            occurred_at: JsonCompatible::convert(&old.occurred_at.unwrap()),
            details: old.details.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<i32> for v79::audit_log_event_v1::EventType {
    fn upgrade_from(value: i32) -> Self {
        match value {
            0 => Self::Unknown,
            1 => Self::Create,
            2 => Self::Drop,
            3 => Self::Alter,
            4 => Self::Grant,
            5 => Self::Revoke,
            6 => Self::Comment,
            x => panic!("invalid EventType: {x}"),
        }
    }
}

impl UpgradeFrom<i32> for v79::audit_log_event_v1::ObjectType {
    fn upgrade_from(value: i32) -> Self {
        match value {
            0 => Self::Unknown,
            1 => Self::Cluster,
            2 => Self::ClusterReplica,
            3 => Self::Connection,
            4 => Self::Database,
            5 => Self::Func,
            6 => Self::Index,
            7 => Self::MaterializedView,
            8 => Self::Role,
            9 => Self::Secret,
            10 => Self::Schema,
            11 => Self::Sink,
            12 => Self::Source,
            13 => Self::Table,
            14 => Self::Type,
            15 => Self::View,
            16 => Self::System,
            17 => Self::ContinualTask,
            18 => Self::NetworkPolicy,
            x => panic!("invalid ObjectType: {x}"),
        }
    }
}

impl UpgradeFrom<v78::audit_log_event_v1::Details> for v79::audit_log_event_v1::Details {
    fn upgrade_from(old: v78::audit_log_event_v1::Details) -> Self {
        use v78::audit_log_event_v1::Details::*;
        match old {
            CreateClusterReplicaV1(x) => Self::CreateClusterReplicaV1(x.upgrade_into()),
            CreateClusterReplicaV2(x) => Self::CreateClusterReplicaV2(x.upgrade_into()),
            CreateClusterReplicaV3(x) => Self::CreateClusterReplicaV3(x.upgrade_into()),
            CreateClusterReplicaV4(x) => Self::CreateClusterReplicaV4(x.upgrade_into()),
            DropClusterReplicaV1(x) => Self::DropClusterReplicaV1(x.upgrade_into()),
            DropClusterReplicaV2(x) => Self::DropClusterReplicaV2(x.upgrade_into()),
            DropClusterReplicaV3(x) => Self::DropClusterReplicaV3(x.upgrade_into()),
            CreateSourceSinkV1(x) => Self::CreateSourceSinkV1(x.upgrade_into()),
            CreateSourceSinkV2(x) => Self::CreateSourceSinkV2(x.upgrade_into()),
            CreateSourceSinkV3(x) => Self::CreateSourceSinkV3(x.upgrade_into()),
            CreateSourceSinkV4(x) => Self::CreateSourceSinkV4(x.upgrade_into()),
            CreateIndexV1(x) => Self::CreateIndexV1(x.upgrade_into()),
            CreateMaterializedViewV1(x) => Self::CreateMaterializedViewV1(x.upgrade_into()),
            AlterSourceSinkV1(x) => Self::AlterSourceSinkV1(x.upgrade_into()),
            AlterSetClusterV1(x) => Self::AlterSetClusterV1(x.upgrade_into()),
            AlterRetainHistoryV1(x) => Self::AlterRetainHistoryV1(JsonCompatible::convert(&x)),
            AlterDefaultPrivilegeV1(x) => Self::AlterDefaultPrivilegeV1(x.upgrade_into()),
            GrantRoleV1(x) => Self::GrantRoleV1(JsonCompatible::convert(&x)),
            GrantRoleV2(x) => Self::GrantRoleV2(JsonCompatible::convert(&x)),
            RevokeRoleV1(x) => Self::RevokeRoleV1(JsonCompatible::convert(&x)),
            RevokeRoleV2(x) => Self::RevokeRoleV2(JsonCompatible::convert(&x)),
            UpdatePrivilegeV1(x) => Self::UpdatePrivilegeV1(JsonCompatible::convert(&x)),
            UpdateOwnerV1(x) => Self::UpdateOwnerV1(JsonCompatible::convert(&x)),
            UpdateItemV1(x) => Self::UpdateItemV1(x.upgrade_into()),
            IdFullNameV1(x) => Self::IdFullNameV1(x.upgrade_into()),
            IdNameV1(x) => Self::IdNameV1(JsonCompatible::convert(&x)),
            RenameClusterV1(x) => Self::RenameClusterV1(JsonCompatible::convert(&x)),
            RenameClusterReplicaV1(x) => Self::RenameClusterReplicaV1(JsonCompatible::convert(&x)),
            RenameItemV1(x) => Self::RenameItemV1(x.upgrade_into()),
            RenameSchemaV1(x) => Self::RenameSchemaV1(JsonCompatible::convert(&x)),
            SchemaV1(x) => Self::SchemaV1(JsonCompatible::convert(&x)),
            SchemaV2(x) => Self::SchemaV2(x.upgrade_into()),
            ToNewIdV1(x) => Self::ToNewIdV1(JsonCompatible::convert(&x)),
            FromPreviousIdV1(x) => Self::FromPreviousIdV1(JsonCompatible::convert(&x)),
            SetV1(x) => Self::SetV1(JsonCompatible::convert(&x)),
            ResetAllV1(_) => Self::ResetAllV1,
            RotateKeysV1(x) => Self::RotateKeysV1(JsonCompatible::convert(&x)),
        }
    }
}

impl UpgradeFrom<v78::audit_log_event_v1::CreateClusterReplicaV1>
    for v79::audit_log_event_v1::CreateClusterReplicaV1
{
    fn upgrade_from(old: v78::audit_log_event_v1::CreateClusterReplicaV1) -> Self {
        Self {
            cluster_id: old.cluster_id,
            cluster_name: old.cluster_name,
            replica_id: old.replica_id.map(|s| s.inner),
            replica_name: old.replica_name,
            logical_size: old.logical_size,
            disk: old.disk,
            billed_as: old.billed_as,
            internal: old.internal,
        }
    }
}

impl UpgradeFrom<v78::audit_log_event_v1::CreateClusterReplicaV2>
    for v79::audit_log_event_v1::CreateClusterReplicaV2
{
    fn upgrade_from(old: v78::audit_log_event_v1::CreateClusterReplicaV2) -> Self {
        Self {
            cluster_id: old.cluster_id,
            cluster_name: old.cluster_name,
            replica_id: old.replica_id.map(|s| s.inner),
            replica_name: old.replica_name,
            logical_size: old.logical_size,
            disk: old.disk,
            billed_as: old.billed_as,
            internal: old.internal,
            reason: old.reason.unwrap().upgrade_into(),
            scheduling_policies: old.scheduling_policies.map(UpgradeInto::upgrade_into),
        }
    }
}

impl UpgradeFrom<v78::audit_log_event_v1::CreateClusterReplicaV3>
    for v79::audit_log_event_v1::CreateClusterReplicaV3
{
    fn upgrade_from(old: v78::audit_log_event_v1::CreateClusterReplicaV3) -> Self {
        Self {
            cluster_id: old.cluster_id,
            cluster_name: old.cluster_name,
            replica_id: old.replica_id.map(|s| s.inner),
            replica_name: old.replica_name,
            logical_size: old.logical_size,
            disk: old.disk,
            billed_as: old.billed_as,
            internal: old.internal,
            reason: old.reason.unwrap().upgrade_into(),
            scheduling_policies: old.scheduling_policies.map(UpgradeInto::upgrade_into),
        }
    }
}

impl UpgradeFrom<v78::audit_log_event_v1::CreateClusterReplicaV4>
    for v79::audit_log_event_v1::CreateClusterReplicaV4
{
    fn upgrade_from(old: v78::audit_log_event_v1::CreateClusterReplicaV4) -> Self {
        Self {
            cluster_id: old.cluster_id,
            cluster_name: old.cluster_name,
            replica_id: old.replica_id.map(|s| s.inner),
            replica_name: old.replica_name,
            logical_size: old.logical_size,
            billed_as: old.billed_as,
            internal: old.internal,
            reason: old.reason.unwrap().upgrade_into(),
            scheduling_policies: old.scheduling_policies.map(UpgradeInto::upgrade_into),
        }
    }
}

impl UpgradeFrom<v78::audit_log_event_v1::DropClusterReplicaV1>
    for v79::audit_log_event_v1::DropClusterReplicaV1
{
    fn upgrade_from(old: v78::audit_log_event_v1::DropClusterReplicaV1) -> Self {
        Self {
            cluster_id: old.cluster_id,
            cluster_name: old.cluster_name,
            replica_id: old.replica_id.map(|s| s.inner),
            replica_name: old.replica_name,
        }
    }
}

impl UpgradeFrom<v78::audit_log_event_v1::DropClusterReplicaV2>
    for v79::audit_log_event_v1::DropClusterReplicaV2
{
    fn upgrade_from(old: v78::audit_log_event_v1::DropClusterReplicaV2) -> Self {
        Self {
            cluster_id: old.cluster_id,
            cluster_name: old.cluster_name,
            replica_id: old.replica_id.map(|s| s.inner),
            replica_name: old.replica_name,
            reason: old.reason.unwrap().upgrade_into(),
            scheduling_policies: old.scheduling_policies.map(UpgradeInto::upgrade_into),
        }
    }
}

impl UpgradeFrom<v78::audit_log_event_v1::DropClusterReplicaV3>
    for v79::audit_log_event_v1::DropClusterReplicaV3
{
    fn upgrade_from(old: v78::audit_log_event_v1::DropClusterReplicaV3) -> Self {
        Self {
            cluster_id: old.cluster_id,
            cluster_name: old.cluster_name,
            replica_id: old.replica_id.map(|s| s.inner),
            replica_name: old.replica_name,
            reason: old.reason.unwrap().upgrade_into(),
            scheduling_policies: old.scheduling_policies.map(UpgradeInto::upgrade_into),
        }
    }
}

impl UpgradeFrom<v78::audit_log_event_v1::CreateSourceSinkV1>
    for v79::audit_log_event_v1::CreateSourceSinkV1
{
    fn upgrade_from(old: v78::audit_log_event_v1::CreateSourceSinkV1) -> Self {
        Self {
            id: old.id,
            name: JsonCompatible::convert(&old.name.unwrap()),
            size: old.size.map(|s| s.inner),
        }
    }
}

impl UpgradeFrom<v78::audit_log_event_v1::CreateSourceSinkV2>
    for v79::audit_log_event_v1::CreateSourceSinkV2
{
    fn upgrade_from(old: v78::audit_log_event_v1::CreateSourceSinkV2) -> Self {
        Self {
            id: old.id,
            name: JsonCompatible::convert(&old.name.unwrap()),
            size: old.size.map(|s| s.inner),
            external_type: old.external_type,
        }
    }
}

impl UpgradeFrom<v78::audit_log_event_v1::CreateSourceSinkV3>
    for v79::audit_log_event_v1::CreateSourceSinkV3
{
    fn upgrade_from(old: v78::audit_log_event_v1::CreateSourceSinkV3) -> Self {
        Self {
            id: old.id,
            name: JsonCompatible::convert(&old.name.unwrap()),
            external_type: old.external_type,
        }
    }
}

impl UpgradeFrom<v78::audit_log_event_v1::CreateSourceSinkV4>
    for v79::audit_log_event_v1::CreateSourceSinkV4
{
    fn upgrade_from(old: v78::audit_log_event_v1::CreateSourceSinkV4) -> Self {
        Self {
            id: old.id,
            cluster_id: old.cluster_id.map(|s| s.inner),
            name: JsonCompatible::convert(&old.name.unwrap()),
            external_type: old.external_type,
        }
    }
}

impl UpgradeFrom<v78::audit_log_event_v1::CreateIndexV1>
    for v79::audit_log_event_v1::CreateIndexV1
{
    fn upgrade_from(old: v78::audit_log_event_v1::CreateIndexV1) -> Self {
        Self {
            id: old.id,
            cluster_id: old.cluster_id,
            name: JsonCompatible::convert(&old.name.unwrap()),
        }
    }
}

impl UpgradeFrom<v78::audit_log_event_v1::CreateMaterializedViewV1>
    for v79::audit_log_event_v1::CreateMaterializedViewV1
{
    fn upgrade_from(old: v78::audit_log_event_v1::CreateMaterializedViewV1) -> Self {
        Self {
            id: old.id,
            cluster_id: old.cluster_id,
            name: JsonCompatible::convert(&old.name.unwrap()),
        }
    }
}

impl UpgradeFrom<v78::audit_log_event_v1::AlterSourceSinkV1>
    for v79::audit_log_event_v1::AlterSourceSinkV1
{
    fn upgrade_from(old: v78::audit_log_event_v1::AlterSourceSinkV1) -> Self {
        Self {
            id: old.id,
            name: JsonCompatible::convert(&old.name.unwrap()),
            old_size: old.old_size.map(|s| s.inner),
            new_size: old.new_size.map(|s| s.inner),
        }
    }
}

impl UpgradeFrom<v78::audit_log_event_v1::AlterSetClusterV1>
    for v79::audit_log_event_v1::AlterSetClusterV1
{
    fn upgrade_from(old: v78::audit_log_event_v1::AlterSetClusterV1) -> Self {
        Self {
            id: old.id,
            name: JsonCompatible::convert(&old.name.unwrap()),
            old_cluster: old.old_cluster.map(|s| s.inner),
            new_cluster: old.new_cluster.map(|s| s.inner),
        }
    }
}

impl UpgradeFrom<v78::audit_log_event_v1::AlterDefaultPrivilegeV1>
    for v79::audit_log_event_v1::AlterDefaultPrivilegeV1
{
    fn upgrade_from(old: v78::audit_log_event_v1::AlterDefaultPrivilegeV1) -> Self {
        Self {
            role_id: old.role_id,
            database_id: old.database_id.map(|s| s.inner),
            schema_id: old.schema_id.map(|s| s.inner),
            grantee_id: old.grantee_id,
            privileges: old.privileges,
        }
    }
}

impl UpgradeFrom<v78::audit_log_event_v1::UpdateItemV1> for v79::audit_log_event_v1::UpdateItemV1 {
    fn upgrade_from(old: v78::audit_log_event_v1::UpdateItemV1) -> Self {
        Self {
            id: old.id,
            name: JsonCompatible::convert(&old.name.unwrap()),
        }
    }
}

impl UpgradeFrom<v78::audit_log_event_v1::IdFullNameV1> for v79::audit_log_event_v1::IdFullNameV1 {
    fn upgrade_from(old: v78::audit_log_event_v1::IdFullNameV1) -> Self {
        Self {
            id: old.id,
            name: JsonCompatible::convert(&old.name.unwrap()),
        }
    }
}

impl UpgradeFrom<v78::audit_log_event_v1::RenameItemV1> for v79::audit_log_event_v1::RenameItemV1 {
    fn upgrade_from(old: v78::audit_log_event_v1::RenameItemV1) -> Self {
        Self {
            id: old.id,
            old_name: JsonCompatible::convert(&old.old_name.unwrap()),
            new_name: JsonCompatible::convert(&old.new_name.unwrap()),
        }
    }
}

impl UpgradeFrom<v78::audit_log_event_v1::SchemaV2> for v79::audit_log_event_v1::SchemaV2 {
    fn upgrade_from(old: v78::audit_log_event_v1::SchemaV2) -> Self {
        Self {
            id: old.id,
            name: old.name,
            database_name: old.database_name.map(|s| s.inner),
        }
    }
}

impl UpgradeFrom<v78::audit_log_event_v1::CreateOrDropClusterReplicaReasonV1>
    for v79::audit_log_event_v1::CreateOrDropClusterReplicaReasonV1
{
    fn upgrade_from(old: v78::audit_log_event_v1::CreateOrDropClusterReplicaReasonV1) -> Self {
        use v78::audit_log_event_v1::create_or_drop_cluster_replica_reason_v1::Reason::*;
        match old.reason.unwrap() {
            Manual(_) => Self::Manual,
            Schedule(_) => Self::Schedule,
            System(_) => Self::System,
        }
    }
}

impl UpgradeFrom<v78::audit_log_event_v1::SchedulingDecisionsWithReasonsV1>
    for v79::audit_log_event_v1::SchedulingDecisionsWithReasonsV1
{
    fn upgrade_from(old: v78::audit_log_event_v1::SchedulingDecisionsWithReasonsV1) -> Self {
        Self {
            on_refresh: old.on_refresh.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::audit_log_event_v1::SchedulingDecisionsWithReasonsV2>
    for v79::audit_log_event_v1::SchedulingDecisionsWithReasonsV2
{
    fn upgrade_from(old: v78::audit_log_event_v1::SchedulingDecisionsWithReasonsV2) -> Self {
        Self {
            on_refresh: old.on_refresh.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::audit_log_event_v1::RefreshDecisionWithReasonV1>
    for v79::audit_log_event_v1::RefreshDecisionWithReasonV1
{
    fn upgrade_from(old: v78::audit_log_event_v1::RefreshDecisionWithReasonV1) -> Self {
        Self {
            objects_needing_refresh: old.objects_needing_refresh,
            rehydration_time_estimate: old.rehydration_time_estimate,
            decision: old.decision.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::audit_log_event_v1::RefreshDecisionWithReasonV2>
    for v79::audit_log_event_v1::RefreshDecisionWithReasonV2
{
    fn upgrade_from(old: v78::audit_log_event_v1::RefreshDecisionWithReasonV2) -> Self {
        Self {
            objects_needing_refresh: old.objects_needing_refresh,
            objects_needing_compaction: old.objects_needing_compaction,
            rehydration_time_estimate: old.rehydration_time_estimate,
            decision: old.decision.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::audit_log_event_v1::refresh_decision_with_reason_v1::Decision>
    for v79::audit_log_event_v1::RefreshDecision
{
    fn upgrade_from(
        old: v78::audit_log_event_v1::refresh_decision_with_reason_v1::Decision,
    ) -> Self {
        use v78::audit_log_event_v1::refresh_decision_with_reason_v1::Decision::*;
        match old {
            On(_) => Self::On,
            Off(_) => Self::Off,
        }
    }
}

impl UpgradeFrom<v78::audit_log_event_v1::refresh_decision_with_reason_v2::Decision>
    for v79::audit_log_event_v1::RefreshDecision
{
    fn upgrade_from(
        old: v78::audit_log_event_v1::refresh_decision_with_reason_v2::Decision,
    ) -> Self {
        use v78::audit_log_event_v1::refresh_decision_with_reason_v2::Decision::*;
        match old {
            On(_) => Self::On,
            Off(_) => Self::Off,
        }
    }
}

// --- State update kind converters ---

impl UpgradeFrom<v78::state_update_kind::Cluster> for v79::Cluster {
    fn upgrade_from(old: v78::state_update_kind::Cluster) -> Self {
        Self {
            key: old.key.unwrap().upgrade_into(),
            value: old.value.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::ClusterKey> for v79::ClusterKey {
    fn upgrade_from(old: v78::ClusterKey) -> Self {
        Self {
            id: old.id.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::ClusterValue> for v79::ClusterValue {
    fn upgrade_from(old: v78::ClusterValue) -> Self {
        Self {
            name: old.name,
            owner_id: old.owner_id.unwrap().upgrade_into(),
            privileges: old
                .privileges
                .into_iter()
                .map(UpgradeInto::upgrade_into)
                .collect(),
            config: old.config.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::ClusterId> for v79::ClusterId {
    fn upgrade_from(old: v78::ClusterId) -> Self {
        use v78::cluster_id::Value::*;
        match old.value.unwrap() {
            System(id) => Self::System(id),
            User(id) => Self::User(id),
        }
    }
}

impl UpgradeFrom<v78::RoleId> for v79::RoleId {
    fn upgrade_from(old: v78::RoleId) -> Self {
        use v78::role_id::Value::*;
        match old.value.unwrap() {
            System(id) => Self::System(id),
            User(id) => Self::User(id),
            Public(_) => Self::Public,
            Predefined(id) => Self::Predefined(id),
        }
    }
}

impl UpgradeFrom<v78::MzAclItem> for v79::MzAclItem {
    fn upgrade_from(old: v78::MzAclItem) -> Self {
        Self {
            grantee: old.grantee.unwrap().upgrade_into(),
            grantor: old.grantor.unwrap().upgrade_into(),
            acl_mode: JsonCompatible::convert(&old.acl_mode.unwrap()),
        }
    }
}

impl UpgradeFrom<v78::ClusterConfig> for v79::ClusterConfig {
    fn upgrade_from(old: v78::ClusterConfig) -> Self {
        Self {
            workload_class: old.workload_class,
            variant: old.variant.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::cluster_config::Variant> for v79::ClusterVariant {
    fn upgrade_from(old: v78::cluster_config::Variant) -> Self {
        use v78::cluster_config::Variant::*;
        match old {
            Unmanaged(_) => Self::Unmanaged,
            Managed(m) => Self::Managed(m.upgrade_into()),
        }
    }
}

impl UpgradeFrom<v78::cluster_config::ManagedCluster> for v79::ManagedCluster {
    fn upgrade_from(old: v78::cluster_config::ManagedCluster) -> Self {
        Self {
            size: old.size,
            replication_factor: old.replication_factor,
            availability_zones: old.availability_zones,
            logging: old.logging.unwrap().upgrade_into(),
            optimizer_feature_overrides: old
                .optimizer_feature_overrides
                .iter()
                .map(JsonCompatible::convert)
                .collect(),
            schedule: old.schedule.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::ReplicaLogging> for v79::ReplicaLogging {
    fn upgrade_from(old: v78::ReplicaLogging) -> Self {
        Self {
            log_logging: old.log_logging,
            interval: old.interval.as_ref().map(JsonCompatible::convert),
        }
    }
}

impl UpgradeFrom<v78::ClusterSchedule> for v79::ClusterSchedule {
    fn upgrade_from(old: v78::ClusterSchedule) -> Self {
        use v78::cluster_schedule::Value::*;
        match old.value.unwrap() {
            Manual(_) => Self::Manual,
            Refresh(r) => Self::Refresh(r.upgrade_into()),
        }
    }
}

impl UpgradeFrom<v78::ClusterScheduleRefreshOptions> for v79::ClusterScheduleRefreshOptions {
    fn upgrade_from(old: v78::ClusterScheduleRefreshOptions) -> Self {
        Self {
            rehydration_time_estimate: JsonCompatible::convert(
                &old.rehydration_time_estimate.unwrap(),
            ),
        }
    }
}

impl UpgradeFrom<v78::state_update_kind::ClusterReplica> for v79::ClusterReplica {
    fn upgrade_from(old: v78::state_update_kind::ClusterReplica) -> Self {
        Self {
            key: old.key.unwrap().upgrade_into(),
            value: old.value.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::ClusterReplicaKey> for v79::ClusterReplicaKey {
    fn upgrade_from(old: v78::ClusterReplicaKey) -> Self {
        Self {
            id: old.id.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::ReplicaId> for v79::ReplicaId {
    fn upgrade_from(old: v78::ReplicaId) -> Self {
        use v78::replica_id::Value::*;
        match old.value.unwrap() {
            System(id) => Self::System(id),
            User(id) => Self::User(id),
        }
    }
}

impl UpgradeFrom<v78::ClusterReplicaValue> for v79::ClusterReplicaValue {
    fn upgrade_from(old: v78::ClusterReplicaValue) -> Self {
        Self {
            cluster_id: old.cluster_id.unwrap().upgrade_into(),
            name: old.name,
            config: old.config.unwrap().upgrade_into(),
            owner_id: old.owner_id.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::ReplicaConfig> for v79::ReplicaConfig {
    fn upgrade_from(old: v78::ReplicaConfig) -> Self {
        Self {
            logging: old.logging.unwrap().upgrade_into(),
            location: old.location.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::replica_config::Location> for v79::ReplicaLocation {
    fn upgrade_from(old: v78::replica_config::Location) -> Self {
        use v78::replica_config::Location::*;
        match old {
            Unmanaged(u) => Self::Unmanaged(JsonCompatible::convert(&u)),
            Managed(m) => Self::Managed(JsonCompatible::convert(&m)),
        }
    }
}

impl UpgradeFrom<v78::state_update_kind::Comment> for v79::Comment {
    fn upgrade_from(old: v78::state_update_kind::Comment) -> Self {
        Self {
            key: old.key.unwrap().upgrade_into(),
            value: JsonCompatible::convert(&old.value.unwrap()),
        }
    }
}

impl UpgradeFrom<v78::CommentKey> for v79::CommentKey {
    fn upgrade_from(old: v78::CommentKey) -> Self {
        Self {
            object: old.object.unwrap().upgrade_into(),
            sub_component: old.sub_component.map(UpgradeInto::upgrade_into),
        }
    }
}

impl UpgradeFrom<v78::comment_key::Object> for v79::CommentObject {
    fn upgrade_from(old: v78::comment_key::Object) -> Self {
        use v78::comment_key::Object::*;
        match old {
            Table(id) => Self::Table(id.upgrade_into()),
            View(id) => Self::View(id.upgrade_into()),
            MaterializedView(id) => Self::MaterializedView(id.upgrade_into()),
            Source(id) => Self::Source(id.upgrade_into()),
            Sink(id) => Self::Sink(id.upgrade_into()),
            Index(id) => Self::Index(id.upgrade_into()),
            Func(id) => Self::Func(id.upgrade_into()),
            Connection(id) => Self::Connection(id.upgrade_into()),
            Type(id) => Self::Type(id.upgrade_into()),
            Secret(id) => Self::Secret(id.upgrade_into()),
            ContinualTask(id) => Self::ContinualTask(id.upgrade_into()),
            Role(id) => Self::Role(id.upgrade_into()),
            Database(id) => Self::Database(id.upgrade_into()),
            Schema(s) => Self::Schema(s.upgrade_into()),
            Cluster(id) => Self::Cluster(id.upgrade_into()),
            ClusterReplica(id) => Self::ClusterReplica(id.upgrade_into()),
            NetworkPolicy(id) => Self::NetworkPolicy(id.upgrade_into()),
        }
    }
}

impl UpgradeFrom<v78::CatalogItemId> for v79::CatalogItemId {
    fn upgrade_from(old: v78::CatalogItemId) -> Self {
        use v78::catalog_item_id::Value::*;
        match old.value.unwrap() {
            System(id) => Self::System(id),
            User(id) => Self::User(id),
            Transient(id) => Self::Transient(id),
            IntrospectionSourceIndex(id) => Self::IntrospectionSourceIndex(id),
        }
    }
}

impl UpgradeFrom<v78::DatabaseId> for v79::DatabaseId {
    fn upgrade_from(old: v78::DatabaseId) -> Self {
        use v78::database_id::Value::*;
        match old.value.unwrap() {
            System(id) => Self::System(id),
            User(id) => Self::User(id),
        }
    }
}

impl UpgradeFrom<v78::ResolvedSchema> for v79::ResolvedSchema {
    fn upgrade_from(old: v78::ResolvedSchema) -> Self {
        Self {
            database: old.database.unwrap().upgrade_into(),
            schema: old.schema.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::ResolvedDatabaseSpecifier> for v79::ResolvedDatabaseSpecifier {
    fn upgrade_from(old: v78::ResolvedDatabaseSpecifier) -> Self {
        use v78::resolved_database_specifier::Spec::*;
        match old.spec.unwrap() {
            Ambient(_) => Self::Ambient,
            Id(id) => Self::Id(id.upgrade_into()),
        }
    }
}

impl UpgradeFrom<v78::SchemaSpecifier> for v79::SchemaSpecifier {
    fn upgrade_from(old: v78::SchemaSpecifier) -> Self {
        use v78::schema_specifier::Spec::*;
        match old.spec.unwrap() {
            Temporary(_) => Self::Temporary,
            Id(id) => Self::Id(id.upgrade_into()),
        }
    }
}

impl UpgradeFrom<v78::SchemaId> for v79::SchemaId {
    fn upgrade_from(old: v78::SchemaId) -> Self {
        use v78::schema_id::Value::*;
        match old.value.unwrap() {
            System(id) => Self::System(id),
            User(id) => Self::User(id),
        }
    }
}

impl UpgradeFrom<v78::ClusterReplicaId> for v79::ClusterReplicaId {
    fn upgrade_from(old: v78::ClusterReplicaId) -> Self {
        Self {
            cluster_id: old.cluster_id.unwrap().upgrade_into(),
            replica_id: old.replica_id.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::NetworkPolicyId> for v79::NetworkPolicyId {
    fn upgrade_from(old: v78::NetworkPolicyId) -> Self {
        use v78::network_policy_id::Value::*;
        match old.value.unwrap() {
            System(id) => Self::System(id),
            User(id) => Self::User(id),
        }
    }
}

impl UpgradeFrom<v78::comment_key::SubComponent> for v79::CommentSubComponent {
    fn upgrade_from(old: v78::comment_key::SubComponent) -> Self {
        use v78::comment_key::SubComponent::*;
        match old {
            ColumnPos(pos) => Self::ColumnPos(pos),
        }
    }
}

impl UpgradeFrom<v78::state_update_kind::Config> for v79::Config {
    fn upgrade_from(old: v78::state_update_kind::Config) -> Self {
        Self {
            key: JsonCompatible::convert(&old.key.unwrap()),
            value: JsonCompatible::convert(&old.value.unwrap()),
        }
    }
}

impl UpgradeFrom<v78::state_update_kind::Database> for v79::Database {
    fn upgrade_from(old: v78::state_update_kind::Database) -> Self {
        Self {
            key: old.key.unwrap().upgrade_into(),
            value: old.value.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::DatabaseKey> for v79::DatabaseKey {
    fn upgrade_from(old: v78::DatabaseKey) -> Self {
        Self {
            id: old.id.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::DatabaseValue> for v79::DatabaseValue {
    fn upgrade_from(old: v78::DatabaseValue) -> Self {
        Self {
            name: old.name,
            owner_id: old.owner_id.unwrap().upgrade_into(),
            privileges: old
                .privileges
                .into_iter()
                .map(UpgradeInto::upgrade_into)
                .collect(),
            oid: old.oid,
        }
    }
}

impl UpgradeFrom<v78::state_update_kind::DefaultPrivileges> for v79::DefaultPrivilege {
    fn upgrade_from(old: v78::state_update_kind::DefaultPrivileges) -> Self {
        Self {
            key: old.key.unwrap().upgrade_into(),
            value: old.value.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::DefaultPrivilegesKey> for v79::DefaultPrivilegeKey {
    fn upgrade_from(old: v78::DefaultPrivilegesKey) -> Self {
        Self {
            role_id: old.role_id.unwrap().upgrade_into(),
            database_id: old.database_id.map(UpgradeInto::upgrade_into),
            schema_id: old.schema_id.map(UpgradeInto::upgrade_into),
            object_type: old.object_type.upgrade_into(),
            grantee: old.grantee.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<i32> for v79::ObjectType {
    fn upgrade_from(value: i32) -> Self {
        match value {
            0 => Self::Unknown,
            1 => Self::Table,
            2 => Self::View,
            3 => Self::MaterializedView,
            4 => Self::Source,
            5 => Self::Sink,
            6 => Self::Index,
            7 => Self::Type,
            8 => Self::Role,
            9 => Self::Cluster,
            10 => Self::ClusterReplica,
            11 => Self::Secret,
            12 => Self::Connection,
            13 => Self::Database,
            14 => Self::Schema,
            15 => Self::Func,
            16 => Self::ContinualTask,
            17 => Self::NetworkPolicy,
            x => panic!("invalid ObjectType: {x}"),
        }
    }
}

impl UpgradeFrom<v78::DefaultPrivilegesValue> for v79::DefaultPrivilegeValue {
    fn upgrade_from(old: v78::DefaultPrivilegesValue) -> Self {
        Self {
            privileges: JsonCompatible::convert(&old.privileges.unwrap()),
        }
    }
}

impl UpgradeFrom<v78::state_update_kind::IdAlloc> for v79::IdAllocator {
    fn upgrade_from(old: v78::state_update_kind::IdAlloc) -> Self {
        Self {
            key: JsonCompatible::convert(&old.key.unwrap()),
            value: JsonCompatible::convert(&old.value.unwrap()),
        }
    }
}

impl UpgradeFrom<v78::state_update_kind::ClusterIntrospectionSourceIndex>
    for v79::IntrospectionSourceIndex
{
    fn upgrade_from(old: v78::state_update_kind::ClusterIntrospectionSourceIndex) -> Self {
        Self {
            key: old.key.unwrap().upgrade_into(),
            value: old.value.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::ClusterIntrospectionSourceIndexKey> for v79::IntrospectionSourceIndexKey {
    fn upgrade_from(old: v78::ClusterIntrospectionSourceIndexKey) -> Self {
        Self {
            cluster_id: old.cluster_id.unwrap().upgrade_into(),
            name: old.name,
        }
    }
}

impl UpgradeFrom<v78::ClusterIntrospectionSourceIndexValue> for v79::IntrospectionSourceIndexValue {
    fn upgrade_from(old: v78::ClusterIntrospectionSourceIndexValue) -> Self {
        Self {
            catalog_id: v79::IntrospectionSourceIndexCatalogItemId(old.index_id),
            global_id: old.global_id.unwrap().upgrade_into(),
            oid: old.oid,
        }
    }
}

impl UpgradeFrom<v78::IntrospectionSourceIndexGlobalId> for v79::IntrospectionSourceIndexGlobalId {
    fn upgrade_from(old: v78::IntrospectionSourceIndexGlobalId) -> Self {
        Self(old.value)
    }
}

impl UpgradeFrom<v78::state_update_kind::Item> for v79::Item {
    fn upgrade_from(old: v78::state_update_kind::Item) -> Self {
        Self {
            key: old.key.unwrap().upgrade_into(),
            value: old.value.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::ItemKey> for v79::ItemKey {
    fn upgrade_from(old: v78::ItemKey) -> Self {
        Self {
            gid: old.gid.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::ItemValue> for v79::ItemValue {
    fn upgrade_from(old: v78::ItemValue) -> Self {
        Self {
            schema_id: old.schema_id.unwrap().upgrade_into(),
            name: old.name,
            definition: old.definition.unwrap().upgrade_into(),
            owner_id: old.owner_id.unwrap().upgrade_into(),
            privileges: old
                .privileges
                .into_iter()
                .map(UpgradeInto::upgrade_into)
                .collect(),
            oid: old.oid,
            global_id: old.global_id.unwrap().upgrade_into(),
            extra_versions: old
                .extra_versions
                .into_iter()
                .map(UpgradeInto::upgrade_into)
                .collect(),
        }
    }
}

impl UpgradeFrom<v78::CatalogItem> for v79::CatalogItem {
    fn upgrade_from(old: v78::CatalogItem) -> Self {
        use v78::catalog_item::Value::*;
        match old.value.unwrap() {
            V1(v) => Self::V1(v79::CatalogItemV1 {
                create_sql: v.create_sql,
            }),
        }
    }
}

impl UpgradeFrom<v78::GlobalId> for v79::GlobalId {
    fn upgrade_from(old: v78::GlobalId) -> Self {
        use v78::global_id::Value::*;
        match old.value.unwrap() {
            System(id) => Self::System(id),
            User(id) => Self::User(id),
            Transient(id) => Self::Transient(id),
            Explain(_) => Self::Explain,
            IntrospectionSourceIndex(id) => Self::IntrospectionSourceIndex(id),
        }
    }
}

impl UpgradeFrom<v78::ItemVersion> for v79::ItemVersion {
    fn upgrade_from(old: v78::ItemVersion) -> Self {
        Self {
            global_id: old.global_id.unwrap().upgrade_into(),
            version: JsonCompatible::convert(&old.version.unwrap()),
        }
    }
}

impl UpgradeFrom<v78::state_update_kind::Role> for v79::Role {
    fn upgrade_from(old: v78::state_update_kind::Role) -> Self {
        Self {
            key: old.key.unwrap().upgrade_into(),
            value: old.value.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::RoleKey> for v79::RoleKey {
    fn upgrade_from(old: v78::RoleKey) -> Self {
        Self {
            id: old.id.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::RoleValue> for v79::RoleValue {
    fn upgrade_from(old: v78::RoleValue) -> Self {
        Self {
            name: old.name,
            attributes: JsonCompatible::convert(&old.attributes.unwrap()),
            membership: old.membership.unwrap().upgrade_into(),
            vars: old.vars.unwrap().upgrade_into(),
            oid: old.oid,
        }
    }
}

impl UpgradeFrom<v78::RoleMembership> for v79::RoleMembership {
    fn upgrade_from(old: v78::RoleMembership) -> Self {
        Self {
            map: old.map.into_iter().map(UpgradeInto::upgrade_into).collect(),
        }
    }
}

impl UpgradeFrom<v78::role_membership::Entry> for v79::RoleMembershipEntry {
    fn upgrade_from(old: v78::role_membership::Entry) -> Self {
        Self {
            key: old.key.unwrap().upgrade_into(),
            value: old.value.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::RoleVars> for v79::RoleVars {
    fn upgrade_from(old: v78::RoleVars) -> Self {
        Self {
            entries: old
                .entries
                .into_iter()
                .map(UpgradeInto::upgrade_into)
                .collect(),
        }
    }
}

impl UpgradeFrom<v78::role_vars::Entry> for v79::RoleVarsEntry {
    fn upgrade_from(old: v78::role_vars::Entry) -> Self {
        Self {
            key: old.key,
            val: old.val.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::role_vars::entry::Val> for v79::RoleVar {
    fn upgrade_from(old: v78::role_vars::entry::Val) -> Self {
        use v78::role_vars::entry::Val::*;
        match old {
            Flat(s) => Self::Flat(s),
            SqlSet(s) => Self::SqlSet(s.entries),
        }
    }
}

impl UpgradeFrom<v78::state_update_kind::Schema> for v79::Schema {
    fn upgrade_from(old: v78::state_update_kind::Schema) -> Self {
        Self {
            key: old.key.unwrap().upgrade_into(),
            value: old.value.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::SchemaKey> for v79::SchemaKey {
    fn upgrade_from(old: v78::SchemaKey) -> Self {
        Self {
            id: old.id.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::SchemaValue> for v79::SchemaValue {
    fn upgrade_from(old: v78::SchemaValue) -> Self {
        Self {
            database_id: old.database_id.map(UpgradeInto::upgrade_into),
            name: old.name,
            owner_id: old.owner_id.unwrap().upgrade_into(),
            privileges: old
                .privileges
                .into_iter()
                .map(UpgradeInto::upgrade_into)
                .collect(),
            oid: old.oid,
        }
    }
}

impl UpgradeFrom<v78::state_update_kind::Setting> for v79::Setting {
    fn upgrade_from(old: v78::state_update_kind::Setting) -> Self {
        Self {
            key: JsonCompatible::convert(&old.key.unwrap()),
            value: JsonCompatible::convert(&old.value.unwrap()),
        }
    }
}

impl UpgradeFrom<v78::state_update_kind::ServerConfiguration> for v79::SystemConfiguration {
    fn upgrade_from(old: v78::state_update_kind::ServerConfiguration) -> Self {
        Self {
            key: JsonCompatible::convert(&old.key.unwrap()),
            value: JsonCompatible::convert(&old.value.unwrap()),
        }
    }
}

impl UpgradeFrom<v78::state_update_kind::GidMapping> for v79::SystemObjectMapping {
    fn upgrade_from(old: v78::state_update_kind::GidMapping) -> Self {
        Self {
            key: old.key.unwrap().upgrade_into(),
            value: old.value.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::GidMappingKey> for v79::SystemObjectMappingKey {
    fn upgrade_from(old: v78::GidMappingKey) -> Self {
        Self {
            schema_name: old.schema_name,
            object_type: old.object_type.upgrade_into(),
            object_name: old.object_name,
        }
    }
}

impl UpgradeFrom<i32> for v79::CatalogItemType {
    fn upgrade_from(value: i32) -> Self {
        match value {
            0 => Self::Unknown,
            1 => Self::Table,
            2 => Self::Source,
            3 => Self::Sink,
            4 => Self::View,
            5 => Self::MaterializedView,
            6 => Self::Index,
            7 => Self::Type,
            8 => Self::Func,
            9 => Self::Secret,
            10 => Self::Connection,
            11 => Self::ContinualTask,
            x => panic!("invalid CatalogItemType: {x}"),
        }
    }
}

impl UpgradeFrom<v78::GidMappingValue> for v79::SystemObjectMappingValue {
    fn upgrade_from(old: v78::GidMappingValue) -> Self {
        Self {
            catalog_id: v79::SystemCatalogItemId(old.id),
            global_id: old.global_id.unwrap().upgrade_into(),
            fingerprint: old.fingerprint,
        }
    }
}

impl UpgradeFrom<v78::SystemGlobalId> for v79::SystemGlobalId {
    fn upgrade_from(old: v78::SystemGlobalId) -> Self {
        Self(old.value)
    }
}

impl UpgradeFrom<v78::state_update_kind::SystemPrivileges> for v79::SystemPrivilege {
    fn upgrade_from(old: v78::state_update_kind::SystemPrivileges) -> Self {
        Self {
            key: old.key.unwrap().upgrade_into(),
            value: old.value.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::SystemPrivilegesKey> for v79::SystemPrivilegeKey {
    fn upgrade_from(old: v78::SystemPrivilegesKey) -> Self {
        Self {
            grantee: old.grantee.unwrap().upgrade_into(),
            grantor: old.grantor.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::SystemPrivilegesValue> for v79::SystemPrivilegeValue {
    fn upgrade_from(old: v78::SystemPrivilegesValue) -> Self {
        Self {
            acl_mode: JsonCompatible::convert(&old.acl_mode.unwrap()),
        }
    }
}

impl UpgradeFrom<v78::state_update_kind::StorageCollectionMetadata>
    for v79::StorageCollectionMetadata
{
    fn upgrade_from(old: v78::state_update_kind::StorageCollectionMetadata) -> Self {
        Self {
            key: old.key.unwrap().upgrade_into(),
            value: JsonCompatible::convert(&old.value.unwrap()),
        }
    }
}

impl UpgradeFrom<v78::StorageCollectionMetadataKey> for v79::StorageCollectionMetadataKey {
    fn upgrade_from(old: v78::StorageCollectionMetadataKey) -> Self {
        Self {
            id: old.id.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::state_update_kind::UnfinalizedShard> for v79::UnfinalizedShard {
    fn upgrade_from(old: v78::state_update_kind::UnfinalizedShard) -> Self {
        Self {
            key: JsonCompatible::convert(&old.key.unwrap()),
        }
    }
}

impl UpgradeFrom<v78::state_update_kind::TxnWalShard> for v79::TxnWalShard {
    fn upgrade_from(old: v78::state_update_kind::TxnWalShard) -> Self {
        Self {
            value: JsonCompatible::convert(&old.value.unwrap()),
        }
    }
}

impl UpgradeFrom<v78::state_update_kind::SourceReferences> for v79::SourceReferences {
    fn upgrade_from(old: v78::state_update_kind::SourceReferences) -> Self {
        Self {
            key: old.key.unwrap().upgrade_into(),
            value: old.value.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::SourceReferencesKey> for v79::SourceReferencesKey {
    fn upgrade_from(old: v78::SourceReferencesKey) -> Self {
        Self {
            source: old.source.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::SourceReferencesValue> for v79::SourceReferencesValue {
    fn upgrade_from(old: v78::SourceReferencesValue) -> Self {
        Self {
            references: old.references.iter().map(JsonCompatible::convert).collect(),
            updated_at: JsonCompatible::convert(&old.updated_at.unwrap()),
        }
    }
}

impl UpgradeFrom<v78::state_update_kind::FenceToken> for v79::FenceToken {
    fn upgrade_from(old: v78::state_update_kind::FenceToken) -> Self {
        JsonCompatible::convert(&old)
    }
}

impl UpgradeFrom<v78::state_update_kind::NetworkPolicy> for v79::NetworkPolicy {
    fn upgrade_from(old: v78::state_update_kind::NetworkPolicy) -> Self {
        Self {
            key: old.key.unwrap().upgrade_into(),
            value: old.value.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::NetworkPolicyKey> for v79::NetworkPolicyKey {
    fn upgrade_from(old: v78::NetworkPolicyKey) -> Self {
        Self {
            id: old.id.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::NetworkPolicyValue> for v79::NetworkPolicyValue {
    fn upgrade_from(old: v78::NetworkPolicyValue) -> Self {
        Self {
            name: old.name,
            rules: old
                .rules
                .into_iter()
                .map(UpgradeInto::upgrade_into)
                .collect(),
            owner_id: old.owner_id.unwrap().upgrade_into(),
            privileges: old
                .privileges
                .into_iter()
                .map(UpgradeInto::upgrade_into)
                .collect(),
            oid: old.oid,
        }
    }
}

impl UpgradeFrom<v78::NetworkPolicyRule> for v79::NetworkPolicyRule {
    fn upgrade_from(old: v78::NetworkPolicyRule) -> Self {
        Self {
            name: old.name,
            address: old.address,
            action: old.action.unwrap().upgrade_into(),
            direction: old.direction.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::network_policy_rule::Action> for v79::NetworkPolicyRuleAction {
    fn upgrade_from(old: v78::network_policy_rule::Action) -> Self {
        use v78::network_policy_rule::Action::*;
        match old {
            Allow(_) => Self::Allow,
        }
    }
}

impl UpgradeFrom<v78::network_policy_rule::Direction> for v79::NetworkPolicyRuleDirection {
    fn upgrade_from(old: v78::network_policy_rule::Direction) -> Self {
        use v78::network_policy_rule::Direction::*;
        match old {
            Ingress(_) => Self::Ingress,
        }
    }
}

impl UpgradeFrom<v78::state_update_kind::RoleAuth> for v79::RoleAuth {
    fn upgrade_from(old: v78::state_update_kind::RoleAuth) -> Self {
        Self {
            key: old.key.unwrap().upgrade_into(),
            value: old.value.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::RoleAuthKey> for v79::RoleAuthKey {
    fn upgrade_from(old: v78::RoleAuthKey) -> Self {
        Self {
            id: old.id.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::RoleAuthValue> for v79::RoleAuthValue {
    fn upgrade_from(old: v78::RoleAuthValue) -> Self {
        Self {
            password_hash: old.password_hash,
            updated_at: JsonCompatible::convert(&old.updated_at.unwrap()),
        }
    }
}
