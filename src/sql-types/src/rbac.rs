// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Leaf role-based access control helpers.
//!
//! These are pure `const fn`s that compute default privileges from object types.
//! They live here, rather than in `mz_sql::rbac`, so that lower-level crates can
//! depend on them without pulling in the planner. `mz_sql::rbac` re-exports them.

use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem};
use mz_repr::role_id::RoleId;

use crate::catalog::{ObjectType, SystemObjectType};
use crate::session::user::{MZ_SUPPORT_ROLE_ID, MZ_SYSTEM_ROLE_ID};

pub const fn all_object_privileges(object_type: SystemObjectType) -> AclMode {
    const TABLE_ACL_MODE: AclMode = AclMode::INSERT
        .union(AclMode::SELECT)
        .union(AclMode::UPDATE)
        .union(AclMode::DELETE);
    const USAGE_CREATE_ACL_MODE: AclMode = AclMode::USAGE.union(AclMode::CREATE);
    const ALL_SYSTEM_PRIVILEGES: AclMode = AclMode::CREATE_ROLE
        .union(AclMode::CREATE_DB)
        .union(AclMode::CREATE_CLUSTER)
        .union(AclMode::CREATE_NETWORK_POLICY);

    const EMPTY_ACL_MODE: AclMode = AclMode::empty();
    match object_type {
        SystemObjectType::Object(ObjectType::Table) => TABLE_ACL_MODE,
        SystemObjectType::Object(ObjectType::View) => AclMode::SELECT,
        SystemObjectType::Object(ObjectType::MaterializedView) => AclMode::SELECT,
        SystemObjectType::Object(ObjectType::Source) => AclMode::SELECT,
        SystemObjectType::Object(ObjectType::Sink) => EMPTY_ACL_MODE,
        SystemObjectType::Object(ObjectType::Index) => EMPTY_ACL_MODE,
        SystemObjectType::Object(ObjectType::Type) => AclMode::USAGE,
        SystemObjectType::Object(ObjectType::Role) => EMPTY_ACL_MODE,
        SystemObjectType::Object(ObjectType::Cluster) => USAGE_CREATE_ACL_MODE,
        SystemObjectType::Object(ObjectType::ClusterReplica) => EMPTY_ACL_MODE,
        SystemObjectType::Object(ObjectType::Secret) => AclMode::USAGE,
        SystemObjectType::Object(ObjectType::NetworkPolicy) => AclMode::USAGE,
        SystemObjectType::Object(ObjectType::Connection) => AclMode::USAGE,
        SystemObjectType::Object(ObjectType::Database) => USAGE_CREATE_ACL_MODE,
        SystemObjectType::Object(ObjectType::Schema) => USAGE_CREATE_ACL_MODE,
        SystemObjectType::Object(ObjectType::Func) => EMPTY_ACL_MODE,
        SystemObjectType::System => ALL_SYSTEM_PRIVILEGES,
    }
}

pub const fn owner_privilege(object_type: ObjectType, owner_id: RoleId) -> MzAclItem {
    MzAclItem {
        grantee: owner_id,
        grantor: owner_id,
        acl_mode: all_object_privileges(SystemObjectType::Object(object_type)),
    }
}

const fn default_builtin_object_acl_mode(object_type: ObjectType) -> AclMode {
    match object_type {
        ObjectType::Table
        | ObjectType::View
        | ObjectType::MaterializedView
        | ObjectType::Source => AclMode::SELECT,
        ObjectType::Type | ObjectType::Schema => AclMode::USAGE,
        ObjectType::Sink
        | ObjectType::Index
        | ObjectType::Role
        | ObjectType::Cluster
        | ObjectType::ClusterReplica
        | ObjectType::Secret
        | ObjectType::Connection
        | ObjectType::Database
        | ObjectType::Func
        | ObjectType::NetworkPolicy => AclMode::empty(),
    }
}

pub const fn support_builtin_object_privilege(object_type: ObjectType) -> MzAclItem {
    let acl_mode = default_builtin_object_acl_mode(object_type);
    MzAclItem {
        grantee: MZ_SUPPORT_ROLE_ID,
        grantor: MZ_SYSTEM_ROLE_ID,
        acl_mode,
    }
}

pub const fn default_builtin_object_privilege(object_type: ObjectType) -> MzAclItem {
    let acl_mode = default_builtin_object_acl_mode(object_type);
    MzAclItem {
        grantee: RoleId::Public,
        grantor: MZ_SYSTEM_ROLE_ID,
        acl_mode,
    }
}
