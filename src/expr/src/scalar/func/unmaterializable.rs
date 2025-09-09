// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
//
// Portions of this file are derived from the PostgreSQL project. The original
// source code is subject to the terms of the PostgreSQL license, a copy of
// which can be found in the LICENSE file at the root of this repository.

//! Unmaterializable functions.
//!
//! The definitions are placeholders and cannot be evaluated directly.
//! Evaluation is handled directly within the `mz-adapter` crate.

use std::fmt;

use mz_lowertest::MzReflect;
use mz_proto::{RustType, TryFromProtoError};
use mz_repr::{ColumnType, ScalarType};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use crate::scalar::ProtoUnmaterializableFunc;

#[derive(
    Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect,
)]
pub enum UnmaterializableFunc {
    CurrentDatabase,
    CurrentSchema,
    CurrentSchemasWithSystem,
    CurrentSchemasWithoutSystem,
    CurrentTimestamp,
    CurrentUser,
    IsRbacEnabled,
    MzEnvironmentId,
    MzIsSuperuser,
    MzNow,
    MzRoleOidMemberships,
    MzSessionId,
    MzUptime,
    MzVersion,
    MzVersionNum,
    PgBackendPid,
    PgPostmasterStartTime,
    SessionUser,
    Version,
    ViewableVariables,
}

impl UnmaterializableFunc {
    pub fn output_type(&self) -> ColumnType {
        match self {
            UnmaterializableFunc::CurrentDatabase => ScalarType::String.nullable(false),
            // TODO: The `CurrentSchema` function should return `name`. This is
            // tricky in Materialize because `name` truncates to 63 characters
            // but Materialize does not have a limit on identifier length.
            UnmaterializableFunc::CurrentSchema => ScalarType::String.nullable(true),
            // TODO: The `CurrentSchemas` function should return `name[]`. This
            // is tricky in Materialize because `name` truncates to 63
            // characters but Materialize does not have a limit on identifier
            // length.
            UnmaterializableFunc::CurrentSchemasWithSystem => {
                ScalarType::Array(Box::new(ScalarType::String)).nullable(false)
            }
            UnmaterializableFunc::CurrentSchemasWithoutSystem => {
                ScalarType::Array(Box::new(ScalarType::String)).nullable(false)
            }
            UnmaterializableFunc::CurrentTimestamp => {
                ScalarType::TimestampTz { precision: None }.nullable(false)
            }
            UnmaterializableFunc::CurrentUser => ScalarType::String.nullable(false),
            UnmaterializableFunc::IsRbacEnabled => ScalarType::Bool.nullable(false),
            UnmaterializableFunc::MzEnvironmentId => ScalarType::String.nullable(false),
            UnmaterializableFunc::MzIsSuperuser => ScalarType::Bool.nullable(false),
            UnmaterializableFunc::MzNow => ScalarType::MzTimestamp.nullable(false),
            UnmaterializableFunc::MzRoleOidMemberships => ScalarType::Map {
                value_type: Box::new(ScalarType::Array(Box::new(ScalarType::String))),
                custom_id: None,
            }
            .nullable(false),
            UnmaterializableFunc::MzSessionId => ScalarType::Uuid.nullable(false),
            UnmaterializableFunc::MzUptime => ScalarType::Interval.nullable(true),
            UnmaterializableFunc::MzVersion => ScalarType::String.nullable(false),
            UnmaterializableFunc::MzVersionNum => ScalarType::Int32.nullable(false),
            UnmaterializableFunc::PgBackendPid => ScalarType::Int32.nullable(false),
            UnmaterializableFunc::PgPostmasterStartTime => {
                ScalarType::TimestampTz { precision: None }.nullable(false)
            }
            UnmaterializableFunc::SessionUser => ScalarType::String.nullable(false),
            UnmaterializableFunc::Version => ScalarType::String.nullable(false),
            UnmaterializableFunc::ViewableVariables => ScalarType::Map {
                value_type: Box::new(ScalarType::String),
                custom_id: None,
            }
            .nullable(false),
        }
    }
}

impl fmt::Display for UnmaterializableFunc {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            UnmaterializableFunc::CurrentDatabase => f.write_str("current_database"),
            UnmaterializableFunc::CurrentSchema => f.write_str("current_schema"),
            UnmaterializableFunc::CurrentSchemasWithSystem => f.write_str("current_schemas(true)"),
            UnmaterializableFunc::CurrentSchemasWithoutSystem => {
                f.write_str("current_schemas(false)")
            }
            UnmaterializableFunc::CurrentTimestamp => f.write_str("current_timestamp"),
            UnmaterializableFunc::CurrentUser => f.write_str("current_user"),
            UnmaterializableFunc::IsRbacEnabled => f.write_str("is_rbac_enabled"),
            UnmaterializableFunc::MzEnvironmentId => f.write_str("mz_environment_id"),
            UnmaterializableFunc::MzIsSuperuser => f.write_str("mz_is_superuser"),
            UnmaterializableFunc::MzNow => f.write_str("mz_now"),
            UnmaterializableFunc::MzRoleOidMemberships => f.write_str("mz_role_oid_memberships"),
            UnmaterializableFunc::MzSessionId => f.write_str("mz_session_id"),
            UnmaterializableFunc::MzUptime => f.write_str("mz_uptime"),
            UnmaterializableFunc::MzVersion => f.write_str("mz_version"),
            UnmaterializableFunc::MzVersionNum => f.write_str("mz_version_num"),
            UnmaterializableFunc::PgBackendPid => f.write_str("pg_backend_pid"),
            UnmaterializableFunc::PgPostmasterStartTime => f.write_str("pg_postmaster_start_time"),
            UnmaterializableFunc::SessionUser => f.write_str("session_user"),
            UnmaterializableFunc::Version => f.write_str("version"),
            UnmaterializableFunc::ViewableVariables => f.write_str("viewable_variables"),
        }
    }
}

impl RustType<ProtoUnmaterializableFunc> for UnmaterializableFunc {
    fn into_proto(&self) -> ProtoUnmaterializableFunc {
        use crate::scalar::proto_unmaterializable_func::Kind::*;
        let kind = match self {
            UnmaterializableFunc::CurrentDatabase => CurrentDatabase(()),
            UnmaterializableFunc::CurrentSchema => CurrentSchema(()),
            UnmaterializableFunc::CurrentSchemasWithSystem => CurrentSchemasWithSystem(()),
            UnmaterializableFunc::CurrentSchemasWithoutSystem => CurrentSchemasWithoutSystem(()),
            UnmaterializableFunc::ViewableVariables => CurrentSetting(()),
            UnmaterializableFunc::CurrentTimestamp => CurrentTimestamp(()),
            UnmaterializableFunc::CurrentUser => CurrentUser(()),
            UnmaterializableFunc::IsRbacEnabled => IsRbacEnabled(()),
            UnmaterializableFunc::MzEnvironmentId => MzEnvironmentId(()),
            UnmaterializableFunc::MzIsSuperuser => MzIsSuperuser(()),
            UnmaterializableFunc::MzNow => MzNow(()),
            UnmaterializableFunc::MzRoleOidMemberships => MzRoleOidMemberships(()),
            UnmaterializableFunc::MzSessionId => MzSessionId(()),
            UnmaterializableFunc::MzUptime => MzUptime(()),
            UnmaterializableFunc::MzVersion => MzVersion(()),
            UnmaterializableFunc::MzVersionNum => MzVersionNum(()),
            UnmaterializableFunc::PgBackendPid => PgBackendPid(()),
            UnmaterializableFunc::PgPostmasterStartTime => PgPostmasterStartTime(()),
            UnmaterializableFunc::SessionUser => SessionUser(()),
            UnmaterializableFunc::Version => Version(()),
        };
        ProtoUnmaterializableFunc { kind: Some(kind) }
    }

    fn from_proto(proto: ProtoUnmaterializableFunc) -> Result<Self, TryFromProtoError> {
        use crate::scalar::proto_unmaterializable_func::Kind::*;
        if let Some(kind) = proto.kind {
            match kind {
                CurrentDatabase(()) => Ok(UnmaterializableFunc::CurrentDatabase),
                CurrentSchema(()) => Ok(UnmaterializableFunc::CurrentSchema),
                CurrentSchemasWithSystem(()) => Ok(UnmaterializableFunc::CurrentSchemasWithSystem),
                CurrentSchemasWithoutSystem(()) => {
                    Ok(UnmaterializableFunc::CurrentSchemasWithoutSystem)
                }
                CurrentTimestamp(()) => Ok(UnmaterializableFunc::CurrentTimestamp),
                CurrentSetting(()) => Ok(UnmaterializableFunc::ViewableVariables),
                CurrentUser(()) => Ok(UnmaterializableFunc::CurrentUser),
                IsRbacEnabled(()) => Ok(UnmaterializableFunc::IsRbacEnabled),
                MzEnvironmentId(()) => Ok(UnmaterializableFunc::MzEnvironmentId),
                MzIsSuperuser(()) => Ok(UnmaterializableFunc::MzIsSuperuser),
                MzNow(()) => Ok(UnmaterializableFunc::MzNow),
                MzRoleOidMemberships(()) => Ok(UnmaterializableFunc::MzRoleOidMemberships),
                MzSessionId(()) => Ok(UnmaterializableFunc::MzSessionId),
                MzUptime(()) => Ok(UnmaterializableFunc::MzUptime),
                MzVersion(()) => Ok(UnmaterializableFunc::MzVersion),
                MzVersionNum(()) => Ok(UnmaterializableFunc::MzVersionNum),
                PgBackendPid(()) => Ok(UnmaterializableFunc::PgBackendPid),
                PgPostmasterStartTime(()) => Ok(UnmaterializableFunc::PgPostmasterStartTime),
                SessionUser(()) => Ok(UnmaterializableFunc::SessionUser),
                Version(()) => Ok(UnmaterializableFunc::Version),
            }
        } else {
            Err(TryFromProtoError::missing_field(
                "`ProtoUnmaterializableFunc::kind`",
            ))
        }
    }
}
