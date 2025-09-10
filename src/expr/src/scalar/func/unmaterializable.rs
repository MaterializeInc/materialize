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
use mz_repr::{SqlColumnType, SqlScalarType};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

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
    pub fn output_type(&self) -> SqlColumnType {
        match self {
            UnmaterializableFunc::CurrentDatabase => SqlScalarType::String.nullable(false),
            // TODO: The `CurrentSchema` function should return `name`. This is
            // tricky in Materialize because `name` truncates to 63 characters
            // but Materialize does not have a limit on identifier length.
            UnmaterializableFunc::CurrentSchema => SqlScalarType::String.nullable(true),
            // TODO: The `CurrentSchemas` function should return `name[]`. This
            // is tricky in Materialize because `name` truncates to 63
            // characters but Materialize does not have a limit on identifier
            // length.
            UnmaterializableFunc::CurrentSchemasWithSystem => {
                SqlScalarType::Array(Box::new(SqlScalarType::String)).nullable(false)
            }
            UnmaterializableFunc::CurrentSchemasWithoutSystem => {
                SqlScalarType::Array(Box::new(SqlScalarType::String)).nullable(false)
            }
            UnmaterializableFunc::CurrentTimestamp => {
                SqlScalarType::TimestampTz { precision: None }.nullable(false)
            }
            UnmaterializableFunc::CurrentUser => SqlScalarType::String.nullable(false),
            UnmaterializableFunc::IsRbacEnabled => SqlScalarType::Bool.nullable(false),
            UnmaterializableFunc::MzEnvironmentId => SqlScalarType::String.nullable(false),
            UnmaterializableFunc::MzIsSuperuser => SqlScalarType::Bool.nullable(false),
            UnmaterializableFunc::MzNow => SqlScalarType::MzTimestamp.nullable(false),
            UnmaterializableFunc::MzRoleOidMemberships => SqlScalarType::Map {
                value_type: Box::new(SqlScalarType::Array(Box::new(SqlScalarType::String))),
                custom_id: None,
            }
            .nullable(false),
            UnmaterializableFunc::MzSessionId => SqlScalarType::Uuid.nullable(false),
            UnmaterializableFunc::MzUptime => SqlScalarType::Interval.nullable(true),
            UnmaterializableFunc::MzVersion => SqlScalarType::String.nullable(false),
            UnmaterializableFunc::MzVersionNum => SqlScalarType::Int32.nullable(false),
            UnmaterializableFunc::PgBackendPid => SqlScalarType::Int32.nullable(false),
            UnmaterializableFunc::PgPostmasterStartTime => {
                SqlScalarType::TimestampTz { precision: None }.nullable(false)
            }
            UnmaterializableFunc::SessionUser => SqlScalarType::String.nullable(false),
            UnmaterializableFunc::Version => SqlScalarType::String.nullable(false),
            UnmaterializableFunc::ViewableVariables => SqlScalarType::Map {
                value_type: Box::new(SqlScalarType::String),
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
