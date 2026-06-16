// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Leaf session/user role-ID and user-name constants.
//!
//! These live here so that lower-level crates (e.g. the catalog) can reference the
//! built-in role IDs and user names without depending on the full `mz-sql`.
//! `mz_sql::session::user` re-exports them at their original paths.

use mz_repr::role_id::RoleId;

pub const SYSTEM_USER_NAME: &str = "mz_system";
pub const SUPPORT_USER_NAME: &str = "mz_support";
pub const ANALYTICS_USER_NAME: &str = "mz_analytics";

pub const MZ_SYSTEM_ROLE_ID: RoleId = RoleId::System(1);
pub const MZ_SUPPORT_ROLE_ID: RoleId = RoleId::System(2);
pub const MZ_ANALYTICS_ROLE_ID: RoleId = RoleId::System(3);
/// Sentinel role ID for JWT group-sync-managed role memberships.
/// Not a login role — exists only to distinguish sync grants from manual grants.
pub const MZ_JWT_SYNC_ROLE_ID: RoleId = RoleId::System(4);
pub const JWT_SYNC_ROLE_NAME: &str = "mz_jwt_sync";
pub const MZ_MONITOR_ROLE_ID: RoleId = RoleId::Predefined(1);
pub const MZ_MONITOR_REDACTED_ROLE_ID: RoleId = RoleId::Predefined(2);
