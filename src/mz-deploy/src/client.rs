// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Database client layer for communicating with a Materialize region.
//!
//! All interaction with the live database flows through this module. The
//! `Client` type (defined in `connection`) holds a `tokio_postgres`
//! connection and exposes scoped sub-clients that group related operations:
//!
//! - **`introspection`** — Read-only catalog queries: schema/cluster/object
//!   existence checks, dependency lookups, and batch metadata retrieval.
//! - **`provisioning`** — DDL operations that create or alter databases,
//!   schemas, and clusters to match the project definition.
//! - **`deployment_ops`** — Blue/green deployment lifecycle: staging,
//!   hydration monitoring, cutover, and abort.
//! - **`validation`** — Pre-deployment validation: checks that the target
//!   environment matches expected state before applying changes.
//! - **`type_info`** — `SHOW COLUMNS` queries used to generate and refresh
//!   the `types.lock` data-contract file.
//!
//! ## Supporting Submodules
//!
//! - **`models`** — Data structures shared across sub-clients (deployment
//!   records, cluster configs, conflict records, etc.).
//! - **`errors`** — Error types: `ConnectionError` for transport/query
//!   failures, `DatabaseValidationError` for semantic mismatches.
//!
//! Most sub-client types are internal; this module re-exports the key public
//! types so that consumers only need `use crate::client::*`.

mod connection;
mod deployment_ops;
mod dev_overlays;
mod errors;
mod introspection;
mod models;
mod provisioning;
mod type_info;
mod validation;

/// Name of the dedicated cluster mz-deploy creates during `setup` and
/// pins every connection to via libpq options.
pub const SERVER_CLUSTER_NAME: &str = "_mz_deploy_server";

pub use crate::config::Profile;
pub use connection::{Client, DevOverlaysClient};
pub(crate) use connection::{build_options_string, default_sslmode, is_loopback_host};

/// Double-quote a SQL identifier, escaping any embedded double quotes.
pub fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Build a comma-separated `$1, $2, …, $n` placeholder string for parameterized queries.
pub fn sql_placeholders(n: usize) -> String {
    (1..=n)
        .map(|i| format!("${}", i))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Build a `LIKE` pattern (used with `ESCAPE '\'`) matching any name that ends
/// in the staging suffix `_<deploy_id>`.
///
/// The suffix is matched *literally*: `_`, `%`, and the escape character `\` are
/// LIKE metacharacters, so they are escaped. Only the leading `%` stays a
/// wildcard. Without escaping, the `_` separating the suffix would act as a
/// single-character wildcard — pattern `%_prod` would match any name ending in
/// `<any char>prod` (e.g. a production schema `fooprod` or cluster `dataprod`),
/// and a `deploy_id` containing `%` would match nearly everything. Used by both
/// the staging-discovery queries (which feed `DROP ... CASCADE`) and the
/// hydration-status / `wait` readiness queries.
pub(crate) fn staging_suffix_like_pattern(deploy_id: &str) -> String {
    let mut pattern = String::from("%");
    // The literal suffix is the separating underscore followed by the deploy id.
    for ch in std::iter::once('_').chain(deploy_id.chars()) {
        if matches!(ch, '\\' | '_' | '%') {
            pattern.push('\\');
        }
        pattern.push(ch);
    }
    pattern
}

#[cfg(test)]
mod tests {
    use super::staging_suffix_like_pattern;

    #[mz_ore::test]
    fn test_staging_suffix_like_pattern_escapes_separator() {
        // Regression test for QA Finding 3.
        //
        // The `_` separating the staging suffix must be escaped so it matches a
        // literal underscore, not a single-character wildcard. With deploy id
        // `prod` the pattern must be `%\_prod` (used with `ESCAPE '\'`), which
        // matches only names ending in the literal `_prod` — NOT `fooprod` or any
        // other `<char>prod`, which the unescaped `%_prod` would have matched.
        assert_eq!(staging_suffix_like_pattern("prod"), r"%\_prod");
    }

    #[mz_ore::test]
    fn test_staging_suffix_like_pattern_escapes_metacharacters() {
        // A deploy id containing LIKE metacharacters must not inject wildcards.
        // `%` and `_` inside the id are escaped to literals; the only wildcard is
        // the leading `%`.
        assert_eq!(staging_suffix_like_pattern("a%b_c"), r"%\_a\%b\_c");
        // Backslashes (the escape char itself) are also escaped.
        assert_eq!(staging_suffix_like_pattern(r"a\b"), r"%\_a\\b");
    }

    #[mz_ore::test]
    fn test_staging_suffix_like_pattern_plain_id() {
        // A plain alphanumeric id only escapes the separating underscore.
        assert_eq!(staging_suffix_like_pattern("deploy123"), r"%\_deploy123");
    }
}
pub use deployment_ops::{
    ClusterDeploymentStatus, ClusterStatusContext, DEFAULT_ALLOWED_LAG_SECS, FailureReason,
    HydrationStatusUpdate,
};
pub use errors::{ConnectionError, DatabaseValidationError, format_relative_path};
pub use introspection::DependentSink;
pub use models::{
    ApplyState, Cluster, ClusterConfig, ClusterOptions, ClusterReplica, ConflictRecord,
    DeploymentDetails, DeploymentHistoryEntry, DeploymentKind, DeploymentMetadata, DeploymentMode,
    DeploymentObjectRecord, ObjectGrant, PendingStatement, ProductionClusterRecord,
    ReplacementMvRecord, SchemaDeploymentRecord, StagingDeployment,
};
