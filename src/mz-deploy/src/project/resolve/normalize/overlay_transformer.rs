// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Name transformation for `mz-deploy dev` overlay compilation.
//!
//! This module provides [`OverlayTransformer`], which implements the
//! two-step reference resolution rule for schema-level overlays:
//!
//! 1. **External references** — if the database is not in
//!    `in_project_databases`, emit the name verbatim.
//! 2. **Dirty schemas** — if `(database, schema)` is in `dirty_schemas`,
//!    rewrite the database component to `<database>__<profile_name>`.
//!    Otherwise emit `<database>.<schema>.<object>` (production reference).
//!
//! Unqualified / partially qualified names are fully qualified using the
//! transformer's `fqn` context before the rule is applied.
//!
//! The project planner has already applied any configured `profile_suffix`
//! to database and cluster names before `dev` invokes the transformer, so
//! no suffix handling is required here.

use std::collections::BTreeSet;

use mz_sql_parser::ast::{Ident, UnresolvedItemName};

use crate::project::SchemaQualifier;
use crate::project::ir::compiled::FullyQualifiedName;
use crate::project::resolve::normalize::transformers::{ClusterTransformer, NameTransformer};
use mz_repr::namespaces::is_system_schema;

/// Transforms references for `mz-deploy dev` overlay compilation.
///
/// Applies the two-step reference resolution rule:
///
/// 1. If the referenced database is not in `in_project_databases`, leave
///    the name verbatim (external dependency).
/// 2. If `(database, schema)` is in `dirty_schemas`, rewrite the database
///    component to `<database>__<profile_name>`. Otherwise emit
///    `<database>.<schema>.<object>` (production reference).
///
/// Unqualified / partially qualified names are fully qualified using
/// `fqn` before the rule is applied.
pub(crate) struct OverlayTransformer<'a> {
    pub(crate) fqn: &'a FullyQualifiedName,
    pub(crate) profile_name: &'a str,
    pub(crate) in_project_databases: &'a BTreeSet<String>,
    pub(crate) dirty_schemas: &'a BTreeSet<SchemaQualifier>,
    pub(crate) target_cluster: &'a str,
}

impl<'a> NameTransformer for OverlayTransformer<'a> {
    fn transform_name(&self, name: &UnresolvedItemName) -> UnresolvedItemName {
        // System catalog references are database-less and aren't part of any
        // project; leave them verbatim so the server resolves them natively.
        if name.0.len() == 2 && is_system_schema(name.0[0].as_str()) {
            return name.clone();
        }

        // Normalize to 3-part name first
        let (database, schema, object) = match name.0.len() {
            1 => {
                // Unqualified: use fqn database + schema
                let database = Ident::new(self.fqn.database()).expect("valid database identifier");
                let schema = Ident::new(self.fqn.schema()).expect("valid schema identifier");
                let object = name.0[0].clone();
                (database, schema, object)
            }
            2 => {
                // Schema-qualified: prepend fqn database
                let database = Ident::new(self.fqn.database()).expect("valid database identifier");
                let schema = name.0[0].clone();
                let object = name.0[1].clone();
                (database, schema, object)
            }
            3 => {
                // Already fully qualified
                let database = name.0[0].clone();
                let schema = name.0[1].clone();
                let object = name.0[2].clone();
                (database, schema, object)
            }
            _ => {
                // Invalid — return as-is (matches FullyQualifyingTransformer behavior)
                return name.clone();
            }
        };

        let db_str = database.to_string();

        // Step 1: external check — leave verbatim if not in project.
        if !self.in_project_databases.contains(&db_str) {
            return UnresolvedItemName(vec![database, schema, object]);
        }

        // Step 2: dirty check — rewrite to overlay db if dirty, else prod.
        let qualifier = SchemaQualifier::new(db_str.clone(), schema.to_string());
        let final_db_str = if self.dirty_schemas.contains(&qualifier) {
            format!("{}__{}", db_str, self.profile_name)
        } else {
            db_str
        };

        let final_db = Ident::new(&final_db_str).expect("valid database identifier");
        UnresolvedItemName(vec![final_db, schema, object])
    }

    fn database_name(&self) -> &str {
        self.fqn.database()
    }
}

impl<'a> ClusterTransformer for OverlayTransformer<'a> {
    fn transform_cluster(&self, _: &Ident) -> Ident {
        Ident::new(self.target_cluster).expect("valid cluster identifier")
    }

    fn get_original_cluster_name(&self, name: &str) -> String {
        name.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::project::ir::compiled::FullyQualifiedName;
    use crate::project::ir::object_id::ObjectId;

    fn make_fqn(database: &str, schema: &str, object: &str) -> FullyQualifiedName {
        ObjectId::new(database.to_string(), schema.to_string(), object.to_string()).into()
    }

    fn make_name(parts: &[&str]) -> UnresolvedItemName {
        UnresolvedItemName(
            parts
                .iter()
                .map(|s| Ident::new(*s).expect("valid identifier"))
                .collect(),
        )
    }

    /// Build an OverlayTransformer for tests that need in-project dbs.
    fn make_transformer<'a>(
        fqn: &'a FullyQualifiedName,
        profile_name: &'a str,
        in_project_databases: &'a BTreeSet<String>,
        dirty_schemas: &'a BTreeSet<SchemaQualifier>,
    ) -> OverlayTransformer<'a> {
        OverlayTransformer {
            fqn,
            profile_name,
            in_project_databases,
            dirty_schemas,
            target_cluster: "quickstart_dev",
        }
    }

    // External reference: database not in in_project_databases → verbatim.
    #[test]
    fn external_reference_unchanged() {
        let fqn = make_fqn("mydb", "public", "ctx");
        let in_project = BTreeSet::from(["mydb".to_string()]);
        let dirty: BTreeSet<SchemaQualifier> = BTreeSet::new();
        let t = make_transformer(&fqn, "alice", &in_project, &dirty);

        let input = make_name(&["external_db", "analytics", "events"]);
        let result = t.transform_name(&input);

        assert_eq!(result.0[0].as_str(), "external_db");
        assert_eq!(result.0[1].as_str(), "analytics");
        assert_eq!(result.0[2].as_str(), "events");
    }

    // In-project DB, clean schema → unchanged 3-part name.
    #[test]
    fn in_project_clean_schema_routes_to_prod() {
        let fqn = make_fqn("mydb", "public", "ctx");
        let in_project = BTreeSet::from(["mydb".to_string()]);
        let dirty: BTreeSet<SchemaQualifier> = BTreeSet::new();
        let t = make_transformer(&fqn, "alice", &in_project, &dirty);

        let input = make_name(&["mydb", "public", "orders"]);
        let result = t.transform_name(&input);

        assert_eq!(result.0[0].as_str(), "mydb");
        assert_eq!(result.0[1].as_str(), "public");
        assert_eq!(result.0[2].as_str(), "orders");
    }

    // In-project DB, schema IS dirty → db becomes db__profile.
    #[test]
    fn in_project_dirty_schema_routes_to_overlay() {
        let fqn = make_fqn("mydb", "public", "ctx");
        let in_project = BTreeSet::from(["mydb".to_string()]);
        let dirty = BTreeSet::from([SchemaQualifier::new(
            "mydb".to_string(),
            "public".to_string(),
        )]);
        let t = make_transformer(&fqn, "alice", &in_project, &dirty);

        let input = make_name(&["mydb", "public", "orders"]);
        let result = t.transform_name(&input);

        assert_eq!(result.0[0].as_str(), "mydb__alice");
        assert_eq!(result.0[1].as_str(), "public");
        assert_eq!(result.0[2].as_str(), "orders");
    }

    // Sparse overlay: in-project DB that has SOME schema dirty, but this
    // reference targets a non-dirty schema → routes to prod (not overlay).
    #[test]
    fn in_project_dirty_db_with_non_dirty_schema() {
        let fqn = make_fqn("mydb", "public", "ctx");
        let in_project = BTreeSet::from(["mydb".to_string()]);
        // "mydb.analytics" is dirty, but NOT "mydb.public"
        let dirty = BTreeSet::from([SchemaQualifier::new(
            "mydb".to_string(),
            "analytics".to_string(),
        )]);
        let t = make_transformer(&fqn, "alice", &in_project, &dirty);

        let input = make_name(&["mydb", "public", "orders"]);
        let result = t.transform_name(&input);

        // "public" is not dirty → production reference, no overlay rewrite
        assert_eq!(result.0[0].as_str(), "mydb");
        assert_eq!(result.0[1].as_str(), "public");
        assert_eq!(result.0[2].as_str(), "orders");
    }

    // Unqualified (1-part) name: fqn database + schema used, then
    // routed to overlay if schema is dirty.
    #[test]
    fn unqualified_name_resolved_via_fqn_then_routed_to_overlay() {
        let fqn = make_fqn("mydb", "public", "ctx");
        let in_project = BTreeSet::from(["mydb".to_string()]);
        let dirty = BTreeSet::from([SchemaQualifier::new(
            "mydb".to_string(),
            "public".to_string(),
        )]);
        let t = make_transformer(&fqn, "alice", &in_project, &dirty);

        // 1-part: just "orders"
        let input = make_name(&["orders"]);
        let result = t.transform_name(&input);

        assert_eq!(result.0[0].as_str(), "mydb__alice");
        assert_eq!(result.0[1].as_str(), "public");
        assert_eq!(result.0[2].as_str(), "orders");
    }

    // Cluster rewrite: any input cluster name → target_cluster.
    #[test]
    fn transform_cluster_rewrites_to_target() {
        let fqn = make_fqn("mydb", "public", "ctx");
        let in_project = BTreeSet::from(["mydb".to_string()]);
        let dirty: BTreeSet<SchemaQualifier> = BTreeSet::new();
        let t = make_transformer(&fqn, "alice", &in_project, &dirty);

        let input = Ident::new("prod").expect("valid identifier");
        let out = t.transform_cluster(&input);
        assert_eq!(out.as_str(), "quickstart_dev");

        let input2 = Ident::new("anything_else").expect("valid identifier");
        let out2 = t.transform_cluster(&input2);
        assert_eq!(out2.as_str(), "quickstart_dev");
    }

    // Schema-qualified (2-part) name: fqn database prepended, then
    // routed to overlay if the explicit schema is dirty.
    #[test]
    fn schema_qualified_name_resolved_via_fqn_then_routed_to_overlay() {
        let fqn = make_fqn("mydb", "public", "ctx");
        let in_project = BTreeSet::from(["mydb".to_string()]);
        let dirty = BTreeSet::from([SchemaQualifier::new(
            "mydb".to_string(),
            "analytics".to_string(),
        )]);
        let t = make_transformer(&fqn, "alice", &in_project, &dirty);

        // 2-part: "analytics.summary" — fqn database prepended
        let input = make_name(&["analytics", "summary"]);
        let result = t.transform_name(&input);

        assert_eq!(result.0[0].as_str(), "mydb__alice");
        assert_eq!(result.0[1].as_str(), "analytics");
        assert_eq!(result.0[2].as_str(), "summary");
    }
}
