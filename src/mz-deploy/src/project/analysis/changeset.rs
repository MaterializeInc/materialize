// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Change detection for incremental deployment.
//!
//! Determines which objects, schemas, and clusters need redeployment after a
//! project changes, by running the dirty propagation rules in Materialize and
//! decoding the result into a [`ChangeSet`].
//!
//! The rules are a Datalog fixed point expressed as a `WITH MUTUALLY
//! RECURSIVE` query in `changeset/dirty_propagation.sql`. That file's header
//! is the owning documentation for the rule semantics, the fact contract, and
//! why each rule is shaped the way it is. This module only loads facts, runs
//! the query, and decodes its single jsonb row.
//!
//! Running the fixed point server-side keeps one description of the rules
//! rather than a SQL one and a Rust one that can drift, and `stage` already
//! holds a session to Materialize before any of this runs.

mod facts;
mod types;

pub(crate) use types::ChangeSet;

use crate::client::{Client, ConnectionError};
use crate::project::SchemaQualifier;
use crate::project::analysis::deployment_snapshot::DeploymentSnapshot;
use crate::project::ir::graph::Project;
use crate::project::ir::object_id::ObjectId;
use serde_json::Value;
use std::collections::BTreeSet;

/// The dirty propagation fixed point. Reads the tables `facts::load` creates.
const DIRTY_PROPAGATION: &str = include_str!("changeset/dirty_propagation.sql");

impl ChangeSet {
    /// Compute the change set by comparing two deployment snapshots.
    ///
    /// An empty `old_snapshot` yields a full deployment: every object reads as
    /// added, so no separate first-deploy path is needed.
    ///
    /// `forced_dirty_schemas` seeds the fixed point with schemas the caller
    /// redeploys unconditionally, such as `stage --redeploy-schema`. Pass an
    /// empty set for pure change-driven dirtiness.
    pub(crate) async fn compute(
        client: &Client,
        project: &Project,
        old_snapshot: &DeploymentSnapshot,
        new_snapshot: &DeploymentSnapshot,
        forced_dirty_schemas: &BTreeSet<SchemaQualifier>,
    ) -> Result<Self, ConnectionError> {
        facts::load(
            client,
            project,
            old_snapshot,
            new_snapshot,
            forced_dirty_schemas,
        )
        .await?;

        let row = client.query_one(DIRTY_PROPAGATION, &[]).await?;
        let plan: Value = row.try_get(0).map_err(ConnectionError::from)?;
        decode(&plan)
    }
}

fn malformed(key: &str) -> ConnectionError {
    ConnectionError::Message(format!(
        "dirty propagation returned no usable `{}` field",
        key
    ))
}

fn array<'a>(plan: &'a Value, key: &str) -> Result<&'a Vec<Value>, ConnectionError> {
    plan.get(key)
        .and_then(Value::as_array)
        .ok_or_else(|| malformed(key))
}

fn field<'a>(entry: &'a Value, key: &str, outer: &str) -> Result<&'a str, ConnectionError> {
    entry
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| malformed(outer))
}

/// Rebuild object ids from their three raw name components.
///
/// An empty database marks a system-schema object. The query never emits one,
/// since such objects reach it only as dependency parents, but decoding them
/// keeps the round trip total.
fn object_ids(plan: &Value, key: &str) -> Result<BTreeSet<ObjectId>, ConnectionError> {
    array(plan, key)?
        .iter()
        .map(|entry| {
            let database = field(entry, "database", key)?;
            let schema = field(entry, "schema", key)?.to_string();
            let object = field(entry, "object", key)?.to_string();
            Ok(if database.is_empty() {
                ObjectId::new_system(schema, object)
            } else {
                ObjectId::new(database.to_string(), schema, object)
            })
        })
        .collect()
}

fn schema_qualifiers(
    plan: &Value,
    key: &str,
) -> Result<BTreeSet<SchemaQualifier>, ConnectionError> {
    array(plan, key)?
        .iter()
        .map(|entry| {
            Ok(SchemaQualifier::new(
                field(entry, "database", key)?.to_string(),
                field(entry, "schema", key)?.to_string(),
            ))
        })
        .collect()
}

fn names(plan: &Value, key: &str) -> Result<BTreeSet<String>, ConnectionError> {
    array(plan, key)?
        .iter()
        .map(|entry| {
            entry
                .as_str()
                .map(str::to_string)
                .ok_or_else(|| malformed(key))
        })
        .collect()
}

fn decode(plan: &Value) -> Result<ChangeSet, ConnectionError> {
    Ok(ChangeSet {
        changed_objects: object_ids(plan, "changed_objects")?,
        dirty_schemas: schema_qualifiers(plan, "dirty_schemas")?,
        objects_to_deploy: object_ids(plan, "objects_to_deploy")?,
        stage_objects: object_ids(plan, "stage_objects")?,
        stage_sinks: object_ids(plan, "stage_sinks")?,
        stage_replacement_mvs: object_ids(plan, "stage_replacement_mvs")?,
        schemas_to_create: schema_qualifiers(plan, "schemas_to_create")?,
        clusters_to_create: names(plan, "clusters_to_create")?,
        new_replacement_objects: object_ids(plan, "replacement_objects_new")?,
        apply_managed_count: usize::try_from(
            plan.get("apply_managed_count")
                .and_then(Value::as_u64)
                .ok_or_else(|| malformed("apply_managed_count"))?,
        )
        .map_err(|_| malformed("apply_managed_count"))?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn full_plan() -> Value {
        json!({
            "changed_objects": [
                {"database": "db", "schema": "core", "object": "orders"}
            ],
            "objects_to_deploy": [
                {"database": "db", "schema": "core", "object": "orders"},
                {"database": "db", "schema": "core", "object": "orders_sink"}
            ],
            "dirty_schemas": [{"database": "db", "schema": "core"}],
            "schemas_to_create": [{"database": "db", "schema": "core"}],
            "clusters_to_create": ["ingest", "serve"],
            "stage_objects": [{"database": "db", "schema": "core", "object": "orders"}],
            "stage_replacement_mvs": [],
            "stage_sinks": [{"database": "db", "schema": "core", "object": "orders_sink"}],
            "replacement_objects_new": [],
            "apply_managed_count": 2
        })
    }

    #[mz_ore::test]
    fn decodes_every_field() {
        let cs = decode(&full_plan()).unwrap();
        assert_eq!(cs.changed_objects.len(), 1);
        assert_eq!(cs.objects_to_deploy.len(), 2);
        assert!(!cs.is_empty());
        assert_eq!(
            cs.clusters_to_create,
            BTreeSet::from(["ingest".to_string(), "serve".to_string()])
        );
        assert_eq!(cs.apply_managed_count, 2);
        assert!(cs.stage_replacement_mvs.is_empty());
        assert_eq!(
            cs.stage_sinks.iter().next().unwrap().object(),
            "orders_sink"
        );
    }

    #[mz_ore::test]
    fn decodes_an_empty_plan() {
        let mut plan = full_plan();
        for (_, value) in plan.as_object_mut().unwrap().iter_mut() {
            *value = if value.is_array() {
                json!([])
            } else {
                json!(0)
            };
        }
        let cs = decode(&plan).unwrap();
        assert!(cs.is_empty());
        assert_eq!(cs.apply_managed_count, 0);
    }

    #[mz_ore::test]
    fn a_missing_field_is_an_error() {
        let mut plan = full_plan();
        plan.as_object_mut().unwrap().remove("clusters_to_create");
        assert!(decode(&plan).is_err());
    }
}
