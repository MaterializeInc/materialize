// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Core `ChangeSet` type and its display formatting.
//!
//! A `ChangeSet` is the decoded result of `dirty_propagation.sql`: everything
//! `stage` needs to decide what to redeploy, which resources to create, and
//! how to route each object. The rules that produce each field are documented
//! in that file.
//!
//! ## Field relationships
//!
//! [`ChangeSet::check_invariants`] enforces these on every decoded result.
//!
//! - `changed_objects` ⊆ `objects_to_deploy`. A directly changed object is
//!   always deployed, and `objects_to_deploy` additionally holds everything
//!   pulled in by dependency, cluster, or schema propagation.
//! - `stage_objects`, `stage_sinks`, and `stage_replacement_mvs` are disjoint
//!   subsets of `objects_to_deploy`. The remainder is the apply-managed
//!   objects, counted by `apply_managed_count`, and any deleted object, which
//!   has no project statement to deploy.
//! - `new_replacement_objects` ⊆ `objects_to_deploy` and is disjoint from
//!   `stage_replacement_mvs`.
//! - `schemas_to_create` is exactly the schemas of
//!   `stage_objects ∪ stage_replacement_mvs`, and `clusters_to_create` is the
//!   clusters those objects and their indexes use. They describe resources to
//!   provision, so they are wider than the relations that propagate
//!   dirtiness inside the fixed point: a cluster or schema reached only by
//!   propagation still needs creating. The cluster side is not checked,
//!   because a `ChangeSet` carries no cluster assignments.

use crate::project::SchemaQualifier;
use crate::project::ir::object_id::ObjectId;
use std::collections::BTreeSet;
use std::fmt::{Display, Formatter};

/// Represents the set of changes between two project states.
#[derive(Debug, Clone, Default)]
pub(crate) struct ChangeSet {
    /// Objects whose content hash differs between the two snapshots.
    pub changed_objects: BTreeSet<ObjectId>,

    /// Every object reached by the fixed point, including sinks, apply-managed
    /// objects, and objects deleted from the project.
    pub objects_to_deploy: BTreeSet<ObjectId>,

    /// Views and materialized views `stage` creates in the staging schemas.
    pub stage_objects: BTreeSet<ObjectId>,

    /// Sinks, which `apply` creates after the swap.
    pub stage_sinks: BTreeSet<ObjectId>,

    /// Materialized views deployed through the `CREATE REPLACEMENT` protocol.
    pub stage_replacement_mvs: BTreeSet<ObjectId>,

    /// Schemas `stage` creates and later swaps.
    pub schemas_to_create: BTreeSet<SchemaQualifier>,

    /// Clusters `stage` creates, including those used only by indexes.
    pub clusters_to_create: BTreeSet<String>,

    /// Dirty replacement MVs that deploy by blue-green swap rather than by the
    /// replacement protocol, because they are new to production or their
    /// production schema was not already of replacement kind.
    pub new_replacement_objects: BTreeSet<ObjectId>,

    /// Dirty objects that `apply` owns and `stage` skips.
    pub apply_managed_count: usize,
}

impl ChangeSet {
    /// Check if any changes were detected.
    pub(crate) fn is_empty(&self) -> bool {
        self.objects_to_deploy.is_empty()
    }

    /// Check the field relationships in the module docs, naming the first one
    /// that fails.
    pub(crate) fn check_invariants(&self) -> Result<(), String> {
        let subset = |name: &str, set: &BTreeSet<ObjectId>| match set
            .difference(&self.objects_to_deploy)
            .next()
        {
            Some(id) => Err(format!(
                "{name} holds {id}, which is not in objects_to_deploy"
            )),
            None => Ok(()),
        };
        let disjoint = |a: &str, x: &BTreeSet<ObjectId>, b: &str, y: &BTreeSet<ObjectId>| match x
            .intersection(y)
            .next()
        {
            Some(id) => Err(format!("{id} is in both {a} and {b}")),
            None => Ok(()),
        };

        subset("changed_objects", &self.changed_objects)?;
        subset("stage_objects", &self.stage_objects)?;
        subset("stage_sinks", &self.stage_sinks)?;
        subset("stage_replacement_mvs", &self.stage_replacement_mvs)?;
        subset("new_replacement_objects", &self.new_replacement_objects)?;

        disjoint(
            "stage_objects",
            &self.stage_objects,
            "stage_sinks",
            &self.stage_sinks,
        )?;
        disjoint(
            "stage_objects",
            &self.stage_objects,
            "stage_replacement_mvs",
            &self.stage_replacement_mvs,
        )?;
        disjoint(
            "stage_sinks",
            &self.stage_sinks,
            "stage_replacement_mvs",
            &self.stage_replacement_mvs,
        )?;
        disjoint(
            "new_replacement_objects",
            &self.new_replacement_objects,
            "stage_replacement_mvs",
            &self.stage_replacement_mvs,
        )?;

        let staged = self.stage_objects.len()
            + self.stage_sinks.len()
            + self.stage_replacement_mvs.len()
            + self.apply_managed_count;
        if staged > self.objects_to_deploy.len() {
            return Err(format!(
                "{staged} staged and apply-managed objects exceed {} objects_to_deploy",
                self.objects_to_deploy.len()
            ));
        }

        let staged_schemas: BTreeSet<SchemaQualifier> = self
            .stage_objects
            .iter()
            .chain(&self.stage_replacement_mvs)
            .map(|id| {
                SchemaQualifier::new(
                    id.database().unwrap_or("").to_string(),
                    id.schema().to_string(),
                )
            })
            .collect();
        if staged_schemas != self.schemas_to_create {
            return Err(
                "schemas_to_create is not the set of schemas of the staged objects".to_string(),
            );
        }

        Ok(())
    }
}

impl Display for ChangeSet {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "Incremental deployment: {} objects need redeployment",
            self.objects_to_deploy.len()
        )?;

        if !self.changed_objects.is_empty() {
            writeln!(f, "Changed objects:")?;
            for obj in &self.changed_objects {
                writeln!(f, "  - {}", obj)?;
            }
        }

        if !self.schemas_to_create.is_empty() {
            writeln!(f, "Schemas to create:")?;
            for sq in &self.schemas_to_create {
                writeln!(f, "  - {}.{}", sq.database, sq.schema)?;
            }
        }

        if !self.clusters_to_create.is_empty() {
            writeln!(f, "Clusters to create:")?;
            for cluster in &self.clusters_to_create {
                writeln!(f, "  - {}", cluster)?;
            }
        }

        if !self.objects_to_deploy.is_empty() {
            writeln!(f, "Objects to deploy:")?;
            for obj in &self.objects_to_deploy {
                writeln!(f, "  - {}", obj)?;
            }
        }

        Ok(())
    }
}
