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
//! in that file's header, which owns the rule semantics.
//!
//! ## Field relationships
//!
//! - `changed_objects` ⊆ `objects_to_deploy` — a directly changed object is
//!   always deployed, and `objects_to_deploy` additionally holds everything
//!   pulled in by dependency, cluster, or schema propagation.
//! - `stage_objects`, `stage_sinks`, and `stage_replacement_mvs` partition the
//!   deployable part of `objects_to_deploy`. The remainder is the
//!   apply-managed objects, counted by `apply_managed_count`, and any deleted
//!   object, which has no project statement to deploy.
//! - `schemas_to_create` and `clusters_to_create` cover
//!   `stage_objects ∪ stage_replacement_mvs`. They describe resources to
//!   provision, so they are wider than the relations that propagate
//!   dirtiness inside the fixed point: a cluster or schema reached only by
//!   propagation still needs creating.

use crate::project::SchemaQualifier;
use crate::project::ir::object_id::ObjectId;
use std::collections::BTreeSet;
use std::fmt::{Display, Formatter};

/// Represents the set of changes between two project states.
#[derive(Debug, Clone, Default)]
pub(crate) struct ChangeSet {
    /// Objects whose content hash differs between the two snapshots.
    pub changed_objects: BTreeSet<ObjectId>,

    /// Schemas that propagate dirtiness to the objects they contain.
    pub dirty_schemas: BTreeSet<SchemaQualifier>,

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

        if !self.dirty_schemas.is_empty() {
            writeln!(f, "Dirty schemas:")?;
            for sq in &self.dirty_schemas {
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
