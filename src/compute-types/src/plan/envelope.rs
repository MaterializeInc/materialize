// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The JSON envelope that carries user-provided LIR plans.
//!
//! An envelope bundles the relations a plan imports, the plan objects, and the id of the
//! output object. Rows inside plans are written in the readable `{"$row": [...]}` form of
//! [`mz_repr::row_to_readable`].

use std::collections::BTreeMap;

use mz_expr::Id;
use mz_repr::{
    CatalogItemId, GlobalId, RelationVersion, StableRow, row_from_readable, with_readable_rows,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::plan::{LIR_VERSION, LirRelationExpr, LirRelationNode};

/// A set of LIR plan objects together with the relations they import.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct LirPlanEnvelope {
    /// Must equal [`LIR_VERSION`].
    pub lir_version: u64,
    /// The relations the objects read from.
    pub imports: Vec<EnvelopeImport>,
    /// The plans, which refer to imports and to each other by `GlobalId`.
    pub objects: Vec<EnvelopeObject>,
    /// The object whose result is the result of the plan.
    pub output: GlobalId,
}

/// A relation imported into the plan.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct EnvelopeImport {
    /// A placeholder id, which plans use in `Get`.
    pub id: GlobalId,
    /// The imported relation.
    pub source: ImportSource,
    /// Whether to read the relation from persist.
    pub persist: bool,
    /// Indexes on the relation to import.
    pub indexes: Vec<ImportSource>,
}

/// Names a catalog item to import.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum ImportSource {
    /// A name as written by users, resolved during purification.
    Name(String),
    /// The resolved item.
    Item {
        /// The catalog item.
        item_id: CatalogItemId,
        /// The version of the item's relation.
        version: RelationVersion,
    },
}

/// A named plan.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct EnvelopeObject {
    /// A placeholder id, which other plans use in `Get`.
    pub id: GlobalId,
    /// The plan.
    pub plan: LirRelationExpr,
}

/// Errors from [`parse_envelope`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EnvelopeError {
    /// The text is not a valid envelope. Carries serde's message, including the path.
    Json(String),
    /// The envelope has an unsupported version.
    Version {
        /// The version in the envelope.
        found: u64,
        /// The supported version.
        expected: u64,
    },
    /// A `{"$row": ...}` object is malformed.
    Row(String),
}

impl std::fmt::Display for EnvelopeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EnvelopeError::Json(msg) => write!(f, "invalid LIR plan envelope: {msg}"),
            EnvelopeError::Version { found, expected } => {
                write!(f, "unsupported lir_version {found}, expected {expected}")
            }
            EnvelopeError::Row(msg) => write!(f, "invalid row in LIR plan envelope: {msg}"),
        }
    }
}

impl std::error::Error for EnvelopeError {}

/// The size of the stack segment that parsing and printing run on. The segment is mapped
/// lazily, so the large size costs address space only for deeply nested input.
const STACK_SEGMENT: usize = 256 * 1024 * 1024;

/// Parses an envelope and checks its version.
///
/// Never panics and does not overflow the stack on deeply nested input. Semantic validation
/// of the plans is the caller's job.
pub fn parse_envelope(json: &str) -> Result<LirPlanEnvelope, EnvelopeError> {
    // serde_json's recursion limit is disabled below, and neither the `Value` deserializer
    // nor the derived impls have one of their own. One large stack segment covers the
    // recursion of all three. NOTE: `grow` rather than `maybe_grow`, because `maybe_grow`
    // stays on the current stack while it has more than the red zone left, and a single
    // deeply nested input overflows that stack.
    stacker::grow(STACK_SEGMENT, || {
        let mut value = {
            let mut de = serde_json::Deserializer::from_str(json);
            de.disable_recursion_limit();
            let value =
                Value::deserialize(&mut de).map_err(|e| EnvelopeError::Json(e.to_string()))?;
            de.end().map_err(|e| EnvelopeError::Json(e.to_string()))?;
            value
        };
        // Check the version first, a different version may have a different shape.
        if let Some(found) = value.get("lir_version").and_then(Value::as_u64) {
            if found != LIR_VERSION {
                return Err(EnvelopeError::Version {
                    found,
                    expected: LIR_VERSION,
                });
            }
        }
        rewrite_rows(&mut value)?;
        serde_json::from_value(value).map_err(|e| EnvelopeError::Json(e.to_string()))
    })
}

/// Replaces every `{"$row": [...]}` object with the default serialization of the decoded
/// `StableRow`, because `StableRow` deserializes only that form.
///
/// Iterative, so nesting depth does not matter.
fn rewrite_rows(root: &mut Value) -> Result<(), EnvelopeError> {
    let mut stack = vec![root];
    while let Some(value) = stack.pop() {
        if value.as_object().is_some_and(|m| m.contains_key("$row")) {
            let row = row_from_readable(value).map_err(EnvelopeError::Row)?;
            *value = serde_json::to_value(StableRow(row))
                .map_err(|e| EnvelopeError::Row(e.to_string()))?;
            continue;
        }
        match value {
            Value::Object(map) => stack.extend(map.values_mut()),
            Value::Array(items) => stack.extend(items.iter_mut()),
            _ => {}
        }
    }
    Ok(())
}

/// Renders an envelope as pretty JSON with rows in their readable form.
///
/// Rows without a readable form are written in the default serialization, which
/// [`parse_envelope`] does not accept back.
pub fn envelope_to_json(envelope: &LirPlanEnvelope) -> String {
    stacker::grow(STACK_SEGMENT, || {
        with_readable_rows(|| serde_json::to_string_pretty(envelope))
            .expect("envelopes serialize to JSON")
    })
}

/// Rewrites the global ids of all `Get`s in `plan` according to `map`.
///
/// Ids not in `map` are left alone.
pub fn substitute_global_ids(plan: &mut LirRelationExpr, map: &BTreeMap<GlobalId, GlobalId>) {
    let mut stack = vec![plan];
    while let Some(expr) = stack.pop() {
        if let LirRelationNode::Get {
            id: Id::Global(id), ..
        } = &mut expr.node
        {
            if let Some(new) = map.get(id) {
                *id = *new;
            }
        }
        stack.extend(expr.node.children_mut());
    }
}

#[cfg(test)]
mod tests;
