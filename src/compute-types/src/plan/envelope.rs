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

/// The deepest JSON nesting [`parse_envelope`] accepts, counting every array and object.
///
/// Parsing, deserializing, and dropping recurse once per level, so this bounds their stack
/// use. A nested `Negate` operator costs three levels, so the cap allows roughly 1300 of them.
pub const MAX_JSON_DEPTH: usize = 4096;

/// The size of the stack segment that parsing and printing run on. The segment is mapped
/// lazily. Parsing [`MAX_JSON_DEPTH`] levels in an unoptimized build stays well below this
/// size, which the depth tests exercise.
const STACK_SEGMENT: usize = 64 * 1024 * 1024;

/// Reports whether `json` nests arrays and objects deeper than [`MAX_JSON_DEPTH`].
///
/// Iterative, and skips string contents so that brackets inside strings do not count.
/// Malformed input is not diagnosed here, the parser does that.
fn exceeds_max_depth(json: &str) -> bool {
    let (mut depth, mut in_string, mut escaped) = (0usize, false, false);
    for byte in json.bytes() {
        if in_string {
            match (escaped, byte) {
                (true, _) => escaped = false,
                (false, b'\\') => escaped = true,
                (false, b'"') => in_string = false,
                _ => {}
            }
            continue;
        }
        match byte {
            b'"' => in_string = true,
            b'[' | b'{' => {
                depth += 1;
                if depth > MAX_JSON_DEPTH {
                    return true;
                }
            }
            b']' | b'}' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    false
}

/// Parses an envelope and checks its version.
///
/// Never panics. Input nested deeper than [`MAX_JSON_DEPTH`] is rejected with
/// [`EnvelopeError::Json`] before parsing, so the returned envelope is bounded in depth and
/// dropping it on an ordinary thread stack is safe. Semantic validation of the plans is the
/// caller's job.
pub fn parse_envelope(json: &str) -> Result<LirPlanEnvelope, EnvelopeError> {
    if exceeds_max_depth(json) {
        return Err(EnvelopeError::Json(format!(
            "nesting is deeper than the maximum of {MAX_JSON_DEPTH} levels"
        )));
    }
    // serde_json's recursion limit is disabled below, and neither the `Value` deserializer
    // nor the derived impls have one of their own. The depth check above bounds all three.
    // NOTE: `grow` rather than `maybe_grow`, because `maybe_grow` stays on the current stack
    // while it has more than the red zone left.
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
