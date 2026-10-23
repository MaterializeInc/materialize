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

use std::collections::{BTreeMap, BTreeSet};

use itertools::Itertools;
use mz_expr::{Id, MapFilterProject, MfpPlan, MirScalarExpr, SafeMfpPlan};
use mz_ore::str::separated;
use mz_repr::{
    CatalogItemId, GlobalId, RelationVersion, StableRow, row_from_readable, with_readable_rows,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::dataflows::LirDataflowDescription;
use crate::plan::scalar::LirScalarExpr;
use crate::plan::{
    AvailableCollections, GetPlan, LIR_VERSION, LirId, LirRelationExpr, LirRelationNode,
};

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
    /// A SQL item name, resolved during purification. It is parsed with SQL identifier rules,
    /// so unquoted parts fold to lowercase and quoted parts are taken verbatim.
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
fn exceeds_max_depth(json: &str) -> bool {
    json_depth(json) > MAX_JSON_DEPTH
}

/// The deepest nesting of arrays and objects in `json`.
///
/// Iterative, and skips string contents so that brackets inside strings do not count.
/// Malformed input is not diagnosed here, the parser does that.
fn json_depth(json: &str) -> usize {
    let (mut depth, mut max, mut in_string, mut escaped) = (0usize, 0usize, false, false);
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
                max = max.max(depth);
            }
            b']' | b'}' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    max
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
/// [`parse_envelope`] accepts as well.
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

/// Extracts the envelope of `df`, whose result is the built object `output`.
///
/// Placeholders are `GlobalId::Transient(1..)`, assigned to the imported relations in id
/// order, then to the objects. `name_of` names imported relations and indexes. One import
/// stands for a relation's persist import and all its index imports. A source import with
/// non-identity operators becomes a leading `Mfp` object over the raw import, and the plans
/// read that object instead of the import.
pub fn extract_envelope(
    df: &LirDataflowDescription,
    output: GlobalId,
    name_of: &dyn Fn(GlobalId) -> String,
) -> Result<LirPlanEnvelope, String> {
    if !df.objects_to_build.iter().any(|build| build.id == output) {
        return Err(format!("output {output} is not an object of the dataflow"));
    }
    let mut next = 0;
    let mut fresh = || {
        next += 1;
        GlobalId::Transient(next)
    };
    // Original ids to placeholders.
    let mut placeholders = BTreeMap::new();

    let relations: BTreeSet<_> = df
        .source_imports
        .keys()
        .copied()
        .chain(df.index_imports.values().map(|import| import.desc.on_id))
        .collect();
    let mut imports = Vec::with_capacity(relations.len());
    for relation in relations {
        let id = fresh();
        placeholders.insert(relation, id);
        let indexes = df
            .index_imports
            .iter()
            .filter(|(_, import)| import.desc.on_id == relation)
            .map(|(index, _)| ImportSource::Name(name_of(*index)))
            .collect();
        imports.push(EnvelopeImport {
            id,
            source: ImportSource::Name(name_of(relation)),
            persist: df.source_imports.contains_key(&relation),
            indexes,
        });
    }

    // Fresh LIR ids for the pushdown objects, so that LIR ids stay unique in the dataflow.
    let mut next_lir_id = df
        .objects_to_build
        .iter()
        .map(|build| max_lir_id(&build.plan))
        .max()
        .unwrap_or(0);
    let mut fresh_lir_id = || {
        next_lir_id = next_lir_id
            .checked_add(1)
            .ok_or_else(|| "LIR ids are exhausted".to_string())?;
        Ok::<_, String>(LirId(next_lir_id))
    };
    let mut objects = Vec::with_capacity(df.objects_to_build.len());
    for (relation, import) in &df.source_imports {
        let Some(operators) = &import.desc.arguments.operators else {
            continue;
        };
        if operators.is_identity() {
            continue;
        }
        // A `Get` of the relation reads its index imports too, which the pushdown object
        // would hide.
        if df
            .index_imports
            .values()
            .any(|index| index.desc.on_id == *relation)
        {
            return Err(format!(
                "{relation} has both pushed-down source operators and index imports"
            ));
        }
        let mfp =
            pushdown_plan(operators).map_err(|e| format!("source operators of {relation}: {e}"))?;
        let get = LirRelationExpr {
            lir_id: fresh_lir_id()?,
            node: LirRelationNode::Get {
                id: Id::Global(placeholders[relation]),
                keys: AvailableCollections::new_raw(),
                plan: GetPlan::PassArrangements,
            },
        };
        let plan = LirRelationExpr {
            lir_id: fresh_lir_id()?,
            node: LirRelationNode::Mfp {
                input: Box::new(get),
                mfp,
                input_key_val: None,
            },
        };
        let id = fresh();
        // From here on, `Get`s of the relation read the pushdown object.
        placeholders.insert(*relation, id);
        objects.push(EnvelopeObject { id, plan });
    }

    for build in &df.objects_to_build {
        placeholders.insert(build.id, fresh());
    }
    for build in &df.objects_to_build {
        let mut plan = build.plan.clone();
        substitute_global_ids(&mut plan, &placeholders);
        objects.push(EnvelopeObject {
            id: placeholders[&build.id],
            plan,
        });
    }

    Ok(LirPlanEnvelope {
        lir_version: LIR_VERSION,
        imports,
        objects,
        output: placeholders[&output],
    })
}

/// The largest LIR id in `plan`.
fn max_lir_id(plan: &LirRelationExpr) -> u64 {
    let mut max = 0;
    let mut stack = vec![plan];
    while let Some(expr) = stack.pop() {
        max = max.max(expr.lir_id.0);
        stack.extend(expr.node.children());
    }
    max
}

/// Plans source operators the way rendering does, and translates the plan to LIR.
///
/// Planning extracts temporal predicates into bounds first, so operators with `mz_now()`
/// translate as long as it only appears in such predicates.
fn pushdown_plan(
    operators: &MapFilterProject<MirScalarExpr>,
) -> Result<MfpPlan<LirScalarExpr>, String> {
    let (safe, lower, upper) = operators.clone().into_plan()?.into_parts();
    let translate_one = |expr: &MirScalarExpr| {
        LirScalarExpr::try_from(expr)
            .map_err(|funcs| format!("LIR cannot express {}", separated(", ", funcs)))
    };
    let translate = |exprs: &[MirScalarExpr]| {
        exprs
            .iter()
            .map(translate_one)
            .collect::<Result<Vec<_>, _>>()
    };
    let mfp = safe.into_mfp();
    let predicates = mfp
        .predicates
        .iter()
        .map(|(position, predicate)| Ok((*position, translate_one(predicate)?)))
        .collect::<Result<Vec<_>, String>>()?;
    let mfp = MapFilterProject {
        expressions: translate(&mfp.expressions)?,
        predicates,
        projection: mfp.projection,
        input_arity: mfp.input_arity,
    };
    Ok(MfpPlan::from_parts(
        SafeMfpPlan::from_mfp(mfp),
        translate(&lower)?,
        translate(&upper)?,
    ))
}

/// Checks that extracting `df`, printing and parsing the envelope, and mapping its
/// placeholders back restores the imports and objects of `df`, up to the pushdown rewrite of
/// [`extract_envelope`].
///
/// Names in the envelope are the ids' display strings. The last built object is the output.
pub(crate) fn check_extract_round_trip(df: &LirDataflowDescription) -> Result<(), String> {
    let Some(output) = df.objects_to_build.last() else {
        return Ok(());
    };
    let envelope = extract_envelope(df, output.id, &|id| id.to_string())?;
    let json = envelope_to_json(&envelope);
    let parsed = parse_envelope(&json).map_err(|e| {
        format!(
            "{e}, the printed envelope nests {} levels",
            json_depth(&json)
        )
    })?;
    if parsed != envelope {
        return Err(format!(
            "parsing the printed envelope does not restore it: {}",
            first_difference(&envelope, &parsed)
        ));
    }
    restore_and_compare(df, output.id, &parsed)
}

/// Names the first part where `parsed` differs from `printed`.
fn first_difference(printed: &LirPlanEnvelope, parsed: &LirPlanEnvelope) -> String {
    // `zip_eq` panics on different lengths.
    if printed.imports.len() != parsed.imports.len()
        || printed.objects.len() != parsed.objects.len()
    {
        return format!(
            "{} imports and {} objects became {} and {}",
            printed.imports.len(),
            printed.objects.len(),
            parsed.imports.len(),
            parsed.objects.len()
        );
    }
    if let Some((a, b)) = printed
        .imports
        .iter()
        .zip_eq(&parsed.imports)
        .find(|(a, b)| a != b)
    {
        return format!("import {}: {a:?} != {b:?}", a.id);
    }
    if let Some((a, b)) = printed
        .objects
        .iter()
        .zip_eq(&parsed.objects)
        .find(|(a, b)| a != b)
    {
        return format!("object {}:\n{:?}\n!=\n{:?}", a.id, a.plan, b.plan);
    }
    format!(
        "version {} or output {} became version {} or output {}",
        printed.lir_version, printed.output, parsed.lir_version, parsed.output
    )
}

/// Maps the placeholders of `envelope`, extracted from `df` with `output` and display-string
/// names, back to the ids of `df`, and compares the result to `df`.
fn restore_and_compare(
    df: &LirDataflowDescription,
    output: GlobalId,
    envelope: &LirPlanEnvelope,
) -> Result<(), String> {
    let resolve = |source: &ImportSource| match source {
        ImportSource::Name(name) => name
            .parse::<GlobalId>()
            .map_err(|e| format!("import name {name}: {e}")),
        ImportSource::Item { .. } => Err(format!("unexpected resolved import {source:?}")),
    };
    // Placeholders to original ids.
    let mut originals = BTreeMap::new();
    fn bind(
        originals: &mut BTreeMap<GlobalId, GlobalId>,
        placeholder: GlobalId,
        original: GlobalId,
    ) -> Result<(), String> {
        match originals.insert(placeholder, original) {
            None => Ok(()),
            Some(_) => Err(format!("placeholder {placeholder} is bound twice")),
        }
    }

    let mut persist = BTreeSet::new();
    let mut indexes = BTreeMap::new();
    for import in &envelope.imports {
        let relation = resolve(&import.source)?;
        if !import.persist && import.indexes.is_empty() {
            return Err(format!(
                "import {} of {relation} reads neither persist nor an index",
                import.id
            ));
        }
        bind(&mut originals, import.id, relation)?;
        if import.persist {
            persist.insert(relation);
        }
        for index in &import.indexes {
            indexes.insert(resolve(index)?, relation);
        }
    }
    let expected: BTreeSet<_> = df.source_imports.keys().copied().collect();
    if persist != expected {
        return Err(format!(
            "persist imports {persist:?} differ from the source imports {expected:?}"
        ));
    }
    let expected: BTreeMap<_, _> = df
        .index_imports
        .iter()
        .map(|(index, import)| (*index, import.desc.on_id))
        .collect();
    if indexes != expected {
        return Err(format!(
            "index imports {indexes:?} differ from the original index imports {expected:?}"
        ));
    }

    let pushdowns = envelope
        .objects
        .len()
        .checked_sub(df.objects_to_build.len())
        .ok_or_else(|| "the envelope has fewer objects than the dataflow".to_string())?;
    let (pushdown_objects, objects) = envelope.objects.split_at(pushdowns);
    let mut pushed = BTreeSet::new();
    // Placeholders of the raw imports that pushdown objects read.
    let mut raw_imports = BTreeSet::new();
    for object in pushdown_objects {
        let LirRelationNode::Mfp {
            input,
            mfp,
            input_key_val: None,
        } = &object.plan.node
        else {
            return Err(format!("pushdown object {} is not an Mfp", object.id));
        };
        let LirRelationNode::Get {
            id: Id::Global(placeholder),
            plan: GetPlan::PassArrangements,
            ..
        } = &input.node
        else {
            return Err(format!(
                "pushdown object {} does not read an import",
                object.id
            ));
        };
        let relation = *originals
            .get(placeholder)
            .ok_or_else(|| format!("pushdown object {} reads {placeholder}", object.id))?;
        let operators = df
            .source_imports
            .get(&relation)
            .and_then(|import| import.desc.arguments.operators.as_ref())
            .ok_or_else(|| format!("{relation} has no source operators"))?;
        if *mfp != pushdown_plan(operators)? {
            return Err(format!(
                "the pushdown of {relation} differs from its operators"
            ));
        }
        pushed.insert(relation);
        raw_imports.insert(*placeholder);
        bind(&mut originals, object.id, relation)?;
    }
    let expected: BTreeSet<_> = df
        .source_imports
        .iter()
        .filter(|(_, import)| {
            import
                .desc
                .arguments
                .operators
                .as_ref()
                .is_some_and(|operators| !operators.is_identity())
        })
        .map(|(id, _)| *id)
        .collect();
    if pushed != expected {
        return Err(format!(
            "pushdown objects for {pushed:?} differ from the sources with operators {expected:?}"
        ));
    }

    // The raw import and the pushdown object both map back to the relation, so comparing
    // plans cannot tell a `Get` that skips the pushdown from one that reads it.
    for object in objects {
        let mut stack = vec![&object.plan];
        while let Some(expr) = stack.pop() {
            if let LirRelationNode::Get {
                id: Id::Global(id), ..
            } = &expr.node
            {
                if raw_imports.contains(id) {
                    return Err(format!(
                        "object {} reads the raw import {id} of a pushed-down source",
                        object.id
                    ));
                }
            }
            stack.extend(expr.node.children());
        }
    }
    for (index, object) in objects.iter().enumerate() {
        bind(&mut originals, object.id, df.objects_to_build[index].id)?;
    }
    for (index, object) in objects.iter().enumerate() {
        let original = &df.objects_to_build[index];
        let mut plan = object.plan.clone();
        substitute_global_ids(&mut plan, &originals);
        if plan != original.plan {
            return Err(format!(
                "object {} differs after the round trip:\n{:?}\n!=\n{:?}",
                original.id, plan, original.plan
            ));
        }
    }
    if originals.get(&envelope.output) != Some(&output) {
        return Err(format!(
            "output {} does not map back to {output}",
            envelope.output
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
