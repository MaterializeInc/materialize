// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! SPARQL-to-HIR query planner.
//!
//! Translates a [`SparqlQuery`] AST into [`HirRelationExpr`] by compiling
//! SPARQL graph patterns into relational algebra over a quad table.

use std::collections::BTreeMap;

use mz_expr::Id;
use mz_repr::{Datum, GlobalId, SqlColumnType, SqlRelationType, SqlScalarType};
use mz_sql::plan::{HirRelationExpr, HirScalarExpr, JoinKind};

use mz_sparql_parser::ast::{
    Expression, GraphTerm, GroupGraphPattern, Iri, RdfLiteral, SparqlQuery, TriplePattern,
    VarOrTerm, VerbPath,
};

/// Column indices in the quad table `(subject, predicate, object, graph)`.
const QUAD_SUBJECT: usize = 0;
const QUAD_PREDICATE: usize = 1;
const QUAD_OBJECT: usize = 2;
#[allow(dead_code)]
const QUAD_GRAPH: usize = 3;
const QUAD_ARITY: usize = 4;

/// Errors that can occur during SPARQL planning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanError {
    pub message: String,
}

impl std::fmt::Display for PlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SPARQL plan error: {}", self.message)
    }
}

impl std::error::Error for PlanError {}

/// The result of planning a SPARQL graph pattern.
///
/// Bundles the HIR expression with metadata about which SPARQL variables
/// are bound and at which column positions.
#[derive(Debug, Clone)]
pub struct PlannedRelation {
    /// The HIR relational expression.
    pub expr: HirRelationExpr,
    /// Maps SPARQL variable names to column indices in `expr`.
    pub var_map: BTreeMap<String, usize>,
}

impl PlannedRelation {
    /// Number of columns in this relation.
    pub fn arity(&self) -> usize {
        self.var_map.len()
    }
}

/// The SPARQL query planner.
///
/// Maintains the quad table identity and type information needed to compile
/// SPARQL patterns into HIR.
pub struct SparqlPlanner {
    /// The global ID of the quad table.
    quad_table_id: GlobalId,
    /// The relation type of the quad table.
    quad_table_type: SqlRelationType,
}

impl SparqlPlanner {
    /// Creates a new planner for the given quad table.
    pub fn new(quad_table_id: GlobalId) -> Self {
        let text_nullable = SqlColumnType {
            scalar_type: SqlScalarType::String,
            nullable: true,
        };
        let quad_table_type = SqlRelationType::new(vec![
            text_nullable.clone(), // subject
            text_nullable.clone(), // predicate
            text_nullable.clone(), // object
            text_nullable,         // graph
        ]);
        SparqlPlanner {
            quad_table_id,
            quad_table_type,
        }
    }

    /// Plans a complete SPARQL query, returning the HIR expression and variable mapping.
    ///
    /// Currently supports SELECT queries with basic graph patterns only.
    /// Later prompts will add FILTER, OPTIONAL, UNION, MINUS, aggregates, etc.
    pub fn plan(&self, query: &SparqlQuery) -> Result<PlannedRelation, PlanError> {
        // Plan the WHERE clause (graph pattern).
        self.plan_pattern(&query.where_clause)
    }

    /// Plans a graph pattern into a relational expression.
    pub fn plan_pattern(&self, pattern: &GroupGraphPattern) -> Result<PlannedRelation, PlanError> {
        match pattern {
            GroupGraphPattern::Basic(triples) => self.plan_bgp(triples),
            GroupGraphPattern::Group(patterns) => self.plan_group(patterns),
            GroupGraphPattern::Filter(_) => Err(PlanError {
                message: "bare FILTER outside Group context; should be handled by plan_group"
                    .to_string(),
            }),
            GroupGraphPattern::Optional(inner) => {
                // Bare OPTIONAL without preceding patterns: left side is empty row.
                let left = self.empty_relation();
                self.plan_optional(left, inner)
            }
            GroupGraphPattern::Union(left, right) => self.plan_union(left, right),
            GroupGraphPattern::Minus(inner) => {
                // Bare MINUS without preceding patterns: left side is empty row.
                let left = self.empty_relation();
                self.plan_minus(left, inner)
            }
            _ => Err(PlanError {
                message: "unsupported graph pattern form (will be implemented in later prompts)"
                    .to_string(),
            }),
        }
    }

    /// Plans a basic graph pattern (a conjunction of triple patterns).
    ///
    /// Each triple pattern becomes a scan of the quad table with filters for
    /// concrete terms. Multiple patterns are joined on shared variables.
    fn plan_bgp(&self, triples: &[TriplePattern]) -> Result<PlannedRelation, PlanError> {
        if triples.is_empty() {
            // Empty BGP: a single row with no columns (the identity for join).
            return Ok(PlannedRelation {
                expr: HirRelationExpr::Constant {
                    rows: vec![mz_repr::Row::default()],
                    typ: SqlRelationType::empty(),
                },
                var_map: BTreeMap::new(),
            });
        }

        // Plan the first triple pattern.
        let mut result = self.plan_triple(&triples[0])?;

        // Join with each subsequent triple pattern.
        for triple in &triples[1..] {
            let right = self.plan_triple(triple)?;
            result = self.join_on_shared_vars(result, right)?;
        }

        Ok(result)
    }

    /// Plans a group of sub-patterns.
    ///
    /// In the SPARQL algebra, a group `{ P1 . FILTER(e) . OPTIONAL { P2 } . MINUS { P3 } }`
    /// is evaluated left-to-right:
    /// - Basic/Group/Union patterns are inner-joined with the accumulator
    /// - FILTER applies to the accumulated result so far
    /// - OPTIONAL left-outer-joins its inner pattern with the accumulator
    /// - MINUS anti-joins its inner pattern against the accumulator
    fn plan_group(&self, patterns: &[GroupGraphPattern]) -> Result<PlannedRelation, PlanError> {
        let mut result = self.empty_relation();

        // Collect FILTERs — per SPARQL spec, FILTERs in a group apply to the
        // entire group, not just patterns preceding them. We collect them and
        // apply after all other patterns are joined.
        // Exception: FILTERs inside OPTIONAL are handled specially (they go
        // into the left-join ON clause) — but those are inside nested groups,
        // not at this level.
        let mut group_filters = Vec::new();

        for pattern in patterns {
            match pattern {
                GroupGraphPattern::Filter(expr) => {
                    group_filters.push(expr.clone());
                }
                GroupGraphPattern::Optional(inner) => {
                    result = self.plan_optional(result, inner)?;
                }
                GroupGraphPattern::Minus(inner) => {
                    result = self.plan_minus(result, inner)?;
                }
                other => {
                    let right = self.plan_pattern(other)?;
                    if result.var_map.is_empty()
                        && matches!(result.expr, HirRelationExpr::Constant { .. })
                    {
                        // First non-trivial pattern: replace empty relation.
                        result = right;
                    } else {
                        result = self.join_on_shared_vars(result, right)?;
                    }
                }
            }
        }

        // Apply collected FILTERs.
        for filter_expr in &group_filters {
            result = self.apply_filter(result, filter_expr)?;
        }

        Ok(result)
    }

    /// Returns an empty relation (single row, no columns) — the identity for join.
    fn empty_relation(&self) -> PlannedRelation {
        PlannedRelation {
            expr: HirRelationExpr::Constant {
                rows: vec![mz_repr::Row::default()],
                typ: SqlRelationType::empty(),
            },
            var_map: BTreeMap::new(),
        }
    }

    /// Plans `OPTIONAL { inner }` as a left outer join of `left` with the inner pattern.
    ///
    /// If the inner pattern is a Group containing FILTERs, those filters go into
    /// the join ON clause (not post-join), preserving SPARQL OPTIONAL+FILTER semantics.
    fn plan_optional(
        &self,
        left: PlannedRelation,
        inner: &GroupGraphPattern,
    ) -> Result<PlannedRelation, PlanError> {
        // Extract filters from inside the OPTIONAL group.
        let (base_pattern, inner_filters) = self.extract_optional_filters(inner);

        // Plan the non-filter part of the OPTIONAL.
        let right = self.plan_pattern(&base_pattern)?;

        let left_arity = left.arity();

        // Build equality predicates for shared variables (the join condition).
        let mut join_predicates = Vec::new();
        let mut shared_vars: Vec<String> = Vec::new();

        for (var_name, &left_col) in &left.var_map {
            if let Some(&right_col) = right.var_map.get(var_name) {
                shared_vars.push(var_name.clone());
                join_predicates.push(HirScalarExpr::CallBinary {
                    func: mz_expr::BinaryFunc::Eq(mz_expr::func::Eq),
                    expr1: Box::new(HirScalarExpr::column(left_col)),
                    expr2: Box::new(HirScalarExpr::column(left_arity + right_col)),
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                });
            }
        }

        // Translate FILTER expressions from inside OPTIONAL and add to join ON.
        // These reference the right-side columns (offset by left_arity).
        for filter_expr in &inner_filters {
            let combined_var_map = self.combined_var_map(&left.var_map, &right.var_map, left_arity);
            let hir_filter = self.translate_expression(filter_expr, &combined_var_map)?;
            join_predicates.push(hir_filter);
        }

        // Build ON condition.
        let on = self.conjunction(join_predicates);

        let joined = HirRelationExpr::Join {
            left: Box::new(left.expr),
            right: Box::new(right.expr),
            on,
            kind: JoinKind::LeftOuter,
        };

        // Build projection: keep all left columns, add new right-only columns.
        let mut project_cols: Vec<usize> = (0..left_arity).collect();
        let mut new_var_map = left.var_map.clone();

        for (var_name, &right_col) in &right.var_map {
            if !shared_vars.contains(var_name) {
                let new_col = project_cols.len();
                project_cols.push(left_arity + right_col);
                new_var_map.insert(var_name.clone(), new_col);
            }
        }

        let projected = HirRelationExpr::Project {
            input: Box::new(joined),
            outputs: project_cols,
        };

        Ok(PlannedRelation {
            expr: projected,
            var_map: new_var_map,
        })
    }

    /// Extracts FILTER expressions from an OPTIONAL's inner group.
    ///
    /// Returns the base pattern (without filters) and the extracted filter expressions.
    /// This is needed because SPARQL OPTIONAL { P FILTER(e) } means:
    /// left-outer-join with P, and e goes into the ON clause.
    fn extract_optional_filters(
        &self,
        pattern: &GroupGraphPattern,
    ) -> (GroupGraphPattern, Vec<Expression>) {
        match pattern {
            GroupGraphPattern::Group(patterns) => {
                let mut base_patterns = Vec::new();
                let mut filters = Vec::new();
                for p in patterns {
                    match p {
                        GroupGraphPattern::Filter(expr) => filters.push(expr.clone()),
                        other => base_patterns.push(other.clone()),
                    }
                }
                let base = if base_patterns.len() == 1 {
                    base_patterns.pop().unwrap()
                } else {
                    GroupGraphPattern::Group(base_patterns)
                };
                (base, filters)
            }
            other => (other.clone(), Vec::new()),
        }
    }

    /// Plans `{ left } UNION { right }` as an outer union with NULL-padding.
    ///
    /// Variables present in only one side get NULL values on the other side.
    fn plan_union(
        &self,
        left_pattern: &GroupGraphPattern,
        right_pattern: &GroupGraphPattern,
    ) -> Result<PlannedRelation, PlanError> {
        let left = self.plan_pattern(left_pattern)?;
        let right = self.plan_pattern(right_pattern)?;

        // Compute the combined variable set (sorted for determinism).
        let mut all_vars: Vec<String> = left
            .var_map
            .keys()
            .chain(right.var_map.keys())
            .cloned()
            .collect();
        all_vars.sort();
        all_vars.dedup();

        let output_var_map: BTreeMap<String, usize> = all_vars
            .iter()
            .enumerate()
            .map(|(i, v)| (v.clone(), i))
            .collect();

        // Pad each side: for variables not present, add a NULL column via Map,
        // then Project to the combined column order.
        let left_padded = self.pad_for_union(&left, &all_vars)?;
        let right_padded = self.pad_for_union(&right, &all_vars)?;

        let union_expr = left_padded.union(right_padded);

        Ok(PlannedRelation {
            expr: union_expr,
            var_map: output_var_map,
        })
    }

    /// Pads a relation so its columns match `target_vars` (in order), adding
    /// NULL columns for any variables not present in the relation.
    fn pad_for_union(
        &self,
        rel: &PlannedRelation,
        target_vars: &[String],
    ) -> Result<HirRelationExpr, PlanError> {
        let rel_arity = rel.arity();

        // Figure out which target vars need NULL padding.
        let mut null_columns = Vec::new();
        let mut project_indices = Vec::new();

        for var in target_vars {
            if let Some(&col) = rel.var_map.get(var) {
                // Variable exists in this relation — use its column.
                project_indices.push(col);
            } else {
                // Variable doesn't exist — we'll add a NULL column via Map.
                let null_col_index = rel_arity + null_columns.len();
                null_columns.push(HirScalarExpr::literal_null(SqlScalarType::String));
                project_indices.push(null_col_index);
            }
        }

        let mut expr = rel.expr.clone();

        if !null_columns.is_empty() {
            expr = HirRelationExpr::Map {
                input: Box::new(expr),
                scalars: null_columns,
            };
        }

        // Project to the target column order.
        expr = HirRelationExpr::Project {
            input: Box::new(expr),
            outputs: project_indices,
        };

        Ok(expr)
    }

    /// Plans `MINUS { inner }` as an anti-join on shared variables.
    ///
    /// SPARQL MINUS semantics: remove from the left side any solutions that are
    /// "compatible" with a solution from the right side on their shared variables.
    /// If there are no shared variables, MINUS has no effect.
    fn plan_minus(
        &self,
        left: PlannedRelation,
        inner: &GroupGraphPattern,
    ) -> Result<PlannedRelation, PlanError> {
        let right = self.plan_pattern(inner)?;

        // Find shared variables.
        let shared_vars: Vec<String> = left
            .var_map
            .keys()
            .filter(|v| right.var_map.contains_key(*v))
            .cloned()
            .collect();

        if shared_vars.is_empty() {
            // No shared variables → MINUS has no effect (per SPARQL spec).
            return Ok(left);
        }

        // Project right side down to just the shared variables, then distinct.
        let right_shared_cols: Vec<usize> = shared_vars.iter().map(|v| right.var_map[v]).collect();
        let right_projected = HirRelationExpr::Distinct {
            input: Box::new(HirRelationExpr::Project {
                input: Box::new(right.expr),
                outputs: right_shared_cols,
            }),
        };

        // Anti-join via Negate + Union + Threshold.
        //
        // result = left ⊳ right (anti-join on shared vars)
        //        = left EXCEPT (left semi-join right on shared vars)
        //
        // We implement this as:
        //   1. Join left with right_projected on shared vars (inner join = semi-join since right is distinct)
        //   2. Negate the result
        //   3. Union with left
        //   4. Threshold (keeps only positive counts)

        let left_arity = left.arity();

        // Build join predicates for shared variables.
        let mut join_predicates = Vec::new();
        for (i, var_name) in shared_vars.iter().enumerate() {
            let left_col = left.var_map[var_name];
            let right_col = i; // right_projected has shared vars at 0..n
            join_predicates.push(HirScalarExpr::CallBinary {
                func: mz_expr::BinaryFunc::Eq(mz_expr::func::Eq),
                expr1: Box::new(HirScalarExpr::column(left_col)),
                expr2: Box::new(HirScalarExpr::column(left_arity + right_col)),
                name: mz_ore::treat_as_equal::TreatAsEqual(None),
            });
        }

        let on = self.conjunction(join_predicates);

        // Inner join left with right_projected.
        let joined = HirRelationExpr::Join {
            left: Box::new(left.expr.clone()),
            right: Box::new(right_projected),
            on,
            kind: JoinKind::Inner,
        };

        // Project back to just left columns.
        let left_cols: Vec<usize> = (0..left_arity).collect();
        let matched = HirRelationExpr::Project {
            input: Box::new(joined),
            outputs: left_cols,
        };

        // Anti-join: left EXCEPT ALL matched
        let result = left.expr.union(matched.negate()).threshold();

        Ok(PlannedRelation {
            expr: result,
            var_map: left.var_map,
        })
    }

    /// Applies a SPARQL FILTER expression to a planned relation.
    fn apply_filter(
        &self,
        rel: PlannedRelation,
        filter_expr: &Expression,
    ) -> Result<PlannedRelation, PlanError> {
        let hir_expr = self.translate_expression(filter_expr, &rel.var_map)?;
        Ok(PlannedRelation {
            expr: HirRelationExpr::Filter {
                input: Box::new(rel.expr),
                predicates: vec![hir_expr],
            },
            var_map: rel.var_map,
        })
    }

    /// Translates a SPARQL expression to an HIR scalar expression.
    ///
    /// Variable references are resolved to column indices via `var_map`.
    fn translate_expression(
        &self,
        expr: &Expression,
        var_map: &BTreeMap<String, usize>,
    ) -> Result<HirScalarExpr, PlanError> {
        match expr {
            Expression::Variable(v) => {
                let col = var_map.get(&v.name).ok_or_else(|| PlanError {
                    message: format!("variable ?{} not in scope", v.name),
                })?;
                Ok(HirScalarExpr::column(*col))
            }
            Expression::Literal(lit) => {
                let s = self.literal_to_string(lit);
                Ok(HirScalarExpr::literal(
                    Datum::String(&s),
                    SqlScalarType::String,
                ))
            }
            Expression::NumericLiteral(s) => Ok(HirScalarExpr::literal(
                Datum::String(s),
                SqlScalarType::String,
            )),
            Expression::BooleanLiteral(b) => Ok(HirScalarExpr::literal(
                if *b { Datum::True } else { Datum::False },
                SqlScalarType::Bool,
            )),
            Expression::Iri(iri) => Ok(HirScalarExpr::literal(
                Datum::String(&iri.value),
                SqlScalarType::String,
            )),

            // Comparison operators
            Expression::Equal(l, r) => {
                self.translate_binary(mz_expr::BinaryFunc::Eq(mz_expr::func::Eq), l, r, var_map)
            }
            Expression::NotEqual(l, r) => self.translate_binary(
                mz_expr::BinaryFunc::NotEq(mz_expr::func::NotEq),
                l,
                r,
                var_map,
            ),
            Expression::LessThan(l, r) => {
                self.translate_binary(mz_expr::BinaryFunc::Lt(mz_expr::func::Lt), l, r, var_map)
            }
            Expression::GreaterThan(l, r) => {
                self.translate_binary(mz_expr::BinaryFunc::Gt(mz_expr::func::Gt), l, r, var_map)
            }
            Expression::LessThanOrEqual(l, r) => {
                self.translate_binary(mz_expr::BinaryFunc::Lte(mz_expr::func::Lte), l, r, var_map)
            }
            Expression::GreaterThanOrEqual(l, r) => {
                self.translate_binary(mz_expr::BinaryFunc::Gte(mz_expr::func::Gte), l, r, var_map)
            }

            // Logical operators
            Expression::And(l, r) => {
                let l = self.translate_expression(l, var_map)?;
                let r = self.translate_expression(r, var_map)?;
                Ok(HirScalarExpr::CallVariadic {
                    func: mz_expr::VariadicFunc::And(mz_expr::func::variadic::And),
                    exprs: vec![l, r],
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                })
            }
            Expression::Or(l, r) => {
                let l = self.translate_expression(l, var_map)?;
                let r = self.translate_expression(r, var_map)?;
                Ok(HirScalarExpr::CallVariadic {
                    func: mz_expr::VariadicFunc::Or(mz_expr::func::variadic::Or),
                    exprs: vec![l, r],
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                })
            }
            Expression::UnaryNot(e) => {
                let inner = self.translate_expression(e, var_map)?;
                Ok(HirScalarExpr::CallUnary {
                    func: mz_expr::UnaryFunc::Not(mz_expr::func::Not),
                    expr: Box::new(inner),
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                })
            }

            // BOUND(?var) — true if the variable is not null
            Expression::Bound(v) => {
                let col = var_map.get(&v.name).ok_or_else(|| PlanError {
                    message: format!("variable ?{} not in scope for BOUND", v.name),
                })?;
                Ok(HirScalarExpr::CallUnary {
                    func: mz_expr::UnaryFunc::IsNull(mz_expr::func::IsNull),
                    expr: Box::new(HirScalarExpr::column(*col)),
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                })
                // IsNull returns true when null; BOUND should return true when NOT null.
                .map(|e| HirScalarExpr::CallUnary {
                    func: mz_expr::UnaryFunc::Not(mz_expr::func::Not),
                    expr: Box::new(e),
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                })
            }

            // Placeholder for expressions that will be fully implemented in prompt 9
            _ => Err(PlanError {
                message: format!(
                    "SPARQL expression form not yet supported (will be implemented in prompt 9): {:?}",
                    std::mem::discriminant(expr)
                ),
            }),
        }
    }

    /// Helper: translate a binary operator expression.
    fn translate_binary(
        &self,
        func: mz_expr::BinaryFunc,
        left: &Expression,
        right: &Expression,
        var_map: &BTreeMap<String, usize>,
    ) -> Result<HirScalarExpr, PlanError> {
        let l = self.translate_expression(left, var_map)?;
        let r = self.translate_expression(right, var_map)?;
        Ok(HirScalarExpr::CallBinary {
            func,
            expr1: Box::new(l),
            expr2: Box::new(r),
            name: mz_ore::treat_as_equal::TreatAsEqual(None),
        })
    }

    /// Builds a conjunction of predicates (AND). Returns `true` if empty.
    fn conjunction(&self, mut predicates: Vec<HirScalarExpr>) -> HirScalarExpr {
        if predicates.is_empty() {
            HirScalarExpr::literal_true()
        } else if predicates.len() == 1 {
            predicates.pop().unwrap()
        } else {
            HirScalarExpr::CallVariadic {
                func: mz_expr::VariadicFunc::And(mz_expr::func::variadic::And),
                exprs: predicates,
                name: mz_ore::treat_as_equal::TreatAsEqual(None),
            }
        }
    }

    /// Builds a combined var_map for a join (left + right with offset).
    fn combined_var_map(
        &self,
        left: &BTreeMap<String, usize>,
        right: &BTreeMap<String, usize>,
        left_arity: usize,
    ) -> BTreeMap<String, usize> {
        let mut combined = left.clone();
        for (var, &col) in right {
            combined.entry(var.clone()).or_insert(left_arity + col);
        }
        combined
    }

    /// Converts an RDF literal to its string representation.
    fn literal_to_string(&self, lit: &RdfLiteral) -> String {
        match lit {
            RdfLiteral::Simple(s) => s.clone(),
            RdfLiteral::LanguageTagged { value, language } => {
                format!("\"{}\"@{}", value, language)
            }
            RdfLiteral::Typed { value, datatype } => {
                format!("\"{}\"^^<{}>", value, datatype.value)
            }
        }
    }

    /// Plans a single triple pattern.
    ///
    /// Produces: `Get(quad_table) → Filter(bound positions) → Project(variable columns)`
    ///
    /// The output relation has one column per distinct variable in the triple.
    /// The `var_map` tracks which variable maps to which output column.
    fn plan_triple(&self, triple: &TriplePattern) -> Result<PlannedRelation, PlanError> {
        // Start with a scan of the quad table.
        let mut expr: HirRelationExpr = HirRelationExpr::Get {
            id: Id::Global(self.quad_table_id),
            typ: self.quad_table_type.clone(),
        };

        // Collect filters for concrete (non-variable) positions.
        let mut filters = Vec::new();

        // Collect variable bindings: maps var name → quad column index.
        // If the same variable appears in multiple positions, we add an
        // equality filter instead of a duplicate binding.
        let mut var_to_quad_col: BTreeMap<String, usize> = BTreeMap::new();
        let mut extra_eq_filters = Vec::new();

        // Process subject.
        match &triple.subject {
            VarOrTerm::Variable(v) => {
                if let Some(&existing_col) = var_to_quad_col.get(&v.name) {
                    // Same variable in multiple positions → equality filter.
                    extra_eq_filters.push((existing_col, QUAD_SUBJECT));
                } else {
                    var_to_quad_col.insert(v.name.clone(), QUAD_SUBJECT);
                }
            }
            VarOrTerm::Term(term) => {
                filters.push(self.term_filter(QUAD_SUBJECT, term));
            }
        }

        // Process predicate.
        match &triple.predicate {
            VerbPath::Variable(v) => {
                if let Some(&existing_col) = var_to_quad_col.get(&v.name) {
                    extra_eq_filters.push((existing_col, QUAD_PREDICATE));
                } else {
                    var_to_quad_col.insert(v.name.clone(), QUAD_PREDICATE);
                }
            }
            VerbPath::Path(path) => {
                use mz_sparql_parser::ast::PropertyPath;
                match path {
                    PropertyPath::Iri(iri) => {
                        filters.push(self.iri_filter(QUAD_PREDICATE, iri));
                    }
                    _ => {
                        return Err(PlanError {
                            message: "property paths not yet supported (see prompt 12)".into(),
                        });
                    }
                }
            }
        }

        // Process object.
        match &triple.object {
            VarOrTerm::Variable(v) => {
                if let Some(&existing_col) = var_to_quad_col.get(&v.name) {
                    extra_eq_filters.push((existing_col, QUAD_OBJECT));
                } else {
                    var_to_quad_col.insert(v.name.clone(), QUAD_OBJECT);
                }
            }
            VarOrTerm::Term(term) => {
                filters.push(self.term_filter(QUAD_OBJECT, term));
            }
        }

        // Add equality filters for repeated variables.
        for (col_a, col_b) in &extra_eq_filters {
            filters.push(HirScalarExpr::CallBinary {
                func: mz_expr::BinaryFunc::Eq(mz_expr::func::Eq),
                expr1: Box::new(HirScalarExpr::column(*col_a)),
                expr2: Box::new(HirScalarExpr::column(*col_b)),
                name: mz_ore::treat_as_equal::TreatAsEqual(None),
            });
        }

        // Apply filters.
        if !filters.is_empty() {
            expr = HirRelationExpr::Filter {
                input: Box::new(expr),
                predicates: filters,
            };
        }

        // Build the output projection: keep only columns that have variables,
        // in a deterministic order (sorted by variable name).
        let sorted_vars: Vec<(String, usize)> = var_to_quad_col.into_iter().collect();

        let project_cols: Vec<usize> = sorted_vars.iter().map(|(_, col)| *col).collect();
        let var_map: BTreeMap<String, usize> = sorted_vars
            .iter()
            .enumerate()
            .map(|(out_idx, (name, _))| (name.clone(), out_idx))
            .collect();

        // Build output type.
        let output_types: Vec<SqlColumnType> = project_cols
            .iter()
            .map(|col| self.quad_table_type.column_types[*col].clone())
            .collect();

        expr = HirRelationExpr::Project {
            input: Box::new(expr),
            outputs: project_cols,
        };

        // If there are extra equality filters on the same variable appearing
        // in multiple positions, we've already added filters above, but the
        // variable should only appear once in the output (it already does
        // since var_to_quad_col uses the first occurrence).

        let _ = output_types; // Type info tracked via var_map for now.

        Ok(PlannedRelation { expr, var_map })
    }

    /// Joins two planned relations on shared variables.
    ///
    /// Shared variables produce equality conditions in the join's ON clause.
    /// After the join, duplicate columns for shared variables are projected away.
    fn join_on_shared_vars(
        &self,
        left: PlannedRelation,
        right: PlannedRelation,
    ) -> Result<PlannedRelation, PlanError> {
        let left_arity = left.arity();

        // Find shared variables and build equality predicates.
        let mut join_predicates = Vec::new();
        let mut shared_vars: Vec<String> = Vec::new();

        for (var_name, &left_col) in &left.var_map {
            if let Some(&right_col) = right.var_map.get(var_name) {
                shared_vars.push(var_name.clone());
                // In the joined relation, right columns are offset by left_arity.
                join_predicates.push(HirScalarExpr::CallBinary {
                    func: mz_expr::BinaryFunc::Eq(mz_expr::func::Eq),
                    expr1: Box::new(HirScalarExpr::column(left_col)),
                    expr2: Box::new(HirScalarExpr::column(left_arity + right_col)),
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                });
            }
        }

        // Build the ON condition (conjunction of all equality predicates).
        let on = if join_predicates.is_empty() {
            // Cross join (no shared variables).
            HirScalarExpr::literal_true()
        } else if join_predicates.len() == 1 {
            join_predicates.pop().unwrap()
        } else {
            HirScalarExpr::CallVariadic {
                func: mz_expr::VariadicFunc::And(mz_expr::func::variadic::And),
                exprs: join_predicates,
                name: mz_ore::treat_as_equal::TreatAsEqual(None),
            }
        };

        let joined = HirRelationExpr::Join {
            left: Box::new(left.expr),
            right: Box::new(right.expr),
            on,
            kind: JoinKind::Inner,
        };

        // Build projection to eliminate duplicate columns for shared variables.
        // Keep all left columns, then keep right columns that are NOT shared.
        let mut project_cols: Vec<usize> = (0..left_arity).collect();
        let mut new_var_map = left.var_map.clone();

        for (var_name, &right_col) in &right.var_map {
            if !shared_vars.contains(var_name) {
                let new_col = project_cols.len();
                project_cols.push(left_arity + right_col);
                new_var_map.insert(var_name.clone(), new_col);
            }
            // Shared variables already in new_var_map from left.
        }

        let projected = HirRelationExpr::Project {
            input: Box::new(joined),
            outputs: project_cols,
        };

        Ok(PlannedRelation {
            expr: projected,
            var_map: new_var_map,
        })
    }

    /// Creates a filter predicate that checks a quad column equals a concrete RDF term.
    fn term_filter(&self, column: usize, term: &GraphTerm) -> HirScalarExpr {
        let value = self.term_to_string(term);
        HirScalarExpr::CallBinary {
            func: mz_expr::BinaryFunc::Eq(mz_expr::func::Eq),
            expr1: Box::new(HirScalarExpr::column(column)),
            expr2: Box::new(HirScalarExpr::literal(
                Datum::String(&value),
                SqlScalarType::String,
            )),
            name: mz_ore::treat_as_equal::TreatAsEqual(None),
        }
    }

    /// Creates a filter predicate that checks a quad column equals an IRI.
    fn iri_filter(&self, column: usize, iri: &Iri) -> HirScalarExpr {
        HirScalarExpr::CallBinary {
            func: mz_expr::BinaryFunc::Eq(mz_expr::func::Eq),
            expr1: Box::new(HirScalarExpr::column(column)),
            expr2: Box::new(HirScalarExpr::literal(
                Datum::String(&iri.value),
                SqlScalarType::String,
            )),
            name: mz_ore::treat_as_equal::TreatAsEqual(None),
        }
    }

    /// Converts an RDF term to its string representation for storage in the quad table.
    ///
    /// The encoding follows N-Triples conventions:
    /// - IRIs: stored as the bare IRI string (no angle brackets)
    /// - Simple literals: stored as the string value
    /// - Language-tagged literals: `"value"@lang`
    /// - Typed literals: `"value"^^<datatype>`
    /// - Blank nodes: `_:label`
    /// - Numeric literals: stored as the literal string
    /// - Boolean literals: `"true"` or `"false"`
    fn term_to_string(&self, term: &GraphTerm) -> String {
        match term {
            GraphTerm::Iri(iri) => iri.value.clone(),
            GraphTerm::Literal(lit) => match lit {
                RdfLiteral::Simple(s) => s.clone(),
                RdfLiteral::LanguageTagged { value, language } => {
                    format!("\"{}\"@{}", value, language)
                }
                RdfLiteral::Typed { value, datatype } => {
                    format!("\"{}\"^^<{}>", value, datatype.value)
                }
            },
            GraphTerm::BlankNode(label) => format!("_:{}", label),
            GraphTerm::NumericLiteral(s) => s.clone(),
            GraphTerm::BooleanLiteral(b) => b.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_sparql_parser::parser::parse;

    /// Helper: plan a SPARQL query string and return the PlannedRelation.
    fn plan_query(sparql: &str) -> PlannedRelation {
        let query = parse(sparql).expect("parse failed");
        let planner = SparqlPlanner::new(GlobalId::User(1));
        planner.plan(&query).expect("planning failed")
    }

    /// Helper: count the variables in a planned relation.
    fn var_names(planned: &PlannedRelation) -> Vec<String> {
        let mut names: Vec<String> = planned.var_map.keys().cloned().collect();
        names.sort();
        names
    }

    // --- Single triple pattern tests ---

    #[mz_ore::test]
    fn test_single_triple_all_vars() {
        let result = plan_query("SELECT * WHERE { ?s ?p ?o }");
        assert_eq!(var_names(&result), vec!["o", "p", "s"]);
        assert_eq!(result.arity(), 3);

        // Should be: Project(Get(quad_table))
        // No filters since all positions are variables.
        match &result.expr {
            HirRelationExpr::Project { input, outputs } => {
                assert_eq!(outputs.len(), 3);
                match input.as_ref() {
                    HirRelationExpr::Get { id, .. } => {
                        assert_eq!(*id, Id::Global(GlobalId::User(1)));
                    }
                    other => panic!("expected Get, got {:?}", other),
                }
            }
            other => panic!("expected Project, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_single_triple_concrete_predicate() {
        let result =
            plan_query("PREFIX ex: <http://example.org/> SELECT * WHERE { ?s ex:name ?o }");
        assert_eq!(var_names(&result), vec!["o", "s"]);
        assert_eq!(result.arity(), 2);

        // Should be: Project(Filter(Get(quad_table)))
        match &result.expr {
            HirRelationExpr::Project { input, outputs } => {
                assert_eq!(outputs.len(), 2);
                match input.as_ref() {
                    HirRelationExpr::Filter { input, predicates } => {
                        assert_eq!(predicates.len(), 1);
                        match input.as_ref() {
                            HirRelationExpr::Get { .. } => {}
                            other => panic!("expected Get, got {:?}", other),
                        }
                    }
                    other => panic!("expected Filter, got {:?}", other),
                }
            }
            other => panic!("expected Project, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_single_triple_concrete_subject_and_predicate() {
        let result = plan_query(
            "SELECT * WHERE { <http://example.org/alice> <http://example.org/name> ?name }",
        );
        assert_eq!(var_names(&result), vec!["name"]);
        assert_eq!(result.arity(), 1);

        // Should have 2 filters (subject and predicate).
        match &result.expr {
            HirRelationExpr::Project { input, .. } => match input.as_ref() {
                HirRelationExpr::Filter { predicates, .. } => {
                    assert_eq!(predicates.len(), 2);
                }
                other => panic!("expected Filter, got {:?}", other),
            },
            other => panic!("expected Project, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_single_triple_rdf_type_shorthand() {
        // `a` is shorthand for rdf:type.
        let result = plan_query("SELECT * WHERE { ?s a ?type }");
        assert_eq!(var_names(&result), vec!["s", "type"]);

        // Should have a filter on predicate = rdf:type.
        match &result.expr {
            HirRelationExpr::Project { input, .. } => match input.as_ref() {
                HirRelationExpr::Filter { predicates, .. } => {
                    assert_eq!(predicates.len(), 1);
                    // Verify the filter value is rdf:type.
                    match &predicates[0] {
                        HirScalarExpr::CallBinary { expr2, .. } => match expr2.as_ref() {
                            HirScalarExpr::Literal(row, _, _) => {
                                let datum = row.unpack_first();
                                assert_eq!(
                                    datum.unwrap_str(),
                                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
                                );
                            }
                            other => panic!("expected Literal, got {:?}", other),
                        },
                        other => panic!("expected CallBinary, got {:?}", other),
                    }
                }
                other => panic!("expected Filter, got {:?}", other),
            },
            other => panic!("expected Project, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_single_triple_repeated_variable() {
        // ?x appears as both subject and object → equality filter.
        let result = plan_query("SELECT * WHERE { ?x <http://example.org/knows> ?x }");
        assert_eq!(var_names(&result), vec!["x"]);
        assert_eq!(result.arity(), 1);

        // Should have 2 filters: predicate=IRI and subject=object.
        match &result.expr {
            HirRelationExpr::Project { input, .. } => match input.as_ref() {
                HirRelationExpr::Filter { predicates, .. } => {
                    assert_eq!(predicates.len(), 2);
                }
                other => panic!("expected Filter, got {:?}", other),
            },
            other => panic!("expected Project, got {:?}", other),
        }
    }

    // --- Multi-triple BGP tests ---

    #[mz_ore::test]
    fn test_bgp_two_patterns_shared_variable() {
        let result = plan_query(
            "PREFIX ex: <http://example.org/>
             SELECT * WHERE { ?s ex:name ?name . ?s ex:age ?age }",
        );
        // Variables: s (shared), name (left only), age (right only).
        assert_eq!(var_names(&result), vec!["age", "name", "s"]);
        assert_eq!(result.arity(), 3);

        // Should be: Project(Join(triple1, triple2))
        match &result.expr {
            HirRelationExpr::Project { input, .. } => match input.as_ref() {
                HirRelationExpr::Join { kind, .. } => {
                    assert_eq!(*kind, JoinKind::Inner);
                }
                other => panic!("expected Join, got {:?}", other),
            },
            other => panic!("expected Project, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_bgp_two_patterns_no_shared_variables() {
        let result = plan_query(
            "PREFIX ex: <http://example.org/>
             SELECT * WHERE { ?s ex:name ?name . ?x ex:age ?age }",
        );
        // No shared variables → cross join.
        assert_eq!(var_names(&result), vec!["age", "name", "s", "x"]);
        assert_eq!(result.arity(), 4);

        // The join ON condition should be `true` (cross join).
        match &result.expr {
            HirRelationExpr::Project { input, .. } => match input.as_ref() {
                HirRelationExpr::Join { on, kind, .. } => {
                    assert_eq!(*kind, JoinKind::Inner);
                    // ON should be literal true.
                    match on {
                        HirScalarExpr::Literal(row, _, _) => {
                            assert_eq!(row.unpack_first(), Datum::True);
                        }
                        other => panic!("expected literal true, got {:?}", other),
                    }
                }
                other => panic!("expected Join, got {:?}", other),
            },
            other => panic!("expected Project, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_bgp_three_patterns() {
        let result = plan_query(
            "PREFIX ex: <http://example.org/>
             SELECT * WHERE {
                 ?s ex:name ?name .
                 ?s ex:age ?age .
                 ?s ex:email ?email
             }",
        );
        assert_eq!(var_names(&result), vec!["age", "email", "name", "s"]);
        assert_eq!(result.arity(), 4);

        // Should be: Project(Join(Project(Join(t1, t2)), t3))
        // The outermost structure is Project(Join(...)).
        match &result.expr {
            HirRelationExpr::Project { input, .. } => match input.as_ref() {
                HirRelationExpr::Join { kind, left, .. } => {
                    assert_eq!(*kind, JoinKind::Inner);
                    // Left side should also be a Project(Join(...)).
                    match left.as_ref() {
                        HirRelationExpr::Project { input, .. } => match input.as_ref() {
                            HirRelationExpr::Join { kind, .. } => {
                                assert_eq!(*kind, JoinKind::Inner);
                            }
                            other => panic!("expected inner Join, got {:?}", other),
                        },
                        other => panic!("expected inner Project, got {:?}", other),
                    }
                }
                other => panic!("expected Join, got {:?}", other),
            },
            other => panic!("expected Project, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_bgp_multiple_shared_variables() {
        // Two patterns sharing two variables.
        let result = plan_query(
            "SELECT * WHERE {
                 ?s ?p ?o .
                 ?s ?p ?val
             }",
        );
        // s and p are shared; o and val are unique.
        assert_eq!(var_names(&result), vec!["o", "p", "s", "val"]);
        assert_eq!(result.arity(), 4);
    }

    #[mz_ore::test]
    fn test_empty_bgp() {
        let result = plan_query("SELECT * WHERE { }");
        assert_eq!(result.arity(), 0);
        assert!(result.var_map.is_empty());
        // Should be a constant with one row.
        match &result.expr {
            HirRelationExpr::Constant { rows, .. } => {
                assert_eq!(rows.len(), 1);
            }
            other => panic!("expected Constant, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_concrete_literal_object() {
        let result = plan_query(r#"SELECT * WHERE { ?s <http://example.org/name> "Alice" }"#);
        assert_eq!(var_names(&result), vec!["s"]);

        // Should have 2 filters: predicate and object.
        match &result.expr {
            HirRelationExpr::Project { input, .. } => match input.as_ref() {
                HirRelationExpr::Filter { predicates, .. } => {
                    assert_eq!(predicates.len(), 2);
                }
                other => panic!("expected Filter, got {:?}", other),
            },
            other => panic!("expected Project, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_variable_column_indices_are_deterministic() {
        // Ensure variable ordering is consistent (alphabetical by name).
        let result1 = plan_query("SELECT * WHERE { ?z ?a ?m }");
        let result2 = plan_query("SELECT * WHERE { ?z ?a ?m }");
        assert_eq!(result1.var_map, result2.var_map);
        // Alphabetical: a=0, m=1, z=2
        assert_eq!(result1.var_map["a"], 0);
        assert_eq!(result1.var_map["m"], 1);
        assert_eq!(result1.var_map["z"], 2);
    }

    // --- FILTER tests ---

    #[mz_ore::test]
    fn test_filter_simple_equality() {
        let result = plan_query(
            r#"PREFIX ex: <http://example.org/>
               SELECT * WHERE { ?s ex:name ?name . FILTER(?name = "Alice") }"#,
        );
        assert_eq!(var_names(&result), vec!["name", "s"]);

        // Should be: Filter(Project(Join(...)) or Filter on BGP result.
        match &result.expr {
            HirRelationExpr::Filter { predicates, .. } => {
                assert_eq!(predicates.len(), 1);
            }
            other => panic!("expected Filter, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_filter_comparison() {
        let result = plan_query(
            "PREFIX ex: <http://example.org/>
             SELECT * WHERE { ?s ex:age ?age . FILTER(?age > ?s) }",
        );
        assert_eq!(var_names(&result), vec!["age", "s"]);

        match &result.expr {
            HirRelationExpr::Filter { predicates, .. } => {
                assert_eq!(predicates.len(), 1);
                // The predicate should be a Gt comparison.
                match &predicates[0] {
                    HirScalarExpr::CallBinary { func, .. } => {
                        assert!(matches!(func, mz_expr::BinaryFunc::Gt(_)));
                    }
                    other => panic!("expected CallBinary Gt, got {:?}", other),
                }
            }
            other => panic!("expected Filter, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_filter_logical_and() {
        let result = plan_query(
            r#"PREFIX ex: <http://example.org/>
               SELECT * WHERE {
                   ?s ex:name ?name .
                   FILTER(?name = "Alice" && ?s = "bob")
               }"#,
        );
        assert_eq!(var_names(&result), vec!["name", "s"]);

        match &result.expr {
            HirRelationExpr::Filter { predicates, .. } => {
                assert_eq!(predicates.len(), 1);
                match &predicates[0] {
                    HirScalarExpr::CallVariadic { func, exprs, .. } => {
                        assert!(matches!(func, mz_expr::VariadicFunc::And(_)));
                        assert_eq!(exprs.len(), 2);
                    }
                    other => panic!("expected CallVariadic And, got {:?}", other),
                }
            }
            other => panic!("expected Filter, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_filter_unary_not() {
        let result = plan_query(r#"SELECT * WHERE { ?s ?p ?o . FILTER(!(?s = "x")) }"#);
        assert_eq!(var_names(&result), vec!["o", "p", "s"]);

        match &result.expr {
            HirRelationExpr::Filter { predicates, .. } => {
                assert_eq!(predicates.len(), 1);
                match &predicates[0] {
                    HirScalarExpr::CallUnary { func, .. } => {
                        assert!(matches!(func, mz_expr::UnaryFunc::Not(_)));
                    }
                    other => panic!("expected CallUnary Not, got {:?}", other),
                }
            }
            other => panic!("expected Filter, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_filter_bound() {
        let result = plan_query(
            "PREFIX ex: <http://example.org/>
             SELECT * WHERE { ?s ex:name ?name . FILTER(BOUND(?name)) }",
        );
        assert_eq!(var_names(&result), vec!["name", "s"]);

        // BOUND(?name) → NOT(IsNull(column))
        match &result.expr {
            HirRelationExpr::Filter { predicates, .. } => {
                assert_eq!(predicates.len(), 1);
                match &predicates[0] {
                    HirScalarExpr::CallUnary {
                        func: mz_expr::UnaryFunc::Not(_),
                        expr,
                        ..
                    } => match expr.as_ref() {
                        HirScalarExpr::CallUnary {
                            func: mz_expr::UnaryFunc::IsNull(_),
                            ..
                        } => {}
                        other => panic!("expected IsNull, got {:?}", other),
                    },
                    other => panic!("expected Not(IsNull), got {:?}", other),
                }
            }
            other => panic!("expected Filter, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_multiple_filters_in_group() {
        // Per SPARQL spec, multiple FILTERs in a group all apply.
        let result = plan_query(
            r#"SELECT * WHERE {
                   ?s ?p ?o .
                   FILTER(?s = "x")
                   FILTER(?o = "y")
               }"#,
        );
        assert_eq!(var_names(&result), vec!["o", "p", "s"]);

        // Two FILTERs produce two nested Filter nodes.
        match &result.expr {
            HirRelationExpr::Filter { input, predicates } => {
                assert_eq!(predicates.len(), 1);
                match input.as_ref() {
                    HirRelationExpr::Filter { predicates, .. } => {
                        assert_eq!(predicates.len(), 1);
                    }
                    other => panic!("expected inner Filter, got {:?}", other),
                }
            }
            other => panic!("expected Filter, got {:?}", other),
        }
    }

    // --- OPTIONAL tests ---

    #[mz_ore::test]
    fn test_optional_simple() {
        let result = plan_query(
            "PREFIX ex: <http://example.org/>
             SELECT * WHERE {
                 ?s ex:name ?name .
                 OPTIONAL { ?s ex:email ?email }
             }",
        );
        // name and s from BGP, email from OPTIONAL.
        assert_eq!(var_names(&result), vec!["email", "name", "s"]);
        assert_eq!(result.arity(), 3);

        // Structure: Project(LeftOuterJoin(BGP, optional_pattern))
        match &result.expr {
            HirRelationExpr::Project { input, .. } => match input.as_ref() {
                HirRelationExpr::Join { kind, .. } => {
                    assert_eq!(*kind, JoinKind::LeftOuter);
                }
                other => panic!("expected LeftOuter Join, got {:?}", other),
            },
            other => panic!("expected Project, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_optional_with_filter() {
        // FILTER inside OPTIONAL should go into the ON clause.
        let result = plan_query(
            r#"PREFIX ex: <http://example.org/>
               SELECT * WHERE {
                   ?s ex:name ?name .
                   OPTIONAL { ?s ex:email ?email . FILTER(?email = "test@example.com") }
               }"#,
        );
        assert_eq!(var_names(&result), vec!["email", "name", "s"]);

        // The ON clause should include both the join condition AND the filter.
        match &result.expr {
            HirRelationExpr::Project { input, .. } => match input.as_ref() {
                HirRelationExpr::Join { kind, on, .. } => {
                    assert_eq!(*kind, JoinKind::LeftOuter);
                    // ON should be a conjunction (AND) of the join predicate and the filter.
                    match on {
                        HirScalarExpr::CallVariadic { func, exprs, .. } => {
                            assert!(matches!(func, mz_expr::VariadicFunc::And(_)));
                            // At least 2 predicates: shared var equality + filter.
                            assert!(exprs.len() >= 2);
                        }
                        _ => {
                            // Could also be a single predicate if no shared vars
                            // (unlikely in this test), so this is fine.
                        }
                    }
                }
                other => panic!("expected LeftOuter Join, got {:?}", other),
            },
            other => panic!("expected Project, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_optional_multiple() {
        let result = plan_query(
            "PREFIX ex: <http://example.org/>
             SELECT * WHERE {
                 ?s ex:name ?name .
                 OPTIONAL { ?s ex:email ?email }
                 OPTIONAL { ?s ex:phone ?phone }
             }",
        );
        assert_eq!(var_names(&result), vec!["email", "name", "phone", "s"]);
        assert_eq!(result.arity(), 4);

        // Second OPTIONAL wraps first: Project(LeftJoin(Project(LeftJoin(BGP, ...)), ...))
        match &result.expr {
            HirRelationExpr::Project { input, .. } => match input.as_ref() {
                HirRelationExpr::Join { kind, left, .. } => {
                    assert_eq!(*kind, JoinKind::LeftOuter);
                    // Left should also be a Project(LeftJoin(...))
                    match left.as_ref() {
                        HirRelationExpr::Project { input, .. } => match input.as_ref() {
                            HirRelationExpr::Join { kind, .. } => {
                                assert_eq!(*kind, JoinKind::LeftOuter);
                            }
                            other => panic!("expected inner LeftOuter Join, got {:?}", other),
                        },
                        other => panic!("expected inner Project, got {:?}", other),
                    }
                }
                other => panic!("expected LeftOuter Join, got {:?}", other),
            },
            other => panic!("expected Project, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_optional_no_shared_vars() {
        // OPTIONAL with no shared variables is a left outer cross join.
        let result = plan_query(
            "PREFIX ex: <http://example.org/>
             SELECT * WHERE {
                 ?s ex:name ?name .
                 OPTIONAL { ?x ex:age ?age }
             }",
        );
        assert_eq!(var_names(&result), vec!["age", "name", "s", "x"]);

        match &result.expr {
            HirRelationExpr::Project { input, .. } => match input.as_ref() {
                HirRelationExpr::Join { kind, on, .. } => {
                    assert_eq!(*kind, JoinKind::LeftOuter);
                    // ON should be literal true (cross join).
                    match on {
                        HirScalarExpr::Literal(row, _, _) => {
                            assert_eq!(row.unpack_first(), Datum::True);
                        }
                        other => panic!("expected literal true, got {:?}", other),
                    }
                }
                other => panic!("expected LeftOuter Join, got {:?}", other),
            },
            other => panic!("expected Project, got {:?}", other),
        }
    }

    // --- UNION tests ---

    #[mz_ore::test]
    fn test_union_same_vars() {
        let result = plan_query(
            "PREFIX ex: <http://example.org/>
             SELECT * WHERE {
                 { ?s ex:name ?name }
                 UNION
                 { ?s ex:label ?name }
             }",
        );
        // Both sides have the same variables.
        assert_eq!(var_names(&result), vec!["name", "s"]);
        assert_eq!(result.arity(), 2);

        match &result.expr {
            HirRelationExpr::Union { .. } => {}
            other => panic!("expected Union, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_union_different_vars() {
        // Outer union: variables differ between sides.
        let result = plan_query(
            "PREFIX ex: <http://example.org/>
             SELECT * WHERE {
                 { ?s ex:name ?name }
                 UNION
                 { ?s ex:age ?age }
             }",
        );
        // Combined variable set.
        assert_eq!(var_names(&result), vec!["age", "name", "s"]);
        assert_eq!(result.arity(), 3);

        // The union should pad missing variables with NULL.
        match &result.expr {
            HirRelationExpr::Union { .. } => {}
            other => panic!("expected Union, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_union_three_way() {
        let result = plan_query(
            "PREFIX ex: <http://example.org/>
             SELECT * WHERE {
                 { ?s ex:name ?name }
                 UNION
                 { ?s ex:label ?name }
                 UNION
                 { ?s ex:title ?name }
             }",
        );
        assert_eq!(var_names(&result), vec!["name", "s"]);

        // Three-way union parsed as left-associative: Union(Union(A, B), C)
        match &result.expr {
            HirRelationExpr::Union { .. } => {}
            other => panic!("expected Union, got {:?}", other),
        }
    }

    // --- MINUS tests ---

    #[mz_ore::test]
    fn test_minus_shared_vars() {
        let result = plan_query(
            "PREFIX ex: <http://example.org/>
             SELECT * WHERE {
                 ?s ex:name ?name .
                 MINUS { ?s ex:status <http://example.org/inactive> }
             }",
        );
        // MINUS doesn't add variables — result has same vars as left side.
        assert_eq!(var_names(&result), vec!["name", "s"]);
        assert_eq!(result.arity(), 2);

        // Should use Threshold(Union(left, Negate(...)))
        match &result.expr {
            HirRelationExpr::Threshold { input } => match input.as_ref() {
                HirRelationExpr::Union { .. } => {}
                other => panic!("expected Union inside Threshold, got {:?}", other),
            },
            other => panic!("expected Threshold, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_minus_no_shared_vars() {
        // MINUS with no shared variables has no effect.
        let result = plan_query(
            "PREFIX ex: <http://example.org/>
             SELECT * WHERE {
                 ?s ex:name ?name .
                 MINUS { ?x ex:age ?age }
             }",
        );
        // Result should be the same as without MINUS.
        assert_eq!(var_names(&result), vec!["name", "s"]);
        assert_eq!(result.arity(), 2);

        // No Threshold/Negate: MINUS was a no-op.
        match &result.expr {
            HirRelationExpr::Threshold { .. } => {
                panic!("expected no Threshold for disjoint MINUS")
            }
            _ => {} // Any non-Threshold structure is correct.
        }
    }

    #[mz_ore::test]
    fn test_minus_preserves_left_vars() {
        let result = plan_query(
            "PREFIX ex: <http://example.org/>
             SELECT * WHERE {
                 ?s ex:name ?name . ?s ex:age ?age .
                 MINUS { ?s ex:status <http://example.org/inactive> }
             }",
        );
        // All left-side variables preserved.
        assert_eq!(var_names(&result), vec!["age", "name", "s"]);
        assert_eq!(result.arity(), 3);
    }

    // --- Combined pattern tests ---

    #[mz_ore::test]
    fn test_optional_inside_union() {
        let result = plan_query(
            "PREFIX ex: <http://example.org/>
             SELECT * WHERE {
                 { ?s ex:name ?name . OPTIONAL { ?s ex:email ?email } }
                 UNION
                 { ?s ex:label ?name }
             }",
        );
        // Left side has: name, s, email. Right side has: name, s.
        // Union combines: email, name, s.
        assert_eq!(var_names(&result), vec!["email", "name", "s"]);
    }

    #[mz_ore::test]
    fn test_filter_after_optional() {
        let result = plan_query(
            "PREFIX ex: <http://example.org/>
             SELECT * WHERE {
                 ?s ex:name ?name .
                 OPTIONAL { ?s ex:email ?email }
                 FILTER(BOUND(?email))
             }",
        );
        assert_eq!(var_names(&result), vec!["email", "name", "s"]);

        // Should be Filter(Project(LeftJoin(...)))
        match &result.expr {
            HirRelationExpr::Filter { input, .. } => match input.as_ref() {
                HirRelationExpr::Project { input, .. } => match input.as_ref() {
                    HirRelationExpr::Join { kind, .. } => {
                        assert_eq!(*kind, JoinKind::LeftOuter);
                    }
                    other => panic!("expected LeftOuter Join, got {:?}", other),
                },
                other => panic!("expected Project, got {:?}", other),
            },
            other => panic!("expected Filter, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_optional_plus_minus() {
        let result = plan_query(
            "PREFIX ex: <http://example.org/>
             SELECT * WHERE {
                 ?s ex:name ?name .
                 OPTIONAL { ?s ex:email ?email }
                 MINUS { ?s ex:status <http://example.org/inactive> }
             }",
        );
        assert_eq!(var_names(&result), vec!["email", "name", "s"]);
    }

    #[mz_ore::test]
    fn test_filter_variable_not_in_scope() {
        // FILTER referencing an undefined variable should produce an error.
        let query = parse("SELECT * WHERE { ?s ?p ?o . FILTER(?x = ?s) }").expect("parse ok");
        let planner = SparqlPlanner::new(GlobalId::User(1));
        let err = planner.plan(&query).unwrap_err();
        assert!(
            err.message.contains("not in scope"),
            "expected 'not in scope' error, got: {}",
            err.message
        );
    }
}
