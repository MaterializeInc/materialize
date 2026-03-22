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
    VarOrTerm, Variable, VerbPath,
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
            GroupGraphPattern::Bind(expr, var) => {
                // Bare BIND without preceding patterns: apply to empty row.
                let left = self.empty_relation();
                self.plan_bind(left, expr, var)
            }
            GroupGraphPattern::Values { variables, rows } => self.plan_values(variables, rows),
            GroupGraphPattern::SubSelect(_) => Err(PlanError {
                message: "subqueries not yet supported (will be implemented in prompt 10)"
                    .to_string(),
            }),
            GroupGraphPattern::Graph(_, _) => Err(PlanError {
                message: "GRAPH patterns not yet supported (will be implemented in a later prompt)"
                    .to_string(),
            }),
            GroupGraphPattern::Service { .. } => Err(PlanError {
                message: "SERVICE patterns not supported".to_string(),
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
                GroupGraphPattern::Bind(expr, var) => {
                    result = self.plan_bind(result, expr, var)?;
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

    /// Plans `BIND(expr AS ?var)` — adds a computed column to the current relation.
    ///
    /// The expression is evaluated in the context of the current variable scope,
    /// and the result is bound to the new variable.
    fn plan_bind(
        &self,
        input: PlannedRelation,
        expr: &Expression,
        var: &Variable,
    ) -> Result<PlannedRelation, PlanError> {
        let hir_expr = self.translate_expression(expr, &input.var_map)?;
        let new_col = input.arity();
        let mapped = HirRelationExpr::Map {
            input: Box::new(input.expr),
            scalars: vec![hir_expr],
        };
        let mut new_var_map = input.var_map;
        new_var_map.insert(var.name.clone(), new_col);
        Ok(PlannedRelation {
            expr: mapped,
            var_map: new_var_map,
        })
    }

    /// Plans `VALUES (?var1 ?var2) { (val1 val2) ... }` — inline data.
    ///
    /// Each row becomes a row in a `Constant` relation. UNDEF values become NULL.
    fn plan_values(
        &self,
        variables: &[Variable],
        rows: &[Vec<Option<GraphTerm>>],
    ) -> Result<PlannedRelation, PlanError> {
        let var_map: BTreeMap<String, usize> = variables
            .iter()
            .enumerate()
            .map(|(i, v)| (v.name.clone(), i))
            .collect();

        let typ = SqlRelationType::new(
            variables
                .iter()
                .map(|_| SqlColumnType {
                    scalar_type: SqlScalarType::String,
                    nullable: true,
                })
                .collect(),
        );

        let datum_rows: Vec<mz_repr::Row> = rows
            .iter()
            .map(|row| {
                // Compute owned strings first so they live long enough.
                let strings: Vec<Option<String>> = row
                    .iter()
                    .map(|val| val.as_ref().map(|term| self.term_to_string(term)))
                    .collect();
                let datums: Vec<Datum> = strings
                    .iter()
                    .map(|s| match s {
                        Some(s) => Datum::String(s.as_str()),
                        None => Datum::Null,
                    })
                    .collect();
                mz_repr::Row::pack_slice(&datums)
            })
            .collect();

        Ok(PlannedRelation {
            expr: HirRelationExpr::Constant {
                rows: datum_rows,
                typ,
            },
            var_map,
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

            // Arithmetic operators — operate on string-encoded numeric values.
            // We cast strings to float64 for arithmetic, then back to string.
            Expression::Add(l, r) => self.translate_arithmetic(
                mz_expr::BinaryFunc::AddFloat64(mz_expr::func::AddFloat64),
                l,
                r,
                var_map,
            ),
            Expression::Subtract(l, r) => self.translate_arithmetic(
                mz_expr::BinaryFunc::SubFloat64(mz_expr::func::SubFloat64),
                l,
                r,
                var_map,
            ),
            Expression::Multiply(l, r) => self.translate_arithmetic(
                mz_expr::BinaryFunc::MulFloat64(mz_expr::func::MulFloat64),
                l,
                r,
                var_map,
            ),
            Expression::Divide(l, r) => self.translate_arithmetic(
                mz_expr::BinaryFunc::DivFloat64(mz_expr::func::DivFloat64),
                l,
                r,
                var_map,
            ),
            Expression::UnaryPlus(e) => {
                // Unary plus is identity on numeric values.
                self.translate_expression(e, var_map)
            }
            Expression::UnaryMinus(e) => {
                let inner = self.to_float64(self.translate_expression(e, var_map)?);
                let negated = HirScalarExpr::CallUnary {
                    func: mz_expr::UnaryFunc::NegFloat64(mz_expr::func::NegFloat64),
                    expr: Box::new(inner),
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                };
                Ok(self.from_float64(negated))
            }

            // IF(cond, then, else)
            Expression::If(cond, then, els) => {
                let cond = self.translate_expression(cond, var_map)?;
                let then = self.translate_expression(then, var_map)?;
                let els = self.translate_expression(els, var_map)?;
                Ok(HirScalarExpr::If {
                    cond: Box::new(cond),
                    then: Box::new(then),
                    els: Box::new(els),
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                })
            }

            // COALESCE(expr, ...)
            Expression::Coalesce(exprs) => {
                let hir_exprs: Result<Vec<_>, _> = exprs
                    .iter()
                    .map(|e| self.translate_expression(e, var_map))
                    .collect();
                Ok(HirScalarExpr::CallVariadic {
                    func: mz_expr::VariadicFunc::Coalesce(mz_expr::func::variadic::Coalesce),
                    exprs: hir_exprs?,
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                })
            }

            // EXISTS { pattern } — correlated subquery
            Expression::Exists(pattern) => {
                let planned = self.plan_pattern(pattern)?;
                // Apply variable bindings from outer scope via filters.
                let correlated = self.correlate_subquery(planned, var_map)?;
                Ok(HirScalarExpr::Exists(
                    Box::new(correlated),
                    mz_ore::treat_as_equal::TreatAsEqual(None),
                ))
            }

            // NOT EXISTS { pattern }
            Expression::NotExists(pattern) => {
                let planned = self.plan_pattern(pattern)?;
                let correlated = self.correlate_subquery(planned, var_map)?;
                let exists = HirScalarExpr::Exists(
                    Box::new(correlated),
                    mz_ore::treat_as_equal::TreatAsEqual(None),
                );
                Ok(HirScalarExpr::CallUnary {
                    func: mz_expr::UnaryFunc::Not(mz_expr::func::Not),
                    expr: Box::new(exists),
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                })
            }

            // Type tests — best-effort on string encoding.
            // IRI values are bare strings without quotes or `_:` prefix.
            Expression::IsIri(e) => {
                // An IRI in our encoding doesn't start with `"` or `_:`.
                // This is an approximation — simple literals are also bare strings.
                let inner = self.translate_expression(e, var_map)?;
                let not_blank = HirScalarExpr::CallUnary {
                    func: mz_expr::UnaryFunc::Not(mz_expr::func::Not),
                    expr: Box::new(self.string_starts_with(inner.clone(), "_:")),
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                };
                let not_quoted = HirScalarExpr::CallUnary {
                    func: mz_expr::UnaryFunc::Not(mz_expr::func::Not),
                    expr: Box::new(self.string_starts_with(inner.clone(), "\"")),
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                };
                let not_null = HirScalarExpr::CallUnary {
                    func: mz_expr::UnaryFunc::Not(mz_expr::func::Not),
                    expr: Box::new(HirScalarExpr::CallUnary {
                        func: mz_expr::UnaryFunc::IsNull(mz_expr::func::IsNull),
                        expr: Box::new(inner),
                        name: mz_ore::treat_as_equal::TreatAsEqual(None),
                    }),
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                };
                Ok(HirScalarExpr::CallVariadic {
                    func: mz_expr::VariadicFunc::And(mz_expr::func::variadic::And),
                    exprs: vec![not_null, not_blank, not_quoted],
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                })
            }
            Expression::IsBlank(e) => {
                let inner = self.translate_expression(e, var_map)?;
                Ok(self.string_starts_with(inner, "_:"))
            }
            Expression::IsLiteral(e) => {
                // Literals in our encoding start with `"` or are bare simple strings.
                // This is approximate — we check that it's not an IRI-like pattern
                // and not a blank node.
                let inner = self.translate_expression(e, var_map)?;
                let not_blank = HirScalarExpr::CallUnary {
                    func: mz_expr::UnaryFunc::Not(mz_expr::func::Not),
                    expr: Box::new(self.string_starts_with(inner.clone(), "_:")),
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                };
                // For now, approximate: anything that's not a blank node and not null.
                let not_null = HirScalarExpr::CallUnary {
                    func: mz_expr::UnaryFunc::Not(mz_expr::func::Not),
                    expr: Box::new(HirScalarExpr::CallUnary {
                        func: mz_expr::UnaryFunc::IsNull(mz_expr::func::IsNull),
                        expr: Box::new(inner),
                        name: mz_ore::treat_as_equal::TreatAsEqual(None),
                    }),
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                };
                Ok(HirScalarExpr::CallVariadic {
                    func: mz_expr::VariadicFunc::And(mz_expr::func::variadic::And),
                    exprs: vec![not_null, not_blank],
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                })
            }
            Expression::IsNumeric(e) => {
                // Best-effort: try to cast to float64 and check if it succeeds.
                // For now, use a regex match for numeric strings.
                let inner = self.translate_expression(e, var_map)?;
                Ok(HirScalarExpr::CallBinary {
                    func: mz_expr::BinaryFunc::IsRegexpMatchCaseSensitive(
                        mz_expr::func::IsRegexpMatchCaseSensitive,
                    ),
                    expr1: Box::new(inner),
                    expr2: Box::new(HirScalarExpr::literal(
                        Datum::String("^-?[0-9]+(\\.[0-9]+)?([eE][+-]?[0-9]+)?$"),
                        SqlScalarType::String,
                    )),
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                })
            }

            // Accessor functions — extract parts of RDF term encoding.
            Expression::Str(e) => {
                // STR() returns the lexical form. For simple strings, that's the
                // value itself. For tagged/typed literals, extract the value.
                // For IRIs, return the IRI string.
                // Simplification: return the value as-is (correct for IRIs and
                // simple literals; typed/tagged literal extraction deferred to prompt 17).
                self.translate_expression(e, var_map)
            }
            Expression::Lang(e) => {
                // LANG() returns the language tag of a literal.
                // In our encoding, language-tagged literals are `"value"@lang`.
                // For non-tagged values, return "".
                // Full implementation deferred to prompt 17; return "" for now.
                let _inner = self.translate_expression(e, var_map)?;
                Ok(HirScalarExpr::literal(
                    Datum::String(""),
                    SqlScalarType::String,
                ))
            }
            Expression::Datatype(e) => {
                // DATATYPE() returns the datatype IRI.
                // Full implementation deferred to prompt 17; return xsd:string for now.
                let _inner = self.translate_expression(e, var_map)?;
                Ok(HirScalarExpr::literal(
                    Datum::String("http://www.w3.org/2001/XMLSchema#string"),
                    SqlScalarType::String,
                ))
            }

            // String functions — operate on string values directly.
            Expression::Strlen(e) => {
                let inner = self.translate_expression(e, var_map)?;
                // CharLength returns Int32; cast to string for RDF consistency.
                let len = HirScalarExpr::CallUnary {
                    func: mz_expr::UnaryFunc::CharLength(mz_expr::func::CharLength),
                    expr: Box::new(inner),
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                };
                Ok(HirScalarExpr::CallUnary {
                    func: mz_expr::UnaryFunc::CastInt32ToString(mz_expr::func::CastInt32ToString),
                    expr: Box::new(len),
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                })
            }
            Expression::Ucase(e) => {
                let inner = self.translate_expression(e, var_map)?;
                Ok(HirScalarExpr::CallUnary {
                    func: mz_expr::UnaryFunc::Upper(mz_expr::func::Upper),
                    expr: Box::new(inner),
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                })
            }
            Expression::Lcase(e) => {
                let inner = self.translate_expression(e, var_map)?;
                Ok(HirScalarExpr::CallUnary {
                    func: mz_expr::UnaryFunc::Lower(mz_expr::func::Lower),
                    expr: Box::new(inner),
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                })
            }
            Expression::Contains(haystack, needle) => {
                // CONTAINS(str, substr) — use regex: needle is a literal substring.
                // We use position() > 0 as an alternative.
                let h = self.translate_expression(haystack, var_map)?;
                let n = self.translate_expression(needle, var_map)?;
                let pos = HirScalarExpr::CallBinary {
                    func: mz_expr::BinaryFunc::Position(mz_expr::func::Position),
                    expr1: Box::new(h),
                    expr2: Box::new(n),
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                };
                Ok(HirScalarExpr::CallBinary {
                    func: mz_expr::BinaryFunc::Gt(mz_expr::func::Gt),
                    expr1: Box::new(pos),
                    expr2: Box::new(HirScalarExpr::literal(
                        Datum::Int32(0),
                        SqlScalarType::Int32,
                    )),
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                })
            }
            Expression::StrStarts(string, prefix) => {
                let s = self.translate_expression(string, var_map)?;
                let p = self.translate_expression(prefix, var_map)?;
                Ok(HirScalarExpr::CallBinary {
                    func: mz_expr::BinaryFunc::StartsWith(mz_expr::func::StartsWith),
                    expr1: Box::new(s),
                    expr2: Box::new(p),
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                })
            }
            Expression::StrEnds(string, suffix) => {
                // STRENDS: use position of suffix at end.
                // Implement as: right(str, strlen(suffix)) = suffix.
                // Or use regex: str ~ (suffix || '$').
                // Simplest: negate "not ends with" using a regex.
                let s = self.translate_expression(string, var_map)?;
                let suf = self.translate_expression(suffix, var_map)?;
                // Use: position(reverse(string), reverse(suffix)) = 1
                // Actually simpler: use Right(string, charlen(suffix)) = suffix
                let suffix_len = HirScalarExpr::CallUnary {
                    func: mz_expr::UnaryFunc::CharLength(mz_expr::func::CharLength),
                    expr: Box::new(suf.clone()),
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                };
                let right_part = HirScalarExpr::CallBinary {
                    func: mz_expr::BinaryFunc::Right(mz_expr::func::Right),
                    expr1: Box::new(s),
                    expr2: Box::new(HirScalarExpr::CallUnary {
                        func: mz_expr::UnaryFunc::CastInt32ToInt64(mz_expr::func::CastInt32ToInt64),
                        expr: Box::new(suffix_len),
                        name: mz_ore::treat_as_equal::TreatAsEqual(None),
                    }),
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                };
                Ok(HirScalarExpr::CallBinary {
                    func: mz_expr::BinaryFunc::Eq(mz_expr::func::Eq),
                    expr1: Box::new(right_part),
                    expr2: Box::new(suf),
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                })
            }
            Expression::Substr(string, start, len) => {
                let s = self.translate_expression(string, var_map)?;
                let start_expr = self.translate_expression(start, var_map)?;
                let start_i64 = self.to_int64(start_expr);
                let mut args = vec![s, start_i64];
                if let Some(length) = len {
                    let len_expr = self.translate_expression(length, var_map)?;
                    args.push(self.to_int64(len_expr));
                }
                Ok(HirScalarExpr::CallVariadic {
                    func: mz_expr::VariadicFunc::Substr(mz_expr::func::variadic::Substr),
                    exprs: args,
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                })
            }
            Expression::Concat(exprs) => {
                let hir_exprs: Result<Vec<_>, _> = exprs
                    .iter()
                    .map(|e| self.translate_expression(e, var_map))
                    .collect();
                Ok(HirScalarExpr::CallVariadic {
                    func: mz_expr::VariadicFunc::Concat(mz_expr::func::variadic::Concat),
                    exprs: hir_exprs?,
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                })
            }
            Expression::Replace(string, pattern, replacement, flags) => {
                let s = self.translate_expression(string, var_map)?;
                let p = self.translate_expression(pattern, var_map)?;
                let r = self.translate_expression(replacement, var_map)?;
                let mut args = vec![s, p, r];
                if let Some(f) = flags {
                    args.push(self.translate_expression(f, var_map)?);
                }
                Ok(HirScalarExpr::CallVariadic {
                    func: mz_expr::VariadicFunc::Replace(mz_expr::func::variadic::Replace),
                    exprs: args,
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                })
            }
            Expression::Regex(string, pattern, flags) => {
                let s = self.translate_expression(string, var_map)?;
                let p = self.translate_expression(pattern, var_map)?;
                if let Some(f) = flags {
                    let f_expr = self.translate_expression(f, var_map)?;
                    // With flags, use case-insensitive if flags contain 'i'.
                    // For simplicity, use the regex match with flags via RegexpMatch.
                    // Actually, we just need a boolean result.
                    // Use IsRegexpMatchCaseInsensitive when flags indicate 'i'.
                    let _ = f_expr;
                    Ok(HirScalarExpr::CallBinary {
                        func: mz_expr::BinaryFunc::IsRegexpMatchCaseInsensitive(
                            mz_expr::func::IsRegexpMatchCaseInsensitive,
                        ),
                        expr1: Box::new(s),
                        expr2: Box::new(p),
                        name: mz_ore::treat_as_equal::TreatAsEqual(None),
                    })
                } else {
                    Ok(HirScalarExpr::CallBinary {
                        func: mz_expr::BinaryFunc::IsRegexpMatchCaseSensitive(
                            mz_expr::func::IsRegexpMatchCaseSensitive,
                        ),
                        expr1: Box::new(s),
                        expr2: Box::new(p),
                        name: mz_ore::treat_as_equal::TreatAsEqual(None),
                    })
                }
            }

            // IN / NOT IN — expand to OR chain of equality checks.
            Expression::In(value, list) => {
                let v = self.translate_expression(value, var_map)?;
                let checks: Result<Vec<_>, _> = list
                    .iter()
                    .map(|e| {
                        let item = self.translate_expression(e, var_map)?;
                        Ok(HirScalarExpr::CallBinary {
                            func: mz_expr::BinaryFunc::Eq(mz_expr::func::Eq),
                            expr1: Box::new(v.clone()),
                            expr2: Box::new(item),
                            name: mz_ore::treat_as_equal::TreatAsEqual(None),
                        })
                    })
                    .collect();
                let checks = checks?;
                if checks.is_empty() {
                    Ok(HirScalarExpr::literal_false())
                } else if checks.len() == 1 {
                    Ok(checks.into_iter().next().unwrap())
                } else {
                    Ok(HirScalarExpr::CallVariadic {
                        func: mz_expr::VariadicFunc::Or(mz_expr::func::variadic::Or),
                        exprs: checks,
                        name: mz_ore::treat_as_equal::TreatAsEqual(None),
                    })
                }
            }
            Expression::NotIn(value, list) => {
                let in_expr = self
                    .translate_expression(&Expression::In(value.clone(), list.clone()), var_map)?;
                Ok(HirScalarExpr::CallUnary {
                    func: mz_expr::UnaryFunc::Not(mz_expr::func::Not),
                    expr: Box::new(in_expr),
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                })
            }

            // Generic function call — not yet supported.
            Expression::FunctionCall(iri, _args) => Err(PlanError {
                message: format!("custom function call <{}> not supported", iri.value),
            }),

            // Aggregate functions — handled in prompt 10 (SELECT projection).
            Expression::Count { .. }
            | Expression::Sum { .. }
            | Expression::Avg { .. }
            | Expression::Min { .. }
            | Expression::Max { .. }
            | Expression::GroupConcat { .. }
            | Expression::Sample { .. } => Err(PlanError {
                message: "aggregate functions are handled during SELECT projection (prompt 10)"
                    .to_string(),
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

    /// Translates a binary arithmetic expression.
    ///
    /// Casts both operands from string to float64, applies the operation,
    /// and casts the result back to string for RDF storage.
    fn translate_arithmetic(
        &self,
        func: mz_expr::BinaryFunc,
        left: &Expression,
        right: &Expression,
        var_map: &BTreeMap<String, usize>,
    ) -> Result<HirScalarExpr, PlanError> {
        let l = self.to_float64(self.translate_expression(left, var_map)?);
        let r = self.to_float64(self.translate_expression(right, var_map)?);
        let result = HirScalarExpr::CallBinary {
            func,
            expr1: Box::new(l),
            expr2: Box::new(r),
            name: mz_ore::treat_as_equal::TreatAsEqual(None),
        };
        Ok(self.from_float64(result))
    }

    /// Casts a string expression to float64.
    fn to_float64(&self, expr: HirScalarExpr) -> HirScalarExpr {
        HirScalarExpr::CallUnary {
            func: mz_expr::UnaryFunc::CastStringToFloat64(mz_expr::func::CastStringToFloat64),
            expr: Box::new(expr),
            name: mz_ore::treat_as_equal::TreatAsEqual(None),
        }
    }

    /// Casts a float64 expression back to string.
    fn from_float64(&self, expr: HirScalarExpr) -> HirScalarExpr {
        HirScalarExpr::CallUnary {
            func: mz_expr::UnaryFunc::CastFloat64ToString(mz_expr::func::CastFloat64ToString),
            expr: Box::new(expr),
            name: mz_ore::treat_as_equal::TreatAsEqual(None),
        }
    }

    /// Casts a string expression to int64 (for SUBSTR position/length).
    fn to_int64(&self, expr: HirScalarExpr) -> HirScalarExpr {
        HirScalarExpr::CallUnary {
            func: mz_expr::UnaryFunc::CastStringToInt64(mz_expr::func::CastStringToInt64),
            expr: Box::new(expr),
            name: mz_ore::treat_as_equal::TreatAsEqual(None),
        }
    }

    /// Checks if a string expression starts with a given prefix.
    fn string_starts_with(&self, expr: HirScalarExpr, prefix: &str) -> HirScalarExpr {
        HirScalarExpr::CallBinary {
            func: mz_expr::BinaryFunc::StartsWith(mz_expr::func::StartsWith),
            expr1: Box::new(expr),
            expr2: Box::new(HirScalarExpr::literal(
                Datum::String(prefix),
                SqlScalarType::String,
            )),
            name: mz_ore::treat_as_equal::TreatAsEqual(None),
        }
    }

    /// Creates a correlated subquery for EXISTS/NOT EXISTS.
    ///
    /// Filters the inner pattern's result on equality with outer-scope variables
    /// that appear in the inner pattern.
    fn correlate_subquery(
        &self,
        inner: PlannedRelation,
        outer_var_map: &BTreeMap<String, usize>,
    ) -> Result<HirRelationExpr, PlanError> {
        // Find variables shared between outer scope and inner pattern.
        let mut filters = Vec::new();
        for (var_name, &inner_col) in &inner.var_map {
            if let Some(&outer_col) = outer_var_map.get(var_name) {
                // Correlate: inner.var = outer.var (outer ref via column).
                filters.push(HirScalarExpr::CallBinary {
                    func: mz_expr::BinaryFunc::Eq(mz_expr::func::Eq),
                    expr1: Box::new(HirScalarExpr::column(inner_col)),
                    expr2: Box::new(HirScalarExpr::column(outer_col)),
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                });
            }
        }
        if filters.is_empty() {
            Ok(inner.expr)
        } else {
            Ok(HirRelationExpr::Filter {
                input: Box::new(inner.expr),
                predicates: filters,
            })
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

    // --- BIND tests (Prompt 9) ---

    #[mz_ore::test]
    fn test_bind_simple() {
        let result = plan_query(
            "PREFIX ex: <http://example.org/>
             SELECT * WHERE { ?s ex:name ?name . BIND(UCASE(?name) AS ?upper) }",
        );
        assert_eq!(var_names(&result), vec!["name", "s", "upper"]);
        assert_eq!(result.arity(), 3);
        // BIND adds a Map node.
        // Structure: Filter(Map(Join or Filter or ..., [upper_expr]))
        // The group has filters collected and applied after BIND, but here there
        // are no FILTERs, so it's just Map on the BGP join result.
    }

    #[mz_ore::test]
    fn test_bind_arithmetic() {
        let result = plan_query("SELECT * WHERE { ?s ?p ?o . BIND((?o + \"1\") AS ?incremented) }");
        assert_eq!(var_names(&result), vec!["incremented", "o", "p", "s"]);
        assert_eq!(result.arity(), 4);
        // Verify the Map expression exists.
        match &result.expr {
            HirRelationExpr::Map { scalars, .. } => {
                assert_eq!(scalars.len(), 1);
                // The arithmetic should produce CastFloat64ToString(AddFloat64(...)).
                match &scalars[0] {
                    HirScalarExpr::CallUnary { func, .. } => {
                        assert!(
                            matches!(func, mz_expr::UnaryFunc::CastFloat64ToString(_)),
                            "expected CastFloat64ToString, got {:?}",
                            func
                        );
                    }
                    other => panic!("expected CallUnary (cast), got {:?}", other),
                }
            }
            other => panic!("expected Map, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_bind_with_filter() {
        // BIND introduces ?len, FILTER uses it.
        let result = plan_query(
            "SELECT * WHERE {
                ?s ?p ?o .
                BIND(STRLEN(?o) AS ?len) .
                FILTER(?len > \"5\")
             }",
        );
        assert_eq!(var_names(&result), vec!["len", "o", "p", "s"]);
        // Structure: Filter on top (because FILTERs are collected and applied after all patterns).
        match &result.expr {
            HirRelationExpr::Filter { .. } => {}
            other => panic!(
                "expected Filter (group filter applied after BIND), got {:?}",
                other
            ),
        }
    }

    // --- VALUES tests (Prompt 9) ---

    #[mz_ore::test]
    fn test_values_single_variable() {
        let result = plan_query(
            "SELECT * WHERE {
                VALUES ?x { <http://example.org/a> <http://example.org/b> }
                ?x ?p ?o
             }",
        );
        assert_eq!(var_names(&result), vec!["o", "p", "x"]);
    }

    #[mz_ore::test]
    fn test_values_multi_variable() {
        let result = plan_query(
            "SELECT * WHERE {
                VALUES (?x ?y) { (<http://a.example/> \"1\") (<http://b.example/> \"2\") }
             }",
        );
        assert_eq!(var_names(&result), vec!["x", "y"]);
        assert_eq!(result.arity(), 2);
        // Should be a Constant with 2 rows.
        match &result.expr {
            HirRelationExpr::Constant { rows, .. } => {
                assert_eq!(rows.len(), 2);
            }
            other => panic!("expected Constant, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_values_with_undef() {
        let result = plan_query(
            "SELECT * WHERE {
                VALUES (?x ?y) { (<http://a.example/> UNDEF) (UNDEF \"2\") }
             }",
        );
        assert_eq!(var_names(&result), vec!["x", "y"]);
        // Verify NULL handling: first row has y=NULL, second has x=NULL.
        match &result.expr {
            HirRelationExpr::Constant { rows, .. } => {
                assert_eq!(rows.len(), 2);
                // Check first row: x is set, y is null.
                let datums0: Vec<_> = rows[0].iter().collect();
                assert!(!datums0[0].is_null()); // x
                assert!(datums0[1].is_null()); // y
                // Check second row: x is null, y is set.
                let datums1: Vec<_> = rows[1].iter().collect();
                assert!(datums1[0].is_null()); // x
                assert!(!datums1[1].is_null()); // y
            }
            other => panic!("expected Constant, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_values_joined_with_bgp() {
        // VALUES should inner-join with the BGP on shared variable ?type.
        let result = plan_query(
            "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
             SELECT * WHERE {
                VALUES ?type { <http://example.org/Person> <http://example.org/Org> }
                ?s rdf:type ?type
             }",
        );
        assert_eq!(var_names(&result), vec!["s", "type"]);
        // Structure: Project(Join(Constant, Project(Filter(Get))))
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

    // --- Expression tests (Prompt 9) ---

    #[mz_ore::test]
    fn test_filter_arithmetic_add() {
        // Addition should produce CastFloat64ToString(AddFloat64(cast, cast)).
        let result = plan_query("SELECT * WHERE { ?s ?p ?o . FILTER((?o + \"1\") = \"43\") }");
        assert_eq!(var_names(&result), vec!["o", "p", "s"]);
        match &result.expr {
            HirRelationExpr::Filter { predicates, .. } => {
                assert_eq!(predicates.len(), 1);
            }
            other => panic!("expected Filter, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_filter_arithmetic_subtract() {
        let result = plan_query("SELECT * WHERE { ?s ?p ?o . FILTER((?o - \"1\") = \"41\") }");
        assert_eq!(var_names(&result), vec!["o", "p", "s"]);
    }

    #[mz_ore::test]
    fn test_filter_arithmetic_multiply_divide() {
        let result = plan_query("SELECT * WHERE { ?s ?p ?o . FILTER((?o * \"2\") = \"84\") }");
        assert_eq!(var_names(&result), vec!["o", "p", "s"]);
        let result2 = plan_query("SELECT * WHERE { ?s ?p ?o . FILTER((?o / \"2\") = \"21\") }");
        assert_eq!(var_names(&result2), vec!["o", "p", "s"]);
    }

    #[mz_ore::test]
    fn test_filter_unary_minus() {
        let result = plan_query("SELECT * WHERE { ?s ?p ?o . FILTER(-?o = \"-42\") }");
        assert_eq!(var_names(&result), vec!["o", "p", "s"]);
    }

    #[mz_ore::test]
    fn test_expression_if() {
        let result = plan_query(
            "SELECT * WHERE {
                ?s ?p ?o .
                BIND(IF(?o = \"yes\", \"true\", \"false\") AS ?bool)
             }",
        );
        assert_eq!(var_names(&result), vec!["bool", "o", "p", "s"]);
        // The BIND should produce a Map with an If expression.
        match &result.expr {
            HirRelationExpr::Map { scalars, .. } => {
                assert_eq!(scalars.len(), 1);
                match &scalars[0] {
                    HirScalarExpr::If { .. } => {}
                    other => panic!("expected If, got {:?}", other),
                }
            }
            other => panic!("expected Map, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_expression_coalesce() {
        let result = plan_query(
            "SELECT * WHERE {
                ?s ?p ?o .
                BIND(COALESCE(?o, \"default\") AS ?val)
             }",
        );
        assert_eq!(var_names(&result), vec!["o", "p", "s", "val"]);
        match &result.expr {
            HirRelationExpr::Map { scalars, .. } => match &scalars[0] {
                HirScalarExpr::CallVariadic { func, exprs, .. } => {
                    assert!(matches!(func, mz_expr::VariadicFunc::Coalesce(_)));
                    assert_eq!(exprs.len(), 2);
                }
                other => panic!("expected CallVariadic(Coalesce), got {:?}", other),
            },
            other => panic!("expected Map, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_expression_string_functions() {
        // UCASE
        let result = plan_query("SELECT * WHERE { ?s ?p ?o . BIND(UCASE(?o) AS ?upper) }");
        assert_eq!(var_names(&result), vec!["o", "p", "s", "upper"]);
        match &result.expr {
            HirRelationExpr::Map { scalars, .. } => match &scalars[0] {
                HirScalarExpr::CallUnary { func, .. } => {
                    assert!(matches!(func, mz_expr::UnaryFunc::Upper(_)));
                }
                other => panic!("expected Upper, got {:?}", other),
            },
            other => panic!("expected Map, got {:?}", other),
        }

        // LCASE
        let result = plan_query("SELECT * WHERE { ?s ?p ?o . BIND(LCASE(?o) AS ?lower) }");
        match &result.expr {
            HirRelationExpr::Map { scalars, .. } => match &scalars[0] {
                HirScalarExpr::CallUnary { func, .. } => {
                    assert!(matches!(func, mz_expr::UnaryFunc::Lower(_)));
                }
                other => panic!("expected Lower, got {:?}", other),
            },
            other => panic!("expected Map, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_expression_strlen() {
        let result = plan_query("SELECT * WHERE { ?s ?p ?o . BIND(STRLEN(?o) AS ?len) }");
        assert_eq!(var_names(&result), vec!["len", "o", "p", "s"]);
        // STRLEN produces CastInt32ToString(CharLength(?o)).
        match &result.expr {
            HirRelationExpr::Map { scalars, .. } => match &scalars[0] {
                HirScalarExpr::CallUnary { func, .. } => {
                    assert!(matches!(func, mz_expr::UnaryFunc::CastInt32ToString(_)));
                }
                other => panic!("expected CastInt32ToString, got {:?}", other),
            },
            other => panic!("expected Map, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_expression_contains() {
        let result = plan_query("SELECT * WHERE { ?s ?p ?o . FILTER(CONTAINS(?o, \"test\")) }");
        assert_eq!(var_names(&result), vec!["o", "p", "s"]);
        // CONTAINS uses Position > 0.
        match &result.expr {
            HirRelationExpr::Filter { predicates, .. } => {
                assert_eq!(predicates.len(), 1);
                match &predicates[0] {
                    HirScalarExpr::CallBinary { func, .. } => {
                        assert!(matches!(func, mz_expr::BinaryFunc::Gt(_)));
                    }
                    other => panic!("expected Gt (Position > 0), got {:?}", other),
                }
            }
            other => panic!("expected Filter, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_expression_strstarts_strends() {
        let result = plan_query("SELECT * WHERE { ?s ?p ?o . FILTER(STRSTARTS(?o, \"http\")) }");
        match &result.expr {
            HirRelationExpr::Filter { predicates, .. } => match &predicates[0] {
                HirScalarExpr::CallBinary { func, .. } => {
                    assert!(matches!(func, mz_expr::BinaryFunc::StartsWith(_)));
                }
                other => panic!("expected StartsWith, got {:?}", other),
            },
            other => panic!("expected Filter, got {:?}", other),
        }

        let result = plan_query("SELECT * WHERE { ?s ?p ?o . FILTER(STRENDS(?o, \".org\")) }");
        // STRENDS uses Eq(Right(str, len), suffix).
        match &result.expr {
            HirRelationExpr::Filter { predicates, .. } => match &predicates[0] {
                HirScalarExpr::CallBinary { func, .. } => {
                    assert!(matches!(func, mz_expr::BinaryFunc::Eq(_)));
                }
                other => panic!("expected Eq (Right comparison), got {:?}", other),
            },
            other => panic!("expected Filter, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_expression_concat() {
        let result =
            plan_query("SELECT * WHERE { ?s ?p ?o . BIND(CONCAT(?s, \" \", ?o) AS ?combined) }");
        assert_eq!(var_names(&result), vec!["combined", "o", "p", "s"]);
        match &result.expr {
            HirRelationExpr::Map { scalars, .. } => match &scalars[0] {
                HirScalarExpr::CallVariadic { func, exprs, .. } => {
                    assert!(matches!(func, mz_expr::VariadicFunc::Concat(_)));
                    assert_eq!(exprs.len(), 3);
                }
                other => panic!("expected Concat, got {:?}", other),
            },
            other => panic!("expected Map, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_expression_regex() {
        let result = plan_query("SELECT * WHERE { ?s ?p ?o . FILTER(REGEX(?o, \"^test\")) }");
        match &result.expr {
            HirRelationExpr::Filter { predicates, .. } => match &predicates[0] {
                HirScalarExpr::CallBinary { func, .. } => {
                    assert!(matches!(
                        func,
                        mz_expr::BinaryFunc::IsRegexpMatchCaseSensitive(_)
                    ));
                }
                other => panic!("expected IsRegexpMatch, got {:?}", other),
            },
            other => panic!("expected Filter, got {:?}", other),
        }

        // With flags.
        let result = plan_query("SELECT * WHERE { ?s ?p ?o . FILTER(REGEX(?o, \"test\", \"i\")) }");
        match &result.expr {
            HirRelationExpr::Filter { predicates, .. } => match &predicates[0] {
                HirScalarExpr::CallBinary { func, .. } => {
                    assert!(matches!(
                        func,
                        mz_expr::BinaryFunc::IsRegexpMatchCaseInsensitive(_)
                    ));
                }
                other => panic!("expected IsRegexpMatchCaseInsensitive, got {:?}", other),
            },
            other => panic!("expected Filter, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_expression_in_not_in() {
        let result =
            plan_query("SELECT * WHERE { ?s ?p ?o . FILTER(?o IN (\"a\", \"b\", \"c\")) }");
        match &result.expr {
            HirRelationExpr::Filter { predicates, .. } => match &predicates[0] {
                HirScalarExpr::CallVariadic { func, exprs, .. } => {
                    assert!(matches!(func, mz_expr::VariadicFunc::Or(_)));
                    assert_eq!(exprs.len(), 3);
                }
                other => panic!("expected Or (IN expansion), got {:?}", other),
            },
            other => panic!("expected Filter, got {:?}", other),
        }

        let result = plan_query("SELECT * WHERE { ?s ?p ?o . FILTER(?o NOT IN (\"x\", \"y\")) }");
        match &result.expr {
            HirRelationExpr::Filter { predicates, .. } => match &predicates[0] {
                HirScalarExpr::CallUnary { func, .. } => {
                    assert!(matches!(func, mz_expr::UnaryFunc::Not(_)));
                }
                other => panic!("expected Not(Or(...)), got {:?}", other),
            },
            other => panic!("expected Filter, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_expression_is_blank() {
        let result = plan_query("SELECT * WHERE { ?s ?p ?o . FILTER(isBlank(?s)) }");
        match &result.expr {
            HirRelationExpr::Filter { predicates, .. } => match &predicates[0] {
                HirScalarExpr::CallBinary { func, .. } => {
                    assert!(matches!(func, mz_expr::BinaryFunc::StartsWith(_)));
                }
                other => panic!("expected StartsWith (_:), got {:?}", other),
            },
            other => panic!("expected Filter, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_expression_is_numeric() {
        let result = plan_query("SELECT * WHERE { ?s ?p ?o . FILTER(isNumeric(?o)) }");
        match &result.expr {
            HirRelationExpr::Filter { predicates, .. } => match &predicates[0] {
                HirScalarExpr::CallBinary { func, .. } => {
                    assert!(matches!(
                        func,
                        mz_expr::BinaryFunc::IsRegexpMatchCaseSensitive(_)
                    ));
                }
                other => panic!("expected IsRegexpMatch (numeric pattern), got {:?}", other),
            },
            other => panic!("expected Filter, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_expression_replace() {
        let result = plan_query(
            "SELECT * WHERE { ?s ?p ?o . BIND(REPLACE(?o, \"old\", \"new\") AS ?replaced) }",
        );
        assert_eq!(var_names(&result), vec!["o", "p", "replaced", "s"]);
        match &result.expr {
            HirRelationExpr::Map { scalars, .. } => match &scalars[0] {
                HirScalarExpr::CallVariadic { func, exprs, .. } => {
                    assert!(matches!(func, mz_expr::VariadicFunc::Replace(_)));
                    assert_eq!(exprs.len(), 3);
                }
                other => panic!("expected Replace, got {:?}", other),
            },
            other => panic!("expected Map, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_expression_substr() {
        let result =
            plan_query("SELECT * WHERE { ?s ?p ?o . BIND(SUBSTR(?o, \"2\", \"3\") AS ?sub) }");
        assert_eq!(var_names(&result), vec!["o", "p", "s", "sub"]);
        match &result.expr {
            HirRelationExpr::Map { scalars, .. } => match &scalars[0] {
                HirScalarExpr::CallVariadic { func, exprs, .. } => {
                    assert!(matches!(func, mz_expr::VariadicFunc::Substr(_)));
                    assert_eq!(exprs.len(), 3); // string, start, length
                }
                other => panic!("expected Substr, got {:?}", other),
            },
            other => panic!("expected Map, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_filter_exists() {
        let result = plan_query(
            "PREFIX ex: <http://example.org/>
             SELECT * WHERE {
                ?s ex:name ?name .
                FILTER EXISTS { ?s ex:email ?email }
             }",
        );
        assert_eq!(var_names(&result), vec!["name", "s"]);
        match &result.expr {
            HirRelationExpr::Filter { predicates, .. } => match &predicates[0] {
                HirScalarExpr::Exists(_, _) => {}
                other => panic!("expected Exists, got {:?}", other),
            },
            other => panic!("expected Filter, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_filter_not_exists() {
        let result = plan_query(
            "PREFIX ex: <http://example.org/>
             SELECT * WHERE {
                ?s ex:name ?name .
                FILTER NOT EXISTS { ?s ex:deleted ?d }
             }",
        );
        assert_eq!(var_names(&result), vec!["name", "s"]);
        match &result.expr {
            HirRelationExpr::Filter { predicates, .. } => match &predicates[0] {
                HirScalarExpr::CallUnary { func, expr, .. } => {
                    assert!(matches!(func, mz_expr::UnaryFunc::Not(_)));
                    assert!(matches!(expr.as_ref(), HirScalarExpr::Exists(_, _)));
                }
                other => panic!("expected Not(Exists), got {:?}", other),
            },
            other => panic!("expected Filter, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_combined_bind_values_filter() {
        // A query using BIND, VALUES, and FILTER together.
        let result = plan_query(
            "PREFIX ex: <http://example.org/>
             SELECT * WHERE {
                VALUES ?type { <http://example.org/Person> }
                ?s a ?type .
                ?s ex:name ?name .
                BIND(UCASE(?name) AS ?upper) .
                FILTER(CONTAINS(?upper, \"ALICE\"))
             }",
        );
        assert_eq!(var_names(&result), vec!["name", "s", "type", "upper"]);
        // The outermost should be Filter (CONTAINS check).
        match &result.expr {
            HirRelationExpr::Filter { .. } => {}
            other => panic!("expected Filter at top level, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_aggregate_error_in_filter() {
        // Aggregates in FILTER should produce an error (they belong in SELECT/HAVING).
        let query =
            parse("SELECT * WHERE { ?s ?p ?o . FILTER(COUNT(?s) > \"5\") }").expect("parse ok");
        let planner = SparqlPlanner::new(GlobalId::User(1));
        let err = planner.plan(&query).unwrap_err();
        assert!(
            err.message.contains("aggregate"),
            "expected aggregate error, got: {}",
            err.message
        );
    }
}
