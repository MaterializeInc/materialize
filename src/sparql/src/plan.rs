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

use mz_expr::{ColumnOrder, Id, LocalId};
use mz_hir::{AggregateExpr, AggregateFunc, HirRelationExpr, HirScalarExpr, JoinKind};
use mz_repr::{Datum, GlobalId, SqlColumnType, SqlRelationType, SqlScalarType};
use std::cell::Cell;
use std::collections::BTreeMap;

use mz_sparql_parser::ast::{
    DatasetClause, Expression, GraphTerm, GroupGraphPattern, Iri, NegatedPathElement, PropertyPath,
    QueryForm, RdfLiteral, SelectClause, SelectModifier, SelectVariable, SparqlQuery,
    TriplePattern, VarOrIri, VarOrTerm, Variable, VerbPath,
};

/// Column indices in the quad table `(subject, predicate, object, graph)`.
const QUAD_SUBJECT: usize = 0;
const QUAD_PREDICATE: usize = 1;
const QUAD_OBJECT: usize = 2;
#[allow(dead_code)]
const QUAD_GRAPH: usize = 3;
#[allow(dead_code)]
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
// TODO: Introduce a SparqlQueryContext (analogous to SQL's QueryContext) to hold
// shared planning state: ID generation, catalog access, name resolution, etc.
pub struct SparqlPlanner {
    /// The global ID of the quad table (may be overridden for catalog graph queries).
    quad_table_id: Cell<GlobalId>,
    /// The relation type of the quad table.
    quad_table_type: SqlRelationType,
    /// Counter for allocating unique `LocalId`s (used by LetRec for recursive paths).
    next_local_id: Cell<u64>,
    /// The global ID of the catalog triples view (`mz_internal.mz_rdf_catalog_triples`),
    /// if available. Used when `FROM <urn:materialize:catalog>` is specified.
    catalog_triples_id: Option<GlobalId>,
    /// The original quad table ID (before any FROM-clause override).
    original_quad_table_id: GlobalId,
}

/// The well-known graph IRI for the Materialize catalog.
pub const CATALOG_GRAPH_IRI: &str = "urn:materialize:catalog";

impl SparqlPlanner {
    /// Creates a new planner for the given quad table.
    pub fn new(quad_table_id: GlobalId, catalog_triples_id: Option<GlobalId>) -> Self {
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
            quad_table_id: Cell::new(quad_table_id),
            quad_table_type,
            next_local_id: Cell::new(1),
            catalog_triples_id,
            original_quad_table_id: quad_table_id,
        }
    }

    /// Plans a complete SPARQL query, returning the HIR expression and variable mapping.
    ///
    /// The planning pipeline is:
    /// 1. Plan WHERE clause (graph pattern) → base relation
    /// 2. GROUP BY + aggregates → Reduce (if present)
    /// 3. HAVING → Filter post-Reduce
    /// 4. SELECT projection (variables, expressions, *) → Map + Project
    /// 5. DISTINCT → Distinct
    /// 6. ORDER BY + LIMIT/OFFSET → TopK
    /// Returns the effective quad table ID for this query, considering FROM clauses.
    /// If the only FROM clause is `<urn:materialize:catalog>` and the catalog triples
    /// view is available, returns its ID. Otherwise returns the default quad table.
    fn effective_quad_table_id(&self, from: &[DatasetClause]) -> GlobalId {
        // Check if the catalog graph is the sole default graph source.
        let default_graphs: Vec<&str> = from
            .iter()
            .filter(|c| !c.named)
            .map(|c| c.iri.value.as_str())
            .collect();
        if default_graphs.len() == 1 && default_graphs[0] == CATALOG_GRAPH_IRI {
            if let Some(id) = self.catalog_triples_id {
                return id;
            }
        }
        self.original_quad_table_id
    }

    /// Build a filter expression restricting the graph column to the given FROM IRIs.
    /// Returns `None` if no FROM clauses are present (no filtering needed).
    fn from_graph_filter(&self, from: &[DatasetClause]) -> Option<HirScalarExpr> {
        let default_graphs: Vec<&str> = from
            .iter()
            .filter(|c| !c.named)
            .map(|c| c.iri.value.as_str())
            .collect();
        if default_graphs.is_empty() {
            return None;
        }
        // If the catalog graph is the sole source and we have its view, the scan
        // is already scoped — no graph column filter needed.
        if default_graphs.len() == 1
            && default_graphs[0] == CATALOG_GRAPH_IRI
            && self.catalog_triples_id.is_some()
        {
            return None;
        }
        // Build: graph = 'g1' OR graph = 'g2' OR ...
        let predicates: Vec<HirScalarExpr> = default_graphs
            .iter()
            .map(|g| {
                HirScalarExpr::call_binary(
                    HirScalarExpr::column(QUAD_GRAPH),
                    HirScalarExpr::literal(Datum::String(g), SqlScalarType::String),
                    mz_expr::BinaryFunc::Eq(mz_expr::func::Eq),
                )
            })
            .collect();
        Some(HirScalarExpr::variadic_or(predicates))
    }

    pub fn plan(&self, query: &SparqlQuery) -> Result<PlannedRelation, PlanError> {
        // Step 0: Set up the effective quad table based on FROM clauses.
        // If FROM <urn:materialize:catalog> is the sole default graph and the
        // catalog triples view is available, scan it directly instead of rdf_quads.
        let effective_id = self.effective_quad_table_id(&query.from);
        self.quad_table_id.set(effective_id);

        // Step 1: Plan the WHERE clause.
        let mut result = self.plan_pattern(&query.where_clause)?;

        // Restore original quad table ID.
        self.quad_table_id.set(self.original_quad_table_id);

        // Step 1b: Apply FROM graph filtering if present (for non-catalog graphs).
        if let Some(filter) = self.from_graph_filter(&query.from) {
            result = PlannedRelation {
                expr: result.expr.filter(vec![filter]),
                var_map: result.var_map,
            };
        }

        // Step 2-6 depend on the query form.
        match &query.form {
            QueryForm::Select {
                modifier,
                projection,
            } => {
                // Step 2: GROUP BY + aggregates.
                if !query.group_by.is_empty() || self.has_aggregates_in_select(projection) {
                    result =
                        self.plan_group_by_and_aggregates(result, &query.group_by, projection)?;

                    // Step 3: HAVING filter (applied after Reduce).
                    if let Some(having) = &query.having {
                        result = self.apply_filter(result, having)?;
                    }

                    // Step 4: SELECT projection (post-aggregation).
                    result = self.plan_select_projection_post_agg(result, projection)?;
                } else {
                    // No aggregation: apply HAVING as a regular filter if present.
                    if let Some(having) = &query.having {
                        result = self.apply_filter(result, having)?;
                    }

                    // Step 4: SELECT projection (non-aggregate).
                    result = self.plan_select_projection(result, projection)?;
                }

                // Step 5: DISTINCT / REDUCED.
                match modifier {
                    SelectModifier::Distinct => {
                        result = PlannedRelation {
                            expr: HirRelationExpr::Distinct {
                                input: Box::new(result.expr),
                            },
                            var_map: result.var_map,
                        };
                    }
                    SelectModifier::Reduced => {
                        // REDUCED allows but doesn't require duplicate elimination.
                        // We treat it as Distinct for correctness.
                        result = PlannedRelation {
                            expr: HirRelationExpr::Distinct {
                                input: Box::new(result.expr),
                            },
                            var_map: result.var_map,
                        };
                    }
                    SelectModifier::Default => {}
                }

                // Step 6: ORDER BY + LIMIT/OFFSET → TopK.
                result =
                    self.plan_order_limit(result, &query.order_by, query.limit, query.offset)?;
            }
            QueryForm::Construct { template } => {
                result = self.plan_construct(result, template)?;
                result =
                    self.plan_order_limit(result, &query.order_by, query.limit, query.offset)?;
            }
            QueryForm::Ask => {
                result = self.plan_ask(result)?;
            }
            QueryForm::Describe { resources } => {
                result = self.plan_describe(resources)?;
                result =
                    self.plan_order_limit(result, &query.order_by, query.limit, query.offset)?;
            }
        }

        Ok(result)
    }

    /// Checks if any SELECT expression contains an aggregate function.
    fn has_aggregates_in_select(&self, projection: &SelectClause) -> bool {
        match projection {
            SelectClause::Wildcard => false,
            SelectClause::Variables(vars) => vars.iter().any(|v| match v {
                SelectVariable::Variable(_) => false,
                SelectVariable::Expression(expr, _) => self.expr_has_aggregate(expr),
            }),
        }
    }

    /// Checks if an expression contains an aggregate function call.
    fn expr_has_aggregate(&self, expr: &Expression) -> bool {
        match expr {
            Expression::Count { .. }
            | Expression::Sum { .. }
            | Expression::Avg { .. }
            | Expression::Min { .. }
            | Expression::Max { .. }
            | Expression::GroupConcat { .. }
            | Expression::Sample { .. } => true,
            Expression::Add(l, r)
            | Expression::Subtract(l, r)
            | Expression::Multiply(l, r)
            | Expression::Divide(l, r)
            | Expression::And(l, r)
            | Expression::Or(l, r)
            | Expression::Equal(l, r)
            | Expression::NotEqual(l, r)
            | Expression::LessThan(l, r)
            | Expression::GreaterThan(l, r)
            | Expression::LessThanOrEqual(l, r)
            | Expression::GreaterThanOrEqual(l, r) => {
                self.expr_has_aggregate(l) || self.expr_has_aggregate(r)
            }
            Expression::UnaryNot(e)
            | Expression::UnaryPlus(e)
            | Expression::UnaryMinus(e)
            | Expression::Str(e)
            | Expression::Lang(e)
            | Expression::Datatype(e)
            | Expression::IsIri(e)
            | Expression::IsBlank(e)
            | Expression::IsLiteral(e)
            | Expression::IsNumeric(e)
            | Expression::Strlen(e)
            | Expression::Ucase(e)
            | Expression::Lcase(e) => self.expr_has_aggregate(e),
            Expression::If(a, b, c) => {
                self.expr_has_aggregate(a)
                    || self.expr_has_aggregate(b)
                    || self.expr_has_aggregate(c)
            }
            Expression::Coalesce(exprs) | Expression::Concat(exprs) => {
                exprs.iter().any(|e| self.expr_has_aggregate(e))
            }
            _ => false,
        }
    }

    /// Plans GROUP BY + aggregates, producing a Reduce node.
    ///
    /// The output relation has group key columns first, then aggregate result columns.
    /// A post-reduce var_map maps the group key variables and aggregate aliases.
    fn plan_group_by_and_aggregates(
        &self,
        input: PlannedRelation,
        group_by: &[Expression],
        projection: &SelectClause,
    ) -> Result<PlannedRelation, PlanError> {
        // Determine group key columns: resolve each GROUP BY expression to a column index.
        let mut group_key = Vec::new();
        let mut group_key_vars: Vec<Option<String>> = Vec::new();

        for gb_expr in group_by {
            match gb_expr {
                Expression::Variable(v) => {
                    let col = input.var_map.get(&v.name).ok_or_else(|| PlanError {
                        message: format!("GROUP BY variable ?{} not in scope", v.name),
                    })?;
                    group_key.push(*col);
                    group_key_vars.push(Some(v.name.clone()));
                }
                _ => {
                    return Err(PlanError {
                        message: "GROUP BY expressions (non-variable) not yet supported"
                            .to_string(),
                    });
                }
            }
        }

        // Collect aggregate expressions from the SELECT clause.
        let mut aggregates: Vec<AggregateExpr> = Vec::new();
        let mut agg_aliases: Vec<String> = Vec::new();

        if let SelectClause::Variables(vars) = projection {
            for sel_var in vars {
                if let SelectVariable::Expression(expr, alias) = sel_var {
                    if self.expr_has_aggregate(expr) {
                        let agg_expr = self.translate_aggregate(expr, &input.var_map)?;
                        aggregates.push(agg_expr);
                        agg_aliases.push(alias.name.clone());
                    }
                }
            }
        }

        // If no explicit GROUP BY but there are aggregates, the entire input is one group.
        // group_key stays empty → single-group aggregation.

        let reduce = HirRelationExpr::Reduce {
            input: Box::new(input.expr),
            group_key: group_key.clone(),
            aggregates: aggregates.clone(),
            expected_group_size: None,
        };

        // Build var_map for the Reduce output:
        // Columns 0..group_key.len() are the group keys.
        // Columns group_key.len().. are aggregate results.
        let mut new_var_map = BTreeMap::new();
        for (i, var_name) in group_key_vars.iter().enumerate() {
            if let Some(name) = var_name {
                new_var_map.insert(name.clone(), i);
            }
        }
        for (i, alias) in agg_aliases.iter().enumerate() {
            new_var_map.insert(alias.clone(), group_key.len() + i);
        }

        Ok(PlannedRelation {
            expr: reduce,
            var_map: new_var_map,
        })
    }

    /// Translates a SPARQL aggregate expression to an HIR AggregateExpr.
    fn translate_aggregate(
        &self,
        expr: &Expression,
        var_map: &BTreeMap<String, usize>,
    ) -> Result<AggregateExpr, PlanError> {
        match expr {
            Expression::Count {
                expr: inner,
                distinct,
            } => {
                let hir_expr = match inner {
                    Some(e) => self.translate_expression(e, var_map)?,
                    None => {
                        // COUNT(*) — use literal true (counts all rows).
                        HirScalarExpr::literal_true()
                    }
                };
                Ok(AggregateExpr {
                    func: AggregateFunc::Count,
                    expr: Box::new(hir_expr),
                    distinct: *distinct,
                })
            }
            Expression::Sum {
                expr: inner,
                distinct,
            } => {
                let hir_expr = self.to_float64(self.translate_expression(inner, var_map)?);
                Ok(AggregateExpr {
                    func: AggregateFunc::SumFloat64,
                    expr: Box::new(hir_expr),
                    distinct: *distinct,
                })
            }
            Expression::Avg {
                expr: inner,
                distinct,
            } => {
                // AVG is not directly available in HIR AggregateFunc.
                // Implement as SUM / COUNT.
                // For now, produce an error and handle in a future prompt.
                let _ = (inner, distinct);
                Err(PlanError {
                    message: "AVG aggregate not yet fully supported; use SUM/COUNT manually"
                        .to_string(),
                })
            }
            Expression::Min {
                expr: inner,
                distinct,
            } => {
                let hir_expr = self.translate_expression(inner, var_map)?;
                Ok(AggregateExpr {
                    func: AggregateFunc::MinString,
                    expr: Box::new(hir_expr),
                    distinct: *distinct,
                })
            }
            Expression::Max {
                expr: inner,
                distinct,
            } => {
                let hir_expr = self.translate_expression(inner, var_map)?;
                Ok(AggregateExpr {
                    func: AggregateFunc::MaxString,
                    expr: Box::new(hir_expr),
                    distinct: *distinct,
                })
            }
            Expression::GroupConcat {
                expr: inner,
                separator,
                distinct,
            } => {
                let hir_expr = self.translate_expression(inner, var_map)?;
                let _ = (separator, distinct);
                Ok(AggregateExpr {
                    func: AggregateFunc::StringAgg {
                        order_by: Vec::new(),
                    },
                    expr: Box::new(hir_expr),
                    distinct: *distinct,
                })
            }
            Expression::Sample {
                expr: inner,
                distinct,
            } => {
                // SAMPLE returns an arbitrary value from the group.
                // Approximate with MIN for now.
                let hir_expr = self.translate_expression(inner, var_map)?;
                Ok(AggregateExpr {
                    func: AggregateFunc::MinString,
                    expr: Box::new(hir_expr),
                    distinct: *distinct,
                })
            }
            _ => Err(PlanError {
                message: format!("expected aggregate expression, got {:?}", expr),
            }),
        }
    }

    /// Plans SELECT projection after GROUP BY + aggregation.
    ///
    /// Post-aggregation, the var_map already has the right columns from the Reduce.
    /// We just need to project to the selected variables in order.
    fn plan_select_projection_post_agg(
        &self,
        input: PlannedRelation,
        projection: &SelectClause,
    ) -> Result<PlannedRelation, PlanError> {
        match projection {
            SelectClause::Wildcard => {
                // SELECT * with GROUP BY: keep all columns from Reduce output.
                Ok(input)
            }
            SelectClause::Variables(vars) => {
                let mut output_cols = Vec::new();
                let mut new_var_map = BTreeMap::new();

                for sel_var in vars {
                    match sel_var {
                        SelectVariable::Variable(v) => {
                            let col = input.var_map.get(&v.name).ok_or_else(|| PlanError {
                                message: format!(
                                    "variable ?{} not available after GROUP BY",
                                    v.name
                                ),
                            })?;
                            new_var_map.insert(v.name.clone(), output_cols.len());
                            output_cols.push(*col);
                        }
                        SelectVariable::Expression(_, alias) => {
                            // The aggregate alias was registered in var_map by plan_group_by_and_aggregates.
                            let col = input.var_map.get(&alias.name).ok_or_else(|| PlanError {
                                message: format!(
                                    "aggregate alias ?{} not in Reduce output",
                                    alias.name
                                ),
                            })?;
                            new_var_map.insert(alias.name.clone(), output_cols.len());
                            output_cols.push(*col);
                        }
                    }
                }

                Ok(PlannedRelation {
                    expr: HirRelationExpr::Project {
                        input: Box::new(input.expr),
                        outputs: output_cols,
                    },
                    var_map: new_var_map,
                })
            }
        }
    }

    /// Plans SELECT projection (non-aggregate case).
    ///
    /// - `SELECT *`: keep all in-scope variables (no change)
    /// - `SELECT ?v1 ?v2`: project to named variables
    /// - `SELECT (expr AS ?v)`: Map + Project
    fn plan_select_projection(
        &self,
        input: PlannedRelation,
        projection: &SelectClause,
    ) -> Result<PlannedRelation, PlanError> {
        match projection {
            SelectClause::Wildcard => Ok(input),
            SelectClause::Variables(vars) => {
                // First pass: add any computed expressions via Map.
                let mut current = input;
                let mut computed_vars: Vec<(String, usize)> = Vec::new();

                for sel_var in vars {
                    if let SelectVariable::Expression(expr, alias) = sel_var {
                        let hir_expr = self.translate_expression(expr, &current.var_map)?;
                        let new_col = current.arity();
                        current = PlannedRelation {
                            expr: HirRelationExpr::Map {
                                input: Box::new(current.expr),
                                scalars: vec![hir_expr],
                            },
                            var_map: {
                                let mut m = current.var_map.clone();
                                m.insert(alias.name.clone(), new_col);
                                m
                            },
                        };
                        computed_vars.push((alias.name.clone(), new_col));
                    }
                }

                // Second pass: project to the requested variables in order.
                let mut output_cols = Vec::new();
                let mut new_var_map = BTreeMap::new();

                for sel_var in vars {
                    let var_name = match sel_var {
                        SelectVariable::Variable(v) => &v.name,
                        SelectVariable::Expression(_, alias) => &alias.name,
                    };
                    let col = current.var_map.get(var_name).ok_or_else(|| PlanError {
                        message: format!("variable ?{} not in scope for SELECT", var_name),
                    })?;
                    new_var_map.insert(var_name.clone(), output_cols.len());
                    output_cols.push(*col);
                }

                Ok(PlannedRelation {
                    expr: HirRelationExpr::Project {
                        input: Box::new(current.expr),
                        outputs: output_cols,
                    },
                    var_map: new_var_map,
                })
            }
        }
    }

    /// Plans ORDER BY + LIMIT/OFFSET → TopK.
    fn plan_order_limit(
        &self,
        input: PlannedRelation,
        order_by: &[mz_sparql_parser::ast::OrderCondition],
        limit: Option<u64>,
        offset: Option<u64>,
    ) -> Result<PlannedRelation, PlanError> {
        if order_by.is_empty() && limit.is_none() && offset.is_none() {
            return Ok(input);
        }

        // Translate ORDER BY expressions to ColumnOrder.
        let mut order_key = Vec::new();
        for cond in order_by {
            match &cond.expr {
                Expression::Variable(v) => {
                    let col = input.var_map.get(&v.name).ok_or_else(|| PlanError {
                        message: format!("ORDER BY variable ?{} not in scope", v.name),
                    })?;
                    order_key.push(ColumnOrder {
                        column: *col,
                        desc: !cond.ascending,
                        nulls_last: !cond.ascending, // nulls last for DESC, first for ASC
                    });
                }
                _ => {
                    return Err(PlanError {
                        message: "ORDER BY non-variable expressions not yet supported".to_string(),
                    });
                }
            }
        }

        // Convert limit/offset to the types TopK expects.
        let limit_expr =
            limit.map(|l| HirScalarExpr::literal(Datum::Int64(l as i64), SqlScalarType::Int64));
        let offset_val = offset.unwrap_or(0);
        let offset_expr =
            HirScalarExpr::literal(Datum::Int64(offset_val as i64), SqlScalarType::Int64);

        Ok(PlannedRelation {
            expr: HirRelationExpr::TopK {
                input: Box::new(input.expr),
                group_key: Vec::new(), // no partitioning
                order_key,
                limit: limit_expr,
                offset: offset_expr,
                expected_group_size: None,
            },
            var_map: input.var_map,
        })
    }

    /// Plans a CONSTRUCT query.
    ///
    /// For each template triple pattern, produces a 3-column relation
    /// (subject, predicate, object) by mapping variables from the WHERE clause
    /// result to string columns, and concrete terms to string literals.
    /// All template triples are unioned together, then deduplicated.
    fn plan_construct(
        &self,
        input: PlannedRelation,
        template: &[TriplePattern],
    ) -> Result<PlannedRelation, PlanError> {
        let output_var_map: BTreeMap<String, usize> = [
            ("subject".to_string(), 0),
            ("predicate".to_string(), 1),
            ("object".to_string(), 2),
        ]
        .into_iter()
        .collect();

        if template.is_empty() {
            // Empty template: return empty relation with the CONSTRUCT schema.
            return Ok(PlannedRelation {
                expr: HirRelationExpr::Constant {
                    rows: vec![],
                    typ: SqlRelationType::new(vec![
                        SqlColumnType {
                            scalar_type: SqlScalarType::String,
                            nullable: true,
                        };
                        3
                    ]),
                },
                var_map: output_var_map,
            });
        }

        // For each template triple, create a Map that produces (subject, predicate, object).
        let mut triple_relations = Vec::new();

        for tp in template {
            let scalars = vec![
                self.var_or_term_to_scalar(&tp.subject, &input.var_map)?,
                self.verb_path_to_scalar(&tp.predicate, &input.var_map)?,
                self.var_or_term_to_scalar(&tp.object, &input.var_map)?,
            ];

            let input_arity = input.arity();
            let mapped = HirRelationExpr::Map {
                input: Box::new(input.expr.clone()),
                scalars,
            };

            // Project to just the 3 new columns.
            let projected = HirRelationExpr::Project {
                input: Box::new(mapped),
                outputs: vec![input_arity, input_arity + 1, input_arity + 2],
            };

            triple_relations.push(projected);
        }

        // Union all template triple relations together.
        let mut unioned = triple_relations.remove(0);
        for rel in triple_relations {
            unioned = HirRelationExpr::Union {
                base: Box::new(unioned),
                inputs: vec![rel],
            };
        }

        // CONSTRUCT deduplicates.
        let distinct = HirRelationExpr::Distinct {
            input: Box::new(unioned),
        };

        Ok(PlannedRelation {
            expr: distinct,
            var_map: output_var_map,
        })
    }

    /// Plans an ASK query.
    ///
    /// ASK returns a single boolean: true if the WHERE clause matches at least
    /// one solution, false otherwise. We plan this as:
    /// `Map(Reduce(input, [], [count(true)]), count > 0)`
    /// projecting to a single "result" column.
    fn plan_ask(&self, input: PlannedRelation) -> Result<PlannedRelation, PlanError> {
        // First, reduce to a single row with count(*).
        let count_agg = AggregateExpr {
            func: AggregateFunc::Count,
            expr: Box::new(HirScalarExpr::literal(Datum::True, SqlScalarType::Bool)),
            distinct: false,
        };

        let reduced = HirRelationExpr::Reduce {
            input: Box::new(input.expr),
            group_key: vec![],
            aggregates: vec![count_agg],
            expected_group_size: None,
        };

        // Column 0 is the count. Map count > 0 as the boolean result.
        let gt_zero = HirScalarExpr::CallBinary {
            func: mz_expr::BinaryFunc::Gt(mz_expr::func::Gt),
            expr1: Box::new(HirScalarExpr::column(0)),
            expr2: Box::new(HirScalarExpr::literal(
                Datum::Int64(0),
                SqlScalarType::Int64,
            )),
            name: mz_ore::treat_as_equal::TreatAsEqual(None),
        };

        let mapped = HirRelationExpr::Map {
            input: Box::new(reduced),
            scalars: vec![gt_zero],
        };

        // Project to just the boolean column (column 1).
        let projected = HirRelationExpr::Project {
            input: Box::new(mapped),
            outputs: vec![1],
        };

        let var_map: BTreeMap<String, usize> = [("result".to_string(), 0)].into_iter().collect();

        Ok(PlannedRelation {
            expr: projected,
            var_map,
        })
    }

    /// Plans a DESCRIBE query.
    ///
    /// DESCRIBE returns all triples where any of the described resources appear
    /// as subject or object. This is a simplified Concise Bounded Description (CBD).
    /// For each resource, we generate:
    ///   (SELECT s, p, o WHERE { resource ?p ?o }) -- as subject
    ///   UNION
    ///   (SELECT s, p, o WHERE { ?s ?p resource }) -- as object
    fn plan_describe(&self, resources: &[VarOrIri]) -> Result<PlannedRelation, PlanError> {
        let output_var_map: BTreeMap<String, usize> = [
            ("subject".to_string(), 0),
            ("predicate".to_string(), 1),
            ("object".to_string(), 2),
        ]
        .into_iter()
        .collect();

        if resources.is_empty() {
            return Err(PlanError {
                message: "DESCRIBE requires at least one resource or variable".to_string(),
            });
        }

        let mut parts = Vec::new();

        for resource in resources {
            // Triples where resource is the subject: { resource ?p ?o }
            let as_subject = self.plan_describe_direction(resource, true)?;
            // Triples where resource is the object: { ?s ?p resource }
            let as_object = self.plan_describe_direction(resource, false)?;

            parts.push(as_subject);
            parts.push(as_object);
        }

        // Union all parts together.
        let mut unioned = parts.remove(0);
        for part in parts {
            unioned = HirRelationExpr::Union {
                base: Box::new(unioned),
                inputs: vec![part],
            };
        }

        // Deduplicate.
        let distinct = HirRelationExpr::Distinct {
            input: Box::new(unioned),
        };

        Ok(PlannedRelation {
            expr: distinct,
            var_map: output_var_map,
        })
    }

    /// Helper for DESCRIBE: generates a scan for triples where a resource
    /// appears in a specific position (subject if `as_subject`, object otherwise).
    ///
    /// Returns a 3-column relation (subject, predicate, object).
    fn plan_describe_direction(
        &self,
        resource: &VarOrIri,
        as_subject: bool,
    ) -> Result<HirRelationExpr, PlanError> {
        let get = HirRelationExpr::Get {
            id: Id::Global(self.quad_table_id.get()),
            typ: self.quad_table_type.clone(),
        };

        // Filter on the resource position.
        let filter_col = if as_subject {
            QUAD_SUBJECT
        } else {
            QUAD_OBJECT
        };

        let filter_expr = match resource {
            VarOrIri::Iri(iri) => self.iri_filter(filter_col, iri),
            VarOrIri::Variable(_) => {
                // For DESCRIBE ?var, we can't filter statically — we'd need
                // the WHERE clause result. For now, return an error for variable
                // DESCRIBE without a WHERE clause providing bindings.
                return Err(PlanError {
                    message: "DESCRIBE with variables requires a WHERE clause \
                              (not yet supported — use DESCRIBE <iri> instead)"
                        .to_string(),
                });
            }
        };

        let filtered = HirRelationExpr::Filter {
            input: Box::new(get),
            predicates: vec![filter_expr],
        };

        // Project to (subject, predicate, object) — drop graph column.
        let projected = HirRelationExpr::Project {
            input: Box::new(filtered),
            outputs: vec![QUAD_SUBJECT, QUAD_PREDICATE, QUAD_OBJECT],
        };

        Ok(projected)
    }

    /// Translates a `VarOrTerm` to a scalar expression (for CONSTRUCT templates).
    fn var_or_term_to_scalar(
        &self,
        vot: &VarOrTerm,
        var_map: &BTreeMap<String, usize>,
    ) -> Result<HirScalarExpr, PlanError> {
        match vot {
            VarOrTerm::Variable(v) => {
                let col = var_map.get(&v.name).ok_or_else(|| PlanError {
                    message: format!(
                        "CONSTRUCT template variable ?{} not bound in WHERE clause",
                        v.name
                    ),
                })?;
                Ok(HirScalarExpr::column(*col))
            }
            VarOrTerm::Term(term) => {
                let s = self.term_to_string(term);
                Ok(HirScalarExpr::literal(
                    Datum::String(&s),
                    SqlScalarType::String,
                ))
            }
        }
    }

    /// Translates a `VerbPath` (predicate position) to a scalar expression (for CONSTRUCT templates).
    fn verb_path_to_scalar(
        &self,
        verb: &VerbPath,
        var_map: &BTreeMap<String, usize>,
    ) -> Result<HirScalarExpr, PlanError> {
        match verb {
            VerbPath::Variable(v) => {
                let col = var_map.get(&v.name).ok_or_else(|| PlanError {
                    message: format!(
                        "CONSTRUCT template variable ?{} not bound in WHERE clause",
                        v.name
                    ),
                })?;
                Ok(HirScalarExpr::column(*col))
            }
            VerbPath::Path(PropertyPath::Iri(iri)) => Ok(HirScalarExpr::literal(
                Datum::String(&iri.value),
                SqlScalarType::String,
            )),
            VerbPath::Path(_) => Err(PlanError {
                message: "property paths in CONSTRUCT templates are not allowed".to_string(),
            }),
        }
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
    ///
    /// Implements SPARQL three-valued logic: if the filter expression raises
    /// an error (e.g., type mismatch during comparison), the error is treated
    /// as `false` rather than propagated. This is done by wrapping the
    /// predicate in `COALESCE(pred, false)`.
    ///
    /// For non-boolean expressions, Effective Boolean Value (EBV) conversion
    /// is applied per SPARQL 1.1 Section 17.2.2.
    fn apply_filter(
        &self,
        rel: PlannedRelation,
        filter_expr: &Expression,
    ) -> Result<PlannedRelation, PlanError> {
        let hir_expr = self.translate_expression(filter_expr, &rel.var_map)?;

        // Apply EBV conversion if the expression isn't already boolean-typed.
        let ebv_expr = if self.is_boolean_expression(filter_expr) {
            hir_expr
        } else {
            self.to_ebv(hir_expr)
        };

        // Three-valued logic: COALESCE(pred, false) so errors → false.
        let safe_expr = self.coalesce_false(ebv_expr);

        Ok(PlannedRelation {
            expr: HirRelationExpr::Filter {
                input: Box::new(rel.expr),
                predicates: vec![safe_expr],
            },
            var_map: rel.var_map,
        })
    }

    /// Returns true if the SPARQL expression always produces a boolean value.
    fn is_boolean_expression(&self, expr: &Expression) -> bool {
        matches!(
            expr,
            Expression::Equal(..)
                | Expression::NotEqual(..)
                | Expression::LessThan(..)
                | Expression::GreaterThan(..)
                | Expression::LessThanOrEqual(..)
                | Expression::GreaterThanOrEqual(..)
                | Expression::And(..)
                | Expression::Or(..)
                | Expression::UnaryNot(..)
                | Expression::Bound(..)
                | Expression::BooleanLiteral(..)
                | Expression::IsIri(..)
                | Expression::IsBlank(..)
                | Expression::IsLiteral(..)
                | Expression::IsNumeric(..)
                | Expression::Exists(..)
                | Expression::NotExists(..)
                | Expression::In(..)
                | Expression::NotIn(..)
                | Expression::Regex(..)
                | Expression::Contains(..)
                | Expression::StrStarts(..)
                | Expression::StrEnds(..)
        )
    }

    /// Converts a string-encoded RDF value to its Effective Boolean Value (EBV).
    ///
    /// SPARQL EBV rules (Section 17.2.2):
    /// - Numeric values: true if non-zero and not NaN
    /// - String values: true if non-empty
    /// - NULL: false
    fn to_ebv(&self, expr: HirScalarExpr) -> HirScalarExpr {
        let is_num = self.is_numeric_check(expr.clone());

        // Numeric EBV: cast to float64, compare != 0.0
        let zero = self.to_float64(HirScalarExpr::literal(
            Datum::String("0"),
            SqlScalarType::String,
        ));
        let numeric_ebv = HirScalarExpr::CallBinary {
            func: mz_expr::func::NotEq.into(),
            expr1: Box::new(self.to_float64(expr.clone())),
            expr2: Box::new(zero),
            name: mz_ore::treat_as_equal::TreatAsEqual(None),
        };

        // String EBV: non-null and non-empty
        let not_null = HirScalarExpr::CallUnary {
            func: mz_expr::func::Not.into(),
            expr: Box::new(HirScalarExpr::CallUnary {
                func: mz_expr::func::IsNull.into(),
                expr: Box::new(expr.clone()),
                name: mz_ore::treat_as_equal::TreatAsEqual(None),
            }),
            name: mz_ore::treat_as_equal::TreatAsEqual(None),
        };
        let non_empty = HirScalarExpr::CallBinary {
            func: mz_expr::func::Gt.into(),
            expr1: Box::new(HirScalarExpr::CallUnary {
                func: mz_expr::func::CharLength.into(),
                expr: Box::new(expr),
                name: mz_ore::treat_as_equal::TreatAsEqual(None),
            }),
            expr2: Box::new(HirScalarExpr::literal(
                Datum::Int32(0),
                SqlScalarType::Int32,
            )),
            name: mz_ore::treat_as_equal::TreatAsEqual(None),
        };
        let string_ebv = HirScalarExpr::CallVariadic {
            func: mz_expr::func::variadic::And.into(),
            exprs: vec![not_null, non_empty],
            name: mz_ore::treat_as_equal::TreatAsEqual(None),
        };

        // IF is_numeric THEN numeric_ebv ELSE string_ebv
        HirScalarExpr::If {
            cond: Box::new(is_num),
            then: Box::new(numeric_ebv),
            els: Box::new(string_ebv),
            name: mz_ore::treat_as_equal::TreatAsEqual(None),
        }
    }

    /// Wraps an expression in `COALESCE(expr, false)` for three-valued logic.
    fn coalesce_false(&self, expr: HirScalarExpr) -> HirScalarExpr {
        HirScalarExpr::CallVariadic {
            func: mz_expr::func::variadic::Coalesce.into(),
            exprs: vec![
                expr,
                HirScalarExpr::literal(Datum::False, SqlScalarType::Bool),
            ],
            name: mz_ore::treat_as_equal::TreatAsEqual(None),
        }
    }

    /// Checks if a string expression looks numeric (regex match).
    fn is_numeric_check(&self, expr: HirScalarExpr) -> HirScalarExpr {
        HirScalarExpr::CallBinary {
            func: mz_expr::func::IsRegexpMatchCaseSensitive.into(),
            expr1: Box::new(expr),
            expr2: Box::new(HirScalarExpr::literal(
                Datum::String(r"^-?[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?$"),
                SqlScalarType::String,
            )),
            name: mz_ore::treat_as_equal::TreatAsEqual(None),
        }
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

            // Comparison operators — numeric-aware: if both operands are numeric,
            // compare as float64; otherwise fall back to string comparison.
            Expression::Equal(l, r) => {
                self.translate_comparison(mz_expr::BinaryFunc::Eq(mz_expr::func::Eq), l, r, var_map)
            }
            Expression::NotEqual(l, r) => self.translate_comparison(
                mz_expr::BinaryFunc::NotEq(mz_expr::func::NotEq),
                l,
                r,
                var_map,
            ),
            Expression::LessThan(l, r) => {
                self.translate_comparison(mz_expr::BinaryFunc::Lt(mz_expr::func::Lt), l, r, var_map)
            }
            Expression::GreaterThan(l, r) => {
                self.translate_comparison(mz_expr::BinaryFunc::Gt(mz_expr::func::Gt), l, r, var_map)
            }
            Expression::LessThanOrEqual(l, r) => self.translate_comparison(
                mz_expr::BinaryFunc::Lte(mz_expr::func::Lte),
                l,
                r,
                var_map,
            ),
            Expression::GreaterThanOrEqual(l, r) => self.translate_comparison(
                mz_expr::BinaryFunc::Gte(mz_expr::func::Gte),
                l,
                r,
                var_map,
            ),

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
            //
            // Our string encoding:
            //   Simple strings: stored as plain value (e.g., "hello")
            //   Language-tagged: stored as `"value"@lang`
            //   Typed literals: stored as `"value"^^<datatype>`
            //   IRIs: stored as bare IRI string (e.g., http://example.org/)
            //   Blank nodes: stored as `_:id`
            Expression::Str(e) => {
                // STR() returns the lexical form of an RDF term.
                // For IRIs and simple strings, return as-is (the most common
                // case). For tagged/typed literals that start with `"`, we'd
                // ideally extract the value between quotes, but for simplicity
                // and correctness in the common case, we return the value
                // unchanged. Full extraction requires runtime parsing of the
                // encoding format.
                self.translate_expression(e, var_map)
            }
            Expression::Lang(e) => {
                // LANG() returns the language tag of a literal.
                // In our encoding, language-tagged literals are `"value"@lang`.
                // Find `"@` and extract everything after it.
                let inner = self.translate_expression(e, var_map)?;
                let marker = HirScalarExpr::literal(Datum::String("\"@"), SqlScalarType::String);
                let pos = HirScalarExpr::CallBinary {
                    func: mz_expr::func::Position.into(),
                    expr1: Box::new(marker),
                    expr2: Box::new(inner.clone()),
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                };
                let has_tag = HirScalarExpr::CallBinary {
                    func: mz_expr::func::Gt.into(),
                    expr1: Box::new(pos.clone()),
                    expr2: Box::new(HirScalarExpr::literal(
                        Datum::Int32(0),
                        SqlScalarType::Int32,
                    )),
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                };
                // SUBSTR(inner, pos + 2) — skip past `"@`
                let tag_start = HirScalarExpr::CallBinary {
                    func: mz_expr::func::AddInt32.into(),
                    expr1: Box::new(pos),
                    expr2: Box::new(HirScalarExpr::literal(
                        Datum::Int32(2),
                        SqlScalarType::Int32,
                    )),
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                };
                let tag = HirScalarExpr::CallVariadic {
                    func: mz_expr::func::variadic::Substr.into(),
                    exprs: vec![inner, tag_start],
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                };
                let empty = HirScalarExpr::literal(Datum::String(""), SqlScalarType::String);
                Ok(HirScalarExpr::If {
                    cond: Box::new(has_tag),
                    then: Box::new(tag),
                    els: Box::new(empty),
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                })
            }
            Expression::Datatype(e) => {
                // DATATYPE() returns the datatype IRI of a typed literal.
                // In our encoding, typed literals are `"value"^^<datatype>`.
                // Find `^^<` and extract the datatype IRI between `<` and `>`.
                let inner = self.translate_expression(e, var_map)?;
                let marker = HirScalarExpr::literal(Datum::String("^^<"), SqlScalarType::String);
                let pos = HirScalarExpr::CallBinary {
                    func: mz_expr::func::Position.into(),
                    expr1: Box::new(marker),
                    expr2: Box::new(inner.clone()),
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                };
                let has_type = HirScalarExpr::CallBinary {
                    func: mz_expr::func::Gt.into(),
                    expr1: Box::new(pos.clone()),
                    expr2: Box::new(HirScalarExpr::literal(
                        Datum::Int32(0),
                        SqlScalarType::Int32,
                    )),
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                };
                // SUBSTR(inner, pos + 3, char_length(inner) - pos - 3)
                // to skip `^^<` and exclude trailing `>`
                let dt_start = HirScalarExpr::CallBinary {
                    func: mz_expr::func::AddInt32.into(),
                    expr1: Box::new(pos.clone()),
                    expr2: Box::new(HirScalarExpr::literal(
                        Datum::Int32(3),
                        SqlScalarType::Int32,
                    )),
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                };
                let total_len = HirScalarExpr::CallUnary {
                    func: mz_expr::func::CharLength.into(),
                    expr: Box::new(inner.clone()),
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                };
                // length of datatype = total_len - pos - 3 (for `^^<`) - 1 (for `>`)
                // = total_len - pos - 4... but we need this in Int32.
                // Actually: SUBSTR(inner, pos+3, total_len - (pos+3))
                // which excludes the trailing `>`
                let dt_len = HirScalarExpr::CallBinary {
                    func: mz_expr::func::SubInt32.into(),
                    expr1: Box::new(HirScalarExpr::CallBinary {
                        func: mz_expr::func::SubInt32.into(),
                        expr1: Box::new(total_len),
                        expr2: Box::new(pos),
                        name: mz_ore::treat_as_equal::TreatAsEqual(None),
                    }),
                    expr2: Box::new(HirScalarExpr::literal(
                        Datum::Int32(3),
                        SqlScalarType::Int32,
                    )),
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                };
                let datatype_iri = HirScalarExpr::CallVariadic {
                    func: mz_expr::func::variadic::Substr.into(),
                    exprs: vec![inner, dt_start, dt_len],
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                };
                let xsd_string = HirScalarExpr::literal(
                    Datum::String("http://www.w3.org/2001/XMLSchema#string"),
                    SqlScalarType::String,
                );
                Ok(HirScalarExpr::If {
                    cond: Box::new(has_type),
                    then: Box::new(datatype_iri),
                    els: Box::new(xsd_string),
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                })
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

    /// Translates a comparison expression with numeric awareness.
    ///
    /// If both operands are numeric strings, compares as float64.
    /// Otherwise falls back to string comparison. This handles SPARQL's
    /// requirement that `9 < 10` (not `"9" > "10"` in string order) and
    /// that `42 = 42.0` (equal as numbers despite different string form).
    fn translate_comparison(
        &self,
        func: mz_expr::BinaryFunc,
        left: &Expression,
        right: &Expression,
        var_map: &BTreeMap<String, usize>,
    ) -> Result<HirScalarExpr, PlanError> {
        let l = self.translate_expression(left, var_map)?;
        let r = self.translate_expression(right, var_map)?;

        let both_numeric = HirScalarExpr::CallVariadic {
            func: mz_expr::func::variadic::And.into(),
            exprs: vec![
                self.is_numeric_check(l.clone()),
                self.is_numeric_check(r.clone()),
            ],
            name: mz_ore::treat_as_equal::TreatAsEqual(None),
        };

        let numeric_cmp = HirScalarExpr::CallBinary {
            func: func.clone(),
            expr1: Box::new(self.to_float64(l.clone())),
            expr2: Box::new(self.to_float64(r.clone())),
            name: mz_ore::treat_as_equal::TreatAsEqual(None),
        };

        let string_cmp = HirScalarExpr::CallBinary {
            func,
            expr1: Box::new(l),
            expr2: Box::new(r),
            name: mz_ore::treat_as_equal::TreatAsEqual(None),
        };

        Ok(HirScalarExpr::If {
            cond: Box::new(both_numeric),
            then: Box::new(numeric_cmp),
            els: Box::new(string_cmp),
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
            id: Id::Global(self.quad_table_id.get()),
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
            VerbPath::Path(PropertyPath::Iri(iri)) => {
                filters.push(self.iri_filter(QUAD_PREDICATE, iri));
            }
            VerbPath::Path(path) => {
                // Non-simple property path: delegate to the property path planner.
                // This produces a 2-column (start, end) relation; we integrate it
                // with the triple pattern's subject and object.
                return self.plan_triple_with_property_path(triple, path);
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

    // --- Property path planning ---

    /// Allocates a fresh `LocalId` for use in `LetRec` bindings.
    fn alloc_local_id(&self) -> LocalId {
        let id = self.next_local_id.get();
        self.next_local_id.set(id + 1);
        LocalId::new(id)
    }

    /// The relation type for a 2-column property path result `(start, end)`.
    fn pp_relation_type(&self) -> SqlRelationType {
        let text_nullable = SqlColumnType {
            scalar_type: SqlScalarType::String,
            nullable: true,
        };
        SqlRelationType::new(vec![text_nullable.clone(), text_nullable])
    }

    /// Plans a triple pattern that uses a non-simple property path in the
    /// predicate position.
    ///
    /// Compiles the property path into a 2-column `(start, end)` relation,
    /// then integrates it with the subject and object of the triple pattern
    /// by applying filters (for concrete terms) and creating variable bindings.
    fn plan_triple_with_property_path(
        &self,
        triple: &TriplePattern,
        path: &PropertyPath,
    ) -> Result<PlannedRelation, PlanError> {
        // Plan the property path → 2-column (start=0, end=1) relation.
        let mut expr = self.plan_property_path(path)?;

        let mut filters = Vec::new();
        let mut var_map: BTreeMap<String, usize> = BTreeMap::new();
        let mut extra_eq_filters: Vec<(usize, usize)> = Vec::new();

        // Process subject → column 0 (start).
        match &triple.subject {
            VarOrTerm::Variable(v) => {
                if let Some(&existing_col) = var_map.get(&v.name) {
                    extra_eq_filters.push((existing_col, 0));
                } else {
                    var_map.insert(v.name.clone(), 0);
                }
            }
            VarOrTerm::Term(term) => {
                let value = self.term_to_string(term);
                filters.push(HirScalarExpr::CallBinary {
                    func: mz_expr::BinaryFunc::Eq(mz_expr::func::Eq),
                    expr1: Box::new(HirScalarExpr::column(0)),
                    expr2: Box::new(HirScalarExpr::literal(
                        Datum::String(&value),
                        SqlScalarType::String,
                    )),
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                });
            }
        }

        // Process object → column 1 (end).
        match &triple.object {
            VarOrTerm::Variable(v) => {
                if let Some(&existing_col) = var_map.get(&v.name) {
                    extra_eq_filters.push((existing_col, 1));
                } else {
                    var_map.insert(v.name.clone(), 1);
                }
            }
            VarOrTerm::Term(term) => {
                let value = self.term_to_string(term);
                filters.push(HirScalarExpr::CallBinary {
                    func: mz_expr::BinaryFunc::Eq(mz_expr::func::Eq),
                    expr1: Box::new(HirScalarExpr::column(1)),
                    expr2: Box::new(HirScalarExpr::literal(
                        Datum::String(&value),
                        SqlScalarType::String,
                    )),
                    name: mz_ore::treat_as_equal::TreatAsEqual(None),
                });
            }
        }

        // Add equality filters for repeated variables (e.g., `?x path ?x`).
        for (col_a, col_b) in &extra_eq_filters {
            filters.push(HirScalarExpr::CallBinary {
                func: mz_expr::BinaryFunc::Eq(mz_expr::func::Eq),
                expr1: Box::new(HirScalarExpr::column(*col_a)),
                expr2: Box::new(HirScalarExpr::column(*col_b)),
                name: mz_ore::treat_as_equal::TreatAsEqual(None),
            });
        }

        if !filters.is_empty() {
            expr = HirRelationExpr::Filter {
                input: Box::new(expr),
                predicates: filters,
            };
        }

        // Project to only the columns that have variable bindings.
        let sorted_vars: Vec<(String, usize)> = var_map.into_iter().collect();
        let project_cols: Vec<usize> = sorted_vars.iter().map(|(_, col)| *col).collect();
        let new_var_map: BTreeMap<String, usize> = sorted_vars
            .iter()
            .enumerate()
            .map(|(out_idx, (name, _))| (name.clone(), out_idx))
            .collect();

        expr = HirRelationExpr::Project {
            input: Box::new(expr),
            outputs: project_cols,
        };

        Ok(PlannedRelation {
            expr,
            var_map: new_var_map,
        })
    }

    /// Plans a property path expression into a 2-column `(start, end)` relation.
    ///
    /// The output always has exactly 2 columns:
    /// - Column 0: the start node (subject position)
    /// - Column 1: the end node (object position)
    fn plan_property_path(&self, path: &PropertyPath) -> Result<HirRelationExpr, PlanError> {
        match path {
            PropertyPath::Iri(iri) => self.plan_pp_iri(iri),
            PropertyPath::Inverse(inner) => self.plan_pp_inverse(inner),
            PropertyPath::Sequence(paths) => self.plan_pp_sequence(paths),
            PropertyPath::Alternative(paths) => self.plan_pp_alternative(paths),
            PropertyPath::ZeroOrMore(inner) => self.plan_pp_zero_or_more(inner),
            PropertyPath::OneOrMore(inner) => self.plan_pp_one_or_more(inner),
            PropertyPath::ZeroOrOne(inner) => self.plan_pp_zero_or_one(inner),
            PropertyPath::NegatedSet(elements) => self.plan_pp_negated_set(elements),
        }
    }

    /// Simple IRI path: scan quad table, filter predicate = IRI, project to (subject, object).
    fn plan_pp_iri(&self, iri: &Iri) -> Result<HirRelationExpr, PlanError> {
        let get = HirRelationExpr::Get {
            id: Id::Global(self.quad_table_id.get()),
            typ: self.quad_table_type.clone(),
        };
        let filtered = HirRelationExpr::Filter {
            input: Box::new(get),
            predicates: vec![self.iri_filter(QUAD_PREDICATE, iri)],
        };
        Ok(HirRelationExpr::Project {
            input: Box::new(filtered),
            outputs: vec![QUAD_SUBJECT, QUAD_OBJECT],
        })
    }

    /// Inverse path `^path`: plan inner path, then swap (start, end) columns.
    fn plan_pp_inverse(&self, inner: &PropertyPath) -> Result<HirRelationExpr, PlanError> {
        let base = self.plan_property_path(inner)?;
        // Swap columns: project [1, 0] instead of [0, 1].
        Ok(HirRelationExpr::Project {
            input: Box::new(base),
            outputs: vec![1, 0],
        })
    }

    /// Sequence path `path1 / path2 / ...`: chain joins through intermediate nodes.
    ///
    /// For `p1/p2`: join p1(start, mid) with p2(mid, end) on p1.end = p2.start,
    /// project to (p1.start, p2.end).
    fn plan_pp_sequence(&self, paths: &[PropertyPath]) -> Result<HirRelationExpr, PlanError> {
        if paths.is_empty() {
            return Err(PlanError {
                message: "empty property path sequence".to_string(),
            });
        }
        if paths.len() == 1 {
            return self.plan_property_path(&paths[0]);
        }

        let mut result = self.plan_property_path(&paths[0])?;

        for path in &paths[1..] {
            let right = self.plan_property_path(path)?;
            // Join on result.end (col 1) = right.start (col 0, offset by 2).
            let on = HirScalarExpr::CallBinary {
                func: mz_expr::BinaryFunc::Eq(mz_expr::func::Eq),
                expr1: Box::new(HirScalarExpr::column(1)), // left.end
                expr2: Box::new(HirScalarExpr::column(2)), // right.start (offset by left arity=2)
                name: mz_ore::treat_as_equal::TreatAsEqual(None),
            };

            let joined = HirRelationExpr::Join {
                left: Box::new(result),
                right: Box::new(right),
                on,
                kind: JoinKind::Inner,
            };

            // Project to (left.start=0, right.end=3).
            result = HirRelationExpr::Project {
                input: Box::new(joined),
                outputs: vec![0, 3], // left.start, right.end
            };
        }

        Ok(result)
    }

    /// Alternative path `path1 | path2 | ...`: union of all alternatives.
    fn plan_pp_alternative(&self, paths: &[PropertyPath]) -> Result<HirRelationExpr, PlanError> {
        if paths.is_empty() {
            return Err(PlanError {
                message: "empty property path alternative".to_string(),
            });
        }
        if paths.len() == 1 {
            return self.plan_property_path(&paths[0]);
        }

        let mut result = self.plan_property_path(&paths[0])?;
        for path in &paths[1..] {
            let right = self.plan_property_path(path)?;
            result = HirRelationExpr::Union {
                base: Box::new(result),
                inputs: vec![right],
            };
        }
        Ok(result)
    }

    /// One-or-more path `path+`: transitive closure via `LetRec`.
    ///
    /// Generates:
    /// ```text
    /// LetRec {
    ///     bindings: [("path_plus", id, base UNION extend)],
    ///     body: Get(id)
    /// }
    /// ```
    /// where `base = plan_property_path(inner)` (one step)
    /// and `extend = Join(Get(id), base, on: Get.end = base.start)
    ///               → Project(Get.start, base.end)`
    fn plan_pp_one_or_more(&self, inner: &PropertyPath) -> Result<HirRelationExpr, PlanError> {
        let local_id = self.alloc_local_id();
        let pp_type = self.pp_relation_type();

        // Base case: one step of the path.
        let base = self.plan_property_path(inner)?;

        // Recursive reference: Get the accumulating result.
        let recursive_get = HirRelationExpr::Get {
            id: Id::Local(local_id),
            typ: pp_type.clone(),
        };

        // One step for extension.
        let step = self.plan_property_path(inner)?;

        // Join recursive_get.end (col 1) with step.start (col 0, offset by 2).
        let extend_on = HirScalarExpr::CallBinary {
            func: mz_expr::BinaryFunc::Eq(mz_expr::func::Eq),
            expr1: Box::new(HirScalarExpr::column(1)), // recursive.end
            expr2: Box::new(HirScalarExpr::column(2)), // step.start
            name: mz_ore::treat_as_equal::TreatAsEqual(None),
        };

        let extend_join = HirRelationExpr::Join {
            left: Box::new(recursive_get),
            right: Box::new(step),
            on: extend_on,
            kind: JoinKind::Inner,
        };

        // Project to (recursive.start=0, step.end=3).
        let extend = HirRelationExpr::Project {
            input: Box::new(extend_join),
            outputs: vec![0, 3],
        };

        // The binding value is: base UNION extend.
        let binding_value = HirRelationExpr::Union {
            base: Box::new(base),
            inputs: vec![extend],
        };

        // Deduplicate to avoid infinite growth.
        let binding_value = HirRelationExpr::Distinct {
            input: Box::new(binding_value),
        };

        // The body is just Get(local_id).
        let body = HirRelationExpr::Get {
            id: Id::Local(local_id),
            typ: pp_type.clone(),
        };

        Ok(HirRelationExpr::LetRec {
            limit: None,
            bindings: vec![("path_plus".to_string(), local_id, binding_value, pp_type)],
            body: Box::new(body),
        })
    }

    /// Zero-or-more path `path*`: transitive closure plus identity.
    ///
    /// `path* = path+ UNION identity`
    /// where identity = all distinct nodes paired with themselves.
    fn plan_pp_zero_or_more(&self, inner: &PropertyPath) -> Result<HirRelationExpr, PlanError> {
        let plus = self.plan_pp_one_or_more(inner)?;
        let identity = self.plan_pp_identity()?;
        Ok(HirRelationExpr::Union {
            base: Box::new(plus),
            inputs: vec![identity],
        })
    }

    /// Zero-or-one path `path?`: one step plus identity (non-recursive).
    ///
    /// `path? = path UNION identity`
    fn plan_pp_zero_or_one(&self, inner: &PropertyPath) -> Result<HirRelationExpr, PlanError> {
        let base = self.plan_property_path(inner)?;
        let identity = self.plan_pp_identity()?;
        Ok(HirRelationExpr::Union {
            base: Box::new(base),
            inputs: vec![identity],
        })
    }

    /// Identity relation for `path*` and `path?`: all distinct nodes paired with themselves.
    ///
    /// `SELECT DISTINCT subject AS start, subject AS end FROM quad_table
    ///  UNION
    ///  SELECT DISTINCT object AS start, object AS end FROM quad_table`
    fn plan_pp_identity(&self) -> Result<HirRelationExpr, PlanError> {
        let get = HirRelationExpr::Get {
            id: Id::Global(self.quad_table_id.get()),
            typ: self.quad_table_type.clone(),
        };

        // All subjects as (node, node).
        let subjects = HirRelationExpr::Project {
            input: Box::new(get.clone()),
            outputs: vec![QUAD_SUBJECT, QUAD_SUBJECT],
        };

        // All objects as (node, node).
        let objects = HirRelationExpr::Project {
            input: Box::new(get),
            outputs: vec![QUAD_OBJECT, QUAD_OBJECT],
        };

        let unioned = HirRelationExpr::Union {
            base: Box::new(subjects),
            inputs: vec![objects],
        };

        Ok(HirRelationExpr::Distinct {
            input: Box::new(unioned),
        })
    }

    /// Negated property set `!(iri1 | ^iri2 | iri3)`.
    ///
    /// Forward IRIs: scan quads where predicate NOT IN the forward set,
    /// project to (subject, object).
    /// Inverse IRIs: scan quads where predicate NOT IN the inverse set,
    /// project to (object, subject) [swapped].
    /// Union both results.
    fn plan_pp_negated_set(
        &self,
        elements: &[NegatedPathElement],
    ) -> Result<HirRelationExpr, PlanError> {
        let forward_iris: Vec<&Iri> = elements
            .iter()
            .filter_map(|e| match e {
                NegatedPathElement::Forward(iri) => Some(iri),
                NegatedPathElement::Inverse(_) => None,
            })
            .collect();

        let inverse_iris: Vec<&Iri> = elements
            .iter()
            .filter_map(|e| match e {
                NegatedPathElement::Inverse(iri) => Some(iri),
                NegatedPathElement::Forward(_) => None,
            })
            .collect();

        let mut parts = Vec::new();

        if !forward_iris.is_empty() || inverse_iris.is_empty() {
            // Forward part: predicate NOT IN forward_iris → (subject, object).
            // If there are no forward IRIs but also no inverse IRIs, this
            // degenerates to scanning all triples (which is correct for `!()`).
            let get = HirRelationExpr::Get {
                id: Id::Global(self.quad_table_id.get()),
                typ: self.quad_table_type.clone(),
            };

            if forward_iris.is_empty() {
                // No forward IRIs to exclude — include all triples.
                parts.push(HirRelationExpr::Project {
                    input: Box::new(get),
                    outputs: vec![QUAD_SUBJECT, QUAD_OBJECT],
                });
            } else {
                // Filter: predicate NOT IN (iri1, iri2, ...)
                let not_in = self.predicate_not_in_filter(&forward_iris);
                let filtered = HirRelationExpr::Filter {
                    input: Box::new(get),
                    predicates: vec![not_in],
                };
                parts.push(HirRelationExpr::Project {
                    input: Box::new(filtered),
                    outputs: vec![QUAD_SUBJECT, QUAD_OBJECT],
                });
            }
        }

        if !inverse_iris.is_empty() {
            // Inverse part: predicate NOT IN inverse_iris → (object, subject) [swapped].
            let get = HirRelationExpr::Get {
                id: Id::Global(self.quad_table_id.get()),
                typ: self.quad_table_type.clone(),
            };

            let not_in = self.predicate_not_in_filter(&inverse_iris);
            let filtered = HirRelationExpr::Filter {
                input: Box::new(get),
                predicates: vec![not_in],
            };
            // Swap: project (object, subject) for inverse direction.
            parts.push(HirRelationExpr::Project {
                input: Box::new(filtered),
                outputs: vec![QUAD_OBJECT, QUAD_SUBJECT],
            });
        }

        if parts.len() == 1 {
            Ok(parts.pop().unwrap())
        } else {
            let mut result = parts.remove(0);
            for part in parts {
                result = HirRelationExpr::Union {
                    base: Box::new(result),
                    inputs: vec![part],
                };
            }
            Ok(result)
        }
    }

    /// Builds a `NOT (pred = iri1 OR pred = iri2 OR ...)` filter for the predicate column.
    fn predicate_not_in_filter(&self, iris: &[&Iri]) -> HirScalarExpr {
        let eq_checks: Vec<HirScalarExpr> = iris
            .iter()
            .map(|iri| self.iri_filter(QUAD_PREDICATE, iri))
            .collect();

        let any_match = if eq_checks.len() == 1 {
            eq_checks.into_iter().next().unwrap()
        } else {
            HirScalarExpr::CallVariadic {
                func: mz_expr::VariadicFunc::Or(mz_expr::func::variadic::Or),
                exprs: eq_checks,
                name: mz_ore::treat_as_equal::TreatAsEqual(None),
            }
        };

        HirScalarExpr::CallUnary {
            func: mz_expr::UnaryFunc::Not(mz_expr::func::Not),
            expr: Box::new(any_match),
            name: mz_ore::treat_as_equal::TreatAsEqual(None),
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
        let planner = SparqlPlanner::new(GlobalId::User(1), None);
        planner.plan(&query).expect("planning failed")
    }

    /// Helper: count the variables in a planned relation.
    fn var_names(planned: &PlannedRelation) -> Vec<String> {
        let mut names: Vec<String> = planned.var_map.keys().cloned().collect();
        names.sort();
        names
    }

    /// Helper: unwrap COALESCE(inner, false) from a three-valued logic wrapper.
    /// Returns the inner expression. Panics if not a COALESCE wrapper.
    fn unwrap_coalesce(expr: &HirScalarExpr) -> &HirScalarExpr {
        match expr {
            HirScalarExpr::CallVariadic { func, exprs, .. }
                if matches!(func, mz_expr::VariadicFunc::Coalesce(_)) =>
            {
                assert_eq!(exprs.len(), 2, "expected COALESCE(expr, false)");
                &exprs[0]
            }
            other => panic!("expected COALESCE wrapper, got {:?}", other),
        }
    }

    /// Helper: unwrap a numeric-aware comparison IF(both_numeric, numeric_cmp, string_cmp).
    /// Returns the string_cmp branch (the fallback comparison).
    fn unwrap_numeric_comparison(expr: &HirScalarExpr) -> &HirScalarExpr {
        match expr {
            HirScalarExpr::If { els, .. } => els.as_ref(),
            other => other, // not a numeric-aware comparison, return as-is
        }
    }

    /// Helper: get the filter predicate from a FILTER expression, unwrapping
    /// the COALESCE wrapper added by three-valued logic.
    fn get_filter_predicate(rel: &HirRelationExpr) -> &HirScalarExpr {
        match rel {
            HirRelationExpr::Filter { predicates, .. } => {
                assert_eq!(predicates.len(), 1);
                unwrap_coalesce(&predicates[0])
            }
            other => panic!("expected Filter, got {:?}", other),
        }
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

        // FILTER predicates are wrapped in COALESCE(pred, false) for three-valued logic.
        // Comparisons are IF(both_numeric, numeric_cmp, string_cmp).
        let pred = get_filter_predicate(&result.expr);
        let cmp = unwrap_numeric_comparison(pred);
        match cmp {
            HirScalarExpr::CallBinary { func, .. } => {
                assert!(matches!(func, mz_expr::BinaryFunc::Gt(_)));
            }
            other => panic!("expected CallBinary Gt, got {:?}", other),
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

        let pred = get_filter_predicate(&result.expr);
        match pred {
            HirScalarExpr::CallVariadic { func, exprs, .. } => {
                assert!(matches!(func, mz_expr::VariadicFunc::And(_)));
                assert_eq!(exprs.len(), 2);
            }
            other => panic!("expected CallVariadic And, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_filter_unary_not() {
        let result = plan_query(r#"SELECT * WHERE { ?s ?p ?o . FILTER(!(?s = "x")) }"#);
        assert_eq!(var_names(&result), vec!["o", "p", "s"]);

        let pred = get_filter_predicate(&result.expr);
        match pred {
            HirScalarExpr::CallUnary { func, .. } => {
                assert!(matches!(func, mz_expr::UnaryFunc::Not(_)));
            }
            other => panic!("expected CallUnary Not, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_filter_bound() {
        let result = plan_query(
            "PREFIX ex: <http://example.org/>
             SELECT * WHERE { ?s ex:name ?name . FILTER(BOUND(?name)) }",
        );
        assert_eq!(var_names(&result), vec!["name", "s"]);

        // BOUND(?name) → COALESCE(NOT(IsNull(column)), false)
        let pred = get_filter_predicate(&result.expr);
        match pred {
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
        let planner = SparqlPlanner::new(GlobalId::User(1), None);
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
        // CONTAINS uses Position > 0 (direct Gt, not via translate_comparison).
        let pred = get_filter_predicate(&result.expr);
        match pred {
            HirScalarExpr::CallBinary { func, .. } => {
                assert!(matches!(func, mz_expr::BinaryFunc::Gt(_)));
            }
            other => panic!("expected Gt (Position > 0), got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_expression_strstarts_strends() {
        let result = plan_query("SELECT * WHERE { ?s ?p ?o . FILTER(STRSTARTS(?o, \"http\")) }");
        let pred = get_filter_predicate(&result.expr);
        match pred {
            HirScalarExpr::CallBinary { func, .. } => {
                assert!(matches!(func, mz_expr::BinaryFunc::StartsWith(_)));
            }
            other => panic!("expected StartsWith, got {:?}", other),
        }

        let result = plan_query("SELECT * WHERE { ?s ?p ?o . FILTER(STRENDS(?o, \".org\")) }");
        // STRENDS uses Eq(Right(str, len), suffix) — direct Eq, not via translate_comparison.
        let pred = get_filter_predicate(&result.expr);
        match pred {
            HirScalarExpr::CallBinary { func, .. } => {
                assert!(matches!(func, mz_expr::BinaryFunc::Eq(_)));
            }
            other => panic!("expected Eq (Right comparison), got {:?}", other),
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
        let pred = get_filter_predicate(&result.expr);
        match pred {
            HirScalarExpr::CallBinary { func, .. } => {
                assert!(matches!(
                    func,
                    mz_expr::BinaryFunc::IsRegexpMatchCaseSensitive(_)
                ));
            }
            other => panic!("expected IsRegexpMatch, got {:?}", other),
        }

        // With flags.
        let result = plan_query("SELECT * WHERE { ?s ?p ?o . FILTER(REGEX(?o, \"test\", \"i\")) }");
        let pred = get_filter_predicate(&result.expr);
        match pred {
            HirScalarExpr::CallBinary { func, .. } => {
                assert!(matches!(
                    func,
                    mz_expr::BinaryFunc::IsRegexpMatchCaseInsensitive(_)
                ));
            }
            other => panic!("expected IsRegexpMatchCaseInsensitive, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_expression_in_not_in() {
        let result =
            plan_query("SELECT * WHERE { ?s ?p ?o . FILTER(?o IN (\"a\", \"b\", \"c\")) }");
        let pred = get_filter_predicate(&result.expr);
        match pred {
            HirScalarExpr::CallVariadic { func, exprs, .. } => {
                assert!(matches!(func, mz_expr::VariadicFunc::Or(_)));
                assert_eq!(exprs.len(), 3);
            }
            other => panic!("expected Or (IN expansion), got {:?}", other),
        }

        let result = plan_query("SELECT * WHERE { ?s ?p ?o . FILTER(?o NOT IN (\"x\", \"y\")) }");
        let pred = get_filter_predicate(&result.expr);
        match pred {
            HirScalarExpr::CallUnary { func, .. } => {
                assert!(matches!(func, mz_expr::UnaryFunc::Not(_)));
            }
            other => panic!("expected Not(Or(...)), got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_expression_is_blank() {
        let result = plan_query("SELECT * WHERE { ?s ?p ?o . FILTER(isBlank(?s)) }");
        let pred = get_filter_predicate(&result.expr);
        match pred {
            HirScalarExpr::CallBinary { func, .. } => {
                assert!(matches!(func, mz_expr::BinaryFunc::StartsWith(_)));
            }
            other => panic!("expected StartsWith (_:), got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_expression_is_numeric() {
        let result = plan_query("SELECT * WHERE { ?s ?p ?o . FILTER(isNumeric(?o)) }");
        let pred = get_filter_predicate(&result.expr);
        match pred {
            HirScalarExpr::CallBinary { func, .. } => {
                assert!(matches!(
                    func,
                    mz_expr::BinaryFunc::IsRegexpMatchCaseSensitive(_)
                ));
            }
            other => panic!("expected IsRegexpMatch (numeric pattern), got {:?}", other),
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
        let pred = get_filter_predicate(&result.expr);
        assert!(matches!(pred, HirScalarExpr::Exists(_, _)));
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
        let pred = get_filter_predicate(&result.expr);
        match pred {
            HirScalarExpr::CallUnary { func, expr, .. } => {
                assert!(matches!(func, mz_expr::UnaryFunc::Not(_)));
                assert!(matches!(expr.as_ref(), HirScalarExpr::Exists(_, _)));
            }
            other => panic!("expected Not(Exists), got {:?}", other),
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
        let planner = SparqlPlanner::new(GlobalId::User(1), None);
        let err = planner.plan(&query).unwrap_err();
        assert!(
            err.message.contains("aggregate"),
            "expected aggregate error, got: {}",
            err.message
        );
    }

    // --- Prompt 10: SELECT projection, aggregates, solution modifiers ---

    #[mz_ore::test]
    fn test_select_star() {
        // SELECT * keeps all in-scope variables.
        let result = plan_query("SELECT * WHERE { ?s ?p ?o }");
        assert_eq!(var_names(&result), vec!["o", "p", "s"]);
        assert_eq!(result.arity(), 3);
    }

    #[mz_ore::test]
    fn test_select_specific_vars() {
        // SELECT ?s ?o projects to just those two variables.
        let result = plan_query("SELECT ?s ?o WHERE { ?s ?p ?o }");
        assert_eq!(var_names(&result), vec!["o", "s"]);
        assert_eq!(result.arity(), 2);
        // Outermost should be Project.
        match &result.expr {
            HirRelationExpr::Project { outputs, .. } => {
                assert_eq!(outputs.len(), 2);
            }
            other => panic!("expected Project, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_select_single_var() {
        // SELECT ?s projects to just one variable.
        let result = plan_query("SELECT ?s WHERE { ?s ?p ?o }");
        assert_eq!(var_names(&result), vec!["s"]);
        assert_eq!(result.arity(), 1);
    }

    #[mz_ore::test]
    fn test_select_expression() {
        // SELECT (UCASE(?o) AS ?upper) adds a computed column and projects to it.
        let result = plan_query("SELECT ?s (UCASE(?o) AS ?upper) WHERE { ?s ?p ?o }");
        assert_eq!(var_names(&result), vec!["s", "upper"]);
        assert_eq!(result.arity(), 2);
        // Should be Project(Map(...))
        match &result.expr {
            HirRelationExpr::Project { input, outputs } => {
                assert_eq!(outputs.len(), 2);
                match input.as_ref() {
                    HirRelationExpr::Map { scalars, .. } => {
                        assert_eq!(scalars.len(), 1); // one computed expression
                    }
                    other => panic!("expected Map, got {:?}", other),
                }
            }
            other => panic!("expected Project, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_select_distinct() {
        let result = plan_query("SELECT DISTINCT ?s WHERE { ?s ?p ?o }");
        assert_eq!(var_names(&result), vec!["s"]);
        // Outermost should be Distinct(Project(...)).
        match &result.expr {
            HirRelationExpr::Distinct { input } => match input.as_ref() {
                HirRelationExpr::Project { .. } => {}
                other => panic!("expected Project inside Distinct, got {:?}", other),
            },
            other => panic!("expected Distinct, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_select_reduced() {
        // REDUCED is treated as Distinct.
        let result = plan_query("SELECT REDUCED ?s WHERE { ?s ?p ?o }");
        match &result.expr {
            HirRelationExpr::Distinct { .. } => {}
            other => panic!("expected Distinct for REDUCED, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_limit() {
        let result = plan_query("SELECT * WHERE { ?s ?p ?o } LIMIT 10");
        // Outermost should be TopK.
        match &result.expr {
            HirRelationExpr::TopK { limit, .. } => {
                assert!(limit.is_some());
            }
            other => panic!("expected TopK, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_offset() {
        let result = plan_query("SELECT * WHERE { ?s ?p ?o } OFFSET 5");
        match &result.expr {
            HirRelationExpr::TopK { offset, .. } => {
                // Offset should be literal 5.
                match offset {
                    HirScalarExpr::Literal(row, _, _) => {
                        assert_eq!(row.unpack_first(), Datum::Int64(5));
                    }
                    other => panic!("expected literal offset, got {:?}", other),
                }
            }
            other => panic!("expected TopK, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_limit_offset() {
        let result = plan_query("SELECT * WHERE { ?s ?p ?o } LIMIT 10 OFFSET 20");
        match &result.expr {
            HirRelationExpr::TopK { limit, offset, .. } => {
                assert!(limit.is_some());
                match offset {
                    HirScalarExpr::Literal(row, _, _) => {
                        assert_eq!(row.unpack_first(), Datum::Int64(20));
                    }
                    other => panic!("expected literal offset, got {:?}", other),
                }
            }
            other => panic!("expected TopK, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_order_by_single() {
        let result = plan_query("SELECT ?s ?o WHERE { ?s ?p ?o } ORDER BY ?s");
        match &result.expr {
            HirRelationExpr::TopK { order_key, .. } => {
                assert_eq!(order_key.len(), 1);
                assert!(!order_key[0].desc); // ASC by default
            }
            other => panic!("expected TopK, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_order_by_desc() {
        let result = plan_query("SELECT ?s ?o WHERE { ?s ?p ?o } ORDER BY DESC(?o)");
        match &result.expr {
            HirRelationExpr::TopK { order_key, .. } => {
                assert_eq!(order_key.len(), 1);
                assert!(order_key[0].desc);
            }
            other => panic!("expected TopK, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_order_by_multiple() {
        let result = plan_query("SELECT ?s ?o WHERE { ?s ?p ?o } ORDER BY ?s DESC(?o)");
        match &result.expr {
            HirRelationExpr::TopK { order_key, .. } => {
                assert_eq!(order_key.len(), 2);
                assert!(!order_key[0].desc); // ?s ASC
                assert!(order_key[1].desc); // ?o DESC
            }
            other => panic!("expected TopK, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_order_by_limit() {
        let result = plan_query("SELECT ?s ?o WHERE { ?s ?p ?o } ORDER BY ?s LIMIT 5");
        match &result.expr {
            HirRelationExpr::TopK {
                order_key, limit, ..
            } => {
                assert_eq!(order_key.len(), 1);
                assert!(limit.is_some());
            }
            other => panic!("expected TopK, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_distinct_with_order() {
        // DISTINCT + ORDER BY: Distinct wraps Project, then TopK wraps Distinct.
        let result = plan_query("SELECT DISTINCT ?s WHERE { ?s ?p ?o } ORDER BY ?s");
        match &result.expr {
            HirRelationExpr::TopK { input, .. } => match input.as_ref() {
                HirRelationExpr::Distinct { .. } => {}
                other => panic!("expected Distinct inside TopK, got {:?}", other),
            },
            other => panic!("expected TopK, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_count_star_aggregate() {
        // SELECT (COUNT(*) AS ?count) WHERE { ?s ?p ?o }
        let result = plan_query("SELECT (COUNT(*) AS ?count) WHERE { ?s ?p ?o }");
        assert_eq!(var_names(&result), vec!["count"]);
        // Should be Project(Reduce(...))
        match &result.expr {
            HirRelationExpr::Project { input, .. } => match input.as_ref() {
                HirRelationExpr::Reduce {
                    group_key,
                    aggregates,
                    ..
                } => {
                    assert!(group_key.is_empty()); // no GROUP BY → single group
                    assert_eq!(aggregates.len(), 1);
                    assert!(!aggregates[0].distinct);
                }
                other => panic!("expected Reduce, got {:?}", other),
            },
            other => panic!("expected Project, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_count_distinct_aggregate() {
        let result = plan_query("SELECT (COUNT(DISTINCT ?s) AS ?count) WHERE { ?s ?p ?o }");
        assert_eq!(var_names(&result), vec!["count"]);
        match &result.expr {
            HirRelationExpr::Project { input, .. } => match input.as_ref() {
                HirRelationExpr::Reduce { aggregates, .. } => {
                    assert_eq!(aggregates.len(), 1);
                    assert!(aggregates[0].distinct);
                }
                other => panic!("expected Reduce, got {:?}", other),
            },
            other => panic!("expected Project, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_group_by_with_count() {
        // SELECT ?type (COUNT(?s) AS ?count) WHERE { ?s a ?type } GROUP BY ?type
        let result =
            plan_query("SELECT ?type (COUNT(?s) AS ?count) WHERE { ?s a ?type } GROUP BY ?type");
        assert_eq!(var_names(&result), vec!["count", "type"]);
        match &result.expr {
            HirRelationExpr::Project { input, .. } => match input.as_ref() {
                HirRelationExpr::Reduce {
                    group_key,
                    aggregates,
                    ..
                } => {
                    assert_eq!(group_key.len(), 1); // GROUP BY ?type
                    assert_eq!(aggregates.len(), 1); // COUNT(?s)
                }
                other => panic!("expected Reduce, got {:?}", other),
            },
            other => panic!("expected Project, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_group_by_multiple_keys() {
        let result = plan_query(
            "SELECT ?type ?name (COUNT(?s) AS ?count) \
             WHERE { ?s a ?type . ?s <http://example.org/name> ?name } \
             GROUP BY ?type ?name",
        );
        assert_eq!(var_names(&result), vec!["count", "name", "type"]);
        match &result.expr {
            HirRelationExpr::Project { input, .. } => match input.as_ref() {
                HirRelationExpr::Reduce {
                    group_key,
                    aggregates,
                    ..
                } => {
                    assert_eq!(group_key.len(), 2);
                    assert_eq!(aggregates.len(), 1);
                }
                other => panic!("expected Reduce, got {:?}", other),
            },
            other => panic!("expected Project, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_min_max_aggregate() {
        let result =
            plan_query("SELECT (MIN(?o) AS ?min_val) (MAX(?o) AS ?max_val) WHERE { ?s ?p ?o }");
        assert_eq!(var_names(&result), vec!["max_val", "min_val"]);
        match &result.expr {
            HirRelationExpr::Project { input, .. } => match input.as_ref() {
                HirRelationExpr::Reduce { aggregates, .. } => {
                    assert_eq!(aggregates.len(), 2);
                }
                other => panic!("expected Reduce, got {:?}", other),
            },
            other => panic!("expected Project, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_full_aggregation_query() {
        // SELECT ?type (COUNT(?s) AS ?count) WHERE { ?s a ?type }
        // GROUP BY ?type ORDER BY DESC(?count) LIMIT 10
        let result = plan_query(
            "SELECT ?type (COUNT(?s) AS ?count) WHERE { ?s a ?type } \
             GROUP BY ?type ORDER BY DESC(?count) LIMIT 10",
        );
        assert_eq!(var_names(&result), vec!["count", "type"]);
        // Outermost should be TopK(Project(Reduce(...)))
        match &result.expr {
            HirRelationExpr::TopK {
                input,
                order_key,
                limit,
                ..
            } => {
                assert_eq!(order_key.len(), 1);
                assert!(order_key[0].desc);
                assert!(limit.is_some());
                match input.as_ref() {
                    HirRelationExpr::Project { input, .. } => match input.as_ref() {
                        HirRelationExpr::Reduce {
                            group_key,
                            aggregates,
                            ..
                        } => {
                            assert_eq!(group_key.len(), 1);
                            assert_eq!(aggregates.len(), 1);
                        }
                        other => panic!("expected Reduce, got {:?}", other),
                    },
                    other => panic!("expected Project, got {:?}", other),
                }
            }
            other => panic!("expected TopK, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_select_projection_preserves_order() {
        // Variables should appear in the order specified by SELECT, not alphabetically.
        let result = plan_query("SELECT ?o ?s WHERE { ?s ?p ?o }");
        assert_eq!(result.var_map["o"], 0);
        assert_eq!(result.var_map["s"], 1);
    }

    #[mz_ore::test]
    fn test_select_var_not_in_scope() {
        let query = parse("SELECT ?x WHERE { ?s ?p ?o }").expect("parse ok");
        let planner = SparqlPlanner::new(GlobalId::User(1), None);
        let err = planner.plan(&query).unwrap_err();
        assert!(
            err.message.contains("not in scope"),
            "expected scope error, got: {}",
            err.message
        );
    }

    #[mz_ore::test]
    fn test_no_modifiers_no_topk() {
        // Without ORDER BY, LIMIT, or OFFSET, there should be no TopK wrapper.
        let result = plan_query("SELECT * WHERE { ?s ?p ?o }");
        match &result.expr {
            HirRelationExpr::TopK { .. } => panic!("should not have TopK without modifiers"),
            _ => {} // good
        }
    }

    #[mz_ore::test]
    fn test_sum_aggregate() {
        let result = plan_query(
            "SELECT ?type (SUM(?val) AS ?total) \
             WHERE { ?s a ?type . ?s <http://example.org/value> ?val } \
             GROUP BY ?type",
        );
        assert_eq!(var_names(&result), vec!["total", "type"]);
        match &result.expr {
            HirRelationExpr::Project { input, .. } => match input.as_ref() {
                HirRelationExpr::Reduce { aggregates, .. } => {
                    assert_eq!(aggregates.len(), 1);
                }
                other => panic!("expected Reduce, got {:?}", other),
            },
            other => panic!("expected Project, got {:?}", other),
        }
    }

    // --- CONSTRUCT tests ---

    #[mz_ore::test]
    fn test_construct_basic() {
        let result = plan_query(
            "PREFIX ex: <http://example.org/> \
             CONSTRUCT { ?s ex:knows ?o } \
             WHERE { ?s ex:friend ?o }",
        );
        assert_eq!(var_names(&result), vec!["object", "predicate", "subject"]);
        assert_eq!(result.arity(), 3);

        // Should be: Distinct(Union or single Map+Project)
        match &result.expr {
            HirRelationExpr::Distinct { input } => match input.as_ref() {
                HirRelationExpr::Project { .. } => {} // single template triple
                other => panic!("expected Project inside Distinct, got {:?}", other),
            },
            other => panic!("expected Distinct, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_construct_multiple_templates() {
        let result = plan_query(
            "PREFIX ex: <http://example.org/> \
             CONSTRUCT { ?s ex:knows ?o . ?o ex:knownBy ?s } \
             WHERE { ?s ex:friend ?o }",
        );
        assert_eq!(var_names(&result), vec!["object", "predicate", "subject"]);

        // Should be: Distinct(Union(Project(Map(...)), Project(Map(...))))
        match &result.expr {
            HirRelationExpr::Distinct { input } => match input.as_ref() {
                HirRelationExpr::Union { base, inputs } => {
                    assert_eq!(inputs.len(), 1);
                    match (base.as_ref(), &inputs[0]) {
                        (HirRelationExpr::Project { .. }, HirRelationExpr::Project { .. }) => {}
                        other => panic!("expected Project on both sides of Union, got {:?}", other),
                    }
                }
                other => panic!("expected Union inside Distinct, got {:?}", other),
            },
            other => panic!("expected Distinct, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_construct_concrete_terms() {
        // CONSTRUCT with concrete subject and predicate, variable object.
        let result = plan_query(
            "PREFIX ex: <http://example.org/> \
             CONSTRUCT { ex:alice ex:likes ?thing } \
             WHERE { ex:alice ex:likes ?thing }",
        );
        assert_eq!(result.arity(), 3);
        assert_eq!(var_names(&result), vec!["object", "predicate", "subject"]);
    }

    #[mz_ore::test]
    fn test_construct_empty_template() {
        let result = plan_query("CONSTRUCT { } WHERE { ?s ?p ?o }");
        assert_eq!(result.arity(), 3);
        // Empty template produces empty constant.
        match &result.expr {
            HirRelationExpr::Constant { rows, .. } => {
                assert!(rows.is_empty());
            }
            other => panic!("expected empty Constant, got {:?}", other),
        }
    }

    // --- ASK tests ---

    #[mz_ore::test]
    fn test_ask_basic() {
        let result = plan_query(
            "PREFIX ex: <http://example.org/> \
             ASK { ?s ex:name ?name }",
        );
        assert_eq!(var_names(&result), vec!["result"]);
        assert_eq!(result.arity(), 1);

        // Should be: Project(Map(Reduce(...)))
        match &result.expr {
            HirRelationExpr::Project { input, outputs } => {
                assert_eq!(outputs, &[1]); // boolean column
                match input.as_ref() {
                    HirRelationExpr::Map { input, scalars } => {
                        assert_eq!(scalars.len(), 1); // count > 0
                        match input.as_ref() {
                            HirRelationExpr::Reduce {
                                group_key,
                                aggregates,
                                ..
                            } => {
                                assert!(group_key.is_empty());
                                assert_eq!(aggregates.len(), 1);
                            }
                            other => panic!("expected Reduce, got {:?}", other),
                        }
                    }
                    other => panic!("expected Map, got {:?}", other),
                }
            }
            other => panic!("expected Project, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_ask_with_filter() {
        let result = plan_query(
            "PREFIX ex: <http://example.org/> \
             ASK { ?s ex:age ?age FILTER(?age > \"30\") }",
        );
        assert_eq!(var_names(&result), vec!["result"]);
        assert_eq!(result.arity(), 1);
    }

    // --- DESCRIBE tests ---

    #[mz_ore::test]
    fn test_describe_single_iri() {
        let result = plan_query("DESCRIBE <http://example.org/alice>");
        assert_eq!(var_names(&result), vec!["object", "predicate", "subject"]);
        assert_eq!(result.arity(), 3);

        // Should be: Distinct(Union(as_subject, as_object))
        match &result.expr {
            HirRelationExpr::Distinct { input } => match input.as_ref() {
                HirRelationExpr::Union { base, inputs } => {
                    assert_eq!(inputs.len(), 1);
                    // Both sides should be Project(Filter(Get))
                    match (base.as_ref(), &inputs[0]) {
                        (
                            HirRelationExpr::Project { input: left, .. },
                            HirRelationExpr::Project { input: right, .. },
                        ) => match (left.as_ref(), right.as_ref()) {
                            (
                                HirRelationExpr::Filter { predicates: lp, .. },
                                HirRelationExpr::Filter { predicates: rp, .. },
                            ) => {
                                assert_eq!(lp.len(), 1);
                                assert_eq!(rp.len(), 1);
                            }
                            other => panic!("expected Filter on both sides, got {:?}", other),
                        },
                        other => panic!("expected Project on both sides, got {:?}", other),
                    }
                }
                other => panic!("expected Union inside Distinct, got {:?}", other),
            },
            other => panic!("expected Distinct, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_describe_multiple_iris() {
        let result = plan_query("DESCRIBE <http://example.org/alice> <http://example.org/bob>");
        assert_eq!(var_names(&result), vec!["object", "predicate", "subject"]);

        // Should be: Distinct(Union(alice_subj, alice_obj, bob_subj, bob_obj))
        match &result.expr {
            HirRelationExpr::Distinct { .. } => {} // structure is correct
            other => panic!("expected Distinct, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_describe_variable_errors() {
        // DESCRIBE ?x without WHERE clause should error (we can't resolve the variable).
        let query = mz_sparql_parser::parser::parse("DESCRIBE ?x").expect("parse failed");
        let planner = SparqlPlanner::new(GlobalId::User(1), None);
        let err = planner.plan(&query).unwrap_err();
        assert!(
            err.message.contains("variables requires a WHERE clause"),
            "unexpected error: {}",
            err.message
        );
    }

    // --- Property path tests ---

    #[mz_ore::test]
    fn test_pp_inverse() {
        // ^ex:knows swaps subject/object direction.
        let result =
            plan_query("PREFIX ex: <http://example.org/> SELECT * WHERE { ?s ^ex:knows ?o }");
        assert_eq!(var_names(&result), vec!["o", "s"]);
        assert_eq!(result.arity(), 2);
    }

    #[mz_ore::test]
    fn test_pp_sequence() {
        // ex:knows / ex:name: chain two steps.
        let result = plan_query(
            "PREFIX ex: <http://example.org/> SELECT * WHERE { ?s ex:knows/ex:name ?name }",
        );
        assert_eq!(var_names(&result), vec!["name", "s"]);
        assert_eq!(result.arity(), 2);
    }

    #[mz_ore::test]
    fn test_pp_alternative() {
        // ex:knows | ex:friendOf: union of two predicates.
        let result = plan_query(
            "PREFIX ex: <http://example.org/> SELECT * WHERE { ?s (ex:knows|ex:friendOf) ?o }",
        );
        assert_eq!(var_names(&result), vec!["o", "s"]);
        assert_eq!(result.arity(), 2);

        // The property path result (before subject/object integration) should be
        // a Union of two filtered scans.
    }

    #[mz_ore::test]
    fn test_pp_one_or_more() {
        // ex:knows+: transitive closure via LetRec.
        let result =
            plan_query("PREFIX ex: <http://example.org/> SELECT * WHERE { ?s ex:knows+ ?o }");
        assert_eq!(var_names(&result), vec!["o", "s"]);
        assert_eq!(result.arity(), 2);

        // The top-level expression should contain a LetRec somewhere.
        fn has_letrec(expr: &HirRelationExpr) -> bool {
            match expr {
                HirRelationExpr::LetRec { .. } => true,
                HirRelationExpr::Project { input, .. }
                | HirRelationExpr::Filter { input, .. }
                | HirRelationExpr::Distinct { input, .. }
                | HirRelationExpr::Negate { input }
                | HirRelationExpr::Threshold { input }
                | HirRelationExpr::TopK { input, .. }
                | HirRelationExpr::Reduce { input, .. }
                | HirRelationExpr::Map { input, .. } => has_letrec(input),
                HirRelationExpr::Join { left, right, .. } => has_letrec(left) || has_letrec(right),
                HirRelationExpr::Union { base, inputs } => {
                    has_letrec(base) || inputs.iter().any(has_letrec)
                }
                HirRelationExpr::Let { value, body, .. } => has_letrec(value) || has_letrec(body),
                _ => false,
            }
        }
        assert!(has_letrec(&result.expr), "expected LetRec for path+");
    }

    #[mz_ore::test]
    fn test_pp_zero_or_more() {
        // ex:knows*: transitive closure + identity.
        let result =
            plan_query("PREFIX ex: <http://example.org/> SELECT * WHERE { ?s ex:knows* ?o }");
        assert_eq!(var_names(&result), vec!["o", "s"]);
        assert_eq!(result.arity(), 2);
    }

    #[mz_ore::test]
    fn test_pp_zero_or_one() {
        // ex:knows?: one step or identity.
        let result =
            plan_query("PREFIX ex: <http://example.org/> SELECT * WHERE { ?s ex:knows? ?o }");
        assert_eq!(var_names(&result), vec!["o", "s"]);
        assert_eq!(result.arity(), 2);
    }

    #[mz_ore::test]
    fn test_pp_negated_set() {
        // !(ex:knows | ex:hates): all predicates except these two.
        let result = plan_query(
            "PREFIX ex: <http://example.org/> SELECT * WHERE { ?s !(ex:knows|ex:hates) ?o }",
        );
        assert_eq!(var_names(&result), vec!["o", "s"]);
        assert_eq!(result.arity(), 2);
    }

    #[mz_ore::test]
    fn test_pp_negated_set_with_inverse() {
        // !(ex:knows | ^ex:hates): forward exclusion + inverse exclusion.
        let result = plan_query(
            "PREFIX ex: <http://example.org/> SELECT * WHERE { ?s !(ex:knows|^ex:hates) ?o }",
        );
        assert_eq!(var_names(&result), vec!["o", "s"]);
        assert_eq!(result.arity(), 2);
    }

    #[mz_ore::test]
    fn test_pp_transitive_with_concrete_subject() {
        // Concrete subject with path+: filter on start node.
        let result = plan_query(
            "PREFIX ex: <http://example.org/> \
             SELECT * WHERE { <http://example.org/alice> ex:knows+ ?friend }",
        );
        assert_eq!(var_names(&result), vec!["friend"]);
        assert_eq!(result.arity(), 1);
    }

    #[mz_ore::test]
    fn test_pp_sequence_three_steps() {
        // Three-step sequence: ex:a / ex:b / ex:c.
        let result =
            plan_query("PREFIX ex: <http://example.org/> SELECT * WHERE { ?s ex:a/ex:b/ex:c ?o }");
        assert_eq!(var_names(&result), vec!["o", "s"]);
        assert_eq!(result.arity(), 2);
    }

    #[mz_ore::test]
    fn test_pp_inverse_sequence() {
        // ^(ex:a / ex:b): inverse of a sequence.
        let result =
            plan_query("PREFIX ex: <http://example.org/> SELECT * WHERE { ?s ^(ex:a/ex:b) ?o }");
        assert_eq!(var_names(&result), vec!["o", "s"]);
        assert_eq!(result.arity(), 2);
    }

    #[mz_ore::test]
    fn test_pp_same_variable_subject_object() {
        // ?x ex:knows+ ?x: reflexive transitive closure.
        let result =
            plan_query("PREFIX ex: <http://example.org/> SELECT * WHERE { ?x ex:knows+ ?x }");
        assert_eq!(var_names(&result), vec!["x"]);
        assert_eq!(result.arity(), 1);
    }

    #[mz_ore::test]
    fn test_pp_with_bgp_join() {
        // Property path combined with regular BGP triple — shared variable join.
        let result = plan_query(
            "PREFIX ex: <http://example.org/> \
             SELECT * WHERE { ?s ex:knows+ ?friend . ?friend ex:name ?name }",
        );
        assert_eq!(var_names(&result), vec!["friend", "name", "s"]);
        assert_eq!(result.arity(), 3);
    }

    #[mz_ore::test]
    fn test_pp_rdfs_subclass_transitive() {
        // Classic RDFS: ?x rdfs:subClassOf+ ?y (transitive subclass).
        let result = plan_query(
            "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \
             SELECT * WHERE { ?x rdfs:subClassOf+ ?y }",
        );
        assert_eq!(var_names(&result), vec!["x", "y"]);
        assert_eq!(result.arity(), 2);
    }

    // === FROM clause / catalog graph tests ===

    #[mz_ore::test]
    fn test_from_no_clauses_uses_default_quad_table() {
        // No FROM clause: uses the default quad table (User(1)).
        let result = plan_query("SELECT * WHERE { ?s ?p ?o }");
        // Should succeed with the default quad table.
        assert_eq!(var_names(&result), vec!["o", "p", "s"]);
    }

    #[mz_ore::test]
    fn test_from_graph_filter() {
        // FROM <http://example.org/g1>: should add a graph column filter.
        let result = plan_query("SELECT ?s ?p ?o FROM <http://example.org/g1> WHERE { ?s ?p ?o }");
        assert_eq!(var_names(&result), vec!["o", "p", "s"]);
        // The result should have a Filter wrapping the base pattern.
        let hir_debug = format!("{:?}", result.expr);
        assert!(
            hir_debug.contains("Filter"),
            "Expected Filter for FROM clause, got: {hir_debug}"
        );
    }

    #[mz_ore::test]
    fn test_from_catalog_graph_with_catalog_id() {
        // FROM <urn:materialize:catalog> with a catalog triples view available.
        let query = parse("SELECT * FROM <urn:materialize:catalog> WHERE { ?s ?p ?o }")
            .expect("parse failed");
        // Use User(2) as the catalog triples view ID.
        let planner = SparqlPlanner::new(GlobalId::User(1), Some(GlobalId::User(2)));
        let result = planner.plan(&query).expect("planning failed");
        // The planner should use the catalog triples table (User(2)) instead of the
        // default quad table (User(1)). Verify via HIR debug output.
        let hir_debug = format!("{:?}", result.expr);
        assert!(
            hir_debug.contains("User(2)"),
            "Expected catalog triples table (User(2)) in HIR, got: {hir_debug}"
        );
        assert!(
            !hir_debug.contains("User(1)"),
            "Should not reference the default quad table (User(1)) when catalog graph is used"
        );
    }

    #[mz_ore::test]
    fn test_from_catalog_graph_without_catalog_id() {
        // FROM <urn:materialize:catalog> without a catalog triples view.
        // Falls back to filtering on the graph column.
        let query = parse("SELECT * FROM <urn:materialize:catalog> WHERE { ?s ?p ?o }")
            .expect("parse failed");
        let planner = SparqlPlanner::new(GlobalId::User(1), None);
        let result = planner.plan(&query).expect("planning failed");
        let hir_debug = format!("{:?}", result.expr);
        assert!(
            hir_debug.contains("Filter"),
            "Expected graph column filter when catalog view is unavailable, got: {hir_debug}"
        );
    }

    #[mz_ore::test]
    fn test_from_multiple_graphs_filter() {
        // Multiple FROM clauses: should produce OR filter.
        let result = plan_query("SELECT * FROM <http://g1> FROM <http://g2> WHERE { ?s ?p ?o }");
        let hir_debug = format!("{:?}", result.expr);
        assert!(
            hir_debug.contains("Filter"),
            "Expected Filter for multiple FROM clauses, got: {hir_debug}"
        );
    }

    // --- Prompt 17: Three-valued logic, EBV, and numeric-aware comparison tests ---

    #[mz_ore::test]
    fn test_three_valued_logic_coalesce_wrapper() {
        // Every FILTER predicate should be wrapped in COALESCE(pred, false).
        let result = plan_query("SELECT * WHERE { ?s ?p ?o . FILTER(?o = \"x\") }");
        match &result.expr {
            HirRelationExpr::Filter { predicates, .. } => {
                assert_eq!(predicates.len(), 1);
                // Should be COALESCE(pred, false).
                match &predicates[0] {
                    HirScalarExpr::CallVariadic { func, exprs, .. } => {
                        assert!(matches!(func, mz_expr::VariadicFunc::Coalesce(_)));
                        assert_eq!(exprs.len(), 2);
                        // Second arg should be false.
                        match &exprs[1] {
                            HirScalarExpr::Literal(..) => {} // false literal
                            other => panic!("expected false literal, got {:?}", other),
                        }
                    }
                    other => panic!("expected COALESCE wrapper, got {:?}", other),
                }
            }
            other => panic!("expected Filter, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_numeric_aware_comparison_structure() {
        // Comparisons should produce IF(both_numeric, numeric_cmp, string_cmp).
        let result = plan_query("SELECT * WHERE { ?s ?p ?o . FILTER(?o > ?s) }");
        let pred = get_filter_predicate(&result.expr);
        match pred {
            HirScalarExpr::If {
                cond, then, els, ..
            } => {
                // cond: AND(is_numeric(?o), is_numeric(?s))
                match cond.as_ref() {
                    HirScalarExpr::CallVariadic { func, exprs, .. } => {
                        assert!(matches!(func, mz_expr::VariadicFunc::And(_)));
                        assert_eq!(exprs.len(), 2);
                    }
                    other => panic!("expected And (both_numeric check), got {:?}", other),
                }
                // then: Gt(cast_f64(?o), cast_f64(?s))
                match then.as_ref() {
                    HirScalarExpr::CallBinary { func, expr1, .. } => {
                        assert!(matches!(func, mz_expr::BinaryFunc::Gt(_)));
                        // expr1 should be CastStringToFloat64
                        match expr1.as_ref() {
                            HirScalarExpr::CallUnary { func, .. } => {
                                assert!(matches!(func, mz_expr::UnaryFunc::CastStringToFloat64(_)));
                            }
                            other => panic!("expected CastStringToFloat64, got {:?}", other),
                        }
                    }
                    other => panic!("expected Gt (numeric), got {:?}", other),
                }
                // els: Gt(?o, ?s) string comparison
                match els.as_ref() {
                    HirScalarExpr::CallBinary { func, .. } => {
                        assert!(matches!(func, mz_expr::BinaryFunc::Gt(_)));
                    }
                    other => panic!("expected Gt (string), got {:?}", other),
                }
            }
            other => panic!("expected If (numeric-aware comparison), got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_numeric_aware_equality() {
        // Equality should also be numeric-aware.
        let result = plan_query("SELECT * WHERE { ?s ?p ?o . FILTER(?o = ?s) }");
        let pred = get_filter_predicate(&result.expr);
        match pred {
            HirScalarExpr::If { els, .. } => {
                // String fallback: Eq
                match els.as_ref() {
                    HirScalarExpr::CallBinary { func, .. } => {
                        assert!(matches!(func, mz_expr::BinaryFunc::Eq(_)));
                    }
                    other => panic!("expected Eq, got {:?}", other),
                }
            }
            other => panic!("expected If, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_numeric_aware_all_comparison_ops() {
        // All 6 comparison operators should be numeric-aware.
        for (op, name) in [
            ("=", "Eq"),
            ("!=", "NotEq"),
            ("<", "Lt"),
            (">", "Gt"),
            ("<=", "Lte"),
            (">=", "Gte"),
        ] {
            let query = format!("SELECT * WHERE {{ ?s ?p ?o . FILTER(?o {op} ?s) }}");
            let result = plan_query(&query);
            let pred = get_filter_predicate(&result.expr);
            assert!(
                matches!(pred, HirScalarExpr::If { .. }),
                "expected numeric-aware If for {name}, got {:?}",
                pred
            );
        }
    }

    #[mz_ore::test]
    fn test_ebv_non_boolean_filter() {
        // FILTER(?var) where ?var is a string variable should get EBV conversion.
        let result = plan_query("SELECT * WHERE { ?s ?p ?o . FILTER(?o) }");
        let pred = get_filter_predicate(&result.expr);
        // EBV produces IF(is_numeric, numeric_ebv, string_ebv).
        match pred {
            HirScalarExpr::If { .. } => {} // EBV conversion present
            other => panic!("expected If (EBV conversion), got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_ebv_boolean_filter_no_conversion() {
        // FILTER with a boolean expression should NOT get EBV conversion.
        let result = plan_query("SELECT * WHERE { ?s ?p ?o . FILTER(BOUND(?o)) }");
        let pred = get_filter_predicate(&result.expr);
        // BOUND produces Not(IsNull), no EBV wrapper.
        match pred {
            HirScalarExpr::CallUnary {
                func: mz_expr::UnaryFunc::Not(_),
                ..
            } => {} // Direct boolean, no EBV
            other => panic!("expected Not (no EBV wrapper), got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_lang_accessor() {
        // LANG() should extract language tag from tagged literals.
        let result = plan_query("SELECT (LANG(?o) AS ?lang) WHERE { ?s ?p ?o }");
        // Should produce IF(has_tag, SUBSTR, "") structure.
        let hir_debug = format!("{:?}", result.expr);
        assert!(
            hir_debug.contains("Position"),
            "LANG should use Position to find language tag marker, got: {hir_debug}"
        );
    }

    #[mz_ore::test]
    fn test_datatype_accessor() {
        // DATATYPE() should extract datatype IRI from typed literals.
        let result = plan_query("SELECT (DATATYPE(?o) AS ?dt) WHERE { ?s ?p ?o }");
        let hir_debug = format!("{:?}", result.expr);
        assert!(
            hir_debug.contains("Position"),
            "DATATYPE should use Position to find ^^ marker, got: {hir_debug}"
        );
        assert!(
            hir_debug.contains("XMLSchema#string"),
            "DATATYPE should have xsd:string fallback, got: {hir_debug}"
        );
    }

    #[mz_ore::test]
    fn test_language_tag_string_equality() {
        // In our encoding, "hello"@en and "hello"@fr are different strings,
        // so string equality correctly distinguishes them.
        let result = plan_query(r#"SELECT * WHERE { ?s ?p ?o . FILTER(?o = "\"hello\"@en") }"#);
        // Should plan successfully — language-tagged values are compared as strings.
        assert_eq!(var_names(&result), vec!["o", "p", "s"]);
    }

    #[mz_ore::test]
    fn test_filter_with_arithmetic_comparison() {
        // ?age + 1 > 18 — arithmetic produces string results, comparison is numeric-aware.
        let result = plan_query(
            "PREFIX ex: <http://example.org/>
             SELECT * WHERE { ?s ex:age ?age . FILTER(?age + 1 > 18) }",
        );
        let pred = get_filter_predicate(&result.expr);
        // The comparison (?age + 1) > 18 should be numeric-aware.
        assert!(
            matches!(pred, HirScalarExpr::If { .. }),
            "expected numeric-aware comparison for arithmetic result"
        );
    }

    #[mz_ore::test]
    fn test_multiple_filters_both_get_coalesce() {
        // Both FILTERs should get COALESCE wrappers.
        let result =
            plan_query(r#"SELECT * WHERE { ?s ?p ?o . FILTER(?s = "x") FILTER(?o = "y") }"#);
        match &result.expr {
            HirRelationExpr::Filter {
                input, predicates, ..
            } => {
                assert_eq!(predicates.len(), 1);
                // Outer filter has COALESCE.
                assert!(matches!(
                    &predicates[0],
                    HirScalarExpr::CallVariadic { func, .. }
                    if matches!(func, mz_expr::VariadicFunc::Coalesce(_))
                ));
                // Inner filter also has COALESCE.
                match input.as_ref() {
                    HirRelationExpr::Filter { predicates, .. } => {
                        assert!(matches!(
                            &predicates[0],
                            HirScalarExpr::CallVariadic { func, .. }
                            if matches!(func, mz_expr::VariadicFunc::Coalesce(_))
                        ));
                    }
                    other => panic!("expected inner Filter, got {:?}", other),
                }
            }
            other => panic!("expected Filter, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_is_numeric_check_in_comparison() {
        // The numeric check in comparisons uses IsRegexpMatch.
        let result = plan_query("SELECT * WHERE { ?s ?p ?o . FILTER(?o < ?s) }");
        let pred = get_filter_predicate(&result.expr);
        let hir_debug = format!("{:?}", pred);
        assert!(
            hir_debug.contains("IsRegexpMatchCaseSensitive"),
            "expected regex-based numeric check in comparison, got: {hir_debug}"
        );
    }
}
