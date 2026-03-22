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
    GraphTerm, GroupGraphPattern, Iri, RdfLiteral, SparqlQuery, TriplePattern, VarOrTerm, VerbPath,
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
            _ => Err(PlanError {
                message: "unsupported graph pattern form (will be implemented in later prompts)".to_string(),
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

    /// Plans a group of sub-patterns (joined together).
    fn plan_group(&self, patterns: &[GroupGraphPattern]) -> Result<PlannedRelation, PlanError> {
        if patterns.is_empty() {
            return Ok(PlannedRelation {
                expr: HirRelationExpr::Constant {
                    rows: vec![mz_repr::Row::default()],
                    typ: SqlRelationType::empty(),
                },
                var_map: BTreeMap::new(),
            });
        }

        let mut result = self.plan_pattern(&patterns[0])?;
        for pattern in &patterns[1..] {
            let right = self.plan_pattern(pattern)?;
            result = self.join_on_shared_vars(result, right)?;
        }
        Ok(result)
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
}
