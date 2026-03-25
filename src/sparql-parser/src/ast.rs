// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! SPARQL 1.1 Abstract Syntax Tree.
//!
//! These types represent the parsed structure of a SPARQL query, closely
//! following the grammar in [W3C SPARQL 1.1 Query Language, Section 19](
//! https://www.w3.org/TR/sparql11-query/#sparqlGrammar).

use std::fmt;

/// A SPARQL variable, e.g. `?x` or `$x`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Variable {
    pub name: String,
}

impl fmt::Display for Variable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "?{}", self.name)
    }
}

/// An IRI (Internationalized Resource Identifier).
///
/// May be a full IRI (`<http://example.org/foo>`) or resolved from a
/// prefixed name (`ex:foo`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Iri {
    pub value: String,
}

impl fmt::Display for Iri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<{}>", self.value)
    }
}

/// An RDF literal value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RdfLiteral {
    /// A simple string literal: `"hello"`.
    Simple(String),
    /// A language-tagged literal: `"hello"@en`.
    LanguageTagged { value: String, language: String },
    /// A datatype-annotated literal: `"42"^^<xsd:integer>`.
    Typed { value: String, datatype: Iri },
}

impl fmt::Display for RdfLiteral {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RdfLiteral::Simple(s) => write!(f, "\"{}\"", s),
            RdfLiteral::LanguageTagged { value, language } => {
                write!(f, "\"{}\"@{}", value, language)
            }
            RdfLiteral::Typed { value, datatype } => write!(f, "\"{}\"^^{}", value, datatype),
        }
    }
}

/// An RDF term that can appear in a triple pattern.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphTerm {
    Iri(Iri),
    Literal(RdfLiteral),
    /// A blank node label, e.g. `_:b0`.
    BlankNode(String),
    /// Numeric literal (stored as string to preserve exact form).
    NumericLiteral(String),
    /// Boolean literal (`true` or `false`).
    BooleanLiteral(bool),
}

/// A node in subject or object position that can be a variable or a concrete term.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VarOrTerm {
    Variable(Variable),
    Term(GraphTerm),
}

/// A node in predicate position: a variable, an IRI, or the shorthand `a`
/// (which stands for `rdf:type`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VarOrIri {
    Variable(Variable),
    Iri(Iri),
}

/// A property path expression (SPARQL 1.1 Section 9).
///
/// Property paths allow traversal of RDF graphs beyond simple predicate matching.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PropertyPath {
    /// A single IRI predicate.
    Iri(Iri),
    /// Inverse path: `^path`.
    Inverse(Box<PropertyPath>),
    /// Sequence path: `path1 / path2`.
    Sequence(Vec<PropertyPath>),
    /// Alternative path: `path1 | path2`.
    Alternative(Vec<PropertyPath>),
    /// Zero or more: `path*`.
    ZeroOrMore(Box<PropertyPath>),
    /// One or more: `path+`.
    OneOrMore(Box<PropertyPath>),
    /// Zero or one: `path?`.
    ZeroOrOne(Box<PropertyPath>),
    /// Negated property set: `!(iri1 | ^iri2 | iri3)`.
    NegatedSet(Vec<NegatedPathElement>),
}

/// An element of a negated property set: either a forward or inverse IRI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NegatedPathElement {
    Forward(Iri),
    Inverse(Iri),
}

/// The predicate in a triple pattern: either a simple variable/IRI or a
/// property path expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerbPath {
    /// A simple variable.
    Variable(Variable),
    /// A property path (which may be a simple IRI).
    Path(PropertyPath),
}

/// A triple pattern: `?s ?p ?o`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriplePattern {
    pub subject: VarOrTerm,
    pub predicate: VerbPath,
    pub object: VarOrTerm,
}

/// A SPARQL expression (used in FILTER, BIND, SELECT expressions, HAVING, ORDER BY).
#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    /// A variable reference.
    Variable(Variable),
    /// An RDF literal.
    Literal(RdfLiteral),
    /// A numeric literal.
    NumericLiteral(String),
    /// A boolean literal.
    BooleanLiteral(bool),
    /// An IRI used as a value.
    Iri(Iri),

    // Arithmetic
    Add(Box<Expression>, Box<Expression>),
    Subtract(Box<Expression>, Box<Expression>),
    Multiply(Box<Expression>, Box<Expression>),
    Divide(Box<Expression>, Box<Expression>),
    UnaryPlus(Box<Expression>),
    UnaryMinus(Box<Expression>),
    UnaryNot(Box<Expression>),

    // Comparison
    Equal(Box<Expression>, Box<Expression>),
    NotEqual(Box<Expression>, Box<Expression>),
    LessThan(Box<Expression>, Box<Expression>),
    GreaterThan(Box<Expression>, Box<Expression>),
    LessThanOrEqual(Box<Expression>, Box<Expression>),
    GreaterThanOrEqual(Box<Expression>, Box<Expression>),

    // Logical
    And(Box<Expression>, Box<Expression>),
    Or(Box<Expression>, Box<Expression>),

    // Built-in functions
    /// `BOUND(?var)`
    Bound(Variable),
    /// `IF(cond, then, else)`
    If(Box<Expression>, Box<Expression>, Box<Expression>),
    /// `COALESCE(expr, ...)`
    Coalesce(Vec<Expression>),
    /// `EXISTS { pattern }`
    Exists(Box<GroupGraphPattern>),
    /// `NOT EXISTS { pattern }`
    NotExists(Box<GroupGraphPattern>),

    // Type testing functions
    IsIri(Box<Expression>),
    IsBlank(Box<Expression>),
    IsLiteral(Box<Expression>),
    IsNumeric(Box<Expression>),

    // Accessor functions
    Str(Box<Expression>),
    Lang(Box<Expression>),
    Datatype(Box<Expression>),

    // String functions
    Strlen(Box<Expression>),
    Substr(Box<Expression>, Box<Expression>, Option<Box<Expression>>),
    Ucase(Box<Expression>),
    Lcase(Box<Expression>),
    StrStarts(Box<Expression>, Box<Expression>),
    StrEnds(Box<Expression>, Box<Expression>),
    Contains(Box<Expression>, Box<Expression>),
    Concat(Vec<Expression>),
    Replace(
        Box<Expression>,
        Box<Expression>,
        Box<Expression>,
        Option<Box<Expression>>,
    ),
    Regex(Box<Expression>, Box<Expression>, Option<Box<Expression>>),

    // Aggregate functions (valid only in SELECT/HAVING expressions)
    Count {
        expr: Option<Box<Expression>>,
        distinct: bool,
    },
    Sum {
        expr: Box<Expression>,
        distinct: bool,
    },
    Avg {
        expr: Box<Expression>,
        distinct: bool,
    },
    Min {
        expr: Box<Expression>,
        distinct: bool,
    },
    Max {
        expr: Box<Expression>,
        distinct: bool,
    },
    GroupConcat {
        expr: Box<Expression>,
        distinct: bool,
        separator: Option<String>,
    },
    Sample {
        expr: Box<Expression>,
        distinct: bool,
    },

    /// A generic function call: `iri(args...)`.
    FunctionCall(Iri, Vec<Expression>),

    /// `IN (expr, ...)` — value membership test.
    In(Box<Expression>, Vec<Expression>),
    /// `NOT IN (expr, ...)`.
    NotIn(Box<Expression>, Vec<Expression>),
}

/// A PREFIX declaration: `PREFIX ex: <http://example.org/>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrefixDecl {
    pub prefix: String,
    pub iri: Iri,
}

/// A single entry in a VALUES block: a row of values.
pub type ValuesRow = Vec<Option<GraphTerm>>;

/// A graph pattern — the body of a WHERE clause.
///
/// This corresponds to `GroupGraphPattern` in the SPARQL grammar and
/// represents the different forms of graph patterns that can be composed.
#[derive(Debug, Clone, PartialEq)]
pub enum GroupGraphPattern {
    /// Basic graph pattern: a conjunction of triple patterns.
    Basic(Vec<TriplePattern>),
    /// A group of sub-patterns (evaluated as a join).
    Group(Vec<GroupGraphPattern>),
    /// `OPTIONAL { pattern }` — left outer join.
    Optional(Box<GroupGraphPattern>),
    /// `{ pattern } UNION { pattern }` — outer union.
    Union(Box<GroupGraphPattern>, Box<GroupGraphPattern>),
    /// `MINUS { pattern }` — anti-join on shared variables.
    Minus(Box<GroupGraphPattern>),
    /// `FILTER(expression)` — constraint on solutions.
    Filter(Expression),
    /// `BIND(expression AS ?var)` — introduce a new binding.
    Bind(Expression, Variable),
    /// `VALUES (?var1 ?var2) { (val1 val2) ... }` — inline data.
    Values {
        variables: Vec<Variable>,
        rows: Vec<ValuesRow>,
    },
    /// `{ SELECT ... }` — subquery.
    SubSelect(Box<SparqlQuery>),
    /// `GRAPH <iri> { pattern }` or `GRAPH ?var { pattern }`.
    Graph(VarOrIri, Box<GroupGraphPattern>),
    /// `SERVICE <iri> { pattern }` — federated query.
    Service {
        silent: bool,
        endpoint: VarOrIri,
        pattern: Box<GroupGraphPattern>,
    },
}

/// The projection in a SELECT query.
#[derive(Debug, Clone, PartialEq)]
pub enum SelectClause {
    /// `SELECT *` — all in-scope variables.
    Wildcard,
    /// `SELECT ?var1 (?expr AS ?var2) ...`
    Variables(Vec<SelectVariable>),
}

/// A single item in a SELECT projection.
#[derive(Debug, Clone, PartialEq)]
pub enum SelectVariable {
    /// `?var`
    Variable(Variable),
    /// `(expression AS ?var)`
    Expression(Expression, Variable),
}

/// Modifiers controlling result set (DISTINCT, REDUCED, or default).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectModifier {
    Default,
    Distinct,
    Reduced,
}

/// An ORDER BY clause item.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderCondition {
    pub expr: Expression,
    pub ascending: bool,
}

/// The query form: SELECT, CONSTRUCT, ASK, or DESCRIBE.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryForm {
    Select {
        modifier: SelectModifier,
        projection: SelectClause,
    },
    Construct {
        template: Vec<TriplePattern>,
    },
    Ask,
    Describe {
        resources: Vec<VarOrIri>,
    },
}

/// A dataset clause specifying which graphs to query.
#[derive(Debug, Clone, PartialEq)]
pub struct DatasetClause {
    /// The graph IRI.
    pub iri: Iri,
    /// Whether this is a FROM NAMED clause (vs plain FROM).
    pub named: bool,
}

/// A complete SPARQL query.
#[derive(Debug, Clone, PartialEq)]
pub struct SparqlQuery {
    /// BASE declaration, if any.
    pub base: Option<Iri>,
    /// PREFIX declarations.
    pub prefixes: Vec<PrefixDecl>,
    /// The query form (SELECT / CONSTRUCT / ASK / DESCRIBE).
    pub form: QueryForm,
    /// Dataset clauses (FROM / FROM NAMED).
    pub from: Vec<DatasetClause>,
    /// The WHERE clause (graph pattern).
    pub where_clause: GroupGraphPattern,
    /// GROUP BY expressions.
    pub group_by: Vec<Expression>,
    /// HAVING condition.
    pub having: Option<Expression>,
    /// ORDER BY conditions.
    pub order_by: Vec<OrderCondition>,
    /// LIMIT.
    pub limit: Option<u64>,
    /// OFFSET.
    pub offset: Option<u64>,
}
