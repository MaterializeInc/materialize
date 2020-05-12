// Copyright 2018 sqlparser-rs contributors. All rights reserved.
// Copyright Materialize, Inc. All rights reserved.
//
// This file is derived from the sqlparser-rs project, available at
// https://github.com/andygrove/sqlparser-rs. It was incorporated
// directly into Materialize on December 21, 2019.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! SQL Abstract Syntax Tree (AST) types

#[macro_use]
pub mod display;
mod data_type;
mod ddl;
mod operator;
mod query;
mod value;
#[macro_use]
mod visit_macro;

pub mod visit {
    // Visiting an enum struct variant with many arguments forces our hand here.
    // If, over time, we convert these large variants to dedicated structs, we
    // can remove this escape hatch.
    #![allow(clippy::too_many_arguments)]
    // Disable lints that want us to rewrite `&Ident` as `&str`, as `&str` is not as
    // self-documenting as &Ident.
    #![allow(clippy::ptr_arg)]
    make_visitor!(Visit: &);
}

pub mod visit_mut {
    // See justification for these attributes in the `visit` module.
    #![allow(clippy::too_many_arguments)]
    #![allow(clippy::ptr_arg)]
    make_visitor!(VisitMut: &mut);
}

use std::fmt;
use std::str::FromStr;

use repr::datetime::DateTimeField;

pub use self::data_type::DataType;
pub use self::ddl::{
    AlterTableOperation, ColumnDef, ColumnOption, ColumnOptionDef, TableConstraint,
};
use self::display::{AstDisplay, AstFormatter};
pub use self::operator::{BinaryOperator, UnaryOperator};
pub use self::query::{
    Cte, Fetch, Join, JoinConstraint, JoinOperator, OrderByExpr, Query, Select, SelectItem,
    SetExpr, SetOperator, TableAlias, TableFactor, TableWithJoins, Values,
};
pub use self::value::{ExtractField, IntervalValue, Value};
use std::path::PathBuf;

use crate::keywords::is_reserved_keyword;

struct DisplaySeparated<'a, T>
where
    T: AstDisplay,
{
    slice: &'a [T],
    sep: &'static str,
}

impl<'a, T> AstDisplay for DisplaySeparated<'a, T>
where
    T: AstDisplay,
{
    fn fmt(&self, f: &mut AstFormatter) {
        let mut delim = "";
        for t in self.slice {
            f.write_str(delim);
            delim = self.sep;
            t.fmt(f);
        }
    }
}

fn display_separated<'a, T>(slice: &'a [T], sep: &'static str) -> DisplaySeparated<'a, T>
where
    T: AstDisplay,
{
    DisplaySeparated { slice, sep }
}

fn display_comma_separated<T>(slice: &[T]) -> DisplaySeparated<'_, T>
where
    T: AstDisplay,
{
    DisplaySeparated { slice, sep: ", " }
}

/// An identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Ident(String);

impl Ident {
    /// Create a new identifier with the given value.
    pub fn new<S>(value: S) -> Self
    where
        S: Into<String>,
    {
        Ident(value.into())
    }

    /// Creates a new identifier with the normalized form of the given value. This should only be
    /// used on identifiers read from SQL that were not quoted.
    pub fn new_normalized<S>(value: S) -> Self
    where
        S: Into<String>,
    {
        Ident(value.into().to_lowercase())
    }

    /// An identifier can be printed in bare mode if
    ///  * it matches the regex [a-z_][a-z0-9_]* and
    ///  * it is not a "reserved keyword."
    pub fn can_be_printed_bare(&self) -> bool {
        let mut chars = self.0.chars();
        chars
            .next()
            .map(|ch| (ch >= 'a' && ch <= 'z') || (ch == '_'))
            .unwrap_or(false)
            && chars.all(|ch| (ch >= 'a' && ch <= 'z') || (ch == '_') || (ch >= '0' && ch <= '9'))
            && !is_reserved_keyword(&self.0)
    }

    pub fn value(&self) -> String {
        self.0.to_string()
    }

    pub fn as_str<'a>(&'a self) -> &'a str {
        &self.0
    }
}

impl From<&str> for Ident {
    fn from(value: &str) -> Self {
        Ident(value.to_string())
    }
}

/// More-or-less a direct translation of the Postgres function for doing the same thing:
///
///   https://github.com/postgres/postgres/blob/master/src/backend/utils/adt/ruleutils.c#L10730-L10812
///
/// Quotation is forced when printing in Stable mode.
impl AstDisplay for Ident {
    fn fmt(&self, f: &mut AstFormatter) {
        if self.can_be_printed_bare() && !f.stable() {
            f.write_str(&self.0);
        } else {
            f.write_str("\"");
            for ch in self.0.chars() {
                // Double up on double-quotes.
                if ch == '"' {
                    f.write_str("\"");
                }
                f.write_str(ch);
            }
            f.write_str("\"");
        }
    }
}
impl_display!(Ident);

/// A name of a table, view, custom type, etc., possibly multi-part, i.e. db.schema.obj
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectName(pub Vec<Ident>);

impl AstDisplay for ObjectName {
    fn fmt(&self, f: &mut AstFormatter) {
        display_separated(&self.0, ".").fmt(f);
    }
}
impl_display!(ObjectName);

impl AstDisplay for &ObjectName {
    fn fmt(&self, f: &mut AstFormatter) {
        display_separated(&self.0, ".").fmt(f);
    }
}

/// An SQL expression of any type.
///
/// The parser does not distinguish between expressions of different types
/// (e.g. boolean vs string), so the caller must handle expressions of
/// inappropriate type, like `WHERE 1` or `SELECT 1=1`, as necessary.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Expr {
    /// Identifier e.g. table name or column name
    Identifier(Vec<Ident>),
    /// Qualified wildcard, e.g. `alias.*` or `schema.table.*`.
    /// (Same caveats apply to `QualifiedWildcard` as to `Wildcard`.)
    QualifiedWildcard(Vec<Ident>),
    /// A positional parameter, e.g., `$1` or `$42`
    Parameter(usize),
    /// `IS NULL` expression
    IsNull(Box<Expr>),
    /// `IS NOT NULL` expression
    IsNotNull(Box<Expr>),
    /// `[ NOT ] IN (val1, val2, ...)`
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    /// `[ NOT ] IN (SELECT ...)`
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<Query>,
        negated: bool,
    },
    /// `<expr> [ NOT ] BETWEEN <low> AND <high>`
    Between {
        expr: Box<Expr>,
        negated: bool,
        low: Box<Expr>,
        high: Box<Expr>,
    },
    /// Binary operation e.g. `1 + 1` or `foo > bar`
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
    /// Unary operation e.g. `NOT foo`
    UnaryOp { op: UnaryOperator, expr: Box<Expr> },
    /// CAST an expression to a different data type e.g. `CAST(foo AS VARCHAR(123))`
    Cast {
        expr: Box<Expr>,
        data_type: DataType,
    },
    Extract {
        field: ExtractField,
        expr: Box<Expr>,
    },
    /// `expr COLLATE collation`
    Collate {
        expr: Box<Expr>,
        collation: ObjectName,
    },
    /// Nested expression e.g. `(foo > bar)` or `(1)`
    Nested(Box<Expr>),
    /// A literal value, such as string, number, date or NULL
    Value(Value),
    /// Scalar function call e.g. `LEFT(foo, 5)`
    Function(Function),
    /// `CASE [<operand>] WHEN <condition> THEN <result> ... [ELSE <result>] END`
    ///
    /// Note we only recognize a complete single expression as `<condition>`,
    /// not `< 0` nor `1, 2, 3` as allowed in a `<simple when clause>` per
    /// <https://jakewheat.github.io/sql-overview/sql-2011-foundation-grammar.html#simple-when-clause>
    Case {
        operand: Option<Box<Expr>>,
        conditions: Vec<Expr>,
        results: Vec<Expr>,
        else_result: Option<Box<Expr>>,
    },
    /// An exists expression `EXISTS(SELECT ...)`, used in expressions like
    /// `WHERE EXISTS (SELECT ...)`.
    Exists(Box<Query>),
    /// A parenthesized subquery `(SELECT ...)`, used in expression like
    /// `SELECT (subquery) AS x` or `WHERE (subquery) = x`
    Subquery(Box<Query>),
    /// `<expr> <op> ANY/SOME (<query>)`
    Any {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Query>,
        some: bool, // just tracks which syntax was used
    },
    /// `<expr> <op> ALL (<query>)`
    All {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Query>,
    },
    /// `LIST[<expr>*]`
    List(Vec<Expr>),
}

impl AstDisplay for Expr {
    fn fmt(&self, f: &mut AstFormatter) {
        match self {
            Expr::Identifier(s) => f.write_node(&display_separated(s, ".")),
            Expr::QualifiedWildcard(q) => {
                f.write_node(&display_separated(q, "."));
                f.write_str(".*");
            }
            Expr::Parameter(n) => f.write_str(&format!("${}", n)),
            Expr::IsNull(ast) => {
                f.write_node(&ast);
                f.write_str(" IS NULL");
            }
            Expr::IsNotNull(ast) => {
                f.write_node(&ast);
                f.write_str(" IS NOT NULL");
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                f.write_node(&expr);
                f.write_str(" ");
                if *negated {
                    f.write_str("NOT ");
                }
                f.write_str("IN (");
                f.write_node(&display_comma_separated(list));
                f.write_str(")");
            }
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                f.write_node(&expr);
                f.write_str(" ");
                if *negated {
                    f.write_str("NOT ");
                }
                f.write_str("IN (");
                f.write_node(&subquery);
                f.write_str(")");
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                f.write_node(&expr);
                if *negated {
                    f.write_str(" NOT");
                }
                f.write_str(" BETWEEN ");
                f.write_node(&low);
                f.write_str(" AND ");
                f.write_node(&high);
            }
            Expr::BinaryOp { left, op, right } => {
                f.write_node(&left);
                f.write_str(" ");
                f.write_str(op);
                f.write_str(" ");
                f.write_node(&right);
            }
            Expr::UnaryOp { op, expr } => {
                f.write_str(op);
                f.write_str(" ");
                f.write_node(&expr);
            }
            Expr::Cast { expr, data_type } => {
                f.write_str("CAST(");
                f.write_node(&expr);
                f.write_str(" AS ");
                f.write_node(data_type);
                f.write_str(")");
            }
            Expr::Extract { field, expr } => {
                f.write_str("EXTRACT(");
                f.write_node(field);
                f.write_str(" FROM ");
                f.write_node(&expr);
                f.write_str(")");
            }
            Expr::Collate { expr, collation } => {
                f.write_node(&expr);
                f.write_str(" COLLATE ");
                f.write_node(&collation);
            }
            Expr::Nested(ast) => {
                f.write_str("(");
                f.write_node(&ast);
                f.write_str(")");
            }
            Expr::Value(v) => {
                f.write_node(v);
            }
            Expr::Function(fun) => {
                f.write_node(fun);
            }
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                f.write_str("CASE");
                if let Some(operand) = operand {
                    f.write_str(" ");
                    f.write_node(&operand);
                }
                for (c, r) in conditions.iter().zip(results) {
                    f.write_str(" WHEN ");
                    f.write_node(c);
                    f.write_str(" THEN ");
                    f.write_node(r);
                }

                if let Some(else_result) = else_result {
                    f.write_str(" ELSE ");
                    f.write_node(&else_result);
                }
                f.write_str(" END")
            }
            Expr::Exists(s) => {
                f.write_str("EXISTS (");
                f.write_node(&s);
                f.write_str(")");
            }
            Expr::Subquery(s) => {
                f.write_str("(");
                f.write_node(&s);
                f.write_str(")");
            }
            Expr::Any {
                left,
                op,
                right,
                some,
            } => {
                f.write_node(&left);
                f.write_str(" ");
                f.write_str(op);
                if *some {
                    f.write_str(" SOME ");
                } else {
                    f.write_str(" ANY ");
                }
                f.write_str("(");
                f.write_node(&right);
                f.write_str(")");
            }
            Expr::All { left, op, right } => {
                f.write_node(&left);
                f.write_str(" ");
                f.write_str(op);
                f.write_str(" ALL (");
                f.write_node(&right);
                f.write_str(")");
            }
            Expr::List(exprs) => {
                let mut exprs = exprs.iter().peekable();
                f.write_str("LIST[");
                while let Some(expr) = exprs.next() {
                    f.write_node(expr);
                    if exprs.peek().is_some() {
                        f.write_str(", ");
                    }
                }
                f.write_str("]");
            }
        }
    }
}
impl_display!(Expr);

/// A window specification (i.e. `OVER (PARTITION BY .. ORDER BY .. etc.)`)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WindowSpec {
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<OrderByExpr>,
    pub window_frame: Option<WindowFrame>,
}

impl AstDisplay for WindowSpec {
    fn fmt(&self, f: &mut AstFormatter) {
        let mut delim = "";
        if !self.partition_by.is_empty() {
            delim = " ";
            f.write_str("PARTITION BY ");
            f.write_node(&display_comma_separated(&self.partition_by));
        }
        if !self.order_by.is_empty() {
            f.write_str(delim);
            delim = " ";
            f.write_str("ORDER BY ");
            f.write_node(&display_comma_separated(&self.order_by));
        }
        if let Some(window_frame) = &self.window_frame {
            if let Some(end_bound) = &window_frame.end_bound {
                f.write_str(delim);
                f.write_node(&window_frame.units);
                f.write_str(" BETWEEN ");
                f.write_node(&window_frame.start_bound);
                f.write_str(" AND ");
                f.write_node(&*end_bound);
            } else {
                f.write_str(delim);
                f.write_node(&window_frame.units);
                f.write_str(" ");
                f.write_node(&window_frame.start_bound);
            }
        }
    }
}
impl_display!(WindowSpec);

/// Specifies the data processed by a window function, e.g.
/// `RANGE UNBOUNDED PRECEDING` or `ROWS BETWEEN 5 PRECEDING AND CURRENT ROW`.
///
/// Note: The parser does not validate the specified bounds; the caller should
/// reject invalid bounds like `ROWS UNBOUNDED FOLLOWING` before execution.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WindowFrame {
    pub units: WindowFrameUnits,
    pub start_bound: WindowFrameBound,
    /// The right bound of the `BETWEEN .. AND` clause. The end bound of `None`
    /// indicates the shorthand form (e.g. `ROWS 1 PRECEDING`), which must
    /// behave the same as `end_bound = WindowFrameBound::CurrentRow`.
    pub end_bound: Option<WindowFrameBound>,
    // TBD: EXCLUDE
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WindowFrameUnits {
    Rows,
    Range,
    Groups,
}

impl AstDisplay for WindowFrameUnits {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str(match self {
            WindowFrameUnits::Rows => "ROWS",
            WindowFrameUnits::Range => "RANGE",
            WindowFrameUnits::Groups => "GROUPS",
        })
    }
}
impl_display!(WindowFrameUnits);

impl FromStr for WindowFrameUnits {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ROWS" => Ok(WindowFrameUnits::Rows),
            "RANGE" => Ok(WindowFrameUnits::Range),
            "GROUPS" => Ok(WindowFrameUnits::Groups),
            _ => Err(format!("Expected ROWS, RANGE, or GROUPS, found: {}", s)),
        }
    }
}

/// Specifies [WindowFrame]'s `start_bound` and `end_bound`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WindowFrameBound {
    /// `CURRENT ROW`
    CurrentRow,
    /// `<N> PRECEDING` or `UNBOUNDED PRECEDING`
    Preceding(Option<u64>),
    /// `<N> FOLLOWING` or `UNBOUNDED FOLLOWING`.
    Following(Option<u64>),
}

impl AstDisplay for WindowFrameBound {
    fn fmt(&self, f: &mut AstFormatter) {
        match self {
            WindowFrameBound::CurrentRow => f.write_str("CURRENT ROW"),
            WindowFrameBound::Preceding(None) => f.write_str("UNBOUNDED PRECEDING"),
            WindowFrameBound::Following(None) => f.write_str("UNBOUNDED FOLLOWING"),
            WindowFrameBound::Preceding(Some(n)) => {
                f.write_str(n);
                f.write_str(" PRECEDING");
            }
            WindowFrameBound::Following(Some(n)) => {
                f.write_str(n);
                f.write_str(" FOLLOWING");
            }
        }
    }
}
impl_display!(WindowFrameBound);

/// Specifies what [Statement::Explain] is actually explaining
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExplainStage {
    /// The original sql string
    Sql,
    /// The sql::RelationExpr after parsing
    RawPlan,
    /// The expr::RelationExpr after decorrelation
    DecorrelatedPlan,
    /// The expr::RelationExpr after optimization
    OptimizedPlan,
}

impl AstDisplay for ExplainStage {
    fn fmt(&self, f: &mut AstFormatter) {
        match self {
            ExplainStage::Sql => f.write_str("SQL"),
            ExplainStage::RawPlan => f.write_str("RAW PLAN"),
            ExplainStage::DecorrelatedPlan => f.write_str("DECORRELATED PLAN"),
            ExplainStage::OptimizedPlan => f.write_str("OPTIMIZED PLAN"),
        }
    }
}
impl_display!(ExplainStage);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Schema {
    File(PathBuf),
    Inline(String),
}

impl AstDisplay for Schema {
    fn fmt(&self, f: &mut AstFormatter) {
        match self {
            Self::File(path) => {
                f.write_str("SCHEMA FILE '");
                f.write_node(&value::escape_single_quote_string(
                    &path.display().to_string(),
                ));
                f.write_str("'");
            }
            Self::Inline(inner) => {
                f.write_str("SCHEMA '");
                f.write_node(&value::escape_single_quote_string(inner));
                f.write_str("'");
            }
        }
    }
}
impl_display!(Schema);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AvroSchema {
    CsrUrl { url: String, seed: Option<CsrSeed> },
    Schema(Schema),
}

impl AstDisplay for AvroSchema {
    fn fmt(&self, f: &mut AstFormatter) {
        match self {
            Self::CsrUrl { url, seed } => {
                f.write_str("CONFLUENT SCHEMA REGISTRY '");
                f.write_node(&value::escape_single_quote_string(url));
                f.write_str("'");
                if let Some(seed) = seed {
                    f.write_str(" ");
                    f.write_node(seed);
                }
            }
            Self::Schema(schema) => schema.fmt(f),
        }
    }
}
impl_display!(AvroSchema);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CsrSeed {
    pub key_schema: Option<String>,
    pub value_schema: String,
}

impl AstDisplay for CsrSeed {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("SEED");
        if let Some(key_schema) = &self.key_schema {
            f.write_str(" KEY SCHEMA '");
            f.write_node(&value::escape_single_quote_string(key_schema));
            f.write_str("'");
        }
        f.write_str(" VALUE SCHEMA '");
        f.write_node(&value::escape_single_quote_string(&self.value_schema));
        f.write_str("'");
    }
}
impl_display!(CsrSeed);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Format {
    Bytes,
    Avro(AvroSchema),
    Protobuf {
        message_name: String,
        schema: Schema,
    },
    Regex(String),
    Csv {
        header_row: bool,
        n_cols: Option<usize>,
        delimiter: char,
    },
    Json,
    Text,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Envelope {
    None,
    Debezium,
    Upsert(Option<Format>),
}

impl Default for Envelope {
    fn default() -> Self {
        Self::None
    }
}

impl AstDisplay for Envelope {
    fn fmt(&self, f: &mut AstFormatter) {
        match self {
            Self::None => {
                // this is unreachable as long as the default is None, but include it in case we ever change that
                f.write_str("NONE");
            }
            Self::Debezium => {
                f.write_str("DEBEZIUM");
            }
            Self::Upsert(format) => {
                f.write_str("UPSERT");
                if let Some(format) = format {
                    f.write_str(" FORMAT ");
                    f.write_node(format);
                }
            }
        }
    }
}
impl_display!(Envelope);

impl AstDisplay for Format {
    fn fmt(&self, f: &mut AstFormatter) {
        match self {
            Self::Bytes => f.write_str("BYTES"),
            Self::Avro(inner) => {
                f.write_str("AVRO USING ");
                f.write_node(inner);
            }
            Self::Protobuf {
                message_name,
                schema,
            } => {
                f.write_str("PROTOBUF MESSAGE '");
                f.write_node(&value::escape_single_quote_string(message_name));
                f.write_str("' USING ");
                f.write_str(schema);
            }
            Self::Regex(regex) => {
                f.write_str("REGEX '");
                f.write_node(&value::escape_single_quote_string(regex));
                f.write_str("'");
            }
            Self::Csv {
                header_row,
                n_cols,
                delimiter,
            } => {
                f.write_str("CSV WITH ");
                if *header_row {
                    f.write_str("HEADER");
                } else {
                    f.write_str(n_cols.unwrap());
                    f.write_str(" COLUMNS");
                }
                if *delimiter != ',' {
                    f.write_str(" DELIMITED BY '");
                    f.write_node(&value::escape_single_quote_string(&delimiter.to_string()));
                    f.write_str("'");
                }
            }
            Self::Json => f.write_str("JSON"),
            Self::Text => f.write_str("TEXT"),
        }
    }
}
impl_display!(Format);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Connector {
    File {
        path: String,
    },
    Kafka {
        broker: String,
        topic: String,
    },
    Kinesis {
        arn: String,
    },
    /// Avro Object Container File
    AvroOcf {
        path: String,
    },
}

impl AstDisplay for Connector {
    fn fmt(&self, f: &mut AstFormatter) {
        match self {
            Connector::File { path } => {
                f.write_str("FILE '");
                f.write_node(&value::escape_single_quote_string(path));
                f.write_str("'");
            }
            Connector::Kafka { broker, topic } => {
                f.write_str("KAFKA BROKER '");
                f.write_node(&value::escape_single_quote_string(broker));
                f.write_str("'");
                f.write_str(" TOPIC '");
                f.write_node(&value::escape_single_quote_string(topic));
                f.write_str("'");
            }
            Connector::Kinesis { arn } => {
                f.write_str("KINESIS ARN '");
                f.write_node(&value::escape_single_quote_string(arn));
                f.write_str("'");
            }
            Connector::AvroOcf { path } => {
                f.write_str("AVRO OCF '");
                f.write_node(&value::escape_single_quote_string(path));
                f.write_str("'");
            }
        }
    }
}
impl_display!(Connector);

/// A top-level statement (SELECT, INSERT, CREATE, etc.)
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Statement {
    /// `SELECT`
    Query(Box<Query>),
    /// `INSERT`
    Insert {
        /// TABLE
        table_name: ObjectName,
        /// COLUMNS
        columns: Vec<Ident>,
        /// A SQL query that specifies what to insert
        source: Box<Query>,
    },
    Copy {
        /// TABLE
        table_name: ObjectName,
        /// COLUMNS
        columns: Vec<Ident>,
        /// VALUES a vector of values to be copied
        values: Vec<Option<String>>,
    },
    /// `UPDATE`
    Update {
        /// TABLE
        table_name: ObjectName,
        /// Column assignments
        assignments: Vec<Assignment>,
        /// WHERE
        selection: Option<Expr>,
    },
    /// `DELETE`
    Delete {
        /// `FROM`
        table_name: ObjectName,
        /// `WHERE`
        selection: Option<Expr>,
    },
    /// `CREATE DATABASE`
    CreateDatabase {
        name: Ident,
        if_not_exists: bool,
    },
    /// `CREATE SCHEMA`
    CreateSchema {
        name: ObjectName,
        if_not_exists: bool,
    },
    /// `CREATE SOURCE`
    CreateSource {
        name: ObjectName,
        col_names: Vec<Ident>,
        connector: Connector,
        with_options: Vec<SqlOption>,
        format: Option<Format>,
        envelope: Envelope,
        if_not_exists: bool,
        materialized: bool,
    },
    /// `CREATE SINK`
    CreateSink {
        name: ObjectName,
        from: ObjectName,
        connector: Connector,
        with_options: Vec<SqlOption>,
        format: Option<Format>,
        if_not_exists: bool,
    },
    /// `CREATE VIEW`
    CreateView {
        /// View name
        name: ObjectName,
        columns: Vec<Ident>,
        query: Box<Query>,
        if_exists: IfExistsBehavior,
        temporary: bool,
        materialized: bool,
        with_options: Vec<SqlOption>,
    },
    /// `CREATE TABLE`
    CreateTable {
        /// Table name
        name: ObjectName,
        /// Optional schema
        columns: Vec<ColumnDef>,
        constraints: Vec<TableConstraint>,
        with_options: Vec<SqlOption>,
        if_not_exists: bool,
    },
    /// `CREATE INDEX`
    CreateIndex {
        /// Index name
        name: Ident,
        /// `ON` table or view name
        on_name: ObjectName,
        /// Expressions that form part of the index key
        key_parts: Vec<Expr>,
        if_not_exists: bool,
    },
    /// `ALTER TABLE`
    AlterTable {
        /// Table name
        name: ObjectName,
        operation: AlterTableOperation,
    },
    DropDatabase {
        name: Ident,
        if_exists: bool,
    },
    /// `DROP`
    DropObjects {
        /// The type of the object to drop: TABLE, VIEW, etc.
        object_type: ObjectType,
        /// An optional `IF EXISTS` clause. (Non-standard.)
        if_exists: bool,
        /// One or more objects to drop. (ANSI SQL requires exactly one.)
        names: Vec<ObjectName>,
        /// Whether `CASCADE` was specified. This will be `false` when
        /// `RESTRICT` or no drop behavior at all was specified.
        cascade: bool,
    },
    /// `SET <variable>`
    ///
    /// Note: this is not a standard SQL statement, but it is supported by at
    /// least MySQL and PostgreSQL. Not all MySQL-specific syntatic forms are
    /// supported yet.
    SetVariable {
        local: bool,
        variable: Ident,
        value: SetVariableValue,
    },
    /// `SHOW <variable>`
    ///
    /// Note: this is a PostgreSQL-specific statement.
    ShowVariable {
        variable: Ident,
    },
    /// `SHOW DATABASES`
    ShowDatabases {
        filter: Option<ShowStatementFilter>,
    },
    /// `SHOW <object>S`
    ///
    /// ```sql
    /// SHOW TABLES;
    /// SHOW SOURCES;
    /// SHOW VIEWS;
    /// SHOW SINKS;
    /// ```
    ShowObjects {
        object_type: ObjectType,
        from: Option<ObjectName>,
        extended: bool,
        full: bool,
        materialized: bool,
        filter: Option<ShowStatementFilter>,
    },
    /// `SHOW INDEX|INDEXES|KEYS`
    ///
    /// Note: this is a MySQL-specific statement
    ShowIndexes {
        table_name: ObjectName,
        extended: bool,
        filter: Option<ShowStatementFilter>,
    },
    /// `SHOW COLUMNS`
    ///
    /// Note: this is a MySQL-specific statement.
    ShowColumns {
        extended: bool,
        full: bool,
        table_name: ObjectName,
        filter: Option<ShowStatementFilter>,
    },
    /// `SHOW CREATE VIEW <view>`
    ShowCreateView {
        view_name: ObjectName,
    },
    /// `SHOW CREATE SOURCE <source>`
    ShowCreateSource {
        source_name: ObjectName,
    },
    /// `SHOW CREATE SINK <sink>`
    ShowCreateSink {
        sink_name: ObjectName,
    },
    /// `{ BEGIN [ TRANSACTION | WORK ] | START TRANSACTION } ...`
    StartTransaction {
        modes: Vec<TransactionMode>,
    },
    /// `SET TRANSACTION ...`
    SetTransaction {
        modes: Vec<TransactionMode>,
    },
    /// `COMMIT [ TRANSACTION | WORK ] [ AND [ NO ] CHAIN ]`
    Commit {
        chain: bool,
    },
    /// `ROLLBACK [ TRANSACTION | WORK ] [ AND [ NO ] CHAIN ]`
    Rollback {
        chain: bool,
    },
    /// `TAIL`
    Tail {
        name: ObjectName,
        with_snapshot: bool,
        as_of: Option<Expr>,
    },
    /// `EXPLAIN [ DATAFLOW | PLAN ] FOR`
    Explain {
        stage: ExplainStage,
        explainee: Explainee,
        options: ExplainOptions,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Explainee {
    View(ObjectName),
    Query(Query),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExplainOptions {
    pub typed: bool,
}

impl AstDisplay for Statement {
    fn fmt(&self, f: &mut AstFormatter) {
        match self {
            Statement::Query(s) => f.write_node(&s),
            Statement::Insert {
                table_name,
                columns,
                source,
            } => {
                f.write_str("INSERT INTO ");
                f.write_node(&table_name);
                if !columns.is_empty() {
                    f.write_str(" (");
                    f.write_node(&display_comma_separated(columns));
                    f.write_str(")");
                }
                f.write_str(" ");
                f.write_node(&source);
            }
            Statement::Copy {
                table_name,
                columns,
                values,
            } => {
                f.write_str("COPY ");
                f.write_node(&table_name);
                if !columns.is_empty() {
                    f.write_str("(");
                    f.write_node(&display_comma_separated(columns));
                    f.write_str(")");
                }
                f.write_str(" FROM stdin; ");
                if !values.is_empty() {
                    f.write_str("\n");
                    let mut delim = "";
                    for v in values {
                        f.write_str(delim);
                        delim = "\t";
                        if let Some(v) = v {
                            f.write_str(v);
                        } else {
                            f.write_str("\\N");
                        }
                    }
                }
                f.write_str("\n\\.");
            }
            Statement::Update {
                table_name,
                assignments,
                selection,
            } => {
                f.write_str("UPDATE ");
                f.write_node(&table_name);
                if !assignments.is_empty() {
                    f.write_str(" SET ");
                    f.write_node(&display_comma_separated(assignments));
                }
                if let Some(selection) = selection {
                    f.write_str(" WHERE ");
                    f.write_node(selection);
                }
            }
            Statement::Delete {
                table_name,
                selection,
            } => {
                f.write_str("DELETE FROM ");
                f.write_node(&table_name);
                if let Some(selection) = selection {
                    f.write_str(" WHERE ");
                    f.write_node(selection);
                }
            }
            Statement::CreateDatabase {
                name,
                if_not_exists,
            } => {
                f.write_str("CREATE DATABASE ");
                if *if_not_exists {
                    f.write_str("IF NOT EXISTS ");
                }
                f.write_node(name);
            }
            Statement::CreateSchema {
                name,
                if_not_exists,
            } => {
                f.write_str("CREATE SCHEMA ");
                if *if_not_exists {
                    f.write_str("IF NOT EXISTS ");
                }
                f.write_node(&name);
            }
            Statement::CreateSource {
                name,
                col_names,
                connector,
                with_options,
                format,
                envelope,
                if_not_exists,
                materialized,
            } => {
                f.write_str("CREATE ");
                if *materialized {
                    f.write_str("MATERIALIZED ");
                }
                f.write_str("SOURCE ");
                if *if_not_exists {
                    f.write_str("IF NOT EXISTS ");
                }
                f.write_node(&name);
                f.write_str(" ");
                if !col_names.is_empty() {
                    f.write_str("(");
                    f.write_node(&display_comma_separated(col_names));
                    f.write_str(") ");
                }
                f.write_str("FROM ");
                f.write_node(connector);
                if !with_options.is_empty() {
                    f.write_str(" WITH (");
                    f.write_node(&display_comma_separated(with_options));
                    f.write_str(")");
                }
                if let Some(format) = format {
                    f.write_str(" FORMAT ");
                    f.write_node(format);
                }
                if *envelope != Default::default() {
                    f.write_str(" ENVELOPE ");
                    f.write_node(envelope);
                }
            }
            Statement::CreateSink {
                name,
                from,
                connector,
                with_options,
                format,
                if_not_exists,
            } => {
                f.write_str("CREATE SINK ");
                if *if_not_exists {
                    f.write_str("IF NOT EXISTS ");
                }
                f.write_node(&name);
                f.write_str(" FROM ");
                f.write_node(&from);
                f.write_str(" INTO ");
                f.write_node(connector);
                if !with_options.is_empty() {
                    f.write_str(" WITH (");
                    f.write_node(&display_comma_separated(with_options));
                    f.write_str(")");
                }
                if let Some(format) = format {
                    f.write_str(" FORMAT ");
                    f.write_node(format);
                }
            }
            Statement::CreateView {
                name,
                columns,
                query,
                temporary,
                materialized,
                if_exists,
                with_options,
            } => {
                f.write_str("CREATE");
                if *if_exists == IfExistsBehavior::Replace {
                    f.write_str(" OR REPLACE");
                }
                if *temporary {
                    f.write_str(" TEMPORARY");
                }
                if *materialized {
                    f.write_str(" MATERIALIZED");
                }

                f.write_str(" VIEW");

                if *if_exists == IfExistsBehavior::Skip {
                    f.write_str(" IF NOT EXISTS");
                }

                f.write_str(" ");
                f.write_node(&name);

                if !with_options.is_empty() {
                    f.write_str(" WITH (");
                    f.write_node(&display_comma_separated(with_options));
                    f.write_str(")");
                }

                if !columns.is_empty() {
                    f.write_str(" (");
                    f.write_node(&display_comma_separated(columns));
                    f.write_str(")");
                }

                f.write_str(" AS ");
                f.write_node(&query);
            }
            Statement::CreateTable {
                name,
                columns,
                constraints,
                with_options,
                if_not_exists,
            } => {
                f.write_str("CREATE TABLE ");
                if *if_not_exists {
                    f.write_str("IF NOT EXISTS ");
                }
                f.write_node(&name);
                f.write_str(" (");
                f.write_node(&display_comma_separated(columns));
                if !constraints.is_empty() {
                    f.write_str(", ");
                    f.write_node(&display_comma_separated(constraints));
                }
                f.write_str(")");

                if !with_options.is_empty() {
                    f.write_str(" WITH (");
                    f.write_node(&display_comma_separated(with_options));
                    f.write_str(")");
                }
            }
            Statement::CreateIndex {
                name,
                on_name,
                key_parts,
                if_not_exists,
            } => {
                f.write_str("CREATE INDEX ");
                if *if_not_exists {
                    f.write_str("IF NOT EXISTS ");
                }
                f.write_node(name);
                f.write_str(" ON ");
                f.write_node(&on_name);
                f.write_str(" (");
                f.write_node(&display_comma_separated(key_parts));
                f.write_str(")");
            }
            Statement::AlterTable { name, operation } => {
                f.write_str("ALTER TABLE ");
                f.write_node(&name);
                f.write_str(" ");
                f.write_node(operation);
            }
            Statement::DropDatabase { name, if_exists } => {
                f.write_str("DROP DATABASE ");
                if *if_exists {
                    f.write_str("IF EXISTS ");
                }
                f.write_node(name);
            }
            Statement::DropObjects {
                object_type,
                if_exists,
                names,
                cascade,
            } => {
                f.write_str("DROP ");
                f.write_node(object_type);
                f.write_str(" ");
                if *if_exists {
                    f.write_str("IF EXISTS ");
                }
                f.write_node(&display_comma_separated(names));
                if *cascade {
                    f.write_str(" CASCADE");
                }
            }
            Statement::SetVariable {
                local,
                variable,
                value,
            } => {
                f.write_str("SET ");
                if *local {
                    f.write_str("LOCAL ");
                }
                f.write_node(variable);
                f.write_str(" = ");
                f.write_node(value);
            }
            Statement::ShowVariable { variable } => {
                f.write_str("SHOW ");
                f.write_node(variable);
            }
            Statement::ShowDatabases { filter } => {
                f.write_str("SHOW DATABASES");
                if let Some(filter) = filter {
                    f.write_str(" ");
                    f.write_node(filter);
                }
            }
            Statement::ShowObjects {
                object_type,
                filter,
                full,
                materialized,
                from,
                extended,
            } => {
                f.write_str("SHOW");
                if *extended {
                    f.write_str(" EXTENDED");
                }
                if *full {
                    f.write_str(" FULL");
                }
                if *materialized {
                    f.write_str(" MATERIALIZED");
                }
                f.write_str(" ");
                f.write_str(match object_type {
                    ObjectType::Schema => "SCHEMAS",
                    ObjectType::Table => "TABLES",
                    ObjectType::View => "VIEWS",
                    ObjectType::Source => "SOURCES",
                    ObjectType::Sink => "SINKS",
                    ObjectType::Index => unreachable!(),
                });
                if let Some(from) = from {
                    f.write_str(" FROM ");
                    f.write_node(&from);
                }
                if let Some(filter) = filter {
                    f.write_str(" ");
                    f.write_node(filter);
                }
            }
            Statement::ShowIndexes {
                table_name,
                extended,
                filter,
            } => {
                f.write_str("SHOW ");
                if *extended {
                    f.write_str("EXTENDED ");
                }
                f.write_str("INDEXES FROM ");
                f.write_node(&table_name);
                if let Some(filter) = filter {
                    f.write_str(" ");
                    f.write_node(filter);
                }
            }
            Statement::ShowColumns {
                extended,
                full,
                table_name,
                filter,
            } => {
                f.write_str("SHOW ");
                if *extended {
                    f.write_str("EXTENDED ");
                }
                if *full {
                    f.write_str("FULL ");
                }
                f.write_str("COLUMNS FROM ");
                f.write_node(&table_name);
                if let Some(filter) = filter {
                    f.write_str(" ");
                    f.write_node(filter);
                }
            }
            Statement::ShowCreateView { view_name } => {
                f.write_str("SHOW CREATE VIEW ");
                f.write_node(&view_name);
            }
            Statement::ShowCreateSource { source_name } => {
                f.write_str("SHOW CREATE SOURCE ");
                f.write_node(&source_name);
            }
            Statement::ShowCreateSink { sink_name } => {
                f.write_str("SHOW CREATE SINK ");
                f.write_node(&sink_name);
            }
            Statement::StartTransaction { modes } => {
                f.write_str("START TRANSACTION");
                if !modes.is_empty() {
                    f.write_str(" ");
                    f.write_node(&display_comma_separated(modes));
                }
            }
            Statement::SetTransaction { modes } => {
                f.write_str("SET TRANSACTION");
                if !modes.is_empty() {
                    f.write_str(" ");
                    f.write_node(&display_comma_separated(modes));
                }
            }
            Statement::Commit { chain } => {
                f.write_str("COMMIT");
                if *chain {
                    f.write_str(" AND CHAIN");
                }
            }
            Statement::Rollback { chain } => {
                f.write_str("ROLLBACK");
                if *chain {
                    f.write_str(" AND CHAIN");
                }
            }
            Statement::Tail {
                name,
                with_snapshot,
                as_of,
            } => {
                f.write_str("TAIL ");
                f.write_node(&name);
                if *with_snapshot {
                    f.write_str(" WITH SNAPSHOT");
                }
                if let Some(as_of) = as_of {
                    f.write_str(" AS OF ");
                    f.write_node(as_of);
                }
            }
            Statement::Explain {
                stage,
                explainee,
                options,
            } => {
                f.write_str("EXPLAIN ");
                if options.typed {
                    f.write_str("TYPED ");
                }
                f.write_node(stage);
                f.write_str(" FOR ");
                f.write_node(explainee);
            }
        }
    }
}
impl_display!(Statement);

impl AstDisplay for Explainee {
    fn fmt(&self, f: &mut AstFormatter) {
        match self {
            Explainee::View(name) => {
                f.write_str("VIEW ");
                f.write_node(&name);
            }
            Explainee::Query(query) => f.write_node(query),
        }
    }
}
impl_display!(Explainee);

/// SQL assignment `foo = expr` as used in SQLUpdate
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Assignment {
    pub id: Ident,
    pub value: Expr,
}

impl AstDisplay for Assignment {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_node(&self.id);
        f.write_str(" = ");
        f.write_node(&self.value);
    }
}
impl_display!(Assignment);

/// A function call
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Function {
    pub name: ObjectName,
    pub args: FunctionArgs,
    // aggregate functions may specify e.g. `COUNT(DISTINCT X) FILTER (WHERE ...)`
    pub filter: Option<Box<Expr>>,
    pub over: Option<WindowSpec>,
    // aggregate functions may specify eg `COUNT(DISTINCT x)`
    pub distinct: bool,
}

impl AstDisplay for Function {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_node(&self.name);
        f.write_str("(");
        if self.distinct {
            f.write_str("DISTINCT ")
        }
        f.write_node(&self.args);
        f.write_str(")");
        if let Some(filter) = &self.filter {
            f.write_str(" FILTER (WHERE ");
            f.write_node(&filter);
            f.write_str(")");
        }
        if let Some(o) = &self.over {
            f.write_str(" OVER (");
            f.write_node(o);
            f.write_str(")");
        }
    }
}
impl_display!(Function);

/// Arguments for a function call.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FunctionArgs {
    /// The special star argument, as in `count(*)`.
    Star,
    /// A normal list of arguments.
    Args(Vec<Expr>),
}

impl AstDisplay for FunctionArgs {
    fn fmt(&self, f: &mut AstFormatter) {
        match self {
            FunctionArgs::Star => f.write_str("*"),
            FunctionArgs::Args(args) => f.write_node(&display_comma_separated(&args)),
        }
    }
}
impl_display!(FunctionArgs);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub enum ObjectType {
    Schema,
    Table,
    View,
    Source,
    Sink,
    Index,
}

impl AstDisplay for ObjectType {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str(match self {
            ObjectType::Schema => "SCHEMA",
            ObjectType::Table => "TABLE",
            ObjectType::View => "VIEW",
            ObjectType::Source => "SOURCE",
            ObjectType::Sink => "SINK",
            ObjectType::Index => "INDEX",
        })
    }
}
impl_display!(ObjectType);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SqlOption {
    pub name: Ident,
    pub value: Value,
}

impl AstDisplay for SqlOption {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_node(&self.name);
        f.write_str(" = ");
        f.write_node(&self.value);
    }
}
impl_display!(SqlOption);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TransactionMode {
    AccessMode(TransactionAccessMode),
    IsolationLevel(TransactionIsolationLevel),
}

impl AstDisplay for TransactionMode {
    fn fmt(&self, f: &mut AstFormatter) {
        use TransactionMode::*;
        match self {
            AccessMode(access_mode) => f.write_node(access_mode),
            IsolationLevel(iso_level) => {
                f.write_str("ISOLATION LEVEL ");
                f.write_node(iso_level);
            }
        }
    }
}
impl_display!(TransactionMode);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TransactionAccessMode {
    ReadOnly,
    ReadWrite,
}

impl AstDisplay for TransactionAccessMode {
    fn fmt(&self, f: &mut AstFormatter) {
        use TransactionAccessMode::*;
        f.write_str(match self {
            ReadOnly => "READ ONLY",
            ReadWrite => "READ WRITE",
        })
    }
}
impl_display!(TransactionAccessMode);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TransactionIsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

impl AstDisplay for TransactionIsolationLevel {
    fn fmt(&self, f: &mut AstFormatter) {
        use TransactionIsolationLevel::*;
        f.write_str(match self {
            ReadUncommitted => "READ UNCOMMITTED",
            ReadCommitted => "READ COMMITTED",
            RepeatableRead => "REPEATABLE READ",
            Serializable => "SERIALIZABLE",
        })
    }
}
impl_display!(TransactionIsolationLevel);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ShowStatementFilter {
    Like(String),
    Where(Expr),
}

impl AstDisplay for ShowStatementFilter {
    fn fmt(&self, f: &mut AstFormatter) {
        use ShowStatementFilter::*;
        match self {
            Like(pattern) => {
                f.write_str("LIKE '");
                f.write_node(&value::escape_single_quote_string(pattern));
                f.write_str("'");
            }
            Where(expr) => {
                f.write_str("WHERE ");
                f.write_node(expr);
            }
        }
    }
}
impl_display!(ShowStatementFilter);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SetVariableValue {
    Ident(Ident),
    Literal(Value),
}

impl AstDisplay for SetVariableValue {
    fn fmt(&self, f: &mut AstFormatter) {
        use SetVariableValue::*;
        match self {
            Ident(ident) => f.write_node(ident),
            Literal(literal) => f.write_node(literal),
        }
    }
}
impl_display!(SetVariableValue);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IfExistsBehavior {
    Error,
    Skip,
    Replace,
}
