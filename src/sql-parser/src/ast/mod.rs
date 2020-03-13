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
pub use self::operator::{BinaryOperator, UnaryOperator};
pub use self::query::{
    Cte, Fetch, Join, JoinConstraint, JoinOperator, OrderByExpr, Query, Select, SelectItem,
    SetExpr, SetOperator, TableAlias, TableFactor, TableWithJoins, Values,
};
pub use self::value::{ExtractField, IntervalValue, Value};
use std::path::PathBuf;

struct DisplaySeparated<'a, T>
where
    T: fmt::Display,
{
    slice: &'a [T],
    sep: &'static str,
}

impl<'a, T> fmt::Display for DisplaySeparated<'a, T>
where
    T: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut delim = "";
        for t in self.slice {
            write!(f, "{}", delim)?;
            delim = self.sep;
            write!(f, "{}", t)?;
        }
        Ok(())
    }
}

fn display_separated<'a, T>(slice: &'a [T], sep: &'static str) -> DisplaySeparated<'a, T>
where
    T: fmt::Display,
{
    DisplaySeparated { slice, sep }
}

fn display_comma_separated<T>(slice: &[T]) -> DisplaySeparated<'_, T>
where
    T: fmt::Display,
{
    DisplaySeparated { slice, sep: ", " }
}

/// An identifier, decomposed into its value or character data and the quote style.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Ident {
    /// The value of the identifier without quotes.
    pub value: String,
    /// The starting quote if any. Valid quote characters are the single quote,
    /// double quote, backtick, and opening square bracket.
    pub quote_style: Option<char>,
}

impl Ident {
    /// Create a new identifier with the given value and no quotes.
    pub fn new<S>(value: S) -> Self
    where
        S: Into<String>,
    {
        Ident {
            value: value.into(),
            quote_style: None,
        }
    }

    /// Create a new quoted identifier with the given quote and value. This function
    /// panics if the given quote is not a valid quote character.
    pub fn with_quote<S>(quote: char, value: S) -> Self
    where
        S: Into<String>,
    {
        assert!(quote == '\'' || quote == '"' || quote == '`' || quote == '[');
        Ident {
            value: value.into(),
            quote_style: Some(quote),
        }
    }
}

impl From<&str> for Ident {
    fn from(value: &str) -> Self {
        Ident {
            value: value.to_string(),
            quote_style: None,
        }
    }
}

impl fmt::Display for Ident {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.quote_style {
            Some(q) if q == '"' || q == '\'' || q == '`' => write!(f, "{}{}{}", q, self.value, q),
            Some(q) if q == '[' => write!(f, "[{}]", self.value),
            None => f.write_str(&self.value),
            _ => panic!("unexpected quote style"),
        }
    }
}

/// A name of a table, view, custom type, etc., possibly multi-part, i.e. db.schema.obj
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectName(pub Vec<Ident>);

impl fmt::Display for ObjectName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", display_separated(&self.0, "."))
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
    Identifier(Ident),
    /// Unqualified wildcard (`*`). SQL allows this in limited contexts, such as:
    /// - right after `SELECT` (which is represented as a [SelectItem::Wildcard] instead)
    /// - or as part of an aggregate function, e.g. `COUNT(*)`,
    ///
    /// ...but we currently also accept it in contexts where it doesn't make
    /// sense, such as `* + *`
    Wildcard,
    /// Qualified wildcard, e.g. `alias.*` or `schema.table.*`.
    /// (Same caveats apply to `QualifiedWildcard` as to `Wildcard`.)
    QualifiedWildcard(Vec<Ident>),
    /// Multi-part identifier, e.g. `table_alias.column` or `schema.table.col`
    CompoundIdentifier(Vec<Ident>),
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
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Expr::Identifier(s) => write!(f, "{}", s),
            Expr::Wildcard => f.write_str("*"),
            Expr::QualifiedWildcard(q) => write!(f, "{}.*", display_separated(q, ".")),
            Expr::CompoundIdentifier(s) => write!(f, "{}", display_separated(s, ".")),
            Expr::Parameter(n) => write!(f, "${}", n),
            Expr::IsNull(ast) => write!(f, "{} IS NULL", ast),
            Expr::IsNotNull(ast) => write!(f, "{} IS NOT NULL", ast),
            Expr::InList {
                expr,
                list,
                negated,
            } => write!(
                f,
                "{} {}IN ({})",
                expr,
                if *negated { "NOT " } else { "" },
                display_comma_separated(list)
            ),
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => write!(
                f,
                "{} {}IN ({})",
                expr,
                if *negated { "NOT " } else { "" },
                subquery
            ),
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => write!(
                f,
                "{} {}BETWEEN {} AND {}",
                expr,
                if *negated { "NOT " } else { "" },
                low,
                high
            ),
            Expr::BinaryOp { left, op, right } => write!(f, "{} {} {}", left, op, right),
            Expr::UnaryOp { op, expr } => write!(f, "{} {}", op, expr),
            Expr::Cast { expr, data_type } => write!(f, "CAST({} AS {})", expr, data_type),
            Expr::Extract { field, expr } => write!(f, "EXTRACT({} FROM {})", field, expr),
            Expr::Collate { expr, collation } => write!(f, "{} COLLATE {}", expr, collation),
            Expr::Nested(ast) => write!(f, "({})", ast),
            Expr::Value(v) => write!(f, "{}", v),
            Expr::Function(fun) => write!(f, "{}", fun),
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                f.write_str("CASE")?;
                if let Some(operand) = operand {
                    write!(f, " {}", operand)?;
                }
                for (c, r) in conditions.iter().zip(results) {
                    write!(f, " WHEN {} THEN {}", c, r)?;
                }

                if let Some(else_result) = else_result {
                    write!(f, " ELSE {}", else_result)?;
                }
                f.write_str(" END")
            }
            Expr::Exists(s) => write!(f, "EXISTS ({})", s),
            Expr::Subquery(s) => write!(f, "({})", s),
            Expr::Any {
                left,
                op,
                right,
                some,
            } => write!(
                f,
                "{} {} {} ({})",
                left,
                op,
                if *some { "SOME" } else { "ANY" },
                right
            ),
            Expr::All { left, op, right } => write!(f, "{} {} ALL ({})", left, op, right),
        }
    }
}

/// A window specification (i.e. `OVER (PARTITION BY .. ORDER BY .. etc.)`)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WindowSpec {
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<OrderByExpr>,
    pub window_frame: Option<WindowFrame>,
}

impl fmt::Display for WindowSpec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut delim = "";
        if !self.partition_by.is_empty() {
            delim = " ";
            write!(
                f,
                "PARTITION BY {}",
                display_comma_separated(&self.partition_by)
            )?;
        }
        if !self.order_by.is_empty() {
            f.write_str(delim)?;
            delim = " ";
            write!(f, "ORDER BY {}", display_comma_separated(&self.order_by))?;
        }
        if let Some(window_frame) = &self.window_frame {
            if let Some(end_bound) = &window_frame.end_bound {
                f.write_str(delim)?;
                write!(
                    f,
                    "{} BETWEEN {} AND {}",
                    window_frame.units, window_frame.start_bound, end_bound
                )?;
            } else {
                f.write_str(delim)?;
                write!(f, "{} {}", window_frame.units, window_frame.start_bound)?;
            }
        }
        Ok(())
    }
}

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

impl fmt::Display for WindowFrameUnits {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            WindowFrameUnits::Rows => "ROWS",
            WindowFrameUnits::Range => "RANGE",
            WindowFrameUnits::Groups => "GROUPS",
        })
    }
}

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

impl fmt::Display for WindowFrameBound {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            WindowFrameBound::CurrentRow => f.write_str("CURRENT ROW"),
            WindowFrameBound::Preceding(None) => f.write_str("UNBOUNDED PRECEDING"),
            WindowFrameBound::Following(None) => f.write_str("UNBOUNDED FOLLOWING"),
            WindowFrameBound::Preceding(Some(n)) => write!(f, "{} PRECEDING", n),
            WindowFrameBound::Following(Some(n)) => write!(f, "{} FOLLOWING", n),
        }
    }
}

/// Specifies what [Statement::Explain] is actually explaining
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Stage {
    /// The dataflow graph after translation from SQL.
    Dataflow,
    /// The dataflow graph after optimization in the coordinator.
    Plan,
    // FIXME: Add introspection into dataflow execution.
}

impl fmt::Display for Stage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Stage::Dataflow => f.write_str("DATAFLOW"),
            Stage::Plan => f.write_str("PLAN"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Schema {
    File(PathBuf),
    Inline(String),
}

impl fmt::Display for Schema {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::File(path) => write!(
                f,
                "SCHEMA FILE '{}'",
                value::escape_single_quote_string(&path.display().to_string())
            ),
            Self::Inline(inner) => {
                write!(f, "SCHEMA '{}'", value::escape_single_quote_string(inner))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AvroSchema {
    CsrUrl { url: String, seed: Option<CsrSeed> },
    Schema(Schema),
}

impl fmt::Display for AvroSchema {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::CsrUrl { url, seed } => {
                write!(
                    f,
                    "CONFLUENT SCHEMA REGISTRY '{}'",
                    value::escape_single_quote_string(url)
                )?;
                if let Some(seed) = seed {
                    write!(f, " {}", seed)?;
                }
                Ok(())
            }
            Self::Schema(schema) => schema.fmt(f),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CsrSeed {
    pub key_schema: Option<String>,
    pub value_schema: String,
}

impl fmt::Display for CsrSeed {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("SEED")?;
        if let Some(key_schema) = &self.key_schema {
            write!(
                f,
                " KEY SCHEMA '{}'",
                value::escape_single_quote_string(key_schema)
            )?;
        }
        write!(
            f,
            " VALUE SCHEMA '{}'",
            value::escape_single_quote_string(&self.value_schema)
        )
    }
}

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
        n_cols: usize,
        delimiter: char,
    },
    Json,
    Text,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum Envelope {
    None,
    Debezium,
}

impl Default for Envelope {
    fn default() -> Self {
        Self::None
    }
}

impl fmt::Display for Envelope {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::None => "NONE", // this is unreachable as long as the default is None, but include it in case we ever change that
                Self::Debezium => "DEBEZIUM",
            }
        )
    }
}

impl fmt::Display for Format {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Bytes => write!(f, "BYTES"),
            Self::Avro(inner) => write!(f, "AVRO USING {}", inner),
            Self::Protobuf {
                message_name,
                schema,
            } => write!(
                f,
                "PROTOBUF MESSAGE '{}' USING {}",
                value::escape_single_quote_string(message_name),
                schema
            ),
            Self::Regex(regex) => write!(f, "REGEX '{}'", value::escape_single_quote_string(regex)),
            Self::Csv { n_cols, delimiter } => write!(
                f,
                "CSV WITH {} COLUMNS{}",
                n_cols,
                if *delimiter == ',' {
                    "".to_owned()
                } else {
                    format!(
                        " DELIMITED BY '{}'",
                        value::escape_single_quote_string(&delimiter.to_string())
                    )
                }
            ),
            Self::Json => write!(f, "JSON"),
            Self::Text => write!(f, "TEXT"),
        }
    }
}

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

impl fmt::Display for Connector {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Connector::File { path } => {
                write!(f, "FILE '{}'", value::escape_single_quote_string(path))?;
                Ok(())
            }
            Connector::Kafka { broker, topic } => {
                write!(
                    f,
                    "KAFKA BROKER '{}' TOPIC '{}'",
                    value::escape_single_quote_string(broker),
                    value::escape_single_quote_string(topic),
                )?;
                Ok(())
            }
            Connector::Kinesis { arn } => {
                write!(
                    f,
                    "KINESIS ARN '{}'",
                    value::escape_single_quote_string(arn),
                )?;
                Ok(())
            }
            Connector::AvroOcf { path } => {
                write!(f, "AVRO OCF '{}'", value::escape_single_quote_string(path))?;
                Ok(())
            }
        }
    }
}

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
        format: Format,
        if_not_exists: bool,
    },
    /// `CREATE VIEW`
    CreateView {
        /// View name
        name: ObjectName,
        columns: Vec<Ident>,
        query: Box<Query>,
        if_exists: IfExistsBehavior,
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
    },
    /// `EXPLAIN [ DATAFLOW | PLAN ] FOR`
    Explain {
        stage: Stage,
        query: Box<Query>,
    },
}

impl fmt::Display for Statement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Statement::Query(s) => write!(f, "{}", s),
            Statement::Insert {
                table_name,
                columns,
                source,
            } => {
                write!(f, "INSERT INTO {} ", table_name)?;
                if !columns.is_empty() {
                    write!(f, "({}) ", display_comma_separated(columns))?;
                }
                write!(f, "{}", source)
            }
            Statement::Copy {
                table_name,
                columns,
                values,
            } => {
                write!(f, "COPY {}", table_name)?;
                if !columns.is_empty() {
                    write!(f, " ({})", display_comma_separated(columns))?;
                }
                write!(f, " FROM stdin; ")?;
                if !values.is_empty() {
                    writeln!(f)?;
                    let mut delim = "";
                    for v in values {
                        write!(f, "{}", delim)?;
                        delim = "\t";
                        if let Some(v) = v {
                            write!(f, "{}", v)?;
                        } else {
                            write!(f, "\\N")?;
                        }
                    }
                }
                write!(f, "\n\\.")
            }
            Statement::Update {
                table_name,
                assignments,
                selection,
            } => {
                write!(f, "UPDATE {}", table_name)?;
                if !assignments.is_empty() {
                    write!(f, " SET ")?;
                    write!(f, "{}", display_comma_separated(assignments))?;
                }
                if let Some(selection) = selection {
                    write!(f, " WHERE {}", selection)?;
                }
                Ok(())
            }
            Statement::Delete {
                table_name,
                selection,
            } => {
                write!(f, "DELETE FROM {}", table_name)?;
                if let Some(selection) = selection {
                    write!(f, " WHERE {}", selection)?;
                }
                Ok(())
            }
            Statement::CreateDatabase {
                name,
                if_not_exists,
            } => {
                write!(f, "CREATE DATABASE ")?;
                if *if_not_exists {
                    write!(f, "IF NOT EXISTS ")?;
                }
                write!(f, "{}", name)
            }
            Statement::CreateSchema {
                name,
                if_not_exists,
            } => {
                write!(f, "CREATE SCHEMA ")?;
                if *if_not_exists {
                    write!(f, "IF NOT EXISTS ")?;
                }
                write!(f, "{}", name)
            }
            Statement::CreateSource {
                name,
                connector,
                with_options,
                format,
                envelope,
                if_not_exists,
                materialized,
            } => {
                write!(f, "CREATE ")?;
                if *materialized {
                    write!(f, "MATERIALIZED ")?;
                }
                write!(f, "SOURCE ")?;
                if *if_not_exists {
                    write!(f, "IF NOT EXISTS ")?;
                }
                write!(f, "{} FROM {}", name, connector,)?;
                if !with_options.is_empty() {
                    write!(f, " WITH ({})", display_comma_separated(with_options))?;
                }
                if let Some(format) = format {
                    write!(f, " FORMAT {}", format)?;
                }
                if *envelope != Default::default() {
                    write!(f, " ENVELOPE {}", envelope)?;
                }
                Ok(())
            }
            Statement::CreateSink {
                name,
                from,
                connector,
                format,
                if_not_exists,
            } => {
                write!(f, "CREATE SINK ")?;
                if *if_not_exists {
                    write!(f, "IF NOT EXISTS ")?;
                }
                write!(
                    f,
                    "{} FROM {} INTO {} FORMAT {}",
                    name, from, connector, format
                )?;
                Ok(())
            }
            Statement::CreateView {
                name,
                columns,
                query,
                materialized,
                if_exists,
                with_options,
            } => {
                write!(f, "CREATE")?;
                if *if_exists == IfExistsBehavior::Replace {
                    write!(f, " OR REPLACE")?;
                }
                if *materialized {
                    write!(f, " MATERIALIZED")?;
                }

                write!(f, " VIEW")?;

                if *if_exists == IfExistsBehavior::Skip {
                    write!(f, " IF NOT EXISTS")?;
                }

                write!(f, " {}", name)?;

                if !with_options.is_empty() {
                    write!(f, " WITH ({})", display_comma_separated(with_options))?;
                }

                if !columns.is_empty() {
                    write!(f, " ({})", display_comma_separated(columns))?;
                }

                write!(f, " AS {}", query)
            }
            Statement::CreateTable {
                name,
                columns,
                constraints,
                with_options,
                if_not_exists,
            } => {
                write!(f, "CREATE TABLE ")?;
                if *if_not_exists {
                    write!(f, "IF NOT EXISTS ")?;
                }
                write!(f, "{} ({}", name, display_comma_separated(columns))?;
                if !constraints.is_empty() {
                    write!(f, ", {}", display_comma_separated(constraints))?;
                }
                write!(f, ")")?;

                if !with_options.is_empty() {
                    write!(f, " WITH ({})", display_comma_separated(with_options))?;
                }
                Ok(())
            }
            Statement::CreateIndex {
                name,
                on_name,
                key_parts,
                if_not_exists,
            } => {
                write!(f, "CREATE INDEX ")?;
                if *if_not_exists {
                    write!(f, "IF NOT EXISTS ")?;
                }
                write!(
                    f,
                    "{} ON {} ({})",
                    name,
                    on_name,
                    display_comma_separated(key_parts),
                )?;
                Ok(())
            }
            Statement::AlterTable { name, operation } => {
                write!(f, "ALTER TABLE {} {}", name, operation)
            }
            Statement::DropDatabase { name, if_exists } => {
                write!(f, "DROP DATABASE ")?;
                if *if_exists {
                    write!(f, "IF EXISTS ")?;
                }
                write!(f, "{}", name)
            }
            Statement::DropObjects {
                object_type,
                if_exists,
                names,
                cascade,
            } => write!(
                f,
                "DROP {}{} {}{}",
                object_type,
                if *if_exists { " IF EXISTS" } else { "" },
                display_comma_separated(names),
                if *cascade { " CASCADE" } else { "" },
            ),
            Statement::SetVariable {
                local,
                variable,
                value,
            } => {
                f.write_str("SET ")?;
                if *local {
                    f.write_str("LOCAL ")?;
                }
                write!(f, "{} = {}", variable, value)
            }
            Statement::ShowVariable { variable } => write!(f, "SHOW {}", variable),
            Statement::ShowDatabases { filter } => {
                f.write_str("SHOW DATABASES")?;
                if let Some(filter) = filter {
                    write!(f, " {}", filter)?;
                }
                Ok(())
            }
            Statement::ShowObjects {
                object_type,
                filter,
                full,
                materialized,
                from,
                extended,
            } => {
                f.write_str("SHOW")?;
                if *extended {
                    f.write_str(" EXTENDED")?;
                }
                if *full {
                    f.write_str(" FULL")?;
                }
                if *materialized {
                    f.write_str(" MATERIALIZED")?;
                }
                write!(
                    f,
                    " {}",
                    match object_type {
                        ObjectType::Schema => "SCHEMAS",
                        ObjectType::Table => "TABLES",
                        ObjectType::View => "VIEWS",
                        ObjectType::Source => "SOURCES",
                        ObjectType::Sink => "SINKS",
                        ObjectType::Index => unreachable!(),
                    }
                )?;
                if let Some(from) = from {
                    write!(f, " FROM {}", from)?;
                }
                if let Some(filter) = filter {
                    write!(f, " {}", filter)?;
                }
                Ok(())
            }
            Statement::ShowIndexes {
                table_name,
                extended,
                filter,
            } => {
                write!(f, "SHOW ")?;
                if *extended {
                    f.write_str("EXTENDED ")?;
                }
                write!(f, "INDEXES FROM {}", table_name)?;
                if let Some(filter) = filter {
                    write!(f, " {}", filter)?;
                }
                Ok(())
            }
            Statement::ShowColumns {
                extended,
                full,
                table_name,
                filter,
            } => {
                f.write_str("SHOW ")?;
                if *extended {
                    f.write_str("EXTENDED ")?;
                }
                if *full {
                    f.write_str("FULL ")?;
                }
                write!(f, "COLUMNS FROM {}", table_name)?;
                if let Some(filter) = filter {
                    write!(f, " {}", filter)?;
                }
                Ok(())
            }
            Statement::ShowCreateView { view_name } => {
                f.write_str("SHOW CREATE VIEW ")?;
                write!(f, "{}", view_name)
            }
            Statement::ShowCreateSource { source_name } => {
                f.write_str("SHOW CREATE SOURCE ")?;
                write!(f, "{}", source_name)
            }
            Statement::ShowCreateSink { sink_name } => {
                f.write_str("SHOW CREATE SINK ")?;
                write!(f, "{}", sink_name)
            }
            Statement::StartTransaction { modes } => {
                write!(f, "START TRANSACTION")?;
                if !modes.is_empty() {
                    write!(f, " {}", display_comma_separated(modes))?;
                }
                Ok(())
            }
            Statement::SetTransaction { modes } => {
                write!(f, "SET TRANSACTION")?;
                if !modes.is_empty() {
                    write!(f, " {}", display_comma_separated(modes))?;
                }
                Ok(())
            }
            Statement::Commit { chain } => {
                write!(f, "COMMIT{}", if *chain { " AND CHAIN" } else { "" },)
            }
            Statement::Rollback { chain } => {
                write!(f, "ROLLBACK{}", if *chain { " AND CHAIN" } else { "" },)
            }
            Statement::Tail { name } => write!(f, "TAIL {}", name),
            Statement::Explain { stage, query } => write!(f, "EXPLAIN {} FOR {}", stage, query),
        }
    }
}

/// SQL assignment `foo = expr` as used in SQLUpdate
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Assignment {
    pub id: Ident,
    pub value: Expr,
}

impl fmt::Display for Assignment {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} = {}", self.id, self.value)
    }
}

/// A function call
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Function {
    pub name: ObjectName,
    pub args: Vec<Expr>,
    pub over: Option<WindowSpec>,
    // aggregate functions may specify eg `COUNT(DISTINCT x)`
    pub distinct: bool,
}

impl fmt::Display for Function {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}({}{})",
            self.name,
            if self.distinct { "DISTINCT " } else { "" },
            display_comma_separated(&self.args),
        )?;
        if let Some(o) = &self.over {
            write!(f, " OVER ({})", o)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub enum ObjectType {
    Schema,
    Table,
    View,
    Source,
    Sink,
    Index,
}

impl fmt::Display for ObjectType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SqlOption {
    pub name: Ident,
    pub value: Value,
}

impl fmt::Display for SqlOption {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} = {}", self.name, self.value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TransactionMode {
    AccessMode(TransactionAccessMode),
    IsolationLevel(TransactionIsolationLevel),
}

impl fmt::Display for TransactionMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use TransactionMode::*;
        match self {
            AccessMode(access_mode) => write!(f, "{}", access_mode),
            IsolationLevel(iso_level) => write!(f, "ISOLATION LEVEL {}", iso_level),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TransactionAccessMode {
    ReadOnly,
    ReadWrite,
}

impl fmt::Display for TransactionAccessMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use TransactionAccessMode::*;
        f.write_str(match self {
            ReadOnly => "READ ONLY",
            ReadWrite => "READ WRITE",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TransactionIsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

impl fmt::Display for TransactionIsolationLevel {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use TransactionIsolationLevel::*;
        f.write_str(match self {
            ReadUncommitted => "READ UNCOMMITTED",
            ReadCommitted => "READ COMMITTED",
            RepeatableRead => "REPEATABLE READ",
            Serializable => "SERIALIZABLE",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ShowStatementFilter {
    Like(String),
    Where(Expr),
}

impl fmt::Display for ShowStatementFilter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use ShowStatementFilter::*;
        match self {
            Like(pattern) => write!(f, "LIKE '{}'", value::escape_single_quote_string(pattern)),
            Where(expr) => write!(f, "WHERE {}", expr),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SetVariableValue {
    Ident(Ident),
    Literal(Value),
}

impl fmt::Display for SetVariableValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use SetVariableValue::*;
        match self {
            Ident(ident) => write!(f, "{}", ident),
            Literal(literal) => write!(f, "{}", literal),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IfExistsBehavior {
    Error,
    Skip,
    Replace,
}
