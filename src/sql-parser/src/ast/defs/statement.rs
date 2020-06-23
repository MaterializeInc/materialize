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

use crate::ast::display::{self, AstDisplay, AstFormatter};
use crate::ast::{
    AlterTableOperation, ColumnDef, Connector, Envelope, Expr, Format, Ident, ObjectName, Query,
    TableConstraint, Value,
};

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
        with_options: Vec<SqlOption>,
        query: Box<Query>,
        if_exists: IfExistsBehavior,
        temporary: bool,
        materialized: bool,
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
        /// Optional index name.
        name: Option<Ident>,
        /// `ON` table or view name
        on_name: ObjectName,
        /// Expressions that form part of the index key. If not included, the
        /// key_parts will be inferred from the named object.
        key_parts: Option<Vec<Expr>>,
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
        without_snapshot: bool,
        as_of: Option<Expr>,
    },
    /// `EXPLAIN [ DATAFLOW | PLAN ] FOR`
    Explain {
        stage: ExplainStage,
        explainee: Explainee,
        options: ExplainOptions,
    },
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
                    f.write_node(&display::comma_separated(columns));
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
                    f.write_node(&display::comma_separated(columns));
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
                    f.write_node(&display::comma_separated(assignments));
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
                    f.write_node(&display::comma_separated(col_names));
                    f.write_str(") ");
                }
                f.write_str("FROM ");
                f.write_node(connector);
                if !with_options.is_empty() {
                    f.write_str(" WITH (");
                    f.write_node(&display::comma_separated(with_options));
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
                    f.write_node(&display::comma_separated(with_options));
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
                    f.write_node(&display::comma_separated(with_options));
                    f.write_str(")");
                }

                if !columns.is_empty() {
                    f.write_str(" (");
                    f.write_node(&display::comma_separated(columns));
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
                f.write_node(&display::comma_separated(columns));
                if !constraints.is_empty() {
                    f.write_str(", ");
                    f.write_node(&display::comma_separated(constraints));
                }
                f.write_str(")");

                if !with_options.is_empty() {
                    f.write_str(" WITH (");
                    f.write_node(&display::comma_separated(with_options));
                    f.write_str(")");
                }
            }
            Statement::CreateIndex {
                name,
                on_name,
                key_parts,
                if_not_exists,
            } => {
                f.write_str("CREATE ");
                if key_parts.is_none() {
                    f.write_str("DEFAULT ");
                }
                f.write_str("INDEX ");
                if *if_not_exists {
                    f.write_str("IF NOT EXISTS ");
                }
                if let Some(name) = name {
                    f.write_node(name);
                    f.write_str(" ");
                }
                f.write_str("ON ");
                f.write_node(&on_name);
                if let Some(key_parts) = key_parts {
                    f.write_str(" (");
                    f.write_node(&display::comma_separated(key_parts));
                    f.write_str(")");
                }
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
                f.write_node(&display::comma_separated(names));
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
                    f.write_node(&display::comma_separated(modes));
                }
            }
            Statement::SetTransaction { modes } => {
                f.write_str("SET TRANSACTION");
                if !modes.is_empty() {
                    f.write_str(" ");
                    f.write_node(&display::comma_separated(modes));
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
                without_snapshot,
                as_of,
            } => {
                f.write_str("TAIL ");
                f.write_node(&name);

                if *without_snapshot {
                    f.write_str(" WITHOUT SNAPSHOT");
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
                f.write_node(&display::escape_single_quote_string(pattern));
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
pub enum Explainee {
    View(ObjectName),
    Query(Query),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExplainOptions {
    pub typed: bool,
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IfExistsBehavior {
    Error,
    Skip,
    Replace,
}
