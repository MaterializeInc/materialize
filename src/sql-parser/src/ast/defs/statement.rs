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
    ColumnDef, Connector, Envelope, Expr, Format, Ident, ObjectName, Query, TableConstraint, Value,
};

/// A top-level statement (SELECT, INSERT, CREATE, etc.)
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Copy(CopyStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    CreateDatabase(CreateDatabaseStatement),
    CreateSchema(CreateSchemaStatement),
    CreateSource(CreateSourceStatement),
    CreateSink(CreateSinkStatement),
    CreateView(CreateViewStatement),
    CreateTable(CreateTableStatement),
    CreateIndex(CreateIndexStatement),
    AlterObjectRename(AlterObjectRenameStatement),
    DropDatabase(DropDatabaseStatement),
    DropObjects(DropObjectsStatement),
    SetVariable(SetVariableStatement),
    ShowDatabases(ShowDatabasesStatement),
    ShowObjects(ShowObjectsStatement),
    ShowIndexes(ShowIndexesStatement),
    ShowColumns(ShowColumnsStatement),
    ShowCreateView(ShowCreateViewStatement),
    ShowCreateSource(ShowCreateSourceStatement),
    ShowCreateTable(ShowCreateTableStatement),
    ShowCreateSink(ShowCreateSinkStatement),
    ShowCreateIndex(ShowCreateIndexStatement),
    ShowVariable(ShowVariableStatement),
    StartTransaction(StartTransactionStatement),
    SetTransaction(SetTransactionStatement),
    Commit(CommitStatement),
    Rollback(RollbackStatement),
    Tail(TailStatement),
    Explain(ExplainStatement),
}

impl AstDisplay for Statement {
    fn fmt(&self, f: &mut AstFormatter) {
        match self {
            Statement::Select(stmt) => f.write_node(stmt),
            Statement::Insert(stmt) => f.write_node(stmt),
            Statement::Copy(stmt) => f.write_node(stmt),
            Statement::Update(stmt) => f.write_node(stmt),
            Statement::Delete(stmt) => f.write_node(stmt),
            Statement::CreateDatabase(stmt) => f.write_node(stmt),
            Statement::CreateSchema(stmt) => f.write_node(stmt),
            Statement::CreateSource(stmt) => f.write_node(stmt),
            Statement::CreateSink(stmt) => f.write_node(stmt),
            Statement::CreateView(stmt) => f.write_node(stmt),
            Statement::CreateTable(stmt) => f.write_node(stmt),
            Statement::CreateIndex(stmt) => f.write_node(stmt),
            Statement::AlterObjectRename(stmt) => f.write_node(stmt),
            Statement::DropDatabase(stmt) => f.write_node(stmt),
            Statement::DropObjects(stmt) => f.write_node(stmt),
            Statement::SetVariable(stmt) => f.write_node(stmt),
            Statement::ShowDatabases(stmt) => f.write_node(stmt),
            Statement::ShowObjects(stmt) => f.write_node(stmt),
            Statement::ShowIndexes(stmt) => f.write_node(stmt),
            Statement::ShowColumns(stmt) => f.write_node(stmt),
            Statement::ShowCreateView(stmt) => f.write_node(stmt),
            Statement::ShowCreateSource(stmt) => f.write_node(stmt),
            Statement::ShowCreateTable(stmt) => f.write_node(stmt),
            Statement::ShowCreateSink(stmt) => f.write_node(stmt),
            Statement::ShowCreateIndex(stmt) => f.write_node(stmt),
            Statement::ShowVariable(stmt) => f.write_node(stmt),
            Statement::StartTransaction(stmt) => f.write_node(stmt),
            Statement::SetTransaction(stmt) => f.write_node(stmt),
            Statement::Commit(stmt) => f.write_node(stmt),
            Statement::Rollback(stmt) => f.write_node(stmt),
            Statement::Tail(stmt) => f.write_node(stmt),
            Statement::Explain(stmt) => f.write_node(stmt),
        }
    }
}
impl_display!(Statement);

/// `SELECT`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SelectStatement {
    pub query: Box<Query>,
    pub as_of: Option<Expr>,
}

impl AstDisplay for SelectStatement {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_node(&self.query);
        if let Some(as_of) = &self.as_of {
            f.write_str(" AS OF ");
            f.write_node(as_of);
        }
    }
}
impl_display!(SelectStatement);

/// `INSERT`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct InsertStatement {
    /// TABLE
    pub table_name: ObjectName,
    /// COLUMNS
    pub columns: Vec<Ident>,
    /// A SQL query that specifies what to insert.
    pub source: InsertSource,
}

impl AstDisplay for InsertStatement {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("INSERT INTO ");
        f.write_node(&self.table_name);
        if !self.columns.is_empty() {
            f.write_str(" (");
            f.write_node(&display::comma_separated(&self.columns));
            f.write_str(")");
        }
        f.write_str(" ");
        f.write_node(&self.source);
    }
}
impl_display!(InsertStatement);

/// `COPY`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CopyStatement {
    /// TABLE
    pub table_name: ObjectName,
    /// COLUMNS
    pub columns: Vec<Ident>,
    /// VALUES a vector of values to be copied
    pub values: Vec<Option<String>>,
}

impl AstDisplay for CopyStatement {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("COPY ");
        f.write_node(&self.table_name);
        if !self.columns.is_empty() {
            f.write_str("(");
            f.write_node(&display::comma_separated(&self.columns));
            f.write_str(")");
        }
        f.write_str(" FROM stdin; ");
        if !self.values.is_empty() {
            f.write_str("\n");
            let mut delim = "";
            for v in &self.values {
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
}
impl_display!(CopyStatement);

/// `UPDATE`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UpdateStatement {
    /// TABLE
    pub table_name: ObjectName,
    /// Column assignments
    pub assignments: Vec<Assignment>,
    /// WHERE
    pub selection: Option<Expr>,
}

impl AstDisplay for UpdateStatement {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("UPDATE ");
        f.write_node(&self.table_name);
        if !self.assignments.is_empty() {
            f.write_str(" SET ");
            f.write_node(&display::comma_separated(&self.assignments));
        }
        if let Some(selection) = &self.selection {
            f.write_str(" WHERE ");
            f.write_node(selection);
        }
    }
}
impl_display!(UpdateStatement);

/// `DELETE`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DeleteStatement {
    /// `FROM`
    pub table_name: ObjectName,
    /// `WHERE`
    pub selection: Option<Expr>,
}

impl AstDisplay for DeleteStatement {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("DELETE FROM ");
        f.write_node(&self.table_name);
        if let Some(selection) = &self.selection {
            f.write_str(" WHERE ");
            f.write_node(selection);
        }
    }
}
impl_display!(DeleteStatement);

/// `CREATE DATABASE`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateDatabaseStatement {
    pub name: Ident,
    pub if_not_exists: bool,
}

impl AstDisplay for CreateDatabaseStatement {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("CREATE DATABASE ");
        if self.if_not_exists {
            f.write_str("IF NOT EXISTS ");
        }
        f.write_node(&self.name);
    }
}
impl_display!(CreateDatabaseStatement);

/// `CREATE SCHEMA`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateSchemaStatement {
    pub name: ObjectName,
    pub if_not_exists: bool,
}

impl AstDisplay for CreateSchemaStatement {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("CREATE SCHEMA ");
        if self.if_not_exists {
            f.write_str("IF NOT EXISTS ");
        }
        f.write_node(&self.name);
    }
}
impl_display!(CreateSchemaStatement);

/// `CREATE SOURCE`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateSourceStatement {
    pub name: ObjectName,
    pub col_names: Vec<Ident>,
    pub connector: Connector,
    pub with_options: Vec<SqlOption>,
    pub format: Option<Format>,
    pub envelope: Envelope,
    pub if_not_exists: bool,
    pub materialized: bool,
}

impl AstDisplay for CreateSourceStatement {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("CREATE ");
        if self.materialized {
            f.write_str("MATERIALIZED ");
        }
        f.write_str("SOURCE ");
        if self.if_not_exists {
            f.write_str("IF NOT EXISTS ");
        }
        f.write_node(&self.name);
        f.write_str(" ");
        if !self.col_names.is_empty() {
            f.write_str("(");
            f.write_node(&display::comma_separated(&self.col_names));
            f.write_str(") ");
        }
        f.write_str("FROM ");
        f.write_node(&self.connector);
        if !self.with_options.is_empty() {
            f.write_str(" WITH (");
            f.write_node(&display::comma_separated(&self.with_options));
            f.write_str(")");
        }
        if let Some(format) = &self.format {
            f.write_str(" FORMAT ");
            f.write_node(format);
        }
        if self.envelope != Default::default() {
            f.write_str(" ENVELOPE ");
            f.write_node(&self.envelope);
        }
    }
}
impl_display!(CreateSourceStatement);

/// `CREATE SINK`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateSinkStatement {
    pub name: ObjectName,
    pub from: ObjectName,
    pub connector: Connector,
    pub with_options: Vec<SqlOption>,
    pub format: Option<Format>,
    pub with_snapshot: bool,
    pub as_of: Option<Expr>,
    pub if_not_exists: bool,
}

impl AstDisplay for CreateSinkStatement {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("CREATE SINK ");
        if self.if_not_exists {
            f.write_str("IF NOT EXISTS ");
        }
        f.write_node(&self.name);
        f.write_str(" FROM ");
        f.write_node(&self.from);
        f.write_str(" INTO ");
        f.write_node(&self.connector);
        if !self.with_options.is_empty() {
            f.write_str(" WITH (");
            f.write_node(&display::comma_separated(&self.with_options));
            f.write_str(")");
        }
        if let Some(format) = &self.format {
            f.write_str(" FORMAT ");
            f.write_node(format);
        }
        if self.with_snapshot {
            f.write_str(" WITH SNAPSHOT");
        } else {
            f.write_str(" WITHOUT SNAPSHOT");
        }

        if let Some(as_of) = &self.as_of {
            f.write_str(" AS OF ");
            f.write_node(as_of);
        }
    }
}
impl_display!(CreateSinkStatement);

/// `CREATE VIEW`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateViewStatement {
    /// View name
    pub name: ObjectName,
    pub columns: Vec<Ident>,
    pub with_options: Vec<SqlOption>,
    pub query: Box<Query>,
    pub if_exists: IfExistsBehavior,
    pub temporary: bool,
    pub materialized: bool,
}

impl AstDisplay for CreateViewStatement {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("CREATE");
        if self.if_exists == IfExistsBehavior::Replace {
            f.write_str(" OR REPLACE");
        }
        if self.temporary {
            f.write_str(" TEMPORARY");
        }
        if self.materialized {
            f.write_str(" MATERIALIZED");
        }

        f.write_str(" VIEW");

        if self.if_exists == IfExistsBehavior::Skip {
            f.write_str(" IF NOT EXISTS");
        }

        f.write_str(" ");
        f.write_node(&self.name);

        if !self.with_options.is_empty() {
            f.write_str(" WITH (");
            f.write_node(&display::comma_separated(&self.with_options));
            f.write_str(")");
        }

        if !self.columns.is_empty() {
            f.write_str(" (");
            f.write_node(&display::comma_separated(&self.columns));
            f.write_str(")");
        }

        f.write_str(" AS ");
        f.write_node(&self.query);
    }
}
impl_display!(CreateViewStatement);

/// `CREATE TABLE`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateTableStatement {
    /// Table name
    pub name: ObjectName,
    /// Optional schema
    pub columns: Vec<ColumnDef>,
    pub constraints: Vec<TableConstraint>,
    pub with_options: Vec<SqlOption>,
    pub if_not_exists: bool,
}

impl AstDisplay for CreateTableStatement {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("CREATE TABLE ");
        if self.if_not_exists {
            f.write_str("IF NOT EXISTS ");
        }
        f.write_node(&self.name);
        f.write_str(" (");
        f.write_node(&display::comma_separated(&self.columns));
        if !self.constraints.is_empty() {
            f.write_str(", ");
            f.write_node(&display::comma_separated(&self.constraints));
        }
        f.write_str(")");

        if !self.with_options.is_empty() {
            f.write_str(" WITH (");
            f.write_node(&display::comma_separated(&self.with_options));
            f.write_str(")");
        }
    }
}
impl_display!(CreateTableStatement);

/// `CREATE INDEX`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateIndexStatement {
    /// Optional index name.
    pub name: Option<Ident>,
    /// `ON` table or view name
    pub on_name: ObjectName,
    /// Expressions that form part of the index key. If not included, the
    /// key_parts will be inferred from the named object.
    pub key_parts: Option<Vec<Expr>>,
    pub if_not_exists: bool,
}

impl AstDisplay for CreateIndexStatement {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("CREATE ");
        if self.key_parts.is_none() {
            f.write_str("DEFAULT ");
        }
        f.write_str("INDEX ");
        if self.if_not_exists {
            f.write_str("IF NOT EXISTS ");
        }
        if let Some(name) = &self.name {
            f.write_node(name);
            f.write_str(" ");
        }
        f.write_str("ON ");
        f.write_node(&self.on_name);
        if let Some(key_parts) = &self.key_parts {
            f.write_str(" (");
            f.write_node(&display::comma_separated(key_parts));
            f.write_str(")");
        }
    }
}
impl_display!(CreateIndexStatement);

/// `ALTER <OBJECT> ... RENAME TO`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AlterObjectRenameStatement {
    pub object_type: ObjectType,
    pub if_exists: bool,
    pub name: ObjectName,
    pub to_item_name: Ident,
}

impl AstDisplay for AlterObjectRenameStatement {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("ALTER ");
        f.write_node(&self.object_type);
        f.write_str(" ");
        if self.if_exists {
            f.write_str("IF EXISTS ");
        }
        f.write_node(&self.name);
        f.write_str(" RENAME TO ");
        f.write_node(&self.to_item_name);
    }
}
impl_display!(AlterObjectRenameStatement);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DropDatabaseStatement {
    pub name: Ident,
    pub if_exists: bool,
}

impl AstDisplay for DropDatabaseStatement {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("DROP DATABASE ");
        if self.if_exists {
            f.write_str("IF EXISTS ");
        }
        f.write_node(&self.name);
    }
}
impl_display!(DropDatabaseStatement);

/// `DROP`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DropObjectsStatement {
    /// The type of the object to drop: TABLE, VIEW, etc.
    pub object_type: ObjectType,
    /// An optional `IF EXISTS` clause. (Non-standard.)
    pub if_exists: bool,
    /// One or more objects to drop. (ANSI SQL requires exactly one.)
    pub names: Vec<ObjectName>,
    /// Whether `CASCADE` was specified. This will be `false` when
    /// `RESTRICT` or no drop behavior at all was specified.
    pub cascade: bool,
}

impl AstDisplay for DropObjectsStatement {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("DROP ");
        f.write_node(&self.object_type);
        f.write_str(" ");
        if self.if_exists {
            f.write_str("IF EXISTS ");
        }
        f.write_node(&display::comma_separated(&self.names));
        if self.cascade {
            f.write_str(" CASCADE");
        }
    }
}
impl_display!(DropObjectsStatement);

/// `SET <variable>`
///
/// Note: this is not a standard SQL statement, but it is supported by at
/// least MySQL and PostgreSQL. Not all MySQL-specific syntatic forms are
/// supported yet.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SetVariableStatement {
    pub local: bool,
    pub variable: Ident,
    pub value: SetVariableValue,
}

impl AstDisplay for SetVariableStatement {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("SET ");
        if self.local {
            f.write_str("LOCAL ");
        }
        f.write_node(&self.variable);
        f.write_str(" = ");
        f.write_node(&self.value);
    }
}
impl_display!(SetVariableStatement);

/// `SHOW <variable>`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShowVariableStatement {
    pub variable: Ident,
}

impl AstDisplay for ShowVariableStatement {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("SHOW ");
        f.write_node(&self.variable);
    }
}
impl_display!(ShowVariableStatement);

/// `SHOW DATABASES`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShowDatabasesStatement {
    pub filter: Option<Expr>,
}

impl AstDisplay for ShowDatabasesStatement {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("SHOW DATABASES");
        if let Some(filter) = &self.filter {
            f.write_str(" ");
            f.write_node(filter);
        }
    }
}
impl_display!(ShowDatabasesStatement);

/// `SHOW <object>S`
///
/// ```sql
/// SHOW TABLES;
/// SHOW SOURCES;
/// SHOW VIEWS;
/// SHOW SINKS;
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShowObjectsStatement {
    pub object_type: ObjectType,
    pub from: Option<ObjectName>,
    pub extended: bool,
    pub full: bool,
    pub materialized: bool,
    pub filter: Option<ShowStatementFilter>,
}

impl AstDisplay for ShowObjectsStatement {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("SHOW");
        if self.extended {
            f.write_str(" EXTENDED");
        }
        if self.full {
            f.write_str(" FULL");
        }
        if self.materialized {
            f.write_str(" MATERIALIZED");
        }
        f.write_str(" ");
        f.write_str(match &self.object_type {
            ObjectType::Schema => "SCHEMAS",
            ObjectType::Table => "TABLES",
            ObjectType::View => "VIEWS",
            ObjectType::Source => "SOURCES",
            ObjectType::Sink => "SINKS",
            ObjectType::Index => unreachable!(),
        });
        if let Some(from) = &self.from {
            f.write_str(" FROM ");
            f.write_node(&from);
        }
        if let Some(filter) = &self.filter {
            f.write_str(" ");
            f.write_node(filter);
        }
    }
}
impl_display!(ShowObjectsStatement);

/// `SHOW INDEX|INDEXES|KEYS`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShowIndexesStatement {
    pub table_name: ObjectName,
    pub extended: bool,
    pub filter: Option<ShowStatementFilter>,
}

impl AstDisplay for ShowIndexesStatement {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("SHOW ");
        if self.extended {
            f.write_str("EXTENDED ");
        }
        f.write_str("INDEXES FROM ");
        f.write_node(&self.table_name);
        if let Some(filter) = &self.filter {
            f.write_str(" ");
            f.write_node(filter);
        }
    }
}
impl_display!(ShowIndexesStatement);

/// `SHOW COLUMNS`
///
/// Note: this is a MySQL-specific statement.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShowColumnsStatement {
    pub extended: bool,
    pub full: bool,
    pub table_name: ObjectName,
    pub filter: Option<ShowStatementFilter>,
}

impl AstDisplay for ShowColumnsStatement {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("SHOW ");
        if self.extended {
            f.write_str("EXTENDED ");
        }
        if self.full {
            f.write_str("FULL ");
        }
        f.write_str("COLUMNS FROM ");
        f.write_node(&self.table_name);
        if let Some(filter) = &self.filter {
            f.write_str(" ");
            f.write_node(filter);
        }
    }
}
impl_display!(ShowColumnsStatement);

/// `SHOW CREATE VIEW <view>`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShowCreateViewStatement {
    pub view_name: ObjectName,
}

impl AstDisplay for ShowCreateViewStatement {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("SHOW CREATE VIEW ");
        f.write_node(&self.view_name);
    }
}
impl_display!(ShowCreateViewStatement);

/// `SHOW CREATE SOURCE <source>`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShowCreateSourceStatement {
    pub source_name: ObjectName,
}

impl AstDisplay for ShowCreateSourceStatement {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("SHOW CREATE SOURCE ");
        f.write_node(&self.source_name);
    }
}
impl_display!(ShowCreateSourceStatement);

/// `SHOW CREATE TABLE <table>`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShowCreateTableStatement {
    pub table_name: ObjectName,
}

impl AstDisplay for ShowCreateTableStatement {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("SHOW CREATE TABLE ");
        f.write_node(&self.table_name);
    }
}
impl_display!(ShowCreateTableStatement);

/// `SHOW CREATE SINK <sink>`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShowCreateSinkStatement {
    pub sink_name: ObjectName,
}

impl AstDisplay for ShowCreateSinkStatement {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("SHOW CREATE SINK ");
        f.write_node(&self.sink_name);
    }
}
impl_display!(ShowCreateSinkStatement);

/// `SHOW CREATE INDEX <index>`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShowCreateIndexStatement {
    pub index_name: ObjectName,
}

impl AstDisplay for ShowCreateIndexStatement {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("SHOW CREATE INDEX ");
        f.write_node(&self.index_name);
    }
}
impl_display!(ShowCreateIndexStatement);

/// `{ BEGIN [ TRANSACTION | WORK ] | START TRANSACTION } ...`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StartTransactionStatement {
    pub modes: Vec<TransactionMode>,
}

impl AstDisplay for StartTransactionStatement {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("START TRANSACTION");
        if !self.modes.is_empty() {
            f.write_str(" ");
            f.write_node(&display::comma_separated(&self.modes));
        }
    }
}
impl_display!(StartTransactionStatement);

/// `SET TRANSACTION ...`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SetTransactionStatement {
    pub modes: Vec<TransactionMode>,
}

impl AstDisplay for SetTransactionStatement {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("SET TRANSACTION");
        if !self.modes.is_empty() {
            f.write_str(" ");
            f.write_node(&display::comma_separated(&self.modes));
        }
    }
}
impl_display!(SetTransactionStatement);

/// `COMMIT [ TRANSACTION | WORK ] [ AND [ NO ] CHAIN ]`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CommitStatement {
    pub chain: bool,
}

impl AstDisplay for CommitStatement {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("COMMIT");
        if self.chain {
            f.write_str(" AND CHAIN");
        }
    }
}
impl_display!(CommitStatement);

/// `ROLLBACK [ TRANSACTION | WORK ] [ AND [ NO ] CHAIN ]`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RollbackStatement {
    pub chain: bool,
}

impl AstDisplay for RollbackStatement {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("ROLLBACK");
        if self.chain {
            f.write_str(" AND CHAIN");
        }
    }
}
impl_display!(RollbackStatement);

/// `TAIL`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TailStatement {
    pub name: ObjectName,
    pub with_snapshot: bool,
    pub as_of: Option<Expr>,
}

impl AstDisplay for TailStatement {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("TAIL ");
        f.write_node(&self.name);

        if self.with_snapshot {
            f.write_str(" WITH SNAPSHOT");
        } else {
            f.write_str(" WITHOUT SNAPSHOT");
        }
        if let Some(as_of) = &self.as_of {
            f.write_str(" AS OF ");
            f.write_node(as_of);
        }
    }
}
impl_display!(TailStatement);

/// `EXPLAIN ...`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExplainStatement {
    pub stage: ExplainStage,
    pub explainee: Explainee,
    pub options: ExplainOptions,
}

impl AstDisplay for ExplainStatement {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("EXPLAIN ");
        if self.options.typed {
            f.write_str("TYPED ");
        }
        f.write_node(&self.stage);
        f.write_str(" FOR ");
        f.write_node(&self.explainee);
    }
}
impl_display!(ExplainStatement);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum InsertSource {
    Query(Box<Query>),
    DefaultValues,
}

impl AstDisplay for InsertSource {
    fn fmt(&self, f: &mut AstFormatter) {
        match self {
            InsertSource::Query(query) => f.write_node(query),
            InsertSource::DefaultValues => f.write_str("DEFAULT VALUES"),
        }
    }
}
impl_display!(InsertSource);

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
