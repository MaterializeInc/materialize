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
    AstInfo, ColumnDef, Connector, DataType, Envelope, Expr, Format, Ident, MultiConnector, Query,
    TableConstraint, UnresolvedObjectName, Value,
};

/// A top-level statement (SELECT, INSERT, CREATE, etc.)
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Statement<T: AstInfo> {
    Select(SelectStatement<T>),
    Insert(InsertStatement<T>),
    Copy(CopyStatement<T>),
    Update(UpdateStatement<T>),
    Delete(DeleteStatement<T>),
    CreateDatabase(CreateDatabaseStatement),
    CreateSchema(CreateSchemaStatement),
    CreateSource(CreateSourceStatement<T>),
    CreateSources(CreateSourcesStatement<T>),
    CreateSink(CreateSinkStatement<T>),
    CreateView(CreateViewStatement<T>),
    CreateTable(CreateTableStatement<T>),
    CreateIndex(CreateIndexStatement<T>),
    CreateType(CreateTypeStatement<T>),
    CreateRole(CreateRoleStatement),
    AlterObjectRename(AlterObjectRenameStatement),
    AlterIndexOptions(AlterIndexOptionsStatement),
    Discard(DiscardStatement),
    DropDatabase(DropDatabaseStatement),
    DropObjects(DropObjectsStatement),
    SetVariable(SetVariableStatement),
    ShowDatabases(ShowDatabasesStatement<T>),
    ShowObjects(ShowObjectsStatement<T>),
    ShowIndexes(ShowIndexesStatement<T>),
    ShowColumns(ShowColumnsStatement<T>),
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
    Tail(TailStatement<T>),
    Explain(ExplainStatement<T>),
    Declare(DeclareStatement<T>),
    Fetch(FetchStatement),
    Close(CloseStatement),
}

impl<T: AstInfo> Statement<T> {
    /// Reports whether the statement is cursor-related.
    pub fn is_cursor(&self) -> bool {
        matches!(
            self,
            Statement::Declare(_) | Statement::Fetch(_) | Statement::Close(_)
        )
    }
}

impl<T: AstInfo> AstDisplay for Statement<T> {
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
            Statement::CreateSources(stmt) => f.write_node(stmt),
            Statement::CreateSink(stmt) => f.write_node(stmt),
            Statement::CreateView(stmt) => f.write_node(stmt),
            Statement::CreateTable(stmt) => f.write_node(stmt),
            Statement::CreateIndex(stmt) => f.write_node(stmt),
            Statement::CreateRole(stmt) => f.write_node(stmt),
            Statement::CreateType(stmt) => f.write_node(stmt),
            Statement::AlterObjectRename(stmt) => f.write_node(stmt),
            Statement::AlterIndexOptions(stmt) => f.write_node(stmt),
            Statement::Discard(stmt) => f.write_node(stmt),
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
            Statement::Declare(stmt) => f.write_node(stmt),
            Statement::Close(stmt) => f.write_node(stmt),
            Statement::Fetch(stmt) => f.write_node(stmt),
        }
    }
}
impl_display_t!(Statement);

/// `SELECT`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SelectStatement<T: AstInfo> {
    pub query: Query<T>,
    pub as_of: Option<Expr<T>>,
}

impl<T: AstInfo> AstDisplay for SelectStatement<T> {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_node(&self.query);
        if let Some(as_of) = &self.as_of {
            f.write_str(" AS OF ");
            f.write_node(as_of);
        }
    }
}
impl_display_t!(SelectStatement);

/// `INSERT`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct InsertStatement<T: AstInfo> {
    /// TABLE
    pub table_name: UnresolvedObjectName,
    /// COLUMNS
    pub columns: Vec<Ident>,
    /// A SQL query that specifies what to insert.
    pub source: InsertSource<T>,
}

impl<T: AstInfo> AstDisplay for InsertStatement<T> {
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
impl_display_t!(InsertStatement);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CopyRelation<T: AstInfo> {
    Table {
        name: UnresolvedObjectName,
        columns: Vec<Ident>,
    },
    Select(SelectStatement<T>),
    Tail(TailStatement<T>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CopyDirection {
    To,
    From,
}

impl AstDisplay for CopyDirection {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str(match self {
            CopyDirection::To => "TO",
            CopyDirection::From => "FROM",
        })
    }
}
impl_display!(CopyDirection);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CopyTarget {
    Stdin,
    Stdout,
}

impl AstDisplay for CopyTarget {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str(match self {
            CopyTarget::Stdin => "STDIN",
            CopyTarget::Stdout => "STDOUT",
        })
    }
}
impl_display!(CopyTarget);

/// `COPY`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CopyStatement<T: AstInfo> {
    /// RELATION
    pub relation: CopyRelation<T>,
    /// DIRECTION
    pub direction: CopyDirection,
    // TARGET
    pub target: CopyTarget,
    // OPTIONS
    pub options: Vec<WithOption>,
}

impl<T: AstInfo> AstDisplay for CopyStatement<T> {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("COPY ");
        match &self.relation {
            CopyRelation::Table { name, columns } => {
                f.write_node(&name);
                if !columns.is_empty() {
                    f.write_str("(");
                    f.write_node(&display::comma_separated(&columns));
                    f.write_str(")");
                }
            }
            CopyRelation::Select(query) => {
                f.write_str("(");
                f.write_node(query);
                f.write_str(")");
            }
            CopyRelation::Tail(query) => {
                f.write_str("(");
                f.write_node(query);
                f.write_str(")");
            }
        };
        f.write_str(" ");
        f.write_node(&self.direction);
        f.write_str(" ");
        f.write_node(&self.target);
        if !self.options.is_empty() {
            f.write_str(" WITH (");
            f.write_node(&display::comma_separated(&self.options));
            f.write_str(")");
        }
    }
}
impl_display_t!(CopyStatement);

/// `UPDATE`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UpdateStatement<T: AstInfo> {
    /// TABLE
    pub table_name: UnresolvedObjectName,
    /// Column assignments
    pub assignments: Vec<Assignment<T>>,
    /// WHERE
    pub selection: Option<Expr<T>>,
}

impl<T: AstInfo> AstDisplay for UpdateStatement<T> {
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
impl_display_t!(UpdateStatement);

/// `DELETE`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DeleteStatement<T: AstInfo> {
    /// `FROM`
    pub table_name: UnresolvedObjectName,
    /// `WHERE`
    pub selection: Option<Expr<T>>,
}

impl<T: AstInfo> AstDisplay for DeleteStatement<T> {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("DELETE FROM ");
        f.write_node(&self.table_name);
        if let Some(selection) = &self.selection {
            f.write_str(" WHERE ");
            f.write_node(selection);
        }
    }
}
impl_display_t!(DeleteStatement);

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
    pub name: UnresolvedObjectName,
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
pub struct CreateSourceStatement<T: AstInfo> {
    pub name: UnresolvedObjectName,
    pub col_names: Vec<Ident>,
    pub connector: Connector<T>,
    pub with_options: Vec<SqlOption<T>>,
    pub format: Option<Format<T>>,
    pub envelope: Envelope<T>,
    pub if_not_exists: bool,
    pub materialized: bool,
}

impl<T: AstInfo> AstDisplay for CreateSourceStatement<T> {
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
        match self.envelope {
            Envelope::None => (),
            _ => {
                f.write_str(" ENVELOPE ");
                f.write_node(&self.envelope);
            }
        }
    }
}
impl_display_t!(CreateSourceStatement);

/// `CREATE SOURCES`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateSourcesStatement<T: AstInfo> {
    pub connector: MultiConnector<T>,
    pub stmts: Vec<CreateSourceStatement<T>>,
}

impl<T: AstInfo> AstDisplay for CreateSourcesStatement<T> {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("CREATE SOURCES FROM ");
        f.write_node(&self.connector);
    }
}
impl_display_t!(CreateSourcesStatement);

/// `CREATE SINK`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateSinkStatement<T: AstInfo> {
    pub name: UnresolvedObjectName,
    pub from: UnresolvedObjectName,
    pub connector: Connector<T>,
    pub with_options: Vec<SqlOption<T>>,
    pub format: Option<Format<T>>,
    pub envelope: Option<Envelope<T>>,
    pub with_snapshot: bool,
    pub as_of: Option<Expr<T>>,
    pub if_not_exists: bool,
}

impl<T: AstInfo> AstDisplay for CreateSinkStatement<T> {
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
        if let Some(envelope) = &self.envelope {
            f.write_str(" ENVELOPE ");
            f.write_node(envelope);
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
impl_display_t!(CreateSinkStatement);

/// `CREATE VIEW`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateViewStatement<T: AstInfo> {
    /// View name
    pub name: UnresolvedObjectName,
    pub columns: Vec<Ident>,
    pub with_options: Vec<SqlOption<T>>,
    pub query: Query<T>,
    pub if_exists: IfExistsBehavior,
    pub temporary: bool,
    pub materialized: bool,
}

impl<T: AstInfo> AstDisplay for CreateViewStatement<T> {
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
impl_display_t!(CreateViewStatement);

/// `CREATE TABLE`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateTableStatement<T: AstInfo> {
    /// Table name
    pub name: UnresolvedObjectName,
    /// Optional schema
    pub columns: Vec<ColumnDef<T>>,
    pub constraints: Vec<TableConstraint<T>>,
    pub with_options: Vec<SqlOption<T>>,
    pub if_not_exists: bool,
    pub temporary: bool,
}

impl<T: AstInfo> AstDisplay for CreateTableStatement<T> {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("CREATE ");
        if self.temporary {
            f.write_str("TEMPORARY ");
        }
        f.write_str("TABLE ");
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
impl_display_t!(CreateTableStatement);

/// `CREATE INDEX`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateIndexStatement<T: AstInfo> {
    /// Optional index name.
    pub name: Option<Ident>,
    /// `ON` table or view name
    pub on_name: UnresolvedObjectName,
    /// Expressions that form part of the index key. If not included, the
    /// key_parts will be inferred from the named object.
    pub key_parts: Option<Vec<Expr<T>>>,
    pub with_options: Vec<WithOption>,
    pub if_not_exists: bool,
}

impl<T: AstInfo> AstDisplay for CreateIndexStatement<T> {
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
        if !self.with_options.is_empty() {
            f.write_str(" WITH (");
            f.write_node(&display::comma_separated(&self.with_options));
            f.write_str(")");
        }
    }
}
impl_display_t!(CreateIndexStatement);

/// A `CREATE ROLE` statement.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateRoleStatement {
    /// Whether this was actually a `CREATE USER` statement.
    pub is_user: bool,
    /// The specified role.
    pub name: Ident,
    /// Any options that were attached, in the order they were presented.
    pub options: Vec<CreateRoleOption>,
}

impl AstDisplay for CreateRoleStatement {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("CREATE ");
        if self.is_user {
            f.write_str("USER ");
        } else {
            f.write_str("ROLE ");
        }
        f.write_node(&self.name);
        for option in &self.options {
            f.write_str(" ");
            option.fmt(f)
        }
    }
}
impl_display!(CreateRoleStatement);

/// Options that can be attached to [`CreateRoleStatement`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CreateRoleOption {
    /// The `SUPERUSER` option.
    SuperUser,
    /// The `NOSUPERUSER` option.
    NoSuperUser,
    /// The `LOGIN` option.
    Login,
    /// The `NOLOGIN` option.
    NoLogin,
}

impl AstDisplay for CreateRoleOption {
    fn fmt(&self, f: &mut AstFormatter) {
        match self {
            CreateRoleOption::SuperUser => f.write_str("SUPERUSER"),
            CreateRoleOption::NoSuperUser => f.write_str("NOSUPERUSER"),
            CreateRoleOption::Login => f.write_str("LOGIN"),
            CreateRoleOption::NoLogin => f.write_str("NOLOGIN"),
        }
    }
}
impl_display!(CreateRoleOption);

/// `CREATE TYPE ..`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateTypeStatement<T: AstInfo> {
    /// Name of the created type.
    pub name: UnresolvedObjectName,
    /// The new type's "base type".
    pub as_type: CreateTypeAs,
    /// Provides the name and type for the key
    /// and value.
    pub with_options: Vec<SqlOption<T>>,
}

impl<T: AstInfo> AstDisplay for CreateTypeStatement<T> {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("CREATE TYPE ");
        f.write_node(&self.name);
        f.write_str(" AS ");
        f.write_str(&self.as_type);
        f.write_str("( ");
        if !self.with_options.is_empty() {
            f.write_node(&display::comma_separated(&self.with_options));
        }
        f.write_str(" )");
    }
}
impl_display_t!(CreateTypeStatement);

/// `CREATE TYPE .. AS <TYPE>`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CreateTypeAs {
    List,
    Map,
}

impl AstDisplay for CreateTypeAs {
    fn fmt(&self, f: &mut AstFormatter) {
        match self {
            CreateTypeAs::List => f.write_str("LIST "),
            CreateTypeAs::Map => f.write_str("MAP "),
        }
    }
}
impl_display!(CreateTypeAs);

/// `ALTER <OBJECT> ... RENAME TO`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AlterObjectRenameStatement {
    pub object_type: ObjectType,
    pub if_exists: bool,
    pub name: UnresolvedObjectName,
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
pub enum AlterIndexOptionsList {
    Set(Vec<WithOption>),
    Reset(Vec<Ident>),
}

/// `ALTER INDEX ... {RESET, SET}`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AlterIndexOptionsStatement {
    pub index_name: UnresolvedObjectName,
    pub if_exists: bool,
    pub options: AlterIndexOptionsList,
}

impl AstDisplay for AlterIndexOptionsStatement {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("ALTER INDEX ");
        if self.if_exists {
            f.write_str("IF EXISTS ");
        }
        f.write_node(&self.index_name);
        f.write_str(" ");

        match &self.options {
            AlterIndexOptionsList::Set(options) => {
                f.write_str("SET (");
                f.write_node(&display::comma_separated(&options));
                f.write_str(")");
            }
            AlterIndexOptionsList::Reset(options) => {
                f.write_str("RESET (");
                f.write_node(&display::comma_separated(&options));
                f.write_str(")");
            }
        }
    }
}

impl_display!(AlterIndexOptionsStatement);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DiscardStatement {
    pub target: DiscardTarget,
}

impl AstDisplay for DiscardStatement {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("DISCARD ");
        f.write_node(&self.target);
    }
}
impl_display!(DiscardStatement);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DiscardTarget {
    Plans,
    Sequences,
    Temp,
    All,
}

impl AstDisplay for DiscardTarget {
    fn fmt(&self, f: &mut AstFormatter) {
        match self {
            DiscardTarget::Plans => f.write_str("PLANS"),
            DiscardTarget::Sequences => f.write_str("SEQUENCES"),
            DiscardTarget::Temp => f.write_str("TEMP"),
            DiscardTarget::All => f.write_str("ALL"),
        }
    }
}
impl_display!(DiscardTarget);

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
    pub names: Vec<UnresolvedObjectName>,
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
pub struct ShowDatabasesStatement<T: AstInfo> {
    pub filter: Option<ShowStatementFilter<T>>,
}

impl<T: AstInfo> AstDisplay for ShowDatabasesStatement<T> {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("SHOW DATABASES");
        if let Some(filter) = &self.filter {
            f.write_str(" ");
            f.write_node(filter);
        }
    }
}
impl_display_t!(ShowDatabasesStatement);

/// `SHOW <object>S`
///
/// ```sql
/// SHOW TABLES;
/// SHOW SOURCES;
/// SHOW VIEWS;
/// SHOW SINKS;
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShowObjectsStatement<T: AstInfo> {
    pub object_type: ObjectType,
    pub from: Option<UnresolvedObjectName>,
    pub extended: bool,
    pub full: bool,
    pub materialized: bool,
    pub filter: Option<ShowStatementFilter<T>>,
}

impl<T: AstInfo> AstDisplay for ShowObjectsStatement<T> {
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
            ObjectType::Type => "TYPES",
            ObjectType::Role => "ROLES",
            ObjectType::Object => "OBJECTS",
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
impl_display_t!(ShowObjectsStatement);

/// `SHOW INDEX|INDEXES|KEYS`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShowIndexesStatement<T: AstInfo> {
    pub table_name: UnresolvedObjectName,
    pub extended: bool,
    pub filter: Option<ShowStatementFilter<T>>,
}

impl<T: AstInfo> AstDisplay for ShowIndexesStatement<T> {
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
impl_display_t!(ShowIndexesStatement);

/// `SHOW COLUMNS`
///
/// Note: this is a MySQL-specific statement.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShowColumnsStatement<T: AstInfo> {
    pub extended: bool,
    pub full: bool,
    pub table_name: UnresolvedObjectName,
    pub filter: Option<ShowStatementFilter<T>>,
}

impl<T: AstInfo> AstDisplay for ShowColumnsStatement<T> {
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
impl_display_t!(ShowColumnsStatement);

/// `SHOW CREATE VIEW <view>`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShowCreateViewStatement {
    pub view_name: UnresolvedObjectName,
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
    pub source_name: UnresolvedObjectName,
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
    pub table_name: UnresolvedObjectName,
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
    pub sink_name: UnresolvedObjectName,
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
    pub index_name: UnresolvedObjectName,
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
pub struct TailStatement<T: AstInfo> {
    pub name: UnresolvedObjectName,
    pub options: Vec<WithOption>,
    pub as_of: Option<Expr<T>>,
}

impl<T: AstInfo> AstDisplay for TailStatement<T> {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("TAIL ");
        f.write_node(&self.name);
        if !self.options.is_empty() {
            f.write_str(" WITH (");
            f.write_node(&display::comma_separated(&self.options));
            f.write_str(")");
        }
        if let Some(as_of) = &self.as_of {
            f.write_str(" AS OF ");
            f.write_node(as_of);
        }
    }
}
impl_display_t!(TailStatement);

/// `EXPLAIN ...`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExplainStatement<T: AstInfo> {
    pub stage: ExplainStage,
    pub explainee: Explainee<T>,
    pub options: ExplainOptions,
}

impl<T: AstInfo> AstDisplay for ExplainStatement<T> {
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
impl_display_t!(ExplainStatement);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum InsertSource<T: AstInfo> {
    Query(Query<T>),
    DefaultValues,
}

impl<T: AstInfo> AstDisplay for InsertSource<T> {
    fn fmt(&self, f: &mut AstFormatter) {
        match self {
            InsertSource::Query(query) => f.write_node(query),
            InsertSource::DefaultValues => f.write_str("DEFAULT VALUES"),
        }
    }
}
impl_display_t!(InsertSource);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub enum ObjectType {
    Schema,
    Table,
    View,
    Source,
    Sink,
    Index,
    Type,
    Role,
    Object,
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
            ObjectType::Type => "TYPE",
            ObjectType::Role => "ROLE",
            ObjectType::Object => "OBJECT",
        })
    }
}
impl_display!(ObjectType);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ShowStatementFilter<T: AstInfo> {
    Like(String),
    Where(Expr<T>),
}

impl<T: AstInfo> AstDisplay for ShowStatementFilter<T> {
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
impl_display_t!(ShowStatementFilter);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SqlOption<T: AstInfo> {
    Value {
        name: Ident,
        value: Value,
    },
    ObjectName {
        name: Ident,
        object_name: UnresolvedObjectName,
    },
    DataType {
        name: Ident,
        data_type: DataType<T>,
    },
}

impl<T: AstInfo> SqlOption<T> {
    pub fn name(&self) -> &Ident {
        match self {
            SqlOption::Value { name, .. } => name,
            SqlOption::ObjectName { name, .. } => name,
            SqlOption::DataType { name, .. } => name,
        }
    }
}

impl<T: AstInfo> AstDisplay for SqlOption<T> {
    fn fmt(&self, f: &mut AstFormatter) {
        match self {
            SqlOption::Value { name, value } => {
                f.write_node(name);
                f.write_str(" = ");
                f.write_node(value);
            }
            SqlOption::ObjectName { name, object_name } => {
                f.write_node(name);
                f.write_str(" = ");
                f.write_node(object_name);
            }
            SqlOption::DataType { name, data_type } => {
                f.write_node(name);
                f.write_str(" = ");
                f.write_node(data_type);
            }
        }
    }
}
impl_display_t!(SqlOption);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WithOption {
    pub key: Ident,
    pub value: Option<WithOptionValue>,
}

impl AstDisplay for WithOption {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_node(&self.key);
        if let Some(opt) = &self.value {
            f.write_str(" = ");
            f.write_node(opt);
        }
    }
}
impl_display!(WithOption);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WithOptionValue {
    Value(Value),
    ObjectName(UnresolvedObjectName),
}

impl AstDisplay for WithOptionValue {
    fn fmt(&self, f: &mut AstFormatter) {
        match self {
            WithOptionValue::Value(value) => f.write_node(value),
            WithOptionValue::ObjectName(name) => f.write_node(name),
        }
    }
}
impl_display!(WithOptionValue);

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
pub struct Assignment<T: AstInfo> {
    pub id: Ident,
    pub value: Expr<T>,
}

impl<T: AstInfo> AstDisplay for Assignment<T> {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_node(&self.id);
        f.write_str(" = ");
        f.write_node(&self.value);
    }
}
impl_display_t!(Assignment);

/// Specifies what [Statement::Explain] is actually explaining
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExplainStage {
    /// The sql::HirRelationExpr after parsing
    RawPlan,
    /// The expr::MirRelationExpr after decorrelation
    DecorrelatedPlan,
    /// The expr::MirRelationExpr after optimization
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
pub enum Explainee<T: AstInfo> {
    View(UnresolvedObjectName),
    Query(Query<T>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExplainOptions {
    pub typed: bool,
}

impl<T: AstInfo> AstDisplay for Explainee<T> {
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
impl_display_t!(Explainee);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IfExistsBehavior {
    Error,
    Skip,
    Replace,
}

/// `DECLARE ...`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DeclareStatement<T: AstInfo> {
    pub name: Ident,
    pub stmt: Box<Statement<T>>,
}

impl<T: AstInfo> AstDisplay for DeclareStatement<T> {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("DECLARE ");
        f.write_node(&self.name);
        f.write_str(" CURSOR FOR ");
        f.write_node(&self.stmt);
    }
}
impl_display_t!(DeclareStatement);

/// `CLOSE ...`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CloseStatement {
    pub name: Ident,
}

impl AstDisplay for CloseStatement {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("CLOSE ");
        f.write_node(&self.name);
    }
}
impl_display!(CloseStatement);

/// `FETCH ...`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FetchStatement {
    pub name: Ident,
    pub count: Option<FetchDirection>,
    pub options: Vec<WithOption>,
}

impl AstDisplay for FetchStatement {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("FETCH ");
        if let Some(ref count) = self.count {
            f.write_str(format!("{} ", count));
        }
        f.write_node(&self.name);
        if !self.options.is_empty() {
            f.write_str(" WITH (");
            f.write_node(&display::comma_separated(&self.options));
            f.write_str(")");
        }
    }
}
impl_display!(FetchStatement);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FetchDirection {
    ForwardAll,
    ForwardCount(u64),
}

impl AstDisplay for FetchDirection {
    fn fmt(&self, f: &mut AstFormatter) {
        match self {
            FetchDirection::ForwardAll => f.write_str("ALL"),
            FetchDirection::ForwardCount(count) => f.write_str(format!("{}", count)),
        }
    }
}
impl_display!(FetchDirection);
