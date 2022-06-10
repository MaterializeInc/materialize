// Copyright 2018 sqlparser-rs contributors. All rights reserved.
// Copyright Materialize, Inc. and contributors. All rights reserved.
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

use std::fmt;

use crate::ast::display::{self, AstDisplay, AstFormatter};
use crate::ast::{
    AstInfo, ColumnDef, CreateConnector, CreateSinkConnector, CreateSourceConnector,
    CreateSourceFormat, Envelope, Expr, Format, Ident, KeyConstraint, Query, SourceIncludeMetadata,
    TableAlias, TableConstraint, TableWithJoins, UnresolvedDatabaseName, UnresolvedObjectName,
    UnresolvedSchemaName, Value,
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
    CreateConnector(CreateConnectorStatement<T>),
    CreateDatabase(CreateDatabaseStatement),
    CreateSchema(CreateSchemaStatement),
    CreateSource(CreateSourceStatement<T>),
    CreateSink(CreateSinkStatement<T>),
    CreateView(CreateViewStatement<T>),
    CreateViews(CreateViewsStatement<T>),
    CreateTable(CreateTableStatement<T>),
    CreateIndex(CreateIndexStatement<T>),
    CreateType(CreateTypeStatement<T>),
    CreateRole(CreateRoleStatement),
    CreateCluster(CreateClusterStatement<T>),
    CreateClusterReplica(CreateClusterReplicaStatement<T>),
    CreateSecret(CreateSecretStatement<T>),
    AlterObjectRename(AlterObjectRenameStatement),
    AlterIndex(AlterIndexStatement<T>),
    AlterSecret(AlterSecretStatement<T>),
    Discard(DiscardStatement),
    DropDatabase(DropDatabaseStatement),
    DropSchema(DropSchemaStatement),
    DropObjects(DropObjectsStatement),
    DropRoles(DropRolesStatement),
    DropClusters(DropClustersStatement),
    DropClusterReplicas(DropClusterReplicasStatement),
    SetVariable(SetVariableStatement),
    ResetVariable(ResetVariableStatement),
    ShowDatabases(ShowDatabasesStatement<T>),
    ShowSchemas(ShowSchemasStatement<T>),
    ShowObjects(ShowObjectsStatement<T>),
    ShowIndexes(ShowIndexesStatement<T>),
    ShowColumns(ShowColumnsStatement<T>),
    ShowCreateView(ShowCreateViewStatement<T>),
    ShowCreateSource(ShowCreateSourceStatement<T>),
    ShowCreateTable(ShowCreateTableStatement<T>),
    ShowCreateSink(ShowCreateSinkStatement<T>),
    ShowCreateIndex(ShowCreateIndexStatement<T>),
    ShowCreateConnector(ShowCreateConnectorStatement<T>),
    ShowVariable(ShowVariableStatement),
    StartTransaction(StartTransactionStatement),
    SetTransaction(SetTransactionStatement),
    Commit(CommitStatement),
    Rollback(RollbackStatement),
    Tail(TailStatement<T>),
    Explain(ExplainStatement<T>),
    Declare(DeclareStatement<T>),
    Fetch(FetchStatement<T>),
    Close(CloseStatement),
    Prepare(PrepareStatement<T>),
    Execute(ExecuteStatement<T>),
    Deallocate(DeallocateStatement),
    Raise(RaiseStatement),
}

impl<T: AstInfo> AstDisplay for Statement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            Statement::Select(stmt) => f.write_node(stmt),
            Statement::Insert(stmt) => f.write_node(stmt),
            Statement::Copy(stmt) => f.write_node(stmt),
            Statement::Update(stmt) => f.write_node(stmt),
            Statement::Delete(stmt) => f.write_node(stmt),
            Statement::CreateConnector(stmt) => f.write_node(stmt),
            Statement::CreateDatabase(stmt) => f.write_node(stmt),
            Statement::CreateSchema(stmt) => f.write_node(stmt),
            Statement::CreateSource(stmt) => f.write_node(stmt),
            Statement::CreateSink(stmt) => f.write_node(stmt),
            Statement::CreateView(stmt) => f.write_node(stmt),
            Statement::CreateViews(stmt) => f.write_node(stmt),
            Statement::CreateTable(stmt) => f.write_node(stmt),
            Statement::CreateIndex(stmt) => f.write_node(stmt),
            Statement::CreateRole(stmt) => f.write_node(stmt),
            Statement::CreateSecret(stmt) => f.write_node(stmt),
            Statement::CreateType(stmt) => f.write_node(stmt),
            Statement::CreateCluster(stmt) => f.write_node(stmt),
            Statement::CreateClusterReplica(stmt) => f.write_node(stmt),
            Statement::AlterObjectRename(stmt) => f.write_node(stmt),
            Statement::AlterIndex(stmt) => f.write_node(stmt),
            Statement::AlterSecret(stmt) => f.write_node(stmt),
            Statement::Discard(stmt) => f.write_node(stmt),
            Statement::DropDatabase(stmt) => f.write_node(stmt),
            Statement::DropSchema(stmt) => f.write_node(stmt),
            Statement::DropObjects(stmt) => f.write_node(stmt),
            Statement::DropRoles(stmt) => f.write_node(stmt),
            Statement::DropClusters(stmt) => f.write_node(stmt),
            Statement::DropClusterReplicas(stmt) => f.write_node(stmt),
            Statement::SetVariable(stmt) => f.write_node(stmt),
            Statement::ResetVariable(stmt) => f.write_node(stmt),
            Statement::ShowDatabases(stmt) => f.write_node(stmt),
            Statement::ShowSchemas(stmt) => f.write_node(stmt),
            Statement::ShowObjects(stmt) => f.write_node(stmt),
            Statement::ShowIndexes(stmt) => f.write_node(stmt),
            Statement::ShowColumns(stmt) => f.write_node(stmt),
            Statement::ShowCreateView(stmt) => f.write_node(stmt),
            Statement::ShowCreateSource(stmt) => f.write_node(stmt),
            Statement::ShowCreateTable(stmt) => f.write_node(stmt),
            Statement::ShowCreateSink(stmt) => f.write_node(stmt),
            Statement::ShowCreateIndex(stmt) => f.write_node(stmt),
            Statement::ShowCreateConnector(stmt) => f.write_node(stmt),
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
            Statement::Prepare(stmt) => f.write_node(stmt),
            Statement::Execute(stmt) => f.write_node(stmt),
            Statement::Deallocate(stmt) => f.write_node(stmt),
            Statement::Raise(stmt) => f.write_node(stmt),
        }
    }
}
impl_display_t!(Statement);

/// `SELECT`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SelectStatement<T: AstInfo> {
    pub query: Query<T>,
    pub as_of: Option<AsOf<T>>,
}

impl<T: AstInfo> AstDisplay for SelectStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_node(&self.query);
        if let Some(as_of) = &self.as_of {
            f.write_str(" ");
            f.write_node(as_of);
        }
    }
}
impl_display_t!(SelectStatement);

/// `INSERT`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct InsertStatement<T: AstInfo> {
    /// TABLE
    pub table_name: T::ObjectName,
    /// COLUMNS
    pub columns: Vec<Ident>,
    /// A SQL query that specifies what to insert.
    pub source: InsertSource<T>,
}

impl<T: AstInfo> AstDisplay for InsertStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
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
        name: T::ObjectName,
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
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
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
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str(match self {
            CopyTarget::Stdin => "STDIN",
            CopyTarget::Stdout => "STDOUT",
        })
    }
}
impl_display!(CopyTarget);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CopyOptionName {
    Format,
    Delimiter,
    Null,
    Escape,
    Quote,
    Header,
}

impl AstDisplay for CopyOptionName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str(match self {
            CopyOptionName::Format => "FORMAT",
            CopyOptionName::Delimiter => "DELIMITER",
            CopyOptionName::Null => "NULL",
            CopyOptionName::Escape => "ESCAPE",
            CopyOptionName::Quote => "QUOTE",
            CopyOptionName::Header => "HEADER",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CopyOption<T: AstInfo> {
    pub name: CopyOptionName,
    pub value: Option<WithOptionValue<T>>,
}

impl<T: AstInfo> AstDisplay for CopyOption<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_node(&self.name);
        if let Some(v) = &self.value {
            f.write_str(" = ");
            f.write_node(v);
        }
    }
}

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
    pub options: Vec<CopyOption<T>>,
}

impl<T: AstInfo> AstDisplay for CopyStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("COPY ");
        match &self.relation {
            CopyRelation::Table { name, columns } => {
                f.write_node(name);
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
    /// `FROM`
    pub table_name: T::ObjectName,
    /// Column assignments
    pub assignments: Vec<Assignment<T>>,
    /// WHERE
    pub selection: Option<Expr<T>>,
}

impl<T: AstInfo> AstDisplay for UpdateStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
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
    pub table_name: T::ObjectName,
    /// `AS`
    pub alias: Option<TableAlias>,
    /// `USING`
    pub using: Vec<TableWithJoins<T>>,
    /// `WHERE`
    pub selection: Option<Expr<T>>,
}

impl<T: AstInfo> AstDisplay for DeleteStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("DELETE FROM ");
        f.write_node(&self.table_name);
        if let Some(alias) = &self.alias {
            f.write_str(" AS ");
            f.write_node(alias);
        }
        if !self.using.is_empty() {
            f.write_str(" USING ");
            f.write_node(&display::comma_separated(&self.using));
        }
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
    pub name: UnresolvedDatabaseName,
    pub if_not_exists: bool,
}

impl AstDisplay for CreateDatabaseStatement {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
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
    pub name: UnresolvedSchemaName,
    pub if_not_exists: bool,
}

impl AstDisplay for CreateSchemaStatement {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("CREATE SCHEMA ");
        if self.if_not_exists {
            f.write_str("IF NOT EXISTS ");
        }
        f.write_node(&self.name);
    }
}
impl_display!(CreateSchemaStatement);

/// `CREATE CONNECTOR`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateConnectorStatement<T: AstInfo> {
    pub name: UnresolvedObjectName,
    pub connector: CreateConnector<T>,
    pub if_not_exists: bool,
}

impl<T: AstInfo> AstDisplay for CreateConnectorStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("CREATE CONNECTOR ");
        if self.if_not_exists {
            f.write_str("IF NOT EXISTS ");
        }
        f.write_node(&self.name);
        f.write_str(" FOR ");
        self.connector.fmt(f)
    }
}
impl_display_t!(CreateConnectorStatement);

/// `CREATE SOURCE`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateSourceStatement<T: AstInfo> {
    pub name: UnresolvedObjectName,
    pub col_names: Vec<Ident>,
    pub connector: CreateSourceConnector<T>,
    pub with_options: Vec<WithOption<T>>,
    pub include_metadata: Vec<SourceIncludeMetadata>,
    pub format: CreateSourceFormat<T>,
    pub envelope: Option<Envelope<T>>,
    pub if_not_exists: bool,
    pub materialized: bool,
    pub key_constraint: Option<KeyConstraint>,
}

impl<T: AstInfo> AstDisplay for CreateSourceStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
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
            if self.key_constraint.is_some() {
                f.write_str(", ");
                f.write_node(self.key_constraint.as_ref().unwrap());
            }
            f.write_str(") ");
        } else if self.key_constraint.is_some() {
            f.write_str("(");
            f.write_node(self.key_constraint.as_ref().unwrap());
            f.write_str(") ")
        }
        f.write_str("FROM ");
        f.write_node(&self.connector);
        if !self.with_options.is_empty() {
            f.write_str(" WITH (");
            f.write_node(&display::comma_separated(&self.with_options));
            f.write_str(")");
        }
        f.write_node(&self.format);
        if !self.include_metadata.is_empty() {
            f.write_str(" INCLUDE ");
            f.write_node(&display::comma_separated(&self.include_metadata));
        }

        match &self.envelope {
            None => (),
            Some(Envelope::None) => (),
            Some(envelope) => {
                f.write_str(" ENVELOPE ");
                f.write_node(envelope);
            }
        }
    }
}
impl_display_t!(CreateSourceStatement);

/// `CREATE SINK`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateSinkStatement<T: AstInfo> {
    pub name: UnresolvedObjectName,
    pub in_cluster: Option<T::ClusterName>,
    pub from: T::ObjectName,
    pub connector: CreateSinkConnector<T>,
    pub with_options: Vec<WithOption<T>>,
    pub format: Option<Format<T>>,
    pub envelope: Option<Envelope<T>>,
    pub with_snapshot: bool,
    pub as_of: Option<AsOf<T>>,
    pub if_not_exists: bool,
}

impl<T: AstInfo> AstDisplay for CreateSinkStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("CREATE SINK ");
        if self.if_not_exists {
            f.write_str("IF NOT EXISTS ");
        }
        f.write_node(&self.name);
        if let Some(cluster) = &self.in_cluster {
            f.write_str(" IN CLUSTER ");
            f.write_node(cluster);
        }
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
            f.write_str(" ");
            f.write_node(as_of);
        }
    }
}
impl_display_t!(CreateSinkStatement);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ViewDefinition<T: AstInfo> {
    /// View name
    pub name: UnresolvedObjectName,
    pub columns: Vec<Ident>,
    pub with_options: Vec<WithOption<T>>,
    pub query: Query<T>,
}

impl<T: AstInfo> AstDisplay for ViewDefinition<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
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
impl_display_t!(ViewDefinition);

/// `CREATE VIEW`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateViewStatement<T: AstInfo> {
    pub if_exists: IfExistsBehavior,
    pub temporary: bool,
    pub materialized: bool,
    pub definition: ViewDefinition<T>,
}

impl<T: AstInfo> AstDisplay for CreateViewStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
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
        f.write_node(&self.definition);
    }
}
impl_display_t!(CreateViewStatement);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateViewsSourceTarget {
    pub name: UnresolvedObjectName,
    pub alias: Option<UnresolvedObjectName>,
}

impl AstDisplay for CreateViewsSourceTarget {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_node(&self.name);
        if let Some(alias) = &self.alias {
            f.write_str(" AS ");
            f.write_node(alias);
        }
    }
}
impl_display!(CreateViewsSourceTarget);

/// `CREATE VIEWS`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateViewsStatement<T: AstInfo> {
    pub if_exists: IfExistsBehavior,
    pub temporary: bool,
    pub materialized: bool,
    pub source: T::ObjectName,
    pub targets: Option<Vec<CreateViewsSourceTarget>>,
}

impl<T: AstInfo> AstDisplay for CreateViewsStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
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

        f.write_str(" VIEWS");

        if self.if_exists == IfExistsBehavior::Skip {
            f.write_str(" IF NOT EXISTS");
        }

        f.write_str(" FROM SOURCE ");
        f.write_node(&self.source);
        if let Some(targets) = &self.targets {
            f.write_str(" (");
            f.write_node(&display::comma_separated(&targets));
            f.write_str(")");
        }
    }
}
impl_display_t!(CreateViewsStatement);

/// `CREATE TABLE`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateTableStatement<T: AstInfo> {
    /// Table name
    pub name: UnresolvedObjectName,
    /// Optional schema
    pub columns: Vec<ColumnDef<T>>,
    pub constraints: Vec<TableConstraint<T>>,
    pub with_options: Vec<WithOption<T>>,
    pub if_not_exists: bool,
    pub temporary: bool,
}

impl<T: AstInfo> AstDisplay for CreateTableStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
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
    pub in_cluster: Option<T::ClusterName>,
    /// `ON` table or view name
    pub on_name: T::ObjectName,
    /// Expressions that form part of the index key. If not included, the
    /// key_parts will be inferred from the named object.
    pub key_parts: Option<Vec<Expr<T>>>,
    pub with_options: Vec<IndexOption<T>>,
    pub if_not_exists: bool,
}

impl<T: AstInfo> AstDisplay for CreateIndexStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
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
        if let Some(cluster) = &self.in_cluster {
            f.write_str("IN CLUSTER ");
            f.write_node(cluster);
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

/// An option in a `CREATE CLUSTER` statement.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IndexOptionName {
    // The `LOGICAL COMPACTION WINDOW` option
    LogicalCompactionWindow,
}

impl AstDisplay for IndexOptionName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            IndexOptionName::LogicalCompactionWindow => {
                f.write_str("LOGICAL COMPACTION WINDOW");
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexOption<T: AstInfo> {
    pub name: IndexOptionName,
    pub value: Option<WithOptionValue<T>>,
}

impl<T: AstInfo> AstDisplay for IndexOption<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_node(&self.name);
        if let Some(v) = &self.value {
            f.write_str(" = ");
            f.write_node(v);
        }
    }
}

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
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
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
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            CreateRoleOption::SuperUser => f.write_str("SUPERUSER"),
            CreateRoleOption::NoSuperUser => f.write_str("NOSUPERUSER"),
            CreateRoleOption::Login => f.write_str("LOGIN"),
            CreateRoleOption::NoLogin => f.write_str("NOLOGIN"),
        }
    }
}
impl_display!(CreateRoleOption);

/// A `CREATE SECRET` statement.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateSecretStatement<T: AstInfo> {
    pub name: UnresolvedObjectName,
    pub if_not_exists: bool,
    pub value: Expr<T>,
}

impl<T: AstInfo> AstDisplay for CreateSecretStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("CREATE SECRET ");
        if self.if_not_exists {
            f.write_str("IF NOT EXISTS ");
        }
        f.write_node(&self.name);
        f.write_str(" AS ");
        f.write_node(&self.value);
    }
}
impl_display_t!(CreateSecretStatement);

/// `CREATE TYPE ..`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateTypeStatement<T: AstInfo> {
    /// Name of the created type.
    pub name: UnresolvedObjectName,
    /// The new type's "base type".
    pub as_type: CreateTypeAs<T>,
}

impl<T: AstInfo> AstDisplay for CreateTypeStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("CREATE TYPE ");
        f.write_node(&self.name);
        f.write_str(" AS ");
        match &self.as_type {
            CreateTypeAs::List { with_options } | CreateTypeAs::Map { with_options } => {
                f.write_str(&self.as_type);
                f.write_str("( ");
                if !with_options.is_empty() {
                    f.write_node(&display::comma_separated(&with_options));
                }
                f.write_str(" )");
            }
            CreateTypeAs::Record { column_defs } => {
                f.write_str("( ");
                if !column_defs.is_empty() {
                    f.write_node(&display::comma_separated(&column_defs));
                }
                f.write_str(" )");
            }
        };
    }
}
impl_display_t!(CreateTypeStatement);

/// `CREATE CLUSTER ..`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateClusterStatement<T: AstInfo> {
    /// Name of the created cluster.
    pub name: Ident,
    /// The comma-separated options.
    pub options: Vec<ClusterOption<T>>,
}

impl<T: AstInfo> AstDisplay for CreateClusterStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("CREATE CLUSTER ");
        f.write_node(&self.name);
        if !self.options.is_empty() {
            f.write_str(" ");
            f.write_node(&display::comma_separated(&self.options));
        }
    }
}
impl_display_t!(CreateClusterStatement);

/// An option in a `CREATE CLUSTER` statement.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ClusterOption<T: AstInfo> {
    /// The `INTROSPECTION GRANULARITY [[=] <interval>] option.
    IntrospectionGranularity(WithOptionValue<T>),
    /// The `INTROSPECTION DEBUGGING [[=] <enabled>] option.
    IntrospectionDebugging(WithOptionValue<T>),
    /// The `REPLICAS` option.
    Replicas(Vec<ReplicaDefinition<T>>),
}

impl<T: AstInfo> AstDisplay for ClusterOption<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            ClusterOption::IntrospectionGranularity(granularity) => {
                f.write_str("INTROSPECTION GRANULARITY ");
                f.write_node(granularity);
            }
            ClusterOption::IntrospectionDebugging(debugging) => {
                f.write_str("INTROSPECTION DEBUGGING ");
                f.write_node(debugging);
            }
            ClusterOption::Replicas(replicas) => {
                f.write_str("REPLICAS (");
                f.write_node(&display::comma_separated(&replicas));
                f.write_str(")");
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReplicaDefinition<T: AstInfo> {
    /// Name of the created replica.
    pub name: Ident,
    /// The comma-separated options.
    pub options: Vec<ReplicaOption<T>>,
}

// Note that this display is meant for replicas defined inline when creating
// clusters.
impl<T: AstInfo> AstDisplay for ReplicaDefinition<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_node(&self.name);
        f.write_str(" (");
        f.write_node(&display::comma_separated(&self.options));
        f.write_str(")");
    }
}
impl_display_t!(ReplicaDefinition);

/// `CREATE CLUSTER REPLICA ..`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateClusterReplicaStatement<T: AstInfo> {
    /// Name of the replica's cluster.
    pub of_cluster: Ident,
    /// The replica's definition.
    pub definition: ReplicaDefinition<T>,
}

impl<T: AstInfo> AstDisplay for CreateClusterReplicaStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("CREATE CLUSTER REPLICA ");
        f.write_node(&self.of_cluster);
        f.write_str(".");
        f.write_node(&self.definition.name);
        f.write_str(" ");
        f.write_node(&display::comma_separated(&self.definition.options));
    }
}
impl_display_t!(CreateClusterReplicaStatement);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ReplicaOptionName {
    /// The `REMOTE [<host> [, <host> ...]]` option.
    Remote,
    /// The `SIZE [[=] <size>]` option.
    Size,
    /// The `AVAILABILITY ZONE [[=] <size>]` option.
    AvailabilityZone,
}

impl AstDisplay for ReplicaOptionName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            ReplicaOptionName::Remote => f.write_str("REMOTE"),
            ReplicaOptionName::Size => f.write_str("SIZE"),
            ReplicaOptionName::AvailabilityZone => f.write_str("AVAILABILITY ZONE"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
/// An option in a `CREATE CLUSTER` or `CREATE CLUSTER REPLICA` statement.
pub struct ReplicaOption<T: AstInfo> {
    pub name: ReplicaOptionName,
    pub value: Option<WithOptionValue<T>>,
}

impl<T: AstInfo> AstDisplay for ReplicaOption<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_node(&self.name);
        if let Some(v) = &self.value {
            f.write_str(" = ");
            f.write_node(v);
        }
    }
}

/// `CREATE TYPE .. AS <TYPE>`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CreateTypeAs<T: AstInfo> {
    List { with_options: Vec<WithOption<T>> },
    Map { with_options: Vec<WithOption<T>> },
    Record { column_defs: Vec<ColumnDef<T>> },
}

impl<T: AstInfo> AstDisplay for CreateTypeAs<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            CreateTypeAs::List { .. } => f.write_str("LIST "),
            CreateTypeAs::Map { .. } => f.write_str("MAP "),
            CreateTypeAs::Record { .. } => f.write_str("RECORD "),
        }
    }
}
impl_display_t!(CreateTypeAs);

/// `ALTER <OBJECT> ... RENAME TO`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AlterObjectRenameStatement {
    pub object_type: ObjectType,
    pub if_exists: bool,
    pub name: UnresolvedObjectName,
    pub to_item_name: Ident,
}

impl AstDisplay for AlterObjectRenameStatement {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
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
pub enum AlterIndexAction<T: AstInfo> {
    SetOptions(Vec<IndexOption<T>>),
    ResetOptions(Vec<IndexOptionName>),
}

/// `ALTER INDEX ... {RESET, SET}`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AlterIndexStatement<T: AstInfo> {
    pub index_name: UnresolvedObjectName,
    pub if_exists: bool,
    pub action: AlterIndexAction<T>,
}

impl<T: AstInfo> AstDisplay for AlterIndexStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("ALTER INDEX ");
        if self.if_exists {
            f.write_str("IF EXISTS ");
        }
        f.write_node(&self.index_name);
        f.write_str(" ");

        match &self.action {
            AlterIndexAction::SetOptions(options) => {
                f.write_str("SET (");
                f.write_node(&display::comma_separated(&options));
                f.write_str(")");
            }
            AlterIndexAction::ResetOptions(options) => {
                f.write_str("RESET (");
                f.write_node(&display::comma_separated(&options));
                f.write_str(")");
            }
        }
    }
}

impl_display_t!(AlterIndexStatement);

/// `ALTER SECRET ... AS`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AlterSecretStatement<T: AstInfo> {
    pub name: UnresolvedObjectName,
    pub if_exists: bool,
    pub value: Expr<T>,
}

impl<T: AstInfo> AstDisplay for AlterSecretStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("ALTER SECRET ");
        if self.if_exists {
            f.write_str("IF EXISTS ");
        }
        f.write_node(&self.name);
        f.write_str(" AS ");
        f.write_node(&self.value);
    }
}

impl_display_t!(AlterSecretStatement);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DiscardStatement {
    pub target: DiscardTarget,
}

impl AstDisplay for DiscardStatement {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
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
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
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
    pub name: UnresolvedDatabaseName,
    pub if_exists: bool,
    pub restrict: bool,
}

impl AstDisplay for DropDatabaseStatement {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("DROP DATABASE ");
        if self.if_exists {
            f.write_str("IF EXISTS ");
        }
        f.write_node(&self.name);
        if self.restrict {
            f.write_str(" RESTRICT");
        }
    }
}
impl_display!(DropDatabaseStatement);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DropSchemaStatement {
    pub name: UnresolvedSchemaName,
    pub if_exists: bool,
    pub cascade: bool,
}

impl AstDisplay for DropSchemaStatement {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("DROP SCHEMA ");
        if self.if_exists {
            f.write_str("IF EXISTS ");
        }
        f.write_node(&self.name);
        if self.cascade {
            f.write_str(" CASCADE");
        }
    }
}
impl_display!(DropSchemaStatement);

/// `DROP`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DropObjectsStatement {
    /// If this was constructed as `DROP MATERIALIZED <type>`
    pub materialized: bool,
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
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DropRolesStatement {
    /// An optional `IF EXISTS` clause. (Non-standard.)
    pub if_exists: bool,
    /// One or more objects to drop. (ANSI SQL requires exactly one.)
    pub names: Vec<UnresolvedObjectName>,
}

impl AstDisplay for DropRolesStatement {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("DROP ROLE ");
        if self.if_exists {
            f.write_str("IF EXISTS ");
        }
        f.write_node(&display::comma_separated(&self.names));
    }
}
impl_display!(DropRolesStatement);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DropClustersStatement {
    /// An optional `IF EXISTS` clause. (Non-standard.)
    pub if_exists: bool,
    /// One or more objects to drop. (ANSI SQL requires exactly one.)
    pub names: Vec<UnresolvedObjectName>,
    /// Whether `CASCADE` was specified. This will be `false` when
    /// `RESTRICT` or no drop behavior at all was specified.
    pub cascade: bool,
}

impl AstDisplay for DropClustersStatement {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("DROP CLUSTER ");
        if self.if_exists {
            f.write_str("IF EXISTS ");
        }
        f.write_node(&display::comma_separated(&self.names));
        if self.cascade {
            f.write_str(" CASCADE");
        }
    }
}
impl_display!(DropClustersStatement);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QualifiedReplica {
    pub cluster: Ident,
    pub replica: Ident,
}

impl AstDisplay for QualifiedReplica {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_node(&self.cluster);
        f.write_str(".");
        f.write_node(&self.replica);
    }
}
impl_display!(QualifiedReplica);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DropClusterReplicasStatement {
    /// An optional `IF EXISTS` clause. (Non-standard.)
    pub if_exists: bool,
    /// One or more objects to drop. (ANSI SQL requires exactly one.)
    pub names: Vec<QualifiedReplica>,
}

impl AstDisplay for DropClusterReplicasStatement {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("DROP CLUSTER REPLICA ");
        if self.if_exists {
            f.write_str("IF EXISTS ");
        }
        f.write_node(&display::comma_separated(&self.names));
    }
}
impl_display!(DropClusterReplicasStatement);

/// `SET <variable>`
///
/// Note: this is not a standard SQL statement, but it is supported by at
/// least MySQL and PostgreSQL. Not all MySQL-specific syntactic forms are
/// supported yet.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SetVariableStatement {
    pub local: bool,
    pub variable: Ident,
    pub value: SetVariableValue,
}

impl AstDisplay for SetVariableStatement {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
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

/// `RESET <variable>`
///
/// Note: this is not a standard SQL statement, but it is supported by at
/// least MySQL and PostgreSQL. Not all syntactic forms are supported yet.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResetVariableStatement {
    pub variable: Ident,
}

impl AstDisplay for ResetVariableStatement {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("RESET ");
        f.write_node(&self.variable);
    }
}
impl_display!(ResetVariableStatement);

/// `SHOW <variable>`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShowVariableStatement {
    pub variable: Ident,
}

impl AstDisplay for ShowVariableStatement {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
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
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("SHOW DATABASES");
        if let Some(filter) = &self.filter {
            f.write_str(" ");
            f.write_node(filter);
        }
    }
}
impl_display_t!(ShowDatabasesStatement);

/// `SHOW SCHEMAS`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShowSchemasStatement<T: AstInfo> {
    pub from: Option<T::DatabaseName>,
    pub extended: bool,
    pub full: bool,
    pub filter: Option<ShowStatementFilter<T>>,
}

impl<T: AstInfo> AstDisplay for ShowSchemasStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("SHOW");
        if self.extended {
            f.write_str(" EXTENDED");
        }
        if self.full {
            f.write_str(" FULL");
        }
        f.write_str(" SCHEMAS");
        if let Some(from) = &self.from {
            f.write_str(" FROM ");
            f.write_node(from);
        }
        if let Some(filter) = &self.filter {
            f.write_str(" ");
            f.write_node(filter);
        }
    }
}
impl_display_t!(ShowSchemasStatement);

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
    pub from: Option<T::SchemaName>,
    pub in_cluster: Option<T::ClusterName>,
    pub extended: bool,
    pub full: bool,
    pub materialized: bool,
    pub filter: Option<ShowStatementFilter<T>>,
}

impl<T: AstInfo> AstDisplay for ShowObjectsStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
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
            ObjectType::Table => "TABLES",
            ObjectType::View => "VIEWS",
            ObjectType::Source => "SOURCES",
            ObjectType::Sink => "SINKS",
            ObjectType::Type => "TYPES",
            ObjectType::Role => "ROLES",
            ObjectType::Cluster => "CLUSTERS",
            ObjectType::ClusterReplica => "CLUSTER REPLICAS",
            ObjectType::Object => "OBJECTS",
            ObjectType::Secret => "SECRETS",
            ObjectType::Connector => "CONNECTORS",
            ObjectType::Index => unreachable!(),
        });
        if let Some(from) = &self.from {
            f.write_str(" FROM ");
            f.write_node(from);
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
    pub table_name: Option<T::ObjectName>,
    pub in_cluster: Option<T::ClusterName>,
    pub extended: bool,
    pub filter: Option<ShowStatementFilter<T>>,
}

impl<T: AstInfo> AstDisplay for ShowIndexesStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("SHOW ");
        if self.extended {
            f.write_str("EXTENDED ");
        }
        f.write_str("INDEXES ");
        if let Some(table_name) = &self.table_name {
            f.write_str("FROM ");
            f.write_node(table_name);
        }
        if let Some(in_cluster) = &self.in_cluster {
            f.write_str("IN CLUSTER ");
            f.write_node(in_cluster);
        }
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
    pub table_name: T::ObjectName,
    pub filter: Option<ShowStatementFilter<T>>,
}

impl<T: AstInfo> AstDisplay for ShowColumnsStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
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
pub struct ShowCreateViewStatement<T: AstInfo> {
    pub view_name: T::ObjectName,
}

impl<T: AstInfo> AstDisplay for ShowCreateViewStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("SHOW CREATE VIEW ");
        f.write_node(&self.view_name);
    }
}
impl_display_t!(ShowCreateViewStatement);

/// `SHOW CREATE SOURCE <source>`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShowCreateSourceStatement<T: AstInfo> {
    pub source_name: T::ObjectName,
}

impl<T: AstInfo> AstDisplay for ShowCreateSourceStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("SHOW CREATE SOURCE ");
        f.write_node(&self.source_name);
    }
}
impl_display_t!(ShowCreateSourceStatement);

/// `SHOW CREATE TABLE <table>`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShowCreateTableStatement<T: AstInfo> {
    pub table_name: T::ObjectName,
}

impl<T: AstInfo> AstDisplay for ShowCreateTableStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("SHOW CREATE TABLE ");
        f.write_node(&self.table_name);
    }
}
impl_display_t!(ShowCreateTableStatement);

/// `SHOW CREATE SINK <sink>`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShowCreateSinkStatement<T: AstInfo> {
    pub sink_name: T::ObjectName,
}

impl<T: AstInfo> AstDisplay for ShowCreateSinkStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("SHOW CREATE SINK ");
        f.write_node(&self.sink_name);
    }
}
impl_display_t!(ShowCreateSinkStatement);

/// `SHOW CREATE INDEX <index>`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShowCreateIndexStatement<T: AstInfo> {
    pub index_name: T::ObjectName,
}

impl<T: AstInfo> AstDisplay for ShowCreateIndexStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("SHOW CREATE INDEX ");
        f.write_node(&self.index_name);
    }
}
impl_display_t!(ShowCreateIndexStatement);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShowCreateConnectorStatement<T: AstInfo> {
    pub connector_name: T::ObjectName,
}

impl<T: AstInfo> AstDisplay for ShowCreateConnectorStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("SHOW CREATE CONNECTOR ");
        f.write_node(&self.connector_name);
    }
}

/// `{ BEGIN [ TRANSACTION | WORK ] | START TRANSACTION } ...`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StartTransactionStatement {
    pub modes: Vec<TransactionMode>,
}

impl AstDisplay for StartTransactionStatement {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
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
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
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
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
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
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("ROLLBACK");
        if self.chain {
            f.write_str(" AND CHAIN");
        }
    }
}
impl_display!(RollbackStatement);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TailOptionName {
    Snapshot,
    Progress,
}

impl AstDisplay for TailOptionName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            TailOptionName::Snapshot => f.write_str("SNAPSHOT"),
            TailOptionName::Progress => f.write_str("PROGRESS"),
        }
    }
}
impl_display!(TailOptionName);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TailOption<T: AstInfo> {
    pub name: TailOptionName,
    pub value: Option<WithOptionValue<T>>,
}

impl<T: AstInfo> AstDisplay for TailOption<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_node(&self.name);
        if let Some(v) = &self.value {
            f.write_str(" = ");
            f.write_node(v);
        }
    }
}
impl_display_t!(TailOption);

/// `TAIL`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TailStatement<T: AstInfo> {
    pub relation: TailRelation<T>,
    pub options: Vec<TailOption<T>>,
    pub as_of: Option<AsOf<T>>,
}

impl<T: AstInfo> AstDisplay for TailStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("TAIL ");
        f.write_node(&self.relation);
        if !self.options.is_empty() {
            f.write_str(" WITH (");
            f.write_node(&display::comma_separated(&self.options));
            f.write_str(")");
        }
        if let Some(as_of) = &self.as_of {
            f.write_str(" ");
            f.write_node(as_of);
        }
    }
}
impl_display_t!(TailStatement);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TailRelation<T: AstInfo> {
    Name(T::ObjectName),
    Query(Query<T>),
}

impl<T: AstInfo> AstDisplay for TailRelation<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            TailRelation::Name(name) => f.write_node(name),
            TailRelation::Query(query) => {
                f.write_str("(");
                f.write_node(query);
                f.write_str(")");
            }
        }
    }
}
impl_display_t!(TailRelation);

/// `EXPLAIN ...`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExplainStatement<T: AstInfo> {
    pub stage: ExplainStage,
    pub explainee: Explainee<T>,
    pub options: ExplainOptions,
}

impl<T: AstInfo> AstDisplay for ExplainStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("EXPLAIN ");
        if self.options.timing {
            f.write_str("(TIMING ");
            f.write_str(self.options.timing);
            f.write_str(") ");
        }
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
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            InsertSource::Query(query) => f.write_node(query),
            InsertSource::DefaultValues => f.write_str("DEFAULT VALUES"),
        }
    }
}
impl_display_t!(InsertSource);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub enum ObjectType {
    Table,
    View,
    Source,
    Sink,
    Index,
    Type,
    Role,
    Cluster,
    ClusterReplica,
    Object,
    Secret,
    Connector,
}

impl AstDisplay for ObjectType {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str(match self {
            ObjectType::Table => "TABLE",
            ObjectType::View => "VIEW",
            ObjectType::Source => "SOURCE",
            ObjectType::Sink => "SINK",
            ObjectType::Index => "INDEX",
            ObjectType::Type => "TYPE",
            ObjectType::Role => "ROLE",
            ObjectType::Cluster => "CLUSTER",
            ObjectType::ClusterReplica => "CLUSTER REPLICA",
            ObjectType::Object => "OBJECT",
            ObjectType::Secret => "SECRET",
            ObjectType::Connector => "CONNECTOR",
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
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
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
pub struct WithOption<T: AstInfo> {
    pub key: Ident,
    pub value: Option<WithOptionValue<T>>,
}

impl<T: AstInfo> AstDisplay for WithOption<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_node(&self.key);
        if let Some(opt) = &self.value {
            f.write_str(" = ");
            f.write_node(opt);
        }
    }
}
impl_display_t!(WithOption);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WithOptionValue<T: AstInfo> {
    Value(Value),
    Ident(Ident),
    DataType(T::DataType),
    // Temporary variant until we have support for connectors, which will use
    // explicit fields for each secret reference.
    Secret(T::ObjectName),
}

impl<T: AstInfo> AstDisplay for WithOptionValue<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            WithOptionValue::Value(value) => f.write_node(value),
            WithOptionValue::Ident(id) => f.write_node(id),
            WithOptionValue::DataType(typ) => f.write_node(typ),
            WithOptionValue::Secret(name) => {
                f.write_str("SECRET ");
                f.write_node(name)
            }
        }
    }
}
impl_display_t!(WithOptionValue);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TransactionMode {
    AccessMode(TransactionAccessMode),
    IsolationLevel(TransactionIsolationLevel),
}

impl AstDisplay for TransactionMode {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
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
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
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
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
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
    Default,
}

impl AstDisplay for SetVariableValue {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        use SetVariableValue::*;
        match self {
            Ident(ident) => f.write_node(ident),
            Literal(literal) => f.write_node(literal),
            Default => f.write_str("DEFAULT"),
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
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
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
    /// Query Graph
    QueryGraph,
    /// Optimized Query Graph
    OptimizedQueryGraph,
    /// The mz_expr::MirRelationExpr after decorrelation
    DecorrelatedPlan,
    /// The mz_expr::MirRelationExpr after optimization
    OptimizedPlan,
    /// The render::plan::Plan
    PhysicalPlan,
    /// The dependent and selected timestamps
    Timestamp,
}

impl AstDisplay for ExplainStage {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            ExplainStage::RawPlan => f.write_str("RAW PLAN"),
            ExplainStage::OptimizedQueryGraph => f.write_str("OPTIMIZED QUERY GRAPH"),
            ExplainStage::QueryGraph => f.write_str("QUERY GRAPH"),
            ExplainStage::DecorrelatedPlan => f.write_str("DECORRELATED PLAN"),
            ExplainStage::OptimizedPlan => f.write_str("OPTIMIZED PLAN"),
            ExplainStage::PhysicalPlan => f.write_str("PHYSICAL PLAN"),
            ExplainStage::Timestamp => f.write_str("TIMESTAMP"),
        }
    }
}
impl_display!(ExplainStage);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Explainee<T: AstInfo> {
    View(T::ObjectName),
    Query(Query<T>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExplainOptions {
    pub typed: bool,
    pub timing: bool,
}

impl<T: AstInfo> AstDisplay for Explainee<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            Self::View(name) => {
                f.write_str("VIEW ");
                f.write_node(name);
            }
            Self::Query(query) => f.write_node(query),
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
    pub stmt: Box<T::NestedStatement>,
}

impl<T: AstInfo> AstDisplay for DeclareStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
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
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("CLOSE ");
        f.write_node(&self.name);
    }
}
impl_display!(CloseStatement);

/// `FETCH ...`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FetchStatement<T: AstInfo> {
    pub name: Ident,
    pub count: Option<FetchDirection>,
    pub options: Vec<WithOption<T>>,
}

impl<T: AstInfo> AstDisplay for FetchStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
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
impl_display_t!(FetchStatement);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FetchDirection {
    ForwardAll,
    ForwardCount(u64),
}

impl AstDisplay for FetchDirection {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            FetchDirection::ForwardAll => f.write_str("ALL"),
            FetchDirection::ForwardCount(count) => f.write_str(format!("{}", count)),
        }
    }
}
impl_display!(FetchDirection);

/// `PREPARE ...`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PrepareStatement<T: AstInfo> {
    pub name: Ident,
    pub stmt: Box<T::NestedStatement>,
}

impl<T: AstInfo> AstDisplay for PrepareStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("PREPARE ");
        f.write_node(&self.name);
        f.write_str(" AS ");
        f.write_node(&self.stmt);
    }
}
impl_display_t!(PrepareStatement);

/// `EXECUTE ...`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExecuteStatement<T: AstInfo> {
    pub name: Ident,
    pub params: Vec<Expr<T>>,
}

impl<T: AstInfo> AstDisplay for ExecuteStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("EXECUTE ");
        f.write_node(&self.name);
        if !self.params.is_empty() {
            f.write_str(" (");
            f.write_node(&display::comma_separated(&self.params));
            f.write_str(")");
        }
    }
}
impl_display_t!(ExecuteStatement);

/// `DEALLOCATE ...`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DeallocateStatement {
    pub name: Option<Ident>,
}

impl AstDisplay for DeallocateStatement {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("DEALLOCATE ");
        match &self.name {
            Some(name) => f.write_node(name),
            None => f.write_str("ALL"),
        };
    }
}
impl_display!(DeallocateStatement);

/// `RAISE ...`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RaiseStatement {
    pub severity: NoticeSeverity,
}

impl AstDisplay for RaiseStatement {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("RAISE ");
        f.write_node(&self.severity);
    }
}
impl_display!(RaiseStatement);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum NoticeSeverity {
    Debug,
    Info,
    Log,
    Notice,
    Warning,
}

impl AstDisplay for NoticeSeverity {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str(match self {
            NoticeSeverity::Debug => "DEBUG",
            NoticeSeverity::Info => "INFO",
            NoticeSeverity::Log => "LOG",
            NoticeSeverity::Notice => "NOTICE",
            NoticeSeverity::Warning => "WARNING",
        })
    }
}
impl_display!(NoticeSeverity);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AsOf<T: AstInfo> {
    At(Expr<T>),
    AtLeast(Expr<T>),
}

impl<T: AstInfo> AstDisplay for AsOf<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("AS OF ");
        match self {
            AsOf::At(expr) => f.write_node(expr),
            AsOf::AtLeast(expr) => {
                f.write_str("AT LEAST ");
                f.write_node(expr);
            }
        }
    }
}
impl_display_t!(AsOf);
