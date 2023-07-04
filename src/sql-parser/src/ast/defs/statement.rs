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

// `EnumKind` unconditionally introduces a lifetime. TODO: remove this once
// https://github.com/rust-lang/rust-clippy/pull/9037 makes it into stable
#![allow(clippy::extra_unused_lifetimes)]

use std::fmt;

use enum_kinds::EnumKind;
use serde::{Deserialize, Serialize};

use crate::ast::display::{self, AstDisplay, AstFormatter};
use crate::ast::{
    AstInfo, ColumnDef, CreateConnection, CreateConnectionOption, CreateSinkConnection,
    CreateSourceConnection, CreateSourceFormat, CreateSourceOption, CreateSourceOptionName,
    DeferredItemName, Envelope, Expr, Format, Ident, KeyConstraint, Query, SelectItem,
    SourceIncludeMetadata, SubscribeOutput, TableAlias, TableConstraint, TableWithJoins,
    UnresolvedDatabaseName, UnresolvedItemName, UnresolvedObjectName, UnresolvedSchemaName, Value,
};

/// A top-level statement (SELECT, INSERT, CREATE, etc.)
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq, Hash, EnumKind)]
#[enum_kind(StatementKind, derive(Serialize, Deserialize))]
pub enum Statement<T: AstInfo> {
    Select(SelectStatement<T>),
    Insert(InsertStatement<T>),
    Copy(CopyStatement<T>),
    Update(UpdateStatement<T>),
    Delete(DeleteStatement<T>),
    CreateConnection(CreateConnectionStatement<T>),
    CreateDatabase(CreateDatabaseStatement),
    CreateSchema(CreateSchemaStatement),
    CreateSource(CreateSourceStatement<T>),
    CreateSubsource(CreateSubsourceStatement<T>),
    CreateSink(CreateSinkStatement<T>),
    CreateView(CreateViewStatement<T>),
    CreateMaterializedView(CreateMaterializedViewStatement<T>),
    CreateTable(CreateTableStatement<T>),
    CreateIndex(CreateIndexStatement<T>),
    CreateType(CreateTypeStatement<T>),
    CreateRole(CreateRoleStatement),
    CreateCluster(CreateClusterStatement<T>),
    CreateClusterReplica(CreateClusterReplicaStatement<T>),
    CreateSecret(CreateSecretStatement<T>),
    AlterCluster(AlterClusterStatement<T>),
    AlterOwner(AlterOwnerStatement<T>),
    AlterObjectRename(AlterObjectRenameStatement),
    AlterIndex(AlterIndexStatement<T>),
    AlterSecret(AlterSecretStatement<T>),
    AlterSink(AlterSinkStatement<T>),
    AlterSource(AlterSourceStatement<T>),
    AlterSystemSet(AlterSystemSetStatement),
    AlterSystemReset(AlterSystemResetStatement),
    AlterSystemResetAll(AlterSystemResetAllStatement),
    AlterConnection(AlterConnectionStatement),
    AlterRole(AlterRoleStatement<T>),
    Discard(DiscardStatement),
    DropObjects(DropObjectsStatement),
    DropOwned(DropOwnedStatement<T>),
    SetVariable(SetVariableStatement),
    ResetVariable(ResetVariableStatement),
    Show(ShowStatement<T>),
    StartTransaction(StartTransactionStatement),
    SetTransaction(SetTransactionStatement),
    Commit(CommitStatement),
    Rollback(RollbackStatement),
    Subscribe(SubscribeStatement<T>),
    Explain(ExplainStatement<T>),
    Declare(DeclareStatement<T>),
    Fetch(FetchStatement<T>),
    Close(CloseStatement),
    Prepare(PrepareStatement<T>),
    Execute(ExecuteStatement<T>),
    Deallocate(DeallocateStatement),
    Raise(RaiseStatement),
    GrantRole(GrantRoleStatement<T>),
    RevokeRole(RevokeRoleStatement<T>),
    GrantPrivileges(GrantPrivilegesStatement<T>),
    RevokePrivileges(RevokePrivilegesStatement<T>),
    AlterDefaultPrivileges(AlterDefaultPrivilegesStatement<T>),
    ReassignOwned(ReassignOwnedStatement<T>),
    ValidateConnection(ValidateConnectionStatement<T>),
}

impl<T: AstInfo> AstDisplay for Statement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            Statement::Select(stmt) => f.write_node(stmt),
            Statement::Insert(stmt) => f.write_node(stmt),
            Statement::Copy(stmt) => f.write_node(stmt),
            Statement::Update(stmt) => f.write_node(stmt),
            Statement::Delete(stmt) => f.write_node(stmt),
            Statement::CreateConnection(stmt) => f.write_node(stmt),
            Statement::CreateDatabase(stmt) => f.write_node(stmt),
            Statement::CreateSchema(stmt) => f.write_node(stmt),
            Statement::CreateSource(stmt) => f.write_node(stmt),
            Statement::CreateSubsource(stmt) => f.write_node(stmt),
            Statement::CreateSink(stmt) => f.write_node(stmt),
            Statement::CreateView(stmt) => f.write_node(stmt),
            Statement::CreateMaterializedView(stmt) => f.write_node(stmt),
            Statement::CreateTable(stmt) => f.write_node(stmt),
            Statement::CreateIndex(stmt) => f.write_node(stmt),
            Statement::CreateRole(stmt) => f.write_node(stmt),
            Statement::CreateSecret(stmt) => f.write_node(stmt),
            Statement::CreateType(stmt) => f.write_node(stmt),
            Statement::CreateCluster(stmt) => f.write_node(stmt),
            Statement::CreateClusterReplica(stmt) => f.write_node(stmt),
            Statement::AlterCluster(stmt) => f.write_node(stmt),
            Statement::AlterOwner(stmt) => f.write_node(stmt),
            Statement::AlterObjectRename(stmt) => f.write_node(stmt),
            Statement::AlterIndex(stmt) => f.write_node(stmt),
            Statement::AlterSecret(stmt) => f.write_node(stmt),
            Statement::AlterSink(stmt) => f.write_node(stmt),
            Statement::AlterSource(stmt) => f.write_node(stmt),
            Statement::AlterSystemSet(stmt) => f.write_node(stmt),
            Statement::AlterSystemReset(stmt) => f.write_node(stmt),
            Statement::AlterSystemResetAll(stmt) => f.write_node(stmt),
            Statement::AlterConnection(stmt) => f.write_node(stmt),
            Statement::AlterRole(stmt) => f.write_node(stmt),
            Statement::Discard(stmt) => f.write_node(stmt),
            Statement::DropObjects(stmt) => f.write_node(stmt),
            Statement::DropOwned(stmt) => f.write_node(stmt),
            Statement::SetVariable(stmt) => f.write_node(stmt),
            Statement::ResetVariable(stmt) => f.write_node(stmt),
            Statement::Show(stmt) => f.write_node(stmt),
            Statement::StartTransaction(stmt) => f.write_node(stmt),
            Statement::SetTransaction(stmt) => f.write_node(stmt),
            Statement::Commit(stmt) => f.write_node(stmt),
            Statement::Rollback(stmt) => f.write_node(stmt),
            Statement::Subscribe(stmt) => f.write_node(stmt),
            Statement::Explain(stmt) => f.write_node(stmt),
            Statement::Declare(stmt) => f.write_node(stmt),
            Statement::Close(stmt) => f.write_node(stmt),
            Statement::Fetch(stmt) => f.write_node(stmt),
            Statement::Prepare(stmt) => f.write_node(stmt),
            Statement::Execute(stmt) => f.write_node(stmt),
            Statement::Deallocate(stmt) => f.write_node(stmt),
            Statement::Raise(stmt) => f.write_node(stmt),
            Statement::GrantRole(stmt) => f.write_node(stmt),
            Statement::RevokeRole(stmt) => f.write_node(stmt),
            Statement::GrantPrivileges(stmt) => f.write_node(stmt),
            Statement::RevokePrivileges(stmt) => f.write_node(stmt),
            Statement::AlterDefaultPrivileges(stmt) => f.write_node(stmt),
            Statement::ReassignOwned(stmt) => f.write_node(stmt),
            Statement::ValidateConnection(stmt) => f.write_node(stmt),
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
    pub table_name: T::ItemName,
    /// COLUMNS
    pub columns: Vec<Ident>,
    /// A SQL query that specifies what to insert.
    pub source: InsertSource<T>,
    /// RETURNING
    pub returning: Vec<SelectItem<T>>,
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
        if !self.returning.is_empty() {
            f.write_str(" RETURNING ");
            f.write_node(&display::comma_separated(&self.returning));
        }
    }
}
impl_display_t!(InsertStatement);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CopyRelation<T: AstInfo> {
    Table {
        name: T::ItemName,
        columns: Vec<Ident>,
    },
    Select(SelectStatement<T>),
    Subscribe(SubscribeStatement<T>),
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
                    f.write_node(&display::comma_separated(columns));
                    f.write_str(")");
                }
            }
            CopyRelation::Select(query) => {
                f.write_str("(");
                f.write_node(query);
                f.write_str(")");
            }
            CopyRelation::Subscribe(query) => {
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
    pub table_name: T::ItemName,
    pub alias: Option<TableAlias>,
    /// Column assignments
    pub assignments: Vec<Assignment<T>>,
    /// WHERE
    pub selection: Option<Expr<T>>,
}

impl<T: AstInfo> AstDisplay for UpdateStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("UPDATE ");
        f.write_node(&self.table_name);
        if let Some(alias) = &self.alias {
            f.write_str(" AS ");
            f.write_node(alias);
        }
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
    pub table_name: T::ItemName,
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct KafkaBroker<T: AstInfo> {
    pub address: String,
    pub tunnel: KafkaBrokerTunnel<T>,
}

impl<T: AstInfo> AstDisplay for KafkaBroker<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("'");
        f.write_node(&display::escape_single_quote_string(&self.address));
        f.write_str("'");
        f.write_node(&self.tunnel);
    }
}

impl_display_t!(KafkaBroker);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum KafkaBrokerTunnel<T: AstInfo> {
    Direct,
    AwsPrivatelink(KafkaBrokerAwsPrivatelink<T>),
    SshTunnel(T::ItemName),
}

impl<T: AstInfo> AstDisplay for KafkaBrokerTunnel<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        use KafkaBrokerTunnel::*;
        match self {
            Direct => {}
            AwsPrivatelink(aws) => {
                f.write_str(" ");
                f.write_node(aws);
            }
            Self::SshTunnel(connection) => {
                f.write_str("USING SSH TUNNEL ");
                f.write_node(connection);
            }
        }
    }
}

impl_display_t!(KafkaBrokerTunnel);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum KafkaBrokerAwsPrivatelinkOptionName {
    AvailabilityZone,
    Port,
}

impl AstDisplay for KafkaBrokerAwsPrivatelinkOptionName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            Self::AvailabilityZone => f.write_str("AVAILABILITY ZONE"),
            Self::Port => f.write_str("PORT"),
        }
    }
}
impl_display!(KafkaBrokerAwsPrivatelinkOptionName);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct KafkaBrokerAwsPrivatelinkOption<T: AstInfo> {
    pub name: KafkaBrokerAwsPrivatelinkOptionName,
    pub value: Option<WithOptionValue<T>>,
}

impl<T: AstInfo> AstDisplay for KafkaBrokerAwsPrivatelinkOption<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_node(&self.name);
        if let Some(value) = &self.value {
            f.write_str(" ");
            f.write_node(value);
        }
    }
}
impl_display_t!(KafkaBrokerAwsPrivatelinkOption);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct KafkaBrokerAwsPrivatelink<T: AstInfo> {
    pub connection: T::ItemName,
    pub options: Vec<KafkaBrokerAwsPrivatelinkOption<T>>,
}

impl<T: AstInfo> AstDisplay for KafkaBrokerAwsPrivatelink<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("USING AWS PRIVATELINK ");
        f.write_node(&self.connection);
        if !self.options.is_empty() {
            f.write_str(" (");
            f.write_node(&display::comma_separated(&self.options));
            f.write_str(")");
        }
    }
}
impl_display_t!(KafkaBrokerAwsPrivatelink);

/// `CREATE CONNECTION`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateConnectionStatement<T: AstInfo> {
    pub name: UnresolvedItemName,
    pub connection: CreateConnection<T>,
    pub if_not_exists: bool,
    pub with_options: Vec<CreateConnectionOption<T>>,
}

impl<T: AstInfo> AstDisplay for CreateConnectionStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("CREATE CONNECTION ");
        if self.if_not_exists {
            f.write_str("IF NOT EXISTS ");
        }
        f.write_node(&self.name);
        f.write_str(" TO ");
        self.connection.fmt(f);
        if !self.with_options.is_empty() {
            f.write_str(" WITH (");
            f.write_node(&display::comma_separated(&self.with_options));
            f.write_str(")");
        }
    }
}
impl_display_t!(CreateConnectionStatement);

/// `VALIDATE CONNECTION`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ValidateConnectionStatement<T: AstInfo> {
    /// The connection to validate
    pub name: T::ItemName,
}

impl<T: AstInfo> AstDisplay for ValidateConnectionStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("VALIDATE CONNECTION ");
        f.write_node(&self.name);
    }
}
impl_display_t!(ValidateConnectionStatement);

/// `CREATE SOURCE`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateSourceStatement<T: AstInfo> {
    pub name: UnresolvedItemName,
    pub in_cluster: Option<T::ClusterName>,
    pub col_names: Vec<Ident>,
    pub connection: CreateSourceConnection<T>,
    pub include_metadata: Vec<SourceIncludeMetadata>,
    pub format: CreateSourceFormat<T>,
    pub envelope: Option<Envelope>,
    pub if_not_exists: bool,
    pub key_constraint: Option<KeyConstraint>,
    pub with_options: Vec<CreateSourceOption<T>>,
    pub referenced_subsources: Option<ReferencedSubsources<T>>,
    pub progress_subsource: Option<DeferredItemName<T>>,
}

impl<T: AstInfo> AstDisplay for CreateSourceStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("CREATE SOURCE ");
        if self.if_not_exists {
            f.write_str("IF NOT EXISTS ");
        }
        f.write_node(&self.name);
        if !self.col_names.is_empty() {
            f.write_str(" (");
            f.write_node(&display::comma_separated(&self.col_names));
            if self.key_constraint.is_some() {
                f.write_str(", ");
                f.write_node(self.key_constraint.as_ref().unwrap());
            }
            f.write_str(")");
        } else if self.key_constraint.is_some() {
            f.write_str(" (");
            f.write_node(self.key_constraint.as_ref().unwrap());
            f.write_str(")")
        }
        if let Some(cluster) = &self.in_cluster {
            f.write_str(" IN CLUSTER ");
            f.write_node(cluster);
        }
        f.write_str(" FROM ");
        f.write_node(&self.connection);
        f.write_node(&self.format);
        if !self.include_metadata.is_empty() {
            f.write_str(" INCLUDE ");
            f.write_node(&display::comma_separated(&self.include_metadata));
        }

        if let Some(envelope) = &self.envelope {
            f.write_str(" ENVELOPE ");
            f.write_node(envelope);
        }

        if let Some(subsources) = &self.referenced_subsources {
            f.write_str(" ");
            f.write_node(subsources);
        }

        if let Some(progress) = &self.progress_subsource {
            f.write_str(" EXPOSE PROGRESS AS ");
            f.write_node(progress);
        }

        if !self.with_options.is_empty() {
            f.write_str(" WITH (");
            f.write_node(&display::comma_separated(&self.with_options));
            f.write_str(")");
        }
    }
}
impl_display_t!(CreateSourceStatement);

/// A selected subsource in a FOR TABLES (..) statement
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CreateSourceSubsource<T: AstInfo> {
    pub reference: UnresolvedItemName,
    pub subsource: Option<DeferredItemName<T>>,
}

impl<T: AstInfo> AstDisplay for CreateSourceSubsource<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_node(&self.reference);
        if let Some(subsource) = &self.subsource {
            f.write_str(" AS ");
            f.write_node(subsource);
        }
    }
}
impl_display_t!(CreateSourceSubsource);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ReferencedSubsources<T: AstInfo> {
    /// A subset defined with FOR TABLES (...)
    SubsetTables(Vec<CreateSourceSubsource<T>>),
    /// A subset defined with FOR SCHEMAS (...)
    SubsetSchemas(Vec<Ident>),
    /// FOR ALL TABLES
    All,
}

impl<T: AstInfo> AstDisplay for ReferencedSubsources<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            Self::SubsetTables(subsources) => {
                f.write_str("FOR TABLES (");
                f.write_node(&display::comma_separated(subsources));
                f.write_str(")");
            }
            Self::SubsetSchemas(schemas) => {
                f.write_str("FOR SCHEMAS (");
                f.write_node(&display::comma_separated(schemas));
                f.write_str(")");
            }
            Self::All => f.write_str("FOR ALL TABLES"),
        }
    }
}
impl_display_t!(ReferencedSubsources);

/// An option in a `CREATE SUBSOURCE` statement.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CreateSubsourceOptionName {
    Progress,
    References,
}

impl AstDisplay for CreateSubsourceOptionName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            CreateSubsourceOptionName::Progress => {
                f.write_str("PROGRESS");
            }
            CreateSubsourceOptionName::References => {
                f.write_str("REFERENCES");
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateSubsourceOption<T: AstInfo> {
    pub name: CreateSubsourceOptionName,
    pub value: Option<WithOptionValue<T>>,
}

impl<T: AstInfo> AstDisplay for CreateSubsourceOption<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_node(&self.name);
        if let Some(v) = &self.value {
            f.write_str(" = ");
            f.write_node(v);
        }
    }
}

/// `CREATE SUBSOURCE`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateSubsourceStatement<T: AstInfo> {
    pub name: UnresolvedItemName,
    pub columns: Vec<ColumnDef<T>>,
    pub constraints: Vec<TableConstraint<T>>,
    pub if_not_exists: bool,
    pub with_options: Vec<CreateSubsourceOption<T>>,
}

impl<T: AstInfo> AstDisplay for CreateSubsourceStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("CREATE SUBSOURCE ");
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
impl_display_t!(CreateSubsourceStatement);

/// An option in a `CREATE SINK` statement.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CreateSinkOptionName {
    Size,
    Snapshot,
}

impl AstDisplay for CreateSinkOptionName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            CreateSinkOptionName::Size => {
                f.write_str("SIZE");
            }
            CreateSinkOptionName::Snapshot => {
                f.write_str("SNAPSHOT");
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CreateSinkOption<T: AstInfo> {
    pub name: CreateSinkOptionName,
    pub value: Option<WithOptionValue<T>>,
}

impl<T: AstInfo> AstDisplay for CreateSinkOption<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_node(&self.name);
        if let Some(v) = &self.value {
            f.write_str(" = ");
            f.write_node(v);
        }
    }
}

/// `CREATE SINK`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateSinkStatement<T: AstInfo> {
    pub name: UnresolvedItemName,
    pub in_cluster: Option<T::ClusterName>,
    pub if_not_exists: bool,
    pub from: T::ItemName,
    pub connection: CreateSinkConnection<T>,
    pub format: Option<Format<T>>,
    pub envelope: Option<Envelope>,
    pub with_options: Vec<CreateSinkOption<T>>,
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
        f.write_node(&self.connection);
        if let Some(format) = &self.format {
            f.write_str(" FORMAT ");
            f.write_node(format);
        }
        if let Some(envelope) = &self.envelope {
            f.write_str(" ENVELOPE ");
            f.write_node(envelope);
        }

        if !self.with_options.is_empty() {
            f.write_str(" WITH (");
            f.write_node(&display::comma_separated(&self.with_options));
            f.write_str(")");
        }
    }
}
impl_display_t!(CreateSinkStatement);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ViewDefinition<T: AstInfo> {
    /// View name
    pub name: UnresolvedItemName,
    pub columns: Vec<Ident>,
    pub query: Query<T>,
}

impl<T: AstInfo> AstDisplay for ViewDefinition<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_node(&self.name);

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

        f.write_str(" VIEW");

        if self.if_exists == IfExistsBehavior::Skip {
            f.write_str(" IF NOT EXISTS");
        }

        f.write_str(" ");
        f.write_node(&self.definition);
    }
}
impl_display_t!(CreateViewStatement);

/// `CREATE MATERIALIZED VIEW`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateMaterializedViewStatement<T: AstInfo> {
    pub if_exists: IfExistsBehavior,
    pub name: UnresolvedItemName,
    pub columns: Vec<Ident>,
    pub in_cluster: Option<T::ClusterName>,
    pub query: Query<T>,
}

impl<T: AstInfo> AstDisplay for CreateMaterializedViewStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("CREATE");
        if self.if_exists == IfExistsBehavior::Replace {
            f.write_str(" OR REPLACE");
        }

        f.write_str(" MATERIALIZED VIEW");

        if self.if_exists == IfExistsBehavior::Skip {
            f.write_str(" IF NOT EXISTS");
        }

        f.write_str(" ");
        f.write_node(&self.name);

        if !self.columns.is_empty() {
            f.write_str(" (");
            f.write_node(&display::comma_separated(&self.columns));
            f.write_str(")");
        }

        if let Some(cluster) = &self.in_cluster {
            f.write_str(" IN CLUSTER ");
            f.write_node(cluster);
        }

        f.write_str(" AS ");
        f.write_node(&self.query);
    }
}
impl_display_t!(CreateMaterializedViewStatement);

/// `CREATE TABLE`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateTableStatement<T: AstInfo> {
    /// Table name
    pub name: UnresolvedItemName,
    /// Optional schema
    pub columns: Vec<ColumnDef<T>>,
    pub constraints: Vec<TableConstraint<T>>,
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
    pub on_name: T::ItemName,
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
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
    /// The specified role.
    pub name: Ident,
    /// Any options that were attached, in the order they were presented.
    pub options: Vec<RoleAttribute>,
}

impl AstDisplay for CreateRoleStatement {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("CREATE ");
        f.write_str("ROLE ");
        f.write_node(&self.name);
        for option in &self.options {
            f.write_str(" ");
            option.fmt(f)
        }
    }
}
impl_display!(CreateRoleStatement);

/// Attributes that can be attached to roles.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RoleAttribute {
    /// The `INHERIT` option.
    Inherit,
    /// The `NOINHERIT` option.
    NoInherit,
    // The following are not supported, but included to give helpful error messages.
    Login,
    NoLogin,
    SuperUser,
    NoSuperUser,
    CreateCluster,
    NoCreateCluster,
    CreateDB,
    NoCreateDB,
    CreateRole,
    NoCreateRole,
}

impl AstDisplay for RoleAttribute {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            RoleAttribute::SuperUser => f.write_str("SUPERUSER"),
            RoleAttribute::NoSuperUser => f.write_str("NOSUPERUSER"),
            RoleAttribute::Login => f.write_str("LOGIN"),
            RoleAttribute::NoLogin => f.write_str("NOLOGIN"),
            RoleAttribute::Inherit => f.write_str("INHERIT"),
            RoleAttribute::NoInherit => f.write_str("NOINHERIT"),
            RoleAttribute::CreateCluster => f.write_str("CREATECLUSTER"),
            RoleAttribute::NoCreateCluster => f.write_str("NOCREATECLUSTER"),
            RoleAttribute::CreateDB => f.write_str("CREATEDB"),
            RoleAttribute::NoCreateDB => f.write_str("NOCREATEDB"),
            RoleAttribute::CreateRole => f.write_str("CREATEROLE"),
            RoleAttribute::NoCreateRole => f.write_str("NOCREATEROLE"),
        }
    }
}
impl_display!(RoleAttribute);

/// A `CREATE SECRET` statement.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateSecretStatement<T: AstInfo> {
    pub name: UnresolvedItemName,
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
    pub name: UnresolvedItemName,
    /// The new type's "base type".
    pub as_type: CreateTypeAs<T>,
}

impl<T: AstInfo> AstDisplay for CreateTypeStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("CREATE TYPE ");
        f.write_node(&self.name);
        f.write_str(" AS ");
        match &self.as_type {
            CreateTypeAs::List { options } => {
                f.write_str(&self.as_type);
                f.write_str("( ");
                if !options.is_empty() {
                    f.write_node(&display::comma_separated(options));
                }
                f.write_str(" )");
            }
            CreateTypeAs::Map { options } => {
                f.write_str(&self.as_type);
                f.write_str("( ");
                if !options.is_empty() {
                    f.write_node(&display::comma_separated(options));
                }
                f.write_str(" )");
            }
            CreateTypeAs::Record { column_defs } => {
                f.write_str("( ");
                if !column_defs.is_empty() {
                    f.write_node(&display::comma_separated(column_defs));
                }
                f.write_str(" )");
            }
        };
    }
}
impl_display_t!(CreateTypeStatement);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ClusterOptionName {
    /// The `AVAILABILITY ZONES [[=] '[' <values> ']' ]` option.
    AvailabilityZones,
    /// The `INTROSPECTION INTERVAL [[=] <interval>]` option.
    IntrospectionInterval,
    /// The `INTROSPECTION DEBUGGING [[=] <enabled>]` option.
    IntrospectionDebugging,
    /// The `IDLE ARRANGEMENT MERGE EFFORT [=] <value>` option.
    IdleArrangementMergeEffort,
    /// The `MANAGED` option.
    Managed,
    /// The `REPLICAS` option.
    Replicas,
    /// The `REPLICATION FACTOR` option.
    ReplicationFactor,
    /// The `SIZE` option.
    Size,
}

impl AstDisplay for ClusterOptionName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            ClusterOptionName::AvailabilityZones => f.write_str("AVAILABILITY ZONES"),
            ClusterOptionName::IdleArrangementMergeEffort => {
                f.write_str("IDLE ARRANGEMENT MERGE EFFORT")
            }
            ClusterOptionName::IntrospectionDebugging => f.write_str("INTROSPECTION DEBUGGING"),
            ClusterOptionName::IntrospectionInterval => f.write_str("INTROSPECTION INTERVAL"),
            ClusterOptionName::Managed => f.write_str("MANAGED"),
            ClusterOptionName::Replicas => f.write_str("REPLICAS"),
            ClusterOptionName::ReplicationFactor => f.write_str("REPLICATION FACTOR"),
            ClusterOptionName::Size => f.write_str("SIZE"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
/// An option in a `CREATE CLUSTER` ostatement.
pub struct ClusterOption<T: AstInfo> {
    pub name: ClusterOptionName,
    pub value: Option<WithOptionValue<T>>,
}

impl<T: AstInfo> AstDisplay for ClusterOption<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_node(&self.name);
        if let Some(v) = &self.value {
            f.write_str(" ");
            f.write_node(v);
        }
    }
}

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

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AlterClusterAction<T: AstInfo> {
    SetOptions(Vec<ClusterOption<T>>),
    ResetOptions(Vec<ClusterOptionName>),
}

/// `ALTER CLUSTER .. SET ...`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AlterClusterStatement<T: AstInfo> {
    /// The `IF EXISTS` option.
    pub if_exists: bool,
    /// Name of the altered cluster.
    pub name: Ident,
    /// The action.
    pub action: AlterClusterAction<T>,
}

impl<T: AstInfo> AstDisplay for AlterClusterStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("ALTER CLUSTER ");
        if self.if_exists {
            f.write_str("IF EXISTS ");
        }
        f.write_node(&self.name);
        f.write_str(" ");
        match &self.action {
            AlterClusterAction::SetOptions(options) => {
                f.write_str("SET (");
                f.write_node(&display::comma_separated(options));
                f.write_str(")");
            }
            AlterClusterAction::ResetOptions(options) => {
                f.write_str("RESET (");
                f.write_node(&display::comma_separated(options));
                f.write_str(")");
            }
        }
    }
}
impl_display_t!(AlterClusterStatement);

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

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ReplicaOptionName {
    /// The `SIZE [[=] <size>]` option.
    Size,
    /// The `AVAILABILITY ZONE [[=] <id>]` option.
    AvailabilityZone,
    /// The `STORAGE ADDRESSES` option.
    StorageAddresses,
    /// The `STORAGECTL ADDRESSES` option.
    StoragectlAddresses,
    /// The `COMPUTECTL ADDRESSES` option.
    ComputectlAddresses,
    /// The `COMPUTE ADDRESSES` option.
    ComputeAddresses,
    /// The `WORKERS` option
    Workers,
    /// The `INTROSPECTION INTERVAL [[=] <interval>]` option.
    IntrospectionInterval,
    /// The `INTROSPECTION DEBUGGING [[=] <enabled>]` option.
    IntrospectionDebugging,
    /// The `IDLE ARRANGEMENT MERGE EFFORT [=] <value>` option.
    IdleArrangementMergeEffort,
}

impl AstDisplay for ReplicaOptionName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            ReplicaOptionName::Size => f.write_str("SIZE"),
            ReplicaOptionName::AvailabilityZone => f.write_str("AVAILABILITY ZONE"),
            ReplicaOptionName::StorageAddresses => f.write_str("STORAGE ADDRESSES"),
            ReplicaOptionName::StoragectlAddresses => f.write_str("STORAGECTL ADDRESSES"),
            ReplicaOptionName::ComputectlAddresses => f.write_str("COMPUTECTL ADDRESSES"),
            ReplicaOptionName::ComputeAddresses => f.write_str("COMPUTE ADDRESSES"),
            ReplicaOptionName::Workers => f.write_str("WORKERS"),
            ReplicaOptionName::IntrospectionInterval => f.write_str("INTROSPECTION INTERVAL"),
            ReplicaOptionName::IntrospectionDebugging => f.write_str("INTROSPECTION DEBUGGING"),
            ReplicaOptionName::IdleArrangementMergeEffort => {
                f.write_str("IDLE ARRANGEMENT MERGE EFFORT")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
/// An option in a `CREATE CLUSTER REPLICA` statement.
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
    List {
        options: Vec<CreateTypeListOption<T>>,
    },
    Map {
        options: Vec<CreateTypeMapOption<T>>,
    },
    Record {
        column_defs: Vec<ColumnDef<T>>,
    },
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CreateTypeListOptionName {
    ElementType,
}

impl AstDisplay for CreateTypeListOptionName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str(match self {
            CreateTypeListOptionName::ElementType => "ELEMENT TYPE",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CreateTypeListOption<T: AstInfo> {
    pub name: CreateTypeListOptionName,
    pub value: Option<WithOptionValue<T>>,
}

impl<T: AstInfo> AstDisplay for CreateTypeListOption<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_node(&self.name);
        if let Some(v) = &self.value {
            f.write_str(" = ");
            f.write_node(v);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CreateTypeMapOptionName {
    KeyType,
    ValueType,
}

impl AstDisplay for CreateTypeMapOptionName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str(match self {
            CreateTypeMapOptionName::KeyType => "KEY TYPE",
            CreateTypeMapOptionName::ValueType => "VALUE TYPE",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CreateTypeMapOption<T: AstInfo> {
    pub name: CreateTypeMapOptionName,
    pub value: Option<WithOptionValue<T>>,
}

impl<T: AstInfo> AstDisplay for CreateTypeMapOption<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_node(&self.name);
        if let Some(v) = &self.value {
            f.write_str(" = ");
            f.write_node(v);
        }
    }
}

/// `ALTER <OBJECT> ... OWNER TO`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AlterOwnerStatement<T: AstInfo> {
    pub object_type: ObjectType,
    pub if_exists: bool,
    pub name: UnresolvedObjectName,
    pub new_owner: T::RoleName,
}

impl<T: AstInfo> AstDisplay for AlterOwnerStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("ALTER ");
        f.write_node(&self.object_type);
        f.write_str(" ");
        if self.if_exists {
            f.write_str("IF EXISTS ");
        }
        f.write_node(&self.name);
        f.write_str(" OWNER TO ");
        f.write_node(&self.new_owner);
    }
}
impl_display_t!(AlterOwnerStatement);

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
    pub index_name: UnresolvedItemName,
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
                f.write_node(&display::comma_separated(options));
                f.write_str(")");
            }
            AlterIndexAction::ResetOptions(options) => {
                f.write_str("RESET (");
                f.write_node(&display::comma_separated(options));
                f.write_str(")");
            }
        }
    }
}

impl_display_t!(AlterIndexStatement);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AlterSinkAction<T: AstInfo> {
    SetOptions(Vec<CreateSinkOption<T>>),
    ResetOptions(Vec<CreateSinkOptionName>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AlterSinkStatement<T: AstInfo> {
    pub sink_name: UnresolvedItemName,
    pub if_exists: bool,
    pub action: AlterSinkAction<T>,
}

impl<T: AstInfo> AstDisplay for AlterSinkStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("ALTER SINK ");
        if self.if_exists {
            f.write_str("IF EXISTS ");
        }
        f.write_node(&self.sink_name);
        f.write_str(" ");

        match &self.action {
            AlterSinkAction::SetOptions(options) => {
                f.write_str("SET (");
                f.write_node(&display::comma_separated(options));
                f.write_str(")");
            }
            AlterSinkAction::ResetOptions(options) => {
                f.write_str("RESET (");
                f.write_node(&display::comma_separated(options));
                f.write_str(")");
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AlterSourceAction<T: AstInfo> {
    SetOptions(Vec<CreateSourceOption<T>>),
    ResetOptions(Vec<CreateSourceOptionName>),
    DropSubsources {
        if_exists: bool,
        cascade: bool,
        names: Vec<UnresolvedItemName>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AlterSourceStatement<T: AstInfo> {
    pub source_name: UnresolvedItemName,
    pub if_exists: bool,
    pub action: AlterSourceAction<T>,
}

impl<T: AstInfo> AstDisplay for AlterSourceStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("ALTER SOURCE ");
        if self.if_exists {
            f.write_str("IF EXISTS ");
        }
        f.write_node(&self.source_name);
        f.write_str(" ");

        match &self.action {
            AlterSourceAction::SetOptions(options) => {
                f.write_str("SET (");
                f.write_node(&display::comma_separated(options));
                f.write_str(")");
            }
            AlterSourceAction::ResetOptions(options) => {
                f.write_str("RESET (");
                f.write_node(&display::comma_separated(options));
                f.write_str(")");
            }
            AlterSourceAction::DropSubsources {
                if_exists,
                cascade,
                names,
            } => {
                f.write_str("DROP SUBSOURCE ");
                if *if_exists {
                    f.write_str("IF EXISTS ");
                }

                f.write_node(&display::comma_separated(names));

                if *cascade {
                    f.write_str(" CASCADE");
                }
            }
        }
    }
}

impl_display_t!(AlterSourceStatement);

/// `ALTER SECRET ... AS`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AlterSecretStatement<T: AstInfo> {
    pub name: UnresolvedItemName,
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

/// `ALTER CONNECTION ... ROTATE KEYS`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AlterConnectionStatement {
    pub name: UnresolvedItemName,
    pub if_exists: bool,
}

impl AstDisplay for AlterConnectionStatement {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("ALTER CONNECTION ");
        if self.if_exists {
            f.write_str("IF EXISTS ");
        }
        f.write_node(&self.name);
        f.write_str(" ROTATE KEYS");
    }
}

impl_display!(AlterConnectionStatement);

/// `ALTER ROLE`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AlterRoleStatement<T: AstInfo> {
    /// The specified role.
    pub name: T::RoleName,
    /// Any options that were attached, in the order they were presented.
    pub options: Vec<RoleAttribute>,
}

impl<T: AstInfo> AstDisplay for AlterRoleStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("ALTER ROLE ");
        f.write_node(&self.name);
        for option in &self.options {
            f.write_str(" ");
            option.fmt(f)
        }
    }
}
impl_display_t!(AlterRoleStatement);

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
    /// `RESTRICT` was specified.
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
        if self.cascade && self.object_type != ObjectType::Database {
            f.write_str(" CASCADE");
        } else if !self.cascade && self.object_type == ObjectType::Database {
            f.write_str(" RESTRICT");
        }
    }
}
impl_display!(DropObjectsStatement);

/// `DROP OWNED BY ...`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DropOwnedStatement<T: AstInfo> {
    /// The roles whose owned objects are being dropped.
    pub role_names: Vec<T::RoleName>,
    /// Whether `CASCADE` was specified. This will be `false` when
    /// `RESTRICT` or no drop behavior at all was specified.
    pub cascade: bool,
}

impl<T: AstInfo> AstDisplay for DropOwnedStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("DROP OWNED BY ");
        f.write_node(&display::comma_separated(&self.role_names));
        if self.cascade {
            f.write_str(" CASCADE");
        }
    }
}
impl_display_t!(DropOwnedStatement);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
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

/// `SET <variable>`
///
/// Note: this is not a standard SQL statement, but it is supported by at
/// least MySQL and PostgreSQL. Not all MySQL-specific syntactic forms are
/// supported yet.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SetVariableStatement {
    pub local: bool,
    pub variable: Ident,
    pub to: SetVariableTo,
}

impl AstDisplay for SetVariableStatement {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("SET ");
        if self.local {
            f.write_str("LOCAL ");
        }
        f.write_node(&self.variable);
        f.write_str(" = ");
        f.write_node(&self.to);
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
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
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

/// `INSPECT SHARD <id>`
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct InspectShardStatement {
    pub id: String,
}

impl AstDisplay for InspectShardStatement {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("INSPECT SHARD ");
        f.write_str(&self.id);
    }
}
impl_display!(InspectShardStatement);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ShowObjectType<T: AstInfo> {
    MaterializedView {
        in_cluster: Option<T::ClusterName>,
    },
    Index {
        in_cluster: Option<T::ClusterName>,
        on_object: Option<T::ItemName>,
    },
    Table,
    View,
    Source,
    Sink,
    Type,
    Role,
    Cluster,
    ClusterReplica,
    Object,
    Secret,
    Connection,
    Database,
    Schema {
        from: Option<T::DatabaseName>,
    },
    Subsource {
        on_source: T::ItemName,
    },
}
/// `SHOW <object>S`
///
/// ```sql
/// SHOW TABLES;
/// SHOW SOURCES;
/// SHOW VIEWS;
/// SHOW SINKS;
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ShowObjectsStatement<T: AstInfo> {
    pub object_type: ShowObjectType<T>,
    pub from: Option<T::SchemaName>,
    pub filter: Option<ShowStatementFilter<T>>,
}

impl<T: AstInfo> AstDisplay for ShowObjectsStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("SHOW");
        f.write_str(" ");
        f.write_str(match &self.object_type {
            ShowObjectType::Table => "TABLES",
            ShowObjectType::View => "VIEWS",
            ShowObjectType::Source => "SOURCES",
            ShowObjectType::Sink => "SINKS",
            ShowObjectType::Type => "TYPES",
            ShowObjectType::Role => "ROLES",
            ShowObjectType::Cluster => "CLUSTERS",
            ShowObjectType::ClusterReplica => "CLUSTER REPLICAS",
            ShowObjectType::Object => "OBJECTS",
            ShowObjectType::Secret => "SECRETS",
            ShowObjectType::Connection => "CONNECTIONS",
            ShowObjectType::MaterializedView { .. } => "MATERIALIZED VIEWS",
            ShowObjectType::Index { .. } => "INDEXES",
            ShowObjectType::Database => "DATABASES",
            ShowObjectType::Schema { .. } => "SCHEMAS",
            ShowObjectType::Subsource { .. } => "SUBSOURCES",
        });

        if let ShowObjectType::Index { on_object, .. } = &self.object_type {
            if let Some(on_object) = on_object {
                f.write_str(" ON ");
                f.write_node(on_object);
            }
        }

        if let ShowObjectType::Schema { from: Some(from) } = &self.object_type {
            f.write_str(" FROM ");
            f.write_node(from);
        }

        if let Some(from) = &self.from {
            f.write_str(" FROM ");
            f.write_node(from);
        }

        // append IN CLUSTER clause
        match &self.object_type {
            ShowObjectType::MaterializedView { in_cluster }
            | ShowObjectType::Index { in_cluster, .. } => {
                if let Some(cluster) = in_cluster {
                    f.write_str(" IN CLUSTER ");
                    f.write_node(cluster);
                }
            }
            _ => (),
        }

        if let ShowObjectType::Subsource { on_source } = &self.object_type {
            f.write_str(" ON ");
            f.write_node(on_source);
        }

        if let Some(filter) = &self.filter {
            f.write_str(" ");
            f.write_node(filter);
        }
    }
}
impl_display_t!(ShowObjectsStatement);

/// `SHOW COLUMNS`
///
/// Note: this is a MySQL-specific statement.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ShowColumnsStatement<T: AstInfo> {
    pub table_name: T::ItemName,
    pub filter: Option<ShowStatementFilter<T>>,
}

impl<T: AstInfo> AstDisplay for ShowColumnsStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("SHOW ");
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
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ShowCreateViewStatement<T: AstInfo> {
    pub view_name: T::ItemName,
}

impl<T: AstInfo> AstDisplay for ShowCreateViewStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("SHOW CREATE VIEW ");
        f.write_node(&self.view_name);
    }
}
impl_display_t!(ShowCreateViewStatement);

/// `SHOW CREATE MATERIALIZED VIEW <name>`
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ShowCreateMaterializedViewStatement<T: AstInfo> {
    pub materialized_view_name: T::ItemName,
}

impl<T: AstInfo> AstDisplay for ShowCreateMaterializedViewStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("SHOW CREATE MATERIALIZED VIEW ");
        f.write_node(&self.materialized_view_name);
    }
}
impl_display_t!(ShowCreateMaterializedViewStatement);

/// `SHOW CREATE SOURCE <source>`
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ShowCreateSourceStatement<T: AstInfo> {
    pub source_name: T::ItemName,
}

impl<T: AstInfo> AstDisplay for ShowCreateSourceStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("SHOW CREATE SOURCE ");
        f.write_node(&self.source_name);
    }
}
impl_display_t!(ShowCreateSourceStatement);

/// `SHOW CREATE TABLE <table>`
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ShowCreateTableStatement<T: AstInfo> {
    pub table_name: T::ItemName,
}

impl<T: AstInfo> AstDisplay for ShowCreateTableStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("SHOW CREATE TABLE ");
        f.write_node(&self.table_name);
    }
}
impl_display_t!(ShowCreateTableStatement);

/// `SHOW CREATE SINK <sink>`
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ShowCreateSinkStatement<T: AstInfo> {
    pub sink_name: T::ItemName,
}

impl<T: AstInfo> AstDisplay for ShowCreateSinkStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("SHOW CREATE SINK ");
        f.write_node(&self.sink_name);
    }
}
impl_display_t!(ShowCreateSinkStatement);

/// `SHOW CREATE INDEX <index>`
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ShowCreateIndexStatement<T: AstInfo> {
    pub index_name: T::ItemName,
}

impl<T: AstInfo> AstDisplay for ShowCreateIndexStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("SHOW CREATE INDEX ");
        f.write_node(&self.index_name);
    }
}
impl_display_t!(ShowCreateIndexStatement);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ShowCreateConnectionStatement<T: AstInfo> {
    pub connection_name: T::ItemName,
}

impl<T: AstInfo> AstDisplay for ShowCreateConnectionStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("SHOW CREATE CONNECTION ");
        f.write_node(&self.connection_name);
    }
}

/// `{ BEGIN [ TRANSACTION | WORK ] | START TRANSACTION } ...`
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
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
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SetTransactionStatement {
    pub local: bool,
    pub modes: Vec<TransactionMode>,
}

impl AstDisplay for SetTransactionStatement {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("SET ");
        if !self.local {
            f.write_str("SESSION CHARACTERISTICS AS ");
        }
        f.write_str("TRANSACTION");
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SubscribeOptionName {
    Snapshot,
    Progress,
}

impl AstDisplay for SubscribeOptionName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            SubscribeOptionName::Snapshot => f.write_str("SNAPSHOT"),
            SubscribeOptionName::Progress => f.write_str("PROGRESS"),
        }
    }
}
impl_display!(SubscribeOptionName);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SubscribeOption<T: AstInfo> {
    pub name: SubscribeOptionName,
    pub value: Option<WithOptionValue<T>>,
}

impl<T: AstInfo> AstDisplay for SubscribeOption<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_node(&self.name);
        if let Some(v) = &self.value {
            f.write_str(" = ");
            f.write_node(v);
        }
    }
}
impl_display_t!(SubscribeOption);

/// `SUBSCRIBE`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SubscribeStatement<T: AstInfo> {
    pub relation: SubscribeRelation<T>,
    pub options: Vec<SubscribeOption<T>>,
    pub as_of: Option<AsOf<T>>,
    pub up_to: Option<Expr<T>>,
    pub output: SubscribeOutput<T>,
}

impl<T: AstInfo> AstDisplay for SubscribeStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("SUBSCRIBE ");
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
        if let Some(up_to) = &self.up_to {
            f.write_str(" UP TO ");
            f.write_node(up_to);
        }
        f.write_str(&self.output);
    }
}
impl_display_t!(SubscribeStatement);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SubscribeRelation<T: AstInfo> {
    Name(T::ItemName),
    Query(Query<T>),
}

impl<T: AstInfo> AstDisplay for SubscribeRelation<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            SubscribeRelation::Name(name) => f.write_node(name),
            SubscribeRelation::Query(query) => {
                f.write_str("(");
                f.write_node(query);
                f.write_str(")");
            }
        }
    }
}
impl_display_t!(SubscribeRelation);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExplainStatement<T: AstInfo> {
    pub stage: ExplainStage,
    pub config_flags: Vec<Ident>,
    pub format: ExplainFormat,
    pub no_errors: bool,
    pub explainee: Explainee<T>,
}

impl<T: AstInfo> AstDisplay for ExplainStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("EXPLAIN ");
        f.write_node(&self.stage);
        if !self.config_flags.is_empty() {
            f.write_str(" WITH(");
            f.write_node(&display::comma_separated(&self.config_flags));
            f.write_str(")");
        }
        f.write_str(" AS ");
        f.write_node(&self.format);
        f.write_str(" FOR ");
        if self.no_errors {
            f.write_str("BROKEN ");
        }
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
    MaterializedView,
    Source,
    Sink,
    Index,
    Type,
    Role,
    Cluster,
    ClusterReplica,
    Secret,
    Connection,
    Database,
    Schema,
    Func,
    Subsource,
}

impl ObjectType {
    pub fn lives_in_schema(&self) -> bool {
        match self {
            ObjectType::Table
            | ObjectType::View
            | ObjectType::MaterializedView
            | ObjectType::Source
            | ObjectType::Sink
            | ObjectType::Index
            | ObjectType::Type
            | ObjectType::Secret
            | ObjectType::Connection
            | ObjectType::Func
            | ObjectType::Subsource => true,
            ObjectType::Database
            | ObjectType::Schema
            | ObjectType::Cluster
            | ObjectType::ClusterReplica
            | ObjectType::Role => false,
        }
    }
}

impl AstDisplay for ObjectType {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str(match self {
            ObjectType::Table => "TABLE",
            ObjectType::View => "VIEW",
            ObjectType::MaterializedView => "MATERIALIZED VIEW",
            ObjectType::Source => "SOURCE",
            ObjectType::Sink => "SINK",
            ObjectType::Index => "INDEX",
            ObjectType::Type => "TYPE",
            ObjectType::Role => "ROLE",
            ObjectType::Cluster => "CLUSTER",
            ObjectType::ClusterReplica => "CLUSTER REPLICA",
            ObjectType::Secret => "SECRET",
            ObjectType::Connection => "CONNECTION",
            ObjectType::Database => "DATABASE",
            ObjectType::Schema => "SCHEMA",
            ObjectType::Func => "FUNCTION",
            ObjectType::Subsource => "SUBSOURCE",
        })
    }
}
impl_display!(ObjectType);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum WithOptionValue<T: AstInfo> {
    Value(Value),
    Ident(Ident),
    DataType(T::DataType),
    Secret(T::ItemName),
    Item(T::ItemName),
    UnresolvedItemName(UnresolvedItemName),
    Sequence(Vec<WithOptionValue<T>>),
    // Special cases.
    ClusterReplicas(Vec<ReplicaDefinition<T>>),
    ConnectionKafkaBroker(KafkaBroker<T>),
}

impl<T: AstInfo> AstDisplay for WithOptionValue<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            WithOptionValue::Sequence(values) => {
                f.write_str("(");
                f.write_node(&display::comma_separated(values));
                f.write_str(")");
            }
            WithOptionValue::Value(value) => f.write_node(value),
            WithOptionValue::Ident(id) => f.write_node(id),
            WithOptionValue::DataType(typ) => f.write_node(typ),
            WithOptionValue::Secret(name) => {
                f.write_str("SECRET ");
                f.write_node(name)
            }
            WithOptionValue::Item(obj) => f.write_node(obj),
            WithOptionValue::UnresolvedItemName(r) => f.write_node(r),
            WithOptionValue::ClusterReplicas(replicas) => {
                f.write_str("(");
                f.write_node(&display::comma_separated(replicas));
                f.write_str(")");
            }
            WithOptionValue::ConnectionKafkaBroker(broker) => {
                f.write_node(broker);
            }
        }
    }
}
impl_display_t!(WithOptionValue);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TransactionIsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
    StrictSerializable,
}

impl AstDisplay for TransactionIsolationLevel {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        use TransactionIsolationLevel::*;
        f.write_str(match self {
            ReadUncommitted => "READ UNCOMMITTED",
            ReadCommitted => "READ COMMITTED",
            RepeatableRead => "REPEATABLE READ",
            Serializable => "SERIALIZABLE",
            StrictSerializable => "STRICT SERIALIZABLE",
        })
    }
}
impl_display!(TransactionIsolationLevel);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SetVariableTo {
    Default,
    Values(Vec<SetVariableValue>),
}

impl AstDisplay for SetVariableTo {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        use SetVariableTo::*;
        match self {
            Values(values) => f.write_node(&display::comma_separated(values)),
            Default => f.write_str("DEFAULT"),
        }
    }
}
impl_display!(SetVariableTo);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SetVariableValue {
    Ident(Ident),
    Literal(Value),
}

impl AstDisplay for SetVariableValue {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        use SetVariableValue::*;
        match self {
            Ident(ident) => f.write_node(ident),
            Literal(literal) => f.write_node(literal),
        }
    }
}
impl_display!(SetVariableValue);

impl SetVariableValue {
    /// Returns the underlying value without quotes.
    pub fn into_unquoted_value(self) -> String {
        match self {
            // `lit.to_string` will quote a `Value::String`, so get the unquoted
            // version.
            SetVariableValue::Literal(Value::String(s)) => s,
            SetVariableValue::Literal(lit) => lit.to_string(),
            SetVariableValue::Ident(ident) => ident.into_string(),
        }
    }
}

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
/// The new API
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExplainStage {
    /// The mz_sql::HirRelationExpr after parsing
    RawPlan,
    /// The mz_expr::MirRelationExpr after decorrelation
    DecorrelatedPlan,
    /// The mz_expr::MirRelationExpr after optimization
    OptimizedPlan,
    /// The mz_compute_client::plan::Plan
    PhysicalPlan,
    /// The complete trace of the plan through the optimizer
    Trace,
    /// The dependent and selected timestamps
    Timestamp,
}

impl ExplainStage {
    /// Return the tracing path that corresponds to a given stage.
    pub fn path(&self) -> &'static str {
        match self {
            ExplainStage::RawPlan => "optimize/raw",
            ExplainStage::DecorrelatedPlan => "optimize/hir_to_mir",
            ExplainStage::OptimizedPlan => "optimize/global",
            ExplainStage::PhysicalPlan => "optimize/finalize_dataflow",
            ExplainStage::Trace => unreachable!(),
            ExplainStage::Timestamp => unreachable!(),
        }
    }
}

impl AstDisplay for ExplainStage {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            ExplainStage::RawPlan => f.write_str("RAW PLAN"),
            ExplainStage::DecorrelatedPlan => f.write_str("DECORRELATED PLAN"),
            ExplainStage::OptimizedPlan => f.write_str("OPTIMIZED PLAN"),
            ExplainStage::PhysicalPlan => f.write_str("PHYSICAL PLAN"),
            ExplainStage::Trace => f.write_str("OPTIMIZER TRACE"),
            ExplainStage::Timestamp => f.write_str("TIMESTAMP"),
        }
    }
}
impl_display!(ExplainStage);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Explainee<T: AstInfo> {
    View(T::ItemName),
    MaterializedView(T::ItemName),
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
            Self::MaterializedView(name) => {
                f.write_str("MATERIALIZED VIEW ");
                f.write_node(name);
            }
            Self::Query(query) => f.write_node(query),
        }
    }
}
impl_display_t!(Explainee);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ExplainFormat {
    /// Human readable display format
    Text,
    Json,
    Dot,
}

impl AstDisplay for ExplainFormat {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            Self::Text => f.write_str("TEXT"),
            Self::Json => f.write_str("JSON"),
            Self::Dot => f.write_str("DOT"),
        }
    }
}
impl_display!(ExplainFormat);

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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FetchOptionName {
    Timeout,
}

impl AstDisplay for FetchOptionName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str(match self {
            FetchOptionName::Timeout => "TIMEOUT",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FetchOption<T: AstInfo> {
    pub name: FetchOptionName,
    pub value: Option<WithOptionValue<T>>,
}

impl<T: AstInfo> AstDisplay for FetchOption<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_node(&self.name);
        if let Some(v) = &self.value {
            f.write_str(" = ");
            f.write_node(v);
        }
    }
}

/// `FETCH ...`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FetchStatement<T: AstInfo> {
    pub name: Ident,
    pub count: Option<FetchDirection>,
    pub options: Vec<FetchOption<T>>,
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

/// `ALTER SYSTEM SET ...`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AlterSystemSetStatement {
    pub name: Ident,
    pub to: SetVariableTo,
}

impl AstDisplay for AlterSystemSetStatement {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("ALTER SYSTEM SET ");
        f.write_node(&self.name);
        f.write_str(" = ");
        f.write_node(&self.to);
    }
}
impl_display!(AlterSystemSetStatement);

/// `ALTER SYSTEM RESET ...`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AlterSystemResetStatement {
    pub name: Ident,
}

impl AstDisplay for AlterSystemResetStatement {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("ALTER SYSTEM RESET ");
        f.write_node(&self.name);
    }
}
impl_display!(AlterSystemResetStatement);

/// `ALTER SYSTEM RESET ALL`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AlterSystemResetAllStatement {}

impl AstDisplay for AlterSystemResetAllStatement {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("ALTER SYSTEM RESET ALL");
    }
}
impl_display!(AlterSystemResetAllStatement);

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

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ShowStatement<T: AstInfo> {
    ShowObjects(ShowObjectsStatement<T>),
    ShowColumns(ShowColumnsStatement<T>),
    ShowCreateView(ShowCreateViewStatement<T>),
    ShowCreateMaterializedView(ShowCreateMaterializedViewStatement<T>),
    ShowCreateSource(ShowCreateSourceStatement<T>),
    ShowCreateTable(ShowCreateTableStatement<T>),
    ShowCreateSink(ShowCreateSinkStatement<T>),
    ShowCreateIndex(ShowCreateIndexStatement<T>),
    ShowCreateConnection(ShowCreateConnectionStatement<T>),
    ShowVariable(ShowVariableStatement),
    InspectShard(InspectShardStatement),
}

impl<T: AstInfo> AstDisplay for ShowStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            ShowStatement::ShowObjects(stmt) => f.write_node(stmt),
            ShowStatement::ShowColumns(stmt) => f.write_node(stmt),
            ShowStatement::ShowCreateView(stmt) => f.write_node(stmt),
            ShowStatement::ShowCreateMaterializedView(stmt) => f.write_node(stmt),
            ShowStatement::ShowCreateSource(stmt) => f.write_node(stmt),
            ShowStatement::ShowCreateTable(stmt) => f.write_node(stmt),
            ShowStatement::ShowCreateSink(stmt) => f.write_node(stmt),
            ShowStatement::ShowCreateIndex(stmt) => f.write_node(stmt),
            ShowStatement::ShowCreateConnection(stmt) => f.write_node(stmt),
            ShowStatement::ShowVariable(stmt) => f.write_node(stmt),
            ShowStatement::InspectShard(stmt) => f.write_node(stmt),
        }
    }
}
impl_display_t!(ShowStatement);

/// `GRANT ...`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GrantRoleStatement<T: AstInfo> {
    /// The roles that are gaining members.
    pub role_names: Vec<T::RoleName>,
    /// The roles that will be added to `role_name`.
    pub member_names: Vec<T::RoleName>,
}

impl<T: AstInfo> AstDisplay for GrantRoleStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("GRANT ");
        f.write_node(&display::comma_separated(&self.role_names));
        f.write_str(" TO ");
        f.write_node(&display::comma_separated(&self.member_names));
    }
}
impl_display_t!(GrantRoleStatement);

/// `REVOKE ...`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RevokeRoleStatement<T: AstInfo> {
    /// The roles that are losing members.
    pub role_names: Vec<T::RoleName>,
    /// The roles that will be removed from `role_name`.
    pub member_names: Vec<T::RoleName>,
}

impl<T: AstInfo> AstDisplay for RevokeRoleStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("REVOKE ");
        f.write_node(&display::comma_separated(&self.role_names));
        f.write_str(" FROM ");
        f.write_node(&display::comma_separated(&self.member_names));
    }
}
impl_display_t!(RevokeRoleStatement);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Privilege {
    SELECT,
    INSERT,
    UPDATE,
    DELETE,
    USAGE,
    CREATE,
    CREATEROLE,
    CREATEDB,
    CREATECLUSTER,
}

impl AstDisplay for Privilege {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str(match self {
            Privilege::SELECT => "SELECT",
            Privilege::INSERT => "INSERT",
            Privilege::UPDATE => "UPDATE",
            Privilege::DELETE => "DELETE",
            Privilege::CREATE => "CREATE",
            Privilege::USAGE => "USAGE",
            Privilege::CREATEROLE => "CREATEROLE",
            Privilege::CREATEDB => "CREATEDB",
            Privilege::CREATECLUSTER => "CREATECLUSTER",
        });
    }
}
impl_display!(Privilege);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PrivilegeSpecification {
    All,
    Privileges(Vec<Privilege>),
}

impl AstDisplay for PrivilegeSpecification {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            PrivilegeSpecification::All => f.write_str("ALL"),
            PrivilegeSpecification::Privileges(privileges) => {
                f.write_node(&display::comma_separated(privileges))
            }
        }
    }
}
impl_display!(PrivilegeSpecification);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum GrantTargetSpecification<T: AstInfo> {
    Object {
        /// The type of object.
        ///
        /// Note: For views, materialized views, and sources this will be [`ObjectType::Table`].
        object_type: ObjectType,
        /// Specification of each object affected.
        object_spec_inner: GrantTargetSpecificationInner<T>,
    },
    System,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum GrantTargetSpecificationInner<T: AstInfo> {
    All(GrantTargetAllSpecification<T>),
    Objects { names: Vec<T::ObjectName> },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum GrantTargetAllSpecification<T: AstInfo> {
    All,
    AllDatabases { databases: Vec<T::DatabaseName> },
    AllSchemas { schemas: Vec<T::SchemaName> },
}

impl<T: AstInfo> GrantTargetAllSpecification<T> {
    pub fn len(&self) -> usize {
        match self {
            GrantTargetAllSpecification::All => 1,
            GrantTargetAllSpecification::AllDatabases { databases } => databases.len(),
            GrantTargetAllSpecification::AllSchemas { schemas } => schemas.len(),
        }
    }
}

impl<T: AstInfo> AstDisplay for GrantTargetSpecification<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            GrantTargetSpecification::Object {
                object_type,
                object_spec_inner,
            } => match object_spec_inner {
                GrantTargetSpecificationInner::All(all_spec) => match all_spec {
                    GrantTargetAllSpecification::All => {
                        f.write_str("ALL ");
                        f.write_node(object_type);
                        f.write_str("S");
                    }
                    GrantTargetAllSpecification::AllDatabases { databases } => {
                        f.write_str("ALL ");
                        f.write_node(object_type);
                        f.write_str("S IN DATABASE ");
                        f.write_node(&display::comma_separated(databases));
                    }
                    GrantTargetAllSpecification::AllSchemas { schemas } => {
                        f.write_str("ALL ");
                        f.write_node(object_type);
                        f.write_str("S IN SCHEMA ");
                        f.write_node(&display::comma_separated(schemas));
                    }
                },
                GrantTargetSpecificationInner::Objects { names } => {
                    f.write_node(object_type);
                    f.write_str(" ");
                    f.write_node(&display::comma_separated(names));
                }
            },
            GrantTargetSpecification::System => f.write_str("SYSTEM"),
        }
    }
}
impl_display_t!(GrantTargetSpecification);

/// `GRANT ...`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GrantPrivilegesStatement<T: AstInfo> {
    /// The privileges being granted on an object.
    pub privileges: PrivilegeSpecification,
    /// The objects that are affected by the GRANT.
    pub target: GrantTargetSpecification<T>,
    /// The roles that will granted the privileges.
    pub roles: Vec<T::RoleName>,
}

impl<T: AstInfo> AstDisplay for GrantPrivilegesStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("GRANT ");
        f.write_node(&self.privileges);
        f.write_str(" ON ");
        f.write_node(&self.target);
        f.write_str(" TO ");
        f.write_node(&display::comma_separated(&self.roles));
    }
}
impl_display_t!(GrantPrivilegesStatement);

/// `REVOKE ...`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RevokePrivilegesStatement<T: AstInfo> {
    /// The privileges being revoked.
    pub privileges: PrivilegeSpecification,
    /// The objects that are affected by the REVOKE.
    pub target: GrantTargetSpecification<T>,
    /// The roles that will have privileges revoked.
    pub roles: Vec<T::RoleName>,
}

impl<T: AstInfo> AstDisplay for RevokePrivilegesStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("REVOKE ");
        f.write_node(&self.privileges);
        f.write_str(" ON ");
        f.write_node(&self.target);
        f.write_str(" FROM ");
        f.write_node(&display::comma_separated(&self.roles));
    }
}
impl_display_t!(RevokePrivilegesStatement);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TargetRoleSpecification<T: AstInfo> {
    /// Specific list of roles.
    Roles(Vec<T::RoleName>),
    /// The current role executing the statement.
    CurrentRole,
    /// All current and future roles.
    AllRoles,
}

impl<T: AstInfo> AstDisplay for TargetRoleSpecification<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            TargetRoleSpecification::Roles(roles) => f.write_node(&display::comma_separated(roles)),
            TargetRoleSpecification::CurrentRole => {}
            TargetRoleSpecification::AllRoles => f.write_str("ALL ROLES"),
        }
    }
}
impl_display_t!(TargetRoleSpecification);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AbbreviatedGrantStatement<T: AstInfo> {
    /// The privileges being granted.
    pub privileges: PrivilegeSpecification,
    /// The type of object.
    ///
    /// Note: For views, materialized views, and sources this will be [`ObjectType::Table`].
    pub object_type: ObjectType,
    /// The roles that will granted the privileges.
    pub grantees: Vec<T::RoleName>,
}

impl<T: AstInfo> AstDisplay for AbbreviatedGrantStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("GRANT ");
        f.write_node(&self.privileges);
        f.write_str(" ON ");
        f.write_node(&self.object_type);
        f.write_str("S TO ");
        f.write_node(&display::comma_separated(&self.grantees));
    }
}
impl_display_t!(AbbreviatedGrantStatement);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AbbreviatedRevokeStatement<T: AstInfo> {
    /// The privileges being revoked.
    pub privileges: PrivilegeSpecification,
    /// The type of object.
    ///
    /// Note: For views, materialized views, and sources this will be [`ObjectType::Table`].
    pub object_type: ObjectType,
    /// The roles that the privilege will be revoked from.
    pub revokees: Vec<T::RoleName>,
}

impl<T: AstInfo> AstDisplay for AbbreviatedRevokeStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("REVOKE ");
        f.write_node(&self.privileges);
        f.write_str(" ON ");
        f.write_node(&self.object_type);
        f.write_str("S FROM ");
        f.write_node(&display::comma_separated(&self.revokees));
    }
}
impl_display_t!(AbbreviatedRevokeStatement);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AbbreviatedGrantOrRevokeStatement<T: AstInfo> {
    Grant(AbbreviatedGrantStatement<T>),
    Revoke(AbbreviatedRevokeStatement<T>),
}

impl<T: AstInfo> AbbreviatedGrantOrRevokeStatement<T> {
    pub fn privileges(&self) -> &PrivilegeSpecification {
        match self {
            AbbreviatedGrantOrRevokeStatement::Grant(grant) => &grant.privileges,
            AbbreviatedGrantOrRevokeStatement::Revoke(revoke) => &revoke.privileges,
        }
    }

    pub fn object_type(&self) -> &ObjectType {
        match self {
            AbbreviatedGrantOrRevokeStatement::Grant(grant) => &grant.object_type,
            AbbreviatedGrantOrRevokeStatement::Revoke(revoke) => &revoke.object_type,
        }
    }

    pub fn roles(&self) -> &Vec<T::RoleName> {
        match self {
            AbbreviatedGrantOrRevokeStatement::Grant(grant) => &grant.grantees,
            AbbreviatedGrantOrRevokeStatement::Revoke(revoke) => &revoke.revokees,
        }
    }
}

impl<T: AstInfo> AstDisplay for AbbreviatedGrantOrRevokeStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            AbbreviatedGrantOrRevokeStatement::Grant(grant) => f.write_node(grant),
            AbbreviatedGrantOrRevokeStatement::Revoke(revoke) => f.write_node(revoke),
        }
    }
}
impl_display_t!(AbbreviatedGrantOrRevokeStatement);

/// `ALTER DEFAULT PRIVILEGES ...`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AlterDefaultPrivilegesStatement<T: AstInfo> {
    /// The roles for which created objects are affected.
    pub target_roles: TargetRoleSpecification<T>,
    /// The objects that are affected by the default privilege.
    pub target_objects: GrantTargetAllSpecification<T>,
    /// The privilege to grant or revoke.
    pub grant_or_revoke: AbbreviatedGrantOrRevokeStatement<T>,
}

impl<T: AstInfo> AstDisplay for AlterDefaultPrivilegesStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("ALTER DEFAULT PRIVILEGES");
        match &self.target_roles {
            TargetRoleSpecification::Roles(_) => {
                f.write_str(" FOR ROLE ");
                f.write_node(&self.target_roles);
            }
            TargetRoleSpecification::CurrentRole => {}
            TargetRoleSpecification::AllRoles => {
                f.write_str(" FOR ");
                f.write_node(&self.target_roles);
            }
        }
        match &self.target_objects {
            GrantTargetAllSpecification::All => {}
            GrantTargetAllSpecification::AllDatabases { databases } => {
                f.write_str(" IN DATABASE ");
                f.write_node(&display::comma_separated(databases));
            }
            GrantTargetAllSpecification::AllSchemas { schemas } => {
                f.write_str(" IN SCHEMA ");
                f.write_node(&display::comma_separated(schemas));
            }
        }
        f.write_str(" ");
        f.write_node(&self.grant_or_revoke);
    }
}
impl_display_t!(AlterDefaultPrivilegesStatement);

/// `REASSIGN OWNED ...`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReassignOwnedStatement<T: AstInfo> {
    /// The roles whose owned objects are being reassigned.
    pub old_roles: Vec<T::RoleName>,
    /// The new owner of the objects.
    pub new_role: T::RoleName,
}

impl<T: AstInfo> AstDisplay for ReassignOwnedStatement<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("REASSIGN OWNED BY ");
        f.write_node(&display::comma_separated(&self.old_roles));
        f.write_str(" TO ");
        f.write_node(&self.new_role);
    }
}
impl_display_t!(ReassignOwnedStatement);
