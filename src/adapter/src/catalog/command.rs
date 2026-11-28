// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Catalog commands that can be rechecked when committing a DDL transaction.
//!
//! Statements run against the transaction's working catalog, but the catalog
//! can change before commit. Commands retain the original request and any
//! output we might have sent to the client (aka. the "promise"), so commit can
//! "assert" on that output when re-checking catalog commands against the
//! current catalog.
//!
//! Pending external work abandons the candidate. A continuation rebuilds the
//! prefix before consuming its indexed response, so successful preparation and
//! preparation errors are accepted only when their inputs still match.
//!
//! External mutations are not catalog operations. CREATE CONNECTION retains its allocated
//! ID, generated keys, and successful secret-write completion across retries. ROTATE KEYS
//! uses a pinned publication command instead of retrying its external mutation. Catalog
//! rollback cannot undo either secret writes or changes in external services.

pub(crate) mod connection;
pub(crate) mod drop;
#[cfg(test)]
mod drop_tests;
pub(crate) mod index;
pub(crate) mod namespace;
pub(crate) mod source;
pub(crate) mod source_statement;
pub(crate) mod table;
pub(crate) mod view;

use std::sync::Arc;

use mz_sql::ast::{Raw, Statement};
use mz_sql::catalog::ObjectType;
use mz_sql::plan::{Params, Plan, PlanNotice};
use mz_sql::session::metadata::SessionMetadata;
use tokio::sync::mpsc;

use crate::catalog::command::source_statement::{
    SourceCommand, SourceCompilation, SourceResolution,
};
use crate::catalog::{CatalogState, CommandExecutionError, CommandResponse, CommandWork, Op};
use crate::session::SessionMeta;
use crate::{AdapterError, AdapterNotice};

#[derive(Clone, Debug)]
pub enum CatalogCommand {
    DropObjects(Arc<drop::DropStatement>),
    Rename(Arc<RenameCommand>),
    CatalogResult(Arc<CatalogResultCommand>),
    Description(Arc<DescriptionCommand>),
    AlterConnection(Arc<connection::ConnectionStatement>),
    CreateConnection(Arc<connection::ConnectionStatement>),
    CreateView(Arc<view::ViewStatement>),
    CreateTable(Arc<table::TableStatement>),
    CreateIndex(Arc<index::IndexStatement>),
    CreateNamespace(Arc<namespace::NamespaceStatement>),
    Source(Arc<source_statement::SourceCommand>),
}

impl CatalogCommand {
    pub(crate) fn creates_temporary_item(&self) -> bool {
        match self {
            Self::CreateView(command) => command.is_temporary(),
            Self::CreateTable(command) => command.is_temporary(),
            _ => false,
        }
    }

    /// Resolves a command against the candidate catalog, or requests external work.
    ///
    /// The response must belong to this command's index in the replayed batch.
    pub(super) fn compile(
        &mut self,
        state: &CatalogState,
        compute: Option<&mz_compute_client::controller::ComputeController>,
        index: usize,
        response: Option<CommandResponse>,
    ) -> Result<CommandExecution<'_>, CommandExecutionError> {
        match self {
            Self::DropObjects(command) => {
                assert!(
                    response.is_none(),
                    "drop commands have no external preparation"
                );
                Ok(Arc::make_mut(command).compile(state, compute)?)
            }
            Self::CreateIndex(command) => {
                let response = match response {
                    Some(CommandResponse::Index(response)) => Some(response),
                    None => None,
                    Some(_) => unreachable!("response belongs to its command"),
                };
                let compute = compute.ok_or_else(|| {
                    AdapterError::Internal("index command requires live compute membership".into())
                })?;
                Arc::make_mut(command).compile(state, compute, index, response)
            }
            Self::CreateNamespace(command) => {
                assert!(
                    response.is_none(),
                    "namespace commands have no external preparation"
                );
                Ok(Arc::make_mut(command).compile(state)?)
            }
            Self::CreateTable(command) => {
                assert!(
                    response.is_none(),
                    "table commands have no external preparation"
                );
                Ok(Arc::make_mut(command).compile(state)?)
            }
            Self::CreateView(command) => {
                let command = Arc::make_mut(command);
                let response = match response {
                    Some(CommandResponse::View(response)) => Some(response),
                    None => None,
                    Some(_) => unreachable!("response belongs to its command"),
                };
                match command.resolve(state, response)? {
                    view::ViewResolution::Prepare(request) => Err(CommandExecutionError::Pending {
                        index,
                        work: CommandWork::View(request),
                    }),
                    view::ViewResolution::Ready {
                        ops,
                        name,
                        if_not_exists,
                    } => {
                        let id = command.item_id();
                        Ok(CommandExecution::create_item(
                            ops,
                            id,
                            name,
                            if_not_exists,
                            command.skipped_name_mut(),
                        ))
                    }
                }
            }
            Self::Rename(command) => Ok(CommandExecution::raw(command.execute(state)?)),
            Self::Description(command) => {
                command.execute(state)?;
                Ok(CommandExecution::raw(Vec::new()))
            }
            Self::CatalogResult(command) => {
                command.execute(state)?;
                Ok(CommandExecution::raw(Vec::new()))
            }
            Self::CreateConnection(command) => {
                use connection::CreateConnectionResolution;
                let command = Arc::make_mut(command);
                let (preparation, secret) = match response {
                    Some(CommandResponse::Connection(response)) => (Some(response), None),
                    Some(CommandResponse::ConnectionSecret(response)) => (None, Some(response)),
                    Some(
                        CommandResponse::Source(_)
                        | CommandResponse::View(_)
                        | CommandResponse::Index(_),
                    ) => {
                        unreachable!("response belongs to its command")
                    }
                    None => (None, None),
                };
                let compiled = match command.resolve_create(state, preparation, secret)? {
                    CreateConnectionResolution::Ensure(request) => {
                        return Err(CommandExecutionError::Pending {
                            index,
                            work: CommandWork::ConnectionSecret(request),
                        });
                    }
                    CreateConnectionResolution::Prepare(request) => {
                        return Err(CommandExecutionError::Pending {
                            index,
                            work: CommandWork::Connection(request),
                        });
                    }
                    CreateConnectionResolution::Ready(compiled) => compiled,
                };
                let (id, global_id) = command.create_id();
                let name = compiled.plan.name;
                let ops = vec![Op::CreateItem {
                    id,
                    name: name.clone(),
                    item: mz_catalog::memory::objects::CatalogItem::Connection(
                        mz_catalog::memory::objects::Connection {
                            create_sql: compiled.plan.connection.create_sql,
                            global_id,
                            details: compiled.plan.connection.details,
                            resolved_ids: compiled.resolved_ids,
                        },
                    ),
                    owner_id: command.owner_id(),
                }];
                // Resource checks precede IF NOT EXISTS collision handling.
                state.validate_resource_limits(&ops, command.connection_id())?;
                command.applied(compiled.notices);
                Ok(CommandExecution::create_item(
                    ops,
                    id,
                    name.item,
                    compiled.plan.if_not_exists,
                    command.skipped_name_mut(),
                ))
            }
            Self::AlterConnection(command) => {
                use connection::ConnectionResolution;
                let command = Arc::make_mut(command);
                let response = match response {
                    Some(CommandResponse::Connection(response)) => Some(response),
                    Some(
                        CommandResponse::Source(_)
                        | CommandResponse::ConnectionSecret(_)
                        | CommandResponse::View(_)
                        | CommandResponse::Index(_),
                    ) => {
                        unreachable!("response belongs to its command")
                    }
                    None => None,
                };
                let compiled = match command.resolve_alter(state, response)? {
                    ConnectionResolution::Prepare(request) => {
                        return Err(CommandExecutionError::Pending {
                            index,
                            work: CommandWork::Connection(request),
                        });
                    }
                    ConnectionResolution::Ready(compiled) => compiled,
                };
                let ops = match compiled.connection {
                    Some((id, connection, _)) => {
                        use mz_storage_types::AlterCompatible;
                        let entry = state.get_entry(&id);
                        let current = entry.connection().map_err(AdapterError::from)?;
                        current
                            .details
                            .to_connection()
                            .alter_compatible(
                                current.global_id,
                                &connection.details.to_connection(),
                            )
                            .map_err(mz_storage_types::controller::StorageError::from)
                            .map_err(AdapterError::from)?;
                        vec![Op::UpdateItem {
                            id,
                            name: entry.name().clone(),
                            to_item: mz_catalog::memory::objects::CatalogItem::Connection(
                                connection,
                            ),
                        }]
                    }
                    None => Vec::new(),
                };
                command.applied(compiled.notices);
                Ok(CommandExecution::raw(ops))
            }
            Self::Source(command) => {
                let command = Arc::make_mut(command);
                let resolution = match response {
                    Some(CommandResponse::Source(response)) => {
                        command.statement.finish(state, response)?
                    }
                    Some(
                        CommandResponse::Connection(_)
                        | CommandResponse::ConnectionSecret(_)
                        | CommandResponse::View(_)
                        | CommandResponse::Index(_),
                    ) => {
                        unreachable!("response belongs to its command")
                    }
                    None => command.statement.resolve(state)?,
                };
                match resolution {
                    SourceResolution::Prepare(request) => Err(CommandExecutionError::Pending {
                        index,
                        work: CommandWork::Prepare(request),
                    }),
                    SourceResolution::Allocate(keys) => Err(CommandExecutionError::Pending {
                        index,
                        work: CommandWork::Allocate(keys),
                    }),
                    SourceResolution::Ready(mut compiled) => Ok(CommandExecution {
                        ops: std::mem::take(&mut compiled.ops),
                        outcome: Some(ExecutionOutcome::Source(command, compiled)),
                    }),
                }
            }
        }
    }

    /// The planning notices recorded during preparation.
    pub(crate) fn notices(&self) -> &[PlanNotice] {
        match self {
            Self::Rename(command) => command.notices(),
            Self::CatalogResult(command) => &command.notices,
            Self::Description(_) => &[],
            Self::AlterConnection(command) | Self::CreateConnection(command) => command.notices(),
            Self::Source(command) => command.notices(),
            Self::CreateView(command) => command.notices(),
            Self::CreateTable(command) => command.notices(),
            Self::CreateIndex(command) => command.notices(),
            Self::DropObjects(command) => command.notices(),
            Self::CreateNamespace(command) => command.notices(),
        }
    }
}

/// Catalog writes and the outcome validation required before publishing them.
///
/// Run any collision probe in an isolated transaction before taking the writes.
/// For executions that are not skipped, apply the writes in order and call
/// `finish` before executing the next command.
pub(super) struct CommandExecution<'a> {
    ops: Vec<Op>,
    outcome: Option<ExecutionOutcome<'a>>,
}

enum ExecutionOutcome<'a> {
    CreateNamespace {
        kind: namespace::NamespaceKind,
        name: String,
        skipped_notice: &'a mut Option<AdapterNotice>,
    },
    CreateItem {
        skipped_name: &'a mut Option<String>,
        id: mz_repr::CatalogItemId,
        name: String,
    },
    Source(&'a mut SourceCommand, Box<SourceCompilation>),
}

impl<'a> CommandExecution<'a> {
    fn create_item(
        ops: Vec<Op>,
        id: mz_repr::CatalogItemId,
        name: String,
        if_not_exists: bool,
        skipped_name: &'a mut Option<String>,
    ) -> Self {
        *skipped_name = None;
        Self {
            ops,
            outcome: if_not_exists.then_some(ExecutionOutcome::CreateItem {
                skipped_name,
                id,
                name,
            }),
        }
    }

    pub(super) fn raw(ops: Vec<Op>) -> Self {
        Self { ops, outcome: None }
    }

    pub(super) fn collision_probe_ops(&self) -> Option<&[Op]> {
        match &self.outcome {
            Some(ExecutionOutcome::Source(command, compiled)) => command
                .needs_collision_probe(compiled)
                .then_some(self.ops.as_slice()),
            Some(
                ExecutionOutcome::CreateItem { .. } | ExecutionOutcome::CreateNamespace { .. },
            ) => Some(self.ops.as_slice()),
            None => None,
        }
    }

    /// Validates a requested collision probe, consuming skipped executions.
    pub(super) fn finish_probe(
        mut self,
        result: Result<(), AdapterError>,
    ) -> Result<Option<Self>, AdapterError> {
        let skipped = match self.outcome.as_mut().expect("collision probe requested") {
            ExecutionOutcome::CreateNamespace {
                kind,
                name,
                skipped_notice,
            } => match result {
                Ok(()) => false,
                Err(AdapterError::Catalog(mz_catalog::memory::error::Error {
                    kind: mz_catalog::memory::error::ErrorKind::Sql(error),
                })) if matches!(
                    (&*kind, &error),
                    (
                        namespace::NamespaceKind::Database,
                        mz_sql::catalog::CatalogError::DatabaseAlreadyExists(_)
                    ) | (
                        namespace::NamespaceKind::Schema,
                        mz_sql::catalog::CatalogError::SchemaAlreadyExists(_)
                    )
                ) =>
                {
                    **skipped_notice = Some(match kind {
                        namespace::NamespaceKind::Database => {
                            AdapterNotice::DatabaseAlreadyExists { name: name.clone() }
                        }
                        namespace::NamespaceKind::Schema => {
                            AdapterNotice::SchemaAlreadyExists { name: name.clone() }
                        }
                    });
                    true
                }
                Err(error) => return Err(error),
            },
            ExecutionOutcome::CreateItem {
                skipped_name,
                id,
                name,
            } => match result {
                Ok(()) => false,
                Err(AdapterError::Catalog(mz_catalog::memory::error::Error {
                    kind:
                        mz_catalog::memory::error::ErrorKind::Sql(
                            mz_sql::catalog::CatalogError::ItemAlreadyExists(attempted_id, _),
                        ),
                })) if attempted_id == *id => {
                    **skipped_name = Some(name.clone());
                    true
                }
                Err(error) => return Err(error),
            },
            ExecutionOutcome::Source(command, compiled) => {
                let (_, outcome) = compiled.classify_result(result)?;
                command.check_outcome(outcome)?;
                command.skipped_name().is_some()
            }
        };
        if skipped { Ok(None) } else { Ok(Some(self)) }
    }

    pub(super) fn take_ops(&mut self) -> Vec<Op> {
        std::mem::take(&mut self.ops)
    }

    /// Checks the observed outcome and propagates the result unless that outcome changed.
    pub(super) fn finish<T>(self, result: Result<T, AdapterError>) -> Result<T, AdapterError> {
        if let Some(ExecutionOutcome::Source(command, compiled)) = self.outcome {
            match &result {
                Ok(_) => command.check_outcome(compiled.created())?,
                Err(error) => {
                    if let Some(outcome) = compiled.skipped(error) {
                        command.check_outcome(outcome)?;
                    }
                }
            }
        }
        result
    }
}

/// A rename or swap command.
#[derive(Debug)]
pub struct RenameCommand {
    statement: Arc<Statement<Raw>>,
    params: Params,
    session: SessionMeta,
    outcome: RenameOutcome,
}

#[derive(Debug, PartialEq)]
struct RenameOutcome {
    object_type: ObjectType,
    changed: bool,
    notices: Vec<PlanNotice>,
}

impl RenameCommand {
    /// Prepares a rename or swap and records the outcome that replay must
    /// preserve.
    ///
    /// Returns the initial plan for statement execution. The statement must
    /// plan to a supported rename, swap, or ALTER no-op.
    ///
    /// # Panics
    ///
    /// Panics if the statement plans successfully to anything other than a
    /// supported rename, swap, or ALTER no-op.
    pub(crate) fn prepare(
        state: &CatalogState,
        statement: Arc<Statement<Raw>>,
        params: Params,
        session: SessionMeta,
    ) -> Result<(Self, Plan), AdapterError> {
        let (plan, _, notices) = plan_command(state, &statement, &params, &session)?;
        let (ops, object_type) = rename_ops(&plan);
        Ok((
            Self {
                statement,
                params,
                session,
                outcome: RenameOutcome {
                    object_type,
                    changed: !ops.is_empty(),
                    notices,
                },
            },
            plan,
        ))
    }

    /// The planning notices recorded during preparation.
    pub(crate) fn notices(&self) -> &[PlanNotice] {
        &self.outcome.notices
    }

    /// The object type used in the statement's completion response.
    pub(crate) fn object_type(&self) -> ObjectType {
        self.outcome.object_type
    }

    /// Returns writes for `state` after rechecking planning and authorization.
    ///
    /// Rejects changes to the object type, whether the command is a no-op, or
    /// the planning notices. The caller must apply the returned writes.
    pub(super) fn execute(&self, state: &CatalogState) -> Result<Vec<Op>, AdapterError> {
        let (plan, _, notices) = plan_command(state, &self.statement, &self.params, &self.session)?;
        let (ops, object_type) = rename_ops(&plan);
        let outcome = RenameOutcome {
            object_type,
            changed: !ops.is_empty(),
            notices,
        };
        if outcome != self.outcome {
            return Err(AdapterError::DDLTransactionOutcomeChanged);
        }
        Ok(ops)
    }
}

/// A catalog query whose result and notices must remain unchanged at DDL
/// commit.
///
/// In a way, this puts an "assert" into the list of commands for the DDL
/// transaction, that enforces that the output stays what we have already
/// reported, when committing.
#[derive(Debug)]
pub struct CatalogResultCommand {
    statement: Arc<Statement<Raw>>,
    params: Params,
    session: SessionMeta,
    row: mz_repr::Row,
    notices: Vec<PlanNotice>,
}

impl CatalogResultCommand {
    /// Whether this statement supports retaining its catalog result for replay.
    pub(crate) fn supports_statement(statement: &Statement<Raw>) -> bool {
        use mz_sql::ast::ShowStatement;
        let Statement::Show(show) = statement else {
            return false;
        };
        match show {
            ShowStatement::ShowCreateView(_)
            | ShowStatement::ShowCreateMaterializedView(_)
            | ShowStatement::ShowCreateSource(_)
            | ShowStatement::ShowCreateTable(_)
            | ShowStatement::ShowCreateSink(_)
            | ShowStatement::ShowCreateMetricSink(_)
            | ShowStatement::ShowCreateIndex(_)
            | ShowStatement::ShowCreateConnection(_)
            | ShowStatement::ShowCreateCluster(_)
            | ShowStatement::ShowCreateType(_) => true,
            ShowStatement::ShowObjects(_)
            | ShowStatement::ShowColumns(_)
            | ShowStatement::ShowVariable(_)
            | ShowStatement::InspectShard(_) => false,
        }
    }

    /// Prepares a catalog query and records the row and notices replay must
    /// preserve.
    ///
    /// Returns the initial plan for statement execution. Fails if the plan does
    /// not provide a supported catalog result.
    pub(crate) fn prepare(
        state: &CatalogState,
        statement: Arc<Statement<Raw>>,
        params: Params,
        session: SessionMeta,
    ) -> Result<(Self, Plan), AdapterError> {
        let (plan, _, notices) = plan_command(state, &statement, &params, &session)?;
        let row = Self::result(&plan)?;
        let command = Self {
            statement,
            params,
            session,
            row,
            notices,
        };
        Ok((command, plan))
    }

    /// Rechecks planning and authorization and requires the recorded row and
    /// notices.
    pub(in crate::catalog) fn execute(&self, state: &CatalogState) -> Result<(), AdapterError> {
        let (plan, _, notices) = plan_command(state, &self.statement, &self.params, &self.session)?;
        if Self::result(&plan)? == self.row && notices == self.notices {
            Ok(())
        } else {
            Err(AdapterError::DDLTransactionOutcomeChanged)
        }
    }

    /// Returns the row to send to the client, or an error for an unsupported
    /// plan.
    pub(crate) fn result(plan: &Plan) -> Result<mz_repr::Row, AdapterError> {
        match plan {
            Plan::ShowCreate(show) => Ok(show.row.clone()),
            _ => Err(AdapterError::Unsupported(
                "non-deterministic catalog result",
            )),
        }
    }
}

/// A statement description that must remain unchanged at DDL commit.
///
/// In a way, this puts an "assert" into the list of commands for the DDL
/// transaction, that enforces that the output stays what we have already
/// reported, when committing.
#[derive(Debug)]
pub struct DescriptionCommand {
    statement: Statement<Raw>,
    param_types: Vec<Option<mz_repr::SqlScalarType>>,
    session: SessionMeta,
    expected: mz_sql::plan::StatementDesc,
}

impl DescriptionCommand {
    /// Records a statement description for validation at DDL commit.
    ///
    /// `expected` must be the description obtained for this statement with the
    /// supplied parameter types and session metadata. This does not validate
    /// it.
    pub(crate) fn new(
        statement: Statement<Raw>,
        param_types: Vec<Option<mz_repr::SqlScalarType>>,
        session: SessionMeta,
        expected: mz_sql::plan::StatementDesc,
    ) -> Self {
        Self {
            statement,
            param_types,
            session,
            expected,
        }
    }

    /// Requires the statement to retain its recorded description in `state`.
    pub(in crate::catalog) fn execute(&self, state: &CatalogState) -> Result<(), AdapterError> {
        let (tx, _rx) = mpsc::unbounded_channel();
        let catalog = state.for_session_metadata(&self.session, tx);
        let (statement, _) = mz_sql::names::resolve(&catalog, self.statement.clone())?;
        let current =
            mz_sql::plan::describe(self.session.pcx(), &catalog, statement, &self.param_types)?;
        if current == self.expected {
            Ok(())
        } else {
            Err(AdapterError::DDLTransactionOutcomeChanged)
        }
    }
}

fn plan_command(
    state: &CatalogState,
    statement: &Statement<Raw>,
    params: &Params,
    session: &SessionMeta,
) -> Result<(Plan, mz_sql::names::ResolvedIds, Vec<PlanNotice>), AdapterError> {
    let (result, notices) = plan_command_with_notices(state, statement, params, session);
    result.map(|(plan, resolved_ids)| (plan, resolved_ids, notices))
}

/// Retains planning notices even when planning or authorization fails.
fn plan_command_with_notices(
    state: &CatalogState,
    statement: &Statement<Raw>,
    params: &Params,
    session: &SessionMeta,
) -> (
    Result<(Plan, mz_sql::names::ResolvedIds), AdapterError>,
    Vec<PlanNotice>,
) {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let result = (|| {
        let catalog = state.for_session_metadata(session, tx);
        let (statement, resolved_ids) = mz_sql::names::resolve(&catalog, statement.clone())?;
        let (plan, sql_impl_ids) = mz_sql::plan::plan(
            Some(session.pcx()),
            &catalog,
            statement,
            params,
            &resolved_ids,
        )?;
        mz_sql::rbac::check_plan(
            &catalog,
            None,
            session,
            &plan,
            None,
            &resolved_ids,
            &sql_impl_ids,
        )?;
        Ok((plan, resolved_ids))
    })();
    let mut notices = Vec::new();
    while let Ok(notice) = rx.try_recv() {
        match notice {
            AdapterNotice::PlanNotice(notice) => notices.push(notice),
            _ => unreachable!("planning only emits plan notices"),
        }
    }
    (result, notices)
}

/// Returns the rename/swap ops for the given `Plan`, along with the object
/// type.
///
/// # Panics
///
/// Panics if `plan` is not a supported rename, swap, or ALTER no-op.
fn rename_ops(plan: &Plan) -> (Vec<Op>, ObjectType) {
    match plan {
        Plan::AlterNoop(plan) => (vec![], plan.object_type),
        Plan::AlterItemRename(plan) => (
            vec![Op::RenameItem {
                id: plan.id,
                current_full_name: plan.current_full_name.clone(),
                to_name: plan.to_name.clone(),
            }],
            plan.object_type,
        ),
        Plan::AlterSchemaRename(plan) => (
            vec![Op::RenameSchema {
                database_spec: plan.cur_schema_spec.0,
                schema_spec: plan.cur_schema_spec.1,
                new_name: plan.new_schema_name.clone(),
                check_reserved_names: true,
            }],
            ObjectType::Schema,
        ),
        Plan::AlterSchemaSwap(plan) => (
            vec![
                Op::RenameSchema {
                    database_spec: plan.schema_a_spec.0,
                    schema_spec: plan.schema_a_spec.1,
                    new_name: plan.name_temp.clone(),
                    check_reserved_names: false,
                },
                Op::RenameSchema {
                    database_spec: plan.schema_b_spec.0,
                    schema_spec: plan.schema_b_spec.1,
                    new_name: plan.schema_a_name.clone(),
                    check_reserved_names: false,
                },
                Op::RenameSchema {
                    database_spec: plan.schema_a_spec.0,
                    schema_spec: plan.schema_a_spec.1,
                    new_name: plan.schema_b_name.clone(),
                    check_reserved_names: false,
                },
            ],
            ObjectType::Schema,
        ),
        Plan::AlterClusterRename(plan) => (
            vec![Op::RenameCluster {
                id: plan.id,
                name: plan.name.clone(),
                to_name: plan.to_name.clone(),
                check_reserved_names: true,
            }],
            ObjectType::Cluster,
        ),
        Plan::AlterClusterSwap(plan) => (
            vec![
                Op::RenameCluster {
                    id: plan.id_a,
                    name: plan.name_a.clone(),
                    to_name: plan.name_temp.clone(),
                    check_reserved_names: false,
                },
                Op::RenameCluster {
                    id: plan.id_b,
                    name: plan.name_b.clone(),
                    to_name: plan.name_a.clone(),
                    check_reserved_names: false,
                },
                Op::RenameCluster {
                    id: plan.id_a,
                    name: plan.name_temp.clone(),
                    to_name: plan.name_b.clone(),
                    check_reserved_names: false,
                },
            ],
            ObjectType::Cluster,
        ),
        Plan::AlterClusterReplicaRename(plan) => (
            vec![Op::RenameClusterReplica {
                cluster_id: plan.cluster_id,
                replica_id: plan.replica_id,
                name: plan.name.clone(),
                to_name: plan.to_name.clone(),
            }],
            ObjectType::ClusterReplica,
        ),
        _ => unreachable!("rename/swap planning must produce a rename or no-op plan"),
    }
}

pub(crate) fn plan_subsource(
    catalog: &crate::catalog::ConnCatalog<'_>,
    pcx: &mz_sql::plan::PlanContext,
    params: &Params,
    statement: mz_sql::ast::CreateSubsourceStatement<mz_sql::names::Aug>,
    item_id: mz_repr::CatalogItemId,
    global_id: mz_repr::GlobalId,
) -> Result<mz_sql::plan::CreateSourcePlanBundle, AdapterError> {
    let resolved_ids = mz_sql::names::visit_dependencies(catalog, &statement);
    let (plan, _sql_impl_ids) = mz_sql::plan::plan(
        Some(pcx),
        catalog,
        Statement::CreateSubsource(statement),
        params,
        &resolved_ids,
    )?;
    let Plan::CreateSource(plan) = plan else {
        unreachable!("subsource planning produces a source plan");
    };
    Ok(mz_sql::plan::CreateSourcePlanBundle {
        item_id,
        global_id,
        plan,
        resolved_ids,
        available_source_references: None,
    })
}
