// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::iter;
use std::num::{NonZeroI64, NonZeroUsize};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use futures::future::{BoxFuture, FutureExt};
use futures::{Future, StreamExt, future};
use itertools::Itertools;
use mz_adapter_types::compaction::CompactionWindow;
use mz_adapter_types::connection::ConnectionId;
use mz_adapter_types::dyncfgs::{ENABLE_MULTI_REPLICA_SOURCES, ENABLE_PASSWORD_AUTH};
use mz_catalog::memory::objects::{
    CatalogItem, Connection, DataSourceDesc, Sink, Source, Table, TableDataSource, Type,
};
use mz_cloud_resources::VpcEndpointConfig;
use mz_expr::{
    CollectionPlan, MapFilterProject, OptimizedMirRelationExpr, ResultSpec, RowSetFinishing,
};
use mz_ore::cast::CastFrom;
use mz_ore::collections::{CollectionExt, HashSet};
use mz_ore::task::{self, JoinHandle, spawn};
use mz_ore::tracing::OpenTelemetryContext;
use mz_ore::{assert_none, instrument};
use mz_repr::adt::jsonb::Jsonb;
use mz_repr::adt::mz_acl_item::{MzAclItem, PrivilegeMap};
use mz_repr::explain::ExprHumanizer;
use mz_repr::explain::json::json_string;
use mz_repr::role_id::RoleId;
use mz_repr::{
    CatalogItemId, Datum, Diff, GlobalId, RelationVersion, RelationVersionSelector, Row, RowArena,
    RowIterator, Timestamp,
};
use mz_sql::ast::{
    AlterSourceAddSubsourceOption, CreateSinkOption, CreateSinkOptionName, CreateSourceOptionName,
    CreateSubsourceOption, CreateSubsourceOptionName, SqlServerConfigOption,
    SqlServerConfigOptionName,
};
use mz_sql::ast::{CreateSubsourceStatement, MySqlConfigOptionName, UnresolvedItemName};
use mz_sql::catalog::{
    CatalogCluster, CatalogClusterReplica, CatalogDatabase, CatalogError,
    CatalogItem as SqlCatalogItem, CatalogItemType, CatalogRole, CatalogSchema, CatalogTypeDetails,
    ErrorMessageObjectDescription, ObjectType, RoleAttributesRaw, RoleVars, SessionCatalog,
};
use mz_sql::names::{
    Aug, ObjectId, QualifiedItemName, ResolvedDatabaseSpecifier, ResolvedIds, ResolvedItemName,
    SchemaSpecifier, SystemObjectId,
};
use mz_sql::plan::{
    AlterMaterializedViewApplyReplacementPlan, ConnectionDetails, NetworkPolicyRule,
    StatementContext,
};
use mz_sql::pure::{PurifiedSourceExport, generate_subsource_statements};
use mz_storage_types::sinks::StorageSinkDesc;
use mz_storage_types::sources::GenericSourceConnection;
// Import `plan` module, but only import select elements to avoid merge conflicts on use statements.
use mz_sql::plan::{
    AlterConnectionAction, AlterConnectionPlan, CreateSourcePlanBundle, ExplainSinkSchemaPlan,
    Explainee, ExplaineeStatement, MutationKind, Params, Plan, PlannedAlterRoleOption,
    PlannedRoleVariable, QueryWhen, SideEffectingFunc, UpdatePrivilege, VariableValue,
};
use mz_sql::session::metadata::SessionMetadata;
use mz_sql::session::user::UserKind;
use mz_sql::session::vars::{
    self, IsolationLevel, NETWORK_POLICY, OwnedVarInput, SCHEMA_ALIAS,
    TRANSACTION_ISOLATION_VAR_NAME, Var, VarError, VarInput,
};
use mz_sql::{plan, rbac};
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{
    ConnectionOption, ConnectionOptionName, CreateSourceConnection, DeferredItemName,
    MySqlConfigOption, PgConfigOption, PgConfigOptionName, Statement, TransactionMode,
    WithOptionValue,
};
use mz_ssh_util::keys::SshKeyPairSet;
use mz_storage_client::controller::ExportDescription;
use mz_storage_types::AlterCompatible;
use mz_storage_types::connections::inline::IntoInlineConnection;
use mz_storage_types::controller::StorageError;
use mz_transform::dataflow::DataflowMetainfo;
use smallvec::SmallVec;
use timely::progress::Antichain;
use tokio::sync::{oneshot, watch};
use tracing::{Instrument, Span, info, warn};

use crate::catalog::{self, Catalog, ConnCatalog, DropObjectInfo, UpdatePrivilegeVariant};
use crate::command::{ExecuteResponse, Response};
use crate::coord::appends::{BuiltinTableAppendNotify, DeferredOp, DeferredPlan, PendingWriteTxn};
use crate::coord::sequencer::emit_optimizer_notices;
use crate::coord::{
    AlterConnectionValidationReady, AlterSinkReadyContext, Coordinator,
    CreateConnectionValidationReady, DeferredPlanStatement, ExecuteContext, ExplainContext,
    Message, NetworkPolicyError, PendingRead, PendingReadTxn, PendingTxn, PendingTxnResponse,
    PlanValidity, StageResult, Staged, StagedContext, TargetCluster, WatchSetResponse,
    validate_ip_with_policy_rules,
};
use crate::error::AdapterError;
use crate::notice::{AdapterNotice, DroppedInUseIndex};
use crate::optimize::dataflows::{EvalTime, ExprPrepStyle, prep_scalar_expr};
use crate::optimize::{self, Optimize};
use crate::session::{
    EndTransactionAction, RequireLinearization, Session, TransactionOps, TransactionStatus,
    WriteLocks, WriteOp,
};
use crate::util::{ClientTransmitter, ResultExt, viewable_variables};
use crate::{PeekResponseUnary, ReadHolds};

mod cluster;
mod copy_from;
mod create_continual_task;
mod create_index;
mod create_materialized_view;
mod create_view;
mod explain_timestamp;
mod peek;
mod secret;
mod subscribe;

/// Attempts to evaluate an expression. If an error is returned then the error is sent
/// to the client and the function is exited.
macro_rules! return_if_err {
    ($expr:expr, $ctx:expr) => {
        match $expr {
            Ok(v) => v,
            Err(e) => return $ctx.retire(Err(e.into())),
        }
    };
}

pub(super) use return_if_err;

struct DropOps {
    ops: Vec<catalog::Op>,
    dropped_active_db: bool,
    dropped_active_cluster: bool,
    dropped_in_use_indexes: Vec<DroppedInUseIndex>,
}

// A bundle of values returned from create_source_inner
struct CreateSourceInner {
    ops: Vec<catalog::Op>,
    sources: Vec<(CatalogItemId, Source)>,
    if_not_exists_ids: BTreeMap<CatalogItemId, QualifiedItemName>,
}

impl Coordinator {
    /// Sequences the next staged of a [Staged] plan. This is designed for use with plans that
    /// execute both on and off of the coordinator thread. Stages can either produce another stage
    /// to execute or a final response. An explicit [Span] is passed to allow for convenient
    /// tracing.
    pub(crate) async fn sequence_staged<S>(
        &mut self,
        mut ctx: S::Ctx,
        parent_span: Span,
        mut stage: S,
    ) where
        S: Staged + 'static,
        S::Ctx: Send + 'static,
    {
        return_if_err!(stage.validity().check(self.catalog()), ctx);
        loop {
            let mut cancel_enabled = stage.cancel_enabled();
            if let Some(session) = ctx.session() {
                if cancel_enabled {
                    // Channel to await cancellation. Insert a new channel, but check if the previous one
                    // was already canceled.
                    if let Some((_prev_tx, prev_rx)) = self
                        .staged_cancellation
                        .insert(session.conn_id().clone(), watch::channel(false))
                    {
                        let was_canceled = *prev_rx.borrow();
                        if was_canceled {
                            ctx.retire(Err(AdapterError::Canceled));
                            return;
                        }
                    }
                } else {
                    // If no cancel allowed, remove it so handle_spawn doesn't observe any previous value
                    // when cancel_enabled may have been true on an earlier stage.
                    self.staged_cancellation.remove(session.conn_id());
                }
            } else {
                cancel_enabled = false
            };
            let next = stage
                .stage(self, &mut ctx)
                .instrument(parent_span.clone())
                .await;
            let res = return_if_err!(next, ctx);
            stage = match res {
                StageResult::Handle(handle) => {
                    let internal_cmd_tx = self.internal_cmd_tx.clone();
                    self.handle_spawn(ctx, handle, cancel_enabled, move |ctx, next| {
                        let _ = internal_cmd_tx.send(next.message(ctx, parent_span));
                    });
                    return;
                }
                StageResult::HandleRetire(handle) => {
                    self.handle_spawn(ctx, handle, cancel_enabled, move |ctx, resp| {
                        ctx.retire(Ok(resp));
                    });
                    return;
                }
                StageResult::Response(resp) => {
                    ctx.retire(Ok(resp));
                    return;
                }
                StageResult::Immediate(stage) => *stage,
            }
        }
    }

    fn handle_spawn<C, T, F>(
        &self,
        ctx: C,
        handle: JoinHandle<Result<T, AdapterError>>,
        cancel_enabled: bool,
        f: F,
    ) where
        C: StagedContext + Send + 'static,
        T: Send + 'static,
        F: FnOnce(C, T) + Send + 'static,
    {
        let rx: BoxFuture<()> = if let Some((_tx, rx)) = ctx
            .session()
            .and_then(|session| self.staged_cancellation.get(session.conn_id()))
        {
            let mut rx = rx.clone();
            Box::pin(async move {
                // Wait for true or dropped sender.
                let _ = rx.wait_for(|v| *v).await;
                ()
            })
        } else {
            Box::pin(future::pending())
        };
        spawn(|| "sequence_staged", async move {
            tokio::select! {
                res = handle => {
                    let next = return_if_err!(res, ctx);
                    f(ctx, next);
                }
                _ = rx, if cancel_enabled => {
                    ctx.retire(Err(AdapterError::Canceled));
                }
            }
        });
    }

    async fn create_source_inner(
        &self,
        session: &Session,
        plans: Vec<plan::CreateSourcePlanBundle>,
    ) -> Result<CreateSourceInner, AdapterError> {
        let mut ops = vec![];
        let mut sources = vec![];

        let if_not_exists_ids = plans
            .iter()
            .filter_map(
                |plan::CreateSourcePlanBundle {
                     item_id,
                     global_id: _,
                     plan,
                     resolved_ids: _,
                     available_source_references: _,
                 }| {
                    if plan.if_not_exists {
                        Some((*item_id, plan.name.clone()))
                    } else {
                        None
                    }
                },
            )
            .collect::<BTreeMap<_, _>>();

        for plan::CreateSourcePlanBundle {
            item_id,
            global_id,
            mut plan,
            resolved_ids,
            available_source_references,
        } in plans
        {
            let name = plan.name.clone();

            match plan.source.data_source {
                plan::DataSourceDesc::Ingestion(ref desc)
                | plan::DataSourceDesc::OldSyntaxIngestion { ref desc, .. } => {
                    let cluster_id = plan
                        .in_cluster
                        .expect("ingestion plans must specify cluster");
                    match desc.connection {
                        GenericSourceConnection::Postgres(_)
                        | GenericSourceConnection::MySql(_)
                        | GenericSourceConnection::SqlServer(_)
                        | GenericSourceConnection::Kafka(_)
                        | GenericSourceConnection::LoadGenerator(_) => {
                            if let Some(cluster) = self.catalog().try_get_cluster(cluster_id) {
                                let enable_multi_replica_sources = ENABLE_MULTI_REPLICA_SOURCES
                                    .get(self.catalog().system_config().dyncfgs());

                                if !enable_multi_replica_sources && cluster.replica_ids().len() > 1
                                {
                                    return Err(AdapterError::Unsupported(
                                        "sources in clusters with >1 replicas",
                                    ));
                                }
                            }
                        }
                    }
                }
                plan::DataSourceDesc::Webhook { .. } => {
                    let cluster_id = plan.in_cluster.expect("webhook plans must specify cluster");
                    if let Some(cluster) = self.catalog().try_get_cluster(cluster_id) {
                        let enable_multi_replica_sources = ENABLE_MULTI_REPLICA_SOURCES
                            .get(self.catalog().system_config().dyncfgs());

                        if !enable_multi_replica_sources {
                            if cluster.replica_ids().len() > 1 {
                                return Err(AdapterError::Unsupported(
                                    "webhook sources in clusters with >1 replicas",
                                ));
                            }
                        }
                    }
                }
                plan::DataSourceDesc::IngestionExport { .. } | plan::DataSourceDesc::Progress => {}
            }

            // Attempt to reduce the `CHECK` expression, we timeout if this takes too long.
            if let mz_sql::plan::DataSourceDesc::Webhook {
                validate_using: Some(validate),
                ..
            } = &mut plan.source.data_source
            {
                if let Err(reason) = validate.reduce_expression().await {
                    self.metrics
                        .webhook_validation_reduce_failures
                        .with_label_values(&[reason])
                        .inc();
                    return Err(AdapterError::Internal(format!(
                        "failed to reduce check expression, {reason}"
                    )));
                }
            }

            // If this source contained a set of available source references, update the
            // source references catalog table.
            let mut reference_ops = vec![];
            if let Some(references) = &available_source_references {
                reference_ops.push(catalog::Op::UpdateSourceReferences {
                    source_id: item_id,
                    references: references.clone().into(),
                });
            }

            let source = Source::new(plan, global_id, resolved_ids, None, false);
            ops.push(catalog::Op::CreateItem {
                id: item_id,
                name,
                item: CatalogItem::Source(source.clone()),
                owner_id: *session.current_role_id(),
            });
            sources.push((item_id, source));
            // These operations must be executed after the source is added to the catalog.
            ops.extend(reference_ops);
        }

        Ok(CreateSourceInner {
            ops,
            sources,
            if_not_exists_ids,
        })
    }

    /// Subsources are planned differently from other statements because they
    /// are typically synthesized from other statements, e.g. `CREATE SOURCE`.
    /// Because of this, we have usually "missed" the opportunity to plan them
    /// through the normal statement execution life cycle (the exception being
    /// during bootstrapping).
    ///
    /// The caller needs to provide a `CatalogItemId` and `GlobalId` for the sub-source.
    pub(crate) fn plan_subsource(
        &self,
        session: &Session,
        params: &mz_sql::plan::Params,
        subsource_stmt: CreateSubsourceStatement<mz_sql::names::Aug>,
        item_id: CatalogItemId,
        global_id: GlobalId,
    ) -> Result<CreateSourcePlanBundle, AdapterError> {
        let catalog = self.catalog().for_session(session);
        let resolved_ids = mz_sql::names::visit_dependencies(&catalog, &subsource_stmt);

        let plan = self.plan_statement(
            session,
            Statement::CreateSubsource(subsource_stmt),
            params,
            &resolved_ids,
        )?;
        let plan = match plan {
            Plan::CreateSource(plan) => plan,
            _ => unreachable!(),
        };
        Ok(CreateSourcePlanBundle {
            item_id,
            global_id,
            plan,
            resolved_ids,
            available_source_references: None,
        })
    }

    /// Prepares an `ALTER SOURCE...ADD SUBSOURCE`.
    pub(crate) async fn plan_purified_alter_source_add_subsource(
        &mut self,
        session: &Session,
        params: Params,
        source_name: ResolvedItemName,
        options: Vec<AlterSourceAddSubsourceOption<Aug>>,
        subsources: BTreeMap<UnresolvedItemName, PurifiedSourceExport>,
    ) -> Result<(Plan, ResolvedIds), AdapterError> {
        let mut subsource_plans = Vec::with_capacity(subsources.len());

        // Generate subsource statements
        let conn_catalog = self.catalog().for_system_session();
        let pcx = plan::PlanContext::zero();
        let scx = StatementContext::new(Some(&pcx), &conn_catalog);

        let entry = self.catalog().get_entry(source_name.item_id());
        let source = entry.source().ok_or_else(|| {
            AdapterError::internal(
                "plan alter source",
                format!("expected Source found {entry:?}"),
            )
        })?;

        let item_id = entry.id();
        let ingestion_id = source.global_id();
        let subsource_stmts = generate_subsource_statements(&scx, source_name, subsources)?;

        let id_ts = self.get_catalog_write_ts().await;
        let ids = self
            .catalog()
            .allocate_user_ids(u64::cast_from(subsource_stmts.len()), id_ts)
            .await?;
        for (subsource_stmt, (item_id, global_id)) in
            subsource_stmts.into_iter().zip_eq(ids.into_iter())
        {
            let s = self.plan_subsource(session, &params, subsource_stmt, item_id, global_id)?;
            subsource_plans.push(s);
        }

        let action = mz_sql::plan::AlterSourceAction::AddSubsourceExports {
            subsources: subsource_plans,
            options,
        };

        Ok((
            Plan::AlterSource(mz_sql::plan::AlterSourcePlan {
                item_id,
                ingestion_id,
                action,
            }),
            ResolvedIds::empty(),
        ))
    }

    /// Prepares an `ALTER SOURCE...REFRESH REFERENCES`.
    pub(crate) fn plan_purified_alter_source_refresh_references(
        &self,
        _session: &Session,
        _params: Params,
        source_name: ResolvedItemName,
        available_source_references: plan::SourceReferences,
    ) -> Result<(Plan, ResolvedIds), AdapterError> {
        let entry = self.catalog().get_entry(source_name.item_id());
        let source = entry.source().ok_or_else(|| {
            AdapterError::internal(
                "plan alter source",
                format!("expected Source found {entry:?}"),
            )
        })?;
        let action = mz_sql::plan::AlterSourceAction::RefreshReferences {
            references: available_source_references,
        };

        Ok((
            Plan::AlterSource(mz_sql::plan::AlterSourcePlan {
                item_id: entry.id(),
                ingestion_id: source.global_id(),
                action,
            }),
            ResolvedIds::empty(),
        ))
    }

    /// Prepares a `CREATE SOURCE` statement to create its progress subsource,
    /// the primary source, and any ingestion export subsources (e.g. PG
    /// tables).
    pub(crate) async fn plan_purified_create_source(
        &mut self,
        ctx: &ExecuteContext,
        params: Params,
        progress_stmt: Option<CreateSubsourceStatement<Aug>>,
        mut source_stmt: mz_sql::ast::CreateSourceStatement<Aug>,
        subsources: BTreeMap<UnresolvedItemName, PurifiedSourceExport>,
        available_source_references: plan::SourceReferences,
    ) -> Result<(Plan, ResolvedIds), AdapterError> {
        let mut create_source_plans = Vec::with_capacity(subsources.len() + 2);

        // 1. First plan the progress subsource, if any.
        if let Some(progress_stmt) = progress_stmt {
            // The primary source depends on this subsource because the primary
            // source needs to know its shard ID, and the easiest way of
            // guaranteeing that the shard ID is discoverable is to create this
            // collection first.
            assert_none!(progress_stmt.of_source);
            let id_ts = self.get_catalog_write_ts().await;
            let (item_id, global_id) = self.catalog().allocate_user_id(id_ts).await?;
            let progress_plan =
                self.plan_subsource(ctx.session(), &params, progress_stmt, item_id, global_id)?;
            let progress_full_name = self
                .catalog()
                .resolve_full_name(&progress_plan.plan.name, None);
            let progress_subsource = ResolvedItemName::Item {
                id: progress_plan.item_id,
                qualifiers: progress_plan.plan.name.qualifiers.clone(),
                full_name: progress_full_name,
                print_id: true,
                version: RelationVersionSelector::Latest,
            };

            create_source_plans.push(progress_plan);

            source_stmt.progress_subsource = Some(DeferredItemName::Named(progress_subsource));
        }

        let catalog = self.catalog().for_session(ctx.session());
        let resolved_ids = mz_sql::names::visit_dependencies(&catalog, &source_stmt);

        let propagated_with_options: Vec<_> = source_stmt
            .with_options
            .iter()
            .filter_map(|opt| match opt.name {
                CreateSourceOptionName::TimestampInterval => None,
                CreateSourceOptionName::RetainHistory => Some(CreateSubsourceOption {
                    name: CreateSubsourceOptionName::RetainHistory,
                    value: opt.value.clone(),
                }),
            })
            .collect();

        // 2. Then plan the main source.
        let source_plan = match self.plan_statement(
            ctx.session(),
            Statement::CreateSource(source_stmt),
            &params,
            &resolved_ids,
        )? {
            Plan::CreateSource(plan) => plan,
            p => unreachable!("s must be CreateSourcePlan but got {:?}", p),
        };

        let id_ts = self.get_catalog_write_ts().await;
        let (item_id, global_id) = self.catalog().allocate_user_id(id_ts).await?;

        let source_full_name = self.catalog().resolve_full_name(&source_plan.name, None);
        let of_source = ResolvedItemName::Item {
            id: item_id,
            qualifiers: source_plan.name.qualifiers.clone(),
            full_name: source_full_name,
            print_id: true,
            version: RelationVersionSelector::Latest,
        };

        // Generate subsource statements
        let conn_catalog = self.catalog().for_system_session();
        let pcx = plan::PlanContext::zero();
        let scx = StatementContext::new(Some(&pcx), &conn_catalog);

        let mut subsource_stmts = generate_subsource_statements(&scx, of_source, subsources)?;

        for subsource_stmt in subsource_stmts.iter_mut() {
            subsource_stmt
                .with_options
                .extend(propagated_with_options.iter().cloned())
        }

        create_source_plans.push(CreateSourcePlanBundle {
            item_id,
            global_id,
            plan: source_plan,
            resolved_ids: resolved_ids.clone(),
            available_source_references: Some(available_source_references),
        });

        // 3. Finally, plan all the subsources
        let id_ts = self.get_catalog_write_ts().await;
        let ids = self
            .catalog()
            .allocate_user_ids(u64::cast_from(subsource_stmts.len()), id_ts)
            .await?;
        for (stmt, (item_id, global_id)) in subsource_stmts.into_iter().zip_eq(ids.into_iter()) {
            let plan = self.plan_subsource(ctx.session(), &params, stmt, item_id, global_id)?;
            create_source_plans.push(plan);
        }

        Ok((
            Plan::CreateSources(create_source_plans),
            ResolvedIds::empty(),
        ))
    }

    #[instrument]
    pub(super) async fn sequence_create_source(
        &mut self,
        ctx: &mut ExecuteContext,
        plans: Vec<plan::CreateSourcePlanBundle>,
    ) -> Result<ExecuteResponse, AdapterError> {
        let CreateSourceInner {
            ops,
            sources,
            if_not_exists_ids,
        } = self.create_source_inner(ctx.session(), plans).await?;

        let transact_result = self
            .catalog_transact_with_ddl_transaction(ctx, ops, |_, _| Box::pin(async {}))
            .await;

        // Check if any sources are webhook sources and report them as created.
        for (item_id, source) in &sources {
            if matches!(source.data_source, DataSourceDesc::Webhook { .. }) {
                if let Some(url) = self.catalog().state().try_get_webhook_url(item_id) {
                    ctx.session()
                        .add_notice(AdapterNotice::WebhookSourceCreated { url });
                }
            }
        }

        match transact_result {
            Ok(()) => Ok(ExecuteResponse::CreatedSource),
            Err(AdapterError::Catalog(mz_catalog::memory::error::Error {
                kind:
                    mz_catalog::memory::error::ErrorKind::Sql(CatalogError::ItemAlreadyExists(id, _)),
            })) if if_not_exists_ids.contains_key(&id) => {
                ctx.session()
                    .add_notice(AdapterNotice::ObjectAlreadyExists {
                        name: if_not_exists_ids[&id].item.clone(),
                        ty: "source",
                    });
                Ok(ExecuteResponse::CreatedSource)
            }
            Err(err) => Err(err),
        }
    }

    #[instrument]
    pub(super) async fn sequence_create_connection(
        &mut self,
        mut ctx: ExecuteContext,
        plan: plan::CreateConnectionPlan,
        resolved_ids: ResolvedIds,
    ) {
        let id_ts = self.get_catalog_write_ts().await;
        let (connection_id, connection_gid) = match self.catalog().allocate_user_id(id_ts).await {
            Ok(item_id) => item_id,
            Err(err) => return ctx.retire(Err(err.into())),
        };

        match &plan.connection.details {
            ConnectionDetails::Ssh { key_1, key_2, .. } => {
                let key_1 = match key_1.as_key_pair() {
                    Some(key_1) => key_1.clone(),
                    None => {
                        return ctx.retire(Err(AdapterError::Unstructured(anyhow!(
                            "the PUBLIC KEY 1 option cannot be explicitly specified"
                        ))));
                    }
                };

                let key_2 = match key_2.as_key_pair() {
                    Some(key_2) => key_2.clone(),
                    None => {
                        return ctx.retire(Err(AdapterError::Unstructured(anyhow!(
                            "the PUBLIC KEY 2 option cannot be explicitly specified"
                        ))));
                    }
                };

                let key_set = SshKeyPairSet::from_parts(key_1, key_2);
                let secret = key_set.to_bytes();
                if let Err(err) = self.secrets_controller.ensure(connection_id, &secret).await {
                    return ctx.retire(Err(err.into()));
                }
            }
            _ => (),
        };

        if plan.validate {
            let internal_cmd_tx = self.internal_cmd_tx.clone();
            let transient_revision = self.catalog().transient_revision();
            let conn_id = ctx.session().conn_id().clone();
            let otel_ctx = OpenTelemetryContext::obtain();
            let role_metadata = ctx.session().role_metadata().clone();

            let connection = plan
                .connection
                .details
                .to_connection()
                .into_inline_connection(self.catalog().state());

            let current_storage_parameters = self.controller.storage.config().clone();
            task::spawn(|| format!("validate_connection:{conn_id}"), async move {
                let result = match connection
                    .validate(connection_id, &current_storage_parameters)
                    .await
                {
                    Ok(()) => Ok(plan),
                    Err(err) => Err(err.into()),
                };

                // It is not an error for validation to complete after `internal_cmd_rx` is dropped.
                let result = internal_cmd_tx.send(Message::CreateConnectionValidationReady(
                    CreateConnectionValidationReady {
                        ctx,
                        result,
                        connection_id,
                        connection_gid,
                        plan_validity: PlanValidity::new(
                            transient_revision,
                            resolved_ids.items().copied().collect(),
                            None,
                            None,
                            role_metadata,
                        ),
                        otel_ctx,
                        resolved_ids: resolved_ids.clone(),
                    },
                ));
                if let Err(e) = result {
                    tracing::warn!("internal_cmd_rx dropped before we could send: {:?}", e);
                }
            });
        } else {
            let result = self
                .sequence_create_connection_stage_finish(
                    &mut ctx,
                    connection_id,
                    connection_gid,
                    plan,
                    resolved_ids,
                )
                .await;
            ctx.retire(result);
        }
    }

    #[instrument]
    pub(crate) async fn sequence_create_connection_stage_finish(
        &mut self,
        ctx: &mut ExecuteContext,
        connection_id: CatalogItemId,
        connection_gid: GlobalId,
        plan: plan::CreateConnectionPlan,
        resolved_ids: ResolvedIds,
    ) -> Result<ExecuteResponse, AdapterError> {
        let ops = vec![catalog::Op::CreateItem {
            id: connection_id,
            name: plan.name.clone(),
            item: CatalogItem::Connection(Connection {
                create_sql: plan.connection.create_sql,
                global_id: connection_gid,
                details: plan.connection.details.clone(),
                resolved_ids,
            }),
            owner_id: *ctx.session().current_role_id(),
        }];

        let transact_result = self
            .catalog_transact_with_side_effects(Some(ctx), ops, move |coord, _ctx| {
                Box::pin(async move {
                    match plan.connection.details {
                        ConnectionDetails::AwsPrivatelink(ref privatelink) => {
                            let spec = VpcEndpointConfig {
                                aws_service_name: privatelink.service_name.to_owned(),
                                availability_zone_ids: privatelink.availability_zones.to_owned(),
                            };
                            let cloud_resource_controller =
                                match coord.cloud_resource_controller.as_ref().cloned() {
                                    Some(controller) => controller,
                                    None => {
                                        tracing::warn!("AWS PrivateLink connections unsupported");
                                        return;
                                    }
                                };
                            if let Err(err) = cloud_resource_controller
                                .ensure_vpc_endpoint(connection_id, spec)
                                .await
                            {
                                tracing::warn!(?err, "failed to ensure vpc endpoint!");
                            }
                        }
                        _ => {}
                    }
                })
            })
            .await;

        match transact_result {
            Ok(_) => Ok(ExecuteResponse::CreatedConnection),
            Err(AdapterError::Catalog(mz_catalog::memory::error::Error {
                kind:
                    mz_catalog::memory::error::ErrorKind::Sql(CatalogError::ItemAlreadyExists(_, _)),
            })) if plan.if_not_exists => {
                ctx.session()
                    .add_notice(AdapterNotice::ObjectAlreadyExists {
                        name: plan.name.item,
                        ty: "connection",
                    });
                Ok(ExecuteResponse::CreatedConnection)
            }
            Err(err) => Err(err),
        }
    }

    #[instrument]
    pub(super) async fn sequence_create_database(
        &mut self,
        session: &Session,
        plan: plan::CreateDatabasePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let ops = vec![catalog::Op::CreateDatabase {
            name: plan.name.clone(),
            owner_id: *session.current_role_id(),
        }];
        match self.catalog_transact(Some(session), ops).await {
            Ok(_) => Ok(ExecuteResponse::CreatedDatabase),
            Err(AdapterError::Catalog(mz_catalog::memory::error::Error {
                kind:
                    mz_catalog::memory::error::ErrorKind::Sql(CatalogError::DatabaseAlreadyExists(_)),
            })) if plan.if_not_exists => {
                session.add_notice(AdapterNotice::DatabaseAlreadyExists { name: plan.name });
                Ok(ExecuteResponse::CreatedDatabase)
            }
            Err(err) => Err(err),
        }
    }

    #[instrument]
    pub(super) async fn sequence_create_schema(
        &mut self,
        session: &Session,
        plan: plan::CreateSchemaPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let op = catalog::Op::CreateSchema {
            database_id: plan.database_spec,
            schema_name: plan.schema_name.clone(),
            owner_id: *session.current_role_id(),
        };
        match self.catalog_transact(Some(session), vec![op]).await {
            Ok(_) => Ok(ExecuteResponse::CreatedSchema),
            Err(AdapterError::Catalog(mz_catalog::memory::error::Error {
                kind:
                    mz_catalog::memory::error::ErrorKind::Sql(CatalogError::SchemaAlreadyExists(_)),
            })) if plan.if_not_exists => {
                session.add_notice(AdapterNotice::SchemaAlreadyExists {
                    name: plan.schema_name,
                });
                Ok(ExecuteResponse::CreatedSchema)
            }
            Err(err) => Err(err),
        }
    }

    /// Validates the role attributes for a `CREATE ROLE` statement.
    fn validate_role_attributes(&self, attributes: &RoleAttributesRaw) -> Result<(), AdapterError> {
        if !ENABLE_PASSWORD_AUTH.get(self.catalog().system_config().dyncfgs()) {
            if attributes.superuser.is_some()
                || attributes.password.is_some()
                || attributes.login.is_some()
            {
                return Err(AdapterError::UnavailableFeature {
                    feature: "SUPERUSER, PASSWORD, and LOGIN attributes".to_string(),
                    docs: Some("https://materialize.com/docs/sql/create-role/#details".to_string()),
                });
            }
        }
        Ok(())
    }

    #[instrument]
    pub(super) async fn sequence_create_role(
        &mut self,
        conn_id: Option<&ConnectionId>,
        plan::CreateRolePlan { name, attributes }: plan::CreateRolePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        self.validate_role_attributes(&attributes.clone())?;
        let op = catalog::Op::CreateRole { name, attributes };
        self.catalog_transact_with_context(conn_id, None, vec![op])
            .await
            .map(|_| ExecuteResponse::CreatedRole)
    }

    #[instrument]
    pub(super) async fn sequence_create_network_policy(
        &mut self,
        session: &Session,
        plan::CreateNetworkPolicyPlan { name, rules }: plan::CreateNetworkPolicyPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let op = catalog::Op::CreateNetworkPolicy {
            rules,
            name,
            owner_id: *session.current_role_id(),
        };
        self.catalog_transact_with_context(Some(session.conn_id()), None, vec![op])
            .await
            .map(|_| ExecuteResponse::CreatedNetworkPolicy)
    }

    #[instrument]
    pub(super) async fn sequence_alter_network_policy(
        &mut self,
        session: &Session,
        plan::AlterNetworkPolicyPlan { id, name, rules }: plan::AlterNetworkPolicyPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        // TODO(network_policy): Consider role based network policies here.
        let current_network_policy_name =
            self.catalog().system_config().default_network_policy_name();
        // Check if the way we're alerting the policy is still valid for the current connection.
        if current_network_policy_name == name {
            self.validate_alter_network_policy(session, &rules)?;
        }

        let op = catalog::Op::AlterNetworkPolicy {
            id,
            rules,
            name,
            owner_id: *session.current_role_id(),
        };
        self.catalog_transact_with_context(Some(session.conn_id()), None, vec![op])
            .await
            .map(|_| ExecuteResponse::AlteredObject(ObjectType::NetworkPolicy))
    }

    #[instrument]
    pub(super) async fn sequence_create_table(
        &mut self,
        ctx: &mut ExecuteContext,
        plan: plan::CreateTablePlan,
        resolved_ids: ResolvedIds,
    ) -> Result<ExecuteResponse, AdapterError> {
        let plan::CreateTablePlan {
            name,
            table,
            if_not_exists,
        } = plan;

        let conn_id = if table.temporary {
            Some(ctx.session().conn_id())
        } else {
            None
        };
        let id_ts = self.get_catalog_write_ts().await;
        let (table_id, global_id) = self.catalog().allocate_user_id(id_ts).await?;
        let collections = [(RelationVersion::root(), global_id)].into_iter().collect();

        let data_source = match table.data_source {
            plan::TableDataSource::TableWrites { defaults } => {
                TableDataSource::TableWrites { defaults }
            }
            plan::TableDataSource::DataSource {
                desc: data_source_plan,
                timeline,
            } => match data_source_plan {
                plan::DataSourceDesc::IngestionExport {
                    ingestion_id,
                    external_reference,
                    details,
                    data_config,
                } => TableDataSource::DataSource {
                    desc: DataSourceDesc::IngestionExport {
                        ingestion_id,
                        external_reference,
                        details,
                        data_config,
                    },
                    timeline,
                },
                plan::DataSourceDesc::Webhook {
                    validate_using,
                    body_format,
                    headers,
                    cluster_id,
                } => TableDataSource::DataSource {
                    desc: DataSourceDesc::Webhook {
                        validate_using,
                        body_format,
                        headers,
                        cluster_id: cluster_id.expect("Webhook Tables must have cluster_id set"),
                    },
                    timeline,
                },
                o => {
                    unreachable!("CREATE TABLE data source got {:?}", o)
                }
            },
        };

        let is_webhook = if let TableDataSource::DataSource {
            desc: DataSourceDesc::Webhook { .. },
            timeline: _,
        } = &data_source
        {
            true
        } else {
            false
        };

        let table = Table {
            create_sql: Some(table.create_sql),
            desc: table.desc,
            collections,
            conn_id: conn_id.cloned(),
            resolved_ids,
            custom_logical_compaction_window: table.compaction_window,
            is_retained_metrics_object: false,
            data_source,
        };
        let ops = vec![catalog::Op::CreateItem {
            id: table_id,
            name: name.clone(),
            item: CatalogItem::Table(table.clone()),
            owner_id: *ctx.session().current_role_id(),
        }];

        let catalog_result = self
            .catalog_transact_with_ddl_transaction(ctx, ops, |_, _| Box::pin(async {}))
            .await;

        if is_webhook {
            // try_get_webhook_url will make up a URL for things that are not
            // webhooks, so we guard against that here.
            if let Some(url) = self.catalog().state().try_get_webhook_url(&table_id) {
                ctx.session()
                    .add_notice(AdapterNotice::WebhookSourceCreated { url })
            }
        }

        match catalog_result {
            Ok(()) => Ok(ExecuteResponse::CreatedTable),
            Err(AdapterError::Catalog(mz_catalog::memory::error::Error {
                kind:
                    mz_catalog::memory::error::ErrorKind::Sql(CatalogError::ItemAlreadyExists(_, _)),
            })) if if_not_exists => {
                ctx.session_mut()
                    .add_notice(AdapterNotice::ObjectAlreadyExists {
                        name: name.item,
                        ty: "table",
                    });
                Ok(ExecuteResponse::CreatedTable)
            }
            Err(err) => Err(err),
        }
    }

    #[instrument]
    pub(super) async fn sequence_create_sink(
        &mut self,
        ctx: ExecuteContext,
        plan: plan::CreateSinkPlan,
        resolved_ids: ResolvedIds,
    ) {
        let plan::CreateSinkPlan {
            name,
            sink,
            with_snapshot,
            if_not_exists,
            in_cluster,
        } = plan;

        // First try to allocate an ID and an OID. If either fails, we're done.
        let id_ts = self.get_catalog_write_ts().await;
        let (item_id, global_id) =
            return_if_err!(self.catalog().allocate_user_id(id_ts).await, ctx);

        let catalog_sink = Sink {
            create_sql: sink.create_sql,
            global_id,
            from: sink.from,
            connection: sink.connection,
            envelope: sink.envelope,
            version: sink.version,
            with_snapshot,
            resolved_ids,
            cluster_id: in_cluster,
            commit_interval: sink.commit_interval,
        };

        let ops = vec![catalog::Op::CreateItem {
            id: item_id,
            name: name.clone(),
            item: CatalogItem::Sink(catalog_sink.clone()),
            owner_id: *ctx.session().current_role_id(),
        }];

        let result = self.catalog_transact(Some(ctx.session()), ops).await;

        match result {
            Ok(()) => {}
            Err(AdapterError::Catalog(mz_catalog::memory::error::Error {
                kind:
                    mz_catalog::memory::error::ErrorKind::Sql(CatalogError::ItemAlreadyExists(_, _)),
            })) if if_not_exists => {
                ctx.session()
                    .add_notice(AdapterNotice::ObjectAlreadyExists {
                        name: name.item,
                        ty: "sink",
                    });
                ctx.retire(Ok(ExecuteResponse::CreatedSink));
                return;
            }
            Err(e) => {
                ctx.retire(Err(e));
                return;
            }
        };

        self.create_storage_export(global_id, &catalog_sink)
            .await
            .unwrap_or_terminate("cannot fail to create exports");

        self.initialize_storage_read_policies([item_id].into(), CompactionWindow::Default)
            .await;

        ctx.retire(Ok(ExecuteResponse::CreatedSink))
    }

    /// Validates that a view definition does not contain any expressions that may lead to
    /// ambiguous column references to system tables. For example `NATURAL JOIN` or `SELECT *`.
    ///
    /// We prevent these expressions so that we can add columns to system tables without
    /// changing the definition of the view.
    ///
    /// Here is a bit of a hand wavy proof as to why we only need to check the
    /// immediate view definition for system objects and ambiguous column
    /// references, and not the entire dependency tree:
    ///
    ///   - A view with no object references cannot have any ambiguous column
    ///   references to a system object, because it has no system objects.
    ///   - A view with a direct reference to a system object and a * or
    ///   NATURAL JOIN will be rejected due to ambiguous column references.
    ///   - A view with system objects but no * or NATURAL JOINs cannot have
    ///   any ambiguous column references to a system object, because all column
    ///   references are explicitly named.
    ///   - A view with * or NATURAL JOINs, that doesn't directly reference a
    ///   system object cannot have any ambiguous column references to a system
    ///   object, because there are no system objects in the top level view and
    ///   all sub-views are guaranteed to have no ambiguous column references to
    ///   system objects.
    pub(super) fn validate_system_column_references(
        &self,
        uses_ambiguous_columns: bool,
        depends_on: &BTreeSet<GlobalId>,
    ) -> Result<(), AdapterError> {
        if uses_ambiguous_columns
            && depends_on
                .iter()
                .any(|id| id.is_system() && self.catalog().get_entry_by_global_id(id).is_relation())
        {
            Err(AdapterError::AmbiguousSystemColumnReference)
        } else {
            Ok(())
        }
    }

    #[instrument]
    pub(super) async fn sequence_create_type(
        &mut self,
        session: &Session,
        plan: plan::CreateTypePlan,
        resolved_ids: ResolvedIds,
    ) -> Result<ExecuteResponse, AdapterError> {
        let id_ts = self.get_catalog_write_ts().await;
        let (item_id, global_id) = self.catalog().allocate_user_id(id_ts).await?;
        // Validate the type definition (e.g., composite columns) before storing.
        plan.typ
            .inner
            .desc(&self.catalog().for_session(session))
            .map_err(AdapterError::from)?;
        let typ = Type {
            create_sql: Some(plan.typ.create_sql),
            global_id,
            details: CatalogTypeDetails {
                array_id: None,
                typ: plan.typ.inner,
                pg_metadata: None,
            },
            resolved_ids,
        };
        let op = catalog::Op::CreateItem {
            id: item_id,
            name: plan.name,
            item: CatalogItem::Type(typ),
            owner_id: *session.current_role_id(),
        };
        match self.catalog_transact(Some(session), vec![op]).await {
            Ok(()) => Ok(ExecuteResponse::CreatedType),
            Err(err) => Err(err),
        }
    }

    #[instrument]
    pub(super) async fn sequence_comment_on(
        &mut self,
        session: &Session,
        plan: plan::CommentPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let op = catalog::Op::Comment {
            object_id: plan.object_id,
            sub_component: plan.sub_component,
            comment: plan.comment,
        };
        self.catalog_transact(Some(session), vec![op]).await?;
        Ok(ExecuteResponse::Comment)
    }

    #[instrument]
    pub(super) async fn sequence_drop_objects(
        &mut self,
        ctx: &mut ExecuteContext,
        plan::DropObjectsPlan {
            drop_ids,
            object_type,
            referenced_ids,
        }: plan::DropObjectsPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let referenced_ids_hashset = referenced_ids.iter().collect::<HashSet<_>>();
        let mut objects = Vec::new();
        for obj_id in &drop_ids {
            if !referenced_ids_hashset.contains(obj_id) {
                let object_info = ErrorMessageObjectDescription::from_id(
                    obj_id,
                    &self.catalog().for_session(ctx.session()),
                )
                .to_string();
                objects.push(object_info);
            }
        }

        if !objects.is_empty() {
            ctx.session()
                .add_notice(AdapterNotice::CascadeDroppedObject { objects });
        }

        let DropOps {
            ops,
            dropped_active_db,
            dropped_active_cluster,
            dropped_in_use_indexes,
        } = self.sequence_drop_common(ctx.session(), drop_ids)?;

        self.catalog_transact_with_context(None, Some(ctx), ops)
            .await?;

        fail::fail_point!("after_sequencer_drop_replica");

        if dropped_active_db {
            ctx.session()
                .add_notice(AdapterNotice::DroppedActiveDatabase {
                    name: ctx.session().vars().database().to_string(),
                });
        }
        if dropped_active_cluster {
            ctx.session()
                .add_notice(AdapterNotice::DroppedActiveCluster {
                    name: ctx.session().vars().cluster().to_string(),
                });
        }
        for dropped_in_use_index in dropped_in_use_indexes {
            ctx.session()
                .add_notice(AdapterNotice::DroppedInUseIndex(dropped_in_use_index));
            self.metrics
                .optimization_notices
                .with_label_values(&["DroppedInUseIndex"])
                .inc_by(1);
        }
        Ok(ExecuteResponse::DroppedObject(object_type))
    }

    fn validate_dropped_role_ownership(
        &self,
        session: &Session,
        dropped_roles: &BTreeMap<RoleId, &str>,
    ) -> Result<(), AdapterError> {
        fn privilege_check(
            privileges: &PrivilegeMap,
            dropped_roles: &BTreeMap<RoleId, &str>,
            dependent_objects: &mut BTreeMap<String, Vec<String>>,
            object_id: &SystemObjectId,
            catalog: &ConnCatalog,
        ) {
            for privilege in privileges.all_values() {
                if let Some(role_name) = dropped_roles.get(&privilege.grantee) {
                    let grantor_name = catalog.get_role(&privilege.grantor).name();
                    let object_description =
                        ErrorMessageObjectDescription::from_sys_id(object_id, catalog);
                    dependent_objects
                        .entry(role_name.to_string())
                        .or_default()
                        .push(format!(
                            "privileges on {object_description} granted by {grantor_name}",
                        ));
                }
                if let Some(role_name) = dropped_roles.get(&privilege.grantor) {
                    let grantee_name = catalog.get_role(&privilege.grantee).name();
                    let object_description =
                        ErrorMessageObjectDescription::from_sys_id(object_id, catalog);
                    dependent_objects
                        .entry(role_name.to_string())
                        .or_default()
                        .push(format!(
                            "privileges granted on {object_description} to {grantee_name}"
                        ));
                }
            }
        }

        let catalog = self.catalog().for_session(session);
        let mut dependent_objects: BTreeMap<_, Vec<_>> = BTreeMap::new();
        for entry in self.catalog.entries() {
            let id = SystemObjectId::Object(entry.id().into());
            if let Some(role_name) = dropped_roles.get(entry.owner_id()) {
                let object_description = ErrorMessageObjectDescription::from_sys_id(&id, &catalog);
                dependent_objects
                    .entry(role_name.to_string())
                    .or_default()
                    .push(format!("owner of {object_description}"));
            }
            privilege_check(
                entry.privileges(),
                dropped_roles,
                &mut dependent_objects,
                &id,
                &catalog,
            );
        }
        for database in self.catalog.databases() {
            let database_id = SystemObjectId::Object(database.id().into());
            if let Some(role_name) = dropped_roles.get(&database.owner_id) {
                let object_description =
                    ErrorMessageObjectDescription::from_sys_id(&database_id, &catalog);
                dependent_objects
                    .entry(role_name.to_string())
                    .or_default()
                    .push(format!("owner of {object_description}"));
            }
            privilege_check(
                &database.privileges,
                dropped_roles,
                &mut dependent_objects,
                &database_id,
                &catalog,
            );
            for schema in database.schemas_by_id.values() {
                let schema_id = SystemObjectId::Object(
                    (ResolvedDatabaseSpecifier::Id(database.id()), *schema.id()).into(),
                );
                if let Some(role_name) = dropped_roles.get(&schema.owner_id) {
                    let object_description =
                        ErrorMessageObjectDescription::from_sys_id(&schema_id, &catalog);
                    dependent_objects
                        .entry(role_name.to_string())
                        .or_default()
                        .push(format!("owner of {object_description}"));
                }
                privilege_check(
                    &schema.privileges,
                    dropped_roles,
                    &mut dependent_objects,
                    &schema_id,
                    &catalog,
                );
            }
        }
        for cluster in self.catalog.clusters() {
            let cluster_id = SystemObjectId::Object(cluster.id().into());
            if let Some(role_name) = dropped_roles.get(&cluster.owner_id) {
                let object_description =
                    ErrorMessageObjectDescription::from_sys_id(&cluster_id, &catalog);
                dependent_objects
                    .entry(role_name.to_string())
                    .or_default()
                    .push(format!("owner of {object_description}"));
            }
            privilege_check(
                &cluster.privileges,
                dropped_roles,
                &mut dependent_objects,
                &cluster_id,
                &catalog,
            );
            for replica in cluster.replicas() {
                if let Some(role_name) = dropped_roles.get(&replica.owner_id) {
                    let replica_id =
                        SystemObjectId::Object((replica.cluster_id(), replica.replica_id()).into());
                    let object_description =
                        ErrorMessageObjectDescription::from_sys_id(&replica_id, &catalog);
                    dependent_objects
                        .entry(role_name.to_string())
                        .or_default()
                        .push(format!("owner of {object_description}"));
                }
            }
        }
        privilege_check(
            self.catalog().system_privileges(),
            dropped_roles,
            &mut dependent_objects,
            &SystemObjectId::System,
            &catalog,
        );
        for (default_privilege_object, default_privilege_acl_items) in
            self.catalog.default_privileges()
        {
            if let Some(role_name) = dropped_roles.get(&default_privilege_object.role_id) {
                dependent_objects
                    .entry(role_name.to_string())
                    .or_default()
                    .push(format!(
                        "default privileges on {}S created by {}",
                        default_privilege_object.object_type, role_name
                    ));
            }
            for default_privilege_acl_item in default_privilege_acl_items {
                if let Some(role_name) = dropped_roles.get(&default_privilege_acl_item.grantee) {
                    dependent_objects
                        .entry(role_name.to_string())
                        .or_default()
                        .push(format!(
                            "default privileges on {}S granted to {}",
                            default_privilege_object.object_type, role_name
                        ));
                }
            }
        }

        if !dependent_objects.is_empty() {
            Err(AdapterError::DependentObject(dependent_objects))
        } else {
            Ok(())
        }
    }

    #[instrument]
    pub(super) async fn sequence_drop_owned(
        &mut self,
        session: &Session,
        plan: plan::DropOwnedPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        for role_id in &plan.role_ids {
            self.catalog().ensure_not_reserved_role(role_id)?;
        }

        let mut privilege_revokes = plan.privilege_revokes;

        // Make sure this stays in sync with the beginning of `rbac::check_plan`.
        let session_catalog = self.catalog().for_session(session);
        if rbac::is_rbac_enabled_for_session(session_catalog.system_vars(), session)
            && !session.is_superuser()
        {
            // Obtain all roles that the current session is a member of.
            let role_membership =
                session_catalog.collect_role_membership(session.current_role_id());
            let invalid_revokes: BTreeSet<_> = privilege_revokes
                .extract_if(.., |(_, privilege)| {
                    !role_membership.contains(&privilege.grantor)
                })
                .map(|(object_id, _)| object_id)
                .collect();
            for invalid_revoke in invalid_revokes {
                let object_description =
                    ErrorMessageObjectDescription::from_sys_id(&invalid_revoke, &session_catalog);
                session.add_notice(AdapterNotice::CannotRevoke { object_description });
            }
        }

        let privilege_revoke_ops = privilege_revokes.into_iter().map(|(object_id, privilege)| {
            catalog::Op::UpdatePrivilege {
                target_id: object_id,
                privilege,
                variant: UpdatePrivilegeVariant::Revoke,
            }
        });
        let default_privilege_revoke_ops = plan.default_privilege_revokes.into_iter().map(
            |(privilege_object, privilege_acl_item)| catalog::Op::UpdateDefaultPrivilege {
                privilege_object,
                privilege_acl_item,
                variant: UpdatePrivilegeVariant::Revoke,
            },
        );
        let DropOps {
            ops: drop_ops,
            dropped_active_db,
            dropped_active_cluster,
            dropped_in_use_indexes,
        } = self.sequence_drop_common(session, plan.drop_ids)?;

        let ops = privilege_revoke_ops
            .chain(default_privilege_revoke_ops)
            .chain(drop_ops.into_iter())
            .collect();

        self.catalog_transact(Some(session), ops).await?;

        if dropped_active_db {
            session.add_notice(AdapterNotice::DroppedActiveDatabase {
                name: session.vars().database().to_string(),
            });
        }
        if dropped_active_cluster {
            session.add_notice(AdapterNotice::DroppedActiveCluster {
                name: session.vars().cluster().to_string(),
            });
        }
        for dropped_in_use_index in dropped_in_use_indexes {
            session.add_notice(AdapterNotice::DroppedInUseIndex(dropped_in_use_index));
        }
        Ok(ExecuteResponse::DroppedOwned)
    }

    fn sequence_drop_common(
        &self,
        session: &Session,
        ids: Vec<ObjectId>,
    ) -> Result<DropOps, AdapterError> {
        let mut dropped_active_db = false;
        let mut dropped_active_cluster = false;
        let mut dropped_in_use_indexes = Vec::new();
        let mut dropped_roles = BTreeMap::new();
        let mut dropped_databases = BTreeSet::new();
        let mut dropped_schemas = BTreeSet::new();
        // Dropping either the group role or the member role of a role membership will trigger a
        // revoke role. We use a Set for the revokes to avoid trying to attempt to revoke the same
        // role membership twice.
        let mut role_revokes = BTreeSet::new();
        // Dropping a database or a schema will revoke all default roles associated with that
        // database or schema.
        let mut default_privilege_revokes = BTreeSet::new();

        // Clusters we're dropping
        let mut clusters_to_drop = BTreeSet::new();

        let ids_set = ids.iter().collect::<BTreeSet<_>>();
        for id in &ids {
            match id {
                ObjectId::Database(id) => {
                    let name = self.catalog().get_database(id).name();
                    if name == session.vars().database() {
                        dropped_active_db = true;
                    }
                    dropped_databases.insert(id);
                }
                ObjectId::Schema((_, spec)) => {
                    if let SchemaSpecifier::Id(id) = spec {
                        dropped_schemas.insert(id);
                    }
                }
                ObjectId::Cluster(id) => {
                    clusters_to_drop.insert(*id);
                    if let Some(active_id) = self
                        .catalog()
                        .active_cluster(session)
                        .ok()
                        .map(|cluster| cluster.id())
                    {
                        if id == &active_id {
                            dropped_active_cluster = true;
                        }
                    }
                }
                ObjectId::Role(id) => {
                    let role = self.catalog().get_role(id);
                    let name = role.name();
                    dropped_roles.insert(*id, name);
                    // We must revoke all role memberships that the dropped roles belongs to.
                    for (group_id, grantor_id) in &role.membership.map {
                        role_revokes.insert((*group_id, *id, *grantor_id));
                    }
                }
                ObjectId::Item(id) => {
                    if let Some(index) = self.catalog().get_entry(id).index() {
                        let humanizer = self.catalog().for_session(session);
                        let dependants = self
                            .controller
                            .compute
                            .collection_reverse_dependencies(index.cluster_id, index.global_id())
                            .ok()
                            .into_iter()
                            .flatten()
                            .filter(|dependant_id| {
                                // Transient Ids belong to Peeks. We are not interested for now in
                                // peeks depending on a dropped index.
                                // TODO: show a different notice in this case. Something like
                                // "There is an in-progress ad hoc SELECT that uses the dropped
                                // index. The resources used by the index will be freed when all
                                // such SELECTs complete."
                                if dependant_id.is_transient() {
                                    return false;
                                }
                                // The item should exist, but don't panic if it doesn't.
                                let Some(dependent_id) = humanizer
                                    .try_get_item_by_global_id(dependant_id)
                                    .map(|item| item.id())
                                else {
                                    return false;
                                };
                                // If the dependent object is also being dropped, then there is no
                                // problem, so we don't want a notice.
                                !ids_set.contains(&ObjectId::Item(dependent_id))
                            })
                            .flat_map(|dependant_id| {
                                // If we are not able to find a name for this ID it probably means
                                // we have already dropped the compute collection, in which case we
                                // can ignore it.
                                humanizer.humanize_id(dependant_id)
                            })
                            .collect_vec();
                        if !dependants.is_empty() {
                            dropped_in_use_indexes.push(DroppedInUseIndex {
                                index_name: humanizer
                                    .humanize_id(index.global_id())
                                    .unwrap_or_else(|| id.to_string()),
                                dependant_objects: dependants,
                            });
                        }
                    }
                }
                _ => {}
            }
        }

        for id in &ids {
            match id {
                // Validate that `ClusterReplica` drops do not drop replicas of managed clusters,
                // unless they are internal replicas, which exist outside the scope
                // of managed clusters.
                ObjectId::ClusterReplica((cluster_id, replica_id)) => {
                    if !clusters_to_drop.contains(cluster_id) {
                        let cluster = self.catalog.get_cluster(*cluster_id);
                        if cluster.is_managed() {
                            let replica =
                                cluster.replica(*replica_id).expect("Catalog out of sync");
                            if !replica.config.location.internal() {
                                coord_bail!("cannot drop replica of managed cluster");
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        for role_id in dropped_roles.keys() {
            self.catalog().ensure_not_reserved_role(role_id)?;
        }
        self.validate_dropped_role_ownership(session, &dropped_roles)?;
        // If any role is a member of a dropped role, then we must revoke that membership.
        let dropped_role_ids: BTreeSet<_> = dropped_roles.keys().collect();
        for role in self.catalog().user_roles() {
            for dropped_role_id in
                dropped_role_ids.intersection(&role.membership.map.keys().collect())
            {
                role_revokes.insert((
                    **dropped_role_id,
                    role.id(),
                    *role
                        .membership
                        .map
                        .get(*dropped_role_id)
                        .expect("included in keys above"),
                ));
            }
        }

        for (default_privilege_object, default_privilege_acls) in
            self.catalog().default_privileges()
        {
            if matches!(&default_privilege_object.database_id, Some(database_id) if dropped_databases.contains(database_id))
                || matches!(&default_privilege_object.schema_id, Some(schema_id) if dropped_schemas.contains(schema_id))
            {
                for default_privilege_acl in default_privilege_acls {
                    default_privilege_revokes.insert((
                        default_privilege_object.clone(),
                        default_privilege_acl.clone(),
                    ));
                }
            }
        }

        let ops = role_revokes
            .into_iter()
            .map(|(role_id, member_id, grantor_id)| catalog::Op::RevokeRole {
                role_id,
                member_id,
                grantor_id,
            })
            .chain(default_privilege_revokes.into_iter().map(
                |(privilege_object, privilege_acl_item)| catalog::Op::UpdateDefaultPrivilege {
                    privilege_object,
                    privilege_acl_item,
                    variant: UpdatePrivilegeVariant::Revoke,
                },
            ))
            .chain(iter::once(catalog::Op::DropObjects(
                ids.into_iter()
                    .map(DropObjectInfo::manual_drop_from_object_id)
                    .collect(),
            )))
            .collect();

        Ok(DropOps {
            ops,
            dropped_active_db,
            dropped_active_cluster,
            dropped_in_use_indexes,
        })
    }

    pub(super) fn sequence_explain_schema(
        &self,
        ExplainSinkSchemaPlan { json_schema, .. }: ExplainSinkSchemaPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let json_value: serde_json::Value = serde_json::from_str(&json_schema).map_err(|e| {
            AdapterError::Explain(mz_repr::explain::ExplainError::SerdeJsonError(e))
        })?;

        let json_string = json_string(&json_value);
        let row = Row::pack_slice(&[Datum::String(&json_string)]);
        Ok(Self::send_immediate_rows(row))
    }

    pub(super) fn sequence_show_all_variables(
        &self,
        session: &Session,
    ) -> Result<ExecuteResponse, AdapterError> {
        let mut rows = viewable_variables(self.catalog().state(), session)
            .map(|v| (v.name(), v.value(), v.description()))
            .collect::<Vec<_>>();
        rows.sort_by_cached_key(|(name, _, _)| name.to_lowercase());

        // TODO(parkmycar): Pack all of these into a single RowCollection.
        let rows: Vec<_> = rows
            .into_iter()
            .map(|(name, val, desc)| {
                Row::pack_slice(&[
                    Datum::String(name),
                    Datum::String(&val),
                    Datum::String(desc),
                ])
            })
            .collect();
        Ok(Self::send_immediate_rows(rows))
    }

    pub(super) fn sequence_show_variable(
        &self,
        session: &Session,
        plan: plan::ShowVariablePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        if &plan.name == SCHEMA_ALIAS {
            let schemas = self.catalog.resolve_search_path(session);
            let schema = schemas.first();
            return match schema {
                Some((database_spec, schema_spec)) => {
                    let schema_name = &self
                        .catalog
                        .get_schema(database_spec, schema_spec, session.conn_id())
                        .name()
                        .schema;
                    let row = Row::pack_slice(&[Datum::String(schema_name)]);
                    Ok(Self::send_immediate_rows(row))
                }
                None => {
                    if session.vars().current_object_missing_warnings() {
                        session.add_notice(AdapterNotice::NoResolvableSearchPathSchema {
                            search_path: session
                                .vars()
                                .search_path()
                                .into_iter()
                                .map(|schema| schema.to_string())
                                .collect(),
                        });
                    }
                    Ok(Self::send_immediate_rows(Row::pack_slice(&[Datum::Null])))
                }
            };
        }

        let variable = session
            .vars()
            .get(self.catalog().system_config(), &plan.name)
            .or_else(|_| self.catalog().system_config().get(&plan.name))?;

        // In lieu of plumbing the user to all system config functions, just check that the var is
        // visible.
        variable.visible(session.user(), self.catalog().system_config())?;

        let row = Row::pack_slice(&[Datum::String(&variable.value())]);
        if variable.name() == vars::DATABASE.name()
            && matches!(
                self.catalog().resolve_database(&variable.value()),
                Err(CatalogError::UnknownDatabase(_))
            )
            && session.vars().current_object_missing_warnings()
        {
            let name = variable.value();
            session.add_notice(AdapterNotice::DatabaseDoesNotExist { name });
        } else if variable.name() == vars::CLUSTER.name()
            && matches!(
                self.catalog().resolve_cluster(&variable.value()),
                Err(CatalogError::UnknownCluster(_))
            )
            && session.vars().current_object_missing_warnings()
        {
            let name = variable.value();
            session.add_notice(AdapterNotice::ClusterDoesNotExist { name });
        }
        Ok(Self::send_immediate_rows(row))
    }

    #[instrument]
    pub(super) async fn sequence_inspect_shard(
        &self,
        session: &Session,
        plan: plan::InspectShardPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        // TODO: Not thrilled about this rbac special case here, but probably
        // sufficient for now.
        if !session.user().is_internal() {
            return Err(AdapterError::Unauthorized(
                rbac::UnauthorizedError::MzSystem {
                    action: "inspect".into(),
                },
            ));
        }
        let state = self
            .controller
            .storage
            .inspect_persist_state(plan.id)
            .await?;
        let jsonb = Jsonb::from_serde_json(state)?;
        Ok(Self::send_immediate_rows(jsonb.into_row()))
    }

    #[instrument]
    pub(super) fn sequence_set_variable(
        &self,
        session: &mut Session,
        plan: plan::SetVariablePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let (name, local) = (plan.name, plan.local);
        if &name == TRANSACTION_ISOLATION_VAR_NAME {
            self.validate_set_isolation_level(session)?;
        }
        if &name == vars::CLUSTER.name() {
            self.validate_set_cluster(session)?;
        }

        let vars = session.vars_mut();
        let values = match plan.value {
            plan::VariableValue::Default => None,
            plan::VariableValue::Values(values) => Some(values),
        };

        match values {
            Some(values) => {
                vars.set(
                    self.catalog().system_config(),
                    &name,
                    VarInput::SqlSet(&values),
                    local,
                )?;

                let vars = session.vars();

                // Emit a warning when deprecated variables are used.
                // TODO(database-issues#8069) remove this after sufficient time has passed
                if name == vars::OLD_AUTO_ROUTE_CATALOG_QUERIES {
                    session.add_notice(AdapterNotice::AutoRouteIntrospectionQueriesUsage);
                } else if name == vars::CLUSTER.name()
                    && values[0] == vars::OLD_CATALOG_SERVER_CLUSTER
                {
                    session.add_notice(AdapterNotice::IntrospectionClusterUsage);
                }

                // Database or cluster value does not correspond to a catalog item.
                if name.as_str() == vars::DATABASE.name()
                    && matches!(
                        self.catalog().resolve_database(vars.database()),
                        Err(CatalogError::UnknownDatabase(_))
                    )
                    && session.vars().current_object_missing_warnings()
                {
                    let name = vars.database().to_string();
                    session.add_notice(AdapterNotice::DatabaseDoesNotExist { name });
                } else if name.as_str() == vars::CLUSTER.name()
                    && matches!(
                        self.catalog().resolve_cluster(vars.cluster()),
                        Err(CatalogError::UnknownCluster(_))
                    )
                    && session.vars().current_object_missing_warnings()
                {
                    let name = vars.cluster().to_string();
                    session.add_notice(AdapterNotice::ClusterDoesNotExist { name });
                } else if name.as_str() == TRANSACTION_ISOLATION_VAR_NAME {
                    let v = values.into_first().to_lowercase();
                    if v == IsolationLevel::ReadUncommitted.as_str()
                        || v == IsolationLevel::ReadCommitted.as_str()
                        || v == IsolationLevel::RepeatableRead.as_str()
                    {
                        session.add_notice(AdapterNotice::UnimplementedIsolationLevel {
                            isolation_level: v,
                        });
                    } else if v == IsolationLevel::StrongSessionSerializable.as_str() {
                        session.add_notice(AdapterNotice::StrongSessionSerializable);
                    }
                }
            }
            None => vars.reset(self.catalog().system_config(), &name, local)?,
        }

        Ok(ExecuteResponse::SetVariable { name, reset: false })
    }

    pub(super) fn sequence_reset_variable(
        &self,
        session: &mut Session,
        plan: plan::ResetVariablePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let name = plan.name;
        if &name == TRANSACTION_ISOLATION_VAR_NAME {
            self.validate_set_isolation_level(session)?;
        }
        if &name == vars::CLUSTER.name() {
            self.validate_set_cluster(session)?;
        }
        session
            .vars_mut()
            .reset(self.catalog().system_config(), &name, false)?;
        Ok(ExecuteResponse::SetVariable { name, reset: true })
    }

    pub(super) fn sequence_set_transaction(
        &self,
        session: &mut Session,
        plan: plan::SetTransactionPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        // TODO(jkosh44) Only supports isolation levels for now.
        for mode in plan.modes {
            match mode {
                TransactionMode::AccessMode(_) => {
                    return Err(AdapterError::Unsupported("SET TRANSACTION <access-mode>"));
                }
                TransactionMode::IsolationLevel(isolation_level) => {
                    self.validate_set_isolation_level(session)?;

                    session.vars_mut().set(
                        self.catalog().system_config(),
                        TRANSACTION_ISOLATION_VAR_NAME,
                        VarInput::Flat(&isolation_level.to_ast_string_stable()),
                        plan.local,
                    )?
                }
            }
        }
        Ok(ExecuteResponse::SetVariable {
            name: TRANSACTION_ISOLATION_VAR_NAME.to_string(),
            reset: false,
        })
    }

    fn validate_set_isolation_level(&self, session: &Session) -> Result<(), AdapterError> {
        if session.transaction().contains_ops() {
            Err(AdapterError::InvalidSetIsolationLevel)
        } else {
            Ok(())
        }
    }

    fn validate_set_cluster(&self, session: &Session) -> Result<(), AdapterError> {
        if session.transaction().contains_ops() {
            Err(AdapterError::InvalidSetCluster)
        } else {
            Ok(())
        }
    }

    #[instrument]
    pub(super) async fn sequence_end_transaction(
        &mut self,
        mut ctx: ExecuteContext,
        mut action: EndTransactionAction,
    ) {
        // If the transaction has failed, we can only rollback.
        if let (EndTransactionAction::Commit, TransactionStatus::Failed(_)) =
            (&action, ctx.session().transaction())
        {
            action = EndTransactionAction::Rollback;
        }
        let response = match action {
            EndTransactionAction::Commit => Ok(PendingTxnResponse::Committed {
                params: BTreeMap::new(),
            }),
            EndTransactionAction::Rollback => Ok(PendingTxnResponse::Rolledback {
                params: BTreeMap::new(),
            }),
        };

        let result = self.sequence_end_transaction_inner(&mut ctx, action).await;

        let (response, action) = match result {
            Ok((Some(TransactionOps::Writes(writes)), _)) if writes.is_empty() => {
                (response, action)
            }
            Ok((Some(TransactionOps::Writes(writes)), write_lock_guards)) => {
                // Make sure we have the correct set of write locks for this transaction.
                // Aggressively dropping partial sets of locks to prevent deadlocking separate
                // transactions.
                let validated_locks = match write_lock_guards {
                    None => None,
                    Some(locks) => match locks.validate(writes.iter().map(|op| op.id)) {
                        Ok(locks) => Some(locks),
                        Err(missing) => {
                            tracing::error!(?missing, "programming error, missing write locks");
                            return ctx.retire(Err(AdapterError::WrongSetOfLocks));
                        }
                    },
                };

                let mut collected_writes: BTreeMap<CatalogItemId, SmallVec<_>> = BTreeMap::new();
                for WriteOp { id, rows } in writes {
                    let total_rows = collected_writes.entry(id).or_default();
                    total_rows.push(rows);
                }

                self.submit_write(PendingWriteTxn::User {
                    span: Span::current(),
                    writes: collected_writes,
                    write_locks: validated_locks,
                    pending_txn: PendingTxn {
                        ctx,
                        response,
                        action,
                    },
                });
                return;
            }
            Ok((
                Some(TransactionOps::Peeks {
                    determination,
                    requires_linearization: RequireLinearization::Required,
                    ..
                }),
                _,
            )) if ctx.session().vars().transaction_isolation()
                == &IsolationLevel::StrictSerializable =>
            {
                let conn_id = ctx.session().conn_id().clone();
                let pending_read_txn = PendingReadTxn {
                    txn: PendingRead::Read {
                        txn: PendingTxn {
                            ctx,
                            response,
                            action,
                        },
                    },
                    timestamp_context: determination.timestamp_context,
                    created: Instant::now(),
                    num_requeues: 0,
                    otel_ctx: OpenTelemetryContext::obtain(),
                };
                self.strict_serializable_reads_tx
                    .send((conn_id, pending_read_txn))
                    .expect("sending to strict_serializable_reads_tx cannot fail");
                return;
            }
            Ok((
                Some(TransactionOps::Peeks {
                    determination,
                    requires_linearization: RequireLinearization::Required,
                    ..
                }),
                _,
            )) if ctx.session().vars().transaction_isolation()
                == &IsolationLevel::StrongSessionSerializable =>
            {
                if let Some((timeline, ts)) = determination.timestamp_context.timeline_timestamp() {
                    ctx.session_mut()
                        .ensure_timestamp_oracle(timeline.clone())
                        .apply_write(*ts);
                }
                (response, action)
            }
            Ok((Some(TransactionOps::SingleStatement { stmt, params }), _)) => {
                self.internal_cmd_tx
                    .send(Message::ExecuteSingleStatementTransaction {
                        ctx,
                        otel_ctx: OpenTelemetryContext::obtain(),
                        stmt,
                        params,
                    })
                    .expect("must send");
                return;
            }
            Ok((_, _)) => (response, action),
            Err(err) => (Err(err), EndTransactionAction::Rollback),
        };
        let changed = ctx.session_mut().vars_mut().end_transaction(action);
        // Append any parameters that changed to the response.
        let response = response.map(|mut r| {
            r.extend_params(changed);
            ExecuteResponse::from(r)
        });

        ctx.retire(response);
    }

    #[instrument]
    async fn sequence_end_transaction_inner(
        &mut self,
        ctx: &mut ExecuteContext,
        action: EndTransactionAction,
    ) -> Result<(Option<TransactionOps<Timestamp>>, Option<WriteLocks>), AdapterError> {
        let txn = self.clear_transaction(ctx.session_mut()).await;

        if let EndTransactionAction::Commit = action {
            if let (Some(mut ops), write_lock_guards) = txn.into_ops_and_lock_guard() {
                match &mut ops {
                    TransactionOps::Writes(writes) => {
                        for WriteOp { id, .. } in &mut writes.iter() {
                            // Re-verify this id exists.
                            let _ = self.catalog().try_get_entry(id).ok_or_else(|| {
                                AdapterError::Catalog(mz_catalog::memory::error::Error {
                                    kind: mz_catalog::memory::error::ErrorKind::Sql(
                                        CatalogError::UnknownItem(id.to_string()),
                                    ),
                                })
                            })?;
                        }

                        // `rows` can be empty if, say, a DELETE's WHERE clause had 0 results.
                        writes.retain(|WriteOp { rows, .. }| !rows.is_empty());
                    }
                    TransactionOps::DDL {
                        ops,
                        state: _,
                        side_effects,
                        revision,
                    } => {
                        // Make sure our catalog hasn't changed.
                        if *revision != self.catalog().transient_revision() {
                            return Err(AdapterError::DDLTransactionRace);
                        }
                        // Commit all of our queued ops.
                        let ops = std::mem::take(ops);
                        let side_effects = std::mem::take(side_effects);
                        self.catalog_transact_with_side_effects(
                            Some(ctx),
                            ops,
                            move |a, mut ctx| {
                                Box::pin(async move {
                                    for side_effect in side_effects {
                                        side_effect(a, ctx.as_mut().map(|ctx| &mut **ctx)).await;
                                    }
                                })
                            },
                        )
                        .await?;
                    }
                    _ => (),
                }
                return Ok((Some(ops), write_lock_guards));
            }
        }

        Ok((None, None))
    }

    pub(super) async fn sequence_side_effecting_func(
        &mut self,
        ctx: ExecuteContext,
        plan: SideEffectingFunc,
    ) {
        match plan {
            SideEffectingFunc::PgCancelBackend { connection_id } => {
                if ctx.session().conn_id().unhandled() == connection_id {
                    // As a special case, if we're canceling ourselves, we send
                    // back a canceled resposne to the client issuing the query,
                    // and so we need to do no further processing of the cancel.
                    ctx.retire(Err(AdapterError::Canceled));
                    return;
                }

                let res = if let Some((id_handle, _conn_meta)) =
                    self.active_conns.get_key_value(&connection_id)
                {
                    // check_plan already verified role membership.
                    self.handle_privileged_cancel(id_handle.clone()).await;
                    Datum::True
                } else {
                    Datum::False
                };
                ctx.retire(Ok(Self::send_immediate_rows(Row::pack_slice(&[res]))));
            }
        }
    }

    /// Inner method that performs the actual real-time recency timestamp determination.
    /// This is called by both the old peek sequencing code (via `determine_real_time_recent_timestamp`)
    /// and the new command handler for `Command::DetermineRealTimeRecentTimestamp`.
    pub(crate) async fn determine_real_time_recent_timestamp(
        &self,
        source_ids: impl Iterator<Item = GlobalId>,
        real_time_recency_timeout: Duration,
    ) -> Result<Option<BoxFuture<'static, Result<Timestamp, StorageError<Timestamp>>>>, AdapterError>
    {
        let item_ids = source_ids
            .map(|gid| {
                self.catalog
                    .try_resolve_item_id(&gid)
                    .ok_or_else(|| AdapterError::RtrDropFailure(gid.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Find all dependencies transitively because we need to ensure that
        // RTR queries determine the timestamp from the sources' (i.e.
        // storage objects that ingest data from external systems) remap
        // data. We "cheat" a little bit and filter out any IDs that aren't
        // user objects because we know they are not a RTR source.
        let mut to_visit = VecDeque::from_iter(item_ids.into_iter().filter(CatalogItemId::is_user));
        // If none of the sources are user objects, we don't need to provide
        // a RTR timestamp.
        if to_visit.is_empty() {
            return Ok(None);
        }

        let mut timestamp_objects = BTreeSet::new();

        while let Some(id) = to_visit.pop_front() {
            timestamp_objects.insert(id);
            to_visit.extend(
                self.catalog()
                    .get_entry(&id)
                    .uses()
                    .into_iter()
                    .filter(|id| !timestamp_objects.contains(id) && id.is_user()),
            );
        }
        let timestamp_objects = timestamp_objects
            .into_iter()
            .flat_map(|item_id| self.catalog().get_entry(&item_id).global_ids())
            .collect();

        let r = self
            .controller
            .determine_real_time_recent_timestamp(timestamp_objects, real_time_recency_timeout)
            .await?;

        Ok(Some(r))
    }

    /// Checks to see if the session needs a real time recency timestamp and if so returns
    /// a future that will return the timestamp.
    pub(crate) async fn determine_real_time_recent_timestamp_if_needed(
        &self,
        session: &Session,
        source_ids: impl Iterator<Item = GlobalId>,
    ) -> Result<Option<BoxFuture<'static, Result<Timestamp, StorageError<Timestamp>>>>, AdapterError>
    {
        let vars = session.vars();

        if vars.real_time_recency()
            && vars.transaction_isolation() == &IsolationLevel::StrictSerializable
            && !session.contains_read_timestamp()
        {
            self.determine_real_time_recent_timestamp(source_ids, *vars.real_time_recency_timeout())
                .await
        } else {
            Ok(None)
        }
    }

    #[instrument]
    pub(super) async fn sequence_explain_plan(
        &mut self,
        ctx: ExecuteContext,
        plan: plan::ExplainPlanPlan,
        target_cluster: TargetCluster,
    ) {
        match &plan.explainee {
            plan::Explainee::Statement(stmt) => match stmt {
                plan::ExplaineeStatement::CreateView { .. } => {
                    self.explain_create_view(ctx, plan).await;
                }
                plan::ExplaineeStatement::CreateMaterializedView { .. } => {
                    self.explain_create_materialized_view(ctx, plan).await;
                }
                plan::ExplaineeStatement::CreateIndex { .. } => {
                    self.explain_create_index(ctx, plan).await;
                }
                plan::ExplaineeStatement::Select { .. } => {
                    self.explain_peek(ctx, plan, target_cluster).await;
                }
            },
            plan::Explainee::View(_) => {
                let result = self.explain_view(&ctx, plan);
                ctx.retire(result);
            }
            plan::Explainee::MaterializedView(_) => {
                let result = self.explain_materialized_view(&ctx, plan);
                ctx.retire(result);
            }
            plan::Explainee::Index(_) => {
                let result = self.explain_index(&ctx, plan);
                ctx.retire(result);
            }
            plan::Explainee::ReplanView(_) => {
                self.explain_replan_view(ctx, plan).await;
            }
            plan::Explainee::ReplanMaterializedView(_) => {
                self.explain_replan_materialized_view(ctx, plan).await;
            }
            plan::Explainee::ReplanIndex(_) => {
                self.explain_replan_index(ctx, plan).await;
            }
        };
    }

    pub(super) async fn sequence_explain_pushdown(
        &mut self,
        ctx: ExecuteContext,
        plan: plan::ExplainPushdownPlan,
        target_cluster: TargetCluster,
    ) {
        match plan.explainee {
            Explainee::Statement(ExplaineeStatement::Select {
                broken: false,
                plan,
                desc: _,
            }) => {
                let stage = return_if_err!(
                    self.peek_validate(
                        ctx.session(),
                        plan,
                        target_cluster,
                        None,
                        ExplainContext::Pushdown,
                        Some(ctx.session().vars().max_query_result_size()),
                    ),
                    ctx
                );
                self.sequence_staged(ctx, Span::current(), stage).await;
            }
            Explainee::MaterializedView(item_id) => {
                self.explain_pushdown_materialized_view(ctx, item_id).await;
            }
            _ => {
                ctx.retire(Err(AdapterError::Unsupported(
                    "EXPLAIN FILTER PUSHDOWN queries for this explainee type",
                )));
            }
        };
    }

    /// Executes an EXPLAIN FILTER PUSHDOWN, with read holds passed in.
    async fn execute_explain_pushdown_with_read_holds(
        &self,
        ctx: ExecuteContext,
        as_of: Antichain<Timestamp>,
        mz_now: ResultSpec<'static>,
        read_holds: Option<ReadHolds<Timestamp>>,
        imports: impl IntoIterator<Item = (GlobalId, MapFilterProject)> + 'static,
    ) {
        let fut = self
            .explain_pushdown_future(ctx.session(), as_of, mz_now, imports)
            .await;
        task::spawn(|| "render explain pushdown", async move {
            // Transfer the necessary read holds over to the background task
            let _read_holds = read_holds;
            let res = fut.await;
            ctx.retire(res);
        });
    }

    /// Returns a future that will execute EXPLAIN FILTER PUSHDOWN.
    async fn explain_pushdown_future<I: IntoIterator<Item = (GlobalId, MapFilterProject)>>(
        &self,
        session: &Session,
        as_of: Antichain<Timestamp>,
        mz_now: ResultSpec<'static>,
        imports: I,
    ) -> impl Future<Output = Result<ExecuteResponse, AdapterError>> + use<I> {
        // Get the needed Coordinator stuff and call the freestanding, shared helper.
        super::explain_pushdown_future_inner(
            session,
            &self.catalog,
            &self.controller.storage_collections,
            as_of,
            mz_now,
            imports,
        )
        .await
    }

    #[instrument]
    pub(super) async fn sequence_insert(
        &mut self,
        mut ctx: ExecuteContext,
        plan: plan::InsertPlan,
    ) {
        // Normally, this would get checked when trying to add "write ops" to
        // the transaction but we go down diverging paths below, based on
        // whether the INSERT is only constant values or not.
        //
        // For the non-constant case we sequence an implicit read-then-write,
        // which messes with the transaction ops and would allow an implicit
        // read-then-write to sneak into a read-only transaction.
        if !ctx.session_mut().transaction().allows_writes() {
            ctx.retire(Err(AdapterError::ReadOnlyTransaction));
            return;
        }

        // The structure of this code originates from a time where
        // `ReadThenWritePlan` was carrying an `MirRelationExpr` instead of an
        // optimized `MirRelationExpr`.
        //
        // Ideally, we would like to make the `selection.as_const().is_some()`
        // check on `plan.values` instead. However, `VALUES (1), (3)` statements
        // are planned as a Wrap($n, $vals) call, so until we can reduce
        // HirRelationExpr this will always returns false.
        //
        // Unfortunately, hitting the default path of the match below also
        // causes a lot of tests to fail, so we opted to go with the extra
        // `plan.values.clone()` statements when producing the `optimized_mir`
        // and re-optimize the values in the `sequence_read_then_write` call.
        let optimized_mir = if let Some(..) = &plan.values.as_const() {
            // We don't perform any optimizations on an expression that is already
            // a constant for writes, as we want to maximize bulk-insert throughput.
            let expr = return_if_err!(
                plan.values
                    .clone()
                    .lower(self.catalog().system_config(), None),
                ctx
            );
            OptimizedMirRelationExpr(expr)
        } else {
            // Collect optimizer parameters.
            let optimizer_config = optimize::OptimizerConfig::from(self.catalog().system_config());

            // (`optimize::view::Optimizer` has a special case for constant queries.)
            let mut optimizer = optimize::view::Optimizer::new(optimizer_config, None);

            // HIR ⇒ MIR lowering and MIR ⇒ MIR optimization (local)
            return_if_err!(optimizer.optimize(plan.values.clone()), ctx)
        };

        match optimized_mir.into_inner() {
            selection if selection.as_const().is_some() && plan.returning.is_empty() => {
                let catalog = self.owned_catalog();
                mz_ore::task::spawn(|| "coord::sequence_inner", async move {
                    let result =
                        Self::insert_constant(&catalog, ctx.session_mut(), plan.id, selection);
                    ctx.retire(result);
                });
            }
            // All non-constant values must be planned as read-then-writes.
            _ => {
                let desc_arity = match self.catalog().try_get_entry(&plan.id) {
                    Some(table) => {
                        // Inserts always occur at the latest version of the table.
                        let desc = table.relation_desc_latest().expect("table has a desc");
                        desc.arity()
                    }
                    None => {
                        ctx.retire(Err(AdapterError::Catalog(
                            mz_catalog::memory::error::Error {
                                kind: mz_catalog::memory::error::ErrorKind::Sql(
                                    CatalogError::UnknownItem(plan.id.to_string()),
                                ),
                            },
                        )));
                        return;
                    }
                };

                let finishing = RowSetFinishing {
                    order_by: vec![],
                    limit: None,
                    offset: 0,
                    project: (0..desc_arity).collect(),
                };

                let read_then_write_plan = plan::ReadThenWritePlan {
                    id: plan.id,
                    selection: plan.values,
                    finishing,
                    assignments: BTreeMap::new(),
                    kind: MutationKind::Insert,
                    returning: plan.returning,
                };

                self.sequence_read_then_write(ctx, read_then_write_plan)
                    .await;
            }
        }
    }

    /// ReadThenWrite is a plan whose writes depend on the results of a
    /// read. This works by doing a Peek then queuing a SendDiffs. No writes
    /// or read-then-writes can occur between the Peek and SendDiff otherwise a
    /// serializability violation could occur.
    #[instrument]
    pub(super) async fn sequence_read_then_write(
        &mut self,
        mut ctx: ExecuteContext,
        plan: plan::ReadThenWritePlan,
    ) {
        let mut source_ids: BTreeSet<_> = plan
            .selection
            .depends_on()
            .into_iter()
            .map(|gid| self.catalog().resolve_item_id(&gid))
            .collect();
        source_ids.insert(plan.id);

        // If the transaction doesn't already have write locks, acquire them.
        if ctx.session().transaction().write_locks().is_none() {
            // Pre-define all of the locks we need.
            let mut write_locks = WriteLocks::builder(source_ids.iter().copied());

            // Try acquiring all of our locks.
            for id in &source_ids {
                if let Some(lock) = self.try_grant_object_write_lock(*id) {
                    write_locks.insert_lock(*id, lock);
                }
            }

            // See if we acquired all of the neccessary locks.
            let write_locks = match write_locks.all_or_nothing(ctx.session().conn_id()) {
                Ok(locks) => locks,
                Err(missing) => {
                    // Defer our write if we couldn't acquire all of the locks.
                    let role_metadata = ctx.session().role_metadata().clone();
                    let acquire_future = self.grant_object_write_lock(missing).map(Option::Some);
                    let plan = DeferredPlan {
                        ctx,
                        plan: Plan::ReadThenWrite(plan),
                        validity: PlanValidity::new(
                            self.catalog.transient_revision(),
                            source_ids.clone(),
                            None,
                            None,
                            role_metadata,
                        ),
                        requires_locks: source_ids,
                    };
                    return self.defer_op(acquire_future, DeferredOp::Plan(plan));
                }
            };

            ctx.session_mut()
                .try_grant_write_locks(write_locks)
                .expect("session has already been granted write locks");
        }

        let plan::ReadThenWritePlan {
            id,
            kind,
            selection,
            mut assignments,
            finishing,
            mut returning,
        } = plan;

        // Read then writes can be queued, so re-verify the id exists.
        let desc = match self.catalog().try_get_entry(&id) {
            Some(table) => {
                // Inserts always occur at the latest version of the table.
                table
                    .relation_desc_latest()
                    .expect("table has a desc")
                    .into_owned()
            }
            None => {
                ctx.retire(Err(AdapterError::Catalog(
                    mz_catalog::memory::error::Error {
                        kind: mz_catalog::memory::error::ErrorKind::Sql(CatalogError::UnknownItem(
                            id.to_string(),
                        )),
                    },
                )));
                return;
            }
        };

        // Disallow mz_now in any position because read time and write time differ.
        let contains_temporal = return_if_err!(selection.contains_temporal(), ctx)
            || assignments.values().any(|e| e.contains_temporal())
            || returning.iter().any(|e| e.contains_temporal());
        if contains_temporal {
            ctx.retire(Err(AdapterError::Unsupported(
                "calls to mz_now in write statements",
            )));
            return;
        }

        // Ensure all objects `selection` depends on are valid for `ReadThenWrite` operations:
        //
        // - They do not refer to any objects whose notion of time moves differently than that of
        // user tables. This limitation is meant to ensure no writes occur between this read and the
        // subsequent write.
        // - They do not use mz_now(), whose time produced during read will differ from the write
        //   timestamp.
        fn validate_read_dependencies(
            catalog: &Catalog,
            id: &CatalogItemId,
        ) -> Result<(), AdapterError> {
            use CatalogItemType::*;
            use mz_catalog::memory::objects;
            let mut ids_to_check = Vec::new();
            let valid = match catalog.try_get_entry(id) {
                Some(entry) => {
                    if let CatalogItem::View(objects::View { optimized_expr, .. })
                    | CatalogItem::MaterializedView(objects::MaterializedView {
                        optimized_expr,
                        ..
                    }) = entry.item()
                    {
                        if optimized_expr.contains_temporal() {
                            return Err(AdapterError::Unsupported(
                                "calls to mz_now in write statements",
                            ));
                        }
                    }
                    match entry.item().typ() {
                        typ @ (Func | View | MaterializedView | ContinualTask) => {
                            ids_to_check.extend(entry.uses());
                            let valid_id = id.is_user() || matches!(typ, Func);
                            valid_id
                        }
                        Source | Secret | Connection => false,
                        // Cannot select from sinks or indexes.
                        Sink | Index => unreachable!(),
                        Table => {
                            if !id.is_user() {
                                // We can't read from non-user tables
                                false
                            } else {
                                // We can't read from tables that are source-exports
                                entry.source_export_details().is_none()
                            }
                        }
                        Type => true,
                    }
                }
                None => false,
            };
            if !valid {
                return Err(AdapterError::InvalidTableMutationSelection);
            }
            for id in ids_to_check {
                validate_read_dependencies(catalog, &id)?;
            }
            Ok(())
        }

        for gid in selection.depends_on() {
            let item_id = self.catalog().resolve_item_id(&gid);
            if let Err(err) = validate_read_dependencies(self.catalog(), &item_id) {
                ctx.retire(Err(err));
                return;
            }
        }

        let (peek_tx, peek_rx) = oneshot::channel();
        let peek_client_tx = ClientTransmitter::new(peek_tx, self.internal_cmd_tx.clone());
        let (tx, _, session, extra) = ctx.into_parts();
        // We construct a new execute context for the peek, with a trivial (`Default::default()`)
        // execution context, because this peek does not directly correspond to an execute,
        // and so we don't need to take any action on its retirement.
        // TODO[btv]: we might consider extending statement logging to log the inner
        // statement separately, here. That would require us to plumb through the SQL of the inner statement,
        // and mint a new "real" execution context here. We'd also have to add some logic to
        // make sure such "sub-statements" are always sampled when the top-level statement is
        //
        // It's debatable whether this makes sense conceptually,
        // because the inner fragment here is not actually a
        // "statement" in its own right.
        let peek_ctx = ExecuteContext::from_parts(
            peek_client_tx,
            self.internal_cmd_tx.clone(),
            session,
            Default::default(),
        );

        self.sequence_peek(
            peek_ctx,
            plan::SelectPlan {
                select: None,
                source: selection,
                when: QueryWhen::FreshestTableWrite,
                finishing,
                copy_to: None,
            },
            TargetCluster::Active,
            None,
        )
        .await;

        let internal_cmd_tx = self.internal_cmd_tx.clone();
        let strict_serializable_reads_tx = self.strict_serializable_reads_tx.clone();
        let catalog = self.owned_catalog();
        let max_result_size = self.catalog().system_config().max_result_size();

        task::spawn(|| format!("sequence_read_then_write:{id}"), async move {
            let (peek_response, session) = match peek_rx.await {
                Ok(Response {
                    result: Ok(resp),
                    session,
                    otel_ctx,
                }) => {
                    otel_ctx.attach_as_parent();
                    (resp, session)
                }
                Ok(Response {
                    result: Err(e),
                    session,
                    otel_ctx,
                }) => {
                    let ctx =
                        ExecuteContext::from_parts(tx, internal_cmd_tx.clone(), session, extra);
                    otel_ctx.attach_as_parent();
                    ctx.retire(Err(e));
                    return;
                }
                // It is not an error for these results to be ready after `peek_client_tx` has been dropped.
                Err(e) => return warn!("internal_cmd_rx dropped before we could send: {:?}", e),
            };
            let mut ctx = ExecuteContext::from_parts(tx, internal_cmd_tx.clone(), session, extra);
            let mut timeout_dur = *ctx.session().vars().statement_timeout();

            // Timeout of 0 is equivalent to "off", meaning we will wait "forever."
            if timeout_dur == Duration::ZERO {
                timeout_dur = Duration::MAX;
            }

            let style = ExprPrepStyle::OneShot {
                logical_time: EvalTime::NotAvailable, // We already errored out on mz_now above.
                session: ctx.session(),
                catalog_state: catalog.state(),
            };
            for expr in assignments.values_mut().chain(returning.iter_mut()) {
                return_if_err!(prep_scalar_expr(expr, style.clone()), ctx);
            }

            let make_diffs =
                move |mut rows: Box<dyn RowIterator>| -> Result<(Vec<(Row, Diff)>, u64), AdapterError> {
                    let arena = RowArena::new();
                    let mut diffs = Vec::new();
                    let mut datum_vec = mz_repr::DatumVec::new();

                    while let Some(row) = rows.next() {
                        if !assignments.is_empty() {
                            assert!(
                                matches!(kind, MutationKind::Update),
                                "only updates support assignments"
                            );
                            let mut datums = datum_vec.borrow_with(row);
                            let mut updates = vec![];
                            for (idx, expr) in &assignments {
                                let updated = match expr.eval(&datums, &arena) {
                                    Ok(updated) => updated,
                                    Err(e) => return Err(AdapterError::Unstructured(anyhow!(e))),
                                };
                                updates.push((*idx, updated));
                            }
                            for (idx, new_value) in updates {
                                datums[idx] = new_value;
                            }
                            let updated = Row::pack_slice(&datums);
                            diffs.push((updated, Diff::ONE));
                        }
                        match kind {
                            // Updates and deletes always remove the
                            // current row. Updates will also add an
                            // updated value.
                            MutationKind::Update | MutationKind::Delete => {
                                diffs.push((row.to_owned(), Diff::MINUS_ONE))
                            }
                            MutationKind::Insert => diffs.push((row.to_owned(), Diff::ONE)),
                        }
                    }

                    // Sum of all the rows' byte size, for checking if we go
                    // above the max_result_size threshold.
                    let mut byte_size: u64 = 0;
                    for (row, diff) in &diffs {
                        byte_size = byte_size.saturating_add(u64::cast_from(row.byte_len()));
                        if diff.is_positive() {
                            for (idx, datum) in row.iter().enumerate() {
                                desc.constraints_met(idx, &datum)?;
                            }
                        }
                    }
                    Ok((diffs, byte_size))
                };

            let diffs = match peek_response {
                ExecuteResponse::SendingRowsStreaming {
                    rows: mut rows_stream,
                    ..
                } => {
                    let mut byte_size: u64 = 0;
                    let mut diffs = Vec::new();
                    let result = loop {
                        match tokio::time::timeout(timeout_dur, rows_stream.next()).await {
                            Ok(Some(res)) => match res {
                                PeekResponseUnary::Rows(new_rows) => {
                                    match make_diffs(new_rows) {
                                        Ok((mut new_diffs, new_byte_size)) => {
                                            byte_size = byte_size.saturating_add(new_byte_size);
                                            if byte_size > max_result_size {
                                                break Err(AdapterError::ResultSize(format!(
                                                    "result exceeds max size of {max_result_size}"
                                                )));
                                            }
                                            diffs.append(&mut new_diffs)
                                        }
                                        Err(e) => break Err(e),
                                    };
                                }
                                PeekResponseUnary::Canceled => break Err(AdapterError::Canceled),
                                PeekResponseUnary::Error(e) => {
                                    break Err(AdapterError::Unstructured(anyhow!(e)));
                                }
                            },
                            Ok(None) => break Ok(diffs),
                            Err(_) => {
                                // We timed out, so remove the pending peek. This is
                                // best-effort and doesn't guarantee we won't
                                // receive a response.
                                // It is not an error for this timeout to occur after `internal_cmd_rx` has been dropped.
                                let result = internal_cmd_tx.send(Message::CancelPendingPeeks {
                                    conn_id: ctx.session().conn_id().clone(),
                                });
                                if let Err(e) = result {
                                    warn!("internal_cmd_rx dropped before we could send: {:?}", e);
                                }
                                break Err(AdapterError::StatementTimeout);
                            }
                        }
                    };

                    result
                }
                ExecuteResponse::SendingRowsImmediate { rows } => {
                    make_diffs(rows).map(|(diffs, _byte_size)| diffs)
                }
                resp => Err(AdapterError::Unstructured(anyhow!(
                    "unexpected peek response: {resp:?}"
                ))),
            };

            let mut returning_rows = Vec::new();
            let mut diff_err: Option<AdapterError> = None;
            if let (false, Ok(diffs)) = (returning.is_empty(), &diffs) {
                let arena = RowArena::new();
                for (row, diff) in diffs {
                    if !diff.is_positive() {
                        continue;
                    }
                    let mut returning_row = Row::with_capacity(returning.len());
                    let mut packer = returning_row.packer();
                    for expr in &returning {
                        let datums: Vec<_> = row.iter().collect();
                        match expr.eval(&datums, &arena) {
                            Ok(datum) => {
                                packer.push(datum);
                            }
                            Err(err) => {
                                diff_err = Some(err.into());
                                break;
                            }
                        }
                    }
                    let diff = NonZeroI64::try_from(diff.into_inner()).expect("known to be >= 1");
                    let diff = match NonZeroUsize::try_from(diff) {
                        Ok(diff) => diff,
                        Err(err) => {
                            diff_err = Some(err.into());
                            break;
                        }
                    };
                    returning_rows.push((returning_row, diff));
                    if diff_err.is_some() {
                        break;
                    }
                }
            }
            let diffs = if let Some(err) = diff_err {
                Err(err)
            } else {
                diffs
            };

            // We need to clear out the timestamp context so the write doesn't fail due to a
            // read only transaction.
            let timestamp_context = ctx.session_mut().take_transaction_timestamp_context();
            // No matter what isolation level the client is using, we must linearize this
            // read. The write will be performed right after this, as part of a single
            // transaction, so the write must have a timestamp greater than or equal to the
            // read.
            //
            // Note: It's only OK for the write to have a greater timestamp than the read
            // because the write lock prevents any other writes from happening in between
            // the read and write.
            if let Some(timestamp_context) = timestamp_context {
                let (tx, rx) = tokio::sync::oneshot::channel();
                let conn_id = ctx.session().conn_id().clone();
                let pending_read_txn = PendingReadTxn {
                    txn: PendingRead::ReadThenWrite { ctx, tx },
                    timestamp_context,
                    created: Instant::now(),
                    num_requeues: 0,
                    otel_ctx: OpenTelemetryContext::obtain(),
                };
                let result = strict_serializable_reads_tx.send((conn_id, pending_read_txn));
                // It is not an error for these results to be ready after `strict_serializable_reads_rx` has been dropped.
                if let Err(e) = result {
                    warn!(
                        "strict_serializable_reads_tx dropped before we could send: {:?}",
                        e
                    );
                    return;
                }
                let result = rx.await;
                // It is not an error for these results to be ready after `tx` has been dropped.
                ctx = match result {
                    Ok(Some(ctx)) => ctx,
                    Ok(None) => {
                        // Coordinator took our context and will handle responding to the client.
                        // This usually indicates that our transaction was aborted.
                        return;
                    }
                    Err(e) => {
                        warn!(
                            "tx used to linearize read in read then write transaction dropped before we could send: {:?}",
                            e
                        );
                        return;
                    }
                };
            }

            match diffs {
                Ok(diffs) => {
                    let result = Self::send_diffs(
                        ctx.session_mut(),
                        plan::SendDiffsPlan {
                            id,
                            updates: diffs,
                            kind,
                            returning: returning_rows,
                            max_result_size,
                        },
                    );
                    ctx.retire(result);
                }
                Err(e) => {
                    ctx.retire(Err(e));
                }
            }
        });
    }

    #[instrument]
    pub(super) async fn sequence_alter_item_rename(
        &mut self,
        ctx: &mut ExecuteContext,
        plan: plan::AlterItemRenamePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let op = catalog::Op::RenameItem {
            id: plan.id,
            current_full_name: plan.current_full_name,
            to_name: plan.to_name,
        };
        match self
            .catalog_transact_with_ddl_transaction(ctx, vec![op], |_, _| Box::pin(async {}))
            .await
        {
            Ok(()) => Ok(ExecuteResponse::AlteredObject(plan.object_type)),
            Err(err) => Err(err),
        }
    }

    #[instrument]
    pub(super) async fn sequence_alter_retain_history(
        &mut self,
        ctx: &mut ExecuteContext,
        plan: plan::AlterRetainHistoryPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let ops = vec![catalog::Op::AlterRetainHistory {
            id: plan.id,
            value: plan.value,
            window: plan.window,
        }];
        self.catalog_transact_with_side_effects(Some(ctx), ops, move |coord, _ctx| {
            Box::pin(async move {
                let catalog_item = coord.catalog().get_entry(&plan.id).item();
                let cluster = match catalog_item {
                    CatalogItem::Table(_)
                    | CatalogItem::MaterializedView(_)
                    | CatalogItem::Source(_)
                    | CatalogItem::ContinualTask(_) => None,
                    CatalogItem::Index(index) => Some(index.cluster_id),
                    CatalogItem::Log(_)
                    | CatalogItem::View(_)
                    | CatalogItem::Sink(_)
                    | CatalogItem::Type(_)
                    | CatalogItem::Func(_)
                    | CatalogItem::Secret(_)
                    | CatalogItem::Connection(_) => unreachable!(),
                };
                match cluster {
                    Some(cluster) => {
                        coord.update_compute_read_policy(cluster, plan.id, plan.window.into());
                    }
                    None => {
                        coord.update_storage_read_policies(vec![(plan.id, plan.window.into())]);
                    }
                }
            })
        })
        .await?;
        Ok(ExecuteResponse::AlteredObject(plan.object_type))
    }

    #[instrument]
    pub(super) async fn sequence_alter_schema_rename(
        &mut self,
        ctx: &mut ExecuteContext,
        plan: plan::AlterSchemaRenamePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let (database_spec, schema_spec) = plan.cur_schema_spec;
        let op = catalog::Op::RenameSchema {
            database_spec,
            schema_spec,
            new_name: plan.new_schema_name,
            check_reserved_names: true,
        };
        match self
            .catalog_transact_with_ddl_transaction(ctx, vec![op], |_, _| Box::pin(async {}))
            .await
        {
            Ok(()) => Ok(ExecuteResponse::AlteredObject(ObjectType::Schema)),
            Err(err) => Err(err),
        }
    }

    #[instrument]
    pub(super) async fn sequence_alter_schema_swap(
        &mut self,
        ctx: &mut ExecuteContext,
        plan: plan::AlterSchemaSwapPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let plan::AlterSchemaSwapPlan {
            schema_a_spec: (schema_a_db, schema_a),
            schema_a_name,
            schema_b_spec: (schema_b_db, schema_b),
            schema_b_name,
            name_temp,
        } = plan;

        let op_a = catalog::Op::RenameSchema {
            database_spec: schema_a_db,
            schema_spec: schema_a,
            new_name: name_temp,
            check_reserved_names: false,
        };
        let op_b = catalog::Op::RenameSchema {
            database_spec: schema_b_db,
            schema_spec: schema_b,
            new_name: schema_a_name,
            check_reserved_names: false,
        };
        let op_c = catalog::Op::RenameSchema {
            database_spec: schema_a_db,
            schema_spec: schema_a,
            new_name: schema_b_name,
            check_reserved_names: false,
        };

        match self
            .catalog_transact_with_ddl_transaction(ctx, vec![op_a, op_b, op_c], |_, _| {
                Box::pin(async {})
            })
            .await
        {
            Ok(()) => Ok(ExecuteResponse::AlteredObject(ObjectType::Schema)),
            Err(err) => Err(err),
        }
    }

    #[instrument]
    pub(super) async fn sequence_alter_role(
        &mut self,
        session: &Session,
        plan::AlterRolePlan { id, name, option }: plan::AlterRolePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let catalog = self.catalog().for_session(session);
        let role = catalog.get_role(&id);

        // We'll send these notices to the user, if the operation is successful.
        let mut notices = vec![];

        // Get the attributes and variables from the role, as they currently are.
        let mut attributes: RoleAttributesRaw = role.attributes().clone().into();
        let mut vars = role.vars().clone();

        // Whether to set the password to NULL. This is a special case since the existing
        // password is not stored in the role attributes.
        let mut nopassword = false;

        // Apply our updates.
        match option {
            PlannedAlterRoleOption::Attributes(attrs) => {
                self.validate_role_attributes(&attrs.clone().into())?;

                if let Some(inherit) = attrs.inherit {
                    attributes.inherit = inherit;
                }

                if let Some(password) = attrs.password {
                    attributes.password = Some(password);
                    attributes.scram_iterations =
                        Some(self.catalog().system_config().scram_iterations())
                }

                if let Some(superuser) = attrs.superuser {
                    attributes.superuser = Some(superuser);
                }

                if let Some(login) = attrs.login {
                    attributes.login = Some(login);
                }

                if attrs.nopassword.unwrap_or(false) {
                    nopassword = true;
                }

                if let Some(notice) = self.should_emit_rbac_notice(session) {
                    notices.push(notice);
                }
            }
            PlannedAlterRoleOption::Variable(variable) => {
                // Get the variable to make sure it's valid and visible.
                let session_var = session.vars().inspect(variable.name())?;
                // Return early if it's not visible.
                session_var.visible(session.user(), catalog.system_vars())?;

                // Emit a warning when deprecated variables are used.
                // TODO(database-issues#8069) remove this after sufficient time has passed
                if variable.name() == vars::OLD_AUTO_ROUTE_CATALOG_QUERIES {
                    notices.push(AdapterNotice::AutoRouteIntrospectionQueriesUsage);
                } else if let PlannedRoleVariable::Set {
                    name,
                    value: VariableValue::Values(vals),
                } = &variable
                {
                    if name == vars::CLUSTER.name() && vals[0] == vars::OLD_CATALOG_SERVER_CLUSTER {
                        notices.push(AdapterNotice::IntrospectionClusterUsage);
                    }
                }

                let var_name = match variable {
                    PlannedRoleVariable::Set { name, value } => {
                        // Update our persisted set.
                        match &value {
                            VariableValue::Default => {
                                vars.remove(&name);
                            }
                            VariableValue::Values(vals) => {
                                let var = match &vals[..] {
                                    [val] => OwnedVarInput::Flat(val.clone()),
                                    vals => OwnedVarInput::SqlSet(vals.to_vec()),
                                };
                                // Make sure the input is valid.
                                session_var.check(var.borrow())?;

                                vars.insert(name.clone(), var);
                            }
                        };
                        name
                    }
                    PlannedRoleVariable::Reset { name } => {
                        // Remove it from our persisted values.
                        vars.remove(&name);
                        name
                    }
                };

                // Emit a notice that they need to reconnect to see the change take effect.
                notices.push(AdapterNotice::VarDefaultUpdated {
                    role: Some(name.clone()),
                    var_name: Some(var_name),
                });
            }
        }

        let op = catalog::Op::AlterRole {
            id,
            name,
            attributes,
            nopassword,
            vars: RoleVars { map: vars },
        };
        let response = self
            .catalog_transact(Some(session), vec![op])
            .await
            .map(|_| ExecuteResponse::AlteredRole)?;

        // Send all of our queued notices.
        session.add_notices(notices);

        Ok(response)
    }

    #[instrument]
    pub(super) async fn sequence_alter_sink_prepare(
        &mut self,
        ctx: ExecuteContext,
        plan: plan::AlterSinkPlan,
    ) {
        // Put a read hold on the new relation
        let id_bundle = crate::CollectionIdBundle {
            storage_ids: BTreeSet::from_iter([plan.sink.from]),
            compute_ids: BTreeMap::new(),
        };
        let read_hold = self.acquire_read_holds(&id_bundle);

        let Some(read_ts) = read_hold.least_valid_read().into_option() else {
            ctx.retire(Err(AdapterError::UnreadableSinkCollection));
            return;
        };

        let otel_ctx = OpenTelemetryContext::obtain();
        let from_item_id = self.catalog().resolve_item_id(&plan.sink.from);

        let plan_validity = PlanValidity::new(
            self.catalog().transient_revision(),
            BTreeSet::from_iter([plan.item_id, from_item_id]),
            Some(plan.in_cluster),
            None,
            ctx.session().role_metadata().clone(),
        );

        info!(
            "preparing alter sink for {}: frontiers={:?} export={:?}",
            plan.global_id,
            self.controller
                .storage_collections
                .collections_frontiers(vec![plan.global_id, plan.sink.from]),
            self.controller.storage.export(plan.global_id)
        );

        // Now we must wait for the sink to make enough progress such that there is overlap between
        // the new `from` collection's read hold and the sink's write frontier.
        //
        // TODO(database-issues#9820): If the sink is dropped while we are waiting for progress,
        // the watch set never completes and neither does the `ALTER SINK` command.
        self.install_storage_watch_set(
            ctx.session().conn_id().clone(),
            BTreeSet::from_iter([plan.global_id]),
            read_ts,
            WatchSetResponse::AlterSinkReady(AlterSinkReadyContext {
                ctx: Some(ctx),
                otel_ctx,
                plan,
                plan_validity,
                read_hold,
            }),
        );
    }

    #[instrument]
    pub async fn sequence_alter_sink_finish(&mut self, mut ctx: AlterSinkReadyContext) {
        ctx.otel_ctx.attach_as_parent();

        let plan::AlterSinkPlan {
            item_id,
            global_id,
            sink: sink_plan,
            with_snapshot,
            in_cluster,
        } = ctx.plan.clone();

        // We avoid taking the DDL lock for `ALTER SINK SET FROM` commands, see
        // `Coordinator::must_serialize_ddl`. We therefore must assume that the world has
        // arbitrarily changed since we performed planning, and we must re-assert that it still
        // matches our requirements.
        //
        // The `PlanValidity` check ensures that both the sink and the new source relation still
        // exist. Apart from that we have to ensure that nobody else altered the sink in the mean
        // time, which we do by comparing the catalog sink version to the one in the plan.
        match ctx.plan_validity.check(self.catalog()) {
            Ok(()) => {}
            Err(err) => {
                ctx.retire(Err(err));
                return;
            }
        }

        let entry = self.catalog().get_entry(&item_id);
        let CatalogItem::Sink(old_sink) = entry.item() else {
            panic!("invalid item kind for `AlterSinkPlan`");
        };

        if sink_plan.version != old_sink.version + 1 {
            ctx.retire(Err(AdapterError::ChangedPlan(
                "sink was altered concurrently".into(),
            )));
            return;
        }

        info!(
            "finishing alter sink for {global_id}: frontiers={:?} export={:?}",
            self.controller
                .storage_collections
                .collections_frontiers(vec![global_id, sink_plan.from]),
            self.controller.storage.export(global_id),
        );

        // Assert that we can recover the updates that happened at the timestamps of the write
        // frontier. This must be true in this call.
        let write_frontier = &self
            .controller
            .storage
            .export(global_id)
            .expect("sink known to exist")
            .write_frontier;
        let as_of = ctx.read_hold.least_valid_read();
        assert!(
            write_frontier.iter().all(|t| as_of.less_than(t)),
            "{:?} should be strictly less than {:?}",
            &*as_of,
            &**write_frontier
        );

        // Parse the `create_sql` so we can update it to the new sink definition.
        //
        // Note that we need to use the `create_sql` from the catalog here, not the one from the
        // sink plan. Even though we ensure that the sink version didn't change since planning, the
        // names in the `create_sql` may have changed, for example due to a schema swap.
        let create_sql = &old_sink.create_sql;
        let parsed = mz_sql::parse::parse(create_sql).expect("valid create_sql");
        let Statement::CreateSink(mut stmt) = parsed.into_element().ast else {
            unreachable!("invalid statement kind for sink");
        };

        // Update the sink version.
        stmt.with_options
            .retain(|o| o.name != CreateSinkOptionName::Version);
        stmt.with_options.push(CreateSinkOption {
            name: CreateSinkOptionName::Version,
            value: Some(WithOptionValue::Value(mz_sql::ast::Value::Number(
                sink_plan.version.to_string(),
            ))),
        });

        let conn_catalog = self.catalog().for_system_session();
        let (mut stmt, resolved_ids) =
            mz_sql::names::resolve(&conn_catalog, stmt).expect("resolvable create_sql");

        // Update the `from` relation.
        let from_entry = self.catalog().get_entry_by_global_id(&sink_plan.from);
        let full_name = self.catalog().resolve_full_name(from_entry.name(), None);
        stmt.from = ResolvedItemName::Item {
            id: from_entry.id(),
            qualifiers: from_entry.name.qualifiers.clone(),
            full_name,
            print_id: true,
            version: from_entry.version,
        };

        let new_sink = Sink {
            create_sql: stmt.to_ast_string_stable(),
            global_id,
            from: sink_plan.from,
            connection: sink_plan.connection.clone(),
            envelope: sink_plan.envelope,
            version: sink_plan.version,
            with_snapshot,
            resolved_ids: resolved_ids.clone(),
            cluster_id: in_cluster,
            commit_interval: sink_plan.commit_interval,
        };

        let ops = vec![catalog::Op::UpdateItem {
            id: item_id,
            name: entry.name().clone(),
            to_item: CatalogItem::Sink(new_sink),
        }];

        match self
            .catalog_transact(Some(ctx.ctx().session_mut()), ops)
            .await
        {
            Ok(()) => {}
            Err(err) => {
                ctx.retire(Err(err));
                return;
            }
        }

        let storage_sink_desc = StorageSinkDesc {
            from: sink_plan.from,
            from_desc: from_entry
                .relation_desc()
                .expect("sinks can only be built on items with descs")
                .into_owned(),
            connection: sink_plan
                .connection
                .clone()
                .into_inline_connection(self.catalog().state()),
            envelope: sink_plan.envelope,
            as_of,
            with_snapshot,
            version: sink_plan.version,
            from_storage_metadata: (),
            to_storage_metadata: (),
            commit_interval: sink_plan.commit_interval,
        };

        self.controller
            .storage
            .alter_export(
                global_id,
                ExportDescription {
                    sink: storage_sink_desc,
                    instance_id: in_cluster,
                },
            )
            .await
            .unwrap_or_terminate("cannot fail to alter source desc");

        ctx.retire(Ok(ExecuteResponse::AlteredObject(ObjectType::Sink)));
    }

    #[instrument]
    pub(super) async fn sequence_alter_connection(
        &mut self,
        ctx: ExecuteContext,
        AlterConnectionPlan { id, action }: AlterConnectionPlan,
    ) {
        match action {
            AlterConnectionAction::RotateKeys => {
                self.sequence_rotate_keys(ctx, id).await;
            }
            AlterConnectionAction::AlterOptions {
                set_options,
                drop_options,
                validate,
            } => {
                self.sequence_alter_connection_options(ctx, id, set_options, drop_options, validate)
                    .await
            }
        }
    }

    #[instrument]
    async fn sequence_alter_connection_options(
        &mut self,
        mut ctx: ExecuteContext,
        id: CatalogItemId,
        set_options: BTreeMap<ConnectionOptionName, Option<WithOptionValue<mz_sql::names::Aug>>>,
        drop_options: BTreeSet<ConnectionOptionName>,
        validate: bool,
    ) {
        let cur_entry = self.catalog().get_entry(&id);
        let cur_conn = cur_entry.connection().expect("known to be connection");
        let connection_gid = cur_conn.global_id();

        let inner = || -> Result<Connection, AdapterError> {
            // Parse statement.
            let create_conn_stmt = match mz_sql::parse::parse(&cur_conn.create_sql)
                .expect("invalid create sql persisted to catalog")
                .into_element()
                .ast
            {
                Statement::CreateConnection(stmt) => stmt,
                _ => unreachable!("proved type is source"),
            };

            let catalog = self.catalog().for_system_session();

            // Resolve items in statement
            let (mut create_conn_stmt, resolved_ids) =
                mz_sql::names::resolve(&catalog, create_conn_stmt)
                    .map_err(|e| AdapterError::internal("ALTER CONNECTION", e))?;

            // Retain options that are neither set nor dropped.
            create_conn_stmt
                .values
                .retain(|o| !set_options.contains_key(&o.name) && !drop_options.contains(&o.name));

            // Set new values
            create_conn_stmt.values.extend(
                set_options
                    .into_iter()
                    .map(|(name, value)| ConnectionOption { name, value }),
            );

            // Open a new catalog, which we will use to re-plan our
            // statement with the desired config.
            let mut catalog = self.catalog().for_system_session();
            catalog.mark_id_unresolvable_for_replanning(id);

            // Re-define our source in terms of the amended statement
            let plan = match mz_sql::plan::plan(
                None,
                &catalog,
                Statement::CreateConnection(create_conn_stmt),
                &Params::empty(),
                &resolved_ids,
            )
            .map_err(|e| AdapterError::InvalidAlter("CONNECTION", e))?
            {
                Plan::CreateConnection(plan) => plan,
                _ => unreachable!("create source plan is only valid response"),
            };

            // Parse statement.
            let create_conn_stmt = match mz_sql::parse::parse(&plan.connection.create_sql)
                .expect("invalid create sql persisted to catalog")
                .into_element()
                .ast
            {
                Statement::CreateConnection(stmt) => stmt,
                _ => unreachable!("proved type is source"),
            };

            let catalog = self.catalog().for_system_session();

            // Resolve items in statement
            let (_, new_deps) = mz_sql::names::resolve(&catalog, create_conn_stmt)
                .map_err(|e| AdapterError::internal("ALTER CONNECTION", e))?;

            Ok(Connection {
                create_sql: plan.connection.create_sql,
                global_id: cur_conn.global_id,
                details: plan.connection.details,
                resolved_ids: new_deps,
            })
        };

        let conn = match inner() {
            Ok(conn) => conn,
            Err(e) => {
                return ctx.retire(Err(e));
            }
        };

        if validate {
            let connection = conn
                .details
                .to_connection()
                .into_inline_connection(self.catalog().state());

            let internal_cmd_tx = self.internal_cmd_tx.clone();
            let transient_revision = self.catalog().transient_revision();
            let conn_id = ctx.session().conn_id().clone();
            let otel_ctx = OpenTelemetryContext::obtain();
            let role_metadata = ctx.session().role_metadata().clone();
            let current_storage_parameters = self.controller.storage.config().clone();

            task::spawn(
                || format!("validate_alter_connection:{conn_id}"),
                async move {
                    let resolved_ids = conn.resolved_ids.clone();
                    let dependency_ids: BTreeSet<_> = resolved_ids.items().copied().collect();
                    let result = match connection.validate(id, &current_storage_parameters).await {
                        Ok(()) => Ok(conn),
                        Err(err) => Err(err.into()),
                    };

                    // It is not an error for validation to complete after `internal_cmd_rx` is dropped.
                    let result = internal_cmd_tx.send(Message::AlterConnectionValidationReady(
                        AlterConnectionValidationReady {
                            ctx,
                            result,
                            connection_id: id,
                            connection_gid,
                            plan_validity: PlanValidity::new(
                                transient_revision,
                                dependency_ids.clone(),
                                None,
                                None,
                                role_metadata,
                            ),
                            otel_ctx,
                            resolved_ids,
                        },
                    ));
                    if let Err(e) = result {
                        tracing::warn!("internal_cmd_rx dropped before we could send: {:?}", e);
                    }
                },
            );
        } else {
            let result = self
                .sequence_alter_connection_stage_finish(ctx.session_mut(), id, conn)
                .await;
            ctx.retire(result);
        }
    }

    #[instrument]
    pub(crate) async fn sequence_alter_connection_stage_finish(
        &mut self,
        session: &Session,
        id: CatalogItemId,
        connection: Connection,
    ) -> Result<ExecuteResponse, AdapterError> {
        match self.catalog.get_entry(&id).item() {
            CatalogItem::Connection(curr_conn) => {
                curr_conn
                    .details
                    .to_connection()
                    .alter_compatible(curr_conn.global_id, &connection.details.to_connection())
                    .map_err(StorageError::from)?;
            }
            _ => unreachable!("known to be a connection"),
        };

        let ops = vec![catalog::Op::UpdateItem {
            id,
            name: self.catalog.get_entry(&id).name().clone(),
            to_item: CatalogItem::Connection(connection.clone()),
        }];

        self.catalog_transact(Some(session), ops).await?;

        match connection.details {
            ConnectionDetails::AwsPrivatelink(ref privatelink) => {
                let spec = VpcEndpointConfig {
                    aws_service_name: privatelink.service_name.to_owned(),
                    availability_zone_ids: privatelink.availability_zones.to_owned(),
                };
                self.cloud_resource_controller
                    .as_ref()
                    .ok_or(AdapterError::Unsupported("AWS PrivateLink connections"))?
                    .ensure_vpc_endpoint(id, spec)
                    .await?;
            }
            _ => {}
        };

        let entry = self.catalog().get_entry(&id);

        let mut connections = VecDeque::new();
        connections.push_front(entry.id());

        let mut source_connections = BTreeMap::new();
        let mut sink_connections = BTreeMap::new();
        let mut source_export_data_configs = BTreeMap::new();

        while let Some(id) = connections.pop_front() {
            for id in self.catalog.get_entry(&id).used_by() {
                let entry = self.catalog.get_entry(id);
                match entry.item() {
                    CatalogItem::Connection(_) => connections.push_back(*id),
                    CatalogItem::Source(source) => {
                        let desc = match &entry.source().expect("known to be source").data_source {
                            DataSourceDesc::Ingestion { desc, .. }
                            | DataSourceDesc::OldSyntaxIngestion { desc, .. } => {
                                desc.clone().into_inline_connection(self.catalog().state())
                            }
                            _ => unreachable!("only ingestions reference connections"),
                        };

                        source_connections.insert(source.global_id, desc.connection);
                    }
                    CatalogItem::Sink(sink) => {
                        let export = entry.sink().expect("known to be sink");
                        sink_connections.insert(
                            sink.global_id,
                            export
                                .connection
                                .clone()
                                .into_inline_connection(self.catalog().state()),
                        );
                    }
                    CatalogItem::Table(table) => {
                        // This is a source-fed table that reference a schema registry
                        // connection as a part of its encoding / data config
                        if let Some((_, _, _, export_data_config)) = entry.source_export_details() {
                            let data_config = export_data_config.clone();
                            source_export_data_configs.insert(
                                table.global_id_writes(),
                                data_config.into_inline_connection(self.catalog().state()),
                            );
                        }
                    }
                    t => unreachable!("connection dependency not expected on {:?}", t),
                }
            }
        }

        if !source_connections.is_empty() {
            self.controller
                .storage
                .alter_ingestion_connections(source_connections)
                .await
                .unwrap_or_terminate("cannot fail to alter ingestion connection");
        }

        if !sink_connections.is_empty() {
            self.controller
                .storage
                .alter_export_connections(sink_connections)
                .await
                .unwrap_or_terminate("altering exports after txn must succeed");
        }

        if !source_export_data_configs.is_empty() {
            self.controller
                .storage
                .alter_ingestion_export_data_configs(source_export_data_configs)
                .await
                .unwrap_or_terminate("altering source export data configs after txn must succeed");
        }

        Ok(ExecuteResponse::AlteredObject(ObjectType::Connection))
    }

    #[instrument]
    pub(super) async fn sequence_alter_source(
        &mut self,
        session: &Session,
        plan::AlterSourcePlan {
            item_id,
            ingestion_id,
            action,
        }: plan::AlterSourcePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let cur_entry = self.catalog().get_entry(&item_id);
        let cur_source = cur_entry.source().expect("known to be source");

        let create_sql_to_stmt_deps = |coord: &Coordinator, err_cx, create_source_sql| {
            // Parse statement.
            let create_source_stmt = match mz_sql::parse::parse(create_source_sql)
                .expect("invalid create sql persisted to catalog")
                .into_element()
                .ast
            {
                Statement::CreateSource(stmt) => stmt,
                _ => unreachable!("proved type is source"),
            };

            let catalog = coord.catalog().for_system_session();

            // Resolve items in statement
            mz_sql::names::resolve(&catalog, create_source_stmt)
                .map_err(|e| AdapterError::internal(err_cx, e))
        };

        match action {
            plan::AlterSourceAction::AddSubsourceExports {
                subsources,
                options,
            } => {
                const ALTER_SOURCE: &str = "ALTER SOURCE...ADD SUBSOURCES";

                let mz_sql::plan::AlterSourceAddSubsourceOptionExtracted {
                    text_columns: mut new_text_columns,
                    exclude_columns: mut new_exclude_columns,
                    ..
                } = options.try_into()?;

                // Resolve items in statement
                let (mut create_source_stmt, resolved_ids) =
                    create_sql_to_stmt_deps(self, ALTER_SOURCE, cur_entry.create_sql())?;

                // Get all currently referred-to items
                let catalog = self.catalog();
                let curr_references: BTreeSet<_> = catalog
                    .get_entry(&item_id)
                    .used_by()
                    .into_iter()
                    .filter_map(|subsource| {
                        catalog
                            .get_entry(subsource)
                            .subsource_details()
                            .map(|(_id, reference, _details)| reference)
                    })
                    .collect();

                // We are doing a lot of unwrapping, so just make an error to reference; all of
                // these invariants are guaranteed to be true because of how we plan subsources.
                let purification_err =
                    || AdapterError::internal(ALTER_SOURCE, "error in subsource purification");

                // TODO(roshan): Remove all the text-column/ignore-column option merging here once
                // we remove support for implicitly created subsources from a `CREATE SOURCE`
                // statement.
                match &mut create_source_stmt.connection {
                    CreateSourceConnection::Postgres {
                        options: curr_options,
                        ..
                    } => {
                        let mz_sql::plan::PgConfigOptionExtracted {
                            mut text_columns, ..
                        } = curr_options.clone().try_into()?;

                        // Drop text columns; we will add them back in
                        // as appropriate below.
                        curr_options.retain(|o| !matches!(o.name, PgConfigOptionName::TextColumns));

                        // Drop all text columns that are not currently referred to.
                        text_columns.retain(|column_qualified_reference| {
                            mz_ore::soft_assert_eq_or_log!(
                                column_qualified_reference.0.len(),
                                4,
                                "all TEXT COLUMNS values must be column-qualified references"
                            );
                            let mut table = column_qualified_reference.clone();
                            table.0.truncate(3);
                            curr_references.contains(&table)
                        });

                        // Merge the current text columns into the new text columns.
                        new_text_columns.extend(text_columns);

                        // If we have text columns, add them to the options.
                        if !new_text_columns.is_empty() {
                            new_text_columns.sort();
                            let new_text_columns = new_text_columns
                                .into_iter()
                                .map(WithOptionValue::UnresolvedItemName)
                                .collect();

                            curr_options.push(PgConfigOption {
                                name: PgConfigOptionName::TextColumns,
                                value: Some(WithOptionValue::Sequence(new_text_columns)),
                            });
                        }
                    }
                    CreateSourceConnection::MySql {
                        options: curr_options,
                        ..
                    } => {
                        let mz_sql::plan::MySqlConfigOptionExtracted {
                            mut text_columns,
                            mut exclude_columns,
                            ..
                        } = curr_options.clone().try_into()?;

                        // Drop both ignore and text columns; we will add them back in
                        // as appropriate below.
                        curr_options.retain(|o| {
                            !matches!(
                                o.name,
                                MySqlConfigOptionName::TextColumns
                                    | MySqlConfigOptionName::ExcludeColumns
                            )
                        });

                        // Drop all text / exclude columns that are not currently referred to.
                        let column_referenced =
                            |column_qualified_reference: &UnresolvedItemName| {
                                mz_ore::soft_assert_eq_or_log!(
                                    column_qualified_reference.0.len(),
                                    3,
                                    "all TEXT COLUMNS & EXCLUDE COLUMNS values must be column-qualified references"
                                );
                                let mut table = column_qualified_reference.clone();
                                table.0.truncate(2);
                                curr_references.contains(&table)
                            };
                        text_columns.retain(column_referenced);
                        exclude_columns.retain(column_referenced);

                        // Merge the current text / exclude columns into the new text / exclude columns.
                        new_text_columns.extend(text_columns);
                        new_exclude_columns.extend(exclude_columns);

                        // If we have text columns, add them to the options.
                        if !new_text_columns.is_empty() {
                            new_text_columns.sort();
                            let new_text_columns = new_text_columns
                                .into_iter()
                                .map(WithOptionValue::UnresolvedItemName)
                                .collect();

                            curr_options.push(MySqlConfigOption {
                                name: MySqlConfigOptionName::TextColumns,
                                value: Some(WithOptionValue::Sequence(new_text_columns)),
                            });
                        }
                        // If we have exclude columns, add them to the options.
                        if !new_exclude_columns.is_empty() {
                            new_exclude_columns.sort();
                            let new_exclude_columns = new_exclude_columns
                                .into_iter()
                                .map(WithOptionValue::UnresolvedItemName)
                                .collect();

                            curr_options.push(MySqlConfigOption {
                                name: MySqlConfigOptionName::ExcludeColumns,
                                value: Some(WithOptionValue::Sequence(new_exclude_columns)),
                            });
                        }
                    }
                    CreateSourceConnection::SqlServer {
                        options: curr_options,
                        ..
                    } => {
                        let mz_sql::plan::SqlServerConfigOptionExtracted {
                            mut text_columns,
                            mut exclude_columns,
                            ..
                        } = curr_options.clone().try_into()?;

                        // Drop both ignore and text columns; we will add them back in
                        // as appropriate below.
                        curr_options.retain(|o| {
                            !matches!(
                                o.name,
                                SqlServerConfigOptionName::TextColumns
                                    | SqlServerConfigOptionName::ExcludeColumns
                            )
                        });

                        // Drop all text / exclude columns that are not currently referred to.
                        let column_referenced =
                            |column_qualified_reference: &UnresolvedItemName| {
                                mz_ore::soft_assert_eq_or_log!(
                                    column_qualified_reference.0.len(),
                                    3,
                                    "all TEXT COLUMNS & EXCLUDE COLUMNS values must be column-qualified references"
                                );
                                let mut table = column_qualified_reference.clone();
                                table.0.truncate(2);
                                curr_references.contains(&table)
                            };
                        text_columns.retain(column_referenced);
                        exclude_columns.retain(column_referenced);

                        // Merge the current text / exclude columns into the new text / exclude columns.
                        new_text_columns.extend(text_columns);
                        new_exclude_columns.extend(exclude_columns);

                        // If we have text columns, add them to the options.
                        if !new_text_columns.is_empty() {
                            new_text_columns.sort();
                            let new_text_columns = new_text_columns
                                .into_iter()
                                .map(WithOptionValue::UnresolvedItemName)
                                .collect();

                            curr_options.push(SqlServerConfigOption {
                                name: SqlServerConfigOptionName::TextColumns,
                                value: Some(WithOptionValue::Sequence(new_text_columns)),
                            });
                        }
                        // If we have exclude columns, add them to the options.
                        if !new_exclude_columns.is_empty() {
                            new_exclude_columns.sort();
                            let new_exclude_columns = new_exclude_columns
                                .into_iter()
                                .map(WithOptionValue::UnresolvedItemName)
                                .collect();

                            curr_options.push(SqlServerConfigOption {
                                name: SqlServerConfigOptionName::ExcludeColumns,
                                value: Some(WithOptionValue::Sequence(new_exclude_columns)),
                            });
                        }
                    }
                    _ => return Err(purification_err()),
                };

                let mut catalog = self.catalog().for_system_session();
                catalog.mark_id_unresolvable_for_replanning(cur_entry.id());

                // Re-define our source in terms of the amended statement
                let plan = match mz_sql::plan::plan(
                    None,
                    &catalog,
                    Statement::CreateSource(create_source_stmt),
                    &Params::empty(),
                    &resolved_ids,
                )
                .map_err(|e| AdapterError::internal(ALTER_SOURCE, e))?
                {
                    Plan::CreateSource(plan) => plan,
                    _ => unreachable!("create source plan is only valid response"),
                };

                // Asserting that we've done the right thing with dependencies
                // here requires mocking out objects in the catalog, which is a
                // large task for an operation we have to cover in tests anyway.
                let source = Source::new(
                    plan,
                    cur_source.global_id,
                    resolved_ids,
                    cur_source.custom_logical_compaction_window,
                    cur_source.is_retained_metrics_object,
                );

                // Get new ingestion description for storage.
                let desc = match &source.data_source {
                    DataSourceDesc::Ingestion { desc, .. }
                    | DataSourceDesc::OldSyntaxIngestion { desc, .. } => {
                        desc.clone().into_inline_connection(self.catalog().state())
                    }
                    _ => unreachable!("already verified of type ingestion"),
                };

                self.controller
                    .storage
                    .check_alter_ingestion_source_desc(ingestion_id, &desc)
                    .map_err(|e| AdapterError::internal(ALTER_SOURCE, e))?;

                // Redefine source. This must be done before we create any new
                // subsources so that it has the right ingestion.
                let mut ops = vec![catalog::Op::UpdateItem {
                    id: item_id,
                    // Look this up again so we don't have to hold an immutable reference to the
                    // entry for so long.
                    name: self.catalog.get_entry(&item_id).name().clone(),
                    to_item: CatalogItem::Source(source),
                }];

                let CreateSourceInner {
                    ops: new_ops,
                    sources: _,
                    if_not_exists_ids,
                } = self.create_source_inner(session, subsources).await?;

                ops.extend(new_ops.into_iter());

                assert!(
                    if_not_exists_ids.is_empty(),
                    "IF NOT EXISTS not supported for ALTER SOURCE...ADD SUBSOURCES"
                );

                self.catalog_transact(Some(session), ops).await?;
            }
            plan::AlterSourceAction::RefreshReferences { references } => {
                self.catalog_transact(
                    Some(session),
                    vec![catalog::Op::UpdateSourceReferences {
                        source_id: item_id,
                        references: references.into(),
                    }],
                )
                .await?;
            }
        }

        Ok(ExecuteResponse::AlteredObject(ObjectType::Source))
    }

    #[instrument]
    pub(super) async fn sequence_alter_system_set(
        &mut self,
        session: &Session,
        plan::AlterSystemSetPlan { name, value }: plan::AlterSystemSetPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        self.is_user_allowed_to_alter_system(session, Some(&name))?;
        // We want to ensure that the network policy we're switching too actually exists.
        if NETWORK_POLICY.name.to_string().to_lowercase() == name.clone().to_lowercase() {
            self.validate_alter_system_network_policy(session, &value)?;
        }

        let op = match value {
            plan::VariableValue::Values(values) => catalog::Op::UpdateSystemConfiguration {
                name: name.clone(),
                value: OwnedVarInput::SqlSet(values),
            },
            plan::VariableValue::Default => {
                catalog::Op::ResetSystemConfiguration { name: name.clone() }
            }
        };
        self.catalog_transact(Some(session), vec![op]).await?;

        session.add_notice(AdapterNotice::VarDefaultUpdated {
            role: None,
            var_name: Some(name),
        });
        Ok(ExecuteResponse::AlteredSystemConfiguration)
    }

    #[instrument]
    pub(super) async fn sequence_alter_system_reset(
        &mut self,
        session: &Session,
        plan::AlterSystemResetPlan { name }: plan::AlterSystemResetPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        self.is_user_allowed_to_alter_system(session, Some(&name))?;
        let op = catalog::Op::ResetSystemConfiguration { name: name.clone() };
        self.catalog_transact(Some(session), vec![op]).await?;
        session.add_notice(AdapterNotice::VarDefaultUpdated {
            role: None,
            var_name: Some(name),
        });
        Ok(ExecuteResponse::AlteredSystemConfiguration)
    }

    #[instrument]
    pub(super) async fn sequence_alter_system_reset_all(
        &mut self,
        session: &Session,
        _: plan::AlterSystemResetAllPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        self.is_user_allowed_to_alter_system(session, None)?;
        let op = catalog::Op::ResetAllSystemConfiguration;
        self.catalog_transact(Some(session), vec![op]).await?;
        session.add_notice(AdapterNotice::VarDefaultUpdated {
            role: None,
            var_name: None,
        });
        Ok(ExecuteResponse::AlteredSystemConfiguration)
    }

    // TODO(jkosh44) Move this into rbac.rs once RBAC is always on.
    fn is_user_allowed_to_alter_system(
        &self,
        session: &Session,
        var_name: Option<&str>,
    ) -> Result<(), AdapterError> {
        match (session.user().kind(), var_name) {
            // Only internal superusers can reset all system variables.
            (UserKind::Superuser, None) if session.user().is_internal() => Ok(()),
            // Whether or not a variable can be modified depends if we're an internal superuser.
            (UserKind::Superuser, Some(name))
                if session.user().is_internal()
                    || self.catalog().system_config().user_modifiable(name) =>
            {
                // In lieu of plumbing the user to all system config functions, just check that
                // the var is visible.
                let var = self.catalog().system_config().get(name)?;
                var.visible(session.user(), self.catalog().system_config())?;
                Ok(())
            }
            // If we're not a superuser, but the variable is user modifiable, indicate they can use
            // session variables.
            (UserKind::Regular, Some(name))
                if self.catalog().system_config().user_modifiable(name) =>
            {
                Err(AdapterError::Unauthorized(
                    rbac::UnauthorizedError::Superuser {
                        action: format!("toggle the '{name}' system configuration parameter"),
                    },
                ))
            }
            _ => Err(AdapterError::Unauthorized(
                rbac::UnauthorizedError::MzSystem {
                    action: "alter system".into(),
                },
            )),
        }
    }

    fn validate_alter_system_network_policy(
        &self,
        session: &Session,
        policy_value: &plan::VariableValue,
    ) -> Result<(), AdapterError> {
        let policy_name = match &policy_value {
            // Make sure the compiled in default still exists.
            plan::VariableValue::Default => Some(NETWORK_POLICY.default_value().format()),
            plan::VariableValue::Values(values) if values.len() == 1 => {
                values.iter().next().cloned()
            }
            plan::VariableValue::Values(values) => {
                tracing::warn!(?values, "can't set multiple network policies at once");
                None
            }
        };
        let maybe_network_policy = policy_name
            .as_ref()
            .and_then(|name| self.catalog.get_network_policy_by_name(name));
        let Some(network_policy) = maybe_network_policy else {
            return Err(AdapterError::PlanError(plan::PlanError::VarError(
                VarError::InvalidParameterValue {
                    name: NETWORK_POLICY.name(),
                    invalid_values: vec![policy_name.unwrap_or_else(|| "<none>".to_string())],
                    reason: "no network policy with such name exists".to_string(),
                },
            )));
        };
        self.validate_alter_network_policy(session, &network_policy.rules)
    }

    /// Validates that a set of [`NetworkPolicyRule`]s is valid for the current [`Session`].
    ///
    /// This helps prevent users from modifying network policies in a way that would lock out their
    /// current connection.
    fn validate_alter_network_policy(
        &self,
        session: &Session,
        policy_rules: &Vec<NetworkPolicyRule>,
    ) -> Result<(), AdapterError> {
        // If the user is not an internal user attempt to protect them from
        // blocking themselves.
        if session.user().is_internal() {
            return Ok(());
        }
        if let Some(ip) = session.meta().client_ip() {
            validate_ip_with_policy_rules(ip, policy_rules)
                .map_err(|_| AdapterError::PlanError(plan::PlanError::NetworkPolicyLockoutError))?;
        } else {
            // Sessions without IPs are only temporarily constructed for default values
            // they should not be permitted here.
            return Err(AdapterError::NetworkPolicyDenied(
                NetworkPolicyError::MissingIp,
            ));
        }
        Ok(())
    }

    // Returns the name of the portal to execute.
    #[instrument]
    pub(super) fn sequence_execute(
        &self,
        session: &mut Session,
        plan: plan::ExecutePlan,
    ) -> Result<String, AdapterError> {
        // Verify the stmt is still valid.
        Self::verify_prepared_statement(self.catalog(), session, &plan.name)?;
        let ps = session
            .get_prepared_statement_unverified(&plan.name)
            .expect("known to exist");
        let stmt = ps.stmt().cloned();
        let desc = ps.desc().clone();
        let state_revision = ps.state_revision;
        let logging = Arc::clone(ps.logging());
        session.create_new_portal(stmt, logging, desc, plan.params, Vec::new(), state_revision)
    }

    #[instrument]
    pub(super) async fn sequence_grant_privileges(
        &mut self,
        session: &Session,
        plan::GrantPrivilegesPlan {
            update_privileges,
            grantees,
        }: plan::GrantPrivilegesPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        self.sequence_update_privileges(
            session,
            update_privileges,
            grantees,
            UpdatePrivilegeVariant::Grant,
        )
        .await
    }

    #[instrument]
    pub(super) async fn sequence_revoke_privileges(
        &mut self,
        session: &Session,
        plan::RevokePrivilegesPlan {
            update_privileges,
            revokees,
        }: plan::RevokePrivilegesPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        self.sequence_update_privileges(
            session,
            update_privileges,
            revokees,
            UpdatePrivilegeVariant::Revoke,
        )
        .await
    }

    #[instrument]
    async fn sequence_update_privileges(
        &mut self,
        session: &Session,
        update_privileges: Vec<UpdatePrivilege>,
        grantees: Vec<RoleId>,
        variant: UpdatePrivilegeVariant,
    ) -> Result<ExecuteResponse, AdapterError> {
        let mut ops = Vec::with_capacity(update_privileges.len() * grantees.len());
        let mut warnings = Vec::new();
        let catalog = self.catalog().for_session(session);

        for UpdatePrivilege {
            acl_mode,
            target_id,
            grantor,
        } in update_privileges
        {
            let actual_object_type = catalog.get_system_object_type(&target_id);
            // For all relations we allow all applicable table privileges, but send a warning if the
            // privilege isn't actually applicable to the object type.
            if actual_object_type.is_relation() {
                let applicable_privileges = rbac::all_object_privileges(actual_object_type);
                let non_applicable_privileges = acl_mode.difference(applicable_privileges);
                if !non_applicable_privileges.is_empty() {
                    let object_description =
                        ErrorMessageObjectDescription::from_sys_id(&target_id, &catalog);
                    warnings.push(AdapterNotice::NonApplicablePrivilegeTypes {
                        non_applicable_privileges,
                        object_description,
                    })
                }
            }

            if let SystemObjectId::Object(object_id) = &target_id {
                self.catalog()
                    .ensure_not_reserved_object(object_id, session.conn_id())?;
            }

            let privileges = self
                .catalog()
                .get_privileges(&target_id, session.conn_id())
                // Should be unreachable since the parser will refuse to parse grant/revoke
                // statements on objects without privileges.
                .ok_or(AdapterError::Unsupported(
                    "GRANTs/REVOKEs on an object type with no privileges",
                ))?;

            for grantee in &grantees {
                self.catalog().ensure_not_system_role(grantee)?;
                self.catalog().ensure_not_predefined_role(grantee)?;
                let existing_privilege = privileges
                    .get_acl_item(grantee, &grantor)
                    .map(Cow::Borrowed)
                    .unwrap_or_else(|| Cow::Owned(MzAclItem::empty(*grantee, grantor)));

                match variant {
                    UpdatePrivilegeVariant::Grant
                        if !existing_privilege.acl_mode.contains(acl_mode) =>
                    {
                        ops.push(catalog::Op::UpdatePrivilege {
                            target_id: target_id.clone(),
                            privilege: MzAclItem {
                                grantee: *grantee,
                                grantor,
                                acl_mode,
                            },
                            variant,
                        });
                    }
                    UpdatePrivilegeVariant::Revoke
                        if !existing_privilege
                            .acl_mode
                            .intersection(acl_mode)
                            .is_empty() =>
                    {
                        ops.push(catalog::Op::UpdatePrivilege {
                            target_id: target_id.clone(),
                            privilege: MzAclItem {
                                grantee: *grantee,
                                grantor,
                                acl_mode,
                            },
                            variant,
                        });
                    }
                    // no-op
                    _ => {}
                }
            }
        }

        if ops.is_empty() {
            session.add_notices(warnings);
            return Ok(variant.into());
        }

        let res = self
            .catalog_transact(Some(session), ops)
            .await
            .map(|_| match variant {
                UpdatePrivilegeVariant::Grant => ExecuteResponse::GrantedPrivilege,
                UpdatePrivilegeVariant::Revoke => ExecuteResponse::RevokedPrivilege,
            });
        if res.is_ok() {
            session.add_notices(warnings);
        }
        res
    }

    #[instrument]
    pub(super) async fn sequence_alter_default_privileges(
        &mut self,
        session: &Session,
        plan::AlterDefaultPrivilegesPlan {
            privilege_objects,
            privilege_acl_items,
            is_grant,
        }: plan::AlterDefaultPrivilegesPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let mut ops = Vec::with_capacity(privilege_objects.len() * privilege_acl_items.len());
        let variant = if is_grant {
            UpdatePrivilegeVariant::Grant
        } else {
            UpdatePrivilegeVariant::Revoke
        };
        for privilege_object in &privilege_objects {
            self.catalog()
                .ensure_not_system_role(&privilege_object.role_id)?;
            self.catalog()
                .ensure_not_predefined_role(&privilege_object.role_id)?;
            if let Some(database_id) = privilege_object.database_id {
                self.catalog()
                    .ensure_not_reserved_object(&database_id.into(), session.conn_id())?;
            }
            if let Some(schema_id) = privilege_object.schema_id {
                let database_spec: ResolvedDatabaseSpecifier = privilege_object.database_id.into();
                let schema_spec: SchemaSpecifier = schema_id.into();

                self.catalog().ensure_not_reserved_object(
                    &(database_spec, schema_spec).into(),
                    session.conn_id(),
                )?;
            }
            for privilege_acl_item in &privilege_acl_items {
                self.catalog()
                    .ensure_not_system_role(&privilege_acl_item.grantee)?;
                self.catalog()
                    .ensure_not_predefined_role(&privilege_acl_item.grantee)?;
                ops.push(catalog::Op::UpdateDefaultPrivilege {
                    privilege_object: privilege_object.clone(),
                    privilege_acl_item: privilege_acl_item.clone(),
                    variant,
                })
            }
        }

        self.catalog_transact(Some(session), ops).await?;
        Ok(ExecuteResponse::AlteredDefaultPrivileges)
    }

    #[instrument]
    pub(super) async fn sequence_grant_role(
        &mut self,
        session: &Session,
        plan::GrantRolePlan {
            role_ids,
            member_ids,
            grantor_id,
        }: plan::GrantRolePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let catalog = self.catalog();
        let mut ops = Vec::with_capacity(role_ids.len() * member_ids.len());
        for role_id in role_ids {
            for member_id in &member_ids {
                let member_membership: BTreeSet<_> =
                    catalog.get_role(member_id).membership().keys().collect();
                if member_membership.contains(&role_id) {
                    let role_name = catalog.get_role(&role_id).name().to_string();
                    let member_name = catalog.get_role(member_id).name().to_string();
                    // We need this check so we don't accidentally return a success on a reserved role.
                    catalog.ensure_not_reserved_role(member_id)?;
                    catalog.ensure_grantable_role(&role_id)?;
                    session.add_notice(AdapterNotice::RoleMembershipAlreadyExists {
                        role_name,
                        member_name,
                    });
                } else {
                    ops.push(catalog::Op::GrantRole {
                        role_id,
                        member_id: *member_id,
                        grantor_id,
                    });
                }
            }
        }

        if ops.is_empty() {
            return Ok(ExecuteResponse::GrantedRole);
        }

        self.catalog_transact(Some(session), ops)
            .await
            .map(|_| ExecuteResponse::GrantedRole)
    }

    #[instrument]
    pub(super) async fn sequence_revoke_role(
        &mut self,
        session: &Session,
        plan::RevokeRolePlan {
            role_ids,
            member_ids,
            grantor_id,
        }: plan::RevokeRolePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let catalog = self.catalog();
        let mut ops = Vec::with_capacity(role_ids.len() * member_ids.len());
        for role_id in role_ids {
            for member_id in &member_ids {
                let member_membership: BTreeSet<_> =
                    catalog.get_role(member_id).membership().keys().collect();
                if !member_membership.contains(&role_id) {
                    let role_name = catalog.get_role(&role_id).name().to_string();
                    let member_name = catalog.get_role(member_id).name().to_string();
                    // We need this check so we don't accidentally return a success on a reserved role.
                    catalog.ensure_not_reserved_role(member_id)?;
                    catalog.ensure_grantable_role(&role_id)?;
                    session.add_notice(AdapterNotice::RoleMembershipDoesNotExists {
                        role_name,
                        member_name,
                    });
                } else {
                    ops.push(catalog::Op::RevokeRole {
                        role_id,
                        member_id: *member_id,
                        grantor_id,
                    });
                }
            }
        }

        if ops.is_empty() {
            return Ok(ExecuteResponse::RevokedRole);
        }

        self.catalog_transact(Some(session), ops)
            .await
            .map(|_| ExecuteResponse::RevokedRole)
    }

    #[instrument]
    pub(super) async fn sequence_alter_owner(
        &mut self,
        session: &Session,
        plan::AlterOwnerPlan {
            id,
            object_type,
            new_owner,
        }: plan::AlterOwnerPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let mut ops = vec![catalog::Op::UpdateOwner {
            id: id.clone(),
            new_owner,
        }];

        match &id {
            ObjectId::Item(global_id) => {
                let entry = self.catalog().get_entry(global_id);

                // Cannot directly change the owner of an index.
                if entry.is_index() {
                    let name = self
                        .catalog()
                        .resolve_full_name(entry.name(), Some(session.conn_id()))
                        .to_string();
                    session.add_notice(AdapterNotice::AlterIndexOwner { name });
                    return Ok(ExecuteResponse::AlteredObject(object_type));
                }

                // Alter owner cascades down to dependent indexes.
                let dependent_index_ops = entry
                    .used_by()
                    .into_iter()
                    .filter(|id| self.catalog().get_entry(id).is_index())
                    .map(|id| catalog::Op::UpdateOwner {
                        id: ObjectId::Item(*id),
                        new_owner,
                    });
                ops.extend(dependent_index_ops);

                // Alter owner cascades down to progress collections.
                let dependent_subsources =
                    entry
                        .progress_id()
                        .into_iter()
                        .map(|item_id| catalog::Op::UpdateOwner {
                            id: ObjectId::Item(item_id),
                            new_owner,
                        });
                ops.extend(dependent_subsources);
            }
            ObjectId::Cluster(cluster_id) => {
                let cluster = self.catalog().get_cluster(*cluster_id);
                // Alter owner cascades down to cluster replicas.
                let managed_cluster_replica_ops =
                    cluster.replicas().map(|replica| catalog::Op::UpdateOwner {
                        id: ObjectId::ClusterReplica((cluster.id(), replica.replica_id())),
                        new_owner,
                    });
                ops.extend(managed_cluster_replica_ops);
            }
            _ => {}
        }

        self.catalog_transact(Some(session), ops)
            .await
            .map(|_| ExecuteResponse::AlteredObject(object_type))
    }

    #[instrument]
    pub(super) async fn sequence_reassign_owned(
        &mut self,
        session: &Session,
        plan::ReassignOwnedPlan {
            old_roles,
            new_role,
            reassign_ids,
        }: plan::ReassignOwnedPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        for role_id in old_roles.iter().chain(iter::once(&new_role)) {
            self.catalog().ensure_not_reserved_role(role_id)?;
        }

        let ops = reassign_ids
            .into_iter()
            .map(|id| catalog::Op::UpdateOwner {
                id,
                new_owner: new_role,
            })
            .collect();

        self.catalog_transact(Some(session), ops)
            .await
            .map(|_| ExecuteResponse::ReassignOwned)
    }

    #[instrument]
    pub(crate) async fn handle_deferred_statement(&mut self) {
        // It is possible Message::DeferredStatementReady was sent but then a session cancellation
        // was processed, removing the single element from deferred_statements, so it is expected
        // that this is sometimes empty.
        let Some(DeferredPlanStatement { ctx, ps }) = self.serialized_ddl.pop_front() else {
            return;
        };
        match ps {
            crate::coord::PlanStatement::Statement { stmt, params } => {
                self.handle_execute_inner(stmt, params, ctx).await;
            }
            crate::coord::PlanStatement::Plan { plan, resolved_ids } => {
                self.sequence_plan(ctx, plan, resolved_ids).await;
            }
        }
    }

    #[instrument]
    // TODO(parkmycar): Remove this once we have an actual implementation.
    #[allow(clippy::unused_async)]
    pub(super) async fn sequence_alter_table(
        &mut self,
        ctx: &mut ExecuteContext,
        plan: plan::AlterTablePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let plan::AlterTablePlan {
            relation_id,
            column_name,
            column_type,
            raw_sql_type,
        } = plan;

        // TODO(alter_table): Support allocating GlobalIds without a CatalogItemId.
        let id_ts = self.get_catalog_write_ts().await;
        let (_, new_global_id) = self.catalog.allocate_user_id(id_ts).await?;
        let ops = vec![catalog::Op::AlterAddColumn {
            id: relation_id,
            new_global_id,
            name: column_name,
            typ: column_type,
            sql: raw_sql_type,
        }];

        self.catalog_transact_with_context(None, Some(ctx), ops)
            .await?;

        Ok(ExecuteResponse::AlteredObject(ObjectType::Table))
    }

    #[instrument]
    pub(super) async fn sequence_alter_materialized_view_apply_replacement(
        &mut self,
        ctx: &ExecuteContext,
        plan: AlterMaterializedViewApplyReplacementPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let AlterMaterializedViewApplyReplacementPlan { id, replacement_id } = plan;

        // TODO(alter-mv): Wait until there is overlap between the old MV's write frontier and the
        // new MV's as-of, to ensure no times are skipped.

        let ops = vec![catalog::Op::AlterMaterializedViewApplyReplacement { id, replacement_id }];
        self.catalog_transact(Some(ctx.session()), ops).await?;

        Ok(ExecuteResponse::AlteredObject(ObjectType::MaterializedView))
    }

    pub(super) async fn statistics_oracle(
        &self,
        session: &Session,
        source_ids: &BTreeSet<GlobalId>,
        query_as_of: &Antichain<Timestamp>,
        is_oneshot: bool,
    ) -> Result<Box<dyn mz_transform::StatisticsOracle>, AdapterError> {
        super::statistics_oracle(
            session,
            source_ids,
            query_as_of,
            is_oneshot,
            self.catalog().system_config(),
            self.controller.storage_collections.as_ref(),
        )
        .await
    }
}

impl Coordinator {
    /// Process the metainfo from a newly created non-transient dataflow.
    async fn process_dataflow_metainfo(
        &mut self,
        df_meta: DataflowMetainfo,
        export_id: GlobalId,
        ctx: Option<&mut ExecuteContext>,
        notice_ids: Vec<GlobalId>,
    ) -> Option<BuiltinTableAppendNotify> {
        // Emit raw notices to the user.
        if let Some(ctx) = ctx {
            emit_optimizer_notices(&*self.catalog, ctx.session(), &df_meta.optimizer_notices);
        }

        // Create a metainfo with rendered notices.
        let df_meta = self
            .catalog()
            .render_notices(df_meta, notice_ids, Some(export_id));

        // Attend to optimization notice builtin tables and save the metainfo in the catalog's
        // in-memory state.
        if self.catalog().state().system_config().enable_mz_notices()
            && !df_meta.optimizer_notices.is_empty()
        {
            let mut builtin_table_updates = Vec::with_capacity(df_meta.optimizer_notices.len());
            self.catalog().state().pack_optimizer_notices(
                &mut builtin_table_updates,
                df_meta.optimizer_notices.iter(),
                Diff::ONE,
            );

            // Save the metainfo.
            self.catalog_mut().set_dataflow_metainfo(export_id, df_meta);

            Some(
                self.builtin_table_update()
                    .execute(builtin_table_updates)
                    .await
                    .0,
            )
        } else {
            // Save the metainfo.
            self.catalog_mut().set_dataflow_metainfo(export_id, df_meta);

            None
        }
    }
}
