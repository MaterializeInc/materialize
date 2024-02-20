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
use futures::future::BoxFuture;
use itertools::Itertools;
use maplit::{btreemap, btreeset};
use mz_adapter_types::compaction::CompactionWindow;
use mz_cloud_resources::VpcEndpointConfig;
use mz_controller_types::{ClusterId, ReplicaId};
use mz_expr::{CollectionPlan, MirScalarExpr, OptimizedMirRelationExpr, RowSetFinishing};

use mz_ore::collections::{CollectionExt, HashSet};
use mz_ore::task::spawn;
use mz_ore::tracing::OpenTelemetryContext;
use mz_ore::vec::VecExt;
use mz_ore::{soft_assert_or_log, task};
use mz_repr::adt::jsonb::Jsonb;
use mz_repr::adt::mz_acl_item::{MzAclItem, PrivilegeMap};
use mz_repr::explain::json::json_string;
use mz_repr::explain::{ExplainFormat, ExprHumanizer};
use mz_repr::role_id::RoleId;
use mz_repr::{Datum, Diff, GlobalId, Row, RowArena, Timestamp};
use mz_sql::catalog::{
    CatalogCluster, CatalogClusterReplica, CatalogDatabase, CatalogError,
    CatalogItem as SqlCatalogItem, CatalogItemType, CatalogRole, CatalogSchema, CatalogTypeDetails,
    ErrorMessageObjectDescription, ObjectType, RoleVars, SessionCatalog,
};
use mz_sql::names::{
    ObjectId, QualifiedItemName, ResolvedDatabaseSpecifier, ResolvedIds, ResolvedItemName,
    SchemaSpecifier, SystemObjectId,
};
// Import `plan` module, but only import select elements to avoid merge conflicts on use statements.
use mz_adapter_types::connection::ConnectionId;
use mz_catalog::memory::objects::{
    CatalogItem, Cluster, Connection, DataSourceDesc, Secret, Sink, Source, Table, Type,
};
use mz_ore::instrument;
use mz_sql::plan::{
    AlterConnectionAction, AlterConnectionPlan, ExplainSinkSchemaPlan, Explainee,
    ExplaineeStatement, MutationKind, Params, Plan, PlannedAlterRoleOption, PlannedRoleVariable,
    QueryWhen, SideEffectingFunc, UpdatePrivilege, VariableValue,
};
use mz_sql::session::metadata::SessionMetadata;
use mz_sql::session::user::UserKind;
use mz_sql::session::vars::{
    self, IsolationLevel, OwnedVarInput, SessionVars, Var, VarInput, SCHEMA_ALIAS,
    TRANSACTION_ISOLATION_VAR_NAME,
};
use mz_sql::{plan, rbac};
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{
    AlterSourceAddSubsourceOptionName, ConnectionOption, ConnectionOptionName,
    CreateSourceConnection, CreateSourceSubsource, DeferredItemName, PgConfigOption,
    PgConfigOptionName, ReferencedSubsources, Statement, TransactionMode, WithOptionValue,
};
use mz_ssh_util::keys::SshKeyPairSet;
use mz_storage_client::controller::{CollectionDescription, DataSource, DataSourceOther};
use mz_storage_types::connections::inline::IntoInlineConnection;
use mz_storage_types::controller::StorageError;
use mz_storage_types::AlterCompatible;
use mz_transform::notice::{OptimizerNoticeApi, OptimizerNoticeKind, RawOptimizerNotice};
use mz_transform::EmptyStatisticsOracle;
use timely::progress::Antichain;
use tokio::sync::{oneshot, OwnedMutexGuard};
use tracing::{warn, Instrument, Span};

use crate::catalog::{self, Catalog, ConnCatalog, UpdatePrivilegeVariant};
use crate::command::{ExecuteResponse, Response};
use crate::coord::appends::{Deferred, DeferredPlan, PendingWriteTxn};
use crate::coord::id_bundle::CollectionIdBundle;
use crate::coord::timestamp_selection::{TimestampDetermination, TimestampSource};
use crate::coord::{
    AlterConnectionValidationReady, Coordinator, CreateConnectionValidationReady, ExecuteContext,
    ExplainContext, Message, PeekStage, PeekStageValidate, PendingRead, PendingReadTxn, PendingTxn,
    PendingTxnResponse, PlanValidity, RealTimeRecencyContext, StageResult, Staged, TargetCluster,
};
use crate::error::AdapterError;
use crate::notice::{AdapterNotice, DroppedInUseIndex};
use crate::optimize::dataflows::{prep_scalar_expr, EvalTime, ExprPrepStyle};
use crate::optimize::{self, Optimize};
use crate::session::{
    EndTransactionAction, RequireLinearization, Session, TransactionOps, TransactionStatus, WriteOp,
};
use crate::util::{viewable_variables, ClientTransmitter, ResultExt};
use crate::{
    guard_write_critical_section, PeekResponseUnary, TimelineContext, TimestampExplanation,
};

mod create_index;
mod create_materialized_view;
mod create_view;
mod peek;
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
    sources: Vec<(GlobalId, Source)>,
    if_not_exists_ids: BTreeMap<GlobalId, QualifiedItemName>,
}

impl Coordinator {
    /// Sequences the next staged of a [Staged] plan. This is designed for use with plans that
    /// execute both on and off of the coordinator thread. Stages can either produce another stage
    /// to execute or a final response. An explicit [Span] is passed to allow for convenient
    /// tracing.
    pub(crate) async fn sequence_staged<S: Staged + 'static>(
        &mut self,
        mut ctx: ExecuteContext,
        parent_span: Span,
        mut stage: S,
    ) {
        return_if_err!(stage.validity().check(self.catalog()), ctx);
        let next = stage
            .stage(self, &mut ctx)
            .instrument(parent_span.clone())
            .await;
        let stage = return_if_err!(next, ctx);
        let internal_cmd_tx = self.internal_cmd_tx.clone();
        match stage {
            StageResult::Handle(handle) => {
                spawn(|| "sequence_staged", async move {
                    let next = match handle.await {
                        Ok(next) => return_if_err!(next, ctx),
                        Err(err) => {
                            tracing::error!("sequence_staged join error {err}");
                            ctx.retire(Err(AdapterError::Internal(
                                "sequence_staged join error".into(),
                            )));
                            return;
                        }
                    };
                    let _ = internal_cmd_tx.send(next.message(ctx, parent_span));
                });
            }
            StageResult::Response(resp) => {
                ctx.retire(Ok(resp));
            }
        }
    }

    async fn create_source_inner(
        &mut self,
        session: &Session,
        plans: Vec<plan::CreateSourcePlans>,
    ) -> Result<CreateSourceInner, AdapterError> {
        let mut ops = vec![];
        let mut sources = vec![];

        let if_not_exists_ids = plans
            .iter()
            .filter_map(
                |plan::CreateSourcePlans {
                     source_id,
                     plan,
                     resolved_ids: _,
                 }| {
                    if plan.if_not_exists {
                        Some((*source_id, plan.name.clone()))
                    } else {
                        None
                    }
                },
            )
            .collect::<BTreeMap<_, _>>();

        for plan::CreateSourcePlans {
            source_id,
            mut plan,
            resolved_ids,
        } in plans
        {
            let name = plan.name.clone();
            if matches!(
                plan.source.data_source,
                mz_sql::plan::DataSourceDesc::Ingestion(_)
                    | mz_sql::plan::DataSourceDesc::Webhook { .. }
            ) {
                if let Some(cluster) = self.catalog().try_get_cluster(
                    plan.in_cluster
                        .expect("ingestion, webhook sources must specify cluster"),
                ) {
                    mz_ore::soft_assert_or_log!(
                        cluster.replica_ids().len() <= 1,
                        "cannot create source in cluster {}; has >1 replicas",
                        cluster.id()
                    );
                }
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

            let source = Source::new(source_id, plan, resolved_ids, None, false);
            ops.push(catalog::Op::CreateItem {
                id: source_id,
                name,
                item: CatalogItem::Source(source.clone()),
                owner_id: *session.current_role_id(),
            });
            sources.push((source_id, source));
        }

        Ok(CreateSourceInner {
            ops,
            sources,
            if_not_exists_ids,
        })
    }

    #[instrument]
    pub(super) async fn sequence_create_source(
        &mut self,
        session: &mut Session,
        plans: Vec<plan::CreateSourcePlans>,
    ) -> Result<ExecuteResponse, AdapterError> {
        let CreateSourceInner {
            ops,
            sources,
            if_not_exists_ids,
        } = self.create_source_inner(session, plans).await?;

        let transact_result = self
            .catalog_transact_with_side_effects(Some(session), ops, |coord| async {
                // Buffer an ingestions' read policies in case their subsources
                // need to reference it before it's durably recorded.
                let mut ingestion_read_policies: BTreeMap<_, _> = BTreeMap::new();
                let mut read_policies: BTreeMap<Option<CompactionWindow>, Vec<GlobalId>> =
                    BTreeMap::new();

                for (source_id, mut source) in sources {
                    let source_status_collection_id =
                        Some(coord.catalog().resolve_builtin_storage_collection(
                            &mz_catalog::builtin::MZ_SOURCE_STATUS_HISTORY,
                        ));

                    let (data_source, status_collection_id) = match source.data_source {
                        DataSourceDesc::Ingestion(ingestion) => {
                            let ingestion =
                                ingestion.into_inline_connection(coord.catalog().state());

                            ingestion_read_policies
                                .insert(source_id, source.custom_logical_compaction_window);

                            (
                                DataSource::Ingestion(ingestion),
                                source_status_collection_id,
                            )
                        }
                        DataSourceDesc::SourceExport { id, output_index } => {
                            // Propagate source's compaction window if this
                            // subsource does not have its own value specified.
                            if source.custom_logical_compaction_window.is_none() {
                                // Defined as part of the initial set of subsources.
                                let c = match ingestion_read_policies.get(&id) {
                                    Some(c) => c,
                                    None => {
                                        // Added to an existing source.
                                        let source = coord.catalog().get_entry(&id);
                                        match &source.item {
                                            CatalogItem::Source(source) => {
                                                &source.custom_logical_compaction_window
                                            }
                                            _ => unreachable!(),
                                        }
                                    }
                                };
                                source.custom_logical_compaction_window = c.clone();
                            }

                            (
                                DataSource::SourceExport { id, output_index },
                                source_status_collection_id,
                            )
                        }
                        // Subsources use source statuses.
                        DataSourceDesc::Source => (
                            DataSource::Other(DataSourceOther::Source),
                            source_status_collection_id,
                        ),
                        DataSourceDesc::Progress => (DataSource::Progress, None),
                        DataSourceDesc::Webhook { .. } => {
                            if let Some(url) =
                                coord.catalog().state().try_get_webhook_url(&source_id)
                            {
                                session.add_notice(AdapterNotice::WebhookSourceCreated { url })
                            }

                            (DataSource::Webhook, None)
                        }
                        DataSourceDesc::Introspection(_) => {
                            unreachable!("cannot create sources with introspection data sources")
                        }
                    };

                    let storage_metadata = coord.catalog.state().storage_metadata();

                    coord
                        .controller
                        .storage
                        .create_collections(
                            storage_metadata,
                            None,
                            vec![(
                                source_id,
                                CollectionDescription {
                                    desc: source.desc.clone(),
                                    data_source,
                                    since: None,
                                    status_collection_id,
                                },
                            )],
                        )
                        .await
                        .unwrap_or_terminate("cannot fail to create collections");

                    read_policies
                        .entry(source.custom_logical_compaction_window.clone())
                        .or_default()
                        .push(source_id);
                }

                // It is _very_ important that we only initialize read policies
                // after we have created all the sources/collections. Some of
                // the sources created in this collection might have
                // dependencies on other sources, so the controller must get a
                // chance to install read holds before we set a policy that
                // might make the since advance.
                //
                // One instance of this is the remap shard: it presents as a
                // SUBSOURCE, and all other SUBSOURCES of a SOURCE will depend
                // on it. Both subsources and sources will show up as a `Source`
                // in the above.
                for (compaction_window, storage_policies) in read_policies {
                    coord
                        .initialize_storage_read_policies(
                            storage_policies,
                            compaction_window.unwrap_or(CompactionWindow::Default),
                        )
                        .await;
                }
            })
            .await;

        match transact_result {
            Ok(()) => Ok(ExecuteResponse::CreatedSource),
            Err(AdapterError::Catalog(mz_catalog::memory::error::Error {
                kind:
                    mz_catalog::memory::error::ErrorKind::Sql(CatalogError::ItemAlreadyExists(id, _)),
            })) if if_not_exists_ids.contains_key(&id) => {
                session.add_notice(AdapterNotice::ObjectAlreadyExists {
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
        mut plan: plan::CreateConnectionPlan,
        resolved_ids: ResolvedIds,
    ) {
        let connection_gid = match self.catalog_mut().allocate_user_id().await {
            Ok(gid) => gid,
            Err(err) => return ctx.retire(Err(err.into())),
        };

        match plan.connection.connection {
            mz_storage_types::connections::Connection::Ssh(ref mut ssh) => {
                let key_set = match SshKeyPairSet::new() {
                    Ok(key) => key,
                    Err(err) => return ctx.retire(Err(err.into())),
                };
                let secret = key_set.to_bytes();
                match self
                    .secrets_controller
                    .ensure(connection_gid, &secret)
                    .await
                {
                    Ok(()) => (),
                    Err(err) => return ctx.retire(Err(err.into())),
                }
                ssh.public_keys = Some(key_set.public_keys());
            }
            _ => {}
        }

        if plan.validate {
            let internal_cmd_tx = self.internal_cmd_tx.clone();
            let transient_revision = self.catalog().transient_revision();
            let conn_id = ctx.session().conn_id().clone();
            let otel_ctx = OpenTelemetryContext::obtain();
            let role_metadata = ctx.session().role_metadata().clone();

            let connection = plan
                .connection
                .connection
                .clone()
                .into_inline_connection(self.catalog().state());

            let current_storage_parameters = self.controller.storage.config().clone();
            task::spawn(|| format!("validate_connection:{conn_id}"), async move {
                let result = match connection
                    .validate(connection_gid, &current_storage_parameters)
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
                        connection_gid,
                        plan_validity: PlanValidity {
                            transient_revision,
                            dependency_ids: resolved_ids.0,
                            cluster_id: None,
                            replica_id: None,
                            role_metadata,
                        },
                        otel_ctx,
                    },
                ));
                if let Err(e) = result {
                    tracing::warn!("internal_cmd_rx dropped before we could send: {:?}", e);
                }
            });
        } else {
            let result = self
                .sequence_create_connection_stage_finish(
                    ctx.session_mut(),
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
        session: &mut Session,
        connection_gid: GlobalId,
        plan: plan::CreateConnectionPlan,
        resolved_ids: ResolvedIds,
    ) -> Result<ExecuteResponse, AdapterError> {
        let ops = vec![catalog::Op::CreateItem {
            id: connection_gid,
            name: plan.name.clone(),
            item: CatalogItem::Connection(Connection {
                create_sql: plan.connection.create_sql,
                connection: plan.connection.connection.clone(),
                resolved_ids,
            }),
            owner_id: *session.current_role_id(),
        }];

        let transact_result = self
            .catalog_transact_with_side_effects(Some(session), ops, |coord| async {
                match plan.connection.connection {
                    mz_storage_types::connections::Connection::AwsPrivatelink(ref privatelink) => {
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
                            .ensure_vpc_endpoint(connection_gid, spec)
                            .await
                        {
                            tracing::warn!(?err, "failed to ensure vpc endpoint!");
                        }
                    }
                    _ => {}
                }
            })
            .await;

        match transact_result {
            Ok(_) => Ok(ExecuteResponse::CreatedConnection),
            Err(AdapterError::Catalog(mz_catalog::memory::error::Error {
                kind:
                    mz_catalog::memory::error::ErrorKind::Sql(CatalogError::ItemAlreadyExists(_, _)),
            })) if plan.if_not_exists => Ok(ExecuteResponse::CreatedConnection),
            Err(err) => Err(err),
        }
    }

    #[instrument]
    pub(super) async fn sequence_create_database(
        &mut self,
        session: &mut Session,
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
        session: &mut Session,
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

    #[instrument]
    pub(super) async fn sequence_create_role(
        &mut self,
        conn_id: Option<&ConnectionId>,
        plan::CreateRolePlan { name, attributes }: plan::CreateRolePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let op = catalog::Op::CreateRole { name, attributes };
        self.catalog_transact_conn(conn_id, vec![op])
            .await
            .map(|_| ExecuteResponse::CreatedRole)
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
        let table_id = self.catalog_mut().allocate_user_id().await?;
        let table = Table {
            create_sql: Some(table.create_sql),
            desc: table.desc,
            defaults: table.defaults,
            conn_id: conn_id.cloned(),
            resolved_ids,
            custom_logical_compaction_window: table.compaction_window,
            is_retained_metrics_object: false,
        };
        let ops = vec![catalog::Op::CreateItem {
            id: table_id,
            name: name.clone(),
            item: CatalogItem::Table(table.clone()),
            owner_id: *ctx.session().current_role_id(),
        }];

        let catalog_result = self
            .catalog_transact_with_side_effects(Some(ctx.session()), ops, |coord| async {
                // Determine the initial validity for the table.
                let register_ts = coord.get_local_write_ts().await.timestamp;
                if let Some(id) = ctx.extra().contents() {
                    coord.set_statement_execution_timestamp(id, register_ts);
                }

                let collection_desc = CollectionDescription::from_desc(
                    table.desc.clone(),
                    DataSourceOther::TableWrites,
                );
                let storage_metadata = coord.catalog.state().storage_metadata();
                coord
                    .controller
                    .storage
                    .create_collections(
                        storage_metadata,
                        Some(register_ts),
                        vec![(table_id, collection_desc)],
                    )
                    .await
                    .unwrap_or_terminate("cannot fail to create collections");
                coord.apply_local_write(register_ts).await;

                coord
                    .initialize_storage_read_policies(
                        vec![table_id],
                        table
                            .custom_logical_compaction_window
                            .unwrap_or(CompactionWindow::Default),
                    )
                    .await;
            })
            .await;

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
    pub(super) async fn sequence_create_secret(
        &mut self,
        session: &mut Session,
        plan: plan::CreateSecretPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let plan::CreateSecretPlan {
            name,
            mut secret,
            if_not_exists,
        } = plan;

        let payload = self.extract_secret(session, &mut secret.secret_as)?;

        let id = self.catalog_mut().allocate_user_id().await?;
        let secret = Secret {
            create_sql: secret.create_sql,
        };

        self.secrets_controller.ensure(id, &payload).await?;

        let ops = vec![catalog::Op::CreateItem {
            id,
            name: name.clone(),
            item: CatalogItem::Secret(secret.clone()),
            owner_id: *session.current_role_id(),
        }];

        match self.catalog_transact(Some(session), ops).await {
            Ok(()) => Ok(ExecuteResponse::CreatedSecret),
            Err(AdapterError::Catalog(mz_catalog::memory::error::Error {
                kind:
                    mz_catalog::memory::error::ErrorKind::Sql(CatalogError::ItemAlreadyExists(_, _)),
            })) if if_not_exists => {
                session.add_notice(AdapterNotice::ObjectAlreadyExists {
                    name: name.item,
                    ty: "secret",
                });
                Ok(ExecuteResponse::CreatedSecret)
            }
            Err(err) => {
                if let Err(e) = self.secrets_controller.delete(id).await {
                    warn!(
                        "Dropping newly created secrets has encountered an error: {}",
                        e
                    );
                }
                Err(err)
            }
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
        let id = return_if_err!(self.catalog_mut().allocate_user_id().await, ctx);

        if let Some(cluster) = self.catalog().try_get_cluster(in_cluster) {
            mz_ore::soft_assert_or_log!(
                cluster.replica_ids().len() <= 1,
                "cannot create sink in cluster {}; has >1 replicas",
                cluster.id()
            );
        }

        let catalog_sink = Sink {
            create_sql: sink.create_sql,
            from: sink.from,
            connection: sink.connection,
            envelope: sink.envelope,
            with_snapshot,
            resolved_ids,
            cluster_id: in_cluster,
        };

        let ops = vec![catalog::Op::CreateItem {
            id,
            name: name.clone(),
            item: CatalogItem::Sink(catalog_sink.clone()),
            owner_id: *ctx.session().current_role_id(),
        }];

        let from = self.catalog().get_entry(&catalog_sink.from);
        if let Err(e) = self
            .controller
            .storage
            .collection(sink.from)
            .map_err(|e| match e {
                StorageError::IdentifierMissing(_) => AdapterError::Unstructured(anyhow!(
                    "{} is a {}, which cannot be exported as a sink",
                    from.name().item.clone(),
                    from.item().typ().to_string(),
                )),
                e => AdapterError::Storage(e),
            })
        {
            ctx.retire(Err(e));
            return;
        }

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

        self.create_storage_export(id, &catalog_sink)
            .await
            .unwrap_or_terminate("cannot fail to create exports");

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
                .any(|id| id.is_system() && self.catalog().get_entry(id).is_relation())
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
        let typ = Type {
            create_sql: Some(plan.typ.create_sql),
            desc: plan.typ.inner.desc(&self.catalog().for_session(session))?,
            details: CatalogTypeDetails {
                array_id: None,
                typ: plan.typ.inner,
                pg_metadata: None,
            },
            resolved_ids,
        };
        let id = self.catalog_mut().allocate_user_id().await?;
        let op = catalog::Op::CreateItem {
            id,
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
        session: &Session,
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
                    &self.catalog().for_session(session),
                )
                .to_string();
                objects.push(object_info);
            }
        }

        if !objects.is_empty() {
            session.add_notice(AdapterNotice::CascadeDroppedObject { objects });
        }

        let DropOps {
            ops,
            dropped_active_db,
            dropped_active_cluster,
            dropped_in_use_indexes,
        } = self.sequence_drop_common(session, drop_ids)?;

        self.catalog_transact(Some(session), ops).await?;

        fail::fail_point!("after_sequencer_drop_replica");

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
                .drain_filter_swapping(|(_, privilege)| {
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
                            .collection_reverse_dependencies(index.cluster_id, *id)
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
                                !matches!(dependant_id, GlobalId::Transient(..)) &&
                                // If the dependent object is also being dropped, then there is no
                                // problem, so we don't want a notice.
                                !ids_set.contains(&ObjectId::Item(**dependant_id))
                            })
                            .flat_map(|dependant_id| {
                                // If we are not able to find a name for this ID it probably means
                                // we have already dropped the compute collection, in which case we
                                // can ignore it.
                                humanizer.humanize_id(*dependant_id)
                            })
                            .collect_vec();
                        if !dependants.is_empty() {
                            dropped_in_use_indexes.push(DroppedInUseIndex {
                                index_name: humanizer.humanize_id(*id).unwrap_or(id.to_string()),
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
            .chain(ids.into_iter().map(catalog::Op::DropObject))
            .collect();

        Ok(DropOps {
            ops,
            dropped_active_db,
            dropped_active_cluster,
            dropped_in_use_indexes,
        })
    }

    pub(super) fn sequence_explain_schema(
        &mut self,
        ExplainSinkSchemaPlan { json_schema, .. }: ExplainSinkSchemaPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let json_value: serde_json::Value = serde_json::from_str(&json_schema).map_err(|e| {
            AdapterError::Explain(mz_repr::explain::ExplainError::SerdeJsonError(e))
        })?;

        let json_string = json_string(&json_value);
        Ok(Self::send_immediate_rows(vec![Row::pack_slice(&[
            Datum::String(&json_string),
        ])]))
    }

    pub(super) fn sequence_show_all_variables(
        &mut self,
        session: &Session,
    ) -> Result<ExecuteResponse, AdapterError> {
        let mut rows = viewable_variables(self.catalog().state(), session)
            .map(|v| (v.name(), v.value(), v.description()))
            .collect::<Vec<_>>();
        rows.sort_by_cached_key(|(name, _, _)| name.to_lowercase());
        Ok(Self::send_immediate_rows(
            rows.into_iter()
                .map(|(name, val, desc)| {
                    Row::pack_slice(&[
                        Datum::String(name),
                        Datum::String(&val),
                        Datum::String(desc),
                    ])
                })
                .collect(),
        ))
    }

    pub(super) fn sequence_show_variable(
        &mut self,
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
                    Ok(Self::send_immediate_rows(vec![row]))
                }
                None => {
                    session.add_notice(AdapterNotice::NoResolvableSearchPathSchema {
                        search_path: session
                            .vars()
                            .search_path()
                            .into_iter()
                            .map(|schema| schema.to_string())
                            .collect(),
                    });
                    Ok(Self::send_immediate_rows(vec![Row::pack_slice(&[
                        Datum::Null,
                    ])]))
                }
            };
        }

        let variable = session
            .vars()
            .get(Some(self.catalog().system_config()), &plan.name)
            .or_else(|_| self.catalog().system_config().get(&plan.name))?;

        // In lieu of plumbing the user to all system config functions, just check that the var is
        // visible.
        variable.visible(session.user(), Some(self.catalog().system_config()))?;

        let row = Row::pack_slice(&[Datum::String(&variable.value())]);
        if variable.name() == vars::DATABASE.name()
            && matches!(
                self.catalog().resolve_database(&variable.value()),
                Err(CatalogError::UnknownDatabase(_))
            )
        {
            let name = variable.value();
            session.add_notice(AdapterNotice::DatabaseDoesNotExist { name });
        } else if variable.name() == vars::CLUSTER.name()
            && matches!(
                self.catalog().resolve_cluster(&variable.value()),
                Err(CatalogError::UnknownCluster(_))
            )
        {
            let name = variable.value();
            session.add_notice(AdapterNotice::ClusterDoesNotExist { name });
        }
        Ok(Self::send_immediate_rows(vec![row]))
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
        Ok(Self::send_immediate_rows(vec![jsonb.into_row()]))
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
                    Some(self.catalog().system_config()),
                    &name,
                    VarInput::SqlSet(&values),
                    local,
                )?;

                // Database or cluster value does not correspond to a catalog item.
                if name.as_str() == vars::DATABASE.name()
                    && matches!(
                        self.catalog().resolve_database(vars.database()),
                        Err(CatalogError::UnknownDatabase(_))
                    )
                {
                    let name = vars.database().to_string();
                    session.add_notice(AdapterNotice::DatabaseDoesNotExist { name });
                } else if name.as_str() == vars::CLUSTER.name()
                    && matches!(
                        self.catalog().resolve_cluster(vars.cluster()),
                        Err(CatalogError::UnknownCluster(_))
                    )
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
            None => vars.reset(Some(self.catalog().system_config()), &name, local)?,
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
            .reset(Some(self.catalog().system_config()), &name, false)?;
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
                    return Err(AdapterError::Unsupported("SET TRANSACTION <access-mode>"))
                }
                TransactionMode::IsolationLevel(isolation_level) => {
                    self.validate_set_isolation_level(session)?;

                    session.vars_mut().set(
                        Some(self.catalog().system_config()),
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

        let result = self
            .sequence_end_transaction_inner(ctx.session_mut(), action)
            .await;

        let (response, action) = match result {
            Ok((Some(TransactionOps::Writes(writes)), _)) if writes.is_empty() => {
                (response, action)
            }
            Ok((Some(TransactionOps::Writes(writes)), write_lock_guard)) => {
                self.submit_write(PendingWriteTxn::User {
                    span: Span::current(),
                    writes,
                    write_lock_guard,
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
        session: &mut Session,
        action: EndTransactionAction,
    ) -> Result<
        (
            Option<TransactionOps<Timestamp>>,
            Option<OwnedMutexGuard<()>>,
        ),
        AdapterError,
    > {
        let txn = self.clear_transaction(session).await;

        if let EndTransactionAction::Commit = action {
            if let (Some(mut ops), write_lock_guard) = txn.into_ops_and_lock_guard() {
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
                        revision,
                    } => {
                        // Make sure our catalog hasn't changed.
                        if *revision != self.catalog().transient_revision() {
                            return Err(AdapterError::DDLTransactionRace);
                        }
                        // Commit all of our queued ops.
                        self.catalog_transact(Some(session), std::mem::take(ops))
                            .await?;
                    }
                    _ => (),
                }
                return Ok((Some(ops), write_lock_guard));
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
                ctx.retire(Ok(Self::send_immediate_rows(vec![Row::pack_slice(&[res])])));
            }
        }
    }

    /// Checks to see if the session needs a real time recency timestamp and if so returns
    /// a future that will return the timestamp.
    pub(super) fn recent_timestamp(
        &self,
        session: &Session,
        source_ids: impl Iterator<Item = GlobalId>,
    ) -> Option<BoxFuture<'static, Timestamp>> {
        // Ideally this logic belongs inside of
        // `mz-adapter::coord::timestamp_selection::determine_timestamp`. However, including the
        // logic in there would make it extremely difficult and inconvenient to pull the waiting off
        // of the main coord thread.
        if session.vars().real_time_recency()
            && session.vars().transaction_isolation() == &IsolationLevel::StrictSerializable
            && !session.contains_read_timestamp()
        {
            Some(self.controller.recent_timestamp(source_ids))
        } else {
            None
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
                self.execute_peek_stage(
                    ctx,
                    OpenTelemetryContext::obtain(),
                    PeekStage::Validate(PeekStageValidate {
                        plan,
                        target_cluster,
                        copy_to_ctx: None,
                        explain_ctx: ExplainContext::Pushdown,
                    }),
                )
                .await;
            }
            _ => {
                ctx.retire(Err(AdapterError::Unsupported(
                    "EXPLAIN FILTER PUSHDOWN queries for this explainee type",
                )));
            }
        };
    }

    #[instrument]
    pub async fn sequence_explain_timestamp(
        &mut self,
        mut ctx: ExecuteContext,
        plan: plan::ExplainTimestampPlan,
        target_cluster: TargetCluster,
    ) {
        let when = plan.when.clone();
        let (format, source_ids, optimized_plan, cluster_id, id_bundle) = return_if_err!(
            self.sequence_explain_timestamp_begin_inner(ctx.session(), plan, target_cluster),
            ctx
        );
        match self.recent_timestamp(ctx.session(), source_ids.iter().cloned()) {
            Some(fut) => {
                let validity = PlanValidity {
                    transient_revision: self.catalog().transient_revision(),
                    dependency_ids: source_ids,
                    cluster_id: Some(cluster_id),
                    replica_id: None,
                    role_metadata: ctx.session().role_metadata().clone(),
                };
                let internal_cmd_tx = self.internal_cmd_tx.clone();
                let conn_id = ctx.session().conn_id().clone();
                self.pending_real_time_recency_timestamp.insert(
                    conn_id.clone(),
                    RealTimeRecencyContext::ExplainTimestamp {
                        ctx,
                        format,
                        cluster_id,
                        optimized_plan,
                        when,
                        id_bundle,
                    },
                );
                task::spawn(|| "real_time_recency_explain_timestamp", async move {
                    let real_time_recency_ts = fut.await;
                    // It is not an error for these results to be ready after `internal_cmd_rx` has been dropped.
                    let result = internal_cmd_tx.send(Message::RealTimeRecencyTimestamp {
                        conn_id,
                        real_time_recency_ts,
                        validity,
                    });
                    if let Err(e) = result {
                        warn!("internal_cmd_rx dropped before we could send: {:?}", e);
                    }
                });
            }
            None => {
                let result = self
                    .sequence_explain_timestamp_finish_inner(
                        ctx.session_mut(),
                        format,
                        cluster_id,
                        optimized_plan,
                        id_bundle,
                        when,
                        None,
                    )
                    .await;
                ctx.retire(result);
            }
        }
    }

    fn sequence_explain_timestamp_begin_inner(
        &mut self,
        session: &Session,
        plan: plan::ExplainTimestampPlan,
        target_cluster: TargetCluster,
    ) -> Result<
        (
            ExplainFormat,
            BTreeSet<GlobalId>,
            OptimizedMirRelationExpr,
            ClusterId,
            CollectionIdBundle,
        ),
        AdapterError,
    > {
        let plan::ExplainTimestampPlan {
            format,
            raw_plan,
            when: _,
        } = plan;

        // Collect optimizer parameters.
        let optimizer_config = optimize::OptimizerConfig::from(self.catalog().system_config());

        // Build an optimizer for this VIEW.
        let mut optimizer = optimize::view::Optimizer::new(optimizer_config, None);

        // HIR ⇒ MIR lowering and MIR ⇒ MIR optimization (local)
        let optimized_plan = optimizer.optimize(raw_plan)?;

        let source_ids = optimized_plan.depends_on();
        let cluster = self
            .catalog()
            .resolve_target_cluster(target_cluster, session)?;
        let id_bundle = self
            .index_oracle(cluster.id)
            .sufficient_collections(&source_ids);
        Ok((format, source_ids, optimized_plan, cluster.id(), id_bundle))
    }

    pub(crate) fn explain_timestamp(
        &self,
        session: &Session,
        cluster_id: ClusterId,
        id_bundle: &CollectionIdBundle,
        determination: TimestampDetermination<mz_repr::Timestamp>,
    ) -> TimestampExplanation<mz_repr::Timestamp> {
        let mut sources = Vec::new();
        {
            for id in id_bundle.storage_ids.iter() {
                let state = self
                    .controller
                    .storage
                    .collection(*id)
                    .expect("id does not exist");
                let name = self
                    .catalog()
                    .try_get_entry(id)
                    .map(|item| item.name())
                    .map(|name| {
                        self.catalog()
                            .resolve_full_name(name, Some(session.conn_id()))
                            .to_string()
                    })
                    .unwrap_or_else(|| id.to_string());
                sources.push(TimestampSource {
                    name: format!("{name} ({id}, storage)"),
                    read_frontier: state.implied_capability.elements().to_vec(),
                    write_frontier: state.write_frontier.elements().to_vec(),
                });
            }
        }
        {
            if let Some(compute_ids) = id_bundle.compute_ids.get(&cluster_id) {
                let catalog = self.catalog();
                for id in compute_ids {
                    let state = self
                        .controller
                        .compute
                        .collection(cluster_id, *id)
                        .expect("id does not exist");
                    let name = catalog
                        .try_get_entry(id)
                        .map(|item| item.name())
                        .map(|name| {
                            catalog
                                .resolve_full_name(name, Some(session.conn_id()))
                                .to_string()
                        })
                        .unwrap_or_else(|| id.to_string());
                    sources.push(TimestampSource {
                        name: format!("{name} ({id}, compute)"),
                        read_frontier: state.read_capability().elements().to_vec(),
                        write_frontier: state.write_frontier().to_vec(),
                    });
                }
            }
        }
        let respond_immediately = determination.respond_immediately();
        TimestampExplanation {
            determination,
            sources,
            session_wall_time: session.pcx().wall_time,
            respond_immediately,
        }
    }

    #[instrument]
    pub(super) async fn sequence_explain_timestamp_finish_inner(
        &mut self,
        session: &mut Session,
        format: ExplainFormat,
        cluster_id: ClusterId,
        source: OptimizedMirRelationExpr,
        id_bundle: CollectionIdBundle,
        when: QueryWhen,
        real_time_recency_ts: Option<Timestamp>,
    ) -> Result<ExecuteResponse, AdapterError> {
        let is_json = match format {
            ExplainFormat::Text => false,
            ExplainFormat::Json => true,
            ExplainFormat::Dot => {
                return Err(AdapterError::Unsupported("EXPLAIN TIMESTAMP AS DOT"));
            }
        };
        let source_ids = source.depends_on();
        let mut timeline_context = self.validate_timeline_context(source_ids.clone())?;
        if matches!(timeline_context, TimelineContext::TimestampIndependent)
            && source.contains_temporal()
        {
            // If the source IDs are timestamp independent but the query contains temporal functions,
            // then the timeline context needs to be upgraded to timestamp dependent. This is
            // required because `source_ids` doesn't contain functions.
            timeline_context = TimelineContext::TimestampDependent;
        }

        let oracle_read_ts = self.oracle_read_ts(session, &timeline_context, &when).await;

        let determination = self
            .sequence_peek_timestamp(
                session,
                &when,
                cluster_id,
                timeline_context,
                oracle_read_ts,
                &id_bundle,
                &source_ids,
                real_time_recency_ts,
                RequireLinearization::NotRequired,
            )
            .await?;
        let explanation = self.explain_timestamp(session, cluster_id, &id_bundle, determination);

        let s = if is_json {
            serde_json::to_string_pretty(&explanation).expect("failed to serialize explanation")
        } else {
            explanation.to_string()
        };
        let rows = vec![Row::pack_slice(&[Datum::from(s.as_str())])];
        Ok(Self::send_immediate_rows(rows))
    }

    #[instrument]
    pub(super) async fn sequence_insert(
        &mut self,
        mut ctx: ExecuteContext,
        plan: plan::InsertPlan,
    ) {
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
                plan.values.clone().lower(self.catalog().system_config()),
                ctx
            );
            OptimizedMirRelationExpr(expr)
        } else {
            // Collect optimizer parameters.
            let optimizer_config = optimize::OptimizerConfig::from(self.catalog().system_config());

            // Build an optimizer for this VIEW.
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
                    Some(table) => table
                        .desc(
                            &self
                                .catalog()
                                .resolve_full_name(table.name(), Some(ctx.session().conn_id())),
                        )
                        .expect("desc called on table")
                        .arity(),
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

                if return_if_err!(plan.values.contains_temporal(), ctx) {
                    ctx.retire(Err(AdapterError::Unsupported(
                        "calls to mz_now in write statements",
                    )));
                    return;
                }

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
        let mut source_ids = plan.selection.depends_on();
        source_ids.insert(plan.id);
        guard_write_critical_section!(self, ctx, Plan::ReadThenWrite(plan), source_ids);

        let plan::ReadThenWritePlan {
            id,
            kind,
            selection,
            assignments,
            finishing,
            returning,
        } = plan;

        // Read then writes can be queued, so re-verify the id exists.
        let desc = match self.catalog().try_get_entry(&id) {
            Some(table) => table
                .desc(
                    &self
                        .catalog()
                        .resolve_full_name(table.name(), Some(ctx.session().conn_id())),
                )
                .expect("desc called on table")
                .into_owned(),
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

        // Ensure all objects `selection` depends on are valid for
        // `ReadThenWrite` operations, i.e. they do not refer to any objects
        // whose notion of time moves differently than that of user tables.
        // `true` indicates they're all valid; `false` there are > 0 invalid
        // dependencies.
        //
        // This limitation is meant to ensure no writes occur between this read
        // and the subsequent write.
        fn validate_read_dependencies(catalog: &Catalog, id: &GlobalId) -> bool {
            use CatalogItemType::*;
            match catalog.try_get_entry(id) {
                Some(entry) => match entry.item().typ() {
                    typ @ (Func | View | MaterializedView) => {
                        let valid_id = id.is_user() || matches!(typ, Func);
                        valid_id
                            && (
                                // empty `uses` indicates either system func or
                                // view created from constants
                                entry.uses().is_empty()
                                    || entry
                                        .uses()
                                        .iter()
                                        .all(|id| validate_read_dependencies(catalog, id))
                            )
                    }
                    Source | Secret | Connection => false,
                    // Cannot select from sinks or indexes
                    Sink | Index => unreachable!(),
                    Table => id.is_user(),
                    Type => true,
                },
                None => false,
            }
        }

        for id in selection.depends_on() {
            if !validate_read_dependencies(self.catalog(), &id) {
                ctx.retire(Err(AdapterError::InvalidTableMutationSelection));
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
                source: selection,
                when: QueryWhen::FreshestTableWrite,
                finishing,
                copy_to: None,
            },
            TargetCluster::Active,
        )
        .await;

        let internal_cmd_tx = self.internal_cmd_tx.clone();
        let strict_serializable_reads_tx = self.strict_serializable_reads_tx.clone();
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

            let make_diffs = move |rows: Vec<Row>| -> Result<Vec<(Row, Diff)>, AdapterError> {
                let arena = RowArena::new();
                // Use 2x row len incase there's some assignments.
                let mut diffs = Vec::with_capacity(rows.len() * 2);
                let mut datum_vec = mz_repr::DatumVec::new();
                for row in rows {
                    if !assignments.is_empty() {
                        assert!(
                            matches!(kind, MutationKind::Update),
                            "only updates support assignments"
                        );
                        let mut datums = datum_vec.borrow_with(&row);
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
                        diffs.push((updated, 1));
                    }
                    match kind {
                        // Updates and deletes always remove the
                        // current row. Updates will also add an
                        // updated value.
                        MutationKind::Update | MutationKind::Delete => diffs.push((row, -1)),
                        MutationKind::Insert => diffs.push((row, 1)),
                    }
                }
                for (row, diff) in &diffs {
                    if *diff > 0 {
                        for (idx, datum) in row.iter().enumerate() {
                            desc.constraints_met(idx, &datum)?;
                        }
                    }
                }
                Ok(diffs)
            };
            let diffs = match peek_response {
                ExecuteResponse::SendingRows { future: batch } => {
                    // TODO(jkosh44): This timeout should be removed;
                    // we should instead periodically ensure clusters are
                    // healthy and actively cancel any work waiting on unhealthy
                    // clusters.
                    match tokio::time::timeout(timeout_dur, batch).await {
                        Ok(res) => match res {
                            PeekResponseUnary::Rows(rows) => make_diffs(rows),
                            PeekResponseUnary::Canceled => Err(AdapterError::Canceled),
                            PeekResponseUnary::Error(e) => {
                                Err(AdapterError::Unstructured(anyhow!(e)))
                            }
                        },
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
                            Err(AdapterError::StatementTimeout)
                        }
                    }
                }
                ExecuteResponse::SendingRowsImmediate { rows } => make_diffs(rows),
                resp => Err(AdapterError::Unstructured(anyhow!(
                    "unexpected peek response: {resp:?}"
                ))),
            };
            let mut returning_rows = Vec::new();
            let mut diff_err: Option<AdapterError> = None;
            if !returning.is_empty() && diffs.is_ok() {
                let arena = RowArena::new();
                for (row, diff) in diffs
                    .as_ref()
                    .expect("known to be `Ok` from `is_ok()` call above")
                {
                    if diff < &1 {
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
                    let diff = NonZeroI64::try_from(*diff).expect("known to be >= 1");
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
        session: &mut Session,
        plan: plan::AlterItemRenamePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let op = catalog::Op::RenameItem {
            id: plan.id,
            current_full_name: plan.current_full_name,
            to_name: plan.to_name,
        };
        match self
            .catalog_transact_with_ddl_transaction(session, vec![op])
            .await
        {
            Ok(()) => Ok(ExecuteResponse::AlteredObject(plan.object_type)),
            Err(err) => Err(err),
        }
    }

    #[instrument]
    pub(super) async fn sequence_alter_retain_history(
        &mut self,
        session: &mut Session,
        plan: plan::AlterRetainHistoryPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let ops = vec![catalog::Op::AlterRetainHistory {
            id: plan.id,
            value: plan.value,
            window: plan.window,
        }];
        self.catalog_transact_with_side_effects(Some(session), ops, |coord| async {
            let cluster = match coord.catalog().get_entry(&plan.id).item() {
                CatalogItem::Table(_)
                | CatalogItem::Source(_)
                | CatalogItem::MaterializedView(_) => None,
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
                    coord.update_compute_base_read_policy(cluster, plan.id, plan.window.into())
                }
                None => {
                    coord.update_storage_base_read_policies(vec![(plan.id, plan.window.into())])
                }
            }
        })
        .await?;
        Ok(ExecuteResponse::AlteredObject(plan.object_type))
    }

    #[instrument]
    pub(super) async fn sequence_alter_schema_rename(
        &mut self,
        session: &mut Session,
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
            .catalog_transact_with_ddl_transaction(session, vec![op])
            .await
        {
            Ok(()) => Ok(ExecuteResponse::AlteredObject(ObjectType::Schema)),
            Err(err) => Err(err),
        }
    }

    #[instrument]
    pub(super) async fn sequence_alter_schema_swap(
        &mut self,
        session: &mut Session,
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
            .catalog_transact_with_ddl_transaction(session, vec![op_a, op_b, op_c])
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
        let mut attributes = role.attributes().clone();
        let mut vars = role.vars().clone();

        // Apply our updates.
        match option {
            PlannedAlterRoleOption::Attributes(attrs) => {
                if let Some(inherit) = attrs.inherit {
                    attributes.inherit = inherit;
                }

                if let Some(notice) = self.should_emit_rbac_notice(session) {
                    notices.push(notice);
                }
            }
            PlannedAlterRoleOption::Variable(variable) => {
                // Get the variable to make sure it's valid and visible.
                session
                    .vars()
                    .get(Some(catalog.system_vars()), variable.name())?;

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
                })
            }
        }

        let op = catalog::Op::AlterRole {
            id,
            name,
            attributes,
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
    pub(super) async fn sequence_alter_secret(
        &mut self,
        session: &Session,
        plan: plan::AlterSecretPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let plan::AlterSecretPlan { id, mut secret_as } = plan;

        let payload = self.extract_secret(session, &mut secret_as)?;

        self.secrets_controller.ensure(id, &payload).await?;

        Ok(ExecuteResponse::AlteredObject(ObjectType::Secret))
    }

    #[instrument]
    pub(super) async fn sequence_alter_connection(
        &mut self,
        ctx: ExecuteContext,
        AlterConnectionPlan { id, action }: AlterConnectionPlan,
    ) {
        match action {
            AlterConnectionAction::RotateKeys => {
                let r = self.sequence_rotate_keys(ctx.session(), id).await;
                ctx.retire(r);
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
    async fn sequence_rotate_keys(
        &mut self,
        session: &Session,
        id: GlobalId,
    ) -> Result<ExecuteResponse, AdapterError> {
        let secret = self.secrets_controller.reader().read(id).await?;
        let previous_key_set = SshKeyPairSet::from_bytes(&secret)?;
        let new_key_set = previous_key_set.rotate()?;
        self.secrets_controller
            .ensure(id, &new_key_set.to_bytes())
            .await?;

        let ops = vec![catalog::Op::UpdateRotatedKeys {
            id,
            previous_public_key_pair: previous_key_set.public_keys(),
            new_public_key_pair: new_key_set.public_keys(),
        }];

        match self.catalog_transact(Some(session), ops).await {
            Ok(_) => Ok(ExecuteResponse::AlteredObject(ObjectType::Connection)),
            Err(err) => Err(err),
        }
    }

    #[instrument]
    async fn sequence_alter_connection_options(
        &mut self,
        mut ctx: ExecuteContext,
        id: GlobalId,
        set_options: BTreeMap<ConnectionOptionName, Option<WithOptionValue<mz_sql::names::Aug>>>,
        drop_options: BTreeSet<ConnectionOptionName>,
        validate: bool,
    ) {
        let cur_entry = self.catalog().get_entry(&id);
        let cur_conn = cur_entry.connection().expect("known to be connection");

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
                connection: plan.connection.connection,
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
                .connection
                .clone()
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
                    let dependency_ids = conn.resolved_ids.0.clone();
                    let result = match connection.validate(id, &current_storage_parameters).await {
                        Ok(()) => Ok(conn),
                        Err(err) => Err(err.into()),
                    };

                    // It is not an error for validation to complete after `internal_cmd_rx` is dropped.
                    let result = internal_cmd_tx.send(Message::AlterConnectionValidationReady(
                        AlterConnectionValidationReady {
                            ctx,
                            result,
                            connection_gid: id,
                            plan_validity: PlanValidity {
                                transient_revision,
                                dependency_ids,
                                cluster_id: None,
                                replica_id: None,
                                role_metadata,
                            },
                            otel_ctx,
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
        session: &mut Session,
        id: GlobalId,
        mut connection: Connection,
    ) -> Result<ExecuteResponse, AdapterError> {
        match &mut connection.connection {
            mz_storage_types::connections::Connection::Ssh(ref mut ssh) => {
                // Retain the connection's current SSH keys
                let current_ssh = match &self
                    .catalog
                    .get_entry(&id)
                    .connection()
                    .expect("known to be Connection")
                    .connection
                {
                    mz_storage_types::connections::Connection::Ssh(ssh) => ssh,
                    _ => unreachable!(),
                };

                ssh.public_keys = current_ssh.public_keys.clone();
            }
            _ => {}
        };

        match self.catalog.get_entry(&id).item() {
            CatalogItem::Connection(curr_conn) => {
                curr_conn
                    .connection
                    .alter_compatible(id, &connection.connection)
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

        match connection.connection {
            mz_storage_types::connections::Connection::AwsPrivatelink(ref privatelink) => {
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

        let mut sources = BTreeMap::new();
        let mut sinks = BTreeMap::new();

        while let Some(id) = connections.pop_front() {
            for id in self.catalog.get_entry(&id).used_by() {
                let entry = self.catalog.get_entry(id);
                match entry.item_type() {
                    CatalogItemType::Connection => connections.push_back(*id),
                    CatalogItemType::Source => {
                        let ingestion =
                            match &entry.source().expect("known to be source").data_source {
                                DataSourceDesc::Ingestion(ingestion) => ingestion
                                    .clone()
                                    .into_inline_connection(self.catalog().state()),
                                _ => unreachable!("only ingestions reference connections"),
                            };

                        sources.insert(*id, ingestion);
                    }
                    CatalogItemType::Sink => {
                        let export = entry.sink().expect("known to be sink");
                        sinks.insert(
                            *id,
                            export
                                .connection
                                .clone()
                                .into_inline_connection(self.catalog().state()),
                        );
                    }
                    t => unreachable!("connection dependency not expected on {}", t),
                }
            }
        }

        if !sources.is_empty() {
            self.controller
                .storage
                .alter_collection(sources)
                .await
                .expect("altering collection after txn must succeed");
        }

        if !sinks.is_empty() {
            self.controller
                .storage
                .update_export_connection(sinks)
                .await
                .expect("altering exports after txn must succeed")
        }

        Ok(ExecuteResponse::AlteredObject(ObjectType::Connection))
    }

    #[instrument]
    pub(super) async fn sequence_alter_source(
        &mut self,
        session: &Session,
        plan::AlterSourcePlan { id, action }: plan::AlterSourcePlan,
        to_create_subsources: Vec<plan::CreateSourcePlans>,
    ) -> Result<ExecuteResponse, AdapterError> {
        assert!(
            to_create_subsources.is_empty()
                || matches!(action, plan::AlterSourceAction::AddSubsourceExports { .. }),
            "cannot include subsources with {:?}",
            action
        );

        let cur_entry = self.catalog().get_entry(&id);
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
            plan::AlterSourceAction::DropSubsourceExports { to_drop } => {
                mz_ore::soft_assert_or_log!(!to_drop.is_empty(), "`to_drop` is empty");

                const ALTER_SOURCE: &str = "ALTER SOURCE...DROP TABLES";

                let (mut create_source_stmt, mut resolved_ids) =
                    create_sql_to_stmt_deps(self, ALTER_SOURCE, cur_entry.create_sql())?;

                // Ensure that we are only dropping items on which we depend.
                for t in &to_drop {
                    // Remove dependency.
                    let existed = resolved_ids.0.remove(t);
                    if !existed {
                        Err(AdapterError::internal(
                            ALTER_SOURCE,
                            format!("removed {t}, but {id} did not have dependency"),
                        ))?;
                    }
                }

                // We are doing a lot of unwrapping, so just make an error to reference; all of
                // these invariants are guaranteed to be true because of how we plan subsources.
                let purification_err =
                    || AdapterError::internal(ALTER_SOURCE, "error in subsource purification");

                let referenced_subsources = match create_source_stmt
                    .referenced_subsources
                    .as_mut()
                    .ok_or(purification_err())?
                {
                    ReferencedSubsources::SubsetTables(ref mut s) => s,
                    _ => return Err(purification_err()),
                };

                let mut dropped_references = BTreeSet::new();

                // Fixup referenced_subsources. We panic rather than return
                // errors here because `retain` is an infallible operation and
                // actually erroring here is both incredibly unlikely and
                // recoverable (users can stop trying to drop subsources if it
                // panics).
                referenced_subsources.retain(
                    |CreateSourceSubsource {
                         subsource,
                         reference,
                     }| {
                        match subsource
                            .as_ref()
                            .unwrap_or_else(|| panic!("{}", purification_err().to_string()))
                        {
                            DeferredItemName::Named(name) => match name {
                                // Retain all sources which we still have a dependency on.
                                ResolvedItemName::Item { id, .. } => {
                                    let contains = resolved_ids.0.contains(id);
                                    if !contains {
                                        dropped_references.insert(reference.clone());
                                    }
                                    contains
                                }
                                _ => unreachable!("{}", purification_err()),
                            },
                            _ => unreachable!("{}", purification_err()),
                        }
                    },
                );

                referenced_subsources.sort();

                // Remove dropped references from text columns.
                match &mut create_source_stmt.connection {
                    CreateSourceConnection::Postgres { options, .. } => {
                        options.retain_mut(|option| {
                            if option.name != PgConfigOptionName::TextColumns {
                                return true;
                            }

                            // We know this is text_cols
                            match &mut option.value {
                                Some(WithOptionValue::Sequence(names)) => {
                                    names.retain(|name| match name {
                                        WithOptionValue::UnresolvedItemName(
                                            column_qualified_reference,
                                        ) => {
                                            mz_ore::soft_assert_eq_or_log!(
                                                column_qualified_reference.0.len(), 4
                                            );
                                            if column_qualified_reference.0.len() == 4 {
                                                let mut table = column_qualified_reference.clone();
                                                table.0.truncate(3);
                                                !dropped_references.contains(&table)
                                            } else {
                                                tracing::warn!(
                                                    "PgConfigOptionName::TextColumns had unexpected value {:?}; should have 4 components",
                                                    column_qualified_reference
                                                );
                                                true
                                            }
                                        }
                                        _ => true,
                                    });

                                    names.sort();
                                    // Only retain this option if there are
                                    // names left.
                                    !names.is_empty()
                                }
                                _ => true
                            }
                        })
                    }
                    _ => {}
                }

                // Open a new catalog, which we will use to re-plan our
                // statement with the desired subsources.
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

                // Ensure we have actually removed the subsource from the source's dependency and
                // did not in any other way alter the dependencies.
                let (_, new_resolved_ids) =
                    create_sql_to_stmt_deps(self, ALTER_SOURCE, &plan.source.create_sql)?;

                if let Some(id) = new_resolved_ids.0.iter().find(|id| to_drop.contains(id)) {
                    Err(AdapterError::internal(
                        ALTER_SOURCE,
                        format!("failed to remove dropped ID {id} from dependencies"),
                    ))?;
                }

                if new_resolved_ids.0 != resolved_ids.0 {
                    Err(AdapterError::internal(
                        ALTER_SOURCE,
                        format!("expected resolved items to be {resolved_ids:?}, but is actually {new_resolved_ids:?}"),
                    ))?;
                }

                let source = Source::new(
                    id,
                    plan,
                    resolved_ids,
                    cur_source.custom_logical_compaction_window,
                    cur_source.is_retained_metrics_object,
                );

                // Get new ingestion description for storage.
                let ingestion = match &source.data_source {
                    DataSourceDesc::Ingestion(ingestion) => ingestion
                        .clone()
                        .into_inline_connection(self.catalog().state()),
                    _ => unreachable!("already verified of type ingestion"),
                };

                let collection = btreemap! {id => ingestion};

                self.controller
                    .storage
                    .check_alter_collection(&collection)
                    .map_err(|e| AdapterError::internal(ALTER_SOURCE, e))?;

                // Do not drop this source, even though it's a dependency.
                let primary_source = btreeset! {ObjectId::Item(id)};

                // CASCADE
                let drops = self.catalog().object_dependents_except(
                    &to_drop.into_iter().map(ObjectId::Item).collect(),
                    session.conn_id(),
                    primary_source,
                );

                let DropOps {
                    mut ops,
                    dropped_active_db,
                    dropped_active_cluster,
                    dropped_in_use_indexes,
                } = self.sequence_drop_common(session, drops)?;

                assert!(
                    !dropped_active_db && !dropped_active_cluster,
                    "dropping subsources does not drop DBs or clusters"
                );

                soft_assert_or_log!(
                    dropped_in_use_indexes.is_empty(),
                    "Dropping subsources might drop indexes, but then all objects dependent on the index should also be dropped."
                );

                // Redefine source.
                ops.push(catalog::Op::UpdateItem {
                    id,
                    // Look this up again so we don't have to hold an immutable reference to the
                    // entry for so long.
                    name: self.catalog.get_entry(&id).name().clone(),
                    to_item: CatalogItem::Source(source),
                });

                self.catalog_transact(Some(session), ops).await?;

                // Commit the new ingestion to storage.
                self.controller
                    .storage
                    .alter_collection(collection)
                    .await
                    .expect("altering collection after txn must succeed");
            }
            plan::AlterSourceAction::AddSubsourceExports {
                subsources,
                details,
                options,
            } => {
                const ALTER_SOURCE: &str = "ALTER SOURCE...ADD SUBSOURCES";

                // Resolve items in statement
                let (mut create_source_stmt, resolved_ids) =
                    create_sql_to_stmt_deps(self, ALTER_SOURCE, cur_entry.create_sql())?;

                // We are doing a lot of unwrapping, so just make an error to reference; all of
                // these invariants are guaranteed to be true because of how we plan subsources.
                let purification_err =
                    || AdapterError::internal(ALTER_SOURCE, "error in subsource purification");

                match create_source_stmt
                    .referenced_subsources
                    .as_mut()
                    .ok_or(purification_err())?
                {
                    ReferencedSubsources::SubsetTables(c) => {
                        mz_ore::soft_assert_no_log!(
                            {
                                let current_references: BTreeSet<_> = c
                                    .iter()
                                    .map(|CreateSourceSubsource { reference, .. }| reference)
                                    .collect();
                                let subsources: BTreeSet<_> = subsources
                                    .iter()
                                    .map(|CreateSourceSubsource { reference, .. }| reference)
                                    .collect();

                                current_references
                                    .intersection(&subsources)
                                    .next()
                                    .is_none()
                            },
                            "cannot add subsources that refer to existing PG tables; this should have errored in purification"
                        );

                        c.extend(subsources);
                    }
                    _ => return Err(purification_err()),
                };

                let curr_options = match &mut create_source_stmt.connection {
                    CreateSourceConnection::Postgres { options, .. } => options,
                    _ => return Err(purification_err()),
                };

                // Remove any old detail references
                curr_options
                    .retain(|PgConfigOption { name, .. }| name != &PgConfigOptionName::Details);

                curr_options.push(PgConfigOption {
                    name: PgConfigOptionName::Details,
                    value: details,
                });

                // Merge text columns
                let curr_text_columns = curr_options
                    .iter_mut()
                    .find(|option| option.name == PgConfigOptionName::TextColumns);

                let new_text_columns = options
                    .into_iter()
                    .find(|option| option.name == AlterSourceAddSubsourceOptionName::TextColumns);

                match (curr_text_columns, new_text_columns) {
                    (Some(curr), Some(new)) => {
                        let curr = match curr.value {
                            Some(WithOptionValue::Sequence(ref mut curr)) => curr,
                            _ => unreachable!(),
                        };
                        let new = match new.value {
                            Some(WithOptionValue::Sequence(new)) => new,
                            _ => unreachable!(),
                        };

                        curr.extend(new);
                        curr.sort();

                        mz_ore::soft_assert_no_log!(
                            curr.iter()
                                .all(|v| matches!(v, WithOptionValue::UnresolvedItemName(_))),
                            "all elements of text columns must be UnresolvedItemName, but got {:?}",
                            curr
                        );

                        mz_ore::soft_assert_no_log!(
                            curr.iter().duplicates().next().is_none(),
                            "TEXT COLUMN references must be unique among both sets, but got {:?}",
                            curr
                        );
                    }
                    (None, Some(new)) => {
                        mz_ore::soft_assert_no_log!(
                            match &new.value {
                                Some(WithOptionValue::Sequence(v)) => v
                                    .iter()
                                    .all(|v| matches!(v, WithOptionValue::UnresolvedItemName(_))),
                                _ => false,
                            },
                            "TEXT COLUMNS must have a sequence of unresolved item names but got {:?}",
                            new.value
                        );

                        curr_options.push(PgConfigOption {
                            name: PgConfigOptionName::TextColumns,
                            value: new.value,
                        })
                    }
                    // No change
                    _ => {}
                }

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
                    id,
                    plan,
                    ResolvedIds(
                        resolved_ids
                            .0
                            .into_iter()
                            .chain(to_create_subsources.iter().map(|csp| csp.source_id))
                            .collect(),
                    ),
                    cur_source.custom_logical_compaction_window,
                    cur_source.is_retained_metrics_object,
                );

                let source_compaction_window = source.custom_logical_compaction_window;

                // Get new ingestion description for storage.
                let ingestion = match &source.data_source {
                    DataSourceDesc::Ingestion(ingestion) => ingestion
                        .clone()
                        .into_inline_connection(self.catalog().state()),
                    _ => unreachable!("already verified of type ingestion"),
                };

                let collection = btreemap! {id => ingestion};

                self.controller
                    .storage
                    .check_alter_collection(&collection)
                    .map_err(|e| AdapterError::internal(ALTER_SOURCE, e))?;

                let CreateSourceInner {
                    mut ops,
                    sources,
                    if_not_exists_ids,
                } = self
                    .create_source_inner(session, to_create_subsources)
                    .await?;

                assert!(
                    if_not_exists_ids.is_empty(),
                    "IF NOT EXISTS not supported for ALTER SOURCE...ADD SUBSOURCES"
                );

                // Redefine source.
                ops.push(catalog::Op::UpdateItem {
                    id,
                    // Look this up again so we don't have to hold an immutable reference to the
                    // entry for so long.
                    name: self.catalog.get_entry(&id).name().clone(),
                    to_item: CatalogItem::Source(source),
                });

                self.catalog_transact(Some(session), ops).await?;

                let mut source_ids = Vec::with_capacity(sources.len());
                for (source_id, source) in sources {
                    let source_status_collection_id =
                        Some(self.catalog().resolve_builtin_storage_collection(
                            &mz_catalog::builtin::MZ_SOURCE_STATUS_HISTORY,
                        ));

                    let (data_source, status_collection_id) = match source.data_source {
                        // Subsources use source statuses.
                        DataSourceDesc::Source => (
                            DataSource::Other(DataSourceOther::Source),
                            source_status_collection_id,
                        ),
                        o => {
                            unreachable!(
                                "ALTER SOURCE...ADD SUBSOURCE only creates subsources but got {:?}",
                                o
                            )
                        }
                    };

                    let storage_metadata = self.catalog.state().storage_metadata();

                    self.controller
                        .storage
                        .create_collections(
                            storage_metadata,
                            None,
                            vec![(
                                source_id,
                                CollectionDescription {
                                    desc: source.desc.clone(),
                                    data_source,
                                    since: None,
                                    status_collection_id,
                                },
                            )],
                        )
                        .await
                        .unwrap_or_terminate("cannot fail to create collections");

                    source_ids.push(source_id);
                }

                // Commit the new ingestion to storage.
                self.controller
                    .storage
                    .alter_collection(collection)
                    .await
                    .expect("altering collection after txn must succeed");

                self.initialize_storage_read_policies(
                    source_ids,
                    source_compaction_window.unwrap_or(CompactionWindow::Default),
                )
                .await;
            }
        }

        Ok(ExecuteResponse::AlteredObject(ObjectType::Source))
    }

    fn extract_secret(
        &mut self,
        session: &Session,
        secret_as: &mut MirScalarExpr,
    ) -> Result<Vec<u8>, AdapterError> {
        let temp_storage = RowArena::new();
        prep_scalar_expr(
            secret_as,
            ExprPrepStyle::OneShot {
                logical_time: EvalTime::NotAvailable,
                session,
                catalog_state: self.catalog().state(),
            },
        )?;
        let evaled = secret_as.eval(&[], &temp_storage)?;

        if evaled == Datum::Null {
            coord_bail!("secret value can not be null");
        }

        let payload = evaled.unwrap_bytes();

        // Limit the size of a secret to 512 KiB
        // This is the largest size of a single secret in Consul/Kubernetes
        // We are enforcing this limit across all types of Secrets Controllers
        // Most secrets are expected to be roughly 75B
        if payload.len() > 1024 * 512 {
            coord_bail!("secrets can not be bigger than 512KiB")
        }

        // Enforce that all secrets are valid UTF-8 for now. We expect to lift
        // this restriction in the future, when we discover a connection type
        // that requires binary secrets, but for now it is convenient to ensure
        // here that `SecretsReader::read_string` can never fail due to invalid
        // UTF-8.
        //
        // If you want to remove this line, verify that no caller of
        // `SecretsReader::read_string` will panic if the secret contains
        // invalid UTF-8.
        if std::str::from_utf8(payload).is_err() {
            // Intentionally produce a vague error message (rather than
            // including the invalid bytes, for example), to avoid including
            // secret material in the error message, which might end up in a log
            // file somewhere.
            coord_bail!("secret value must be valid UTF-8");
        }

        Ok(Vec::from(payload))
    }

    #[instrument]
    pub(super) async fn sequence_alter_system_set(
        &mut self,
        session: &Session,
        plan::AlterSystemSetPlan { name, value }: plan::AlterSystemSetPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        self.is_user_allowed_to_alter_system(session, Some(&name))?;
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
                var.visible(session.user(), Some(self.catalog().system_config()))?;
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

    // Returns the name of the portal to execute.
    #[instrument]
    pub(super) fn sequence_execute(
        &mut self,
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
        let revision = ps.catalog_revision;
        let logging = Arc::clone(ps.logging());
        session.create_new_portal(stmt, logging, desc, plan.params, Vec::new(), revision)
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

                // Alter owner cascades down to sub-sources and progress collections.
                let dependent_subsources =
                    entry
                        .subsources()
                        .into_iter()
                        .map(|id| catalog::Op::UpdateOwner {
                            id: ObjectId::Item(id),
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
}

#[derive(Debug)]
struct CachedStatisticsOracle {
    cache: BTreeMap<GlobalId, usize>,
}

impl CachedStatisticsOracle {
    pub async fn new<T: Clone + std::fmt::Debug + timely::PartialOrder + Send + Sync>(
        ids: &BTreeSet<GlobalId>,
        as_of: &Antichain<T>,
        storage: &dyn mz_storage_client::controller::StorageController<Timestamp = T>,
    ) -> Result<Self, StorageError<T>> {
        let mut cache = BTreeMap::new();

        for id in ids {
            let stats = storage.snapshot_stats(*id, as_of.clone()).await;

            match stats {
                Ok(stats) => {
                    cache.insert(*id, stats.num_updates);
                }
                Err(StorageError::IdentifierMissing(id)) => {
                    ::tracing::debug!("no statistics for {id}")
                }
                Err(e) => return Err(e),
            }
        }

        Ok(Self { cache })
    }
}

impl mz_transform::StatisticsOracle for CachedStatisticsOracle {
    fn cardinality_estimate(&self, id: GlobalId) -> Option<usize> {
        self.cache.get(&id).map(|estimate| *estimate)
    }
}

impl Coordinator {
    pub(super) async fn statistics_oracle(
        &self,
        session: &Session,
        source_ids: &BTreeSet<GlobalId>,
        query_as_of: &Antichain<Timestamp>,
        is_oneshot: bool,
    ) -> Result<Box<dyn mz_transform::StatisticsOracle>, AdapterError> {
        if !session.vars().enable_session_cardinality_estimates() {
            return Ok(Box::new(EmptyStatisticsOracle));
        }

        let timeout = if is_oneshot {
            // TODO(mgree): ideally, we would shorten the timeout even more if we think the query could take the fast path
            self.catalog()
                .system_config()
                .optimizer_oneshot_stats_timeout()
        } else {
            self.catalog().system_config().optimizer_stats_timeout()
        };

        let cached_stats = mz_ore::future::timeout(
            timeout,
            CachedStatisticsOracle::new(source_ids, query_as_of, self.controller.storage.as_ref()),
        )
        .await;

        match cached_stats {
            Ok(stats) => Ok(Box::new(stats)),
            Err(mz_ore::future::TimeoutError::DeadlineElapsed) => {
                warn!(
                    is_oneshot = is_oneshot,
                    "optimizer statistics collection timed out after {}ms",
                    timeout.as_millis()
                );

                Ok(Box::new(EmptyStatisticsOracle))
            }
            Err(mz_ore::future::TimeoutError::Inner(e)) => Err(AdapterError::Storage(e)),
        }
    }
}

/// Checks whether we should emit diagnostic
/// information associated with reading per-replica sources.
///
/// If an unrecoverable error is found (today: an untargeted read on a
/// cluster with a non-1 number of replicas), return that.  Otherwise,
/// return a list of associated notices (today: we always emit exactly
/// one notice if there are any per-replica log dependencies and if
/// `emit_introspection_query_notice` is set, and none otherwise.)
pub(super) fn check_log_reads(
    catalog: &Catalog,
    cluster: &Cluster,
    source_ids: &BTreeSet<GlobalId>,
    target_replica: &mut Option<ReplicaId>,
    vars: &SessionVars,
) -> Result<impl IntoIterator<Item = AdapterNotice>, AdapterError>
where
{
    let log_names = source_ids
        .iter()
        .flat_map(|id| catalog.introspection_dependencies(*id))
        .map(|id| catalog.get_entry(&id).name().item.clone())
        .collect::<Vec<_>>();

    if log_names.is_empty() {
        return Ok(None);
    }

    // Reading from log sources on replicated clusters is only allowed if a
    // target replica is selected. Otherwise, we have no way of knowing which
    // replica we read the introspection data from.
    let num_replicas = cluster.replicas().count();
    if target_replica.is_none() {
        if num_replicas == 1 {
            *target_replica = cluster.replicas().map(|r| r.replica_id).next();
        } else {
            return Err(AdapterError::UntargetedLogRead { log_names });
        }
    }

    // Ensure that logging is initialized for the target replica, lest
    // we try to read from a non-existing arrangement.
    let replica_id = target_replica.expect("set to `Some` above");
    let replica = &cluster.replica(replica_id).expect("Replica must exist");
    if !replica.config.compute.logging.enabled() {
        return Err(AdapterError::IntrospectionDisabled { log_names });
    }

    Ok(vars
        .emit_introspection_query_notice()
        .then_some(AdapterNotice::PerReplicaLogRead { log_names }))
}

impl Coordinator {
    /// Forward notices that we got from the optimizer.
    pub(super) fn emit_optimizer_notices(
        &mut self,
        session: &Session,
        notices: &Vec<RawOptimizerNotice>,
    ) {
        let humanizer = self.catalog.for_session(session);
        let system_vars = self.catalog.system_config();
        for notice in notices {
            let kind = OptimizerNoticeKind::from(notice);
            let notice_enabled = match kind {
                OptimizerNoticeKind::IndexAlreadyExists => {
                    system_vars.enable_notices_for_index_already_exists()
                }
                OptimizerNoticeKind::IndexTooWideForLiteralConstraints => {
                    system_vars.enable_notices_for_index_too_wide_for_literal_constraints()
                }
                OptimizerNoticeKind::IndexKeyEmpty => {
                    system_vars.enable_notices_for_index_empty_key()
                }
            };
            if notice_enabled {
                // We don't need to redact the notice parts because
                // `emit_optimizer_notices` is onlyy called by the `sequence_~`
                // method for the DDL that produces that notice.
                session.add_notice(AdapterNotice::OptimizerNotice {
                    notice: notice.message(&humanizer, false).to_string(),
                    hint: notice.hint(&humanizer, false).to_string(),
                });
            }
            self.metrics
                .optimization_notices
                .with_label_values(&[kind.metric_label()])
                .inc_by(1);
        }
    }
}
