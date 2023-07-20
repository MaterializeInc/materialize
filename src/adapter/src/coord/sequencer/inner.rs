// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::iter;
use std::num::{NonZeroI64, NonZeroUsize};
use std::panic::AssertUnwindSafe;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use futures::future::BoxFuture;
use itertools::Itertools;
use maplit::btreeset;
use mz_cloud_resources::VpcEndpointConfig;
use mz_compute_client::types::dataflows::{DataflowDesc, DataflowDescription, IndexDesc};
use mz_compute_client::types::sinks::{
    ComputeSinkConnection, ComputeSinkDesc, SubscribeSinkConnection,
};
use mz_controller::clusters::{ClusterId, ReplicaId};
use mz_expr::{
    permutation_for_arrangement, CollectionPlan, MirRelationExpr, MirScalarExpr,
    OptimizedMirRelationExpr, RowSetFinishing,
};
use mz_ore::collections::CollectionExt;
use mz_ore::result::ResultExt as OreResultExt;
use mz_ore::task;
use mz_ore::tracing::OpenTelemetryContext;
use mz_ore::vec::VecExt;
use mz_repr::adt::jsonb::Jsonb;
use mz_repr::adt::mz_acl_item::{MzAclItem, PrivilegeMap};
use mz_repr::explain::{ExplainFormat, Explainee};
use mz_repr::role_id::RoleId;
use mz_repr::{Datum, Diff, GlobalId, RelationDesc, RelationType, Row, RowArena, Timestamp};
use mz_sql::ast::{ExplainStage, IndexOptionName};
use mz_sql::catalog::{
    CatalogCluster, CatalogClusterReplica, CatalogDatabase, CatalogError,
    CatalogItem as SqlCatalogItem, CatalogItemType, CatalogRole, CatalogSchema, CatalogTypeDetails,
    ErrorMessageObjectDescription, ObjectType, SessionCatalog,
};
use mz_sql::names::{
    ObjectId, QualifiedItemName, ResolvedDatabaseSpecifier, ResolvedIds, ResolvedItemName,
    SchemaSpecifier, SystemObjectId,
};
use mz_sql::plan::{
    AlterDefaultPrivilegesPlan, AlterIndexResetOptionsPlan, AlterIndexSetOptionsPlan,
    AlterItemRenamePlan, AlterOptionParameter, AlterOwnerPlan, AlterRolePlan, AlterSecretPlan,
    AlterSinkPlan, AlterSourceAction, AlterSourcePlan, AlterSystemResetAllPlan,
    AlterSystemResetPlan, AlterSystemSetPlan, CreateConnectionPlan, CreateDatabasePlan,
    CreateIndexPlan, CreateMaterializedViewPlan, CreateRolePlan, CreateSchemaPlan,
    CreateSecretPlan, CreateSinkPlan, CreateSourcePlans, CreateTablePlan, CreateTypePlan,
    CreateViewPlan, DropObjectsPlan, DropOwnedPlan, ExecutePlan, ExplainPlan, GrantPrivilegesPlan,
    GrantRolePlan, IndexOption, InsertPlan, InspectShardPlan, MaterializedView, MutationKind,
    OptimizerConfig, Params, Plan, QueryWhen, ReadThenWritePlan, ReassignOwnedPlan,
    ResetVariablePlan, RevokePrivilegesPlan, RevokeRolePlan, SelectPlan, SendDiffsPlan,
    SetTransactionPlan, SetVariablePlan, ShowVariablePlan, SideEffectingFunc,
    SourceSinkClusterConfig, SubscribeFrom, SubscribePlan, UpdatePrivilege, VariableValue,
};
use mz_sql::session::vars::{
    IsolationLevel, OwnedVarInput, Var, VarInput, CLUSTER_VAR_NAME, DATABASE_VAR_NAME,
    ENABLE_RBAC_CHECKS, SCHEMA_ALIAS, TRANSACTION_ISOLATION_VAR_NAME,
};
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{
    AlterSourceAddSubsourceOptionName, CreateSourceConnection, CreateSourceSubsource,
    DeferredItemName, PgConfigOption, PgConfigOptionName, ReferencedSubsources, Statement,
    TransactionMode, WithOptionValue,
};
use mz_ssh_util::keys::SshKeyPairSet;
use mz_storage_client::controller::{CollectionDescription, DataSource, ReadPolicy, StorageError};
use mz_storage_client::types::sinks::StorageSinkConnectionBuilder;
use mz_transform::{EmptyStatisticsOracle, Optimizer};
use timely::progress::{Antichain, Timestamp as TimelyTimestamp};
use tokio::sync::{mpsc, oneshot, OwnedMutexGuard};
use tracing::instrument::WithSubscriber;
use tracing::{event, warn, Level};

use crate::catalog::{
    self, Catalog, CatalogItem, Cluster, ConnCatalog, Connection, DataSourceDesc, Op,
    StorageSinkConnectionState, UpdatePrivilegeVariant,
};
use crate::command::{ExecuteResponse, Response};
use crate::coord::appends::{Deferred, DeferredPlan, PendingWriteTxn};
use crate::coord::dataflows::{prep_relation_expr, prep_scalar_expr, EvalTime, ExprPrepStyle};
use crate::coord::id_bundle::CollectionIdBundle;
use crate::coord::peek::{FastPathPlan, PlannedPeek};
use crate::coord::read_policy::SINCE_GRANULARITY;
use crate::coord::timeline::TimelineContext;
use crate::coord::timestamp_selection::{
    TimestampContext, TimestampDetermination, TimestampProvider, TimestampSource,
};
use crate::coord::{
    peek, Coordinator, CreateConnectionValidationReady, ExecuteContext, Message, PeekStage,
    PeekStageFinish, PeekStageOptimize, PeekStageTimestamp, PeekStageValidate, PendingRead,
    PendingReadTxn, PendingTxn, PendingTxnResponse, PlanValidity, RealTimeRecencyContext,
    SinkConnectionReady, TargetCluster, DEFAULT_LOGICAL_COMPACTION_WINDOW_TS,
};
use crate::error::AdapterError;
use crate::explain::optimizer_trace::OptimizerTrace;
use crate::notice::AdapterNotice;
use crate::rbac::{self, is_rbac_enabled_for_session};
use crate::session::{EndTransactionAction, Session, TransactionOps, TransactionStatus, WriteOp};
use crate::subscribe::ActiveSubscribe;
use crate::util::{
    send_immediate_rows, viewable_variables, ClientTransmitter, ComputeSinkId, ResultExt,
};
use crate::{guard_write_critical_section, PeekResponseUnary, TimestampExplanation};

/// Attempts to execute an expression. If an error is returned then the error is sent
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
}

// A bundle of values returned from create_source_inner
struct CreateSourceInner {
    ops: Vec<Op>,
    sources: Vec<(GlobalId, catalog::Source)>,
    if_not_exists_ids: BTreeMap<GlobalId, QualifiedItemName>,
}

impl Coordinator {
    async fn create_source_inner(
        &mut self,
        session: &mut Session,
        plans: Vec<CreateSourcePlans>,
    ) -> Result<CreateSourceInner, AdapterError> {
        let mut ops = vec![];
        let mut sources = vec![];

        let if_not_exists_ids = plans
            .iter()
            .filter_map(
                |CreateSourcePlans {
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

        for CreateSourcePlans {
            source_id,
            plan,
            resolved_ids,
        } in plans
        {
            let name = plan.name.clone();
            let source_oid = self.catalog_mut().allocate_oid()?;
            let cluster_id = match plan.source.data_source {
                mz_sql::plan::DataSourceDesc::Ingestion(_) => Some(
                    self.create_linked_cluster_ops(
                        source_id,
                        &plan.name,
                        &plan.cluster_config,
                        &mut ops,
                        session,
                    )
                    .await?,
                ),
                mz_sql::plan::DataSourceDesc::Webhook { .. } => {
                    plan.cluster_config.cluster_id().cloned()
                }
                _ => None,
            };
            let source =
                catalog::Source::new(source_id, plan, cluster_id, resolved_ids, None, false);
            ops.push(catalog::Op::CreateItem {
                id: source_id,
                oid: source_oid,
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

    #[tracing::instrument(level = "debug", skip(self))]
    pub(super) async fn sequence_create_source(
        &mut self,
        session: &mut Session,
        plans: Vec<CreateSourcePlans>,
    ) -> Result<ExecuteResponse, AdapterError> {
        let CreateSourceInner {
            ops,
            sources,
            if_not_exists_ids,
        } = self.create_source_inner(session, plans).await?;

        match self.catalog_transact(Some(session), ops).await {
            Ok(()) => {
                let mut source_ids = Vec::with_capacity(sources.len());
                for (source_id, source) in sources {
                    let source_status_collection_id =
                        Some(self.catalog().resolve_builtin_storage_collection(
                            &crate::catalog::builtin::MZ_SOURCE_STATUS_HISTORY,
                        ));

                    let (data_source, status_collection_id) = match source.data_source {
                        DataSourceDesc::Ingestion(ingestion) => (
                            DataSource::Ingestion(ingestion),
                            source_status_collection_id,
                        ),
                        // Subsources use source statuses.
                        DataSourceDesc::Source => (DataSource::Other, source_status_collection_id),
                        DataSourceDesc::Progress => (DataSource::Progress, None),
                        DataSourceDesc::Webhook { .. } => (DataSource::Webhook, None),
                        DataSourceDesc::Introspection(_) => {
                            unreachable!("cannot create sources with introspection data sources")
                        }
                    };

                    self.maybe_create_linked_cluster(source_id).await;

                    self.controller
                        .storage
                        .create_collections(vec![(
                            source_id,
                            CollectionDescription {
                                desc: source.desc.clone(),
                                data_source,
                                since: None,
                                status_collection_id,
                            },
                        )])
                        .await
                        .unwrap_or_terminate("cannot fail to create collections");

                    source_ids.push(source_id);
                }

                self.initialize_storage_read_policies(
                    source_ids,
                    Some(DEFAULT_LOGICAL_COMPACTION_WINDOW_TS),
                )
                .await;

                Ok(ExecuteResponse::CreatedSource)
            }
            Err(AdapterError::Catalog(catalog::Error {
                kind: catalog::ErrorKind::ItemAlreadyExists(id, _),
                ..
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

    #[tracing::instrument(level = "debug", skip(self))]
    pub(super) async fn sequence_create_connection(
        &mut self,
        mut ctx: ExecuteContext,
        mut plan: CreateConnectionPlan,
        resolved_ids: ResolvedIds,
    ) {
        let connection_gid = match self.catalog_mut().allocate_user_id().await {
            Ok(gid) => gid,
            Err(err) => return ctx.retire(Err(err.into())),
        };

        match plan.connection.connection {
            mz_storage_client::types::connections::Connection::Ssh(ref mut ssh) => {
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
            let connection_context = self.connection_context.clone();
            let otel_ctx = OpenTelemetryContext::obtain();
            task::spawn(|| format!("validate_connection:{conn_id}"), async move {
                let connection = &plan.connection.connection;
                let result = match connection
                    .validate(connection_gid, &connection_context)
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

    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) async fn sequence_create_connection_stage_finish(
        &mut self,
        session: &mut Session,
        connection_gid: GlobalId,
        plan: CreateConnectionPlan,
        resolved_ids: ResolvedIds,
    ) -> Result<ExecuteResponse, AdapterError> {
        let connection_oid = self.catalog_mut().allocate_oid()?;
        let connection = plan.connection.connection;

        let ops = vec![catalog::Op::CreateItem {
            id: connection_gid,
            oid: connection_oid,
            name: plan.name.clone(),
            item: CatalogItem::Connection(Connection {
                create_sql: plan.connection.create_sql,
                connection: connection.clone(),
                resolved_ids,
            }),
            owner_id: *session.current_role_id(),
        }];

        match self.catalog_transact(Some(session), ops).await {
            Ok(_) => {
                match connection {
                    mz_storage_client::types::connections::Connection::AwsPrivatelink(
                        ref privatelink,
                    ) => {
                        let spec = VpcEndpointConfig {
                            aws_service_name: privatelink.service_name.to_owned(),
                            availability_zone_ids: privatelink.availability_zones.to_owned(),
                        };
                        self.cloud_resource_controller
                            .as_ref()
                            .ok_or(AdapterError::Unsupported("AWS PrivateLink connections"))?
                            .ensure_vpc_endpoint(connection_gid, spec)
                            .await?;
                    }
                    _ => {}
                }
                Ok(ExecuteResponse::CreatedConnection)
            }
            Err(AdapterError::Catalog(catalog::Error {
                kind: catalog::ErrorKind::ItemAlreadyExists(_, _),
                ..
            })) if plan.if_not_exists => Ok(ExecuteResponse::CreatedConnection),
            Err(err) => Err(err),
        }
    }

    pub(super) async fn sequence_rotate_keys(
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

    #[tracing::instrument(level = "debug", skip(self))]
    pub(super) async fn sequence_create_database(
        &mut self,
        session: &mut Session,
        plan: CreateDatabasePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let db_oid = self.catalog_mut().allocate_oid()?;
        let schema_oid = self.catalog_mut().allocate_oid()?;
        let ops = vec![catalog::Op::CreateDatabase {
            name: plan.name.clone(),
            oid: db_oid,
            public_schema_oid: schema_oid,
            owner_id: *session.current_role_id(),
        }];
        match self.catalog_transact(Some(session), ops).await {
            Ok(_) => Ok(ExecuteResponse::CreatedDatabase),
            Err(AdapterError::Catalog(catalog::Error {
                kind: catalog::ErrorKind::DatabaseAlreadyExists(_),
                ..
            })) if plan.if_not_exists => {
                session.add_notice(AdapterNotice::DatabaseAlreadyExists { name: plan.name });
                Ok(ExecuteResponse::CreatedDatabase)
            }
            Err(err) => Err(err),
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub(super) async fn sequence_create_schema(
        &mut self,
        session: &mut Session,
        plan: CreateSchemaPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let oid = self.catalog_mut().allocate_oid()?;
        let op = catalog::Op::CreateSchema {
            database_id: plan.database_spec,
            schema_name: plan.schema_name.clone(),
            oid,
            owner_id: *session.current_role_id(),
        };
        match self.catalog_transact(Some(session), vec![op]).await {
            Ok(_) => Ok(ExecuteResponse::CreatedSchema),
            Err(AdapterError::Catalog(catalog::Error {
                kind: catalog::ErrorKind::SchemaAlreadyExists(_),
                ..
            })) if plan.if_not_exists => {
                session.add_notice(AdapterNotice::SchemaAlreadyExists {
                    name: plan.schema_name,
                });
                Ok(ExecuteResponse::CreatedSchema)
            }
            Err(err) => Err(err),
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub(super) async fn sequence_create_role(
        &mut self,
        session: &Session,
        CreateRolePlan { name, attributes }: CreateRolePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let oid = self.catalog_mut().allocate_oid()?;
        let op = catalog::Op::CreateRole {
            name,
            oid,
            attributes,
        };
        self.catalog_transact(Some(session), vec![op])
            .await
            .map(|_| ExecuteResponse::CreatedRole)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub(super) async fn sequence_create_table(
        &mut self,
        session: &mut Session,
        plan: CreateTablePlan,
        resolved_ids: ResolvedIds,
    ) -> Result<ExecuteResponse, AdapterError> {
        let CreateTablePlan {
            name,
            table,
            if_not_exists,
        } = plan;

        let conn_id = if table.temporary {
            Some(session.conn_id())
        } else {
            None
        };
        let table_id = self.catalog_mut().allocate_user_id().await?;
        let table = catalog::Table {
            create_sql: table.create_sql,
            desc: table.desc,
            defaults: table.defaults,
            conn_id: conn_id.cloned(),
            resolved_ids,
            custom_logical_compaction_window: None,
            is_retained_metrics_object: false,
        };
        let table_oid = self.catalog_mut().allocate_oid()?;
        let ops = vec![catalog::Op::CreateItem {
            id: table_id,
            oid: table_oid,
            name: name.clone(),
            item: CatalogItem::Table(table.clone()),
            owner_id: *session.current_role_id(),
        }];
        match self.catalog_transact(Some(session), ops).await {
            Ok(()) => {
                // Determine the initial validity for the table.
                let since_ts = self.peek_local_write_ts();

                let collection_desc = table.desc.clone().into();
                self.controller
                    .storage
                    .create_collections(vec![(table_id, collection_desc)])
                    .await
                    .unwrap_or_terminate("cannot fail to create collections");

                let policy = ReadPolicy::ValidFrom(Antichain::from_elem(since_ts));
                self.controller
                    .storage
                    .set_read_policy(vec![(table_id, policy)]);

                self.initialize_storage_read_policies(
                    vec![table_id],
                    Some(DEFAULT_LOGICAL_COMPACTION_WINDOW_TS),
                )
                .await;

                // Advance the new table to a timestamp higher than the current read timestamp so
                // that the table is immediately readable.
                let upper = since_ts.step_forward();
                let appends = vec![(table_id, Vec::new(), upper)];
                self.controller
                    .storage
                    .append(appends)
                    .expect("invalid table upper initialization")
                    .await
                    .expect("One-shot dropped while waiting synchronously")
                    .unwrap_or_terminate("cannot fail to append");
                Ok(ExecuteResponse::CreatedTable)
            }
            Err(AdapterError::Catalog(catalog::Error {
                kind: catalog::ErrorKind::ItemAlreadyExists(_, _),
                ..
            })) if if_not_exists => {
                session.add_notice(AdapterNotice::ObjectAlreadyExists {
                    name: name.item,
                    ty: "table",
                });
                Ok(ExecuteResponse::CreatedTable)
            }
            Err(err) => Err(err),
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub(super) async fn sequence_create_secret(
        &mut self,
        session: &mut Session,
        plan: CreateSecretPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let CreateSecretPlan {
            name,
            mut secret,
            if_not_exists,
        } = plan;

        let payload = self.extract_secret(session, &mut secret.secret_as)?;

        let id = self.catalog_mut().allocate_user_id().await?;
        let oid = self.catalog_mut().allocate_oid()?;
        let secret = catalog::Secret {
            create_sql: secret.create_sql,
        };

        self.secrets_controller.ensure(id, &payload).await?;

        let ops = vec![catalog::Op::CreateItem {
            id,
            oid,
            name: name.clone(),
            item: CatalogItem::Secret(secret.clone()),
            owner_id: *session.current_role_id(),
        }];

        match self.catalog_transact(Some(session), ops).await {
            Ok(()) => Ok(ExecuteResponse::CreatedSecret),
            Err(AdapterError::Catalog(catalog::Error {
                kind: catalog::ErrorKind::ItemAlreadyExists(_, _),
                ..
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

    #[tracing::instrument(level = "debug", skip(self, ctx))]
    pub(super) async fn sequence_create_sink(
        &mut self,
        ctx: ExecuteContext,
        plan: CreateSinkPlan,
        resolved_ids: ResolvedIds,
    ) {
        let CreateSinkPlan {
            name,
            sink,
            with_snapshot,
            if_not_exists,
            cluster_config: plan_cluster_config,
        } = plan;

        // First try to allocate an ID and an OID. If either fails, we're done.
        let id = return_if_err!(self.catalog_mut().allocate_user_id().await, ctx);
        let oid = return_if_err!(self.catalog_mut().allocate_oid(), ctx);

        let mut ops = vec![];
        let cluster_id = return_if_err!(
            self.create_linked_cluster_ops(
                id,
                &name,
                &plan_cluster_config,
                &mut ops,
                ctx.session()
            )
            .await,
            ctx
        );

        // Knowing that we're only handling kafka sinks here helps us simplify.
        let StorageSinkConnectionBuilder::Kafka(connection_builder) =
            sink.connection_builder.clone();

        // Then try to create a placeholder catalog item with an unknown
        // connection. If that fails, we're done, though if the client specified
        // `if_not_exists` we'll tell the client we succeeded.
        //
        // This placeholder catalog item reserves the name while we create
        // the sink connection, which could take an arbitrarily long time.

        let catalog_sink = catalog::Sink {
            create_sql: sink.create_sql,
            from: sink.from,
            connection: StorageSinkConnectionState::Pending(StorageSinkConnectionBuilder::Kafka(
                connection_builder,
            )),
            envelope: sink.envelope,
            with_snapshot,
            resolved_ids,
            cluster_id,
        };

        ops.push(catalog::Op::CreateItem {
            id,
            oid,
            name: name.clone(),
            item: CatalogItem::Sink(catalog_sink.clone()),
            owner_id: *ctx.session().current_role_id(),
        });

        let from = self.catalog().get_entry(&catalog_sink.from);
        let from_name = from.name().item.clone();
        let from_type = from.item().typ().to_string();
        let result = self
            .catalog_transact_with(Some(ctx.session()), ops, move |txn| {
                // Validate that the from collection is in fact a persist collection we can export.
                txn.dataflow_client
                    .storage
                    .collection(sink.from)
                    .map_err(|e| match e {
                        StorageError::IdentifierMissing(_) => AdapterError::Unstructured(anyhow!(
                            "{from_name} is a {from_type}, which cannot be exported as a sink"
                        )),
                        e => AdapterError::Storage(e),
                    })?;
                Ok(())
            })
            .await;

        match result {
            Ok(()) => {}
            Err(AdapterError::Catalog(catalog::Error {
                kind: catalog::ErrorKind::ItemAlreadyExists(_, _),
                ..
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
        }

        self.maybe_create_linked_cluster(id).await;

        let create_export_token = return_if_err!(
            self.controller
                .storage
                .prepare_export(id, catalog_sink.from),
            ctx
        );

        // Now we're ready to create the sink connection. Arrange to notify the
        // main coordinator thread when the future completes.
        let connection_builder = sink.connection_builder;
        let internal_cmd_tx = self.internal_cmd_tx.clone();
        let connection_context = self.connection_context.clone();
        task::spawn(
            || format!("sink_connection_ready:{}", sink.from),
            async move {
                // It is not an error for sink connections to become ready after `internal_cmd_rx` is dropped.
                let result =
                    internal_cmd_tx.send(Message::SinkConnectionReady(SinkConnectionReady {
                        ctx: Some(ctx),
                        id,
                        oid,
                        create_export_token,
                        result: mz_storage_client::sink::build_sink_connection(
                            connection_builder,
                            connection_context,
                        )
                        .await
                        .map_err(Into::into),
                    }));
                if let Err(e) = result {
                    warn!("internal_cmd_rx dropped before we could send: {:?}", e);
                }
            },
        );
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
    fn validate_system_column_references(
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

    #[tracing::instrument(level = "debug", skip(self))]
    pub(super) async fn sequence_create_view(
        &mut self,
        session: &mut Session,
        plan: CreateViewPlan,
        resolved_ids: ResolvedIds,
    ) -> Result<ExecuteResponse, AdapterError> {
        let if_not_exists = plan.if_not_exists;
        let ops = self.generate_view_ops(session, &plan, resolved_ids).await?;
        match self.catalog_transact(Some(session), ops).await {
            Ok(()) => Ok(ExecuteResponse::CreatedView),
            Err(AdapterError::Catalog(catalog::Error {
                kind: catalog::ErrorKind::ItemAlreadyExists(_, _),
                ..
            })) if if_not_exists => {
                session.add_notice(AdapterNotice::ObjectAlreadyExists {
                    name: plan.name.item,
                    ty: "view",
                });
                Ok(ExecuteResponse::CreatedView)
            }
            Err(err) => Err(err),
        }
    }

    async fn generate_view_ops(
        &mut self,
        session: &Session,
        CreateViewPlan {
            name,
            view,
            drop_ids,
            ambiguous_columns,
            ..
        }: &CreateViewPlan,
        resolved_ids: ResolvedIds,
    ) -> Result<Vec<catalog::Op>, AdapterError> {
        // Validate any references in the view's expression. We do this on the
        // unoptimized plan to better reflect what the user typed. We want to
        // reject queries that depend on a relation in the wrong timeline, for
        // example, even if we can *technically* optimize that reference away.
        let depends_on = view.expr.depends_on();
        self.validate_timeline_context(depends_on.iter().copied())?;
        self.validate_system_column_references(*ambiguous_columns, &depends_on)?;

        let mut ops = vec![];

        ops.extend(
            drop_ids
                .iter()
                .map(|id| catalog::Op::DropObject(ObjectId::Item(*id))),
        );
        let view_id = self.catalog_mut().allocate_user_id().await?;
        let view_oid = self.catalog_mut().allocate_oid()?;
        let optimized_expr = self.view_optimizer.optimize(view.expr.clone())?;
        let desc = RelationDesc::new(optimized_expr.typ(), view.column_names.clone());
        let view = catalog::View {
            create_sql: view.create_sql.clone(),
            optimized_expr,
            desc,
            conn_id: if view.temporary {
                Some(session.conn_id().clone())
            } else {
                None
            },
            resolved_ids,
        };
        ops.push(catalog::Op::CreateItem {
            id: view_id,
            oid: view_oid,
            name: name.clone(),
            item: CatalogItem::View(view),
            owner_id: *session.current_role_id(),
        });

        Ok(ops)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub(super) async fn sequence_create_materialized_view(
        &mut self,
        session: &mut Session,
        plan: CreateMaterializedViewPlan,
        resolved_ids: ResolvedIds,
    ) -> Result<ExecuteResponse, AdapterError> {
        let CreateMaterializedViewPlan {
            name,
            materialized_view:
                MaterializedView {
                    create_sql,
                    expr: view_expr,
                    column_names,
                    cluster_id,
                },
            replace: _,
            drop_ids,
            if_not_exists,
            ambiguous_columns,
        } = plan;

        if !self
            .catalog()
            .state()
            .is_system_schema_specifier(&name.qualifiers.schema_spec)
            && !self.is_compute_cluster(cluster_id)
        {
            let cluster_name = self.catalog().get_cluster(cluster_id).name.clone();
            return Err(AdapterError::BadItemInStorageCluster { cluster_name });
        }

        // Validate any references in the materialized view's expression. We do
        // this on the unoptimized plan to better reflect what the user typed.
        // We want to reject queries that depend on log sources, for example,
        // even if we can *technically* optimize that reference away.
        let expr_depends_on = view_expr.depends_on();
        self.validate_timeline_context(expr_depends_on.iter().cloned())?;
        self.validate_system_column_references(ambiguous_columns, &expr_depends_on)?;
        // Materialized views are not allowed to depend on log sources, as replicas
        // are not producing the same definite collection for these.
        // TODO(teskje): Remove this check once arrangement-based log sources
        // are replaced with persist-based ones.
        let log_names = expr_depends_on
            .iter()
            .flat_map(|id| self.catalog().introspection_dependencies(*id))
            .map(|id| self.catalog().get_entry(&id).name().item.clone())
            .collect::<Vec<_>>();
        if !log_names.is_empty() {
            return Err(AdapterError::InvalidLogDependency {
                object_type: "materialized view".into(),
                log_names,
            });
        }

        // Allocate IDs for the materialized view in the catalog.
        let id = self.catalog_mut().allocate_user_id().await?;
        let oid = self.catalog_mut().allocate_oid()?;
        // Allocate a unique ID that can be used by the dataflow builder to
        // connect the view dataflow to the storage sink.
        let internal_view_id = self.allocate_transient_id()?;

        let optimized_expr = self.view_optimizer.optimize(view_expr)?;
        let desc = RelationDesc::new(optimized_expr.typ(), column_names);

        // Pick the least valid read timestamp as the as-of for the view
        // dataflow. This makes the materialized view include the maximum possible
        // amount of historical detail.
        let id_bundle = self
            .index_oracle(cluster_id)
            .sufficient_collections(&expr_depends_on);
        let as_of = self.least_valid_read(&id_bundle);

        let mut ops = Vec::new();
        ops.extend(
            drop_ids
                .into_iter()
                .map(|id| catalog::Op::DropObject(ObjectId::Item(id))),
        );
        ops.push(catalog::Op::CreateItem {
            id,
            oid,
            name: name.clone(),
            item: CatalogItem::MaterializedView(catalog::MaterializedView {
                create_sql,
                optimized_expr,
                desc: desc.clone(),
                resolved_ids,
                cluster_id,
            }),
            owner_id: *session.current_role_id(),
        });

        match self
            .catalog_transact_with(Some(session), ops, |txn| {
                // Create a dataflow that materializes the view query and sinks
                // it to storage.
                let df = txn
                    .dataflow_builder(cluster_id)
                    .build_materialized_view_dataflow(id, internal_view_id)?;
                Ok(df)
            })
            .await
        {
            Ok(mut df) => {
                // Announce the creation of the materialized view source.
                self.controller
                    .storage
                    .create_collections(vec![(
                        id,
                        CollectionDescription {
                            desc,
                            data_source: DataSource::Other,
                            since: Some(as_of.clone()),
                            status_collection_id: None,
                        },
                    )])
                    .await
                    .unwrap_or_terminate("cannot fail to append");

                self.initialize_storage_read_policies(
                    vec![id],
                    Some(DEFAULT_LOGICAL_COMPACTION_WINDOW_TS),
                )
                .await;

                df.set_as_of(as_of);
                self.must_ship_dataflow(df, cluster_id).await;

                Ok(ExecuteResponse::CreatedMaterializedView)
            }
            Err(AdapterError::Catalog(catalog::Error {
                kind: catalog::ErrorKind::ItemAlreadyExists(_, _),
                ..
            })) if if_not_exists => {
                session.add_notice(AdapterNotice::ObjectAlreadyExists {
                    name: name.item,
                    ty: "materialized view",
                });
                Ok(ExecuteResponse::CreatedMaterializedView)
            }
            Err(err) => Err(err),
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub(super) async fn sequence_create_index(
        &mut self,
        session: &mut Session,
        plan: CreateIndexPlan,
        resolved_ids: ResolvedIds,
    ) -> Result<ExecuteResponse, AdapterError> {
        let CreateIndexPlan {
            name,
            index,
            options,
            if_not_exists,
        } = plan;

        // An index must be created on a specific cluster.
        let cluster_id = index.cluster_id;

        if !self
            .catalog()
            .state()
            .is_system_schema_specifier(&name.qualifiers.schema_spec)
            && !self.is_compute_cluster(cluster_id)
        {
            let cluster_name = self.catalog().get_cluster(cluster_id).name.clone();
            return Err(AdapterError::BadItemInStorageCluster { cluster_name });
        }

        let id = self.catalog_mut().allocate_user_id().await?;
        let index = catalog::Index {
            create_sql: index.create_sql,
            keys: index.keys,
            on: index.on,
            conn_id: None,
            resolved_ids,
            cluster_id,
            is_retained_metrics_object: false,
            custom_logical_compaction_window: None,
        };
        let oid = self.catalog_mut().allocate_oid()?;
        let on = self.catalog().get_entry(&index.on);
        // Indexes have the same owner as their parent relation.
        let owner_id = *on.owner_id();
        let op = catalog::Op::CreateItem {
            id,
            oid,
            name: name.clone(),
            item: CatalogItem::Index(index),
            owner_id,
        };
        match self
            .catalog_transact_with(Some(session), vec![op], |txn| {
                let mut builder = txn.dataflow_builder(cluster_id);
                let df = builder.build_index_dataflow(id)?;
                Ok(df)
            })
            .await
        {
            Ok(df) => {
                self.must_ship_dataflow(df, cluster_id).await;
                self.set_index_options(id, options).expect("index enabled");
                Ok(ExecuteResponse::CreatedIndex)
            }
            Err(AdapterError::Catalog(catalog::Error {
                kind: catalog::ErrorKind::ItemAlreadyExists(_, _),
                ..
            })) if if_not_exists => {
                session.add_notice(AdapterNotice::ObjectAlreadyExists {
                    name: name.item,
                    ty: "index",
                });
                Ok(ExecuteResponse::CreatedIndex)
            }
            Err(err) => Err(err),
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub(super) async fn sequence_create_type(
        &mut self,
        session: &Session,
        plan: CreateTypePlan,
        resolved_ids: ResolvedIds,
    ) -> Result<ExecuteResponse, AdapterError> {
        let typ = catalog::Type {
            create_sql: plan.typ.create_sql,
            details: CatalogTypeDetails {
                array_id: None,
                typ: plan.typ.inner,
            },
            resolved_ids,
        };
        let id = self.catalog_mut().allocate_user_id().await?;
        let oid = self.catalog_mut().allocate_oid()?;
        let op = catalog::Op::CreateItem {
            id,
            oid,
            name: plan.name,
            item: CatalogItem::Type(typ),
            owner_id: *session.current_role_id(),
        };
        match self.catalog_transact(Some(session), vec![op]).await {
            Ok(()) => Ok(ExecuteResponse::CreatedType),
            Err(err) => Err(err),
        }
    }

    pub(super) async fn sequence_drop_objects(
        &mut self,
        session: &mut Session,
        DropObjectsPlan {
            drop_ids,
            object_type,
            referenced_ids: _,
        }: DropObjectsPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let DropOps {
            ops,
            dropped_active_db,
            dropped_active_cluster,
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
                        ErrorMessageObjectDescription::from_id(object_id, catalog);
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
                        ErrorMessageObjectDescription::from_id(object_id, catalog);
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
                let object_description = ErrorMessageObjectDescription::from_id(&id, &catalog);
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
                    ErrorMessageObjectDescription::from_id(&database_id, &catalog);
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
                        ErrorMessageObjectDescription::from_id(&schema_id, &catalog);
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
                    ErrorMessageObjectDescription::from_id(&cluster_id, &catalog);
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
            for replica in cluster.replicas_by_id.values() {
                if let Some(role_name) = dropped_roles.get(&replica.owner_id) {
                    let replica_id =
                        SystemObjectId::Object((replica.cluster_id(), replica.replica_id()).into());
                    let object_description =
                        ErrorMessageObjectDescription::from_id(&replica_id, &catalog);
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

    pub(super) async fn sequence_drop_owned(
        &mut self,
        session: &mut Session,
        plan: DropOwnedPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        for role_id in &plan.role_ids {
            self.catalog().ensure_not_reserved_role(role_id)?;
        }

        let mut privilege_revokes = plan.privilege_revokes;

        // Make sure this stays in sync with the beginning of `rbac::check_plan`.
        let session_catalog = self.catalog().for_session(session);
        if is_rbac_enabled_for_session(session_catalog.system_vars(), session)
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
                    ErrorMessageObjectDescription::from_id(&invalid_revoke, &session_catalog);
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
        Ok(ExecuteResponse::DroppedOwned)
    }

    fn sequence_drop_common(
        &self,
        session: &mut Session,
        ids: Vec<ObjectId>,
    ) -> Result<DropOps, AdapterError> {
        let mut dropped_active_db = false;
        let mut dropped_active_cluster = false;
        // Dropping either the group role or the member role of a role membership will trigger a
        // revoke role. We use a Set for the revokes to avoid trying to attempt to revoke the same
        // role membership twice.
        let mut revokes = BTreeSet::new();

        let mut dropped_roles: BTreeMap<_, _> = ids
            .iter()
            .filter_map(|id| match id {
                ObjectId::Role(role_id) => Some(role_id),
                _ => None,
            })
            .map(|id| {
                let name = self.catalog().get_role(id).name();
                (*id, name)
            })
            .collect();
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
                revokes.insert((
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

        for id in &ids {
            match id {
                ObjectId::Database(id) => {
                    let name = self.catalog().get_database(id).name();
                    if name == session.vars().database() {
                        dropped_active_db = true;
                    }
                }
                ObjectId::Cluster(id) => {
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
                        revokes.insert((*group_id, *id, *grantor_id));
                    }
                }
                _ => {}
            }
        }

        let ops = revokes
            .into_iter()
            .map(|(role_id, member_id, grantor_id)| catalog::Op::RevokeRole {
                role_id,
                member_id,
                grantor_id,
            })
            .chain(ids.into_iter().map(catalog::Op::DropObject))
            .collect();

        Ok(DropOps {
            ops,
            dropped_active_db,
            dropped_active_cluster,
        })
    }

    pub(super) fn sequence_show_all_variables(
        &mut self,
        session: &Session,
    ) -> Result<ExecuteResponse, AdapterError> {
        Ok(send_immediate_rows(
            viewable_variables(self.catalog().state(), session)
                .map(|v| {
                    Row::pack_slice(&[
                        Datum::String(v.name()),
                        Datum::String(&v.value()),
                        Datum::String(v.description()),
                    ])
                })
                .collect(),
        ))
    }

    pub(super) fn sequence_show_variable(
        &self,
        session: &Session,
        plan: ShowVariablePlan,
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
                    Ok(send_immediate_rows(vec![row]))
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
                    Ok(send_immediate_rows(vec![Row::pack_slice(&[Datum::Null])]))
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
        if variable.name() == DATABASE_VAR_NAME
            && matches!(
                self.catalog().resolve_database(&variable.value()),
                Err(CatalogError::UnknownDatabase(_))
            )
        {
            let name = variable.value();
            session.add_notice(AdapterNotice::DatabaseDoesNotExist { name });
        } else if variable.name() == CLUSTER_VAR_NAME
            && matches!(
                self.catalog().resolve_cluster(&variable.value()),
                Err(CatalogError::UnknownCluster(_))
            )
        {
            let name = variable.value();
            session.add_notice(AdapterNotice::ClusterDoesNotExist { name });
        }
        Ok(send_immediate_rows(vec![row]))
    }

    pub(super) async fn sequence_inspect_shard(
        &self,
        session: &Session,
        plan: InspectShardPlan,
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
        Ok(send_immediate_rows(vec![jsonb.into_row()]))
    }

    pub(super) fn sequence_set_variable(
        &self,
        session: &mut Session,
        plan: SetVariablePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let (name, local) = (plan.name, plan.local);
        if &name == TRANSACTION_ISOLATION_VAR_NAME {
            self.validate_set_isolation_level(session)?;
        }

        let vars = session.vars_mut();
        let values = match plan.value {
            VariableValue::Default => None,
            VariableValue::Values(values) => Some(values),
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
                if name.as_str() == DATABASE_VAR_NAME
                    && matches!(
                        self.catalog().resolve_database(vars.database()),
                        Err(CatalogError::UnknownDatabase(_))
                    )
                {
                    let name = vars.database().to_string();
                    session.add_notice(AdapterNotice::DatabaseDoesNotExist { name });
                } else if name.as_str() == CLUSTER_VAR_NAME
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
        plan: ResetVariablePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let name = plan.name;
        if &name == TRANSACTION_ISOLATION_VAR_NAME {
            self.validate_set_isolation_level(session)?;
        }
        session
            .vars_mut()
            .reset(Some(self.catalog().system_config()), &name, false)?;
        Ok(ExecuteResponse::SetVariable { name, reset: true })
    }

    pub(super) fn sequence_set_transaction(
        &self,
        session: &mut Session,
        plan: SetTransactionPlan,
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
                        TRANSACTION_ISOLATION_VAR_NAME.as_str(),
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

    pub(super) fn sequence_end_transaction(
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

        let result = self.sequence_end_transaction_inner(ctx.session_mut(), action);

        let (response, action) = match result {
            Ok((Some(TransactionOps::Writes(writes)), _)) if writes.is_empty() => {
                (response, action)
            }
            Ok((Some(TransactionOps::Writes(writes)), write_lock_guard)) => {
                self.submit_write(PendingWriteTxn::User {
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
            Ok((Some(TransactionOps::Peeks(determination)), _))
                if ctx.session().vars().transaction_isolation()
                    == &IsolationLevel::StrictSerializable =>
            {
                self.strict_serializable_reads_tx
                    .send(PendingReadTxn {
                        txn: PendingRead::Read {
                            txn: PendingTxn {
                                ctx,
                                response,
                                action,
                            },
                            timestamp_context: determination.timestamp_context,
                        },
                        created: Instant::now(),
                        num_requeues: 0,
                    })
                    .expect("sending to strict_serializable_reads_tx cannot fail");
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

    fn sequence_end_transaction_inner(
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
        let txn = self.clear_transaction(session);

        if let EndTransactionAction::Commit = action {
            if let (Some(mut ops), write_lock_guard) = txn.into_ops_and_lock_guard() {
                if let TransactionOps::Writes(writes) = &mut ops {
                    for WriteOp { id, .. } in &mut writes.iter() {
                        // Re-verify this id exists.
                        let _ = self.catalog().try_get_entry(id).ok_or_else(|| {
                            AdapterError::SqlCatalog(CatalogError::UnknownItem(id.to_string()))
                        })?;
                    }

                    // `rows` can be empty if, say, a DELETE's WHERE clause had 0 results.
                    writes.retain(|WriteOp { rows, .. }| !rows.is_empty());
                }
                return Ok((Some(ops), write_lock_guard));
            }
        }

        Ok((None, None))
    }

    pub(super) fn sequence_side_effecting_func(
        &mut self,
        plan: SideEffectingFunc,
    ) -> Result<ExecuteResponse, AdapterError> {
        match plan {
            SideEffectingFunc::PgCancelBackend { connection_id } => {
                let res = if let Some((id_handle, _conn_meta)) =
                    self.active_conns.get_key_value(&connection_id)
                {
                    // check_plan already verified role membership.
                    self.handle_privileged_cancel(id_handle.clone());
                    Datum::True
                } else {
                    Datum::False
                };
                Ok(send_immediate_rows(vec![Row::pack_slice(&[res])]))
            }
        }
    }

    /// Sequence a peek, determining a timestamp and the most efficient dataflow interaction.
    ///
    /// Peeks are sequenced by assigning a timestamp for evaluation, and then determining and
    /// deploying the most efficient evaluation plan. The peek could evaluate to a constant,
    /// be a simple read out of an existing arrangement, or required a new dataflow to build
    /// the results to return.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(super) async fn sequence_peek(
        &mut self,
        ctx: ExecuteContext,
        plan: SelectPlan,
        target_cluster: TargetCluster,
    ) {
        event!(Level::TRACE, plan = format!("{:?}", plan));

        self.sequence_peek_stage(
            ctx,
            PeekStage::Validate(PeekStageValidate {
                plan,
                target_cluster,
            }),
        )
        .await;
    }

    /// Processes as many peek stages as possible.
    pub(crate) async fn sequence_peek_stage(
        &mut self,
        mut ctx: ExecuteContext,
        mut stage: PeekStage,
    ) {
        // Process the current stage and allow for processing the next.
        loop {
            event!(Level::TRACE, stage = format!("{:?}", stage));

            // Always verify peek validity. This is cheap, and prevents programming errors
            // if we move any stages off thread.
            if let Some(validity) = stage.validity() {
                if let Err(err) = validity.check(self.catalog()) {
                    ctx.retire(Err(err));
                    return;
                }
            }

            (ctx, stage) = match stage {
                PeekStage::Validate(stage) => {
                    let next =
                        return_if_err!(self.peek_stage_validate(ctx.session_mut(), stage), ctx);
                    (ctx, PeekStage::Optimize(next))
                }
                PeekStage::Optimize(stage) => {
                    let next = return_if_err!(
                        self.peek_stage_optimize(ctx.session_mut(), stage).await,
                        ctx
                    );
                    (ctx, PeekStage::Timestamp(next))
                }
                PeekStage::Timestamp(stage) => match self.peek_stage_timestamp(ctx, stage) {
                    Some((ctx, next)) => (ctx, PeekStage::Finish(next)),
                    None => return,
                },
                PeekStage::Finish(stage) => {
                    let res = self.peek_stage_finish(ctx.session_mut(), stage).await;
                    ctx.retire(res);
                    return;
                }
            }
        }
    }

    // Do some simple validation. We must defer most of it until after any off-thread work.
    fn peek_stage_validate(
        &mut self,
        session: &mut Session,
        PeekStageValidate {
            plan,
            target_cluster,
        }: PeekStageValidate,
    ) -> Result<PeekStageOptimize, AdapterError> {
        let SelectPlan {
            source,
            when,
            finishing,
            copy_to,
        } = plan;

        // Two transient allocations. We could reclaim these if we don't use them, potentially.
        // TODO: reclaim transient identifiers in fast path cases.
        let view_id = self.allocate_transient_id()?;
        let index_id = self.allocate_transient_id()?;
        let catalog = self.catalog();

        let cluster = catalog.resolve_target_cluster(target_cluster, session)?;

        let target_replica_name = session.vars().cluster_replica();
        let mut target_replica = target_replica_name
            .map(|name| {
                cluster.replica_id_by_name.get(name).copied().ok_or(
                    AdapterError::UnknownClusterReplica {
                        cluster_name: cluster.name.clone(),
                        replica_name: name.to_string(),
                    },
                )
            })
            .transpose()?;

        if cluster.replicas_by_id.is_empty() {
            return Err(AdapterError::NoClusterReplicasAvailable(
                cluster.name.clone(),
            ));
        }

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
        let in_immediate_multi_stmt_txn = session.transaction().is_in_multi_statement_transaction()
            && when == QueryWhen::Immediately;

        check_no_invalid_log_reads(catalog, cluster, &source_ids, &mut target_replica)?;

        let validity = PlanValidity {
            transient_revision: catalog.transient_revision(),
            dependency_ids: source_ids.clone(),
            cluster_id: Some(cluster.id()),
            replica_id: target_replica,
        };

        Ok(PeekStageOptimize {
            validity,
            source,
            finishing,
            copy_to,
            view_id,
            index_id,
            source_ids,
            cluster_id: cluster.id(),
            when,
            target_replica,
            timeline_context,
            in_immediate_multi_stmt_txn,
        })
    }

    async fn peek_stage_optimize(
        &mut self,
        session: &Session,
        PeekStageOptimize {
            validity,
            source,
            finishing,
            copy_to,
            view_id,
            index_id,
            source_ids,
            cluster_id,
            when,
            target_replica,
            timeline_context,
            in_immediate_multi_stmt_txn,
        }: PeekStageOptimize,
    ) -> Result<PeekStageTimestamp, AdapterError> {
        let source = self.view_optimizer.optimize(source)?;

        let id_bundle = self
            .index_oracle(cluster_id)
            .sufficient_collections(&source_ids);

        // We create a dataflow and optimize it, to determine if we can avoid building it.
        // This can happen if the result optimizes to a constant, or to a `Get` expression
        // around a maintained arrangement.
        let typ = source.typ();
        let key: Vec<MirScalarExpr> = typ
            .default_key()
            .iter()
            .map(|k| MirScalarExpr::Column(*k))
            .collect();
        // The assembled dataflow contains a view and an index of that view.
        let mut dataflow = DataflowDesc::new(format!("oneshot-select-{}", view_id));
        let mut builder = self.dataflow_builder(cluster_id);
        builder.import_view_into_dataflow(&view_id, &source, &mut dataflow)?;

        // Resolve all unmaterializable function calls except mz_now(), because we don't yet have a
        // timestamp.
        let style = ExprPrepStyle::OneShot {
            logical_time: EvalTime::Deferred,
            session,
        };
        let state = self.catalog().state();
        dataflow.visit_children(
            |r| prep_relation_expr(state, r, style),
            |s| prep_scalar_expr(state, s, style),
        )?;

        dataflow.export_index(
            index_id,
            IndexDesc {
                on_id: view_id,
                key: key.clone(),
            },
            typ.clone(),
        );

        let query_as_of = self
            .determine_timestamp(
                session,
                &id_bundle,
                &when,
                cluster_id,
                &timeline_context,
                None,
            )?
            .timestamp_context
            .antichain();

        // Optimize the dataflow across views, and any other ways that appeal.
        mz_transform::optimize_dataflow(
            &mut dataflow,
            &builder.index_oracle(),
            self.statistics_oracle(session, &source_ids, query_as_of, true)
                .await?
                .as_ref(),
        )?;

        Ok(PeekStageTimestamp {
            validity,
            dataflow,
            finishing,
            copy_to,
            view_id,
            index_id,
            source_ids,
            cluster_id,
            id_bundle,
            when,
            target_replica,
            timeline_context,
            in_immediate_multi_stmt_txn,
            key,
            typ,
        })
    }

    #[tracing::instrument(level = "debug", skip_all)]
    fn peek_stage_timestamp(
        &mut self,
        ctx: ExecuteContext,
        PeekStageTimestamp {
            validity,
            dataflow,
            finishing,
            copy_to,
            view_id,
            index_id,
            source_ids,
            cluster_id,
            id_bundle,
            when,
            target_replica,
            timeline_context,
            in_immediate_multi_stmt_txn,
            key,
            typ,
        }: PeekStageTimestamp,
    ) -> Option<(ExecuteContext, PeekStageFinish)> {
        match self.recent_timestamp(ctx.session(), source_ids.iter().cloned()) {
            Some(fut) => {
                let internal_cmd_tx = self.internal_cmd_tx.clone();
                let conn_id = ctx.session().conn_id().clone();
                self.pending_real_time_recency_timestamp.insert(
                    conn_id.clone(),
                    RealTimeRecencyContext::Peek {
                        ctx,
                        finishing,
                        copy_to,
                        dataflow,
                        cluster_id,
                        when,
                        target_replica,
                        view_id,
                        index_id,
                        timeline_context,
                        source_ids,
                        in_immediate_multi_stmt_txn,
                        key,
                        typ,
                    },
                );
                task::spawn(|| "real_time_recency_peek", async move {
                    let real_time_recency_ts = fut.await;
                    // It is not an error for these results to be ready after `internal_cmd_rx` has been dropped.
                    let result = internal_cmd_tx.send(Message::RealTimeRecencyTimestamp {
                        conn_id: conn_id.clone(),
                        real_time_recency_ts,
                        validity,
                    });
                    if let Err(e) = result {
                        warn!("internal_cmd_rx dropped before we could send: {:?}", e);
                    }
                });
                None
            }
            None => Some((
                ctx,
                PeekStageFinish {
                    validity,
                    finishing,
                    copy_to,
                    dataflow,
                    cluster_id,
                    id_bundle: Some(id_bundle),
                    when,
                    target_replica,
                    view_id,
                    index_id,
                    timeline_context,
                    source_ids,
                    real_time_recency_ts: None,
                    key,
                    typ,
                },
            )),
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn peek_stage_finish(
        &mut self,
        session: &mut Session,
        PeekStageFinish {
            validity: _,
            finishing,
            copy_to,
            dataflow,
            cluster_id,
            id_bundle,
            when,
            target_replica,
            view_id,
            index_id,
            timeline_context,
            source_ids,
            real_time_recency_ts,
            key,
            typ,
        }: PeekStageFinish,
    ) -> Result<ExecuteResponse, AdapterError> {
        let id_bundle = id_bundle.unwrap_or_else(|| {
            self.index_oracle(cluster_id)
                .sufficient_collections(&source_ids)
        });
        let peek_plan = self.plan_peek(
            dataflow,
            session,
            &when,
            cluster_id,
            view_id,
            index_id,
            timeline_context,
            source_ids,
            &id_bundle,
            real_time_recency_ts,
            key,
            typ,
        )?;

        let determination = peek_plan.determination.clone();

        // Implement the peek, and capture the response.
        let resp = self
            .implement_peek_plan(peek_plan, finishing, cluster_id, target_replica)
            .await?;

        if session.vars().emit_timestamp_notice() {
            let explanation =
                self.explain_timestamp(session, cluster_id, &id_bundle, determination);
            session.add_notice(AdapterNotice::QueryTimestamp { explanation });
        }

        match copy_to {
            None => Ok(resp),
            Some(format) => Ok(ExecuteResponse::CopyTo {
                format,
                resp: Box::new(resp),
            }),
        }
    }

    /// Determines the query timestamp and acquires read holds on dependent sources
    /// if necessary.
    fn sequence_peek_timestamp(
        &mut self,
        session: &mut Session,
        when: &QueryWhen,
        cluster_id: ClusterId,
        timeline_context: TimelineContext,
        source_bundle: &CollectionIdBundle,
        source_ids: &BTreeSet<GlobalId>,
        real_time_recency_ts: Option<Timestamp>,
    ) -> Result<TimestampDetermination<Timestamp>, AdapterError> {
        let in_immediate_multi_stmt_txn = session.transaction().in_immediate_multi_stmt_txn(when);
        let timedomain_bundle;

        // Fetch or generate a timestamp for this query and what the read holds would be if we need to set
        // them.
        let (determination, potential_read_holds) =
            match session.get_transaction_timestamp_determination() {
                // Use the transaction's timestamp if it exists and this isn't an AS OF query.
                Some(
                    determination @ TimestampDetermination {
                        timestamp_context: TimestampContext::TimelineTimestamp(_, _),
                        ..
                    },
                ) if in_immediate_multi_stmt_txn => (determination, None),
                _ => {
                    let determine_bundle = if in_immediate_multi_stmt_txn {
                        // In a transaction, determine a timestamp that will be valid for anything in
                        // any schema referenced by the first query.
                        timedomain_bundle = self.timedomain_for(
                            source_ids,
                            &timeline_context,
                            session.conn_id(),
                            cluster_id,
                        )?;
                        &timedomain_bundle
                    } else {
                        // If not in a transaction, use the source.
                        source_bundle
                    };
                    let determination = self.determine_timestamp(
                        session,
                        determine_bundle,
                        when,
                        cluster_id,
                        &timeline_context,
                        real_time_recency_ts,
                    )?;
                    // We only need read holds if the read depends on a timestamp. We don't set the
                    // read holds here because it makes the code a bit more clear to handle the two
                    // cases for "is this the first statement in a transaction?" in an if/else block
                    // below.
                    let read_holds = determination
                        .timestamp_context
                        .timestamp()
                        .map(|timestamp| (timestamp.clone(), determine_bundle));
                    (determination, read_holds)
                }
            };

        // If we're in a multi-statement transaction and the query does not use `AS OF`,
        // acquire read holds on any sources in the current time-domain if they have not
        // already been acquired. If the query does use `AS OF`, it is not necessary to
        // acquire read holds.
        if in_immediate_multi_stmt_txn {
            // Either set the valid read ids for this transaction (if it's the first statement in a
            // transaction) otherwise verify the ids referenced in this query are in the timedomain.
            if let Some(txn_reads) = self.txn_reads.get(session.conn_id()) {
                // Find referenced ids not in the read hold. A reference could be caused by a
                // user specifying an object in a different schema than the first query. An
                // index could be caused by a CREATE INDEX after the transaction started.
                let allowed_id_bundle = txn_reads.id_bundle();
                let outside = source_bundle.difference(&allowed_id_bundle);
                // Queries without a timestamp and timeline can belong to any existing timedomain.
                if determination.timestamp_context.contains_timestamp() && !outside.is_empty() {
                    let valid_names =
                        self.resolve_collection_id_bundle_names(session, &allowed_id_bundle);
                    let invalid_names = self.resolve_collection_id_bundle_names(session, &outside);
                    return Err(AdapterError::RelationOutsideTimeDomain {
                        relations: invalid_names,
                        names: valid_names,
                    });
                }
            } else {
                if let Some((timestamp, bundle)) = potential_read_holds {
                    let read_holds = self.acquire_read_holds(timestamp, bundle);
                    self.txn_reads.insert(session.conn_id().clone(), read_holds);
                }
            }
        }

        // TODO: Checking for only `InTransaction` and not `Implied` (also `Started`?) seems
        // arbitrary and we don't recall why we did it (possibly an error!). Change this to always
        // set the transaction ops. Decide and document what our policy should be on AS OF queries.
        // Maybe they shouldn't be allowed in transactions at all because it's hard to explain
        // what's going on there. This should probably get a small design document.

        // We only track the peeks in the session if the query doesn't use AS
        // OF or we're inside an explicit transaction. The latter case is
        // necessary to support PG's `BEGIN` semantics, whose behavior can
        // depend on whether or not reads have occurred in the txn.
        let mut transaction_determination = determination.clone();
        if when.is_transactional() {
            session.add_transaction_ops(TransactionOps::Peeks(transaction_determination))?;
        } else if matches!(session.transaction(), &TransactionStatus::InTransaction(_)) {
            // If the query uses AS OF, then ignore the timestamp.
            transaction_determination.timestamp_context = TimestampContext::NoTimestamp;
            session.add_transaction_ops(TransactionOps::Peeks(transaction_determination))?;
        };

        Ok(determination)
    }

    fn plan_peek(
        &mut self,
        mut dataflow: DataflowDescription<OptimizedMirRelationExpr>,
        session: &mut Session,
        when: &QueryWhen,
        cluster_id: ClusterId,
        view_id: GlobalId,
        index_id: GlobalId,
        timeline_context: TimelineContext,
        source_ids: BTreeSet<GlobalId>,
        id_bundle: &CollectionIdBundle,
        real_time_recency_ts: Option<Timestamp>,
        key: Vec<MirScalarExpr>,
        typ: RelationType,
    ) -> Result<PlannedPeek, AdapterError> {
        let conn_id = session.conn_id().clone();
        let determination = self.sequence_peek_timestamp(
            session,
            when,
            cluster_id,
            timeline_context,
            id_bundle,
            &source_ids,
            real_time_recency_ts,
        )?;

        // Now that we have a timestamp, set the as of and resolve calls to mz_now().
        dataflow.set_as_of(determination.timestamp_context.antichain());
        let style = ExprPrepStyle::OneShot {
            logical_time: EvalTime::Time(determination.timestamp_context.timestamp_or_default()),
            session,
        };
        let state = self.catalog().state();
        dataflow.visit_children(
            |r| prep_relation_expr(state, r, style),
            |s| prep_scalar_expr(state, s, style),
        )?;

        let (permutation, thinning) = permutation_for_arrangement(&key, typ.arity());

        // At this point, `dataflow_plan` contains our best optimized dataflow.
        // We will check the plan to see if there is a fast path to escape full dataflow construction.
        let peek_plan = self.create_peek_plan(
            dataflow,
            view_id,
            cluster_id,
            index_id,
            key,
            permutation,
            thinning.len(),
        )?;

        Ok(PlannedPeek {
            plan: peek_plan,
            determination,
            conn_id,
            source_arity: typ.arity(),
            source_ids,
        })
    }

    /// Checks to see if the session needs a real time recency timestamp and if so returns
    /// a future that will return the timestamp.
    fn recent_timestamp(
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

    pub(super) async fn sequence_subscribe(
        &mut self,
        session: &mut Session,
        plan: SubscribePlan,
        target_cluster: TargetCluster,
    ) -> Result<ExecuteResponse, AdapterError> {
        let SubscribePlan {
            from,
            with_snapshot,
            when,
            copy_to,
            emit_progress,
            up_to,
            output,
        } = plan;

        let cluster = self
            .catalog()
            .resolve_target_cluster(target_cluster, session)?;
        let cluster_id = cluster.id;

        let target_replica_name = session.vars().cluster_replica();
        let mut target_replica = target_replica_name
            .map(|name| {
                cluster.replica_id_by_name.get(name).copied().ok_or(
                    AdapterError::UnknownClusterReplica {
                        cluster_name: cluster.name.clone(),
                        replica_name: name.to_string(),
                    },
                )
            })
            .transpose()?;

        // SUBSCRIBE AS OF, similar to peeks, doesn't need to worry about transaction
        // timestamp semantics.
        if when == QueryWhen::Immediately {
            // If this isn't a SUBSCRIBE AS OF, the SUBSCRIBE can be in a transaction if it's the
            // only operation.
            session.add_transaction_ops(TransactionOps::Subscribe)?;
        }

        // Determine the frontier of updates to subscribe *from*.
        // Updates greater or equal to this frontier will be produced.
        let depends_on = from.depends_on();
        check_no_invalid_log_reads(self.catalog(), cluster, &depends_on, &mut target_replica)?;
        let id_bundle = self
            .index_oracle(cluster_id)
            .sufficient_collections(&depends_on);
        let timeline = self.validate_timeline_context(id_bundle.iter())?;
        let as_of = self
            .determine_timestamp(session, &id_bundle, &when, cluster_id, &timeline, None)?
            .timestamp_context
            .timestamp_or_default();

        let make_sink_desc = |coord: &mut Coordinator, session: &mut Session, from, from_desc| {
            let up_to = up_to
                .map(|expr| Coordinator::evaluate_when(coord.catalog().state(), expr, session))
                .transpose()?;
            if let Some(up_to) = up_to {
                if as_of == up_to {
                    session.add_notice(AdapterNotice::EqualSubscribeBounds { bound: up_to });
                } else if as_of > up_to {
                    return Err(AdapterError::AbsurdSubscribeBounds { as_of, up_to });
                }
            }
            let up_to = up_to.map(Antichain::from_elem).unwrap_or_default();
            Ok::<_, AdapterError>(ComputeSinkDesc {
                from,
                from_desc,
                connection: ComputeSinkConnection::Subscribe(SubscribeSinkConnection::default()),
                with_snapshot,
                up_to,
            })
        };

        let mut dataflow = match from {
            SubscribeFrom::Id(from_id) => {
                let from = self.catalog().get_entry(&from_id);
                let from_desc = from
                    .desc(
                        &self
                            .catalog()
                            .resolve_full_name(from.name(), Some(session.conn_id())),
                    )
                    .expect("subscribes can only be run on items with descs")
                    .into_owned();
                let sink_id = self.allocate_transient_id()?;
                let sink_desc = make_sink_desc(self, session, from_id, from_desc)?;
                let sink_name = format!("subscribe-{}", sink_id);
                self.dataflow_builder(cluster_id)
                    .build_sink_dataflow(sink_name, sink_id, sink_desc)?
            }
            SubscribeFrom::Query { expr, desc } => {
                let id = self.allocate_transient_id()?;
                let expr = self.view_optimizer.optimize(expr)?;
                let desc = RelationDesc::new(expr.typ(), desc.iter_names());
                let sink_desc = make_sink_desc(self, session, id, desc)?;
                let mut dataflow = DataflowDesc::new(format!("subscribe-{}", id));
                let mut dataflow_builder = self.dataflow_builder(cluster_id);
                dataflow_builder.import_view_into_dataflow(&id, &expr, &mut dataflow)?;
                dataflow_builder.build_sink_dataflow_into(&mut dataflow, id, sink_desc)?;
                dataflow
            }
        };

        dataflow.set_as_of(Antichain::from_elem(as_of));

        let (&sink_id, sink_desc) = dataflow
            .sink_exports
            .iter()
            .next()
            .expect("subscribes have a single sink export");
        let (tx, rx) = mpsc::unbounded_channel();
        let active_subscribe = ActiveSubscribe {
            user: session.user().clone(),
            conn_id: session.conn_id().clone(),
            channel: tx,
            emit_progress,
            as_of,
            arity: sink_desc.from_desc.arity(),
            cluster_id,
            depends_on: depends_on.into_iter().collect(),
            start_time: self.now(),
            dropping: false,
            output,
        };
        active_subscribe.initialize();
        self.add_active_subscribe(sink_id, active_subscribe).await;

        match self.ship_dataflow(dataflow, cluster_id).await {
            Ok(_) => {}
            Err(e) => {
                self.remove_active_subscribe(sink_id).await;
                return Err(e);
            }
        };
        if let Some(target) = target_replica {
            self.controller
                .compute
                .set_subscribe_target_replica(cluster_id, sink_id, target)
                .unwrap_or_terminate("cannot fail to set subscribe target replica");
        }

        self.active_conns
            .get_mut(session.conn_id())
            .expect("must exist for active sessions")
            .drop_sinks
            .push(ComputeSinkId {
                cluster_id,
                global_id: sink_id,
            });

        let resp = ExecuteResponse::Subscribing { rx };
        match copy_to {
            None => Ok(resp),
            Some(format) => Ok(ExecuteResponse::CopyTo {
                format,
                resp: Box::new(resp),
            }),
        }
    }

    pub(super) async fn sequence_explain(
        &mut self,
        mut ctx: ExecuteContext,
        plan: ExplainPlan,
        target_cluster: TargetCluster,
    ) {
        match plan.stage {
            ExplainStage::Timestamp => self.sequence_explain_timestamp_begin(ctx, plan),
            _ => {
                let result = self
                    .sequence_explain_plan(ctx.session_mut(), plan, target_cluster)
                    .await;
                ctx.retire(result);
            }
        }
    }

    async fn sequence_explain_plan(
        &mut self,
        session: &mut Session,
        plan: ExplainPlan,
        target_cluster: TargetCluster,
    ) -> Result<ExecuteResponse, AdapterError> {
        use ExplainStage::*;

        let ExplainPlan {
            raw_plan,
            row_set_finishing,
            stage,
            format,
            config,
            no_errors,
            explainee,
        } = plan;

        let cluster_id = {
            let catalog = self.catalog();
            let cluster = match explainee {
                Explainee::Dataflow(_) => catalog.active_cluster(session)?,
                Explainee::Query => catalog.resolve_target_cluster(target_cluster, session)?,
            };
            cluster.id
        };

        assert_ne!(stage, ExplainStage::Timestamp);

        let optimizer_trace = match stage {
            Trace => OptimizerTrace::new(), // collect all trace entries
            stage => OptimizerTrace::find(stage.path()), // collect a trace entry only the selected stage
        };

        let pipeline_result = {
            self.sequence_explain_plan_pipeline(explainee, raw_plan, no_errors, cluster_id, session)
                .with_subscriber(&optimizer_trace)
                .await
        };

        let (used_indexes, fast_path_plan) = match pipeline_result {
            Ok((used_indexes, fast_path_plan)) => (used_indexes, fast_path_plan),
            Err(err) => {
                if no_errors {
                    tracing::error!("error while handling EXPLAIN statement: {}", err);

                    let used_indexes: Vec<GlobalId> = vec![];
                    let fast_path_plan: Option<FastPathPlan> = None;

                    (used_indexes, fast_path_plan)
                } else {
                    return Err(err);
                }
            }
        };

        let trace = optimizer_trace.drain_all(
            format,
            config,
            self.catalog().for_session(session),
            row_set_finishing,
            used_indexes,
            fast_path_plan,
        )?;

        let rows = match stage {
            Trace => {
                // For the `Trace` (pseudo-)stage, return the entire trace as (time,
                // path, plan) triples.
                let rows = trace
                    .into_iter()
                    .map(|entry| {
                        // The trace would have to take over 584 years to overflow a u64.
                        let span_duration =
                            u64::try_from(entry.span_duration.as_nanos()).unwrap_or(u64::MAX);
                        Row::pack_slice(&[
                            Datum::from(span_duration),
                            Datum::from(entry.path.as_str()),
                            Datum::from(entry.plan.as_str()),
                        ])
                    })
                    .collect();
                rows
            }
            stage => {
                // For everything else, return the plan for the stage identified
                // by the corresponding path.
                let row = trace
                    .into_iter()
                    .find(|entry| entry.path == stage.path())
                    .map(|entry| Row::pack_slice(&[Datum::from(entry.plan.as_str())]))
                    .ok_or_else(|| {
                        AdapterError::Internal(format!(
                            "stage `{}` not present in the collected optimizer trace",
                            stage.path(),
                        ))
                    })?;
                vec![row]
            }
        };

        Ok(send_immediate_rows(rows))
    }

    #[tracing::instrument(level = "info", name = "optimize", skip_all)]
    async fn sequence_explain_plan_pipeline(
        &mut self,
        explainee: Explainee,
        raw_plan: mz_sql::plan::HirRelationExpr,
        no_errors: bool,
        cluster_id: mz_storage_client::types::instances::StorageInstanceId,
        session: &mut Session,
    ) -> Result<(Vec<GlobalId>, Option<FastPathPlan>), AdapterError> {
        use mz_repr::explain::trace_plan;

        /// Like [`mz_ore::panic::catch_unwind`], with an extra guard that must be true
        /// in order to wrap the function call in a [`mz_ore::panic::catch_unwind`] call.
        fn catch_unwind<R, E, F>(guard: bool, stage: &'static str, f: F) -> Result<R, AdapterError>
        where
            F: FnOnce() -> Result<R, E>,
            E: Into<AdapterError>,
        {
            if guard {
                let r: Result<Result<R, E>, _> = mz_ore::panic::catch_unwind(AssertUnwindSafe(f));
                match r {
                    Ok(result) => result.map_err(Into::into),
                    Err(_) => {
                        let msg = format!("panic at the `{}` optimization stage", stage);
                        Err(AdapterError::Internal(msg))
                    }
                }
            } else {
                f().map_err(Into::into)
            }
        }

        let (explainee_id, is_oneshot) = match explainee {
            Explainee::Dataflow(id) => (id, false),
            Explainee::Query => (GlobalId::Explain, true),
        };

        // Execute the various stages of the optimization pipeline
        // -------------------------------------------------------

        // Trace the pipeline input under `optimize/raw`.
        tracing::span!(Level::INFO, "raw").in_scope(|| {
            trace_plan(&raw_plan);
        });

        // Execute the `optimize/hir_to_mir` stage.
        let decorrelated_plan = catch_unwind(no_errors, "hir_to_mir", || {
            raw_plan.optimize_and_lower(&OptimizerConfig {})
        })?;

        let mut timeline_context =
            self.validate_timeline_context(decorrelated_plan.depends_on())?;
        if matches!(explainee, Explainee::Query)
            && matches!(timeline_context, TimelineContext::TimestampIndependent)
            && decorrelated_plan.contains_temporal()
        {
            // If the source IDs are timestamp independent but the query contains temporal functions,
            // then the timeline context needs to be upgraded to timestamp dependent. This is
            // required because `source_ids` doesn't contain functions.
            timeline_context = TimelineContext::TimestampDependent;
        }

        let source_ids = decorrelated_plan.depends_on();
        let id_bundle = self
            .index_oracle(cluster_id)
            .sufficient_collections(&source_ids);

        // Execute the `optimize/local` stage.
        let optimized_plan = catch_unwind(no_errors, "local", || {
            tracing::span!(Level::INFO, "local").in_scope(|| -> Result<_, AdapterError> {
                let optimized_plan = self.view_optimizer.optimize(decorrelated_plan);
                if let Ok(ref optimized_plan) = optimized_plan {
                    trace_plan(optimized_plan.as_inner());
                }
                optimized_plan.map_err(Into::into)
            })
        })?;

        let mut dataflow = DataflowDesc::new("explanation".to_string());
        let mut builder = self.dataflow_builder(cluster_id);
        builder.import_view_into_dataflow(&explainee_id, &optimized_plan, &mut dataflow)?;

        // Resolve all unmaterializable function calls except mz_now(), because we don't yet have a
        // timestamp.
        let style = ExprPrepStyle::OneShot {
            logical_time: EvalTime::Deferred,
            session,
        };
        let state = self.catalog().state();
        dataflow.visit_children(
            |r| prep_relation_expr(state, r, style),
            |s| prep_scalar_expr(state, s, style),
        )?;

        // Acquire a timestamp (necessary for loading statistics).
        let timestamp_context = self
            .sequence_peek_timestamp(
                session,
                &QueryWhen::Immediately,
                cluster_id,
                timeline_context,
                &id_bundle,
                &source_ids,
                None, // no real-time recency
            )?
            .timestamp_context;
        let query_as_of = timestamp_context.antichain();

        // Load cardinality statistics.
        let stats = self
            .statistics_oracle(session, &source_ids, query_as_of.clone(), is_oneshot)
            .await?;

        // Execute the `optimize/global` stage.
        catch_unwind(no_errors, "global", || {
            mz_transform::optimize_dataflow(
                &mut dataflow,
                &self.index_oracle(cluster_id),
                stats.as_ref(),
            )
        })?;

        // Calculate indexes used by the dataflow at this point
        let used_indexes = dataflow
            .index_imports
            .keys()
            .cloned()
            .collect::<Vec<GlobalId>>();

        // Determine if fast path plan will be used for this explainee
        let fast_path_plan = match explainee {
            Explainee::Query => {
                dataflow.set_as_of(query_as_of);
                let style = ExprPrepStyle::OneShot {
                    logical_time: EvalTime::Time(timestamp_context.timestamp_or_default()),
                    session,
                };
                let state = self.catalog().state();
                dataflow.visit_children(
                    |r| prep_relation_expr(state, r, style),
                    |s| prep_scalar_expr(state, s, style),
                )?;
                peek::create_fast_path_plan(&mut dataflow, GlobalId::Explain)?
            }
            _ => None,
        };

        if matches!(explainee, Explainee::Query) {
            // We have the opportunity to name an `until` frontier that will prevent work we needn't perform.
            // By default, `until` will be `Antichain::new()`, which prevents no updates and is safe.
            if let Some(as_of) = dataflow.as_of.as_ref() {
                if !as_of.is_empty() {
                    if let Some(next) = as_of.as_option().and_then(|as_of| as_of.checked_add(1)) {
                        dataflow.until = timely::progress::Antichain::from_elem(next);
                    }
                }
            }
        }

        // Execute the `optimize/finalize_dataflow` stage.
        let dataflow_plan = catch_unwind(no_errors, "finalize_dataflow", || {
            self.finalize_dataflow(dataflow, cluster_id)
        })?;

        // Trace the resulting plan for the top-level `optimize` path.
        trace_plan(&dataflow_plan);

        // Return objects that need to be passed to the `ExplainContext`
        // when rendering explanations for the various trace entries.
        Ok((used_indexes, fast_path_plan))
    }

    fn sequence_explain_timestamp_begin(&mut self, mut ctx: ExecuteContext, plan: ExplainPlan) {
        let (format, source_ids, optimized_plan, cluster_id, id_bundle) = return_if_err!(
            self.sequence_explain_timestamp_begin_inner(ctx.session(), plan),
            ctx
        );
        match self.recent_timestamp(ctx.session(), source_ids.iter().cloned()) {
            Some(fut) => {
                let validity = PlanValidity {
                    transient_revision: self.catalog().transient_revision(),
                    dependency_ids: source_ids,
                    cluster_id: Some(cluster_id),
                    replica_id: None,
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
                let result = self.sequence_explain_timestamp_finish_inner(
                    ctx.session_mut(),
                    format,
                    cluster_id,
                    optimized_plan,
                    id_bundle,
                    None,
                );
                ctx.retire(result);
            }
        }
    }

    fn sequence_explain_timestamp_begin_inner(
        &mut self,
        session: &Session,
        plan: ExplainPlan,
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
        let ExplainPlan {
            raw_plan, format, ..
        } = plan;

        let decorrelated_plan = raw_plan.optimize_and_lower(&OptimizerConfig {})?;
        let optimized_plan = self.view_optimizer.optimize(decorrelated_plan)?;
        let source_ids = optimized_plan.depends_on();
        let cluster = self.catalog().active_cluster(session)?;
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

    pub(super) fn sequence_explain_timestamp_finish_inner(
        &mut self,
        session: &mut Session,
        format: ExplainFormat,
        cluster_id: ClusterId,
        source: OptimizedMirRelationExpr,
        id_bundle: CollectionIdBundle,
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
        let timeline_context = self.validate_timeline_context(source_ids.clone())?;

        let determination = self.sequence_peek_timestamp(
            session,
            &QueryWhen::Immediately,
            cluster_id,
            timeline_context,
            &id_bundle,
            &source_ids,
            real_time_recency_ts,
        )?;
        let explanation = self.explain_timestamp(session, cluster_id, &id_bundle, determination);

        let s = if is_json {
            serde_json::to_string_pretty(&explanation).expect("failed to serialize explanation")
        } else {
            explanation.to_string()
        };
        let rows = vec![Row::pack_slice(&[Datum::from(s.as_str())])];
        Ok(send_immediate_rows(rows))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    fn sequence_send_diffs(
        session: &mut Session,
        mut plan: SendDiffsPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let affected_rows = {
            let mut affected_rows = Diff::from(0);
            let mut all_positive_diffs = true;
            // If all diffs are positive, the number of affected rows is just the
            // sum of all unconsolidated diffs.
            for (_, diff) in plan.updates.iter() {
                if *diff < 0 {
                    all_positive_diffs = false;
                    break;
                }

                affected_rows += diff;
            }

            if !all_positive_diffs {
                // Consolidate rows. This is useful e.g. for an UPDATE where the row
                // doesn't change, and we need to reflect that in the number of
                // affected rows.
                differential_dataflow::consolidation::consolidate(&mut plan.updates);

                affected_rows = 0;
                // With retractions, the number of affected rows is not the number
                // of rows we see, but the sum of the absolute value of their diffs,
                // e.g. if one row is retracted and another is added, the total
                // number of rows affected is 2.
                for (_, diff) in plan.updates.iter() {
                    affected_rows += diff.abs();
                }
            }

            usize::try_from(affected_rows).expect("positive isize must fit")
        };
        event!(
            Level::TRACE,
            affected_rows,
            id = format!("{:?}", plan.id),
            kind = format!("{:?}", plan.kind),
            updates = plan.updates.len(),
            returning = plan.returning.len(),
        );

        session.add_transaction_ops(TransactionOps::Writes(vec![WriteOp {
            id: plan.id,
            rows: plan.updates,
        }]))?;
        if !plan.returning.is_empty() {
            let finishing = RowSetFinishing {
                order_by: Vec::new(),
                limit: None,
                offset: 0,
                project: (0..plan.returning[0].0.iter().count()).collect(),
            };
            return match finishing.finish(plan.returning, plan.max_result_size) {
                Ok(rows) => Ok(send_immediate_rows(rows)),
                Err(e) => Err(AdapterError::ResultSize(e)),
            };
        }
        Ok(match plan.kind {
            MutationKind::Delete => ExecuteResponse::Deleted(affected_rows),
            MutationKind::Insert => ExecuteResponse::Inserted(affected_rows),
            MutationKind::Update => ExecuteResponse::Updated(affected_rows / 2),
        })
    }

    pub(super) async fn sequence_insert(&mut self, mut ctx: ExecuteContext, plan: InsertPlan) {
        let optimized_mir = if let Some(..) = &plan.values.as_const() {
            // We don't perform any optimizations on an expression that is already
            // a constant for writes, as we want to maximize bulk-insert throughput.
            OptimizedMirRelationExpr(plan.values)
        } else {
            return_if_err!(self.view_optimizer.optimize(plan.values), ctx)
        };

        match optimized_mir.into_inner() {
            selection if selection.as_const().is_some() && plan.returning.is_empty() => {
                let catalog = self.owned_catalog();
                mz_ore::task::spawn(|| "coord::sequence_inner", async move {
                    let result = Self::sequence_insert_constant(
                        &catalog,
                        ctx.session_mut(),
                        plan.id,
                        selection,
                    );
                    ctx.retire(result);
                });
            }
            // All non-constant values must be planned as read-then-writes.
            selection => {
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
                        ctx.retire(Err(AdapterError::SqlCatalog(CatalogError::UnknownItem(
                            plan.id.to_string(),
                        ))));
                        return;
                    }
                };

                if selection.contains_temporal() {
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

                let read_then_write_plan = ReadThenWritePlan {
                    id: plan.id,
                    selection,
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

    fn sequence_insert_constant(
        catalog: &Catalog,
        session: &mut Session,
        id: GlobalId,
        constants: MirRelationExpr,
    ) -> Result<ExecuteResponse, AdapterError> {
        // Insert can be queued, so we need to re-verify the id exists.
        let desc = match catalog.try_get_entry(&id) {
            Some(table) => {
                table.desc(&catalog.resolve_full_name(table.name(), Some(session.conn_id())))?
            }
            None => {
                return Err(AdapterError::SqlCatalog(CatalogError::UnknownItem(
                    id.to_string(),
                )))
            }
        };

        match constants.as_const() {
            Some((rows, ..)) => {
                let rows = rows.clone()?;
                for (row, _) in &rows {
                    for (i, datum) in row.iter().enumerate() {
                        desc.constraints_met(i, &datum)?;
                    }
                }
                let diffs_plan = SendDiffsPlan {
                    id,
                    updates: rows,
                    kind: MutationKind::Insert,
                    returning: Vec::new(),
                    max_result_size: catalog.system_config().max_result_size(),
                };
                Self::sequence_send_diffs(session, diffs_plan)
            }
            None => panic!(
                "tried using sequence_insert_constant on non-constant MirRelationExpr {:?}",
                constants
            ),
        }
    }

    pub(super) fn sequence_copy_rows(
        &mut self,
        mut ctx: ExecuteContext,
        id: GlobalId,
        columns: Vec<usize>,
        rows: Vec<Row>,
    ) {
        let catalog = self.owned_catalog();
        mz_ore::task::spawn(|| "coord::sequence_copy_rows", async move {
            let conn_catalog = catalog.for_session(ctx.session());
            let result: Result<_, AdapterError> =
                mz_sql::plan::plan_copy_from(ctx.session().pcx(), &conn_catalog, id, columns, rows)
                    .err_into()
                    .and_then(|values| values.lower().err_into())
                    .and_then(|values| {
                        Optimizer::logical_optimizer(&mz_transform::typecheck::empty_context())
                            .optimize(values)
                            .err_into()
                    })
                    .and_then(|values| {
                        // Copied rows must always be constants.
                        Self::sequence_insert_constant(
                            &catalog,
                            ctx.session_mut(),
                            id,
                            values.into_inner(),
                        )
                    });
            ctx.retire(result);
        });
    }

    /// ReadThenWrite is a plan whose writes depend on the results of a
    /// read. This works by doing a Peek then queuing a SendDiffs. No writes
    /// or read-then-writes can occur between the Peek and SendDiff otherwise a
    /// serializability violation could occur.
    pub(super) async fn sequence_read_then_write(
        &mut self,
        mut ctx: ExecuteContext,
        plan: ReadThenWritePlan,
    ) {
        let mut source_ids = plan.selection.depends_on();
        source_ids.insert(plan.id);
        guard_write_critical_section!(self, ctx, Plan::ReadThenWrite(plan), source_ids);

        let ReadThenWritePlan {
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
                ctx.retire(Err(AdapterError::SqlCatalog(CatalogError::UnknownItem(
                    id.to_string(),
                ))));
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
                                entry.uses().0.is_empty()
                                    || entry
                                        .uses()
                                        .0
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
        let peek_ctx = ExecuteContext::from_parts(
            peek_client_tx,
            self.internal_cmd_tx.clone(),
            session,
            Default::default(),
        );
        self.sequence_peek(
            peek_ctx,
            SelectPlan {
                source: selection,
                when: QueryWhen::Freshest,
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
                }) => (resp, session),
                Ok(Response {
                    result: Err(e),
                    session,
                }) => {
                    let ctx =
                        ExecuteContext::from_parts(tx, internal_cmd_tx.clone(), session, extra);
                    ctx.retire(Err(e));
                    return;
                }
                // It is not an error for these results to be ready after `peek_client_tx` has been dropped.
                Err(e) => return warn!("internal_cmd_rx dropped before we could send: {:?}", e),
            };
            let mut ctx = ExecuteContext::from_parts(tx, internal_cmd_tx.clone(), session, extra);
            let timeout_dur = *ctx.session().vars().statement_timeout();
            let arena = RowArena::new();
            let diffs = match peek_response {
                ExecuteResponse::SendingRows {
                    future: batch,
                    span: _,
                } => {
                    // TODO: This timeout should be removed once #11782 lands;
                    // we should instead periodically ensure clusters are
                    // healthy and actively cancel any work waiting on unhealthy
                    // clusters.
                    match tokio::time::timeout(timeout_dur, batch).await {
                        Ok(res) => match res {
                            PeekResponseUnary::Rows(rows) => {
                                |rows: Vec<Row>| -> Result<Vec<(Row, Diff)>, AdapterError> {
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
                                                    Err(e) => {
                                                        return Err(AdapterError::Unstructured(
                                                            anyhow!(e),
                                                        ))
                                                    }
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
                                            MutationKind::Update | MutationKind::Delete => {
                                                diffs.push((row, -1))
                                            }
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
                                }(rows)
                            }
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
                            let result = internal_cmd_tx.send(Message::RemovePendingPeeks {
                                conn_id: ctx.session().conn_id().clone(),
                            });
                            if let Err(e) = result {
                                warn!("internal_cmd_rx dropped before we could send: {:?}", e);
                            }
                            Err(AdapterError::StatementTimeout)
                        }
                    }
                }
                resp @ ExecuteResponse::Canceled => {
                    ctx.retire(Ok(resp));
                    return;
                }
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
            if let Some(TimestampContext::TimelineTimestamp(timeline, read_ts)) = timestamp_context
            {
                let (tx, rx) = tokio::sync::oneshot::channel();
                let result = strict_serializable_reads_tx.send(PendingReadTxn {
                    txn: PendingRead::ReadThenWrite {
                        tx,
                        timestamp: (read_ts, timeline),
                    },
                    created: Instant::now(),
                    num_requeues: 0,
                });
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
                if let Err(e) = result {
                    warn!(
                        "tx used to linearize read in read then write transaction dropped before we could send: {:?}",
                        e
                    );
                    return;
                }
            }

            match diffs {
                Ok(diffs) => {
                    let result = Self::sequence_send_diffs(
                        ctx.session_mut(),
                        SendDiffsPlan {
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

    pub(super) async fn sequence_alter_item_rename(
        &mut self,
        session: &Session,
        plan: AlterItemRenamePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let op = catalog::Op::RenameItem {
            id: plan.id,
            current_full_name: plan.current_full_name,
            to_name: plan.to_name,
        };
        match self.catalog_transact(Some(session), vec![op]).await {
            Ok(()) => Ok(ExecuteResponse::AlteredObject(plan.object_type)),
            Err(err) => Err(err),
        }
    }

    pub(super) fn sequence_alter_index_set_options(
        &mut self,
        plan: AlterIndexSetOptionsPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        self.set_index_options(plan.id, plan.options)?;
        Ok(ExecuteResponse::AlteredObject(ObjectType::Index))
    }

    pub(super) fn sequence_alter_index_reset_options(
        &mut self,
        plan: AlterIndexResetOptionsPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let mut options = Vec::with_capacity(plan.options.len());
        for o in plan.options {
            options.push(match o {
                IndexOptionName::LogicalCompactionWindow => {
                    IndexOption::LogicalCompactionWindow(Some(Duration::from_millis(
                        DEFAULT_LOGICAL_COMPACTION_WINDOW_TS.into(),
                    )))
                }
            });
        }

        self.set_index_options(plan.id, options)?;

        Ok(ExecuteResponse::AlteredObject(ObjectType::Index))
    }

    fn set_index_options(
        &mut self,
        id: GlobalId,
        options: Vec<IndexOption>,
    ) -> Result<(), AdapterError> {
        for o in options {
            match o {
                IndexOption::LogicalCompactionWindow(window) => {
                    // The index is on a specific cluster.
                    let cluster = self
                        .catalog()
                        .get_entry(&id)
                        .index()
                        .expect("setting options on index")
                        .cluster_id;
                    let policy = match window {
                        Some(time) => {
                            ReadPolicy::lag_writes_by(time.try_into()?, SINCE_GRANULARITY)
                        }
                        None => ReadPolicy::ValidFrom(Antichain::from_elem(Timestamp::minimum())),
                    };
                    self.update_compute_base_read_policy(cluster, id, policy);
                }
            }
        }
        Ok(())
    }

    pub(super) async fn sequence_alter_role(
        &mut self,
        session: &Session,
        AlterRolePlan {
            id,
            name,
            attributes,
        }: AlterRolePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let catalog = self.catalog().for_session(session);
        let role = catalog.get_role(&id);
        let attributes = (role, attributes).into();
        let op = catalog::Op::AlterRole {
            id,
            name,
            attributes,
        };
        self.catalog_transact(Some(session), vec![op])
            .await
            .map(|_| ExecuteResponse::AlteredRole)
    }

    pub(super) async fn sequence_alter_secret(
        &mut self,
        session: &Session,
        plan: AlterSecretPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let AlterSecretPlan { id, mut secret_as } = plan;

        let payload = self.extract_secret(session, &mut secret_as)?;

        self.secrets_controller.ensure(id, &payload).await?;

        Ok(ExecuteResponse::AlteredObject(ObjectType::Secret))
    }

    pub(super) async fn sequence_alter_sink(
        &mut self,
        session: &Session,
        AlterSinkPlan { id, size }: AlterSinkPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let cluster_config = alter_storage_cluster_config(size);
        if let Some(cluster_config) = cluster_config {
            let mut ops = self.alter_linked_cluster_ops(id, &cluster_config).await?;
            ops.push(catalog::Op::AlterSink {
                id,
                cluster_config: cluster_config.clone(),
            });
            self.catalog_transact(Some(session), ops).await?;

            self.maybe_alter_linked_cluster(id).await;
        }

        Ok(ExecuteResponse::AlteredObject(ObjectType::Sink))
    }

    pub(super) async fn sequence_alter_source(
        &mut self,
        session: &mut Session,
        AlterSourcePlan { id, action }: AlterSourcePlan,
        to_create_subsources: Vec<CreateSourcePlans>,
    ) -> Result<ExecuteResponse, AdapterError> {
        assert!(
            to_create_subsources.is_empty()
                || matches!(action, AlterSourceAction::AddSubsourceExports { .. }),
            "cannot include subsources with {:?}",
            action
        );

        let cur_entry = self.catalog().get_entry(&id);
        let cur_source = cur_entry.source().expect("known to be source");

        let cur_ingestion = match &cur_source.data_source {
            DataSourceDesc::Ingestion(ingestion) => ingestion,
            DataSourceDesc::Introspection(_)
            | DataSourceDesc::Progress
            | DataSourceDesc::Webhook { .. }
            | DataSourceDesc::Source => {
                coord_bail!("cannot ALTER this type of source");
            }
        };

        let create_sql_to_stmt_deps = |coord: &Coordinator, err_cx, create_source_sql| {
            // Parse statement.
            let create_source_stmt = match mz_sql::parse::parse(create_source_sql)
                .expect("invalid create sql persisted to catalog")
                .into_element()
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
            AlterSourceAction::Resize(size) => {
                let cluster_config = alter_storage_cluster_config(size);
                if let Some(cluster_config) = cluster_config {
                    let mut ops = self.alter_linked_cluster_ops(id, &cluster_config).await?;
                    ops.push(catalog::Op::AlterSource {
                        id,
                        cluster_config: cluster_config.clone(),
                    });
                    self.catalog_transact(Some(session), ops).await?;

                    self.maybe_alter_linked_cluster(id).await;
                }
            }
            AlterSourceAction::DropSubsourceExports { to_drop } => {
                mz_ore::soft_assert!(!to_drop.is_empty());

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
                                            mz_ore::soft_assert!(
                                                column_qualified_reference.0.len() == 4
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

                let source = catalog::Source::new(
                    id,
                    plan,
                    // Use the same cluster ID.
                    Some(cur_ingestion.instance_id),
                    resolved_ids,
                    cur_source.custom_logical_compaction_window,
                    cur_source.is_retained_metrics_object,
                );

                // Get new ingestion description for storage.
                let ingestion = match &source.data_source {
                    DataSourceDesc::Ingestion(ingestion) => ingestion.clone(),
                    _ => unreachable!("already verified of type ingestion"),
                };

                self.controller
                    .storage
                    .check_alter_collection(id, ingestion.clone())
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
                } = self.sequence_drop_common(session, drops)?;

                assert!(
                    !dropped_active_db && !dropped_active_cluster,
                    "dropping subsources does not drop DBs or clusters"
                );

                // Redefine source.
                ops.push(Op::UpdateItem {
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
                    .alter_collection(id, ingestion)
                    .await
                    .expect("altering collection after txn must succeed");
            }
            AlterSourceAction::AddSubsourceExports {
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
                        mz_ore::soft_assert!(
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

                        mz_ore::soft_assert!(
                            curr.iter()
                                .all(|v| matches!(v, WithOptionValue::UnresolvedItemName(_))),
                            "all elements of text columns must be UnresolvedItemName, but got {:?}",
                            curr
                        );

                        mz_ore::soft_assert!(
                            curr.iter().duplicates().next().is_none(),
                            "TEXT COLUMN references must be unique among both sets, but got {:?}",
                            curr
                        );
                    }
                    (None, Some(new)) => {
                        mz_ore::soft_assert!(
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
                )
                .map_err(|e| AdapterError::internal(ALTER_SOURCE, e))?
                {
                    Plan::CreateSource(plan) => plan,
                    _ => unreachable!("create source plan is only valid response"),
                };

                // Asserting that we've done the right thing with dependencies
                // here requires mocking out objects in the catalog, which is a
                // large task for an operation we have to cover in tests anyway.
                let source = catalog::Source::new(
                    id,
                    plan,
                    // Use the same cluster ID.
                    Some(cur_ingestion.instance_id),
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

                // Get new ingestion description for storage.
                let ingestion = match &source.data_source {
                    DataSourceDesc::Ingestion(ingestion) => ingestion.clone(),
                    _ => unreachable!("already verified of type ingestion"),
                };

                self.controller
                    .storage
                    .check_alter_collection(id, ingestion.clone())
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
                ops.push(Op::UpdateItem {
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
                            &crate::catalog::builtin::MZ_SOURCE_STATUS_HISTORY,
                        ));

                    let (data_source, status_collection_id) = match source.data_source {
                        // Subsources use source statuses.
                        DataSourceDesc::Source => (DataSource::Other, source_status_collection_id),
                        o => {
                            unreachable!(
                                "ALTER SOURCE...ADD SUBSOURCE only creates subsources but got {:?}",
                                o
                            )
                        }
                    };

                    self.controller
                        .storage
                        .create_collections(vec![(
                            source_id,
                            CollectionDescription {
                                desc: source.desc.clone(),
                                data_source,
                                since: None,
                                status_collection_id,
                            },
                        )])
                        .await
                        .unwrap_or_terminate("cannot fail to create collections");

                    source_ids.push(source_id);
                }

                // Commit the new ingestion to storage.
                self.controller
                    .storage
                    .alter_collection(id, ingestion)
                    .await
                    .expect("altering collection after txn must succeed");

                self.initialize_storage_read_policies(
                    source_ids,
                    Some(DEFAULT_LOGICAL_COMPACTION_WINDOW_TS),
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
            self.catalog().state(),
            secret_as,
            ExprPrepStyle::OneShot {
                logical_time: EvalTime::NotAvailable,
                session,
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

    pub(super) async fn sequence_alter_system_set(
        &mut self,
        session: &Session,
        AlterSystemSetPlan { name, value }: AlterSystemSetPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        self.is_user_allowed_to_alter_system(session, Some(&name))?;
        let op = match value {
            VariableValue::Values(values) => catalog::Op::UpdateSystemConfiguration {
                name,
                value: OwnedVarInput::SqlSet(values),
            },
            VariableValue::Default => catalog::Op::ResetSystemConfiguration { name },
        };
        self.catalog_transact(Some(session), vec![op]).await?;
        Ok(ExecuteResponse::AlteredSystemConfiguration)
    }

    pub(super) async fn sequence_alter_system_reset(
        &mut self,
        session: &Session,
        AlterSystemResetPlan { name }: AlterSystemResetPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        self.is_user_allowed_to_alter_system(session, Some(&name))?;
        let op = catalog::Op::ResetSystemConfiguration { name };
        self.catalog_transact(Some(session), vec![op]).await?;
        Ok(ExecuteResponse::AlteredSystemConfiguration)
    }

    pub(super) async fn sequence_alter_system_reset_all(
        &mut self,
        session: &Session,
        _: AlterSystemResetAllPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        self.is_user_allowed_to_alter_system(session, None)?;
        let op = catalog::Op::ResetAllSystemConfiguration;
        self.catalog_transact(Some(session), vec![op]).await?;
        Ok(ExecuteResponse::AlteredSystemConfiguration)
    }

    // TODO(jkosh44) Move this into rbac.rs once RBAC is always on.
    fn is_user_allowed_to_alter_system(
        &self,
        session: &Session,
        var_name: Option<&str>,
    ) -> Result<(), AdapterError> {
        if session.user().is_system_user()
            || (var_name == Some(ENABLE_RBAC_CHECKS.name()) && session.is_superuser())
        {
            match var_name {
                Some(name) => {
                    // In lieu of plumbing the user to all system config functions, just check that the var is
                    // visible.
                    let var = self.catalog().system_config().get(name)?;
                    var.visible(session.user(), Some(self.catalog().system_config()))?;
                    Ok(())
                }
                None => Ok(()),
            }
        } else if var_name == Some(ENABLE_RBAC_CHECKS.name()) {
            Err(AdapterError::Unauthorized(
                rbac::UnauthorizedError::Superuser {
                    action: format!(
                        "toggle the '{}' system configuration parameter",
                        ENABLE_RBAC_CHECKS.name()
                    ),
                },
            ))
        } else {
            Err(AdapterError::Unauthorized(
                rbac::UnauthorizedError::MzSystem {
                    action: "alter system".into(),
                },
            ))
        }
    }

    // Returns the name of the portal to execute.
    pub(super) fn sequence_execute(
        &mut self,
        session: &mut Session,
        plan: ExecutePlan,
    ) -> Result<String, AdapterError> {
        // Verify the stmt is still valid.
        Self::verify_prepared_statement(self.catalog(), session, &plan.name)?;
        let ps = session
            .get_prepared_statement_unverified(&plan.name)
            .expect("known to exist");
        let stmt = ps.stmt().cloned();
        let desc = ps.desc().clone();
        let revision = ps.catalog_revision;
        session.create_new_portal(stmt, desc, plan.params, Vec::new(), revision)
    }

    pub(super) async fn sequence_grant_privileges(
        &mut self,
        session: &mut Session,
        GrantPrivilegesPlan {
            update_privileges,
            grantees,
        }: GrantPrivilegesPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        self.sequence_update_privileges(
            session,
            update_privileges,
            grantees,
            UpdatePrivilegeVariant::Grant,
        )
        .await
    }

    pub(super) async fn sequence_revoke_privileges(
        &mut self,
        session: &mut Session,
        RevokePrivilegesPlan {
            update_privileges,
            revokees,
        }: RevokePrivilegesPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        self.sequence_update_privileges(
            session,
            update_privileges,
            revokees,
            UpdatePrivilegeVariant::Revoke,
        )
        .await
    }

    async fn sequence_update_privileges(
        &mut self,
        session: &mut Session,
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
                        ErrorMessageObjectDescription::from_id(&target_id, &catalog);
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

    pub(super) async fn sequence_alter_default_privileges(
        &mut self,
        session: &mut Session,
        AlterDefaultPrivilegesPlan {
            privilege_objects,
            privilege_acl_items,
            is_grant,
        }: AlterDefaultPrivilegesPlan,
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
                ops.push(Op::UpdateDefaultPrivilege {
                    privilege_object: privilege_object.clone(),
                    privilege_acl_item: privilege_acl_item.clone(),
                    variant,
                })
            }
        }

        self.catalog_transact(Some(session), ops).await?;
        Ok(ExecuteResponse::AlteredDefaultPrivileges)
    }

    pub(super) async fn sequence_grant_role(
        &mut self,
        session: &mut Session,
        GrantRolePlan {
            role_ids,
            member_ids,
            grantor_id,
        }: GrantRolePlan,
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
                    catalog.ensure_not_reserved_role(&role_id)?;
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

    pub(super) async fn sequence_revoke_role(
        &mut self,
        session: &mut Session,
        RevokeRolePlan {
            role_ids,
            member_ids,
            grantor_id,
        }: RevokeRolePlan,
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
                    catalog.ensure_not_reserved_role(&role_id)?;
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

    pub(super) async fn sequence_alter_owner(
        &mut self,
        session: &mut Session,
        AlterOwnerPlan {
            id,
            object_type,
            new_owner,
        }: AlterOwnerPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let entry = if let ObjectId::Item(global_id) = &id {
            Some(self.catalog().get_entry(global_id))
        } else {
            None
        };

        // Cannot directly change the owner of an index.
        if let Some(entry) = &entry {
            if entry.is_index() {
                let name = self
                    .catalog()
                    .resolve_full_name(entry.name(), Some(session.conn_id()))
                    .to_string();
                session.add_notice(AdapterNotice::AlterIndexOwner { name });
                return Ok(ExecuteResponse::AlteredObject(object_type));
            }
        }

        let mut ops = vec![Op::UpdateOwner { id, new_owner }];
        // Alter owner cascades down to dependent indexes.
        if let Some(entry) = entry {
            let dependent_index_ops = entry
                .used_by()
                .into_iter()
                .filter(|id| self.catalog().get_entry(id).is_index())
                .map(|id| Op::UpdateOwner {
                    id: ObjectId::Item(*id),
                    new_owner,
                });
            ops.extend(dependent_index_ops);
        }

        self.catalog_transact(Some(session), ops)
            .await
            .map(|_| ExecuteResponse::AlteredObject(object_type))
    }

    pub(super) async fn sequence_reassign_owned(
        &mut self,
        session: &mut Session,
        ReassignOwnedPlan {
            old_roles,
            new_role,
            reassign_ids,
        }: ReassignOwnedPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        for role_id in old_roles.iter().chain(iter::once(&new_role)) {
            self.catalog().ensure_not_reserved_role(role_id)?;
        }

        let ops = reassign_ids
            .into_iter()
            .map(|id| Op::UpdateOwner {
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

const OPTIMIZER_MAX_STATS_WAIT: Duration = Duration::from_millis(250);
const OPTIMIZER_ONESHOT_STATS_WAIT: Duration = Duration::from_millis(10);

impl CachedStatisticsOracle {
    pub async fn new<T: Clone + std::fmt::Debug + timely::PartialOrder + Send + Sync>(
        ids: &BTreeSet<GlobalId>,
        as_of: &Antichain<T>,
        storage: &dyn mz_storage_client::controller::StorageController<Timestamp = T>,
    ) -> Result<Self, StorageError> {
        let mut cache = BTreeMap::new();

        for id in ids {
            let stats = storage.snapshot_stats(*id, as_of.clone()).await;

            match stats {
                Ok(stats) => {
                    if timely::PartialOrder::less_than(&stats.as_of, as_of) {
                        ::tracing::warn!(
                            "stale statistics: statistics from {:?} are earlier than query at {as_of:?}",
                            stats.as_of
                        );
                    }

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
    async fn statistics_oracle(
        &self,
        session: &Session,
        source_ids: &BTreeSet<GlobalId>,
        query_as_of: Antichain<Timestamp>,
        is_oneshot: bool,
    ) -> Result<Box<dyn mz_transform::StatisticsOracle>, AdapterError> {
        if !session.vars().enable_session_cardinality_estimates() {
            return Ok(Box::new(EmptyStatisticsOracle));
        }

        let timeout = if is_oneshot {
            // TODO(mgree): ideally, we would shorten the timeout even more if we think the query could take the fast path
            OPTIMIZER_ONESHOT_STATS_WAIT
        } else {
            OPTIMIZER_MAX_STATS_WAIT
        };

        let cached_stats = mz_ore::future::timeout(
            timeout,
            CachedStatisticsOracle::new(source_ids, &query_as_of, self.controller.storage.as_ref()),
        )
        .await;

        match cached_stats {
            Ok(stats) => Ok(Box::new(stats)),
            Err(mz_ore::future::TimeoutError::DeadlineElapsed) => {
                Ok(Box::new(EmptyStatisticsOracle))
            }
            Err(mz_ore::future::TimeoutError::Inner(e)) => Err(AdapterError::Storage(e)),
        }
    }
}

fn check_no_invalid_log_reads(
    catalog: &Catalog,
    cluster: &Cluster,
    source_ids: &BTreeSet<GlobalId>,
    target_replica: &mut Option<ReplicaId>,
) -> Result<(), AdapterError>
where
{
    let log_names = source_ids
        .iter()
        .flat_map(|id| catalog.introspection_dependencies(*id))
        .map(|id| catalog.get_entry(&id).name().item.clone())
        .collect::<Vec<_>>();

    if log_names.is_empty() {
        return Ok(());
    }

    // Reading from log sources on replicated clusters is only allowed if a
    // target replica is selected. Otherwise, we have no way of knowing which
    // replica we read the introspection data from.
    let num_replicas = cluster.replicas_by_id.len();
    if target_replica.is_none() {
        if num_replicas == 1 {
            *target_replica = cluster.replicas_by_id.keys().next().copied();
        } else {
            return Err(AdapterError::UntargetedLogRead { log_names });
        }
    }

    // Ensure that logging is initialized for the target replica, lest
    // we try to read from a non-existing arrangement.
    let replica_id = target_replica.expect("set to `Some` above");
    let replica = &cluster.replicas_by_id[&replica_id];
    if !replica.config.compute.logging.enabled() {
        return Err(AdapterError::IntrospectionDisabled { log_names });
    }

    Ok(())
}

/// Return a [`SourceSinkClusterConfig`] based on the possibly altered
/// parameters.
fn alter_storage_cluster_config(size: AlterOptionParameter) -> Option<SourceSinkClusterConfig> {
    match size {
        AlterOptionParameter::Set(size) => Some(SourceSinkClusterConfig::Linked { size }),
        AlterOptionParameter::Reset => Some(SourceSinkClusterConfig::Undefined),
        AlterOptionParameter::Unchanged => None,
    }
}
