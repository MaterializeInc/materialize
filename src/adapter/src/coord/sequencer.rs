// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logic for executing a planned SQL query.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::num::{NonZeroI64, NonZeroUsize};
use std::time::Duration;

use anyhow::anyhow;
use maplit::btreeset;
use timely::progress::{Antichain, Timestamp as TimelyTimestamp};
use tokio::sync::{mpsc, OwnedMutexGuard};
use tracing::{event, warn, Level};

use mz_cloud_resources::VpcEndpointConfig;
use mz_compute_client::command::{BuildDesc, DataflowDesc, IndexDesc, ReplicaId};
use mz_compute_client::controller::{
    ComputeInstanceId, ComputeReplicaConfig, ComputeReplicaLogging,
};
use mz_compute_client::explain::{TimestampExplanation, TimestampSource};
use mz_compute_client::sinks::{
    ComputeSinkConnection, ComputeSinkDesc, SinkAsOf, SubscribeSinkConnection,
};
use mz_expr::{
    permutation_for_arrangement, CollectionPlan, MirRelationExpr, MirScalarExpr,
    OptimizedMirRelationExpr, RowSetFinishing,
};
use mz_ore::task;
use mz_repr::explain_new::Explainee;
use mz_repr::{Datum, Diff, GlobalId, RelationDesc, Row, RowArena, Timestamp};
use mz_sql::ast::{ExplainStage, IndexOptionName, ObjectType};
use mz_sql::catalog::{CatalogComputeInstance, CatalogError, CatalogItemType, CatalogTypeDetails};
use mz_sql::names::QualifiedObjectName;
use mz_sql::plan::{
    AlterIndexResetOptionsPlan, AlterIndexSetOptionsPlan, AlterItemRenamePlan, AlterSecretPlan,
    AlterSinkPlan, AlterSourcePlan, AlterSystemResetAllPlan, AlterSystemResetPlan,
    AlterSystemSetPlan, CreateComputeInstancePlan, CreateComputeReplicaPlan, CreateConnectionPlan,
    CreateDatabasePlan, CreateIndexPlan, CreateMaterializedViewPlan, CreateRolePlan,
    CreateSchemaPlan, CreateSecretPlan, CreateSinkPlan, CreateSourcePlan, CreateTablePlan,
    CreateTypePlan, CreateViewPlan, DropComputeInstancesPlan, DropComputeReplicasPlan,
    DropDatabasePlan, DropItemsPlan, DropRolesPlan, DropSchemaPlan, ExecutePlan, ExplainPlan,
    FetchPlan, HirRelationExpr, IndexOption, InsertPlan, MaterializedView, MutationKind,
    OptimizerConfig, PeekPlan, Plan, PlanKind, QueryWhen, RaisePlan, ReadThenWritePlan,
    ResetVariablePlan, RotateKeysPlan, SendDiffsPlan, SetVariablePlan, ShowVariablePlan,
    SubscribeFrom, SubscribePlan, View,
};
use mz_ssh_util::keys::SshKeyPairSet;
use mz_stash::Append;
use mz_storage_client::controller::{CollectionDescription, DataSource, ReadPolicy, StorageError};
use mz_storage_client::types::sinks::StorageSinkConnectionBuilder;
use mz_storage_client::types::sources::{IngestionDescription, SourceExport};

use crate::catalog::builtin::{
    INFORMATION_SCHEMA, MZ_CATALOG_SCHEMA, MZ_INTERNAL_SCHEMA, MZ_INTROSPECTION_COMPUTE_INSTANCE,
    MZ_INTROSPECTION_ROLE, MZ_SYSTEM_COMPUTE_INSTANCE, PG_CATALOG_SCHEMA,
};
use crate::catalog::{
    self, Catalog, CatalogItem, ComputeInstance, Connection, DataSourceDesc, Ingestion,
    SerializedComputeReplicaLocation, StorageSinkConnectionState, SYSTEM_USER,
};
use crate::command::{Command, ExecuteResponse};
use crate::coord::appends::{BuiltinTableUpdateSource, Deferred, DeferredPlan, PendingWriteTxn};
use crate::coord::dataflows::{prep_relation_expr, prep_scalar_expr, ExprPrepStyle};
use crate::coord::{
    peek, Coordinator, Message, PendingReadTxn, PendingTxn, SendDiffs, SinkConnectionReady,
    TxnReads, DEFAULT_LOGICAL_COMPACTION_WINDOW_MS,
};
use crate::error::AdapterError;
use crate::explain_new::optimizer_trace::OptimizerTrace;
use crate::notice::AdapterNotice;
use crate::session::vars::IsolationLevel;
use crate::session::{
    EndTransactionAction, PreparedStatement, Session, TransactionOps, TransactionStatus, Var,
    WriteOp,
};
use crate::subscribe::PendingSubscribe;
use crate::util::{send_immediate_rows, ClientTransmitter, ComputeSinkId};
use crate::{guard_write_critical_section, session, PeekResponseUnary};

impl<S: Append + 'static> Coordinator<S> {
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn sequence_plan(
        &mut self,
        mut tx: ClientTransmitter<ExecuteResponse>,
        mut session: Session,
        plan: Plan,
        depends_on: Vec<GlobalId>,
    ) {
        event!(Level::TRACE, plan = format!("{:?}", plan));
        let responses = ExecuteResponse::generated_from(PlanKind::from(&plan));
        tx.set_allowed(responses);

        if session.user().name == MZ_INTROSPECTION_ROLE.name {
            if let Err(e) = self.mz_introspection_user_privilege_hack(&session, &plan, &depends_on)
            {
                return tx.send(Err(e), session);
            }
        }

        match plan {
            Plan::CreateSource(plan) => {
                let source_id = match self.catalog.allocate_user_id().await {
                    Ok(id) => id,
                    Err(e) => return tx.send(Err(e.into()), session),
                };
                tx.send(
                    self.sequence_create_source(&mut session, vec![(source_id, plan, depends_on)])
                        .await,
                    session,
                );
            }
            Plan::CreateConnection(plan) => {
                tx.send(
                    self.sequence_create_connection(&mut session, plan, depends_on)
                        .await,
                    session,
                );
            }
            Plan::CreateDatabase(plan) => {
                tx.send(
                    self.sequence_create_database(&mut session, plan).await,
                    session,
                );
            }
            Plan::CreateSchema(plan) => {
                tx.send(
                    self.sequence_create_schema(&mut session, plan).await,
                    session,
                );
            }
            Plan::CreateRole(plan) => {
                tx.send(self.sequence_create_role(&session, plan).await, session);
            }
            Plan::CreateComputeInstance(plan) => {
                tx.send(
                    self.sequence_create_compute_instance(&session, plan).await,
                    session,
                );
            }
            Plan::CreateComputeReplica(plan) => {
                tx.send(
                    self.sequence_create_compute_replica(&session, plan).await,
                    session,
                );
            }
            Plan::CreateTable(plan) => {
                tx.send(
                    self.sequence_create_table(&mut session, plan, depends_on)
                        .await,
                    session,
                );
            }
            Plan::CreateSecret(plan) => {
                tx.send(
                    self.sequence_create_secret(&mut session, plan).await,
                    session,
                );
            }
            Plan::CreateSink(plan) => {
                self.sequence_create_sink(session, plan, depends_on, tx)
                    .await;
            }
            Plan::CreateView(plan) => {
                tx.send(
                    self.sequence_create_view(&mut session, plan, depends_on)
                        .await,
                    session,
                );
            }
            Plan::CreateMaterializedView(plan) => {
                tx.send(
                    self.sequence_create_materialized_view(&mut session, plan, depends_on)
                        .await,
                    session,
                );
            }
            Plan::CreateIndex(plan) => {
                tx.send(
                    self.sequence_create_index(&mut session, plan, depends_on)
                        .await,
                    session,
                );
            }
            Plan::CreateType(plan) => {
                tx.send(
                    self.sequence_create_type(&session, plan, depends_on).await,
                    session,
                );
            }
            Plan::DropDatabase(plan) => {
                tx.send(self.sequence_drop_database(&session, plan).await, session);
            }
            Plan::DropSchema(plan) => {
                tx.send(self.sequence_drop_schema(&session, plan).await, session);
            }
            Plan::DropRoles(plan) => {
                tx.send(self.sequence_drop_roles(&session, plan).await, session);
            }
            Plan::DropComputeInstances(plan) => {
                tx.send(
                    self.sequence_drop_compute_instances(&session, plan).await,
                    session,
                );
            }
            Plan::DropComputeReplicas(plan) => {
                tx.send(
                    self.sequence_drop_compute_replica(&session, plan).await,
                    session,
                );
            }
            Plan::DropItems(plan) => {
                tx.send(self.sequence_drop_items(&session, plan).await, session);
            }
            Plan::EmptyQuery => {
                tx.send(Ok(ExecuteResponse::EmptyQuery), session);
            }
            Plan::ShowAllVariables => {
                tx.send(self.sequence_show_all_variables(&session), session);
            }
            Plan::ShowVariable(plan) => {
                tx.send(self.sequence_show_variable(&session, plan), session);
            }
            Plan::SetVariable(plan) => {
                tx.send(self.sequence_set_variable(&mut session, plan), session);
            }
            Plan::ResetVariable(plan) => {
                tx.send(self.sequence_reset_variable(&mut session, plan), session);
            }
            Plan::StartTransaction(plan) => {
                if matches!(session.transaction(), TransactionStatus::InTransaction(_)) {
                    session.add_notice(AdapterNotice::ExistingTransactionInProgress);
                }
                let (session, result) = session.start_transaction(
                    self.now_datetime(),
                    plan.access,
                    plan.isolation_level,
                );
                tx.send(result.map(|_| ExecuteResponse::StartedTransaction), session)
            }
            Plan::CommitTransaction | Plan::AbortTransaction => {
                let action = match plan {
                    Plan::CommitTransaction => EndTransactionAction::Commit,
                    Plan::AbortTransaction => EndTransactionAction::Rollback,
                    _ => unreachable!(),
                };
                if session.transaction().is_implicit() {
                    // In Postgres, if a user sends a COMMIT or ROLLBACK in an
                    // implicit transaction, a warning is sent warning them.
                    // (The transaction is still closed and a new implicit
                    // transaction started, though.)
                    session
                        .add_notice(AdapterNotice::ExplicitTransactionControlInImplicitTransaction);
                }
                self.sequence_end_transaction(tx, session, action).await;
            }
            Plan::Peek(plan) => {
                tx.send(self.sequence_peek(&mut session, plan).await, session);
            }
            Plan::Subscribe(plan) => {
                tx.send(
                    self.sequence_subscribe(&mut session, plan, depends_on)
                        .await,
                    session,
                );
            }
            Plan::SendRows(plan) => {
                tx.send(Ok(send_immediate_rows(plan.rows)), session);
            }
            Plan::CopyFrom(plan) => {
                tx.send(
                    Ok(ExecuteResponse::CopyFrom {
                        id: plan.id,
                        columns: plan.columns,
                        params: plan.params,
                    }),
                    session,
                );
            }
            Plan::Explain(plan) => {
                tx.send(self.sequence_explain(&session, plan), session);
            }
            Plan::SendDiffs(plan) => {
                tx.send(self.sequence_send_diffs(&mut session, plan), session);
            }
            Plan::Insert(plan) => {
                self.sequence_insert(tx, session, plan).await;
            }
            Plan::ReadThenWrite(plan) => {
                self.sequence_read_then_write(tx, session, plan).await;
            }
            Plan::AlterNoop(plan) => {
                tx.send(
                    Ok(ExecuteResponse::AlteredObject(plan.object_type)),
                    session,
                );
            }
            Plan::AlterItemRename(plan) => {
                tx.send(
                    self.sequence_alter_item_rename(&session, plan).await,
                    session,
                );
            }
            Plan::AlterIndexSetOptions(plan) => {
                tx.send(self.sequence_alter_index_set_options(plan).await, session);
            }
            Plan::AlterIndexResetOptions(plan) => {
                tx.send(self.sequence_alter_index_reset_options(plan).await, session);
            }
            Plan::AlterSecret(plan) => {
                tx.send(self.sequence_alter_secret(&session, plan).await, session);
            }
            Plan::AlterSink(plan) => {
                tx.send(self.sequence_alter_sink(&session, plan).await, session);
            }
            Plan::AlterSource(plan) => {
                tx.send(self.sequence_alter_source(&session, plan).await, session);
            }
            Plan::AlterSystemSet(plan) => {
                tx.send(
                    self.sequence_alter_system_set(&session, plan).await,
                    session,
                );
            }
            Plan::AlterSystemReset(plan) => {
                tx.send(
                    self.sequence_alter_system_reset(&session, plan).await,
                    session,
                );
            }
            Plan::AlterSystemResetAll(plan) => {
                tx.send(
                    self.sequence_alter_system_reset_all(&session, plan).await,
                    session,
                );
            }
            Plan::DiscardTemp => {
                self.drop_temp_items(&session).await;
                tx.send(Ok(ExecuteResponse::DiscardedTemp), session);
            }
            Plan::DiscardAll => {
                let ret = if let TransactionStatus::Started(_) = session.transaction() {
                    self.drop_temp_items(&session).await;
                    let drop_sinks = session.reset();
                    self.drop_compute_sinks(drop_sinks).await;
                    Ok(ExecuteResponse::DiscardedAll)
                } else {
                    Err(AdapterError::OperationProhibitsTransaction(
                        "DISCARD ALL".into(),
                    ))
                };
                tx.send(ret, session);
            }
            Plan::Declare(plan) => {
                let param_types = vec![];
                let res = self
                    .declare(&mut session, plan.name, plan.stmt, param_types)
                    .map(|()| ExecuteResponse::DeclaredCursor);
                tx.send(res, session);
            }
            Plan::Fetch(FetchPlan {
                name,
                count,
                timeout,
            }) => {
                tx.send(
                    Ok(ExecuteResponse::Fetch {
                        name,
                        count,
                        timeout,
                    }),
                    session,
                );
            }
            Plan::Close(plan) => {
                if session.remove_portal(&plan.name) {
                    tx.send(Ok(ExecuteResponse::ClosedCursor), session);
                } else {
                    tx.send(Err(AdapterError::UnknownCursor(plan.name)), session);
                }
            }
            Plan::Prepare(plan) => {
                if session
                    .get_prepared_statement_unverified(&plan.name)
                    .is_some()
                {
                    tx.send(
                        Err(AdapterError::PreparedStatementExists(plan.name)),
                        session,
                    );
                } else {
                    session.set_prepared_statement(
                        plan.name,
                        PreparedStatement::new(
                            Some(plan.stmt),
                            plan.desc,
                            self.catalog.transient_revision(),
                        ),
                    );
                    tx.send(Ok(ExecuteResponse::Prepare), session);
                }
            }
            Plan::Execute(plan) => {
                match self.sequence_execute(&mut session, plan) {
                    Ok(portal_name) => {
                        self.internal_cmd_tx
                            .send(Message::Command(Command::Execute {
                                portal_name,
                                session,
                                tx: tx.take(),
                                span: tracing::Span::none(),
                            }))
                            .expect("sending to self.internal_cmd_tx cannot fail");
                    }
                    Err(err) => tx.send(Err(err), session),
                };
            }
            Plan::Deallocate(plan) => match plan.name {
                Some(name) => {
                    if session.remove_prepared_statement(&name) {
                        tx.send(Ok(ExecuteResponse::Deallocate { all: false }), session);
                    } else {
                        tx.send(Err(AdapterError::UnknownPreparedStatement(name)), session);
                    }
                }
                None => {
                    session.remove_all_prepared_statements();
                    tx.send(Ok(ExecuteResponse::Deallocate { all: true }), session);
                }
            },
            Plan::Raise(RaisePlan { severity }) => {
                session.add_notice(AdapterNotice::UserRequested { severity });
                tx.send(Ok(ExecuteResponse::Raised), session);
            }
            Plan::RotateKeys(RotateKeysPlan { id }) => {
                tx.send(self.sequence_rotate_keys(&session, id).await, session);
            }
        }
    }

    pub(crate) async fn sequence_create_source(
        &mut self,
        session: &mut Session,
        plans: Vec<(GlobalId, CreateSourcePlan, Vec<GlobalId>)>,
    ) -> Result<ExecuteResponse, AdapterError> {
        let mut ops = vec![];
        let mut sources = vec![];

        let if_not_exists_ids = plans
            .iter()
            .filter_map(|(id, plan, _)| {
                if plan.if_not_exists {
                    Some((*id, plan.name.clone()))
                } else {
                    None
                }
            })
            .collect::<HashMap<_, _>>();

        for (source_id, plan, depends_on) in plans {
            let source_oid = self.catalog.allocate_oid()?;
            let source = catalog::Source {
                create_sql: plan.source.create_sql,
                data_source: match plan.source.ingestion {
                    Some(ingestion) => {
                        let host_config = self.catalog.resolve_storage_host_config(
                            plan.host_config,
                            // Undefined sizes permitted in unsafe mode
                            self.catalog.config().unsafe_mode,
                        )?;
                        DataSourceDesc::Ingestion(catalog::Ingestion {
                            desc: ingestion.desc,
                            source_imports: ingestion.source_imports,
                            subsource_exports: ingestion.subsource_exports,
                            host_config,
                        })
                    }
                    None => {
                        assert!(
                            matches!(plan.host_config, mz_sql::plan::StorageHostConfig::Undefined),
                            "subsources must not have a host config defined"
                        );
                        DataSourceDesc::Source
                    }
                },
                desc: plan.source.desc,
                timeline: plan.timeline,
                depends_on,
            };
            ops.push(catalog::Op::CreateItem {
                id: source_id,
                oid: source_oid,
                name: plan.name.clone(),
                item: CatalogItem::Source(source.clone()),
            });
            sources.push((source_id, source));
        }
        match self
            .catalog_transact(Some(session), ops, move |_| Ok(()))
            .await
        {
            Ok(()) => {
                for (source_id, source) in sources {
                    let source_status_collection_id = if self.catalog.config().unsafe_mode {
                        Some(self.catalog.resolve_builtin_storage_collection(
                            &crate::catalog::builtin::MZ_SOURCE_STATUS_HISTORY,
                        ))
                    } else {
                        None
                    };

                    let (data_source, status_collection_id) = match source.data_source {
                        DataSourceDesc::Ingestion(ingestion) => {
                            let mut source_imports = BTreeMap::new();
                            for source_import in ingestion.source_imports {
                                source_imports.insert(source_import, ());
                            }

                            let mut source_exports = BTreeMap::new();
                            // By convention the first output corresponds to the main source object
                            let main_export = SourceExport {
                                output_index: 0,
                                storage_metadata: (),
                            };
                            source_exports.insert(source_id, main_export);
                            for (subsource, output_index) in ingestion.subsource_exports {
                                let export = SourceExport {
                                    output_index,
                                    storage_metadata: (),
                                };
                                source_exports.insert(subsource, export);
                            }

                            (
                                DataSource::Ingestion(IngestionDescription {
                                    desc: ingestion.desc,
                                    ingestion_metadata: (),
                                    source_imports,
                                    source_exports,
                                    host_config: ingestion.host_config,
                                }),
                                source_status_collection_id,
                            )
                        }
                        DataSourceDesc::Source => (DataSource::Other, None),
                        DataSourceDesc::Introspection(_) => {
                            unreachable!("cannot create sources with introspection data sources")
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
                        .unwrap();

                    self.initialize_storage_read_policies(
                        vec![source_id],
                        DEFAULT_LOGICAL_COMPACTION_WINDOW_MS,
                    )
                    .await;
                }
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

    async fn sequence_create_connection(
        &mut self,
        session: &mut Session,
        plan: CreateConnectionPlan,
        depends_on: Vec<GlobalId>,
    ) -> Result<ExecuteResponse, AdapterError> {
        let connection_oid = self.catalog.allocate_oid()?;
        let connection_gid = self.catalog.allocate_user_id().await?;
        let mut connection = plan.connection.connection.clone();

        match connection {
            mz_storage_client::types::connections::Connection::Ssh(ref mut ssh) => {
                let key_set = SshKeyPairSet::new()?;
                self.secrets_controller
                    .ensure(connection_gid, &key_set.to_bytes())
                    .await?;
                ssh.public_keys = Some(key_set.public_keys());
            }
            mz_storage_client::types::connections::Connection::AwsPrivatelink(ref privatelink) => {
                self.cloud_resource_controller
                    .as_ref()
                    .ok_or(AdapterError::Unsupported("AWS PrivateLink connections"))?
                    .ensure_vpc_endpoint(
                        connection_gid,
                        VpcEndpointConfig {
                            aws_service_name: privatelink.service_name.to_owned(),
                            availability_zone_ids: privatelink.availability_zones.to_owned(),
                        },
                    )
                    .await?;
            }
            _ => {}
        }

        let ops = vec![catalog::Op::CreateItem {
            id: connection_gid,
            oid: connection_oid,
            name: plan.name.clone(),
            item: CatalogItem::Connection(Connection {
                create_sql: plan.connection.create_sql,
                connection,
                depends_on,
            }),
        }];

        match self.catalog_transact(Some(session), ops, |_| Ok(())).await {
            Ok(_) => Ok(ExecuteResponse::CreatedConnection),
            Err(AdapterError::Catalog(catalog::Error {
                kind: catalog::ErrorKind::ItemAlreadyExists(_, _),
                ..
            })) if plan.if_not_exists => Ok(ExecuteResponse::CreatedConnection),
            Err(err) => Err(err),
        }
    }

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

        match self.catalog_transact(Some(session), ops, |_| Ok(())).await {
            Ok(_) => Ok(ExecuteResponse::AlteredObject(ObjectType::Connection)),
            Err(err) => Err(err),
        }
    }

    async fn sequence_create_database(
        &mut self,
        session: &mut Session,
        plan: CreateDatabasePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let db_oid = self.catalog.allocate_oid()?;
        let schema_oid = self.catalog.allocate_oid()?;
        let ops = vec![catalog::Op::CreateDatabase {
            name: plan.name.clone(),
            oid: db_oid,
            public_schema_oid: schema_oid,
        }];
        match self.catalog_transact(Some(session), ops, |_| Ok(())).await {
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

    async fn sequence_create_schema(
        &mut self,
        session: &mut Session,
        plan: CreateSchemaPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let oid = self.catalog.allocate_oid()?;
        let op = catalog::Op::CreateSchema {
            database_id: plan.database_spec,
            schema_name: plan.schema_name.clone(),
            oid,
        };
        match self
            .catalog_transact(Some(session), vec![op], |_| Ok(()))
            .await
        {
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

    pub(crate) async fn sequence_create_role(
        &mut self,
        session: &Session,
        plan: CreateRolePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let oid = self.catalog.allocate_oid()?;
        let op = catalog::Op::CreateRole {
            name: plan.name,
            oid,
        };
        self.catalog_transact(Some(session), vec![op], |_| Ok(()))
            .await
            .map(|_| ExecuteResponse::CreatedRole)
    }

    // Utility function used by both `sequence_create_compute_instance`
    // and `sequence_create_compute_replica`. Chooses the availability zone
    // for a replica arbitrarily based on some state (currently: the number of replicas
    // of the given cluster per AZ).
    //
    // I put this in the `Coordinator`'s impl block in case we ever want to change the logic
    // and make it depend on some other state, but for now it's a pure function of the `n_replicas_per_az`
    // state.
    fn choose_az<'a>(n_replicas_per_az: &'a HashMap<String, usize>) -> String {
        let min = *n_replicas_per_az
            .values()
            .min()
            .expect("Must have at least one availability zone");
        let first_argmin = n_replicas_per_az
            .iter()
            .find_map(|(k, v)| (*v == min).then_some(k))
            .expect("Must have at least one availability zone");
        first_argmin.clone()
    }

    async fn sequence_create_compute_instance(
        &mut self,
        session: &Session,
        CreateComputeInstancePlan { name, replicas }: CreateComputeInstancePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        tracing::debug!("sequence_create_compute_instance");

        // The catalog items for the arranged introspection sources are shared between all replicas
        // of a compute instance, so we create them unconditionally during instance creation.
        // Whether a replica actually maintains introspection arrangements is determined by the
        // per-replica introspection configuration.
        let arranged_introspection_sources =
            self.catalog.allocate_arranged_introspection_sources().await;
        let arranged_introspection_source_ids: Vec<_> = arranged_introspection_sources
            .iter()
            .map(|(_, id)| *id)
            .collect();
        let mut ops = vec![catalog::Op::CreateComputeInstance {
            name: name.clone(),
            arranged_introspection_sources: arranged_introspection_sources.clone(),
        }];

        let azs = self.catalog.state().availability_zones();
        let mut n_replicas_per_az = azs
            .iter()
            .map(|s| (s.clone(), 0))
            .collect::<HashMap<_, _>>();
        for (_name, r) in replicas.iter() {
            if let Some(az) = r.get_az() {
                let ct: &mut usize = n_replicas_per_az.get_mut(az).ok_or_else(|| {
                    AdapterError::InvalidClusterReplicaAz {
                        az: az.to_string(),
                        expected: azs.to_vec(),
                    }
                })?;
                *ct += 1
            }
        }

        // This vector collects introspection sources of all replicas of this compute instance
        let mut persisted_introspection_sources = Vec::new();

        for (replica_name, replica_config) in replicas.into_iter() {
            // If the AZ was not specified, choose one, round-robin, from the ones with
            // the lowest number of configured replicas for this cluster.
            let (location, introspection) = match replica_config {
                mz_sql::plan::ComputeReplicaConfig::Remote {
                    addrs,
                    compute_addrs,
                    workers,
                    introspection,
                } => {
                    let location = SerializedComputeReplicaLocation::Remote {
                        addrs,
                        compute_addrs,
                        workers,
                    };
                    (location, introspection)
                }
                mz_sql::plan::ComputeReplicaConfig::Managed {
                    size,
                    availability_zone,
                    introspection,
                } => {
                    let (availability_zone, user_specified) =
                        availability_zone.map(|az| (az, true)).unwrap_or_else(|| {
                            let az = Self::choose_az(&n_replicas_per_az);
                            *n_replicas_per_az.get_mut(&az).unwrap() += 1;
                            (az, false)
                        });
                    let location = SerializedComputeReplicaLocation::Managed {
                        size,
                        availability_zone,
                        az_user_specified: user_specified,
                    };
                    (location, introspection)
                }
            };

            let logging = if let Some(config) = introspection {
                let sources = self
                    .catalog
                    .allocate_persisted_introspection_sources()
                    .await;
                let views = self.catalog.allocate_persisted_introspection_views().await;
                ComputeReplicaLogging {
                    log_logging: config.debugging,
                    interval: Some(config.interval),
                    sources,
                    views,
                }
            } else {
                ComputeReplicaLogging::default()
            };

            persisted_introspection_sources.extend(
                logging
                    .sources
                    .iter()
                    .map(|(variant, id)| (*id, variant.desc().into())),
            );

            let config = ComputeReplicaConfig {
                location: self.catalog.concretize_replica_location(location)?,
                logging,
            };

            ops.push(catalog::Op::CreateComputeReplica {
                name: replica_name,
                config,
                on_cluster_name: name.clone(),
            });
        }

        self.catalog_transact(Some(session), ops, |_| Ok(()))
            .await?;

        let persisted_introspection_source_ids: Vec<GlobalId> = persisted_introspection_sources
            .iter()
            .map(|(id, _)| *id)
            .collect();

        self.controller
            .storage
            .create_collections(persisted_introspection_sources)
            .await
            .unwrap();

        let instance = self
            .catalog
            .resolve_compute_instance(&name)
            .expect("compute instance must exist after creation");
        let instance_id = instance.id;

        let arranged_logs = arranged_introspection_sources
            .into_iter()
            .map(|(log, id)| (log.variant.clone(), id))
            .collect();
        self.controller.compute.create_instance(
            instance_id,
            arranged_logs,
            self.catalog.system_config().max_result_size(),
        )?;
        for (replica_id, replica) in instance.replicas_by_id.clone() {
            self.controller
                .active_compute()
                .add_replica_to_instance(instance_id, replica_id, replica.config)
                .await
                .unwrap();
        }

        if !arranged_introspection_source_ids.is_empty() {
            self.initialize_compute_read_policies(
                arranged_introspection_source_ids,
                instance_id,
                DEFAULT_LOGICAL_COMPACTION_WINDOW_MS,
            )
            .await;
        }

        if !persisted_introspection_source_ids.is_empty() {
            self.initialize_compute_read_policies(
                persisted_introspection_source_ids.clone(),
                instance_id,
                DEFAULT_LOGICAL_COMPACTION_WINDOW_MS,
            )
            .await;
            self.initialize_storage_read_policies(
                persisted_introspection_source_ids,
                DEFAULT_LOGICAL_COMPACTION_WINDOW_MS,
            )
            .await;
        }

        Ok(ExecuteResponse::CreatedComputeInstance)
    }

    async fn sequence_create_compute_replica(
        &mut self,
        session: &Session,
        CreateComputeReplicaPlan {
            name,
            of_cluster,
            config,
        }: CreateComputeReplicaPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        // Choose default AZ if necessary
        let (location, introspection) = match config {
            mz_sql::plan::ComputeReplicaConfig::Remote {
                addrs,
                compute_addrs,
                workers,
                introspection,
            } => {
                let location = SerializedComputeReplicaLocation::Remote {
                    addrs,
                    compute_addrs,
                    workers,
                };
                (location, introspection)
            }
            mz_sql::plan::ComputeReplicaConfig::Managed {
                size,
                availability_zone,
                introspection,
            } => {
                let (availability_zone, user_specified) = match availability_zone {
                    Some(az) => {
                        let azs = self.catalog.state().availability_zones();
                        if !azs.contains(&az) {
                            return Err(AdapterError::InvalidClusterReplicaAz {
                                az,
                                expected: azs.to_vec(),
                            });
                        }
                        (az, true)
                    }
                    None => {
                        // Choose the least popular AZ among all replicas of this cluster as the default
                        // if none was specified. If there is a tie for "least popular", pick the first one.
                        // That is globally unbiased (for Materialize, not necessarily for this customer)
                        // because we shuffle the AZs on boot in `crate::serve`.
                        let instance = self.catalog.resolve_compute_instance(&of_cluster)?;
                        let azs = self.catalog.state().availability_zones();
                        let mut n_replicas_per_az = azs
                            .iter()
                            .map(|s| (s.clone(), 0))
                            .collect::<HashMap<_, _>>();
                        for r in instance.replicas_by_id.values() {
                            if let Some(az) = r.config.location.get_az() {
                                *n_replicas_per_az.get_mut(az).expect("unknown AZ") += 1;
                            }
                        }
                        let az = Self::choose_az(&n_replicas_per_az);
                        (az, false)
                    }
                };
                let location = SerializedComputeReplicaLocation::Managed {
                    size,
                    availability_zone,
                    az_user_specified: user_specified,
                };
                (location, introspection)
            }
        };

        let logging = if let Some(config) = introspection {
            let sources = self
                .catalog
                .allocate_persisted_introspection_sources()
                .await;
            let views = self.catalog.allocate_persisted_introspection_views().await;
            ComputeReplicaLogging {
                log_logging: config.debugging,
                interval: Some(config.interval),
                sources,
                views,
            }
        } else {
            ComputeReplicaLogging::default()
        };

        let log_source_ids: Vec<_> = logging.source_ids().collect();
        let log_source_collections = logging
            .sources
            .iter()
            .map(|(variant, id)| (*id, variant.desc().into()))
            .collect();

        let config = ComputeReplicaConfig {
            location: self.catalog.concretize_replica_location(location)?,
            logging,
        };

        let op = catalog::Op::CreateComputeReplica {
            name: name.clone(),
            config,
            on_cluster_name: of_cluster.clone(),
        };

        self.catalog_transact(Some(session), vec![op], |_| Ok(()))
            .await?;

        let instance = self.catalog.resolve_compute_instance(&of_cluster)?;
        let replica_id = instance.replica_id_by_name[&name];
        let replica_concrete_config = instance.replicas_by_id[&replica_id].config.clone();

        self.controller
            .storage
            .create_collections(log_source_collections)
            .await
            .unwrap();

        let instance = self.catalog.resolve_compute_instance(&of_cluster)?;
        let instance_id = instance.id;
        let replica_id = instance.replica_id_by_name[&name];

        self.controller
            .active_compute()
            .add_replica_to_instance(instance_id, replica_id, replica_concrete_config)
            .await
            .unwrap();

        if !log_source_ids.is_empty() {
            self.initialize_compute_read_policies(
                log_source_ids.clone(),
                instance_id,
                DEFAULT_LOGICAL_COMPACTION_WINDOW_MS,
            )
            .await;
            self.initialize_storage_read_policies(
                log_source_ids,
                DEFAULT_LOGICAL_COMPACTION_WINDOW_MS,
            )
            .await;
        }

        Ok(ExecuteResponse::CreatedComputeReplica)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn sequence_create_table(
        &mut self,
        session: &mut Session,
        plan: CreateTablePlan,
        depends_on: Vec<GlobalId>,
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
        let table_id = self.catalog.allocate_user_id().await?;
        let table = catalog::Table {
            create_sql: table.create_sql,
            desc: table.desc,
            defaults: table.defaults,
            conn_id,
            depends_on,
        };
        let table_oid = self.catalog.allocate_oid()?;
        let ops = vec![catalog::Op::CreateItem {
            id: table_id,
            oid: table_oid,
            name: name.clone(),
            item: CatalogItem::Table(table.clone()),
        }];
        match self.catalog_transact(Some(session), ops, |_| Ok(())).await {
            Ok(()) => {
                // Determine the initial validity for the table.
                let since_ts = self.peek_local_write_ts();

                let collection_desc = table.desc.clone().into();
                self.controller
                    .storage
                    .create_collections(vec![(table_id, collection_desc)])
                    .await
                    .unwrap();

                let policy = ReadPolicy::ValidFrom(Antichain::from_elem(since_ts));
                self.controller
                    .storage
                    .set_read_policy(vec![(table_id, policy)])
                    .await
                    .unwrap();

                self.initialize_storage_read_policies(
                    vec![table_id],
                    DEFAULT_LOGICAL_COMPACTION_WINDOW_MS,
                )
                .await;
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

    async fn sequence_create_secret(
        &mut self,
        session: &mut Session,
        plan: CreateSecretPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let CreateSecretPlan {
            name,
            mut secret,
            full_name,
            if_not_exists,
        } = plan;

        let payload = self.extract_secret(session, &mut secret.secret_as)?;

        let id = self.catalog.allocate_user_id().await?;
        let oid = self.catalog.allocate_oid()?;
        let secret = catalog::Secret {
            create_sql: format!("CREATE SECRET {} AS '********'", full_name),
        };

        self.secrets_controller.ensure(id, &payload).await?;

        let ops = vec![catalog::Op::CreateItem {
            id,
            oid,
            name: name.clone(),
            item: CatalogItem::Secret(secret.clone()),
        }];

        match self.catalog_transact(Some(session), ops, |_| Ok(())).await {
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

    async fn sequence_create_sink(
        &mut self,
        mut session: Session,
        plan: CreateSinkPlan,
        depends_on: Vec<GlobalId>,
        tx: ClientTransmitter<ExecuteResponse>,
    ) {
        let CreateSinkPlan {
            name,
            sink,
            with_snapshot,
            if_not_exists,
            host_config,
        } = plan;

        // First try to allocate an ID and an OID. If either fails, we're done.
        let id = match self.catalog.allocate_user_id().await {
            Ok(id) => id,
            Err(e) => {
                tx.send(Err(e.into()), session);
                return;
            }
        };
        let oid = match self.catalog.allocate_oid() {
            Ok(id) => id,
            Err(e) => {
                tx.send(Err(e.into()), session);
                return;
            }
        };

        // Validate the storage host config. The size can only be undefined
        // in unsafe mode.
        let allow_undefined_size = self.catalog.config().unsafe_mode;
        let host_config = match self
            .catalog
            .resolve_storage_host_config(host_config, allow_undefined_size)
        {
            Ok(host_config) => host_config,
            Err(e) => {
                tx.send(Err(e), session);
                return;
            }
        };

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
            depends_on,
            host_config,
        };

        let ops = vec![catalog::Op::CreateItem {
            id,
            oid,
            name: name.clone(),
            item: CatalogItem::Sink(catalog_sink.clone()),
        }];

        let from = self.catalog.get_entry(&catalog_sink.from);
        let from_name = from.name().item.clone();
        let from_type = from.item().typ().to_string();
        let result = self
            .catalog_transact(Some(&session), ops, move |txn| {
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
                session.add_notice(AdapterNotice::ObjectAlreadyExists {
                    name: name.item,
                    ty: "sink",
                });
                tx.send(Ok(ExecuteResponse::CreatedSink), session);
                return;
            }
            Err(e) => {
                tx.send(Err(e), session);
                return;
            }
        }

        let create_export_token = match self
            .controller
            .storage
            .prepare_export(id, catalog_sink.from)
            .await
        {
            Ok(t) => t,
            Err(e) => {
                tx.send(Err(e.into()), session);
                return;
            }
        };

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
                        session_and_tx: Some((session, tx)),
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

    async fn sequence_create_view(
        &mut self,
        session: &mut Session,
        plan: CreateViewPlan,
        depends_on: Vec<GlobalId>,
    ) -> Result<ExecuteResponse, AdapterError> {
        let if_not_exists = plan.if_not_exists;
        let ops = self
            .generate_view_ops(
                session,
                plan.name.clone(),
                plan.view.clone(),
                plan.replace,
                depends_on,
            )
            .await?;
        match self.catalog_transact(Some(session), ops, |_| Ok(())).await {
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
        name: QualifiedObjectName,
        view: View,
        replace: Option<GlobalId>,
        depends_on: Vec<GlobalId>,
    ) -> Result<Vec<catalog::Op>, AdapterError> {
        self.validate_timeline(view.expr.depends_on())?;

        let mut ops = vec![];

        if let Some(id) = replace {
            ops.extend(self.catalog.drop_items_ops(&[id]));
        }
        let view_id = self.catalog.allocate_user_id().await?;
        let view_oid = self.catalog.allocate_oid()?;
        let optimized_expr = self.view_optimizer.optimize(view.expr)?;
        let desc = RelationDesc::new(optimized_expr.typ(), view.column_names);
        let view = catalog::View {
            create_sql: view.create_sql,
            optimized_expr,
            desc,
            conn_id: if view.temporary {
                Some(session.conn_id())
            } else {
                None
            },
            depends_on,
        };
        ops.push(catalog::Op::CreateItem {
            id: view_id,
            oid: view_oid,
            name: name.clone(),
            item: CatalogItem::View(view),
        });

        Ok(ops)
    }

    async fn sequence_create_materialized_view(
        &mut self,
        session: &mut Session,
        plan: CreateMaterializedViewPlan,
        depends_on: Vec<GlobalId>,
    ) -> Result<ExecuteResponse, AdapterError> {
        let CreateMaterializedViewPlan {
            name,
            materialized_view:
                MaterializedView {
                    create_sql,
                    expr: view_expr,
                    column_names,
                    compute_instance,
                },
            replace,
            if_not_exists,
        } = plan;

        self.validate_timeline(depends_on.clone())?;

        // Materialized views are not allowed to depend on log sources, as replicas
        // are not producing the same definite collection for these.
        // TODO(teskje): Remove this check once arrangement-based log sources
        // are replaced with persist-based ones.
        let log_names = depends_on
            .iter()
            .flat_map(|id| self.catalog.arranged_introspection_dependencies(*id))
            .map(|id| self.catalog.get_entry(&id).name().item.clone())
            .collect::<Vec<_>>();
        if !log_names.is_empty() {
            return Err(AdapterError::InvalidLogDependency {
                object_type: "materialized view".into(),
                log_names,
            });
        }

        // Allocate IDs for the materialized view in the catalog.
        let id = self.catalog.allocate_user_id().await?;
        let oid = self.catalog.allocate_oid()?;
        // Allocate a unique ID that can be used by the dataflow builder to
        // connect the view dataflow to the storage sink.
        let internal_view_id = self.allocate_transient_id()?;

        let optimized_expr = self.view_optimizer.optimize(view_expr)?;
        let desc = RelationDesc::new(optimized_expr.typ(), column_names);

        // Pick the least valid read timestamp as the as-of for the view
        // dataflow. This makes the materialized view include the maximum possible
        // amount of historical detail.
        let id_bundle = self
            .index_oracle(compute_instance)
            .sufficient_collections(&depends_on);
        let as_of = self.least_valid_read(&id_bundle);

        let mut ops = Vec::new();
        if let Some(drop_id) = replace {
            ops.extend(self.catalog.drop_items_ops(&[drop_id]));
        }
        ops.push(catalog::Op::CreateItem {
            id,
            oid,
            name: name.clone(),
            item: CatalogItem::MaterializedView(catalog::MaterializedView {
                create_sql,
                optimized_expr,
                desc: desc.clone(),
                depends_on,
                compute_instance,
            }),
        });

        match self
            .catalog_transact(Some(session), ops, |txn| {
                // Create a dataflow that materializes the view query and sinks
                // it to storage.
                let df = txn
                    .dataflow_builder(compute_instance)
                    .build_materialized_view_dataflow(id, as_of.clone(), internal_view_id)?;
                Ok(df)
            })
            .await
        {
            Ok(df) => {
                // Announce the creation of the materialized view source.
                self.controller
                    .storage
                    .create_collections(vec![(
                        id,
                        CollectionDescription {
                            desc,
                            data_source: DataSource::Other,
                            since: Some(as_of),
                            status_collection_id: None,
                        },
                    )])
                    .await
                    .unwrap();

                self.initialize_storage_read_policies(
                    vec![id],
                    DEFAULT_LOGICAL_COMPACTION_WINDOW_MS,
                )
                .await;

                self.ship_dataflow(df, compute_instance).await;

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

    async fn sequence_create_index(
        &mut self,
        session: &mut Session,
        plan: CreateIndexPlan,
        depends_on: Vec<GlobalId>,
    ) -> Result<ExecuteResponse, AdapterError> {
        let CreateIndexPlan {
            name,
            index,
            options,
            if_not_exists,
        } = plan;

        // An index must be created on a specific compute instance.
        let compute_instance = index.compute_instance;

        let id = self.catalog.allocate_user_id().await?;
        let index = catalog::Index {
            create_sql: index.create_sql,
            keys: index.keys,
            on: index.on,
            conn_id: None,
            depends_on,
            compute_instance,
        };
        let oid = self.catalog.allocate_oid()?;
        let op = catalog::Op::CreateItem {
            id,
            oid,
            name: name.clone(),
            item: CatalogItem::Index(index),
        };
        match self
            .catalog_transact(Some(session), vec![op], |txn| {
                let mut builder = txn.dataflow_builder(compute_instance);
                let df = builder.build_index_dataflow(id)?;
                Ok(df)
            })
            .await
        {
            Ok(df) => {
                self.ship_dataflow(df, compute_instance).await;
                self.set_index_options(id, options)
                    .await
                    .expect("index enabled");
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

    async fn sequence_create_type(
        &mut self,
        session: &Session,
        plan: CreateTypePlan,
        depends_on: Vec<GlobalId>,
    ) -> Result<ExecuteResponse, AdapterError> {
        let typ = catalog::Type {
            create_sql: plan.typ.create_sql,
            details: CatalogTypeDetails {
                array_id: None,
                typ: plan.typ.inner,
            },
            depends_on,
        };
        let id = self.catalog.allocate_user_id().await?;
        let oid = self.catalog.allocate_oid()?;
        let op = catalog::Op::CreateItem {
            id,
            oid,
            name: plan.name,
            item: CatalogItem::Type(typ),
        };
        match self
            .catalog_transact(Some(session), vec![op], |_| Ok(()))
            .await
        {
            Ok(()) => Ok(ExecuteResponse::CreatedType),
            Err(err) => Err(err),
        }
    }

    async fn sequence_drop_database(
        &mut self,
        session: &Session,
        plan: DropDatabasePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let ops = self.catalog.drop_database_ops(plan.id);
        self.catalog_transact(Some(session), ops, |_| Ok(()))
            .await?;
        Ok(ExecuteResponse::DroppedDatabase)
    }

    async fn sequence_drop_schema(
        &mut self,
        session: &Session,
        plan: DropSchemaPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let ops = self.catalog.drop_schema_ops(plan.id);
        self.catalog_transact(Some(session), ops, |_| Ok(()))
            .await?;
        Ok(ExecuteResponse::DroppedSchema)
    }

    async fn sequence_drop_roles(
        &mut self,
        session: &Session,
        plan: DropRolesPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let ops = plan
            .names
            .into_iter()
            .map(|name| catalog::Op::DropRole { name })
            .collect();
        self.catalog_transact(Some(session), ops, |_| Ok(()))
            .await?;
        Ok(ExecuteResponse::DroppedRole)
    }

    async fn sequence_drop_compute_instances(
        &mut self,
        session: &Session,
        plan: DropComputeInstancesPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let mut ops = Vec::new();
        let mut instance_replica_drop_sets = Vec::with_capacity(plan.names.len());
        for compute_name in plan.names {
            let instance = self.catalog.resolve_compute_instance(&compute_name)?;
            instance_replica_drop_sets.push((
                instance.id,
                instance.replicas_by_id.keys().cloned().collect::<Vec<_>>(),
            ));
            for replica_name in instance.replica_id_by_name.keys() {
                ops.push(catalog::Op::DropComputeReplica {
                    name: replica_name.to_string(),
                    compute_name: compute_name.clone(),
                });
            }

            let mut ids_to_drop: Vec<GlobalId> = instance.exports().iter().cloned().collect();

            // Determine from the replica which additional items to drop. This is the set
            // of items that depend on the introspection sources. The sources
            // itself are removed with Op::DropComputeReplica.
            for replica in instance.replicas_by_id.values() {
                let logging = &replica.config.logging;
                let log_and_view_ids = logging.source_and_view_ids();
                let view_ids: HashSet<_> = logging.view_ids().collect();
                for log_id in log_and_view_ids {
                    // We consider the dependencies of both views and logs, but remove the
                    // views itself. The views are included as they depend on the source,
                    // but we dont need an explicit Op::DropItem as they are dropped together
                    // with the replica.
                    ids_to_drop.extend(
                        self.catalog
                            .get_entry(&log_id)
                            .used_by()
                            .into_iter()
                            .filter(|x| !view_ids.contains(x)),
                    )
                }
            }

            ops.extend(self.catalog.drop_items_ops(&ids_to_drop));
            ops.push(catalog::Op::DropComputeInstance { name: compute_name });
        }

        self.catalog_transact(Some(session), ops, |_| Ok(()))
            .await?;
        for (instance_id, replicas) in instance_replica_drop_sets {
            for replica_id in replicas {
                self.drop_replica(instance_id, replica_id).await.unwrap();
            }
            self.controller.compute.drop_instance(instance_id);
        }

        Ok(ExecuteResponse::DroppedComputeInstance)
    }

    async fn sequence_drop_compute_replica(
        &mut self,
        session: &Session,
        DropComputeReplicasPlan { names }: DropComputeReplicasPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        if names.is_empty() {
            return Ok(ExecuteResponse::DroppedComputeReplica);
        }
        let mut ops = Vec::with_capacity(names.len());
        let mut replicas_to_drop = Vec::with_capacity(names.len());
        let mut ids_to_drop = vec![];
        for (instance_name, replica_name) in names {
            let instance = self.catalog.resolve_compute_instance(&instance_name)?;
            ops.push(catalog::Op::DropComputeReplica {
                name: replica_name.clone(),
                compute_name: instance_name.clone(),
            });
            let replica_id = instance.replica_id_by_name[&replica_name];

            // Determine from the replica which additional items to drop. This is the set
            // of items that depend on the introspection sources. The sources
            // itself are removed with Op::DropComputeReplica.
            let logging = &instance
                .replicas_by_id
                .get(&replica_id)
                .unwrap()
                .config
                .logging;

            let log_and_view_ids = logging.source_and_view_ids();
            let view_ids: HashSet<_> = logging.view_ids().collect();

            for log_id in log_and_view_ids {
                // We consider the dependencies of both views and logs, but remove the
                // views itself. The views are included as they depend on the source,
                // but we dont need an explicit Op::DropItem as they are dropped together
                // with the replica.
                ids_to_drop.extend(
                    self.catalog
                        .get_entry(&log_id)
                        .used_by()
                        .into_iter()
                        .filter(|x| !view_ids.contains(x)),
                )
            }

            replicas_to_drop.push((instance.id, replica_id));
        }

        ops.extend(self.catalog.drop_items_ops(&ids_to_drop));

        self.catalog_transact(Some(session), ops, |_| Ok(()))
            .await?;

        for (compute_id, replica_id) in replicas_to_drop {
            self.drop_replica(compute_id, replica_id).await.unwrap();
        }

        Ok(ExecuteResponse::DroppedComputeReplica)
    }

    async fn drop_replica(
        &mut self,
        instance_id: ComputeInstanceId,
        replica_id: ReplicaId,
    ) -> Result<(), anyhow::Error> {
        if let Some(Some(metadata)) = self.transient_replica_metadata.insert(replica_id, None) {
            let retraction = self
                .catalog
                .state()
                .pack_replica_heartbeat_update(replica_id, metadata, -1);
            self.send_builtin_table_updates(vec![retraction], BuiltinTableUpdateSource::Background)
                .await;
        }
        self.controller
            .active_compute()
            .drop_replica(instance_id, replica_id)
            .await?;
        Ok(())
    }

    async fn sequence_drop_items(
        &mut self,
        session: &Session,
        plan: DropItemsPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let ops = self.catalog.drop_items_ops(&plan.items);
        self.catalog_transact(Some(session), ops, |_| Ok(()))
            .await?;
        Ok(match plan.ty {
            ObjectType::Source => ExecuteResponse::DroppedSource,
            ObjectType::View => ExecuteResponse::DroppedView,
            ObjectType::MaterializedView => ExecuteResponse::DroppedMaterializedView,
            ObjectType::Table => ExecuteResponse::DroppedTable,
            ObjectType::Sink => ExecuteResponse::DroppedSink,
            ObjectType::Index => ExecuteResponse::DroppedIndex,
            ObjectType::Type => ExecuteResponse::DroppedType,
            ObjectType::Secret => ExecuteResponse::DroppedSecret,
            ObjectType::Connection => ExecuteResponse::DroppedConnection,
            ObjectType::Role | ObjectType::Cluster | ObjectType::ClusterReplica => {
                unreachable!("handled through their respective sequence_drop functions")
            }
            ObjectType::Object => unreachable!("generic OBJECT cannot be dropped"),
        })
    }

    fn sequence_show_all_variables(
        &mut self,
        session: &Session,
    ) -> Result<ExecuteResponse, AdapterError> {
        Ok(send_immediate_rows(
            session
                .vars()
                .iter()
                .chain(self.catalog.system_config().iter())
                .filter(|v| !v.experimental())
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

    fn sequence_show_variable(
        &self,
        session: &Session,
        plan: ShowVariablePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let variable = session
            .vars()
            .get(&plan.name)
            .or_else(|_| self.catalog.system_config().get(&plan.name))?;
        let row = Row::pack_slice(&[Datum::String(&variable.value())]);
        Ok(send_immediate_rows(vec![row]))
    }

    fn sequence_set_variable(
        &self,
        session: &mut Session,
        plan: SetVariablePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        use mz_sql::ast::{SetVariableValue, Value};

        let vars = session.vars_mut();
        let (name, local) = (plan.name, plan.local);
        match plan.value {
            SetVariableValue::Default => vars.reset(&name, local)?,
            SetVariableValue::Literal(Value::String(s)) => vars.set(&name, &s, local)?, // pass-through unquoted strings
            SetVariableValue::Ident(ident) => vars.set(&name, &ident.into_string(), local)?, // pass-through unquoted idents
            value => vars.set(&name, &value.to_string(), local)?, // or else use the AstDisplay for SetVariableValue
        }

        Ok(ExecuteResponse::SetVariable { name, reset: false })
    }

    fn sequence_reset_variable(
        &self,
        session: &mut Session,
        plan: ResetVariablePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        session.vars_mut().reset(&plan.name, false)?;
        Ok(ExecuteResponse::SetVariable {
            name: plan.name,
            reset: true,
        })
    }

    pub(crate) async fn sequence_end_transaction(
        &mut self,
        tx: ClientTransmitter<ExecuteResponse>,
        mut session: Session,
        mut action: EndTransactionAction,
    ) {
        // If the transaction has failed, we can only rollback.
        if let (EndTransactionAction::Commit, TransactionStatus::Failed(_)) =
            (&action, session.transaction())
        {
            action = EndTransactionAction::Rollback;
        }
        let response = match action {
            EndTransactionAction::Commit => Ok(ExecuteResponse::TransactionCommitted),
            EndTransactionAction::Rollback => Ok(ExecuteResponse::TransactionRolledBack),
        };

        let result = self
            .sequence_end_transaction_inner(&mut session, action)
            .await;

        let (response, action) = match result {
            Ok((Some(TransactionOps::Writes(writes)), _)) if writes.is_empty() => {
                (response, action)
            }
            Ok((Some(TransactionOps::Writes(writes)), write_lock_guard)) => {
                self.submit_write(PendingWriteTxn::User {
                    writes,
                    write_lock_guard,
                    pending_txn: PendingTxn {
                        client_transmitter: tx,
                        response,
                        session,
                        action,
                    },
                });
                return;
            }
            Ok((Some(TransactionOps::Peeks(timestamp)), _))
                if session.vars().transaction_isolation()
                    == &IsolationLevel::StrictSerializable =>
            {
                self.strict_serializable_reads_tx
                    .send(PendingReadTxn::Read {
                        txn: PendingTxn {
                            client_transmitter: tx,
                            response,
                            session,
                            action,
                        },
                        timestamp,
                    })
                    .expect("sending to strict_serializable_reads_tx cannot fail");
                return;
            }
            Ok((_, _)) => (response, action),
            Err(err) => (Err(err), EndTransactionAction::Rollback),
        };
        session.vars_mut().end_transaction(action);
        tx.send(response, session);
    }

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
                if let TransactionOps::Writes(writes) = &mut ops {
                    for WriteOp { id, .. } in &mut writes.iter() {
                        // Re-verify this id exists.
                        let _ = self.catalog.try_get_entry(id).ok_or_else(|| {
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

    /// Sequence a peek, determining a timestamp and the most efficient dataflow interaction.
    ///
    /// Peeks are sequenced by assigning a timestamp for evaluation, and then determining and
    /// deploying the most efficient evaluation plan. The peek could evaluate to a constant,
    /// be a simple read out of an existing arrangement, or required a new dataflow to build
    /// the results to return.
    #[tracing::instrument(level = "debug", skip_all)]
    async fn sequence_peek(
        &mut self,
        session: &mut Session,
        plan: PeekPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        event!(Level::TRACE, plan = format!("{:?}", plan));

        let PeekPlan {
            mut source,
            when,
            finishing,
            copy_to,
        } = plan;

        let compute_instance = self.catalog.active_compute_instance(session)?;

        let target_replica_name = session.vars().cluster_replica();
        let mut target_replica = target_replica_name
            .map(|name| {
                compute_instance
                    .replica_id_by_name
                    .get(name)
                    .copied()
                    .ok_or(AdapterError::UnknownClusterReplica {
                        cluster_name: compute_instance.name.clone(),
                        replica_name: name.to_string(),
                    })
            })
            .transpose()?;

        if compute_instance.replicas_by_id.is_empty() {
            return Err(AdapterError::NoClusterReplicasAvailable(
                compute_instance.name.clone(),
            ));
        }

        let source_ids = source.depends_on();
        check_no_invalid_log_reads(
            &self.catalog,
            compute_instance,
            &source_ids,
            LogReadStyle::Peek(&mut target_replica),
        )?;

        let compute_instance = compute_instance.id;

        let timeline = self.validate_timeline(source_ids.clone())?;
        let conn_id = session.conn_id();
        // Queries are independent of the logical timestamp iff there are no referenced
        // sources or indexes and there is no reference to `mz_now()`.
        let timestamp_independent = source_ids.is_empty() && !source.contains_temporal();
        // For transactions that do not use AS OF, get the
        // timestamp of the in-progress transaction or create one. If this is an AS OF
        // query, we don't care about any possible transaction timestamp. If this is a
        // single-statement transaction (TransactionStatus::Started), we don't need to
        // worry about preventing compaction or choosing a valid timestamp for future
        // queries.
        let timestamp = if session.transaction().is_in_multi_statement_transaction()
            && when == QueryWhen::Immediately
        {
            // If all previous statements were timestamp-independent and the current one is
            // not, clear the transaction ops so it can get a new timestamp and timedomain.
            if let Some(read_txn) = self.txn_reads.get(&conn_id) {
                if read_txn.timestamp_independent && !timestamp_independent {
                    session.clear_transaction_ops();
                }
            }

            let timestamp = match session.get_transaction_timestamp() {
                Some(ts) => ts,
                _ => {
                    // Determine a timestamp that will be valid for anything in any schema
                    // referenced by the first query.
                    let id_bundle =
                        self.timedomain_for(&source_ids, &timeline, conn_id, compute_instance)?;

                    // We want to prevent compaction of the indexes consulted by
                    // determine_timestamp, not the ones listed in the query.
                    let timestamp = self.determine_timestamp(
                        session,
                        &id_bundle,
                        &QueryWhen::Immediately,
                        compute_instance,
                        &timeline,
                    )?;
                    let read_holds = self.acquire_read_holds(timestamp, id_bundle).await;
                    let txn_reads = TxnReads {
                        timestamp_independent,
                        read_holds,
                    };
                    self.txn_reads.insert(conn_id, txn_reads);
                    timestamp
                }
            };

            // Verify that the references and indexes for this query are in the
            // current read transaction.
            let id_bundle = self
                .index_oracle(compute_instance)
                .sufficient_collections(&source_ids);
            let allowed_id_bundle = &self.txn_reads.get(&conn_id).unwrap().read_holds.id_bundle();
            // Find the first reference or index (if any) that is not in the transaction. A
            // reference could be caused by a user specifying an object in a different
            // schema than the first query. An index could be caused by a CREATE INDEX
            // after the transaction started.
            let outside = id_bundle.difference(allowed_id_bundle);
            if !outside.is_empty() {
                let mut names: Vec<_> = allowed_id_bundle
                    .iter()
                    // This could filter out a view that has been replaced in another transaction.
                    .filter_map(|id| self.catalog.try_get_entry(&id))
                    .map(|item| item.name())
                    .map(|name| {
                        self.catalog
                            .resolve_full_name(name, Some(session.conn_id()))
                            .to_string()
                    })
                    .collect();
                let mut outside: Vec<_> = outside
                    .iter()
                    .filter_map(|id| self.catalog.try_get_entry(&id))
                    .map(|item| item.name())
                    .map(|name| {
                        self.catalog
                            .resolve_full_name(name, Some(session.conn_id()))
                            .to_string()
                    })
                    .collect();
                // Sort so error messages are deterministic.
                names.sort();
                outside.sort();
                return Err(AdapterError::RelationOutsideTimeDomain {
                    relations: outside,
                    names,
                });
            }

            timestamp
        } else {
            // TODO(guswynn): acquire_read_holds for linearized reads
            let id_bundle = self
                .index_oracle(compute_instance)
                .sufficient_collections(&source_ids);
            self.determine_timestamp(session, &id_bundle, &when, compute_instance, &timeline)?
        };

        // before we have the corrected timestamp ^
        // TODO(guswynn&mjibson): partition `sequence_peek` by the response to
        // `linearize_sources(source_ids.iter().collect()).await`
        // ------------------------------
        // after we have the timestamp \/

        let source = self.view_optimizer.optimize(source)?;

        // We create a dataflow and optimize it, to determine if we can avoid building it.
        // This can happen if the result optimizes to a constant, or to a `Get` expression
        // around a maintained arrangement.
        let typ = source.typ();
        let key: Vec<MirScalarExpr> = typ
            .default_key()
            .iter()
            .map(|k| MirScalarExpr::Column(*k))
            .collect();
        let (permutation, thinning) = permutation_for_arrangement(&key, typ.arity());
        // Two transient allocations. We could reclaim these if we don't use them, potentially.
        // TODO: reclaim transient identifiers in fast path cases.
        let view_id = self.allocate_transient_id()?;
        let index_id = self.allocate_transient_id()?;
        // The assembled dataflow contains a view and an index of that view.
        let mut dataflow = DataflowDesc::new(format!("temp-view-{}", view_id));
        dataflow.set_as_of(Antichain::from_elem(timestamp));
        let mut builder = self.dataflow_builder(compute_instance);
        builder.import_view_into_dataflow(&view_id, &source, &mut dataflow)?;
        for BuildDesc { plan, .. } in &mut dataflow.objects_to_build {
            prep_relation_expr(
                self.catalog.state(),
                plan,
                ExprPrepStyle::OneShot {
                    logical_time: Some(timestamp),
                    session,
                },
            )?;
        }
        dataflow.export_index(
            index_id,
            IndexDesc {
                on_id: view_id,
                key: key.clone(),
            },
            typ,
        );

        // Optimize the dataflow across views, and any other ways that appeal.
        mz_transform::optimize_dataflow(&mut dataflow, &builder.index_oracle())?;

        // At this point, `dataflow_plan` contains our best optimized dataflow.
        // We will check the plan to see if there is a fast path to escape full dataflow construction.
        let peek_plan = self.create_peek_plan(
            dataflow,
            view_id,
            compute_instance,
            index_id,
            key,
            permutation,
            thinning.len(),
        )?;

        // We only track the peeks in the session if the query doesn't use AS
        // OF or we're inside an explicit transaction. The latter case is
        // necessary to support PG's `BEGIN` semantics, whose behavior can
        // depend on whether or not reads have occurred in the txn.
        if matches!(session.transaction(), &TransactionStatus::InTransaction(_))
            || when == QueryWhen::Immediately
        {
            let peek_ts = if matches!(
                peek_plan,
                peek::PeekPlan::FastPath(peek::FastPathPlan::Constant(_, _))
            ) && timestamp_independent
            {
                None
            } else {
                Some((timestamp, timeline))
            };

            session.add_transaction_ops(TransactionOps::Peeks(peek_ts))?;
        }

        // Implement the peek, and capture the response.
        let resp = self
            .implement_peek_plan(
                peek_plan,
                timestamp,
                finishing,
                conn_id,
                source.arity(),
                compute_instance,
                target_replica,
            )
            .await?;

        match copy_to {
            None => Ok(resp),
            Some(format) => Ok(ExecuteResponse::CopyTo {
                format,
                resp: Box::new(resp),
            }),
        }
    }

    async fn sequence_subscribe(
        &mut self,
        session: &mut Session,
        plan: SubscribePlan,
        depends_on: Vec<GlobalId>,
    ) -> Result<ExecuteResponse, AdapterError> {
        let SubscribePlan {
            from,
            with_snapshot,
            when,
            copy_to,
            emit_progress,
        } = plan;

        let compute_instance = self.catalog.active_compute_instance(session)?;
        let compute_instance_id = compute_instance.id;

        // SUBSCRIBE AS OF, similar to peeks, doesn't need to worry about transaction
        // timestamp semantics.
        if when == QueryWhen::Immediately {
            // If this isn't a SUBSCRIBE AS OF, the SUBSCRIBE can be in a transaction if it's the
            // only operation.
            session.add_transaction_ops(TransactionOps::Subscribe)?;
        }

        let make_sink_desc = |coord: &mut Coordinator<S>, from, from_desc, uses| {
            // Determine the frontier of updates to subscribe *from*.
            // Updates greater or equal to this frontier will be produced.
            let id_bundle = coord
                .index_oracle(compute_instance_id)
                .sufficient_collections(uses);
            let timeline = coord.validate_timeline(id_bundle.iter())?;
            // If a timestamp was explicitly requested, use that.
            let timestamp = coord.determine_timestamp(
                session,
                &id_bundle,
                &when,
                compute_instance_id,
                &timeline,
            )?;

            Ok::<_, AdapterError>(ComputeSinkDesc {
                from,
                from_desc,
                connection: ComputeSinkConnection::Subscribe(SubscribeSinkConnection::default()),
                as_of: SinkAsOf {
                    frontier: Antichain::from_elem(timestamp),
                    strict: !with_snapshot,
                },
            })
        };

        let dataflow = match from {
            SubscribeFrom::Id(from_id) => {
                check_no_invalid_log_reads(
                    &self.catalog,
                    compute_instance,
                    &btreeset!(from_id),
                    LogReadStyle::Subscribe,
                )?;
                let from = self.catalog.get_entry(&from_id);
                let from_desc = from
                    .desc(
                        &self
                            .catalog
                            .resolve_full_name(from.name(), Some(session.conn_id())),
                    )
                    .unwrap()
                    .into_owned();
                let sink_id = self.catalog.allocate_user_id().await?;
                let sink_desc = make_sink_desc(self, from_id, from_desc, &[from_id][..])?;
                let sink_name = format!("subscribe-{}", sink_id);
                self.dataflow_builder(compute_instance_id)
                    .build_sink_dataflow(sink_name, sink_id, sink_desc)?
            }
            SubscribeFrom::Query { expr, desc } => {
                check_no_invalid_log_reads(
                    &self.catalog,
                    compute_instance,
                    &expr.depends_on(),
                    LogReadStyle::Subscribe,
                )?;
                let id = self.allocate_transient_id()?;
                let expr = self.view_optimizer.optimize(expr)?;
                let desc = RelationDesc::new(expr.typ(), desc.iter_names());
                let sink_desc = make_sink_desc(self, id, desc, &depends_on)?;
                let mut dataflow = DataflowDesc::new(format!("subscribe-{}", id));
                let mut dataflow_builder = self.dataflow_builder(compute_instance_id);
                dataflow_builder.import_view_into_dataflow(&id, &expr, &mut dataflow)?;
                dataflow_builder.build_sink_dataflow_into(&mut dataflow, id, sink_desc)?;
                dataflow
            }
        };

        let (sink_id, sink_desc) = dataflow.sink_exports.iter().next().unwrap();
        session.add_drop_sink(ComputeSinkId {
            compute_instance: compute_instance_id,
            global_id: *sink_id,
        });
        let arity = sink_desc.from_desc.arity();
        let (tx, rx) = mpsc::unbounded_channel();
        self.metrics.active_subscribes.inc();
        self.pending_subscribes
            .insert(*sink_id, PendingSubscribe::new(tx, emit_progress, arity));
        self.ship_dataflow(dataflow, compute_instance_id).await;

        let resp = ExecuteResponse::Subscribing { rx };
        match copy_to {
            None => Ok(resp),
            Some(format) => Ok(ExecuteResponse::CopyTo {
                format,
                resp: Box::new(resp),
            }),
        }
    }

    fn sequence_explain(
        &mut self,
        session: &Session,
        plan: ExplainPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        match plan.stage {
            ExplainStage::Timestamp => self.sequence_explain_timestamp(session, plan.raw_plan),
            _ => self.sequence_explain_plan(session, plan),
        }
    }

    fn sequence_explain_plan(
        &mut self,
        session: &Session,
        plan: ExplainPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        use mz_compute_client::plan::Plan;
        use mz_repr::explain_new::trace_plan;
        use ExplainStage::*;

        let compute_instance = self.catalog.active_compute_instance(session)?.id;

        let ExplainPlan {
            raw_plan,
            row_set_finishing,
            stage,
            format,
            config,
            explainee,
        } = plan;

        assert_ne!(stage, ExplainStage::Timestamp);

        let optimizer_trace = match stage {
            Trace => OptimizerTrace::new(), // collect all trace entries
            QueryGraph | OptimizedQueryGraph => OptimizerTrace::find(""), // don't collect anything
            stage => OptimizerTrace::find(stage.path()), // collect a trace entry only the selected stage
        };

        let (used_indexes, fast_path_plan) =
            optimizer_trace.collect_trace(|| -> Result<_, AdapterError> {
                let raw_plan = raw_plan.clone(); // FIXME: remove `.clone()` once the QGM Model implements Clone.
                let _span = tracing::span!(Level::INFO, "optimize").entered();

                tracing::span!(Level::INFO, "raw").in_scope(|| {
                    trace_plan(&raw_plan);
                });

                // run optimization pipeline
                let decorrelated_plan = raw_plan.optimize_and_lower(&OptimizerConfig {
                    qgm_optimizations: session.vars().qgm_optimizations(),
                })?;

                self.validate_timeline(decorrelated_plan.depends_on())?;

                let mut dataflow = tracing::span!(Level::INFO, "local").in_scope(
                    || -> Result<_, AdapterError> {
                        let optimized_plan = self.view_optimizer.optimize(decorrelated_plan)?;
                        let mut dataflow = DataflowDesc::new("explanation".to_string());
                        self.dataflow_builder(compute_instance)
                            .import_view_into_dataflow(
                                // TODO: If explaining a view, pipe the actual id of the view.
                                &GlobalId::Explain,
                                &optimized_plan,
                                &mut dataflow,
                            )?;
                        mz_repr::explain_new::trace_plan(&dataflow);
                        Ok(dataflow)
                    },
                )?;

                mz_transform::optimize_dataflow(
                    &mut dataflow,
                    &self.index_oracle(compute_instance),
                )?;

                let used_indexes = dataflow
                    .index_imports
                    .keys()
                    .cloned()
                    .collect::<Vec<GlobalId>>();

                let fast_path_plan = match explainee {
                    Explainee::Query => {
                        peek::create_fast_path_plan(&mut dataflow, GlobalId::Explain)?
                    }
                    _ => None,
                };

                let dataflow_plan = Plan::<mz_repr::Timestamp>::finalize_dataflow(dataflow)
                    .expect("Finalized dataflow");

                trace_plan(&dataflow_plan);

                Ok((used_indexes, fast_path_plan))
            })?;

        let trace = if matches!(stage, QueryGraph | OptimizedQueryGraph) {
            vec![] // FIXME: remove this case once the QGM Model implements Clone
        } else {
            optimizer_trace.drain_all(
                format.clone(), // FIXME: remove `.clone()` once the QGM Model implements Clone
                config.clone(), // FIXME: remove `.clone()` once the QGM Model implements Clone
                self.catalog.for_session(session),
                row_set_finishing.clone(), // FIXME: remove `.clone()` once the QGM Model implements Clone
                used_indexes,
                fast_path_plan,
            )?
        };

        let rows = match stage {
            // QGM graphs are not collected in the trace at the moment as they
            // do not implement Clone (see the TODOs in try_qgm_path.
            // Once this is done the next two cases will be handled by the catch-all
            // case at the end of this method.
            QueryGraph => {
                use mz_repr::explain_new::Explain;
                // run partial pipeline
                let mut model = mz_sql::query_model::Model::try_from(raw_plan)?;
                // construct explanation context
                let catalog = self.catalog.for_session(session);
                let context = crate::explain_new::ExplainContext {
                    config: &config,
                    humanizer: &catalog,
                    used_indexes: crate::explain_new::UsedIndexes::new(Default::default()),
                    finishing: row_set_finishing,
                    fast_path_plan: Default::default(),
                };
                // explain plan
                let mut explainable = crate::explain_new::Explainable::new(&mut model);
                let explanation_string = explainable.explain(&format, &config, &context)?;
                // pack rows in result vector
                vec![Row::pack_slice(&[Datum::from(&*explanation_string)])]
            }
            OptimizedQueryGraph => {
                use mz_repr::explain_new::Explain;
                // run partial pipeline
                let mut model = mz_sql::query_model::Model::try_from(raw_plan)?;
                model.optimize();
                // construct explanation context
                let catalog = self.catalog.for_session(session);
                let context = crate::explain_new::ExplainContext {
                    config: &config,
                    humanizer: &catalog,
                    used_indexes: crate::explain_new::UsedIndexes::new(Default::default()),
                    finishing: row_set_finishing,
                    fast_path_plan: Default::default(),
                };
                // explain plan
                let mut explainable = crate::explain_new::Explainable::new(&mut model);
                let explanation_string = explainable.explain(&format, &config, &context)?;
                // pack rows in result vector
                vec![Row::pack_slice(&[Datum::from(&*explanation_string)])]
            }
            // For the `Trace` stage, return the entire trace as (time, path, plan) triples.
            Trace => {
                let rows = trace
                    .into_iter()
                    .map(|entry| {
                        Row::pack_slice(&[
                            Datum::from(entry.duration.as_nanos() as u64),
                            Datum::from(entry.path.as_str()),
                            Datum::from(entry.plan.as_str()),
                        ])
                    })
                    .collect();
                rows
            }
            // For everything else, return the plan for the stage identified by the corresponding path.
            stage => {
                let row = trace
                    .into_iter()
                    .find(|entry| entry.path == stage.path())
                    .map(|entry| Row::pack_slice(&[Datum::from(entry.plan.as_str())]))
                    .unwrap_or_else(|| panic!("plan at {}", stage.path()));
                vec![row]
            }
        };

        Ok(send_immediate_rows(rows))
    }

    fn sequence_explain_timestamp(
        &mut self,
        session: &Session,
        raw_plan: HirRelationExpr,
    ) -> Result<ExecuteResponse, AdapterError> {
        let compute_instance = self.catalog.active_compute_instance(session)?.id;

        let decorrelated_plan = raw_plan.optimize_and_lower(&OptimizerConfig {
            qgm_optimizations: session.vars().qgm_optimizations(),
        })?;
        let optimized_plan = self.view_optimizer.optimize(decorrelated_plan)?;
        let timeline = self.validate_timeline(optimized_plan.depends_on())?;
        let source_ids = optimized_plan.depends_on();
        let id_bundle = self
            .index_oracle(compute_instance)
            .sufficient_collections(&source_ids);
        // TODO: determine_timestamp takes a mut self to track linearizability,
        // so explaining a plan involving tables has side effects. Removing those side
        // effects would be good.
        // TODO(jkosh44): Would be a nice addition to include the timeline in output.
        let timestamp = self.determine_timestamp(
            session,
            &id_bundle,
            &QueryWhen::Immediately,
            compute_instance,
            &timeline,
        )?;
        let since = self.least_valid_read(&id_bundle).elements().to_vec();
        let upper = self.least_valid_write(&id_bundle);
        let respond_immediately = !upper.less_equal(&timestamp);
        let upper = upper.elements().to_vec();
        let mut sources = Vec::new();
        {
            for id in id_bundle.storage_ids.iter() {
                let state = self.controller.storage.collection(*id).unwrap();
                let name = self
                    .catalog
                    .try_get_entry(id)
                    .map(|item| item.name())
                    .map(|name| {
                        self.catalog
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
            if let Some(compute_ids) = id_bundle.compute_ids.get(&compute_instance) {
                for id in compute_ids {
                    let state = self
                        .controller
                        .compute
                        .collection(compute_instance, *id)
                        .unwrap();
                    let name = self
                        .catalog
                        .try_get_entry(id)
                        .map(|item| item.name())
                        .map(|name| {
                            self.catalog
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
        let explanation = TimestampExplanation {
            timestamp,
            since,
            upper,
            global_timestamp: self.get_local_read_ts(),
            respond_immediately,
            sources,
            timeline,
        };
        let rows = vec![Row::pack_slice(&[Datum::from(&*explanation.to_string())])];
        Ok(send_immediate_rows(rows))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn sequence_send_diffs(
        &mut self,
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
            return match finishing.finish(
                plan.returning,
                self.catalog.system_config().max_result_size(),
            ) {
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

    async fn sequence_insert(
        &mut self,
        tx: ClientTransmitter<ExecuteResponse>,
        mut session: Session,
        plan: InsertPlan,
    ) {
        let optimized_mir = if let MirRelationExpr::Constant { .. } = &plan.values {
            // We don't perform any optimizations on an expression that is already
            // a constant for writes, as we want to maximize bulk-insert throughput.
            OptimizedMirRelationExpr(plan.values)
        } else {
            match self.view_optimizer.optimize(plan.values) {
                Ok(m) => m,
                Err(e) => {
                    tx.send(Err(e.into()), session);
                    return;
                }
            }
        };

        match optimized_mir.into_inner() {
            constants @ MirRelationExpr::Constant { .. } if plan.returning.is_empty() => tx.send(
                self.sequence_insert_constant(&mut session, plan.id, constants),
                session,
            ),
            // All non-constant values must be planned as read-then-writes.
            mut selection => {
                let desc_arity = match self.catalog.try_get_entry(&plan.id) {
                    Some(table) => table
                        .desc(
                            &self
                                .catalog
                                .resolve_full_name(table.name(), Some(session.conn_id())),
                        )
                        .expect("desc called on table")
                        .arity(),
                    None => {
                        tx.send(
                            Err(AdapterError::SqlCatalog(CatalogError::UnknownItem(
                                plan.id.to_string(),
                            ))),
                            session,
                        );
                        return;
                    }
                };

                if selection.contains_temporal() {
                    tx.send(
                        Err(AdapterError::Unsupported(
                            "calls to mz_now in write statements",
                        )),
                        session,
                    );
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
                    assignments: HashMap::new(),
                    kind: MutationKind::Insert,
                    returning: plan.returning,
                };

                self.sequence_read_then_write(tx, session, read_then_write_plan)
                    .await;
            }
        }
    }

    fn sequence_insert_constant(
        &mut self,
        session: &mut Session,
        id: GlobalId,
        constants: MirRelationExpr,
    ) -> Result<ExecuteResponse, AdapterError> {
        // Insert can be queued, so we need to re-verify the id exists.
        let desc = match self.catalog.try_get_entry(&id) {
            Some(table) => table.desc(
                &self
                    .catalog
                    .resolve_full_name(table.name(), Some(session.conn_id())),
            )?,
            None => {
                return Err(AdapterError::SqlCatalog(CatalogError::UnknownItem(
                    id.to_string(),
                )))
            }
        };

        match constants {
            MirRelationExpr::Constant { rows, typ: _ } => {
                let rows = rows?;
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
                };
                self.sequence_send_diffs(session, diffs_plan)
            }
            o => panic!(
                "tried using sequence_insert_constant on non-constant MirRelationExpr {:?}",
                o
            ),
        }
    }

    pub(crate) fn sequence_copy_rows(
        &mut self,
        session: &mut Session,
        id: GlobalId,
        columns: Vec<usize>,
        rows: Vec<Row>,
    ) -> Result<ExecuteResponse, AdapterError> {
        let catalog = self.catalog.for_session(session);
        let values = mz_sql::plan::plan_copy_from(session.pcx(), &catalog, id, columns, rows)?;
        let values = self.view_optimizer.optimize(values.lower())?;
        // Copied rows must always be constants.
        self.sequence_insert_constant(session, id, values.into_inner())
    }

    // ReadThenWrite is a plan whose writes depend on the results of a
    // read. This works by doing a Peek then queuing a SendDiffs. No writes
    // or read-then-writes can occur between the Peek and SendDiff otherwise a
    // serializability violation could occur.
    async fn sequence_read_then_write(
        &mut self,
        tx: ClientTransmitter<ExecuteResponse>,
        mut session: Session,
        plan: ReadThenWritePlan,
    ) {
        guard_write_critical_section!(self, tx, session, Plan::ReadThenWrite(plan));

        let ReadThenWritePlan {
            id,
            kind,
            selection,
            assignments,
            finishing,
            returning,
        } = plan;

        // Read then writes can be queued, so re-verify the id exists.
        let desc = match self.catalog.try_get_entry(&id) {
            Some(table) => table
                .desc(
                    &self
                        .catalog
                        .resolve_full_name(table.name(), Some(session.conn_id())),
                )
                .expect("desc called on table")
                .into_owned(),
            None => {
                tx.send(
                    Err(AdapterError::SqlCatalog(CatalogError::UnknownItem(
                        id.to_string(),
                    ))),
                    session,
                );
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
        fn validate_read_dependencies<S>(catalog: &Catalog<S>, id: &GlobalId) -> bool
        where
            S: mz_stash::Append,
        {
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
            if !validate_read_dependencies(&self.catalog, &id) {
                tx.send(Err(AdapterError::InvalidTableMutationSelection), session);
                return;
            }
        }

        let peek_response = match self
            .sequence_peek(
                &mut session,
                PeekPlan {
                    source: selection,
                    when: QueryWhen::Immediately,
                    finishing,
                    copy_to: None,
                },
            )
            .await
        {
            Ok(resp) => resp,
            Err(e) => {
                tx.send(Err(e), session);
                return;
            }
        };

        let timeout_dur = *session.vars().statement_timeout();

        let internal_cmd_tx = self.internal_cmd_tx.clone();
        let strict_serializable_reads_tx = self.strict_serializable_reads_tx.clone();
        task::spawn(|| format!("sequence_read_then_write:{id}"), async move {
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
                                                desc.constraints_met(*idx, &updated)?;
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
                                    Ok(diffs)
                                }(rows)
                            }
                            PeekResponseUnary::Canceled => {
                                Err(AdapterError::Unstructured(anyhow!("execution canceled")))
                            }
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
                                conn_id: session.conn_id(),
                            });
                            if let Err(e) = result {
                                warn!("internal_cmd_rx dropped before we could send: {:?}", e);
                            }
                            Err(AdapterError::StatementTimeout)
                        }
                    }
                }
                _ => Err(AdapterError::Unstructured(anyhow!("expected SendingRows"))),
            };
            let mut returning_rows = Vec::new();
            let mut diff_err: Option<AdapterError> = None;
            if !returning.is_empty() && diffs.is_ok() {
                let arena = RowArena::new();
                for (row, diff) in diffs.as_ref().unwrap() {
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

            // We need to clear out the read ops so the write doesn't fail due to a
            // read only transaction.
            let read_ops = session.take_transaction_read_ops();
            // No matter what isolation level the client is using, we must linearize this
            // read. The write will be performed right after this, as part of a single
            // transaction, so the write must have a timestamp greater than or equal to the
            // read.
            //
            // Note: It's only OK for the write to have a greater timestamp than the read
            // because the write lock prevents any other writes from happening in between
            // the read and write.
            if let Some((read_ts, Some(timeline))) = read_ops {
                let (tx, rx) = tokio::sync::oneshot::channel();
                let result = strict_serializable_reads_tx.send(PendingReadTxn::ReadThenWrite {
                    tx,
                    timestamp: (read_ts, timeline),
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

            // It is not an error for these results to be ready after `internal_cmd_rx` has been dropped.
            let result = internal_cmd_tx.send(Message::SendDiffs(SendDiffs {
                session,
                tx,
                id,
                diffs,
                kind,
                returning: returning_rows,
            }));
            if let Err(e) = result {
                warn!("internal_cmd_rx dropped before we could send: {:?}", e);
            }
        });
    }

    async fn sequence_alter_item_rename(
        &mut self,
        session: &Session,
        plan: AlterItemRenamePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let op = catalog::Op::RenameItem {
            id: plan.id,
            current_full_name: plan.current_full_name,
            to_name: plan.to_name,
        };
        match self
            .catalog_transact(Some(session), vec![op], |_| Ok(()))
            .await
        {
            Ok(()) => Ok(ExecuteResponse::AlteredObject(plan.object_type)),
            Err(err) => Err(err),
        }
    }

    async fn sequence_alter_index_set_options(
        &mut self,
        plan: AlterIndexSetOptionsPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        self.set_index_options(plan.id, plan.options).await?;
        Ok(ExecuteResponse::AlteredObject(ObjectType::Index))
    }

    async fn sequence_alter_index_reset_options(
        &mut self,
        plan: AlterIndexResetOptionsPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let mut options = Vec::with_capacity(plan.options.len());
        for o in plan.options {
            options.push(match o {
                IndexOptionName::LogicalCompactionWindow => IndexOption::LogicalCompactionWindow(
                    DEFAULT_LOGICAL_COMPACTION_WINDOW_MS.map(|ts| Duration::from_millis(ts.into())),
                ),
            });
        }

        self.set_index_options(plan.id, options).await?;

        Ok(ExecuteResponse::AlteredObject(ObjectType::Index))
    }

    async fn set_index_options(
        &mut self,
        id: GlobalId,
        options: Vec<IndexOption>,
    ) -> Result<(), AdapterError> {
        for o in options {
            match o {
                IndexOption::LogicalCompactionWindow(window) => {
                    // The index is on a specific compute instance.
                    let compute_instance = self
                        .catalog
                        .get_entry(&id)
                        .index()
                        .expect("setting options on index")
                        .compute_instance;
                    let policy = match window {
                        Some(time) => ReadPolicy::lag_writes_by(time.try_into()?),
                        None => ReadPolicy::ValidFrom(Antichain::from_elem(Timestamp::minimum())),
                    };
                    self.update_compute_base_read_policy(compute_instance, id, policy)
                        .await;
                }
            }
        }
        Ok(())
    }

    async fn sequence_alter_secret(
        &mut self,
        session: &Session,
        plan: AlterSecretPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let AlterSecretPlan { id, mut secret_as } = plan;

        let payload = self.extract_secret(session, &mut secret_as)?;

        self.secrets_controller.ensure(id, &payload).await?;

        Ok(ExecuteResponse::AlteredObject(ObjectType::Secret))
    }

    async fn sequence_alter_sink(
        &mut self,
        session: &Session,
        AlterSinkPlan { id, size, remote }: AlterSinkPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let op = catalog::Op::AlterSink { id, size, remote };
        self.catalog_transact(Some(session), vec![op], |_| Ok(()))
            .await?;

        // Re-fetch the updated item from the catalog
        let entry = self.catalog.get_entry(&id);
        let updated_sink = entry.sink().ok_or_else(|| {
            CatalogError::UnexpectedType(entry.name().to_string(), CatalogItemType::Sink)
        })?;

        self.controller
            .storage
            .alter_collections(vec![(id, updated_sink.host_config.clone())])
            .await?;

        Ok(ExecuteResponse::AlteredObject(ObjectType::Sink))
    }

    async fn sequence_alter_source(
        &mut self,
        session: &Session,
        AlterSourcePlan { id, size, remote }: AlterSourcePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let op = catalog::Op::AlterSource { id, size, remote };
        self.catalog_transact(Some(session), vec![op], |_| Ok(()))
            .await?;

        // Re-fetch the updated item from the catalog
        let entry = self.catalog.get_entry(&id);
        let updated_source = entry.source().ok_or_else(|| {
            CatalogError::UnexpectedType(entry.name().to_string(), CatalogItemType::Source)
        })?;
        if let DataSourceDesc::Ingestion(Ingestion { host_config, .. }) =
            &updated_source.data_source
        {
            self.controller
                .storage
                .alter_collections(vec![(id, host_config.clone())])
                .await?;
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
            self.catalog.state(),
            secret_as,
            ExprPrepStyle::OneShot {
                logical_time: None,
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

    async fn sequence_alter_system_set(
        &mut self,
        session: &Session,
        AlterSystemSetPlan { name, value }: AlterSystemSetPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        self.is_user_allowed_to_alter_system(session)?;
        use mz_sql::ast::{SetVariableValue, Value};
        let update_max_result_size = name == session::vars::MAX_RESULT_SIZE.name();
        let op = match value {
            SetVariableValue::Default => catalog::Op::ResetSystemConfiguration { name },
            SetVariableValue::Literal(Value::String(value)) => {
                catalog::Op::UpdateSystemConfiguration { name, value }
            }
            SetVariableValue::Ident(ident) => catalog::Op::UpdateSystemConfiguration {
                name,
                value: ident.into_string(),
            },
            value => catalog::Op::UpdateSystemConfiguration {
                name,
                value: value.to_string(),
            },
        };
        self.catalog_transact(Some(session), vec![op], |_| Ok(()))
            .await?;
        if update_max_result_size {
            self.update_max_result_size();
        }
        Ok(ExecuteResponse::AlteredSystemConfiguraion)
    }

    async fn sequence_alter_system_reset(
        &mut self,
        session: &Session,
        AlterSystemResetPlan { name }: AlterSystemResetPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        self.is_user_allowed_to_alter_system(session)?;
        let update_max_result_size = name == session::vars::MAX_RESULT_SIZE.name();
        let op = catalog::Op::ResetSystemConfiguration { name };
        self.catalog_transact(Some(session), vec![op], |_| Ok(()))
            .await?;
        if update_max_result_size {
            self.update_max_result_size();
        }
        Ok(ExecuteResponse::AlteredSystemConfiguraion)
    }

    async fn sequence_alter_system_reset_all(
        &mut self,
        session: &Session,
        _: AlterSystemResetAllPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        self.is_user_allowed_to_alter_system(session)?;
        let op = catalog::Op::ResetAllSystemConfiguration {};
        self.catalog_transact(Some(session), vec![op], |_| Ok(()))
            .await?;
        self.update_max_result_size();
        Ok(ExecuteResponse::AlteredSystemConfiguraion)
    }

    fn is_user_allowed_to_alter_system(&self, session: &Session) -> Result<(), AdapterError> {
        if session.user() == &*SYSTEM_USER {
            Ok(())
        } else {
            Err(AdapterError::Unauthorized(format!(
                "only user '{}' is allowed to execute 'ALTER SYSTEM ...'",
                SYSTEM_USER.name,
            )))
        }
    }

    fn update_max_result_size(&mut self) {
        let mut compute = self.controller.active_compute();
        for compute_instance in self.catalog.compute_instances() {
            compute
                .update_max_result_size(
                    compute_instance.id,
                    self.catalog.system_config().max_result_size(),
                )
                .unwrap();
        }
    }

    // Returns the name of the portal to execute.
    fn sequence_execute(
        &mut self,
        session: &mut Session,
        plan: ExecutePlan,
    ) -> Result<String, AdapterError> {
        // Verify the stmt is still valid.
        self.verify_prepared_statement(session, &plan.name)?;
        let ps = session
            .get_prepared_statement_unverified(&plan.name)
            .expect("known to exist");
        let sql = ps.sql().cloned();
        let desc = ps.desc().clone();
        let revision = ps.catalog_revision;
        session.create_new_portal(sql, desc, plan.params, Vec::new(), revision)
    }

    pub(crate) fn allocate_transient_id(&mut self) -> Result<GlobalId, AdapterError> {
        let id = self.transient_id_counter;
        if id == u64::MAX {
            coord_bail!("id counter overflows i64");
        }
        self.transient_id_counter += 1;
        Ok(GlobalId::Transient(id))
    }

    /// TODO(jkosh44) This function will verify the privileges for the mz_introspection user.
    ///  All of the privileges are hard coded into this function. In the future if we ever add
    ///  a more robust privileges framework, then this function should be replaced with that
    ///  framework.
    fn mz_introspection_user_privilege_hack(
        &self,
        session: &Session,
        plan: &Plan,
        depends_on: &Vec<GlobalId>,
    ) -> Result<(), AdapterError> {
        if session.user().name != MZ_INTROSPECTION_ROLE.name {
            return Ok(());
        }

        match plan {
            Plan::Subscribe(_)
            | Plan::Peek(_)
            | Plan::CopyFrom(_)
            | Plan::SendRows(_)
            | Plan::Explain(_)
            | Plan::ShowAllVariables
            | Plan::ShowVariable(_)
            | Plan::SetVariable(_)
            | Plan::ResetVariable(_)
            | Plan::StartTransaction(_)
            | Plan::CommitTransaction
            | Plan::AbortTransaction
            | Plan::EmptyQuery
            | Plan::Declare(_)
            | Plan::Fetch(_)
            | Plan::Close(_)
            | Plan::Prepare(_)
            | Plan::Execute(_)
            | Plan::Deallocate(_) => {}

            Plan::CreateConnection(_)
            | Plan::CreateDatabase(_)
            | Plan::CreateSchema(_)
            | Plan::CreateRole(_)
            | Plan::CreateComputeInstance(_)
            | Plan::CreateComputeReplica(_)
            | Plan::CreateSource(_)
            | Plan::CreateSecret(_)
            | Plan::CreateSink(_)
            | Plan::CreateTable(_)
            | Plan::CreateView(_)
            | Plan::CreateMaterializedView(_)
            | Plan::CreateIndex(_)
            | Plan::CreateType(_)
            | Plan::DiscardTemp
            | Plan::DiscardAll
            | Plan::DropDatabase(_)
            | Plan::DropSchema(_)
            | Plan::DropRoles(_)
            | Plan::DropComputeInstances(_)
            | Plan::DropComputeReplicas(_)
            | Plan::DropItems(_)
            | Plan::SendDiffs(_)
            | Plan::Insert(_)
            | Plan::AlterNoop(_)
            | Plan::AlterIndexSetOptions(_)
            | Plan::AlterIndexResetOptions(_)
            | Plan::AlterSink(_)
            | Plan::AlterSource(_)
            | Plan::AlterItemRename(_)
            | Plan::AlterSecret(_)
            | Plan::AlterSystemSet(_)
            | Plan::AlterSystemReset(_)
            | Plan::AlterSystemResetAll(_)
            | Plan::ReadThenWrite(_)
            | Plan::Raise(_)
            | Plan::RotateKeys(_) => {
                return Err(AdapterError::Unauthorized(
                    "user 'mz_introspection' is unauthorized to perform this action".into(),
                ))
            }
        }

        if let Ok(active_compute_instance) = self.catalog.active_compute_instance(session) {
            let active_compute_instance = active_compute_instance.name();
            if (matches!(plan, Plan::Peek(_)) || matches!(plan, Plan::Subscribe(_)))
                && active_compute_instance != MZ_INTROSPECTION_COMPUTE_INSTANCE.name
                && active_compute_instance != MZ_SYSTEM_COMPUTE_INSTANCE.name
            {
                return Err(AdapterError::Unauthorized(format!(
                    "user 'mz_introspection' is unauthorized to use cluster {active_compute_instance}",
                )));
            }
        }

        for id in depends_on {
            let entry = self.catalog.get_entry(id);
            let full_name = self
                .catalog
                .resolve_full_name(entry.name(), Some(session.conn_id()));
            let schema = &full_name.schema;
            if schema != MZ_CATALOG_SCHEMA
                && schema != PG_CATALOG_SCHEMA
                && schema != MZ_INTERNAL_SCHEMA
                && schema != INFORMATION_SCHEMA
            {
                return Err(AdapterError::Unauthorized(format!(
                    "user 'mz_introspection' is unauthorized to interact with object {full_name}",
                )));
            }
        }

        Ok(())
    }
}

enum LogReadStyle<'a> {
    Peek(&'a mut Option<ReplicaId>),
    Subscribe,
}

fn check_no_invalid_log_reads<'a, S>(
    catalog: &Catalog<S>,
    compute_instance: &ComputeInstance,
    source_ids: &BTreeSet<GlobalId>,
    log_read_style: LogReadStyle<'a>,
) -> Result<(), AdapterError>
where
    S: Append,
{
    let log_names = source_ids
        .iter()
        .flat_map(|id| catalog.arranged_introspection_dependencies(*id))
        .map(|id| catalog.get_entry(&id).name().item.clone())
        .collect::<Vec<_>>();

    if log_names.is_empty() {
        return Ok(());
    }

    // Peeking from log sources on replicated compute instances is only
    // allowed if a target replica is selected. Otherwise, we have no
    // way of knowing which replica we read the introspection data from.
    //
    // Subscribing from log sources on replicated compute instances is not
    // supported, but in theory could support replica targeting in the same way
    // as peeks.
    let target_replica = match log_read_style {
        LogReadStyle::Peek(target_replica) => target_replica,
        LogReadStyle::Subscribe => {
            return Err(AdapterError::TargetedSubscribe { log_names });
        }
    };

    let num_replicas = compute_instance.replicas_by_id.len();
    if target_replica.is_none() {
        if num_replicas == 1 {
            *target_replica = compute_instance.replicas_by_id.keys().next().copied();
        } else {
            return Err(AdapterError::UntargetedLogRead { log_names });
        }
    }

    // Ensure that logging is initialized for the target replica, lest
    // we try to peek a non-existing arrangement.
    let replica_id = target_replica.unwrap();
    let replica = &compute_instance.replicas_by_id[&replica_id];
    if !replica.config.logging.enabled() {
        return Err(AdapterError::IntrospectionDisabled { log_names });
    }

    Ok(())
}
