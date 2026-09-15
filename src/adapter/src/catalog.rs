// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Adapter serving context around the shared catalog implementation.

use futures::future::BoxFuture;
use futures::{Future, FutureExt};
use mz_adapter_types::connection::ConnectionId;
use mz_build_info::BuildInfo;
use mz_catalog::builtin::MZ_CATALOG_SERVER_CLUSTER;
use mz_catalog::catalog::Catalog as CoreCatalog;
use mz_catalog::config::{Config, StateConfig};
use mz_catalog::durable::{BootstrapArgs, DurableCatalogState, Snapshot};
use mz_catalog::expr_cache::{GlobalExpressions, LocalExpressions};
use mz_catalog::memory::error::Error;
use mz_catalog::memory::objects::{CatalogCollectionEntry, CatalogEntry, Cluster, Index};
use mz_controller_types::ClusterId;
use mz_ore::now::NowFn;
use mz_persist_client::PersistClient;
use mz_repr::role_id::RoleId;
use mz_repr::{CatalogItemId, GlobalId, Timestamp};
use mz_sql::catalog::{CatalogDatabase, EnvironmentId};
use mz_sql::names::{FullItemName, QualifiedItemName};
use mz_sql::session::metadata::SessionMetadata;
use mz_sql::session::user::{SUPPORT_USER, SYSTEM_USER};
use mz_storage_client::controller::StorageTxn;
use mz_storage_client::storage_collections::StorageCollections;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::time::Instant;
use tracing::info;
use uuid::Uuid;

use crate::command::CatalogDump;
use crate::coord::{ConnMeta, TargetCluster};
use crate::optimize::OptimizerCatalog;
use crate::session::Session;
use crate::{AdapterError, ExecuteResponse};
pub use mz_catalog::catalog::{
    BuiltinTableUpdate, CatalogState, CatalogStateView, CatalogUpperHandle, DebugAwsContext,
    DropObjectInfo, InitializeStateResult, InjectedAuditEvent, Op, ReplicaCreateDropReason,
    TransactionResult, UpdatePrivilegeVariant, is_public_role, is_reserved_name,
    is_reserved_role_name,
};
pub(crate) use mz_catalog::catalog::{
    catalog_type_to_audit_object_type, cluster_state, consistency,
};
mod conn_catalog;
mod session_updates;
pub(crate) use session_updates::{pack_session_update, pack_subscribe_update};
mod timeline;
pub use conn_catalog::ConnCatalog;

#[derive(Debug, Clone)]
pub struct Catalog(CoreCatalog);

impl Deref for Catalog {
    type Target = CoreCatalog;
    fn deref(&self) -> &CoreCatalog {
        &self.0
    }
}
impl DerefMut for Catalog {
    fn deref_mut(&mut self) -> &mut CoreCatalog {
        &mut self.0
    }
}

pub struct OpenCatalogResult {
    pub catalog: Catalog,
    pub last_seen_version: Option<semver::Version>,
    pub migrated_storage_collections_0dt: BTreeSet<CatalogItemId>,
    pub new_builtin_collections: BTreeSet<GlobalId>,
    pub builtin_table_updates: Vec<BuiltinTableUpdate>,
    pub cached_global_exprs: BTreeMap<GlobalId, GlobalExpressions>,
    pub uncached_local_exprs: BTreeMap<GlobalId, LocalExpressions>,
}

fn transaction_context(
    session: &ConnMeta,
) -> mz_catalog::catalog::transaction_context::TransactionContext<'_> {
    mz_catalog::catalog::transaction_context::TransactionContext {
        user: session.user(),
        conn_id: session.conn_id(),
        uuid: session.uuid(),
        authenticated_role_id: session.authenticated_role_id(),
    }
}

impl Catalog {
    pub fn open(config: Config<'_>) -> BoxFuture<'static, Result<OpenCatalogResult, AdapterError>> {
        let open = CoreCatalog::open(config);
        async move {
            let mz_catalog::catalog::OpenCatalogResult {
                catalog,
                last_seen_version,
                migrated_storage_collections_0dt,
                new_builtin_collections,
                builtin_table_updates,
                cached_global_exprs,
                uncached_local_exprs,
            } = open.await?;
            Ok(OpenCatalogResult {
                catalog: Self(catalog),
                last_seen_version,
                migrated_storage_collections_0dt,
                new_builtin_collections,
                builtin_table_updates,
                cached_global_exprs,
                uncached_local_exprs,
            })
        }
        .boxed()
    }
    pub async fn initialize_state(
        config: StateConfig,
        storage: &mut Box<dyn DurableCatalogState>,
    ) -> Result<InitializeStateResult, AdapterError> {
        Ok(CoreCatalog::initialize_state(config, storage).await?)
    }
    pub(crate) fn expression_build_version(
        build_info: &mz_build_info::BuildInfo,
    ) -> semver::Version {
        CoreCatalog::expression_build_version(build_info)
    }
    pub(crate) async fn writer_projection(
        &self,
        storage: Box<dyn DurableCatalogState>,
    ) -> Result<Self, AdapterError> {
        Ok(Self(self.0.writer_projection(storage).await?))
    }
    pub(crate) async fn current_upper_if_in_sync(&self) -> Result<Timestamp, AdapterError> {
        Ok(self.0.current_upper_if_in_sync().await?)
    }
    pub(crate) async fn check_durable_consistency(
        &self,
        input: mz_catalog::durable::CatalogSnapshot,
    ) -> Result<(), AdapterError> {
        Ok(self.0.check_durable_consistency(input).await?)
    }
    pub async fn expire(self) {
        self.0.expire().await;
    }
    pub fn dump(&self) -> Result<CatalogDump, Error> {
        Ok(CatalogDump::new(self.0.dump()?))
    }
    pub fn as_optimizer_catalog(self: Arc<Self>) -> Arc<dyn OptimizerCatalog> {
        self
    }
    pub async fn transact(
        &mut self,
        storage_collections: Option<&mut Arc<dyn StorageCollections + Send + Sync>>,
        oracle_write_ts: Timestamp,
        session: Option<&ConnMeta>,
        ops: Vec<Op>,
    ) -> Result<TransactionResult, AdapterError> {
        let session = session.map(transaction_context);
        Ok(self
            .0
            .transact(storage_collections, oracle_write_ts, session.as_ref(), ops)
            .await?)
    }
    pub async fn transact_incremental_dry_run(
        &self,
        base_state: &CatalogState,
        ops: Vec<Op>,
        session: Option<&ConnMeta>,
        prev_snapshot: Option<Snapshot>,
        oracle_write_ts: Timestamp,
    ) -> Result<(CatalogState, Snapshot), AdapterError> {
        let session = session.map(transaction_context);
        Ok(self
            .0
            .transact_incremental_dry_run(
                base_state,
                ops,
                session.as_ref(),
                prev_snapshot,
                oracle_write_ts,
            )
            .await?)
    }
    pub fn for_session<'a>(&'a self, session: &'a Session) -> ConnCatalog<'a> {
        let search_path = self.state().resolve_search_path(session);
        let database = self
            .resolve_database(session.vars().database())
            .ok()
            .map(|db| db.id());
        let state = session
            .transaction()
            .catalog_state()
            .unwrap_or_else(|| self.state());
        ConnCatalog {
            view: CatalogStateView::new(
                state,
                session.conn_id().clone(),
                session.vars().cluster().into(),
                database,
                search_path,
                *session.current_role_id(),
                session.vars().restrict_to_user_objects(),
            ),
            prepared_statements: Some(session.prepared_statements()),
            portals: Some(session.portals()),
            notices_tx: Some(session.retain_notice_transmitter()),
        }
    }
    pub fn for_sessionless_user(&self, role_id: RoleId) -> ConnCatalog<'_> {
        self.state().for_sessionless_user(role_id).into()
    }
    pub fn for_system_session(&self) -> ConnCatalog<'_> {
        self.state().for_system_session().into()
    }
    pub fn resolve_target_cluster(
        &self,
        target_cluster: TargetCluster,
        session: &Session,
    ) -> Result<&Cluster, AdapterError> {
        match target_cluster {
            TargetCluster::CatalogServer => {
                Ok(self.resolve_builtin_cluster(&MZ_CATALOG_SERVER_CLUSTER))
            }
            TargetCluster::Active => self.active_cluster(session),
            TargetCluster::Transaction(cluster_id) => self
                .try_get_cluster(cluster_id)
                .ok_or(AdapterError::ConcurrentClusterDrop),
        }
    }
    pub fn active_cluster(&self, session: &Session) -> Result<&Cluster, AdapterError> {
        // TODO(benesch): this check here is not sufficiently protective. It'd
        // be very easy for a code path to accidentally avoid this check by
        // calling `resolve_cluster(session.vars().cluster())`.
        if session.user().name != SYSTEM_USER.name
            && session.user().name != SUPPORT_USER.name
            && session.vars().cluster() == SYSTEM_USER.name
        {
            coord_bail!(
                "system cluster '{}' cannot execute user queries",
                SYSTEM_USER.name
            );
        }
        let cluster = self.resolve_cluster(session.vars().cluster())?;
        Ok(cluster)
    }
    pub async fn with_debug<F, Fut, T>(f: F) -> T
    where
        F: FnOnce(Self) -> Fut,
        Fut: Future<Output = T>,
    {
        Box::pin(CoreCatalog::with_debug(|catalog| f(Self(catalog)))).await
    }
    pub async fn with_debug_in_bootstrap<F, Fut, T>(f: F) -> T
    where
        F: FnOnce(Self) -> Fut,
        Fut: Future<Output = T>,
    {
        Box::pin(CoreCatalog::with_debug_in_bootstrap(|catalog| {
            f(Self(catalog))
        }))
        .await
    }
    pub async fn with_debug_aws_context<F, Fut, T>(context: DebugAwsContext, f: F) -> T
    where
        F: FnOnce(Self) -> Fut,
        Fut: Future<Output = T>,
    {
        Box::pin(CoreCatalog::with_debug_aws_context(context, |catalog| {
            f(Self(catalog))
        }))
        .await
    }
    pub async fn open_debug_catalog(
        persist_client: PersistClient,
        organization_id: Uuid,
        bootstrap_args: &BootstrapArgs,
    ) -> Result<Catalog, anyhow::Error> {
        Ok(Self(
            Box::pin(CoreCatalog::open_debug_catalog(
                persist_client,
                organization_id,
                bootstrap_args,
            ))
            .await?,
        ))
    }
    pub async fn open_debug_catalog_with_aws_context(
        persist_client: PersistClient,
        organization_id: Uuid,
        bootstrap_args: &BootstrapArgs,
        aws_context: Option<DebugAwsContext>,
    ) -> Result<Catalog, anyhow::Error> {
        Ok(Self(
            Box::pin(CoreCatalog::open_debug_catalog_with_aws_context(
                persist_client,
                organization_id,
                bootstrap_args,
                aws_context,
            ))
            .await?,
        ))
    }
    pub async fn open_debug_read_only_catalog(
        persist_client: PersistClient,
        organization_id: Uuid,
        bootstrap_args: &BootstrapArgs,
    ) -> Result<Catalog, anyhow::Error> {
        Ok(Self(
            Box::pin(CoreCatalog::open_debug_read_only_catalog(
                persist_client,
                organization_id,
                bootstrap_args,
            ))
            .await?,
        ))
    }
    pub async fn open_debug_read_only_persist_catalog_config(
        persist_client: PersistClient,
        now: NowFn,
        environment_id: EnvironmentId,
        system_parameter_defaults: BTreeMap<String, String>,
        build_info: &'static BuildInfo,
        bootstrap_args: &BootstrapArgs,
        enable_expression_cache_override: Option<bool>,
        aws_context: Option<DebugAwsContext>,
    ) -> Result<Catalog, anyhow::Error> {
        Ok(Self(
            Box::pin(CoreCatalog::open_debug_read_only_persist_catalog_config(
                persist_client,
                now,
                environment_id,
                system_parameter_defaults,
                build_info,
                bootstrap_args,
                enable_expression_cache_override,
                aws_context,
            ))
            .await?,
        ))
    }
    pub async fn open_debug_catalog_inner(
        persist_client: PersistClient,
        storage: Box<dyn DurableCatalogState>,
        now: NowFn,
        environment_id: Option<EnvironmentId>,
        build_info: &'static BuildInfo,
        system_parameter_defaults: BTreeMap<String, String>,
        bootstrap_args: &BootstrapArgs,
        enable_expression_cache_override: Option<bool>,
        aws_context: Option<DebugAwsContext>,
    ) -> Result<Catalog, anyhow::Error> {
        Ok(Self(
            Box::pin(CoreCatalog::open_debug_catalog_inner(
                persist_client,
                storage,
                now,
                environment_id,
                build_info,
                system_parameter_defaults,
                bootstrap_args,
                enable_expression_cache_override,
                aws_context,
            ))
            .await?,
        ))
    }
    pub(crate) async fn initialize_table_writer(
        &self,
        persist: mz_persist_client::PersistClient,
        metrics_registry: &mz_ore::metrics::MetricsRegistry,
        read_only: bool,
    ) -> Result<
        (
            Arc<dyn crate::table_writer::TableWriteHandle>,
            Arc<mz_txn_wal::metrics::Metrics>,
        ),
        mz_catalog::durable::CatalogError,
    > {
        let shard = {
            let mut storage = self.storage().await;
            let mut tx = storage.transaction().await?;
            mz_controller::prepare_initialization(&mut tx)
                .map_err(mz_catalog::durable::DurableCatalogError::from)?;
            let updates = tx.get_and_commit_op_updates();
            assert!(
                updates.is_empty(),
                "WAL initialization should not produce catalog projection updates: {updates:?}"
            );
            let shard = tx.get_txn_wal_shard().expect("WAL identity is initialized");
            let commit_ts = tx.upper();
            tx.commit(commit_ts).await?;
            shard
        };
        let metrics = Arc::new(mz_txn_wal::metrics::Metrics::new(metrics_registry));
        let writer =
            crate::table_writer::open(persist, shard, Arc::clone(&metrics), read_only).await;
        Ok((writer, metrics))
    }
    pub(crate) async fn initialize_controller(
        &mut self,
        config: mz_controller::ControllerConfig,
        envd_epoch: core::num::NonZeroI64,
        read_only: bool,
        txns_metrics: Arc<mz_txn_wal::metrics::Metrics>,
    ) -> Result<mz_controller::Controller, mz_catalog::durable::CatalogError> {
        let controller_start = Instant::now();
        info!("startup: controller init: beginning");

        let mut controller = {
            let mut storage = self.storage().await;
            let read_only_tx = storage.transaction().await?;
            mz_controller::Controller::new(
                config,
                envd_epoch,
                read_only,
                self.state().catalog_read_protection_enabled(),
                &read_only_tx,
                txns_metrics,
            )
            .await
        };

        controller.set_catalog_follower_config(
            serde_json::to_string(&self.replica_config())
                .expect("catalog reconstruction configuration is serializable"),
        );
        self.initialize_storage_state(&controller.storage_collections)
            .await?;

        info!(
            "startup: controller init: complete in {:?}",
            controller_start.elapsed()
        );

        Ok(controller)
    }
}
impl OptimizerCatalog for CatalogState {
    fn get_entry(&self, id: &GlobalId) -> CatalogCollectionEntry {
        CatalogState::get_entry_by_global_id(self, id)
    }
    fn get_entry_by_item_id(&self, id: &CatalogItemId) -> &CatalogEntry {
        CatalogState::get_entry(self, id)
    }
    fn resolve_full_name(
        &self,
        name: &QualifiedItemName,
        conn_id: Option<&ConnectionId>,
    ) -> FullItemName {
        CatalogState::resolve_full_name(self, name, conn_id)
    }
    fn get_indexes_on(
        &self,
        id: GlobalId,
        cluster: ClusterId,
    ) -> Box<dyn Iterator<Item = (GlobalId, &Index)> + '_> {
        Box::new(CatalogState::get_indexes_on(self, id, cluster))
    }
}

impl OptimizerCatalog for Catalog {
    fn get_entry(&self, id: &GlobalId) -> CatalogCollectionEntry {
        self.state().get_entry_by_global_id(id)
    }

    fn get_entry_by_item_id(&self, id: &CatalogItemId) -> &CatalogEntry {
        self.state().get_entry(id)
    }

    fn resolve_full_name(
        &self,
        name: &QualifiedItemName,
        conn_id: Option<&ConnectionId>,
    ) -> FullItemName {
        self.state().resolve_full_name(name, conn_id)
    }

    fn get_indexes_on(
        &self,
        id: GlobalId,
        cluster: ClusterId,
    ) -> Box<dyn Iterator<Item = (GlobalId, &Index)> + '_> {
        Box::new(self.state().get_indexes_on(id, cluster))
    }
}

impl From<UpdatePrivilegeVariant> for ExecuteResponse {
    fn from(variant: UpdatePrivilegeVariant) -> Self {
        match variant {
            UpdatePrivilegeVariant::Grant => ExecuteResponse::GrantedPrivilege,
            UpdatePrivilegeVariant::Revoke => ExecuteResponse::RevokedPrivilege,
        }
    }
}

#[cfg(test)]
mod tests;
