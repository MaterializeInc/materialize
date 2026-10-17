// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;

use mz_catalog::SYSTEM_CONN_ID;
use mz_catalog::memory::objects::{Cluster, ClusterReplica, Database, NetworkPolicy, Role, Schema};
use mz_controller_types::{ClusterId, ReplicaId};
use mz_repr::CatalogItemId;
use mz_repr::adt::mz_acl_item::PrivilegeMap;
use mz_repr::network_policy_id::NetworkPolicyId;
use mz_repr::role_id::RoleId;
use mz_sql::catalog::{
    CatalogCluster, CatalogClusterRef, CatalogClusterReplica, CatalogClusterReplicaRef,
    CatalogDatabase, CatalogDatabaseRef, CatalogNetworkPolicy, CatalogNetworkPolicyRef, CatalogRef,
    CatalogRole, CatalogRoleRef, CatalogSchema, CatalogSchemaRef, ClusterPlanError, RoleAttributes,
};
use mz_sql::names::{
    DatabaseId, QualifiedSchemaName, ResolvedDatabaseSpecifier, SchemaId, SchemaSpecifier,
};
use mz_sql::plan::{AutoScalingStrategy, ClusterSchedule, CreateClusterPlan};
use mz_sql::session::vars::OwnedVarInput;

use super::{CatalogReadRecorder, CatalogState, ConnCatalog, ConnectionId};

trait CatalogObject: 'static {
    type Key: Clone + Debug + Send + Sync + 'static;

    fn lookup<'a>(state: &'a CatalogState, key: &Self::Key) -> Option<&'a Self>;
}

macro_rules! keyed_object {
    ($object:ty, $key:ty, $state:ident, $id:ident, $lookup:expr) => {
        impl CatalogObject for $object {
            type Key = $key;

            fn lookup<'a>($state: &'a CatalogState, $id: &Self::Key) -> Option<&'a Self> {
                $lookup
            }
        }
    };
}

keyed_object!(
    Database,
    DatabaseId,
    state,
    id,
    state.database_by_id.get(id)
);
keyed_object!(Role, RoleId, state, id, state.roles_by_id.get(id));
keyed_object!(NetworkPolicy, NetworkPolicyId, state, id, {
    state.network_policies_by_id.get(id)
});
keyed_object!(Cluster, ClusterId, state, id, state.clusters_by_id.get(id));
keyed_object!(ClusterReplica, (ClusterId, ReplicaId), state, id, {
    state.clusters_by_id.get(&id.0)?.replica(id.1)
});
keyed_object!(
    Schema,
    (ResolvedDatabaseSpecifier, SchemaSpecifier, ConnectionId),
    state,
    id,
    state.try_get_schema(&id.0, &id.1, &id.2)
);

#[derive(Debug)]
struct Observed<'a, T: CatalogObject> {
    inner: &'a T,
    key: T::Key,
    recorder: CatalogReadRecorder,
}

impl<'a, T: CatalogObject> Observed<'a, T> {
    fn new(inner: &'a T, key: T::Key, recorder: CatalogReadRecorder) -> Self {
        let lookup_key = key.clone();
        recorder.record("object exists", true, move |state| {
            T::lookup(state, &lookup_key).is_some()
        });
        Self {
            inner,
            key,
            recorder,
        }
    }

    fn observe<U>(&self, label: &'static str, read: impl Fn(&T) -> U + Send + Sync + 'static)
    where
        U: PartialEq + Send + Sync + 'static,
    {
        let key = self.key.clone();
        self.recorder
            .record(label, Some(read(self.inner)), move |state| {
                T::lookup(state, &key).map(&read)
            });
    }
}

macro_rules! properties {
    ($($name:ident -> $result:ty => $read:expr;)*) => {
        $(
            fn $name(&self) -> $result {
                self.observe(stringify!($name), $read);
                self.inner.$name()
            }
        )*
    };
}

macro_rules! factory {
    ($name:ident, $object:ty, $result:ident, $inner:ident, $key:expr) => {
        pub(super) fn $name<'a>(&self, $inner: &'a $object) -> $result<'a> {
            CatalogRef::Owned(Box::new(Observed::new($inner, $key, self.clone())))
        }
    };
}

impl CatalogReadRecorder {
    factory!(database, Database, CatalogDatabaseRef, inner, inner.id);
    factory!(role, Role, CatalogRoleRef, inner, inner.id);
    factory!(
        network_policy,
        NetworkPolicy,
        CatalogNetworkPolicyRef,
        inner,
        inner.id
    );
    factory!(cluster, Cluster, CatalogClusterRef, inner, inner.id);
    factory!(
        replica,
        ClusterReplica,
        CatalogClusterReplicaRef,
        inner,
        (inner.cluster_id, inner.replica_id)
    );

    pub(super) fn schema<'a>(
        &self,
        inner: &'a Schema,
        conn_id: &ConnectionId,
    ) -> CatalogSchemaRef<'a> {
        let key = (inner.name.database, inner.id, conn_id.clone());
        CatalogRef::Owned(Box::new(Observed::new(inner, key, self.clone())))
    }
}

macro_rules! view {
    ($name:ident, $method:ident, $object:ty, $result:ident) => {
        pub(super) fn $name<'a>(&self, inner: &'a $object) -> $result<'a> {
            match &self.read_recorder {
                Some(recorder) => recorder.$method(inner),
                None => CatalogRef::Borrowed(inner),
            }
        }
    };
}

impl ConnCatalog<'_> {
    view!(database_view, database, Database, CatalogDatabaseRef);
    view!(role_view, role, Role, CatalogRoleRef);
    view!(
        network_policy_view,
        network_policy,
        NetworkPolicy,
        CatalogNetworkPolicyRef
    );
    view!(cluster_view, cluster, Cluster, CatalogClusterRef);
    view!(
        replica_view,
        replica,
        ClusterReplica,
        CatalogClusterReplicaRef
    );

    pub(super) fn schema_view<'a>(
        &self,
        inner: &'a Schema,
        conn_id: &ConnectionId,
    ) -> CatalogSchemaRef<'a> {
        match &self.read_recorder {
            Some(recorder) => recorder.schema(inner, conn_id),
            None => CatalogRef::Borrowed(inner),
        }
    }
}

impl CatalogDatabase for Observed<'_, Database> {
    properties! {
        name -> &str => |database| database.name().to_owned();
        id -> DatabaseId => |database| database.id();
        has_schemas -> bool => |database| database.has_schemas();
        schema_ids -> &BTreeMap<String, SchemaId> => |database| database.schema_ids().clone();
        owner_id -> RoleId => |database| database.owner_id();
        privileges -> &PrivilegeMap => |database| database.privileges().clone();
    }

    fn schemas(&self) -> Vec<CatalogSchemaRef<'_>> {
        self.observe("schemas", |database| {
            database.schemas_by_id.keys().copied().collect::<Vec<_>>()
        });
        self.inner
            .schemas_by_id
            .values()
            .map(|schema| self.recorder.schema(schema, &SYSTEM_CONN_ID))
            .collect()
    }
}

impl CatalogSchema for Observed<'_, Schema> {
    properties! {
        database -> &ResolvedDatabaseSpecifier => |schema| *schema.database();
        name -> &QualifiedSchemaName => |schema| schema.name().clone();
        id -> &SchemaSpecifier => |schema| *schema.id();
        has_items -> bool => |schema| schema.has_items();
        item_ids -> Box<dyn Iterator<Item = CatalogItemId> + '_> =>
            |schema| schema.item_ids().collect::<Vec<_>>();
        owner_id -> RoleId => |schema| schema.owner_id();
        privileges -> &PrivilegeMap => |schema| schema.privileges().clone();
    }
}

impl CatalogRole for Observed<'_, Role> {
    properties! {
        name -> &str => |role| role.name().to_owned();
        id -> RoleId => |role| role.id();
        membership -> &BTreeMap<RoleId, RoleId> => |role| role.membership().clone();
        attributes -> &RoleAttributes => |role| role.attributes().clone();
    }

    fn vars(&self) -> &BTreeMap<String, OwnedVarInput> {
        self.observe("vars", |role| CatalogRole::vars(role).clone());
        CatalogRole::vars(self.inner)
    }
}

impl CatalogNetworkPolicy for Observed<'_, NetworkPolicy> {
    properties! {
        name -> &str => |policy| policy.name().to_owned();
        id -> NetworkPolicyId => |policy| policy.id();
        owner_id -> RoleId => |policy| policy.owner_id();
        privileges -> &PrivilegeMap => |policy| policy.privileges().clone();
    }
}

impl CatalogCluster<'_> for Observed<'_, Cluster> {
    properties! {
        name -> &str => |cluster| cluster.name().to_owned();
        id -> ClusterId => |cluster| cluster.id();
        bound_objects -> &BTreeSet<CatalogItemId> => |cluster| cluster.bound_objects().clone();
        owner_id -> RoleId => |cluster| cluster.owner_id();
        privileges -> &PrivilegeMap => |cluster| cluster.privileges().clone();
        is_managed -> bool => |cluster| cluster.is_managed();
        managed_size -> Option<&str> => |cluster| cluster.managed_size().map(str::to_owned);
        schedule -> Option<&ClusterSchedule> => |cluster| cluster.schedule().cloned();
        replication_factor -> Option<u32> => |cluster| cluster.replication_factor();
        auto_scaling_strategy -> Option<&AutoScalingStrategy> =>
            |cluster| cluster.auto_scaling_strategy().cloned();
        try_to_plan -> Result<CreateClusterPlan, ClusterPlanError> =>
            |cluster| cluster.try_to_plan();
    }

    fn replica_id(&self, name: &str) -> Option<ReplicaId> {
        let observed_name = name.to_owned();
        self.observe("replica name", move |cluster| {
            cluster.replica_id(&observed_name)
        });
        self.inner.replica_id(name)
    }

    fn replicas(&self) -> Vec<CatalogClusterReplicaRef<'_>> {
        self.observe("replicas", |cluster| {
            cluster
                .replicas()
                .map(|replica| replica.replica_id)
                .collect::<Vec<_>>()
        });
        self.inner
            .replicas()
            .map(|replica| self.recorder.replica(replica))
            .collect()
    }

    fn replica(&self, id: ReplicaId) -> CatalogClusterReplicaRef<'_> {
        self.recorder
            .replica(self.inner.replica(id).expect("catalog out of sync"))
    }
}

impl CatalogClusterReplica<'_> for Observed<'_, ClusterReplica> {
    properties! {
        name -> &str => |replica| replica.name().to_owned();
        cluster_id -> ClusterId => |replica| replica.cluster_id();
        replica_id -> ReplicaId => |replica| replica.replica_id();
        owner_id -> RoleId => |replica| replica.owner_id();
        internal -> bool => |replica| replica.internal();
    }
}

#[cfg(test)]
mod tests {
    use mz_sql::catalog::SessionCatalog;
    use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;

    use super::*;
    use crate::AdapterError;
    use crate::catalog::{Catalog, Op};
    use crate::session::DEFAULT_DATABASE_NAME;

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // Catalog bootstrap uses unsupported foreign functions.
    async fn enumerated_temporary_schemas_retain_their_owning_connection() {
        Catalog::with_debug(|catalog| async move {
            let mut state = catalog.state().clone();
            for id in [1, 2] {
                let conn_id = ConnectionId::Static(id);
                state
                    .temporary_namespaces
                    .register(conn_id.clone(), uuid::Uuid::from_u128(u128::from(id)));
                state
                    .temporary_namespaces
                    .ensure_schema(&conn_id, MZ_SYSTEM_ROLE_ID);
            }
            let reads = CatalogReadRecorder::default();
            {
                let catalog = state.for_system_session().with_read_recorder(reads.clone());
                assert_eq!(
                    catalog
                        .get_schemas()
                        .iter()
                        .filter(|schema| *schema.id() == SchemaSpecifier::Temporary)
                        .count(),
                    2,
                );
            }
            let reads = reads.snapshot();
            assert!(reads.check(&state).is_ok());
            state
                .temporary_namespaces
                .unregister(&ConnectionId::Static(2));
            assert!(matches!(
                reads.check(&state),
                Err(AdapterError::DDLTransactionRace)
            ));
            catalog.expire().await;
        })
        .await;
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // Catalog bootstrap uses unsupported foreign functions.
    async fn nested_names_and_membership_are_distinct_observations() {
        Catalog::with_debug(|mut catalog| async move {
            let database_id = catalog
                .resolve_database(DEFAULT_DATABASE_NAME)
                .expect("default database")
                .id();
            let name_reads = CatalogReadRecorder::default();
            assert_eq!(
                catalog
                    .for_system_session()
                    .with_read_recorder(name_reads.clone())
                    .get_database(&database_id)
                    .name(),
                DEFAULT_DATABASE_NAME,
            );
            let timestamp = catalog.current_upper().await;
            catalog
                .transact(
                    None,
                    timestamp,
                    None,
                    vec![Op::CreateSchema {
                        database_id: ResolvedDatabaseSpecifier::Id(database_id),
                        schema_name: "observed_schema".to_owned(),
                        owner_id: MZ_SYSTEM_ROLE_ID,
                    }],
                )
                .await
                .expect("create schema");
            assert!(name_reads.snapshot().check(catalog.state()).is_ok());

            let membership_reads = CatalogReadRecorder::default();
            let child_reads = CatalogReadRecorder::default();
            {
                let catalog = catalog
                    .for_system_session()
                    .with_read_recorder(membership_reads.clone());
                assert!(!catalog.get_database(&database_id).schemas().is_empty());
            }
            {
                let catalog = catalog
                    .for_system_session()
                    .with_read_recorder(child_reads.clone());
                let database = catalog.get_database(&database_id);
                assert!(
                    database
                        .schemas()
                        .iter()
                        .any(|schema| schema.name().schema == "observed_schema")
                );
            }
            let schema_spec = *catalog
                .resolve_schema_in_database(
                    &ResolvedDatabaseSpecifier::Id(database_id),
                    "observed_schema",
                    &SYSTEM_CONN_ID,
                )
                .expect("fixture schema")
                .id();
            let timestamp = catalog.current_upper().await;
            catalog
                .transact(
                    None,
                    timestamp,
                    None,
                    vec![Op::RenameSchema {
                        database_spec: ResolvedDatabaseSpecifier::Id(database_id),
                        schema_spec,
                        new_name: "renamed_schema".to_owned(),
                        check_reserved_names: true,
                    }],
                )
                .await
                .expect("rename schema");
            assert!(membership_reads.snapshot().check(catalog.state()).is_ok());
            assert!(matches!(
                child_reads.snapshot().check(catalog.state()),
                Err(AdapterError::DDLTransactionRace)
            ));
            catalog.expire().await;
        })
        .await;
    }
}
