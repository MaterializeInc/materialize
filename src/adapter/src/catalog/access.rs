// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The connection view cannot dereference its backing snapshot. Mutable answers
//! cross a tracked projection or a typed child view. Exempt reads name the
//! synchronization or immutability contract that makes recording unnecessary.

use super::*;

#[derive(Debug)]
pub(super) struct CatalogAccess<'a> {
    state: Cow<'a, CatalogState>,
    recorder: Option<CatalogReadRecorder>,
    database: Option<DatabaseId>,
    search_path: Vec<(ResolvedDatabaseSpecifier, SchemaSpecifier)>,
}

pub(super) enum Exemption {
    /// Humanization and EXPLAIN do not prepare catalog writes.
    Diagnostics,
    /// Builtin identities do not change during a process lifetime.
    BuiltinIdentity,
    /// Startup configuration is immutable, mutable configuration has a global fence.
    Configuration,
}

impl<'a> CatalogAccess<'a> {
    pub(super) fn new(
        state: Cow<'a, CatalogState>,
        recorder: Option<CatalogReadRecorder>,
        database: Option<DatabaseId>,
        search_path: Vec<(ResolvedDatabaseSpecifier, SchemaSpecifier)>,
    ) -> Self {
        Self {
            state,
            recorder,
            database,
            search_path,
        }
    }

    pub(super) fn read_namespace<'s, T, O, F>(
        &'s self,
        read: impl FnOnce(
            &'s CatalogState,
            Option<&'s DatabaseId>,
            &'s [(ResolvedDatabaseSpecifier, SchemaSpecifier)],
        ) -> T,
        capture: impl FnOnce(&T) -> (&'static str, O, F),
    ) -> T
    where
        O: PartialEq + Send + Sync + 'static,
        F: Fn(&CatalogState) -> O + Send + Sync + 'static,
    {
        self.read(
            |state| read(state, self.database.as_ref(), &self.search_path),
            capture,
        )
    }

    pub(super) fn database_for_diagnostics(&self) -> Option<DatabaseId> {
        self.database
    }

    pub(super) fn attach_recorder(&mut self, recorder: CatalogReadRecorder) {
        assert!(
            self.recorder.is_none(),
            "catalog view already has a read recorder"
        );
        self.recorder = Some(recorder);
    }

    pub(super) fn exempt(&self, _reason: Exemption) -> &CatalogState {
        &self.state
    }

    pub(super) fn untracked(&self) -> Option<&CatalogState> {
        self.recorder.is_none().then_some(&self.state)
    }

    pub(super) fn configuration_mut(&mut self) -> &mut SystemVars {
        Arc::make_mut(&mut self.state.to_mut().system_configuration)
    }

    pub(super) fn for_system_session(&self) -> ConnCatalog<'_> {
        let mut catalog = self.state.for_system_session();
        catalog.state.recorder = self.recorder.clone();
        catalog
    }

    /// Reads a projection and captures its equality predicate only when tracking.
    /// The capture must describe exactly the answer's mutable inputs.
    pub(super) fn read<'s, T, O, F>(
        &'s self,
        read: impl FnOnce(&'s CatalogState) -> T,
        capture: impl FnOnce(&T) -> (&'static str, O, F),
    ) -> T
    where
        O: PartialEq + Send + Sync + 'static,
        F: Fn(&CatalogState) -> O + Send + Sync + 'static,
    {
        let answer = read(&self.state);
        if let Some(recorder) = &self.recorder {
            let (label, expected, replay) = capture(&answer);
            recorder.record(label, expected, replay);
        }
        answer
    }

    pub(super) fn observe<T>(
        &self,
        label: &'static str,
        read: impl Fn(&CatalogState) -> T + Send + Sync + 'static,
    ) -> T
    where
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        let answer = read(&self.state);
        if let Some(recorder) = &self.recorder {
            recorder.record(label, answer.clone(), read);
        }
        answer
    }

    pub(super) fn collection(
        &self,
        item_id: CatalogItemId,
        id: &GlobalId,
    ) -> Box<dyn mz_sql::catalog::CatalogCollectionItem> {
        let entry = self.state.get_entry(&item_id);
        let version = match &entry.item {
            mz_catalog::memory::objects::CatalogItem::Table(table) => {
                let (version, _) = table
                    .collections
                    .iter()
                    .find(|(_, gid)| *gid == id)
                    .expect("catalog out of sync, mismatched GlobalId");
                RelationVersionSelector::Specific(*version)
            }
            _ => RelationVersionSelector::Latest,
        };
        self.item(&item_id).at_version(version)
    }

    pub(super) fn item(&self, id: &CatalogItemId) -> CatalogItemRef<'_> {
        let item = self.state.get_entry(id);
        match &self.recorder {
            Some(recorder) => recorder.item(item),
            None => CatalogRef::Borrowed(item),
        }
    }

    pub(super) fn system_type(&self, name: &str) -> CatalogItemRef<'_> {
        let item = self.state.get_system_type(name);
        match &self.recorder {
            Some(recorder) => recorder.item(item),
            None => CatalogRef::Borrowed(item),
        }
    }

    pub(super) fn database(&self, id: &DatabaseId) -> mz_sql::catalog::CatalogDatabaseRef<'_> {
        let inner = self
            .state
            .database_by_id
            .get(id)
            .expect("database doesn't exist");
        match &self.recorder {
            Some(recorder) => recorder.database(inner),
            None => CatalogRef::Borrowed(inner),
        }
    }

    pub(super) fn schema(
        &self,
        database: &ResolvedDatabaseSpecifier,
        schema: &SchemaSpecifier,
        conn_id: &ConnectionId,
    ) -> mz_sql::catalog::CatalogSchemaRef<'_> {
        let inner = self.state.get_schema(database, schema, conn_id);
        match &self.recorder {
            Some(recorder) => recorder.schema(inner, conn_id),
            None => CatalogRef::Borrowed(inner),
        }
    }

    pub(super) fn role(&self, id: &RoleId) -> mz_sql::catalog::CatalogRoleRef<'_> {
        let inner = self.state.get_role(id);
        match &self.recorder {
            Some(recorder) => recorder.role(inner),
            None => CatalogRef::Borrowed(inner),
        }
    }

    pub(super) fn network_policy(
        &self,
        id: &NetworkPolicyId,
    ) -> mz_sql::catalog::CatalogNetworkPolicyRef<'_> {
        let inner = self.state.get_network_policy(id);
        match &self.recorder {
            Some(recorder) => recorder.network_policy(inner),
            None => CatalogRef::Borrowed(inner),
        }
    }

    pub(super) fn cluster(&self, id: ClusterId) -> mz_sql::catalog::CatalogClusterRef<'_> {
        let inner = &self.state.clusters_by_id[&id];
        match &self.recorder {
            Some(recorder) => recorder.cluster(inner),
            None => CatalogRef::Borrowed(inner),
        }
    }

    pub(super) fn replica(
        &self,
        cluster: ClusterId,
        replica: ReplicaId,
    ) -> mz_sql::catalog::CatalogClusterReplicaRef<'_> {
        let inner = self.state.clusters_by_id[&cluster]
            .replica(replica)
            .expect("catalog out of sync");
        match &self.recorder {
            Some(recorder) => recorder.replica(inner),
            None => CatalogRef::Borrowed(inner),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn untracked_projection_does_not_capture() {
        let state = CatalogState::empty_test();
        let access = CatalogAccess::new(Cow::Borrowed(&state), None, None, Vec::new());
        assert_eq!(
            access.read(
                |_| 7,
                |_| -> (&'static str, (), fn(&CatalogState)) { panic!("untracked capture") }
            ),
            7
        );
    }

    #[mz_ore::test]
    #[should_panic(expected = "catalog view already has a read recorder")]
    fn recorder_cannot_be_replaced() {
        let state = CatalogState::empty_test();
        let mut access = CatalogAccess::new(
            Cow::Borrowed(&state),
            Some(CatalogReadRecorder::default()),
            None,
            Vec::new(),
        );
        access.attach_recorder(CatalogReadRecorder::default());
    }
}
