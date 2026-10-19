// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::collections::BTreeSet;

use mz_controller_types::ClusterId;
use mz_expr::MirScalarExpr;
use mz_repr::adt::mz_acl_item::PrivilegeMap;
use mz_repr::role_id::RoleId;
use mz_repr::{CatalogItemId, GlobalId, RelationDesc, RelationVersion, RelationVersionSelector};
use mz_sql::ast::{Expr, UnresolvedItemName};
use mz_sql::catalog::{
    CatalogCollectionItem, CatalogError, CatalogItem, CatalogItemRef, CatalogItemType, CatalogRef,
    CatalogTypeDetails, IdReference,
};
use mz_sql::func::Func;
use mz_sql::names::{Aug, QualifiedItemName, ResolvedIds};
use mz_storage_types::connections::Connection;
use mz_storage_types::connections::inline::ReferencedConnection;
use mz_storage_types::sources::{SourceDesc, SourceExportDataConfig, SourceExportDetails};

use super::CatalogReadRecorder;

use storage::{ItemView, ObservedItem};

pub(super) use storage::item;

macro_rules! properties {
    ($($name:ident -> $result:ty => $value:ident => $snapshot:expr;)*) => {
        $(
            fn $name(&self) -> $result {
                self.read(stringify!($name), |inner| inner.$name(), |inner| {
                    let $value = inner.$name();
                    $snapshot
                })
            }
        )*
    };
}

impl<I: ItemView> CatalogItem for ObservedItem<I> {
    properties! {
        name -> &QualifiedItemName => value => value.clone();
        id -> CatalogItemId => value => value;
        global_ids -> Box<dyn Iterator<Item = GlobalId> + '_> =>
            value => value.collect::<Vec<_>>();
        oid -> u32 => value => value;
        func -> Result<&'static Func, CatalogError> =>
            value => value.map(|func| std::ptr::from_ref(func).addr());
        source_desc -> Result<Option<&SourceDesc<ReferencedConnection>>, CatalogError> =>
            value => value.map(|desc| desc.cloned());
        connection -> Result<Connection<ReferencedConnection>, CatalogError> => value => value;
        item_type -> CatalogItemType => value => value;
        create_sql -> &str => value => value.to_owned();
        references -> &ResolvedIds => value => value.clone();
        uses -> BTreeSet<CatalogItemId> => value => value;
        referenced_by -> &[CatalogItemId] => value => value.to_owned();
        used_by -> &[CatalogItemId] => value => value.to_owned();
        subsource_details -> Option<(
            CatalogItemId,
            &UnresolvedItemName,
            &SourceExportDetails,
        )> =>
            value => value.map(|(id, name, details)| (id, name.clone(), details.clone()));
        source_export_details -> Option<(
            CatalogItemId,
            &UnresolvedItemName,
            &SourceExportDetails,
            &SourceExportDataConfig<ReferencedConnection>,
        )> => value => value.map(|(id, name, details, config)|
            (id, name.clone(), details.clone(), config.clone()));
        is_progress_source -> bool => value => value;
        progress_id -> Option<CatalogItemId> => value => value;
        index_details -> Option<(&[MirScalarExpr], GlobalId)> =>
            value => value.map(|(keys, id)| (keys.to_owned(), id));
        writable_table_details -> Option<&[Expr<Aug>]> =>
            value => value.map(|exprs| exprs.to_owned());
        replacement_target -> Option<CatalogItemId> => value => value;
        type_details -> Option<&CatalogTypeDetails<IdReference>> => value => value.cloned();
        owner_id -> RoleId => value => value;
        privileges -> &PrivilegeMap => value => value.clone();
        cluster_id -> Option<ClusterId> => value => value;
        latest_version -> Option<RelationVersion> => value => value;
    }

    fn at_version(&self, version: RelationVersionSelector) -> Box<dyn CatalogCollectionItem> {
        self.collection_view(version)
    }
}

impl CatalogCollectionItem for ObservedItem<Box<dyn CatalogCollectionItem>> {
    fn relation_desc(&self) -> Option<Cow<'_, RelationDesc>> {
        self.observed_relation_desc()
    }

    fn global_id(&self) -> GlobalId {
        self.observed_global_id()
    }
}

// Raw catalog objects stay behind this boundary. Accessors must supply a
// snapshot covering their read or use a nested view that retains the recorder.
mod storage {
    use super::*;

    pub(in crate::catalog) fn item<'a>(
        recorder: CatalogReadRecorder,
        inner: &'a dyn CatalogItem,
    ) -> CatalogItemRef<'a> {
        let id = inner.id();
        recorder.record("item exists", true, move |state| {
            state.try_get_entry(&id).is_some()
        });
        CatalogRef::Owned(Box::new(ObservedItem {
            inner,
            recorder,
            version: None,
        }))
    }

    pub(super) trait ItemView {
        fn as_item(&self) -> &dyn CatalogItem;
    }

    impl ItemView for &dyn CatalogItem {
        fn as_item(&self) -> &dyn CatalogItem {
            *self
        }
    }

    impl ItemView for Box<dyn CatalogCollectionItem> {
        fn as_item(&self) -> &dyn CatalogItem {
            &**self
        }
    }

    pub(super) struct ObservedItem<I> {
        inner: I,
        recorder: CatalogReadRecorder,
        version: Option<RelationVersionSelector>,
    }

    impl<I: ItemView> ObservedItem<I> {
        pub(super) fn read<'s, T, O>(
            &'s self,
            label: &'static str,
            read: impl FnOnce(&'s dyn CatalogItem) -> T,
            snapshot: impl Fn(&dyn CatalogItem) -> O + Send + Sync + 'static,
        ) -> T
        where
            O: PartialEq + Send + Sync + 'static,
        {
            self.observe(label, snapshot);
            read(self.inner.as_item())
        }

        fn observe<T>(
            &self,
            label: &'static str,
            read: impl Fn(&dyn CatalogItem) -> T + Send + Sync + 'static,
        ) where
            T: PartialEq + Send + Sync + 'static,
        {
            let id = self.inner.as_item().id();
            let version = self.version;
            let expected = Some(read(self.inner.as_item()));
            self.recorder.record(label, expected, move |state| {
                let entry = state.try_get_entry(&id)?;
                Some(match version {
                    Some(version) => read(&*entry.at_version(version)),
                    None => read(entry),
                })
            });
        }
    }

    impl<I: ItemView> ObservedItem<I> {
        pub(super) fn collection_view(
            &self,
            version: RelationVersionSelector,
        ) -> Box<dyn CatalogCollectionItem> {
            Box::new(ObservedItem {
                inner: self.inner.as_item().at_version(version),
                recorder: self.recorder.clone(),
                version: Some(version),
            })
        }
    }

    impl ObservedItem<Box<dyn CatalogCollectionItem>> {
        pub(super) fn observed_relation_desc(&self) -> Option<Cow<'_, RelationDesc>> {
            let id = self.inner.id();
            let version = self.version.expect("collection view has a version");
            let expected = Some(self.inner.relation_desc().map(Cow::into_owned));
            self.recorder
                .record("relation_desc", expected, move |state| {
                    let entry = state.try_get_entry(&id)?;
                    Some(entry.item().relation_desc(version).map(Cow::into_owned))
                });
            self.inner.relation_desc()
        }

        pub(super) fn observed_global_id(&self) -> GlobalId {
            let id = self.inner.id();
            let version = self.version.expect("collection view has a version");
            let answer = self.inner.global_id();
            self.recorder
                .record("global_id", Some(answer), move |state| {
                    state
                        .try_get_entry(&id)?
                        .item()
                        .global_id_for_version(version)
                });
            answer
        }
    }
}

#[cfg(test)]
mod tests {
    use mz_catalog::memory::objects::{CatalogEntry, CatalogItem as Item, Table, TableDataSource};
    use mz_repr::{SqlScalarType, VersionedRelationDesc};
    use mz_sql::catalog::SessionCatalog;
    use mz_sql::names::{
        DatabaseId, ItemQualifiers, ResolvedDatabaseSpecifier, SchemaId, SchemaSpecifier,
    };
    use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;

    use super::*;
    use crate::AdapterError;
    use crate::catalog::CatalogState;

    const ID: CatalogItemId = CatalogItemId::User(1);

    fn state() -> CatalogState {
        let mut state = CatalogState::empty_test();
        state.entry_by_id.insert(
            ID,
            CatalogEntry {
                item: Item::Table(Table {
                    create_sql: Some("CREATE TABLE db.public.t ()".to_owned()),
                    desc: VersionedRelationDesc::new(RelationDesc::empty()),
                    collections: [(RelationVersion::root(), GlobalId::User(1))]
                        .into_iter()
                        .collect(),
                    conn_id: None,
                    resolved_ids: ResolvedIds::empty(),
                    custom_logical_compaction_window: None,
                    is_retained_metrics_object: false,
                    data_source: TableDataSource::TableWrites { defaults: vec![] },
                }),
                referenced_by: vec![],
                used_by: vec![],
                id: ID,
                oid: 1,
                name: QualifiedItemName {
                    qualifiers: ItemQualifiers {
                        database_spec: ResolvedDatabaseSpecifier::Id(DatabaseId::User(1)),
                        schema_spec: SchemaSpecifier::Id(SchemaId::User(1)),
                    },
                    item: "t".to_owned(),
                },
                owner_id: MZ_SYSTEM_ROLE_ID,
                privileges: PrivilegeMap::default(),
            },
        );
        state
    }

    #[mz_ore::test]
    fn only_consumed_item_properties_conflict() {
        let mut state = state();
        let recorder = CatalogReadRecorder::default();
        {
            let catalog = state
                .for_system_session()
                .with_read_recorder(recorder.clone());
            assert_eq!(catalog.for_system_session().get_item(&ID).name().item, "t");
        }
        let reads = recorder.snapshot();

        let entry = state.entry_by_id.get_mut(&ID).expect("fixture item");
        entry.used_by.push(CatalogItemId::User(2));
        entry.oid += 1;
        let Item::Table(table) = &mut entry.item else {
            unreachable!()
        };
        table.create_sql = Some("CREATE TABLE db.public.t ( )".to_owned());
        assert!(reads.check(&state).is_ok());

        state
            .entry_by_id
            .get_mut(&ID)
            .expect("fixture item")
            .name
            .item = "renamed".to_owned();
        assert!(matches!(
            reads.check(&state),
            Err(AdapterError::DDLTransactionRace)
        ));
    }

    #[mz_ore::test]
    fn collection_views_record_properties_and_tolerate_removal() {
        let mut state = state();
        let recorder = CatalogReadRecorder::default();
        let collection = state
            .for_system_session()
            .with_read_recorder(recorder.clone())
            .get_item(&ID)
            .at_version(RelationVersionSelector::Latest);
        assert_eq!(
            collection.relation_desc().map(Cow::into_owned),
            Some(RelationDesc::empty())
        );
        let reads = recorder.snapshot();
        drop(collection);

        let entry = state.entry_by_id.get_mut(&ID).expect("fixture item");
        entry.name.item = "renamed".to_owned();
        assert!(reads.check(&state).is_ok());

        let entry = state.entry_by_id.get_mut(&ID).expect("fixture item");
        let Item::Table(table) = &mut entry.item else {
            unreachable!()
        };
        table.desc = VersionedRelationDesc::new(
            RelationDesc::builder()
                .with_column("n", SqlScalarType::Int32.nullable(false))
                .finish(),
        );
        assert!(matches!(
            reads.check(&state),
            Err(AdapterError::DDLTransactionRace)
        ));

        state.entry_by_id.remove(&ID);
        assert!(matches!(
            reads.check(&state),
            Err(AdapterError::DDLTransactionRace)
        ));
    }
}
