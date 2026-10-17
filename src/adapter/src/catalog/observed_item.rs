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

pub(super) fn item<'a>(
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

trait ItemView {
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

struct ObservedItem<I> {
    inner: I,
    recorder: CatalogReadRecorder,
    version: Option<RelationVersionSelector>,
}

impl<I: ItemView> ObservedItem<I> {
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

macro_rules! properties {
    ($($name:ident -> $result:ty => $read:expr;)*) => {
        $(
            fn $name(&self) -> $result {
                self.observe(stringify!($name), $read);
                self.inner.as_item().$name()
            }
        )*
    };
}

impl<I: ItemView> CatalogItem for ObservedItem<I> {
    properties! {
        name -> &QualifiedItemName => |item| item.name().clone();
        id -> CatalogItemId => |item| item.id();
        global_ids -> Box<dyn Iterator<Item = GlobalId> + '_> =>
            |item| item.global_ids().collect::<Vec<_>>();
        oid -> u32 => |item| item.oid();
        func -> Result<&'static Func, CatalogError> =>
            |item| item.func().map(|func| std::ptr::from_ref(func).addr());
        source_desc -> Result<Option<&SourceDesc<ReferencedConnection>>, CatalogError> =>
            |item| item.source_desc().map(|desc| desc.cloned());
        connection -> Result<Connection<ReferencedConnection>, CatalogError> =>
            |item| item.connection();
        item_type -> CatalogItemType => |item| item.item_type();
        create_sql -> &str => |item| item.create_sql().to_owned();
        references -> &ResolvedIds => |item| item.references().clone();
        uses -> BTreeSet<CatalogItemId> => |item| item.uses();
        referenced_by -> &[CatalogItemId] => |item| item.referenced_by().to_owned();
        used_by -> &[CatalogItemId] => |item| item.used_by().to_owned();
        subsource_details -> Option<(CatalogItemId, &UnresolvedItemName, &SourceExportDetails)> =>
            |item| {
                item.subsource_details()
                    .map(|(id, name, details)| (id, name.clone(), details.clone()))
            };
        source_export_details -> Option<(
            CatalogItemId,
            &UnresolvedItemName,
            &SourceExportDetails,
            &SourceExportDataConfig<ReferencedConnection>,
        )> => |item| item.source_export_details().map(|(id, name, details, config)|
            (id, name.clone(), details.clone(), config.clone()));
        is_progress_source -> bool => |item| item.is_progress_source();
        progress_id -> Option<CatalogItemId> => |item| item.progress_id();
        index_details -> Option<(&[MirScalarExpr], GlobalId)> =>
            |item| item.index_details().map(|(keys, id)| (keys.to_owned(), id));
        writable_table_details -> Option<&[Expr<Aug>]> =>
            |item| item.writable_table_details().map(|exprs| exprs.to_owned());
        replacement_target -> Option<CatalogItemId> => |item| item.replacement_target();
        type_details -> Option<&CatalogTypeDetails<IdReference>> =>
            |item| item.type_details().cloned();
        owner_id -> RoleId => |item| item.owner_id();
        privileges -> &PrivilegeMap => |item| item.privileges().clone();
        cluster_id -> Option<ClusterId> => |item| item.cluster_id();
        latest_version -> Option<RelationVersion> => |item| item.latest_version();
    }

    fn at_version(&self, version: RelationVersionSelector) -> Box<dyn CatalogCollectionItem> {
        Box::new(ObservedItem {
            inner: self.inner.as_item().at_version(version),
            recorder: self.recorder.clone(),
            version: Some(version),
        })
    }
}

impl CatalogCollectionItem for ObservedItem<Box<dyn CatalogCollectionItem>> {
    fn relation_desc(&self) -> Option<Cow<'_, RelationDesc>> {
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

    fn global_id(&self) -> GlobalId {
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
