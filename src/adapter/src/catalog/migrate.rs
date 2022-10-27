// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use semver::Version;
use std::collections::BTreeMap;

use mz_ore::collections::CollectionExt;
use mz_sql::ast::{
    display::AstDisplay, CreateSourceStatement, CreateSourceSubsource, DeferredObjectName,
    RawObjectName, Statement, UnresolvedObjectName,
};
use mz_sql::ast::{CreateReferencedSubsources, Raw};
use mz_stash::Append;

use crate::catalog::{Catalog, ConnCatalog, SerializedCatalogItem, SYSTEM_CONN_ID};

use super::storage::Transaction;

fn rewrite_items<F, S: Append>(tx: &mut Transaction<S>, mut f: F) -> Result<(), anyhow::Error>
where
    F: FnMut(&mut mz_sql::ast::Statement<Raw>) -> Result<(), anyhow::Error>,
{
    let mut updated_items = BTreeMap::new();
    let items = tx.loaded_items();
    for (id, name, SerializedCatalogItem::V1 { create_sql }) in items {
        let mut stmt = mz_sql::parse::parse(&create_sql)?.into_element();

        f(&mut stmt)?;

        let serialized_item = SerializedCatalogItem::V1 {
            create_sql: stmt.to_ast_string_stable(),
        };

        updated_items.insert(id, (name.item, serialized_item));
    }
    tx.update_items(updated_items)?;
    Ok(())
}

pub(crate) async fn migrate<S: Append>(catalog: &mut Catalog<S>) -> Result<(), anyhow::Error> {
    let mut storage = catalog.storage().await;
    let catalog_version = storage.get_catalog_content_version().await?;
    let _catalog_version = match catalog_version {
        Some(v) => Version::parse(&v)?,
        None => Version::new(0, 0, 0),
    };
    let mut tx = storage.transaction().await?;
    // First, do basic AST -> AST transformations.
    rewrite_items(&mut tx, |_stmt| Ok(()))?;

    // Then, load up a temporary catalog with the rewritten items, and perform
    // some transformations that require introspecting the catalog. These
    // migrations are *weird*: they're rewriting the catalog while looking at
    // it. You probably should be adding a basic AST migration above, unless
    // you are really certain you want one of these crazy migrations.
    let cat = Catalog::load_catalog_items(&mut tx, catalog)?;
    let conn_cat = cat.for_system_session();

    rewrite_items(&mut tx, |item| {
        deferred_object_name_rewrite(&conn_cat, item)?;
        Ok(())
    })?;
    tx.commit().await.map_err(|e| e.into())
}

// Add new migrations below their appropriate heading, and precede them with a
// short summary of the migration's purpose and optional additional commentary
// about safety or approach.
//
// The convention is to name the migration function using snake case:
// > <category>_<description>_<version>
//
// Note that:
// - The sum of all migrations must be idempotent because all migrations run
//   every time the catalog opens, unless migrations are explicitly disabled.
//   This might mean changing code outside the migration itself, or only
//   executing some migrations when encountering certain versions.
// - Migrations must preserve backwards compatibility with all past releases of
//   Materialize.
//
// Please include @benesch on any code reviews that add or edit migrations.

// ****************************************************************************
// AST migrations -- Basic AST -> AST transformations
// ****************************************************************************

// ****************************************************************************
// Semantic migrations -- Weird migrations that require access to the catalog
// ****************************************************************************

// Rewrites all subsource references to be qualified by their IDs, which is the
// mechanism by which `DeferredObjectName` differentiates between user input and
// created objects.
fn deferred_object_name_rewrite(
    cat: &ConnCatalog,
    stmt: &mut mz_sql::ast::Statement<Raw>,
) -> Result<(), anyhow::Error> {
    if let Statement::CreateSource(CreateSourceStatement {
        subsources: Some(CreateReferencedSubsources::Subset(create_source_subsources)),
        ..
    }) = stmt
    {
        for CreateSourceSubsource { subsource, .. } in create_source_subsources {
            let object_name = subsource.as_mut().unwrap();
            let name: UnresolvedObjectName = match object_name {
                DeferredObjectName::Deferred(name) => name.clone(),
                DeferredObjectName::Named(..) => continue,
            };

            let partial_subsource_name =
                mz_sql::normalize::unresolved_object_name(name.clone()).expect("resolvable");
            let qualified_subsource_name = cat
                .resolve_item_name(&partial_subsource_name)
                .expect("known to exist");
            let entry = cat
                .state
                .try_get_entry_in_schema(qualified_subsource_name, SYSTEM_CONN_ID)
                .expect("known to exist");
            let id = entry.id();

            *subsource = Some(DeferredObjectName::Named(RawObjectName::Id(
                id.to_string(),
                name,
            )));
        }
    }
    Ok(())
}
