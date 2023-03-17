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
use tracing::info;

use mz_ore::collections::CollectionExt;
use mz_sql::ast::display::AstDisplay;
use mz_sql::ast::Raw;

use crate::catalog::{Catalog, SerializedCatalogItem};

use super::storage::Transaction;

fn rewrite_items<F>(tx: &mut Transaction, mut f: F) -> Result<(), anyhow::Error>
where
    F: FnMut(&mut Transaction, &mut mz_sql::ast::Statement<Raw>) -> Result<(), anyhow::Error>,
{
    let mut updated_items = BTreeMap::new();
    let items = tx.loaded_items();
    for (id, name, SerializedCatalogItem::V1 { create_sql }) in items {
        let mut stmt = mz_sql::parse::parse(&create_sql)?.into_element();

        f(tx, &mut stmt)?;

        let serialized_item = SerializedCatalogItem::V1 {
            create_sql: stmt.to_ast_string_stable(),
        };

        updated_items.insert(id, (name.item, serialized_item));
    }
    tx.update_items(updated_items)?;
    Ok(())
}

pub(crate) async fn migrate(catalog: &mut Catalog) -> Result<(), anyhow::Error> {
    let mut storage = catalog.storage().await;
    let catalog_version = storage.get_catalog_content_version().await?;
    let catalog_version = match catalog_version {
        Some(v) => Version::parse(&v)?,
        None => Version::new(0, 0, 0),
    };

    info!("migrating from catalog version {:?}", catalog_version);

    let mut tx = storage.transaction().await?;
    // First, do basic AST -> AST transformations.
    rewrite_items(&mut tx, |_tx, _stmt| Ok(()))?;

    // Then, load up a temporary catalog with the rewritten items, and perform
    // some transformations that require introspecting the catalog. These
    // migrations are *weird*: they're rewriting the catalog while looking at
    // it. You probably should be adding a basic AST migration above, unless
    // you are really certain you want one of these crazy migrations.
    let cat = Catalog::load_catalog_items(&mut tx, catalog)?;
    let _conn_cat = cat.for_system_session();
    rewrite_items(&mut tx, |_tx, _item| Ok(()))?;
    tx.commit().await?;
    info!(
        "migration from catalog version {:?} complete",
        catalog_version
    );
    Ok(())
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
