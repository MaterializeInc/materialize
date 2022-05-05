// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use semver::Version;

use mz_ore::collections::CollectionExt;
use mz_sql::ast::display::AstDisplay;
use mz_sql::ast::Raw;
use mz_stash::Append;

use crate::catalog::{Catalog, SerializedCatalogItem};

use super::storage::Transaction;

fn rewrite_items<F, S: Append>(tx: &mut Transaction<S>, mut f: F) -> Result<(), anyhow::Error>
where
    F: FnMut(&mut mz_sql::ast::Statement<Raw>) -> Result<(), anyhow::Error>,
{
    let items = tx.loaded_items();
    for (id, name, def) in items {
        let SerializedCatalogItem::V1 {
            create_sql,
            eval_env,
        } = serde_json::from_slice(&def)?;
        let mut stmt = mz_sql::parse::parse(&create_sql)?.into_element();

        f(&mut stmt)?;

        let serialized_item = SerializedCatalogItem::V1 {
            create_sql: stmt.to_ast_string_stable(),
            eval_env,
        };

        let serialized_item =
            serde_json::to_vec(&serialized_item).expect("catalog serialization cannot fail");
        tx.update_item(id, &name.item, &serialized_item)?;
    }
    Ok(())
}

pub(crate) async fn migrate<S: Append>(catalog: &mut Catalog<S>) -> Result<(), anyhow::Error> {
    let mut storage = catalog.storage().await;
    let catalog_version = storage.get_catalog_content_version().await?;
    let _catalog_version = match Version::parse(&catalog_version) {
        Ok(v) => v,
        // Catalog content versions changed to semver after 0.8.3, so all
        // non-semver versions are less than that.
        Err(_) => Version::new(0, 0, 0),
    };
    let mut tx = storage.transaction().await?;
    // First, do basic AST -> AST transformations.
    rewrite_items(&mut tx, |_stmt| Ok(()))?;

    // Then, load up a temporary catalog with the rewritten items, and perform
    // some transformations that require introspecting the catalog. These
    // migrations are *weird*: they're rewriting the catalog while looking at
    // it. You probably should be adding a basic AST migration above, unless
    // you are really certain you want one of these crazy migrations.
    let cat = Catalog::load_catalog_items(&mut tx, &catalog).await?;
    let _conn_cat = cat.for_system_session();
    rewrite_items(&mut tx, |_item| Ok(()))?;
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
//   materialized.
//
// Please include @benesch on any code reviews that add or edit migrations.

// ****************************************************************************
// AST migrations -- Basic AST -> AST transformations
// ****************************************************************************

// ****************************************************************************
// Semantic migrations -- Weird migrations that require access to the catalog
// ****************************************************************************
