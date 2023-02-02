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
use mz_sql::ast::display::AstDisplay;
use mz_sql::ast::{Raw, Statement};

use crate::catalog::{Catalog, SerializedCatalogItem};

use super::storage::Transaction;

fn rewrite_items<F>(tx: &mut Transaction, mut f: F) -> Result<(), anyhow::Error>
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

pub(crate) async fn migrate(catalog: &mut Catalog) -> Result<(), anyhow::Error> {
    let mut storage = catalog.storage().await;
    let catalog_version = storage.get_catalog_content_version().await?;
    let _catalog_version = match catalog_version {
        Some(v) => Version::parse(&v)?,
        None => Version::new(0, 0, 0),
    };
    let mut tx = storage.transaction().await?;
    // First, do basic AST -> AST transformations.
    rewrite_items(&mut tx, |stmt| {
        subsource_type_option_rewrite(stmt);
        csr_url_path_rewrite(stmt);
        Ok(())
    })?;

    // Then, load up a temporary catalog with the rewritten items, and perform
    // some transformations that require introspecting the catalog. These
    // migrations are *weird*: they're rewriting the catalog while looking at
    // it. You probably should be adding a basic AST migration above, unless
    // you are really certain you want one of these crazy migrations.
    let cat = Catalog::load_catalog_items(&mut tx, catalog)?;
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
//   Materialize.
//
// Please include @benesch on any code reviews that add or edit migrations.

// ****************************************************************************
// AST migrations -- Basic AST -> AST transformations
// ****************************************************************************

// Mark all current subsources as "references" subsources in anticipation of
// adding "progress" subsources.
// TODO: delete in version v0.45 (released in v0.43 + 1 additional release)
fn subsource_type_option_rewrite(stmt: &mut mz_sql::ast::Statement<Raw>) {
    use mz_sql::ast::CreateSubsourceOptionName;

    if let Statement::CreateSubsource(mz_sql::ast::CreateSubsourceStatement {
        with_options, ..
    }) = stmt
    {
        if !with_options.iter().any(|option| {
            matches!(
                option.name,
                CreateSubsourceOptionName::Progress | CreateSubsourceOptionName::References
            )
        }) {
            with_options.push(mz_sql::ast::CreateSubsourceOption {
                name: CreateSubsourceOptionName::References,
                value: Some(mz_sql::ast::WithOptionValue::Value(
                    mz_sql::ast::Value::Boolean(true),
                )),
            });
        }
    }
}

// Remove any present CSR URL paths because we now error on them. We clear them
// during planning, so this doesn't affect planning.
// TODO: Released in version 0.43; delete at any later release.
fn csr_url_path_rewrite(stmt: &mut mz_sql::ast::Statement<Raw>) {
    use mz_sql::ast::{CreateConnection, CsrConnectionOptionName, Value, WithOptionValue};

    if let Statement::CreateConnection(mz_sql::ast::CreateConnectionStatement {
        connection: CreateConnection::Csr { with_options },
        ..
    }) = stmt
    {
        for opt in with_options.iter_mut() {
            if opt.name != CsrConnectionOptionName::Url {
                continue;
            }
            let Some(WithOptionValue::Value(Value::String(value))) = opt.value.as_mut() else {
                continue;
            };
            if let Ok(mut url) = reqwest::Url::parse(value) {
                url.set_path("");
                *value = url.to_string();
            }
        }
    }
}

// ****************************************************************************
// Semantic migrations -- Weird migrations that require access to the catalog
// ****************************************************************************
