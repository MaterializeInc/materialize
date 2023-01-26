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
use mz_sql::ast::{Raw, Statement, UnresolvedObjectName};
use mz_sql::plan::{Params, PlanContext};

use crate::catalog::{Catalog, ConnCatalog, SerializedCatalogItem};

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
    rewrite_items(&mut tx, |_tx, stmt| {
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
    let conn_cat = cat.for_system_session();
    rewrite_items(&mut tx, |tx, item| {
        normalize_create_secrets(&conn_cat, item)?;
        progress_collection_rewrite(&conn_cat, tx, item)?;
        Ok(())
    })?;
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

/// Add normalization to all CREATE SECRET statements.
// TODO: Released in version 0.44; delete at any later release.
fn normalize_create_secrets(
    cat: &ConnCatalog,
    stmt: &mut mz_sql::ast::Statement<Raw>,
) -> Result<(), anyhow::Error> {
    if matches!(stmt, mz_sql::ast::Statement::CreateSecret(..)) {
        // Resolve Statement<Raw> to Statement<Aug>.
        let (resolved_stmt, _depends_on) = mz_sql::names::resolve(cat, stmt.clone())?;
        // Ok to use `PlanContext::zero()` because wall time is never used.
        let plan = mz_sql::plan::plan(
            Some(&PlanContext::zero()),
            cat,
            resolved_stmt,
            &Params::empty(),
        )?;
        let create_secret_plan = match plan {
            mz_sql::plan::Plan::CreateSecret(plan) => plan,
            _ => unreachable!("create secret statement can only plan into create secret plan"),
        };
        *stmt = mz_sql::parse::parse(&create_secret_plan.secret.create_sql)?.into_element();
    }
    Ok(())
}

// Rewrites all subsource references to be qualified by their IDs, which is the
// mechanism by which `DeferredObjectName` differentiates between user input and
// created objects.
// TODO: delete in version v0.45 (released in v0.43 + 1 additional release)
fn progress_collection_rewrite(
    cat: &ConnCatalog<'_>,
    tx: &mut Transaction<'_>,
    stmt: &mut mz_sql::ast::Statement<Raw>,
) -> Result<(), anyhow::Error> {
    use mz_sql::ast::{CreateSourceConnection, CreateSubsourceOptionName};

    if let Statement::CreateSource(mz_sql::ast::CreateSourceStatement {
        name,
        connection,
        progress_subsource,
        ..
    }) = stmt
    {
        if progress_subsource.is_some() {
            return Ok(());
        }

        let progress_desc = match connection {
            CreateSourceConnection::Kafka(_) => {
                &mz_storage_client::types::sources::KAFKA_PROGRESS_DESC
            }
            CreateSourceConnection::Kinesis { .. } => {
                &mz_storage_client::types::sources::KINESIS_PROGRESS_DESC
            }
            CreateSourceConnection::S3 { .. } => {
                &mz_storage_client::types::sources::S3_PROGRESS_DESC
            }
            CreateSourceConnection::Postgres { .. } => {
                &mz_storage_client::types::sources::PG_PROGRESS_DESC
            }
            CreateSourceConnection::LoadGenerator { .. } => {
                &mz_storage_client::types::sources::LOAD_GEN_PROGRESS_DESC
            }
            CreateSourceConnection::TestScript { .. } => {
                &mz_storage_client::types::sources::TEST_SCRIPT_PROGRESS_DESC
            }
        };

        // Generate a new GlobalId for the subsource.
        let progress_subsource_id =
            mz_repr::GlobalId::User(tx.get_and_increment_id("user".to_string())?);

        // Generate a StatementContext, which is simplest for handling names.
        let scx = mz_sql::plan::StatementContext::new(None, cat);

        // Find an available name.
        let (item, prefix) = name.0.split_last().expect("must have at least one element");
        let mut suggested_name = prefix.to_vec();
        suggested_name.push(format!("{}_progress", item.as_str()).into());

        let partial =
            mz_sql::normalize::unresolved_object_name(UnresolvedObjectName(suggested_name))?;
        let qualified = scx.allocate_qualified_name(partial)?;
        let found_name = scx.catalog.find_available_name(qualified);
        let full_name = scx.catalog.resolve_full_name(&found_name);

        // Grab the item name, which is necessary to add this item to the
        // current transaction.
        let item_name = full_name.item.clone();

        info!(
            "adding progress subsource to {:?}; named {:?}, with id {:?}",
            name, full_name, progress_subsource_id,
        );

        // Generate an unresolved version of the name, which will
        // ultimately go in the parent `CREATE SOURCE` statement.
        let name = UnresolvedObjectName::from(full_name);

        // Generate the progress subsource schema.
        let (columns, table_constraints) = scx.relation_desc_into_table_defs(progress_desc)?;

        // Create the subsource statement
        let subsource = mz_sql::ast::CreateSubsourceStatement {
            name: name.clone(),
            columns,
            constraints: table_constraints,
            if_not_exists: false,
            with_options: vec![mz_sql::ast::CreateSubsourceOption {
                name: CreateSubsourceOptionName::Progress,
                value: Some(mz_sql::ast::WithOptionValue::Value(
                    mz_sql::ast::Value::Boolean(true),
                )),
            }],
        };

        tx.insert_item(
            progress_subsource_id,
            found_name.qualifiers.schema_spec.into(),
            &item_name,
            SerializedCatalogItem::V1 {
                create_sql: subsource.to_ast_string_stable(),
            },
        )?;

        // Place the newly created subsource into the `CREATE SOURCE` statement.
        *progress_subsource = Some(mz_sql::ast::DeferredObjectName::Named(
            mz_sql::ast::RawObjectName::Id(progress_subsource_id.to_string(), name),
        ));
    }
    Ok(())
}
