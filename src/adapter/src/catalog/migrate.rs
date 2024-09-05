// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use futures::future::BoxFuture;
use mz_catalog::durable::Item;
use mz_catalog::memory::objects::{StateDiff, StateUpdate};
use mz_catalog::{durable::Transaction, memory::objects::StateUpdateKind};
use mz_ore::collections::CollectionExt;
use mz_ore::now::NowFn;
use mz_proto::RustType;
use mz_repr::{GlobalId, Timestamp};
use mz_sql::ast::display::AstDisplay;
use mz_sql::ast::visit_mut::VisitMut;
use mz_sql::ast::{CreateSourceStatement, UnresolvedItemName, Value};
use mz_sql_parser::ast::{Raw, Statement};
use mz_storage_types::connections::ConnectionContext;
use prost::Message;
use semver::Version;
use tracing::info;

// DO NOT add any more imports from `crate` outside of `crate::catalog`.
use crate::catalog::{CatalogState, ConnCatalog};

async fn rewrite_ast_items<F>(tx: &mut Transaction<'_>, mut f: F) -> Result<(), anyhow::Error>
where
    F: for<'a> FnMut(
        &'a mut Transaction<'_>,
        GlobalId,
        &'a mut Statement<Raw>,
        &'a Vec<(Item, Statement<Raw>)>,
    ) -> BoxFuture<'a, Result<(), anyhow::Error>>,
{
    let mut updated_items = BTreeMap::new();
    let items_with_statements = tx
        .get_items()
        .map(|item| {
            let stmt = mz_sql::parse::parse(&item.create_sql)?.into_element().ast;
            Ok((item, stmt))
        })
        .collect::<Result<Vec<_>, anyhow::Error>>()?;

    // Clone this vec to be referenced within the closure if needed
    // TODO(roshan): Remove this once the `ast_rewrite_create_postgres_subsource_text_columns`
    // migration is removed.
    let items_with_statements_ref = items_with_statements.clone();

    for (mut item, mut stmt) in items_with_statements {
        f(tx, item.id, &mut stmt, &items_with_statements_ref).await?;

        item.create_sql = stmt.to_ast_string_stable();

        updated_items.insert(item.id, item);
    }
    tx.update_items(updated_items)?;
    Ok(())
}

async fn rewrite_items<F>(
    tx: &mut Transaction<'_>,
    cat: &ConnCatalog<'_>,
    mut f: F,
) -> Result<(), anyhow::Error>
where
    F: for<'a> FnMut(
        &'a mut Transaction<'_>,
        &'a &ConnCatalog<'_>,
        GlobalId,
        &'a mut Statement<Raw>,
    ) -> BoxFuture<'a, Result<(), anyhow::Error>>,
{
    let mut updated_items = BTreeMap::new();
    let items = tx.get_items();
    for mut item in items {
        let mut stmt = mz_sql::parse::parse(&item.create_sql)?.into_element().ast;

        f(tx, &cat, item.id, &mut stmt).await?;

        item.create_sql = stmt.to_ast_string_stable();

        updated_items.insert(item.id, item);
    }
    tx.update_items(updated_items)?;
    Ok(())
}

pub(crate) async fn migrate(
    state: &CatalogState,
    tx: &mut Transaction<'_>,
    _now: NowFn,
    _boot_ts: Timestamp,
    _connection_context: &ConnectionContext,
) -> Result<(), anyhow::Error> {
    let catalog_version = tx.get_catalog_content_version();
    let catalog_version = match catalog_version {
        Some(v) => Version::parse(&v)?,
        None => Version::new(0, 0, 0),
    };

    info!(
        "migrating statements from catalog version {:?}",
        catalog_version
    );

    rewrite_ast_items(tx, |_tx, _id, stmt, all_items_and_statements| {
        Box::pin(async move {
            // Add per-item AST migrations below.
            //
            // Each migration should be a function that takes `stmt` (the AST
            // representing the creation SQL for the item) as input. Any
            // mutations to `stmt` will be staged for commit to the catalog.
            //
            // Migration functions may also take `tx` as input to stage
            // arbitrary changes to the catalog.
            ast_rewrite_create_postgres_subsource_text_columns(stmt, all_items_and_statements)?;
            ast_rewrite_create_load_gen_subsource_details(stmt)?;
            Ok(())
        })
    })
    .await?;

    // Load up a temporary catalog.
    let mut state = state.clone();
    let item_updates = tx
        .get_items()
        .map(|item| StateUpdate {
            kind: StateUpdateKind::Item(item),
            ts: tx.commit_ts(),
            diff: StateDiff::Addition,
        })
        .collect();
    // The catalog is temporary, so we can throw out the builtin updates.
    let _ = state.apply_updates_for_bootstrap(item_updates).await;

    info!("migrating from catalog version {:?}", catalog_version);

    let conn_cat = state.for_system_session();

    rewrite_items(tx, &conn_cat, |_tx, _conn_cat, _id, _stmt| {
        let _catalog_version = catalog_version.clone();
        Box::pin(async move {
            // Add per-item, post-planning AST migrations below. Most
            // migrations should be in the above `rewrite_ast_items` block.
            //
            // Each migration should be a function that takes `item` (the AST
            // representing the creation SQL for the item) as input. Any
            // mutations to `item` will be staged for commit to the catalog.
            //
            // Be careful if you reference `conn_cat`. Doing so is *weird*,
            // as you'll be rewriting the catalog while looking at it. If
            // possible, make your migration independent of `conn_cat`, and only
            // consider a single item at a time.
            //
            // Migration functions may also take `tx` as input to stage
            // arbitrary changes to the catalog.
            Ok(())
        })
    })
    .await?;

    // Add whole-catalog migrations below.
    //
    // Each migration should be a function that takes `tx` and `conn_cat` as
    // input and stages arbitrary transformations to the catalog on `tx`.

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
// Please include the adapter team on any code reviews that add or edit
// migrations.

/// Copies the 'TEXT COLUMNS' option from the relevant `CREATE SOURCE` statement to each
/// `CREATE SUBSOURCE` statement for Postgres subsources.
/// This is a fix migration for a bug in the previous `ast_rewrite_create_subsource_options`
/// migration that rolled out in `v0.111` and `v0.112` that did not copy the TEXT COLUMNS
/// options for Postgres subsource due to a mistake in the expected structure of each
/// TEXT COLUMN `UnresolvedItemName` for Postgres.
fn ast_rewrite_create_postgres_subsource_text_columns(
    stmt: &mut Statement<Raw>,
    all_items_and_statements: &Vec<(Item, Statement<Raw>)>,
) -> Result<(), anyhow::Error> {
    use mz_sql::ast::{
        CreateSourceConnection, CreateSubsourceOption, CreateSubsourceOptionName,
        CreateSubsourceStatement, PgConfigOptionName, RawItemName, WithOptionValue,
    };

    // Since subsources have named-only references to their `of_source` and some have the
    // global_id of their source, we first generate mapping from all source names and ids to their
    // statements.
    let source_name_to_stmt: BTreeMap<_, _> = all_items_and_statements
        .iter()
        .filter_map(|(_, statement)| match statement {
            Statement::CreateSource(stmt) => Some((stmt.name.clone(), stmt)),
            _ => None,
        })
        .collect();
    let source_id_to_stmt: BTreeMap<_, _> = all_items_and_statements
        .iter()
        .filter_map(|(item, statement)| match statement {
            Statement::CreateSource(stmt) => Some((item.id, stmt)),
            _ => None,
        })
        .collect();

    struct Rewriter<'a> {
        source_name_to_stmt: BTreeMap<UnresolvedItemName, &'a CreateSourceStatement<Raw>>,
        source_id_to_stmt: BTreeMap<GlobalId, &'a CreateSourceStatement<Raw>>,
    }

    impl<'ast> VisitMut<'ast, Raw> for Rewriter<'_> {
        fn visit_create_subsource_statement_mut(
            &mut self,
            node: &'ast mut CreateSubsourceStatement<Raw>,
        ) {
            match &node.of_source {
                // Not a source-export subsource
                None => (),
                Some(source) => {
                    let text_cols_option = node
                        .with_options
                        .iter()
                        .find(|o| o.name == CreateSubsourceOptionName::TextColumns);
                    if text_cols_option.is_some() {
                        // if this subsource already has text-columns it does not need to be fixed.
                        return;
                    }
                    let source_statement = match &source {
                        RawItemName::Name(name) => self
                            .source_name_to_stmt
                            .get(name)
                            .expect("source must exist"),
                        RawItemName::Id(id, _) => {
                            let gid = id
                                .parse()
                                .expect("RawItenName::Id must be uncorrupted GlobalId");
                            self.source_id_to_stmt.get(&gid).expect("source must exist")
                        }
                    };
                    if !matches!(
                        &source_statement.connection,
                        CreateSourceConnection::Postgres { .. }
                    ) {
                        // We only need to fix Postgres subsources.
                        return;
                    }

                    info!("migrate: populating subsource details: {:?}", node);

                    let external_reference = node
                        .with_options
                        .iter()
                        .find(|o| o.name == CreateSubsourceOptionName::ExternalReference)
                        .expect("subsources must have external reference");
                    // For postgres sources the `external_reference` does include the database name
                    // but since all tables in a source belong to the same publication they share
                    // the same database too, so we can effectively ignore it.
                    let (external_schema, external_table) = match &external_reference.value {
                        Some(WithOptionValue::UnresolvedItemName(name)) => {
                            let name_len = name.0.len();
                            (
                                name.0[name_len - 2].clone().into_string(),
                                name.0[name_len - 1].clone().into_string(),
                            )
                        }
                        _ => unreachable!("external reference must be an unresolved item name"),
                    };

                    match &source_statement.connection {
                        CreateSourceConnection::Postgres {
                            connection: _,
                            options,
                        } => {
                            // Copy the relevant Text Columns from the top-level source option into the subsource option
                            let text_columns = options
                                .iter()
                                .find(|o| o.name == PgConfigOptionName::TextColumns);
                            if let Some(text_columns) = text_columns {
                                let table_text_columns = self.postgres_columns_for_table(
                                    &external_schema,
                                    &external_table,
                                    &text_columns.value,
                                );
                                if table_text_columns.len() > 0 {
                                    node.with_options.push(CreateSubsourceOption {
                                        name: CreateSubsourceOptionName::TextColumns,
                                        value: Some(WithOptionValue::Sequence(table_text_columns)),
                                    });
                                }
                            }
                        }
                        _ => unreachable!("already filtered to just postgres subsources"),
                    };
                    info!("migrated subsource: {:?}", node);
                }
            }
        }
    }

    impl Rewriter<'_> {
        fn postgres_columns_for_table(
            &self,
            external_schema: &str,
            external_table: &str,
            all_columns: &Option<WithOptionValue<Raw>>,
        ) -> Vec<WithOptionValue<Raw>> {
            let all_table_columns = match all_columns {
                Some(WithOptionValue::Sequence(columns)) => {
                    columns.into_iter().map(|column| match column {
                        WithOptionValue::UnresolvedItemName(name) => name,
                        _ => unreachable!("text columns must be UnresolvedItemName"),
                    })
                }
                _ => {
                    unreachable!("Source columns value must be a sequence")
                }
            };

            all_table_columns
                .filter_map(|name| {
                    // Postgres CREATE SOURCE statement text columns are an UnresolvedItemName with
                    // (database, schema, table, column) tuples
                    // and we only need to copy the column name for the subsource option
                    let name_len = name.0.len();
                    if name.0[name_len - 3].clone().into_string() == external_schema
                        && name.0[name_len - 2].clone().into_string() == external_table
                    {
                        Some(WithOptionValue::Ident(name.0[name_len - 1].clone()))
                    } else {
                        None
                    }
                })
                .collect()
        }
    }
    Rewriter {
        source_name_to_stmt,
        source_id_to_stmt,
    }
    .visit_statement_mut(stmt);

    Ok(())
}

/// Populates the 'details' option stored on Load Generator Subsource statements
/// with the output type of the subsource, to avoid needing to parse 'external references'
/// anywhere beyond SQL statement purification.
fn ast_rewrite_create_load_gen_subsource_details(
    stmt: &mut Statement<Raw>,
) -> Result<(), anyhow::Error> {
    use mz_sql::ast::{CreateSubsourceOptionName, CreateSubsourceStatement, WithOptionValue};
    use mz_storage_types::sources::load_generator::{
        AuctionView, LoadGeneratorOutput, MarketingView,
        ProtoLoadGeneratorSourceExportStatementDetails, TpchView, LOAD_GENERATOR_DATABASE_NAME,
    };
    use mz_storage_types::sources::proto_source_export_statement_details::Kind;
    use mz_storage_types::sources::ProtoSourceExportStatementDetails;

    struct Rewriter {}

    impl<'ast> VisitMut<'ast, Raw> for Rewriter {
        fn visit_create_subsource_statement_mut(
            &mut self,
            node: &'ast mut CreateSubsourceStatement<Raw>,
        ) {
            match &node.of_source {
                // Not a source-export subsource
                None => (),
                Some(_source) => {
                    let details_option = node
                        .with_options
                        .iter()
                        .find(|o| o.name == CreateSubsourceOptionName::Details)
                        .expect("subsources must have details");
                    let details_val = match &details_option.value {
                        Some(WithOptionValue::Value(Value::String(details))) => details,
                        _ => unreachable!("subsource details value must be a string"),
                    };
                    let details = hex::decode(details_val)
                        .expect("subsource details must be a hex-encoded string");
                    let details = ProtoSourceExportStatementDetails::decode(&*details)
                        .expect("subsource details failed to decode");
                    match details.kind {
                        Some(Kind::Loadgen(loadgen_details)) => {
                            if loadgen_details.output.is_some() {
                                // This subsource already has the output in its details
                                return;
                            }
                        }
                        // Not a loadgen subsource
                        _ => return,
                    }

                    // Figure out the correct load-gen output for this subsource based on the
                    // external reference normalized and stored during purification. This is
                    // only needed one time for this migration and in the future we will determine
                    // the output directly during purification.
                    let external_reference = node
                        .with_options
                        .iter()
                        .find(|o| o.name == CreateSubsourceOptionName::ExternalReference)
                        .expect("subsources must have external reference");
                    let output = match &external_reference.value {
                        Some(WithOptionValue::UnresolvedItemName(name)) => {
                            // This name is generated during purification
                            // by the `load_generator_ast_to_generator` function as
                            // (LOAD_GENERATOR_DATABASE_NAME, LoadGenerator::schema_name(), view_name)
                            let inner = &name.0;
                            assert_eq!(inner[0].as_str(), LOAD_GENERATOR_DATABASE_NAME);
                            match inner[1].as_str() {
                                "auction" => LoadGeneratorOutput::Auction(AuctionView::from(
                                    inner[2].as_str(),
                                )),
                                "marketing" => LoadGeneratorOutput::Marketing(MarketingView::from(
                                    inner[2].as_str(),
                                )),
                                "tpch" => {
                                    LoadGeneratorOutput::Tpch(TpchView::from(inner[2].as_str()))
                                }
                                _ => LoadGeneratorOutput::Default,
                            }
                        }
                        _ => unreachable!("external reference must be an unresolved item name"),
                    };
                    let new_details = ProtoSourceExportStatementDetails {
                        kind: Some(Kind::Loadgen(
                            ProtoLoadGeneratorSourceExportStatementDetails {
                                output: Some(output.into_proto()),
                            },
                        )),
                    };
                    let details_option = node
                        .with_options
                        .iter_mut()
                        .find(|o| o.name == CreateSubsourceOptionName::Details)
                        .expect("subsources must have details");
                    details_option.value = Some(WithOptionValue::Value(Value::String(
                        hex::encode(new_details.encode_to_vec()),
                    )));
                }
            }
        }
    }

    Rewriter {}.visit_statement_mut(stmt);

    Ok(())
}

// Durable migrations

/// Migrations that run only on the durable catalog before any data is loaded into memory.
pub(crate) fn durable_migrate(
    _tx: &mut Transaction,
    _boot_ts: Timestamp,
) -> Result<(), anyhow::Error> {
    Ok(())
}

// Add new migrations below their appropriate heading, and precede them with a
// short summary of the migration's purpose and optional additional commentary
// about safety or approach.
//
// The convention is to name the migration function using snake case:
// > <category>_<description>_<version>
//
// Please include the adapter team on any code reviews that add or edit
// migrations.
