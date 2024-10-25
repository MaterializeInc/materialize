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
use mz_catalog::builtin::BuiltinTable;
use mz_catalog::durable::Transaction;
use mz_catalog::memory::objects::BootstrapStateUpdateKind;
use mz_catalog::memory::objects::StateUpdate;
use mz_ore::collections::CollectionExt;
use mz_ore::now::NowFn;
use mz_repr::{GlobalId, Timestamp};
use mz_sql::ast::display::AstDisplay;
use mz_sql::names::FullItemName;
use mz_sql_parser::ast::{Raw, Statement};
use semver::Version;
use tracing::info;
// DO NOT add any more imports from `crate` outside of `crate::catalog`.
use crate::catalog::open::into_consolidatable_updates_startup;
use crate::catalog::{BuiltinTableUpdate, CatalogState, ConnCatalog};

async fn rewrite_ast_items<F>(tx: &mut Transaction<'_>, mut f: F) -> Result<(), anyhow::Error>
where
    F: for<'a> FnMut(
        &'a mut Transaction<'_>,
        GlobalId,
        &'a mut Statement<Raw>,
    ) -> BoxFuture<'a, Result<(), anyhow::Error>>,
{
    let mut updated_items = BTreeMap::new();

    for mut item in tx.get_items() {
        let mut stmt = mz_sql::parse::parse(&item.create_sql)?.into_element().ast;
        f(tx, item.id, &mut stmt).await?;
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

pub(crate) struct MigrateResult {
    pub(crate) builtin_table_updates: Vec<BuiltinTableUpdate<&'static BuiltinTable>>,
    pub(crate) post_item_updates: Vec<(BootstrapStateUpdateKind, Timestamp, i64)>,
}

/// Migrates all user items and loads them into `state`.
///
/// Returns the builtin updates corresponding to all user items.
pub(crate) async fn migrate(
    state: &mut CatalogState,
    tx: &mut Transaction<'_>,
    item_updates: Vec<StateUpdate>,
    now: NowFn,
    _boot_ts: Timestamp,
) -> Result<MigrateResult, anyhow::Error> {
    let catalog_version = tx.get_catalog_content_version();
    let catalog_version = match catalog_version {
        Some(v) => Version::parse(&v)?,
        None => Version::new(0, 0, 0),
    };

    info!(
        "migrating statements from catalog version {:?}",
        catalog_version
    );

    // Special block for `ast_rewrite_sources_to_tables` migration
    // since it requires a feature flag needs to update multiple AST items at once.
    if state.system_config().force_source_table_syntax() {
        ast_rewrite_sources_to_tables(tx, now)?;
    }

    rewrite_ast_items(tx, |_tx, _id, _stmt| {
        Box::pin(async move {
            // Add per-item AST migrations below.
            //
            // Each migration should be a function that takes `stmt` (the AST
            // representing the creation SQL for the item) as input. Any
            // mutations to `stmt` will be staged for commit to the catalog.
            //
            // Migration functions may also take `tx` as input to stage
            // arbitrary changes to the catalog.

            Ok(())
        })
    })
    .await?;

    // Load items into catalog. We make sure to consolidate the old updates with the new updates to
    // avoid trying to apply unmigrated items.
    let commit_ts = tx.commit_ts();
    let mut item_updates = into_consolidatable_updates_startup(item_updates, commit_ts);
    let op_item_updates = tx.get_and_commit_op_updates();
    let op_item_updates = into_consolidatable_updates_startup(op_item_updates, commit_ts);
    item_updates.extend(op_item_updates);
    differential_dataflow::consolidation::consolidate_updates(&mut item_updates);

    // Since some migrations might introduce non-item 'post-item' updates, we sequester those
    // so they can be applied with other post-item updates after migrations to avoid
    // accumulating negative diffs.
    let (post_item_updates, item_updates): (Vec<_>, Vec<_>) = item_updates
        .into_iter()
        // The only post-item update kind we currently generate is to
        // update storage collection metadata.
        .partition(|(kind, _, _)| {
            matches!(kind, BootstrapStateUpdateKind::StorageCollectionMetadata(_))
        });

    let item_updates = item_updates
        .into_iter()
        .map(|(kind, ts, diff)| StateUpdate {
            kind: kind.into(),
            ts,
            diff: diff.try_into().expect("valid diff"),
        })
        .collect();
    let mut ast_builtin_table_updates = state.apply_updates_for_bootstrap(item_updates).await;

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

    let op_item_updates = tx.get_and_commit_op_updates();
    let item_builtin_table_updates = state.apply_updates_for_bootstrap(op_item_updates).await;

    ast_builtin_table_updates.extend(item_builtin_table_updates);

    info!(
        "migration from catalog version {:?} complete",
        catalog_version
    );
    Ok(MigrateResult {
        builtin_table_updates: ast_builtin_table_updates,
        post_item_updates,
    })
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

/// Migrates all sources to use the new sources as tables model
///
/// First we migrate existing `CREATE SUBSOURCE` statements, turning them into
/// `CREATE TABLE .. FROM SOURCE` statements. This covers existing Postgres,
/// MySQL, and multi-output (tpch, auction, marketing) load-generator subsources.
///
/// Second we migrate existing `CREATE SOURCE` statements for these multi-output
/// sources to remove any subsource-specific options (e.g. TEXT COLUMNS).
///
/// Third we migrate existing single-output `CREATE SOURCE` statements.
/// This includes existing Kafka and single-output load-generator
/// subsources. This will generate an additional `CREATE TABLE .. FROM SOURCE`
/// statement that copies over all the export-specific options. This table will use
/// to the existing source statement's persist shard but use a new GlobalID.
/// The original source statement will be updated to remove the export-specific options,
/// renamed to `<original_name>_source`, and use a new empty shard while keeping its
/// same GlobalId.
///
fn ast_rewrite_sources_to_tables(
    tx: &mut Transaction<'_>,
    now: NowFn,
) -> Result<(), anyhow::Error> {
    use maplit::btreemap;
    use maplit::btreeset;
    use mz_persist_types::ShardId;
    use mz_proto::RustType;
    use mz_sql::ast::{
        CreateSourceConnection, CreateSourceOptionName, CreateSourceStatement,
        CreateSubsourceOptionName, CreateSubsourceStatement, CreateTableFromSourceStatement, Ident,
        KafkaSourceConfigOptionName, LoadGenerator, MySqlConfigOptionName, PgConfigOptionName,
        RawItemName, TableFromSourceColumns, TableFromSourceOption, TableFromSourceOptionName,
        UnresolvedItemName, Value, WithOptionValue,
    };
    use mz_storage_client::controller::StorageTxn;
    use mz_storage_types::sources::load_generator::LoadGeneratorOutput;
    use mz_storage_types::sources::SourceExportStatementDetails;
    use prost::Message;

    let items_with_statements = tx
        .get_items()
        .map(|item| {
            let stmt = mz_sql::parse::parse(&item.create_sql)?.into_element().ast;
            Ok((item, stmt))
        })
        .collect::<Result<Vec<_>, anyhow::Error>>()?;
    let items_with_statements_copied = items_with_statements.clone();

    // Any GlobalId that should be changed to a new GlobalId in any statements that
    // reference it. This is necessary for ensuring downstream statements (e.g.
    // mat views, indexes) that reference a single-output source (e.g. kafka)
    // will now reference the corresponding new table, with the same data, instead.
    let mut changed_ids = BTreeMap::new();

    for (mut item, stmt) in items_with_statements {
        match stmt {
            // Migrate each `CREATE SUBSOURCE` statement to an equivalent
            // `CREATE TABLE .. FROM SOURCE` statement.
            Statement::CreateSubsource(CreateSubsourceStatement {
                name,
                columns,
                constraints,
                of_source,
                if_not_exists,
                mut with_options,
            }) => {
                let raw_source_name = match of_source {
                    // If `of_source` is None then this is a `progress` subsource which we
                    // are not migrating as they are not currently relevant to the new table model.
                    None => return Ok(()),
                    Some(name) => name,
                };
                let source = match raw_source_name {
                    // Some legacy subsources have named-only references to their `of_source`
                    // so we ensure we always use an ID-based reference in the stored
                    // `CREATE TABLE .. FROM SOURCE` statements.
                    RawItemName::Name(name) => {
                        // Convert the name reference to an ID reference.
                        let (source_item, _) = items_with_statements_copied
                            .iter()
                            .find(|(_, statement)| match statement {
                                Statement::CreateSource(stmt) => stmt.name == name,
                                _ => false,
                            })
                            .expect("source must exist");
                        RawItemName::Id(source_item.id.to_string(), name, None)
                    }
                    RawItemName::Id(..) => raw_source_name,
                };

                // The external reference is a `with_option` on subsource statements but is a
                // separate field on table statements.
                let mut i = 0;
                let external_reference = loop {
                    if i >= with_options.len() {
                        panic!("subsource must have an external reference");
                    }
                    if with_options[i].name == CreateSubsourceOptionName::ExternalReference {
                        let option = with_options.remove(i);
                        match option.value {
                            Some(WithOptionValue::UnresolvedItemName(name)) => break name,
                            _ => unreachable!("external reference must be an unresolved item name"),
                        };
                    } else {
                        i += 1;
                    }
                };
                let with_options = with_options
                    .into_iter()
                    .map(|option| {
                        match option.name {
                            CreateSubsourceOptionName::Details => TableFromSourceOption {
                                name: TableFromSourceOptionName::Details,
                                // The `details` option on both subsources and tables is identical, using the same
                                // ProtoSourceExportStatementDetails serialized value.
                                value: option.value,
                            },
                            CreateSubsourceOptionName::TextColumns => TableFromSourceOption {
                                name: TableFromSourceOptionName::TextColumns,
                                value: option.value,
                            },
                            CreateSubsourceOptionName::ExcludeColumns => TableFromSourceOption {
                                name: TableFromSourceOptionName::ExcludeColumns,
                                value: option.value,
                            },
                            CreateSubsourceOptionName::Progress => {
                                panic!("progress option should not exist on this subsource")
                            }
                            CreateSubsourceOptionName::ExternalReference => {
                                // This option is handled separately above.
                                unreachable!()
                            }
                        }
                    })
                    .collect::<Vec<_>>();

                let table = CreateTableFromSourceStatement {
                    name,
                    constraints,
                    columns: mz_sql::ast::TableFromSourceColumns::Defined(columns),
                    if_not_exists,
                    source,
                    external_reference: Some(external_reference.clone()),
                    with_options,
                    // Subsources don't have `envelope`, `include_metadata`, or `format` options.
                    envelope: None,
                    include_metadata: vec![],
                    format: None,
                };

                info!(
                    "migrate: converted subsource {} to table {}",
                    item.create_sql, table
                );
                item.create_sql = Statement::CreateTableFromSource(table).to_ast_string_stable();
                tx.update_item(item.id, item)?;
            }

            // Postgres sources are multi-output sources whose subsources are
            // migrated above. All we need to do is remove the subsource-related
            // options from this statement since they are no longer relevant.
            Statement::CreateSource(CreateSourceStatement {
                connection:
                    mut conn @ (CreateSourceConnection::Postgres { .. }
                    | CreateSourceConnection::Yugabyte { .. }),
                name,
                if_not_exists,
                in_cluster,
                include_metadata,
                format,
                envelope,
                col_names,
                with_options,
                key_constraint,
                external_references,
                progress_subsource,
            }) => {
                let options = match &mut conn {
                    CreateSourceConnection::Postgres { options, .. } => options,
                    CreateSourceConnection::Yugabyte { options, .. } => options,
                    _ => unreachable!("match determined above"),
                };
                // This option storing text columns on the primary source statement is redundant
                // with the option on subsource statements so can just be removed.
                // This was kept for round-tripping of `CREATE SOURCE` statements that automatically
                // generated subsources, which is no longer necessary.
                if options
                    .iter()
                    .any(|o| matches!(o.name, PgConfigOptionName::TextColumns))
                {
                    options.retain(|o| !matches!(o.name, PgConfigOptionName::TextColumns));
                    let stmt = Statement::CreateSource(CreateSourceStatement {
                        connection: conn,
                        name,
                        if_not_exists,
                        in_cluster,
                        include_metadata,
                        format,
                        envelope,
                        col_names,
                        with_options,
                        key_constraint,
                        external_references,
                        progress_subsource,
                    });
                    item.create_sql = stmt.to_ast_string_stable();
                    tx.update_item(item.id, item)?;
                    info!("migrate: converted postgres source {stmt} to remove subsource options");
                }
            }
            // MySQL sources are multi-output sources whose subsources are
            // migrated above. All we need to do is remove the subsource-related
            // options from this statement since they are no longer relevant.
            Statement::CreateSource(CreateSourceStatement {
                connection: mut conn @ CreateSourceConnection::MySql { .. },
                name,
                if_not_exists,
                in_cluster,
                include_metadata,
                format,
                envelope,
                col_names,
                with_options,
                key_constraint,
                external_references,
                progress_subsource,
                ..
            }) => {
                let options = match &mut conn {
                    CreateSourceConnection::MySql { options, .. } => options,
                    _ => unreachable!("match determined above"),
                };
                // These options storing text and exclude columns on the primary source statement
                // are redundant with the options on subsource statements so can just be removed.
                // They was kept for round-tripping of `CREATE SOURCE` statements that automatically
                // generated subsources, which is no longer necessary.
                if options.iter().any(|o| {
                    matches!(
                        o.name,
                        MySqlConfigOptionName::TextColumns | MySqlConfigOptionName::ExcludeColumns
                    )
                }) {
                    options.retain(|o| {
                        !matches!(
                            o.name,
                            MySqlConfigOptionName::TextColumns
                                | MySqlConfigOptionName::ExcludeColumns
                        )
                    });
                    let stmt = Statement::CreateSource(CreateSourceStatement {
                        connection: conn,
                        name,
                        if_not_exists,
                        in_cluster,
                        include_metadata,
                        format,
                        envelope,
                        col_names,
                        with_options,
                        key_constraint,
                        external_references,
                        progress_subsource,
                    });
                    item.create_sql = stmt.to_ast_string_stable();
                    tx.update_item(item.id, item)?;
                    info!("migrate: converted mysql source {stmt} to remove subsource options");
                }
            }
            // Multi-output load generator sources whose subsources are already
            // migrated above. There is no need to remove any options from this
            // statement since they are not export-specific.
            Statement::CreateSource(CreateSourceStatement {
                connection:
                    CreateSourceConnection::LoadGenerator {
                        generator:
                            LoadGenerator::Auction | LoadGenerator::Marketing | LoadGenerator::Tpch,
                        ..
                    },
                ..
            }) => {}
            // Single-output sources that need to be migrated to tables. These sources currently output
            // data to the primary collection of the source statement. We will create a new table
            // statement for them and move all export-specific options over from the source statement,
            // while moving the `CREATE SOURCE` statement to a new name and moving its shard to the
            // new table statement.
            Statement::CreateSource(CreateSourceStatement {
                connection:
                    conn @ (CreateSourceConnection::Kafka { .. }
                    | CreateSourceConnection::LoadGenerator {
                        generator:
                            LoadGenerator::Clock
                            | LoadGenerator::Datums
                            | LoadGenerator::Counter
                            | LoadGenerator::KeyValue,
                        ..
                    }),
                name,
                col_names,
                include_metadata,
                format,
                envelope,
                mut with_options,
                if_not_exists,
                in_cluster,
                progress_subsource,
                external_references,
                key_constraint,
            }) => {
                // To check if this is a source that has already been migrated we use a basic
                // heuristic: if there is at least one existing table for the source, and if
                // the envelope/format/include_metadata options are empty, we assume it's
                // already been migrated.
                let tables_for_source =
                    items_with_statements_copied
                        .iter()
                        .any(|(item, statement)| match statement {
                            Statement::CreateTableFromSource(stmt) => {
                                let source: GlobalId = match &stmt.source {
                                    RawItemName::Name(_) => {
                                        unreachable!("tables store source as ID")
                                    }
                                    RawItemName::Id(source_id, _, _) => {
                                        source_id.parse().expect("valid id")
                                    }
                                };
                                source == item.id
                            }
                            _ => false,
                        });
                if tables_for_source
                    && envelope.is_none()
                    && format.is_none()
                    && include_metadata.is_empty()
                {
                    info!("migrate: skipping already migrated source: {}", name);
                    return Ok(());
                }

                // Use the current source name as the new table name, and rename the source to
                // `<source_name>_source`. This is intended to allow users to continue using
                // queries that reference the source name, since they will now need to query the
                // table instead.
                let new_source_name_ident = Ident::new_unchecked(
                    name.0.last().expect("at least one ident").to_string() + "_source",
                );
                let mut new_source_name = name.clone();
                *new_source_name.0.last_mut().expect("at least one ident") = new_source_name_ident;

                // Also update the name of the source 'item'
                let mut table_item_name = item.name.clone() + "_source";
                std::mem::swap(&mut item.name, &mut table_item_name);

                // A reference to the source that will be included in the table statement
                let source_ref =
                    RawItemName::Id(item.id.to_string(), new_source_name.clone(), None);

                let columns = if col_names.is_empty() {
                    TableFromSourceColumns::NotSpecified
                } else {
                    TableFromSourceColumns::Named(col_names)
                };

                // All source tables must have a `details` option, which is a serialized proto
                // describing any source-specific details for this table statement.
                let details = match &conn {
                    // For kafka sources this proto is currently empty.
                    CreateSourceConnection::Kafka { .. } => SourceExportStatementDetails::Kafka {},
                    CreateSourceConnection::LoadGenerator { .. } => {
                        // Since these load generators are single-output we use the default output.
                        SourceExportStatementDetails::LoadGenerator {
                            output: LoadGeneratorOutput::Default,
                        }
                    }
                    _ => unreachable!("match determined above"),
                };
                let mut table_with_options = vec![TableFromSourceOption {
                    name: TableFromSourceOptionName::Details,
                    value: Some(WithOptionValue::Value(Value::String(hex::encode(
                        details.into_proto().encode_to_vec(),
                    )))),
                }];

                // Move over the IgnoreKeys option if it exists.
                let mut i = 0;
                while i < with_options.len() {
                    if with_options[i].name == CreateSourceOptionName::IgnoreKeys {
                        let option = with_options.remove(i);
                        table_with_options.push(TableFromSourceOption {
                            name: TableFromSourceOptionName::IgnoreKeys,
                            value: option.value,
                        });
                    } else {
                        i += 1;
                    }
                }
                // Move over the Timeline option if it exists.
                i = 0;
                while i < with_options.len() {
                    if with_options[i].name == CreateSourceOptionName::Timeline {
                        let option = with_options.remove(i);
                        table_with_options.push(TableFromSourceOption {
                            name: TableFromSourceOptionName::Timeline,
                            value: option.value,
                        });
                    } else {
                        i += 1;
                    }
                }

                // Generate the same external-reference that would have been generated
                // during purification for single-output sources.
                let external_reference = match &conn {
                    // For kafka sources this proto is currently empty.
                    CreateSourceConnection::Kafka { options, .. } => {
                        let topic_option = options
                            .iter()
                            .find(|o| matches!(o.name, KafkaSourceConfigOptionName::Topic))
                            .expect("kafka sources must have a topic");
                        let topic = match &topic_option.value {
                            Some(WithOptionValue::Value(Value::String(topic))) => topic,
                            _ => unreachable!("topic must be a string"),
                        };

                        Some(UnresolvedItemName::qualified(&[Ident::new(topic)?]))
                    }
                    CreateSourceConnection::LoadGenerator { generator, .. } => {
                        // Since these load generators are single-output the external reference
                        // uses the schema-name for both namespace and name.
                        let name = FullItemName {
                                database: mz_sql::names::RawDatabaseSpecifier::Name(
                                    mz_storage_types::sources::load_generator::LOAD_GENERATOR_DATABASE_NAME
                                        .to_owned(),
                                ),
                                schema: generator.schema_name().to_string(),
                                item: generator.schema_name().to_string(),
                            };
                        Some(UnresolvedItemName::from(name))
                    }
                    _ => unreachable!("match determined above"),
                };

                // The new table statement, stealing the name and the export-specific fields from
                // the create source statement.
                let table = CreateTableFromSourceStatement {
                    name,
                    constraints: vec![],
                    columns,
                    if_not_exists: false,
                    source: source_ref,
                    external_reference,
                    with_options: table_with_options,
                    envelope,
                    include_metadata,
                    format,
                };

                // The source statement with a new name and many of its fields emptied
                let source = CreateSourceStatement {
                    connection: conn,
                    name: new_source_name,
                    if_not_exists,
                    in_cluster,
                    include_metadata: vec![],
                    format: None,
                    envelope: None,
                    col_names: vec![],
                    with_options,
                    key_constraint,
                    external_references,
                    progress_subsource,
                };

                let source_id = item.id;
                let schema_id = item.schema_id.clone();
                let schema = tx.get_schema(&item.schema_id).expect("schema must exist");

                let owner_id = item.owner_id.clone();
                let privileges = item.privileges.clone();

                // Update the source statement in the catalog first, since the name will
                // otherwise conflict with the new table statement.
                info!("migrate: updated source {} to {source}", item.create_sql);
                item.create_sql = Statement::CreateSource(source).to_ast_string_stable();
                tx.update_item(item.id, item)?;

                // Insert the new table statement into the catalog with a new id.
                let ids = tx.allocate_user_item_ids(1)?;
                let new_table_id = ids[0];
                info!("migrate: added table {new_table_id}: {table}");
                tx.insert_user_item(
                    new_table_id,
                    schema_id,
                    &table_item_name,
                    table.to_ast_string_stable(),
                    owner_id,
                    privileges,
                    &Default::default(),
                )?;
                // We need to move the shard currently attached to the source statement to the
                // table statement such that the existing data in the shard is preserved and can
                // be queried on the new table statement. However, we need to keep the GlobalId of
                // the source the same, to preserve existing references to that statement in
                // external tools such as DBT and Terraform. We will insert a new shard for the source
                // statement which will be automatically created after the migration is complete.
                let new_source_shard = ShardId::new();
                let (source_id, existing_source_shard) = tx
                    .delete_collection_metadata(btreeset! {source_id})
                    .pop()
                    .expect("shard should exist");
                tx.insert_collection_metadata(btreemap! {
                    new_table_id => existing_source_shard,
                    source_id => new_source_shard
                })?;

                add_to_audit_log(
                    tx,
                    mz_audit_log::EventType::Create,
                    mz_audit_log::ObjectType::Table,
                    mz_audit_log::EventDetails::IdFullNameV1(mz_audit_log::IdFullNameV1 {
                        id: new_table_id.to_string(),
                        name: mz_audit_log::FullNameV1 {
                            database: schema
                                .database_id
                                .map(|d| d.to_string())
                                .unwrap_or_default(),
                            schema: schema.name,
                            item: table_item_name,
                        },
                    }),
                    now(),
                )?;

                // We also need to update any other statements that reference the source to use the new
                // table id/name instead.
                changed_ids.insert(source_id, new_table_id);
            }

            // When we upgrade to > rust 1.81 we should use #[expect(unreachable_patterns)]
            // to enforce that we have covered all CreateSourceStatement variants.
            #[allow(unreachable_patterns)]
            Statement::CreateSource(_) => {}
            _ => (),
        }
    }

    let mut updated_items = BTreeMap::new();
    for (mut item, mut statement) in items_with_statements_copied {
        match &statement {
            // Don’t rewrite any of the statements we just migrated.
            Statement::CreateSource(_) => {}
            Statement::CreateSubsource(_) => {}
            Statement::CreateTableFromSource(_) => {}
            // We need to rewrite any statements that reference a source id to use the new
            // table id instead, since any contained data in the source will now be in the table.
            // This assumes the table has stolen the source's name, which is the case
            // for all sources that were migrated.
            _ => {
                if mz_sql::names::modify_dependency_item_ids(&mut statement, &changed_ids) {
                    item.create_sql = statement.to_ast_string_stable();
                    updated_items.insert(item.id, item);
                }
            }
        }
    }
    if !updated_items.is_empty() {
        tx.update_items(updated_items)?;
    }

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

fn add_to_audit_log(
    tx: &mut Transaction,
    event_type: mz_audit_log::EventType,
    object_type: mz_audit_log::ObjectType,
    details: mz_audit_log::EventDetails,
    occurred_at: mz_ore::now::EpochMillis,
) -> Result<(), anyhow::Error> {
    let id = tx.get_and_increment_id(mz_catalog::durable::AUDIT_LOG_ID_ALLOC_KEY.to_string())?;
    let event =
        mz_audit_log::VersionedEvent::new(id, event_type, object_type, details, None, occurred_at);
    tx.insert_audit_log_event(event);
    Ok(())
}
