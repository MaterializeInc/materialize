// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use base64::prelude::*;
use maplit::btreeset;
use mz_catalog::builtin::BuiltinTable;
use mz_catalog::durable::{MOCK_AUTHENTICATION_NONCE_KEY, Transaction};
use mz_catalog::memory::objects::{BootstrapStateUpdateKind, StateUpdate};
use mz_ore::collections::CollectionExt;
use mz_ore::now::NowFn;
use mz_persist_types::ShardId;
use mz_proto::RustType;
use mz_repr::{CatalogItemId, Diff, Timestamp};
use mz_sql::ast::display::AstDisplay;
use mz_sql::ast::{
    CreateSinkOptionName, CreateViewStatement, CteBlock, DeferredItemName, Expr, IfExistsBehavior,
    Limit, Query, Select, SetExpr, SqlServerConfigOptionName, TableFactor, TableWithJoins,
    ViewDefinition,
};
use mz_sql::catalog::SessionCatalog;
use mz_sql::names::{FullItemName, QualifiedItemName};
use mz_sql::normalize;
use mz_sql_parser::ast::{Raw, Statement};
use mz_storage_client::controller::StorageTxn;
use mz_storage_types::sources::SourceExportStatementDetails;
use mz_storage_types::sources::load_generator::LoadGeneratorOutput;
use prost::Message;
use semver::Version;
use tracing::info;
use uuid::Uuid;

// DO NOT add any more imports from `crate` outside of `crate::catalog`.
use crate::catalog::open::into_consolidatable_updates_startup;
use crate::catalog::state::LocalExpressionCache;
use crate::catalog::{BuiltinTableUpdate, CatalogState, ConnCatalog};

fn rewrite_ast_items<F>(tx: &mut Transaction<'_>, mut f: F) -> Result<(), anyhow::Error>
where
    F: for<'a> FnMut(
        &'a mut Transaction<'_>,
        CatalogItemId,
        &'a mut Statement<Raw>,
    ) -> Result<(), anyhow::Error>,
{
    let mut updated_items = BTreeMap::new();

    for mut item in tx.get_items() {
        let mut stmt = mz_sql::parse::parse(&item.create_sql)?.into_element().ast;
        f(tx, item.id, &mut stmt)?;

        item.create_sql = stmt.to_ast_string_stable();

        updated_items.insert(item.id, item);
    }
    tx.update_items(updated_items)?;
    Ok(())
}

fn rewrite_items<F>(
    tx: &mut Transaction<'_>,
    cat: &ConnCatalog<'_>,
    mut f: F,
) -> Result<(), anyhow::Error>
where
    F: for<'a> FnMut(
        &'a mut Transaction<'_>,
        &'a &ConnCatalog<'_>,
        CatalogItemId,
        &'a mut Statement<Raw>,
    ) -> Result<(), anyhow::Error>,
{
    let mut updated_items = BTreeMap::new();
    let items = tx.get_items();
    for mut item in items {
        let mut stmt = mz_sql::parse::parse(&item.create_sql)?.into_element().ast;

        f(tx, &cat, item.id, &mut stmt)?;

        item.create_sql = stmt.to_ast_string_stable();

        updated_items.insert(item.id, item);
    }
    tx.update_items(updated_items)?;
    Ok(())
}

pub(crate) struct MigrateResult {
    pub(crate) builtin_table_updates: Vec<BuiltinTableUpdate<&'static BuiltinTable>>,
    pub(crate) post_item_updates: Vec<(BootstrapStateUpdateKind, Timestamp, Diff)>,
}

/// Migrates all user items and loads them into `state`.
///
/// Returns the builtin updates corresponding to all user items.
pub(crate) async fn migrate(
    state: &mut CatalogState,
    tx: &mut Transaction<'_>,
    local_expr_cache: &mut LocalExpressionCache,
    item_updates: Vec<StateUpdate>,
    _now: NowFn,
    _boot_ts: Timestamp,
) -> Result<MigrateResult, anyhow::Error> {
    let catalog_version = tx.get_catalog_content_version();
    let catalog_version = match catalog_version {
        Some(v) => Version::parse(v)?,
        None => Version::new(0, 0, 0),
    };

    info!(
        "migrating statements from catalog version {:?}",
        catalog_version
    );

    rewrite_ast_items(tx, |_tx, _id, stmt| {
        // Add per-item AST migrations below.
        //
        // Each migration should be a function that takes `stmt` (the AST
        // representing the creation SQL for the item) as input. Any
        // mutations to `stmt` will be staged for commit to the catalog.
        //
        // Migration functions may also take `tx` as input to stage
        // arbitrary changes to the catalog.
        ast_rewrite_create_sink_partition_strategy(stmt)?;
        Ok(())
    })?;

    // Load items into catalog. We make sure to consolidate the old updates with the new updates to
    // avoid trying to apply unmigrated items.
    let commit_ts = tx.upper();
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
    let mut ast_builtin_table_updates = state
        .apply_updates_for_bootstrap(item_updates, local_expr_cache)
        .await;

    info!("migrating from catalog version {:?}", catalog_version);

    let conn_cat = state.for_system_session();

    // Special block for `ast_rewrite_sources_to_tables` migration
    // since it requires a feature flag needs to update multiple AST items at once.
    if state.system_config().force_source_table_syntax() {
        rewrite_sources_to_tables(tx, &conn_cat)?;
    }

    rewrite_items(tx, &conn_cat, |_tx, _conn_cat, _id, _stmt| {
        let _catalog_version = catalog_version.clone();
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
    })?;

    // Add whole-catalog migrations below.
    //
    // Each migration should be a function that takes `tx` and `conn_cat` as
    // input and stages arbitrary transformations to the catalog on `tx`.

    let op_item_updates = tx.get_and_commit_op_updates();
    let item_builtin_table_updates = state
        .apply_updates_for_bootstrap(op_item_updates, local_expr_cache)
        .await;

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
fn rewrite_sources_to_tables(
    tx: &mut Transaction<'_>,
    catalog: &ConnCatalog<'_>,
) -> Result<(), anyhow::Error> {
    use mz_sql::ast::{
        CreateSourceConnection, CreateSourceStatement, CreateSubsourceOptionName,
        CreateSubsourceStatement, CreateTableFromSourceStatement, Ident,
        KafkaSourceConfigOptionName, LoadGenerator, MySqlConfigOptionName, PgConfigOptionName,
        RawItemName, TableFromSourceColumns, TableFromSourceOption, TableFromSourceOptionName,
        UnresolvedItemName, Value, WithOptionValue,
    };

    let mut updated_items = BTreeMap::new();

    let mut sources = vec![];
    let mut subsources = vec![];

    for item in tx.get_items() {
        let stmt = mz_sql::parse::parse(&item.create_sql)?.into_element().ast;
        match stmt {
            Statement::CreateSubsource(stmt) => subsources.push((item, stmt)),
            Statement::CreateSource(stmt) => sources.push((item, stmt)),
            _ => {}
        }
    }

    let mut source_mapping = BTreeMap::new();
    let mut pending_progress_statements = BTreeMap::new();
    // We first go over the sources, which depending on the kind determine what happens with the
    // progress statements.
    for (mut item, source_stmt) in sources {
        let CreateSourceStatement {
            name,
            in_cluster,
            col_names,
            mut connection,
            include_metadata,
            format,
            envelope,
            if_not_exists,
            key_constraint,
            with_options,
            external_references,
            progress_subsource,
        } = source_stmt;

        let (progress_name, progress_item) = match progress_subsource {
            Some(DeferredItemName::Named(RawItemName::Name(name))) => {
                let partial_name = normalize::unresolved_item_name(name.clone())?;
                (name, catalog.resolve_item(&partial_name)?)
            }
            Some(DeferredItemName::Named(RawItemName::Id(id, name, _))) => {
                let gid = id.parse()?;
                (name, catalog.get_item(&gid))
            }
            Some(DeferredItemName::Deferred(_)) => {
                unreachable!("invalid progress subsource")
            }
            None => {
                info!("migrate: skipping already migrated source: {name}");
                continue;
            }
        };
        let raw_progress_name =
            RawItemName::Id(progress_item.id().to_string(), progress_name.clone(), None);

        source_mapping.insert(item.id, raw_progress_name.clone());

        // We need to jump through some hoops to get to the raw item name of the source
        let catalog_item = catalog.get_item(&item.id);
        let item_name: &QualifiedItemName = catalog_item.name();
        let item_name: FullItemName = catalog.resolve_full_name(item_name);
        let item_name: UnresolvedItemName = normalize::unresolve(item_name);

        // First, trip the connection options that we no longer need
        match &mut connection {
            CreateSourceConnection::Postgres { options, .. } => {
                options.retain(|o| match o.name {
                    PgConfigOptionName::Details | PgConfigOptionName::Publication => true,
                    PgConfigOptionName::TextColumns => false,
                });
            }
            CreateSourceConnection::SqlServer { options, .. } => {
                options.retain(|o| match o.name {
                    SqlServerConfigOptionName::Details => true,
                    SqlServerConfigOptionName::TextColumns
                    | SqlServerConfigOptionName::ExcludeColumns => false,
                });
            }
            CreateSourceConnection::MySql { options, .. } => {
                options.retain(|o| match o.name {
                    MySqlConfigOptionName::Details => true,
                    MySqlConfigOptionName::TextColumns | MySqlConfigOptionName::ExcludeColumns => {
                        false
                    }
                });
            }
            CreateSourceConnection::Kafka { .. } | CreateSourceConnection::LoadGenerator { .. } => {
            }
        }

        // Then, figure out the new statements for the progress and source.
        let (progress_stmt_replacement, source_stmt_replacement) = match connection {
            connection @ (CreateSourceConnection::Postgres { .. }
            | CreateSourceConnection::MySql { .. }
            | CreateSourceConnection::SqlServer { .. }
            | CreateSourceConnection::LoadGenerator {
                generator: LoadGenerator::Tpch | LoadGenerator::Auction | LoadGenerator::Marketing,
                ..
            }) => {
                // Assert the expected state of the source
                assert_eq!(col_names, &[]);
                assert_eq!(include_metadata, &[]);
                assert_eq!(format, None);
                assert_eq!(envelope, None);
                assert_eq!(key_constraint, None);
                assert_eq!(external_references, None);

                // This is a dummy replacement statement for the source object of multi-output
                // sources. It is describing the query `SELECT FROM foo_progress LIMIT 0` which has
                // the same schema and data of the dummy output we normally create. The degenerate
                // dependency on foo_progress is there so that a `DROP SOURCE foo_progress CASCADE`
                // also drops this dummy object.
                let dummy_source_stmt = Statement::CreateView(CreateViewStatement {
                    if_exists: IfExistsBehavior::Error,
                    temporary: false,
                    definition: ViewDefinition {
                        name: item_name.clone(),
                        columns: vec![],
                        query: Query {
                            ctes: CteBlock::Simple(vec![]),
                            body: SetExpr::Select(Box::new(Select {
                                distinct: None,
                                projection: vec![],
                                from: vec![TableWithJoins {
                                    relation: TableFactor::Table {
                                        name: raw_progress_name.clone(),
                                        alias: None,
                                    },
                                    joins: vec![],
                                }],
                                selection: None,
                                group_by: vec![],
                                having: None,
                                qualify: None,
                                options: vec![],
                            })),
                            order_by: vec![],
                            limit: Some(Limit {
                                with_ties: false,
                                quantity: Expr::Value(Value::Number("0".to_owned())),
                            }),
                            offset: None,
                        },
                    },
                });

                let progress_stmt_replacement = CreateSourceStatement {
                    name: progress_name,
                    in_cluster,
                    col_names: vec![],
                    connection,
                    include_metadata: vec![],
                    format: None,
                    envelope: None,
                    if_not_exists,
                    key_constraint: None,
                    with_options,
                    external_references: None,
                    progress_subsource: None,
                };
                (progress_stmt_replacement, dummy_source_stmt)
            }
            CreateSourceConnection::Kafka {
                options,
                connection,
            } => {
                let constraints = if let Some(_key_constraint) = key_constraint {
                    // Should we ignore not enforced primary key constraints here?
                    vec![]
                } else {
                    vec![]
                };

                let columns = if col_names.is_empty() {
                    TableFromSourceColumns::NotSpecified
                } else {
                    TableFromSourceColumns::Named(col_names)
                };

                // All source tables must have a `details` option, which is a serialized proto
                // describing any source-specific details for this table statement.
                let details = SourceExportStatementDetails::Kafka {};
                let table_with_options = vec![TableFromSourceOption {
                    name: TableFromSourceOptionName::Details,
                    value: Some(WithOptionValue::Value(Value::String(hex::encode(
                        details.into_proto().encode_to_vec(),
                    )))),
                }];
                // The external reference for a kafka source is the just the topic name
                let topic_option = options
                    .iter()
                    .find(|o| matches!(o.name, KafkaSourceConfigOptionName::Topic))
                    .expect("kafka sources must have a topic");
                let topic = match &topic_option.value {
                    Some(WithOptionValue::Value(Value::String(topic))) => topic,
                    _ => unreachable!("topic must be a string"),
                };
                let external_reference = UnresolvedItemName::qualified(&[Ident::new(topic)?]);

                let source_stmt_replacement =
                    Statement::CreateTableFromSource(CreateTableFromSourceStatement {
                        name: item_name,
                        constraints,
                        columns,
                        if_not_exists,
                        source: raw_progress_name,
                        include_metadata,
                        format,
                        envelope,
                        external_reference: Some(external_reference.into()),
                        with_options: table_with_options,
                    });

                let progress_stmt_replacement = CreateSourceStatement {
                    name: progress_name,
                    in_cluster,
                    col_names: vec![],
                    connection: CreateSourceConnection::Kafka {
                        options,
                        connection,
                    },
                    include_metadata: vec![],
                    format: None,
                    envelope: None,
                    if_not_exists,
                    key_constraint: None,
                    with_options,
                    external_references: None,
                    progress_subsource: None,
                };
                (progress_stmt_replacement, source_stmt_replacement)
            }
            CreateSourceConnection::LoadGenerator {
                generator:
                    generator @ (LoadGenerator::Clock
                    | LoadGenerator::Counter
                    | LoadGenerator::Datums
                    | LoadGenerator::KeyValue),
                options,
            } => {
                let constraints = if let Some(_key_constraint) = key_constraint {
                    // Should we ignore not enforced primary key constraints here?
                    vec![]
                } else {
                    vec![]
                };

                let columns = if col_names.is_empty() {
                    TableFromSourceColumns::NotSpecified
                } else {
                    TableFromSourceColumns::Named(col_names)
                };

                // All source tables must have a `details` option, which is a serialized proto
                // describing any source-specific details for this table statement.
                let details = SourceExportStatementDetails::LoadGenerator {
                    output: LoadGeneratorOutput::Default,
                };
                let table_with_options = vec![TableFromSourceOption {
                    name: TableFromSourceOptionName::Details,
                    value: Some(WithOptionValue::Value(Value::String(hex::encode(
                        details.into_proto().encode_to_vec(),
                    )))),
                }];
                // Since these load generators are single-output the external reference
                // uses the schema-name for both namespace and name.
                let external_reference = FullItemName {
                    database: mz_sql::names::RawDatabaseSpecifier::Name(
                        mz_storage_types::sources::load_generator::LOAD_GENERATOR_DATABASE_NAME
                            .to_owned(),
                    ),
                    schema: generator.schema_name().to_string(),
                    item: generator.schema_name().to_string(),
                };

                let source_stmt_replacement =
                    Statement::CreateTableFromSource(CreateTableFromSourceStatement {
                        name: item_name,
                        constraints,
                        columns,
                        if_not_exists,
                        source: raw_progress_name,
                        include_metadata,
                        format,
                        envelope,
                        external_reference: Some(external_reference.into()),
                        with_options: table_with_options,
                    });

                let progress_stmt_replacement = CreateSourceStatement {
                    name: progress_name,
                    in_cluster,
                    col_names: vec![],
                    connection: CreateSourceConnection::LoadGenerator { generator, options },
                    include_metadata: vec![],
                    format: None,
                    envelope: None,
                    if_not_exists,
                    key_constraint: None,
                    with_options,
                    external_references: None,
                    progress_subsource: None,
                };
                (progress_stmt_replacement, source_stmt_replacement)
            }
        };

        // The source can be updated right away but the replacement progress statement will
        // be installed in the next loop where we go over subsources.

        item.create_sql = source_stmt_replacement.to_ast_string_stable();
        updated_items.insert(item.id, item);
        pending_progress_statements.insert(progress_item.id(), progress_stmt_replacement);
    }

    for (mut item, stmt) in subsources {
        match stmt {
            // Migrate progress statements to the corresponding statement produced from the
            // previous step.
            CreateSubsourceStatement {
                of_source: None, ..
            } => {
                let new_stmt = pending_progress_statements.remove(&item.id).unwrap();
                item.create_sql = new_stmt.to_ast_string_stable();
                updated_items.insert(item.id, item);
            }
            // Migrate each `CREATE SUBSOURCE` statement to an equivalent
            // `CREATE TABLE ... FROM SOURCE` statement.
            CreateSubsourceStatement {
                name,
                columns,
                constraints,
                of_source: Some(raw_source_name),
                if_not_exists,
                mut with_options,
            } => {
                let old_source_id = match raw_source_name {
                    // Some legacy subsources have named-only references to their `of_source`
                    // so we ensure we always use an ID-based reference in the stored
                    // `CREATE TABLE ... FROM SOURCE` statements.
                    RawItemName::Name(name) => {
                        let partial_name = normalize::unresolved_item_name(name.clone())?;
                        catalog.resolve_item(&partial_name)?.id()
                    }
                    RawItemName::Id(id, _, _) => id.parse()?,
                };
                let source = source_mapping.get(&old_source_id).unwrap().clone();

                // The external reference is a `with_option` on subsource statements but is a
                // separate field on table statements.
                let external_reference = match with_options
                    .iter()
                    .position(|opt| opt.name == CreateSubsourceOptionName::ExternalReference)
                {
                    Some(i) => match with_options.remove(i).value {
                        Some(WithOptionValue::UnresolvedItemName(name)) => name,
                        _ => unreachable!("external reference must be an unresolved item name"),
                    },
                    None => panic!("subsource must have an external reference"),
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
                            CreateSubsourceOptionName::RetainHistory => TableFromSourceOption {
                                name: TableFromSourceOptionName::RetainHistory,
                                value: option.value,
                            },
                            CreateSubsourceOptionName::Progress => {
                                panic!("progress option should not exist on this subsource")
                            }
                            CreateSubsourceOptionName::ExternalReference => {
                                unreachable!("This option is handled separately above.")
                            }
                        }
                    })
                    .collect::<Vec<_>>();

                let table = CreateTableFromSourceStatement {
                    name,
                    constraints,
                    columns: TableFromSourceColumns::Defined(columns),
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
                updated_items.insert(item.id, item);
            }
        }
    }

    tx.update_items(updated_items)?;

    Ok(())
}

// Durable migrations

/// Migrations that run only on the durable catalog before any data is loaded into memory.
pub(crate) fn durable_migrate(
    tx: &mut Transaction,
    _organization_id: Uuid,
    _boot_ts: Timestamp,
) -> Result<(), anyhow::Error> {
    // Migrate the expression cache to a new shard. We're updating the keys to use the explicit
    // binary version instead of the deploy generation.
    const EXPR_CACHE_MIGRATION_KEY: &str = "expr_cache_migration";
    const EXPR_CACHE_MIGRATION_DONE: u64 = 1;
    if tx.get_config(EXPR_CACHE_MIGRATION_KEY.to_string()) != Some(EXPR_CACHE_MIGRATION_DONE) {
        if let Some(shard_id) = tx.get_expression_cache_shard() {
            tx.mark_shards_as_finalized(btreeset! {shard_id});
            tx.set_expression_cache_shard(ShardId::new())?;
        }
        tx.set_config(
            EXPR_CACHE_MIGRATION_KEY.to_string(),
            Some(EXPR_CACHE_MIGRATION_DONE),
        )?;
    }

    // Migrate the builtin migration shard to a new shard. We're updating the keys to use the explicit
    // binary version instead of the deploy generation.
    const BUILTIN_MIGRATION_SHARD_MIGRATION_KEY: &str = "migration_shard_migration";
    const BUILTIN_MIGRATION_SHARD_MIGRATION_DONE: u64 = 1;
    if tx.get_config(BUILTIN_MIGRATION_SHARD_MIGRATION_KEY.to_string())
        != Some(BUILTIN_MIGRATION_SHARD_MIGRATION_DONE)
    {
        if let Some(shard_id) = tx.get_builtin_migration_shard() {
            tx.mark_shards_as_finalized(btreeset! {shard_id});
            tx.set_builtin_migration_shard(ShardId::new())?;
        }
        tx.set_config(
            BUILTIN_MIGRATION_SHARD_MIGRATION_KEY.to_string(),
            Some(BUILTIN_MIGRATION_SHARD_MIGRATION_DONE),
        )?;
    }

    if tx
        .get_setting(MOCK_AUTHENTICATION_NONCE_KEY.to_string())
        .is_none()
    {
        let mut nonce = [0u8; 24];
        let _ = openssl::rand::rand_bytes(&mut nonce).expect("failed to generate nonce");
        let nonce = BASE64_STANDARD.encode(nonce);
        tx.set_setting(MOCK_AUTHENTICATION_NONCE_KEY.to_string(), Some(nonce))?;
    }

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

// Remove PARTITION STRATEGY from CREATE SINK statements.
fn ast_rewrite_create_sink_partition_strategy(
    stmt: &mut Statement<Raw>,
) -> Result<(), anyhow::Error> {
    let Statement::CreateSink(stmt) = stmt else {
        return Ok(());
    };
    stmt.with_options
        .retain(|op| op.name != CreateSinkOptionName::PartitionStrategy);
    Ok(())
}
