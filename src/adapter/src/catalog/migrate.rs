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
    CreateSinkOptionName, CreateViewStatement, CteBlock, DeferredItemName, IfExistsBehavior, Query,
    SetExpr, SqlServerConfigOptionName, ViewDefinition,
};
use mz_sql::catalog::SessionCatalog;
use mz_sql::names::{FullItemName, QualifiedItemName};
use mz_sql::normalize;
use mz_sql::session::vars::{FORCE_SOURCE_TABLE_SYNTAX, Var, VarInput};
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

    let force_source_table_syntax = state.system_config().force_source_table_syntax();
    // When this flag is set the legacy syntax is denied. But here we are about to perform a
    // migration which requires that we parse the current catalog state. To proceed we temporarily disable
    // the flag and then reset it after migrations are done.
    if force_source_table_syntax {
        state
            .system_config_mut()
            .set(FORCE_SOURCE_TABLE_SYNTAX.name(), VarInput::Flat("off"))
            .expect("known parameter");
    }

    let mut ast_builtin_table_updates = state
        .apply_updates_for_bootstrap(item_updates, local_expr_cache)
        .await;

    info!("migrating from catalog version {:?}", catalog_version);

    let conn_cat = state.for_system_session();

    // Special block for `ast_rewrite_sources_to_tables` migration
    // since it requires a feature flag needs to update multiple AST items at once.
    if force_source_table_syntax {
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

    if force_source_table_syntax {
        state
            .system_config_mut()
            .set(FORCE_SOURCE_TABLE_SYNTAX.name(), VarInput::Flat("on"))
            .expect("known parameter");
    }

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
/// Suppose we have an old-style source named `source_name` with global id `source_id`. The source
/// will also have an associated progress source named `progress_name` (which is almost always
/// `source_name` + "_progress") with global id `progress_id`.
///
/// We have two constraints to satisfy. The migration:
///   1. should not change the schema of a global id *if that global id maps to a
///      durable collection*. The reason for this constraint is that when a durable collection (i.e
///      backed by a persist shard) is opened persist will verify that the schema is the expected
///      one. If we change the Create SQL of a global id to a non-durable definition (e.g a view)
///      then we are free to also change the schema.
///   2. should make it such that the SQL object that is constructed with a new-style `CREATE
///      SOURCE` statement contains the progress data and all other objects related to the
///      old-style source depend on that object.
///
/// With these constraints we consider two cases.
///
/// ## Case 1: A multi-output source
///
/// Multi-output sources have a dummy output as the contents of `source_name` that is useless. So
/// we re-purpose that name to be the `CREATE SOURCE` statement and make `progress_name` be a view
/// of `source_name`. Since the main source is a durable object we must move `source_name` and the
/// corresponding new-style `CREATE SOURCE` statement under `progress_id`. Then `progress_name` can
/// move to `source_id` and since it becomes a view we are free to change its schema.
///
/// Visually, we are changing this mapping:
///
/// |  Global ID  |  SQL Name     | Create SQL                 | Schema   | Durable |
/// +-------------+---------------+----------------------------+----------+---------|
/// | source_id   | source_name   | CREATE SOURCE (old-style) | empty     | yes     |
/// | progress_id | progress_name | CREATE SUBSOURCE .."      | progress  | yes     |
///
/// to this mapping:
///
/// |  Global ID  |  SQL Name     | Create SQL                | Schema        | Durable |
/// +-------------+---------------+-------------------------------------------+---------+
/// | source_id   | progress_name | CREATE VIEW               | progress data | no      |
/// | progress_id | source_name   | CREATE SOURCE (new-style) | progress data | yes     |
///
/// ## Case 2: A single-output source
///
/// Single-output sources have data as the contents of `source_name` and so we can't repurpose that
/// name to be the `CREATE SOURCE` statement. Here we leave everything intact except for the
/// Create SQL of each object. Namely, the old-style `CREATE SOURCE` statement becomes a `CREATE
/// TABLE FROM SOURCE` and the old-style `CREATE SUBSOURCE .. PROGRESS` becomes a new-style `CREATE
/// SOURCE` statement.
///
/// Visually, we are changing this mapping:
///
/// |  Global ID  |  SQL Name     | Create SQL                 | Schema     | Durable |
/// +-------------+---------------+----------------------------+------------+---------|
/// | source_id   | source_name   | CREATE SOURCE (old-style) | source data | yes     |
/// | progress_id | progress_name | CREATE SUBSOURCE .."      | progress    | yes     |
///
/// to this mapping:
///
/// |  Global ID  |  SQL Name     | Create SQL                 | Schema     | Durable |
/// +-------------+---------------+----------------------------+------------+---------|
/// | source_id   | source_name   | CREATE TABLE FROM SOURCE  | source data | yes     |
/// | progress_id | progress_name | CREATE SOURCE (new-style) | progress    | yes     |
///
/// ## Subsource migration
///
/// After the migration goes over all the `CREATE SOURCE` statements it then transforms each
/// non-progress `CREATE SUBSOURCE` statement to be a `CREATE TABLE FROM SOURCE` statement that
/// points to the original `source_name` but with the altered global id (which is now
/// `progress_id`).
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

    let mut pending_progress_items = BTreeMap::new();
    let mut migrated_source_ids = BTreeMap::new();
    // We first go over the sources, which depending on the kind determine what happens with the
    // progress statements.
    for (mut source_item, source_stmt) in sources {
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

        // We need to jump through some hoops to get to the raw item name of the source
        let catalog_item = catalog.get_item(&source_item.id);
        let source_name: &QualifiedItemName = catalog_item.name();
        let full_source_name: FullItemName = catalog.resolve_full_name(source_name);
        let source_name: UnresolvedItemName = normalize::unresolve(full_source_name.clone());

        // First, strip the connection options that we no longer need
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
        let (new_progress_name, new_progress_stmt, new_source_name, new_source_stmt) =
            match connection {
                connection @ (CreateSourceConnection::Postgres { .. }
                | CreateSourceConnection::MySql { .. }
                | CreateSourceConnection::SqlServer { .. }
                | CreateSourceConnection::LoadGenerator {
                    generator:
                        LoadGenerator::Tpch | LoadGenerator::Auction | LoadGenerator::Marketing,
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
                    // sources. It is describing the query `TABLE source_name`. This ensures that
                    // whoever was used to run select queries against the `source_name` + "_progress"
                    // object still gets the same data after the migration. This switch does
                    // changes the schema of the object with `source_item.id` but because we're turning
                    // it into a view, which is not durable, it's ok. We'll never open a persist shard
                    // for this global id anymore.
                    let dummy_source_stmt = Statement::CreateView(CreateViewStatement {
                        if_exists: IfExistsBehavior::Error,
                        temporary: false,
                        definition: ViewDefinition {
                            name: progress_name,
                            columns: vec![],
                            query: Query {
                                ctes: CteBlock::Simple(vec![]),
                                body: SetExpr::Table(RawItemName::Id(
                                    progress_item.id().to_string(),
                                    source_name.clone(),
                                    None,
                                )),
                                order_by: vec![],
                                limit: None,
                                offset: None,
                            },
                        },
                    });

                    let new_progress_stmt = CreateSourceStatement {
                        name: source_name.clone(),
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

                    migrated_source_ids.insert(source_item.id, progress_item.id());

                    (
                        full_source_name.item,
                        new_progress_stmt,
                        progress_item.name().item.clone(),
                        dummy_source_stmt,
                    )
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

                    let new_source_stmt =
                        Statement::CreateTableFromSource(CreateTableFromSourceStatement {
                            name: source_name,
                            constraints,
                            columns,
                            if_not_exists,
                            source: raw_progress_name,
                            include_metadata,
                            format,
                            envelope,
                            external_reference: Some(external_reference),
                            with_options: table_with_options,
                        });

                    let new_progress_stmt = CreateSourceStatement {
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
                    (
                        progress_item.name().item.clone(),
                        new_progress_stmt,
                        full_source_name.item,
                        new_source_stmt,
                    )
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

                    let new_source_stmt =
                        Statement::CreateTableFromSource(CreateTableFromSourceStatement {
                            name: source_name,
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

                    let new_progress_stmt = CreateSourceStatement {
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
                    (
                        progress_item.name().item.clone(),
                        new_progress_stmt,
                        full_source_name.item,
                        new_source_stmt,
                    )
                }
            };

        // The source can be updated right away but the replacement progress statement will
        // be installed in the next loop where we go over subsources.

        info!(
            "migrate: converted source {} to {}",
            source_item.create_sql, new_source_stmt
        );
        source_item.name = new_source_name.clone();
        source_item.create_sql = new_source_stmt.to_ast_string_stable();
        updated_items.insert(source_item.id, source_item);
        pending_progress_items.insert(progress_item.id(), (new_progress_name, new_progress_stmt));
    }

    for (mut item, stmt) in subsources {
        match stmt {
            // Migrate progress statements to the corresponding statement produced from the
            // previous step.
            CreateSubsourceStatement {
                of_source: None, ..
            } => {
                let Some((new_name, new_stmt)) = pending_progress_items.remove(&item.id) else {
                    panic!("encountered orphan progress subsource id: {}", item.id)
                };
                item.name = new_name;
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
                let new_raw_source_name = match raw_source_name {
                    RawItemName::Id(old_id, name, None) => {
                        let old_id: CatalogItemId = old_id.parse().expect("well formed");
                        let new_id = migrated_source_ids[&old_id].clone();
                        RawItemName::Id(new_id.to_string(), name, None)
                    }
                    _ => unreachable!("unexpected source name: {raw_source_name}"),
                };
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
                    source: new_raw_source_name,
                    external_reference: Some(external_reference),
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
    assert!(
        pending_progress_items.is_empty(),
        "unexpected residual progress items: {pending_progress_items:?}"
    );

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
