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
use mz_catalog::builtin::{BuiltinType, BUILTINS};
use mz_catalog::durable::Transaction;
use mz_ore::collections::CollectionExt;
use mz_ore::now::{EpochMillis, NowFn};
use mz_repr::namespaces::PG_CATALOG_SCHEMA;
use mz_sql::ast::display::AstDisplay;
use mz_sql::catalog::NameReference;
use mz_sql_parser::ast::{visit_mut, Ident, Raw, RawDataType, Statement};
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::connections::ConnectionContext;
use mz_storage_types::sources::GenericSourceConnection;
use once_cell::sync::Lazy;
use semver::Version;
use tracing::info;

// DO NOT add any more imports from `crate` outside of `crate::catalog`.
use crate::catalog::{Catalog, CatalogState, ConnCatalog};

async fn rewrite_items<F>(
    tx: &mut Transaction<'_>,
    cat: &ConnCatalog<'_>,
    mut f: F,
) -> Result<(), anyhow::Error>
where
    F: for<'a> FnMut(
        &'a mut Transaction<'_>,
        &'a &ConnCatalog<'_>,
        &'a mut Statement<Raw>,
    ) -> BoxFuture<'a, Result<(), anyhow::Error>>,
{
    let mut updated_items = BTreeMap::new();
    let items = tx.loaded_items();
    for mut item in items {
        let mut stmt = mz_sql::parse::parse(&item.create_sql)?.into_element().ast;

        f(tx, &cat, &mut stmt).await?;

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
    connection_context: &ConnectionContext,
) -> Result<(), anyhow::Error> {
    let catalog_version = tx.get_catalog_content_version();
    let catalog_version = match catalog_version {
        Some(v) => Version::parse(&v)?,
        None => Version::new(0, 0, 0),
    };

    info!("migrating from catalog version {:?}", catalog_version);

    // Load up a temporary catalog.
    let state = Catalog::load_catalog_items(tx, state)?;

    // Perform per-item AST migrations.
    let conn_cat = state.for_system_session();
    rewrite_items(tx, &conn_cat, |_tx, conn_cat, stmt| {
        let catalog_version = catalog_version.clone();
        Box::pin(async move {
            // Add per-item AST migrations below.
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
            if catalog_version <= Version::new(0, 79, u64::MAX) {
                ast_rewrite_create_sink_into_kafka_options_0_80_0(stmt)?;
            }
            ast_rewrite_rewrite_type_schemas_0_81_0(stmt);
            ast_rewrite_linked_cluster_sizes(stmt, &conn_cat.state);

            Ok(())
        })
    })
    .await?;

    // Add whole-catalog migrations below.
    //
    // Each migration should be a function that takes `tx` and `conn_cat` as
    // input and stages arbitrary transformations to the catalog on `tx`.

    // This function blocks for at most 1 second per PG source.
    ast_rewrite_postgres_source_timeline_id_0_80_0(tx, &conn_cat, connection_context.clone())
        .await?;

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

/// Attempt to connect to the upstream PostgreSQL server to get the
/// publication's timeline ID.
async fn ast_rewrite_postgres_source_timeline_id_0_80_0(
    txn: &mut Transaction<'_>,
    conn_catalog: &ConnCatalog<'_>,
    connection_context: ConnectionContext,
) -> Result<(), anyhow::Error> {
    use mz_postgres_util::PostgresError;
    use mz_proto::RustType;
    use mz_sql::catalog::SessionCatalog;
    use mz_sql_parser::ast::{PgConfigOption, PgConfigOptionName, Value, WithOptionValue};
    use mz_storage_types::connections::inline::IntoInlineConnection;
    use mz_storage_types::connections::PostgresConnection;
    use mz_storage_types::sources::postgres::{
        PostgresSourcePublicationDetails, ProtoPostgresSourcePublicationDetails,
    };
    use prost::Message;

    // This cannot be bootstrapped with the correct LD-controlled parameters
    // until AFTER migrations have run and we have bootstrapped the entire coordinator.
    //
    // This means we are forced to use the default pg timeouts and other configurations,
    // which is fine as we are already making connections best-effort.
    let storage_configuration = StorageConfiguration::new(connection_context);

    let mut updated_items = BTreeMap::new();
    for mut item in txn.loaded_items() {
        let catalog_item = conn_catalog.get_item(&item.id);
        if let Ok(Some(source)) = catalog_item.source_desc() {
            let pg_conn = match &source.connection {
                GenericSourceConnection::Postgres(pg_conn) => {
                    pg_conn.clone().into_inline_connection(&conn_catalog)
                }
                _ => continue,
            };

            let mut pg_source_create_stmt =
                match mz_sql::parse::parse(&item.create_sql)?.into_element().ast {
                    mz_sql::ast::Statement::CreateSource(c) => c,
                    _ => unreachable!("PG sources are created w/ CreateSource"),
                };

            let source_options = match &mut pg_source_create_stmt.connection {
                mz_sql::ast::CreateSourceConnection::Postgres {
                    connection: _,
                    options,
                } => options,
                _ => unreachable!("pg source must be pg conn"),
            };

            let details_pos = source_options
                .iter()
                .position(|o| o.name == PgConfigOptionName::Details)
                .expect("pg source must have options");

            let details = source_options.swap_remove(details_pos);
            let details = match details.value.expect("details contains value") {
                WithOptionValue::Value(Value::String(details)) => details,
                _ => unreachable!("details encoded as string value"),
            };

            let details =
                hex::decode(details).expect("PostgresSourcePublicationDetails must be decodable");
            let mut details: ProtoPostgresSourcePublicationDetails =
                ProtoPostgresSourcePublicationDetails::decode(&*details)
                    .expect("corrupted PostgresSourcePublicationDetails");

            if details.timeline_id.is_some() {
                continue;
            }

            async fn get_timeline(
                storage_configuration: &StorageConfiguration,
                connection: PostgresConnection,
            ) -> Result<u64, PostgresError> {
                let config = connection
                    .config(
                        &*storage_configuration.connection_context.secrets_reader,
                        storage_configuration,
                    )
                    .await?;

                let replication_client = config
                    .connect_replication(
                        &storage_configuration.connection_context.ssh_tunnel_manager,
                    )
                    .await?;

                mz_postgres_util::get_timeline_id(&replication_client).await
            }

            let result: Result<Result<u64, PostgresError>, tokio::time::error::Elapsed> =
                tokio::time::timeout(
                    std::time::Duration::from_secs(1),
                    get_timeline(&storage_configuration, pg_conn.connection),
                )
                .await;

            let timeline_id = match result {
                Ok(Ok(timeline_id)) => timeline_id,
                _ => {
                    tracing::info!("could not determine timeline ID for PG source {}", item.id);
                    continue;
                }
            };

            details.timeline_id = Some(timeline_id);

            let publication_details = PostgresSourcePublicationDetails::from_proto(details)
                .expect("proto to rust conversion succeeds for PostgresSourcePublicationDetails");

            source_options.push(PgConfigOption {
                name: PgConfigOptionName::Details,
                value: Some(WithOptionValue::Value(Value::String(hex::encode(
                    publication_details.into_proto().encode_to_vec(),
                )))),
            });

            item.create_sql = pg_source_create_stmt.to_ast_string_stable();
            updated_items.insert(item.id, item);
        }
    }
    txn.update_items(updated_items)?;
    Ok(())
}

fn ast_rewrite_create_sink_into_kafka_options_0_80_0(
    stmt: &mut Statement<Raw>,
) -> Result<(), anyhow::Error> {
    use mz_sql::ast::visit_mut::VisitMut;
    use mz_sql::ast::{
        CreateSinkConnection, CreateSinkStatement, KafkaSinkConfigOption,
        KafkaSinkConfigOptionName, Value, WithOptionValue,
    };

    struct Rewriter;

    impl<'ast> VisitMut<'ast, Raw> for Rewriter {
        fn visit_create_sink_statement_mut(&mut self, node: &'ast mut CreateSinkStatement<Raw>) {
            match &mut node.connection {
                CreateSinkConnection::Kafka { options, .. } => {
                    options.push(KafkaSinkConfigOption {
                        name: KafkaSinkConfigOptionName::LegacyIds,
                        value: Some(WithOptionValue::Value(Value::Boolean(true))),
                    });
                }
            }
        }
    }

    Rewriter.visit_statement_mut(stmt);

    Ok(())
}

/// Rewrite all non-`pg_catalog` system types to have the correct schema.
fn ast_rewrite_rewrite_type_schemas_0_81_0(stmt: &mut Statement<Raw>) {
    use mz_sql::ast::visit_mut::VisitMut;

    static NON_PG_CATALOG_TYPES: Lazy<BTreeMap<&'static str, &'static BuiltinType<NameReference>>> =
        Lazy::new(|| {
            BUILTINS::types()
                .filter(|typ| typ.schema != PG_CATALOG_SCHEMA)
                .map(|typ| (typ.name, typ))
                .collect()
        });

    struct Rewriter;

    impl<'ast> VisitMut<'ast, Raw> for Rewriter {
        fn visit_data_type_mut(&mut self, node: &'ast mut RawDataType) {
            match node {
                RawDataType::Array(_) => {}
                RawDataType::List(_) => {}
                RawDataType::Map { .. } => {}
                RawDataType::Other { name, .. } => {
                    let name = name.name_mut();
                    let name = &mut name.0;
                    let len = name.len();
                    if len >= 2 {
                        let item_name = &name[len - 1];
                        let schema_name = &name[len - 2];

                        if schema_name.as_str() == PG_CATALOG_SCHEMA {
                            if let Some(typ) = NON_PG_CATALOG_TYPES.get(item_name.as_str()) {
                                name[len - 2] = Ident::new_unchecked(typ.schema);
                            }
                        }
                    }
                }
            }
            visit_mut::visit_data_type_mut(self, node);
        }
    }

    Rewriter.visit_statement_mut(stmt);
}

/// Rewrite all non-`pg_catalog` system types to have the correct schema.
fn ast_rewrite_linked_cluster_sizes<'a>(
    stmt: &'a mut Statement<Raw>,
    state: &'a std::borrow::Cow<'a, CatalogState>,
) {
    use mz_sql::ast::visit_mut::VisitMut;

    struct Rewriter<'a> {
        state: &'a std::borrow::Cow<'a, CatalogState>,
    }

    impl<'ast> VisitMut<'ast, Raw> for Rewriter<'_> {
        fn visit_create_source_statement_mut(
            &mut self,
            node: &'ast mut mz_sql_parser::ast::CreateSourceStatement<Raw>,
        ) {
            // If no specified cluster and no specified size, then this is an
            // "old"-style source that used a linked cluster of the default
            // size.
            if node.in_cluster.is_none()
                && !node
                    .with_options
                    .iter()
                    .any(|o| o.name == mz_sql_parser::ast::CreateSourceOptionName::Size)
            {
                node.with_options
                    .push(mz_sql_parser::ast::CreateSourceOption {
                        name: mz_sql_parser::ast::CreateSourceOptionName::Size,
                        value: Some(mz_sql_parser::ast::WithOptionValue::Value(
                            mz_sql_parser::ast::Value::String(
                                self.state.default_linked_cluster_size(),
                            ),
                        )),
                    })
            }
            visit_mut::visit_create_source_statement_mut(self, node);
        }
        fn visit_create_sink_statement_mut(
            &mut self,
            node: &'ast mut mz_sql_parser::ast::CreateSinkStatement<Raw>,
        ) {
            // If no specified cluster and no specified size, then this is an
            // "old"-style sink that used a linked cluster of the default size.
            if node.in_cluster.is_none()
                && !node
                    .with_options
                    .iter()
                    .any(|o| o.name == mz_sql_parser::ast::CreateSinkOptionName::Size)
            {
                node.with_options
                    .push(mz_sql_parser::ast::CreateSinkOption {
                        name: mz_sql_parser::ast::CreateSinkOptionName::Size,
                        value: Some(mz_sql_parser::ast::WithOptionValue::Value(
                            mz_sql_parser::ast::Value::String(
                                self.state.default_linked_cluster_size(),
                            ),
                        )),
                    })
            }
            visit_mut::visit_create_sink_statement_mut(self, node);
        }
    }

    Rewriter { state }.visit_statement_mut(stmt);
}

fn _add_to_audit_log(
    tx: &mut Transaction,
    event_type: mz_audit_log::EventType,
    object_type: mz_audit_log::ObjectType,
    details: mz_audit_log::EventDetails,
    occurred_at: EpochMillis,
) -> Result<(), anyhow::Error> {
    let id = tx.get_and_increment_id(mz_catalog::durable::AUDIT_LOG_ID_ALLOC_KEY.to_string())?;
    let event =
        mz_audit_log::VersionedEvent::new(id, event_type, object_type, details, None, occurred_at);
    tx.insert_audit_log_event(event);
    Ok(())
}
