// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};

use futures::future::BoxFuture;
use mz_catalog::memory::objects::{StateDiff, StateUpdate};
use mz_catalog::{durable::Transaction, memory::objects::StateUpdateKind};
use mz_ore::collections::CollectionExt;
use mz_ore::now::NowFn;
use mz_repr::{GlobalId, Timestamp};
use mz_sql::ast::display::AstDisplay;
use mz_sql::ast::visit_mut::VisitMut;
use mz_sql_parser::ast::{Raw, Statement};
use mz_storage_types::connections::ConnectionContext;
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
    ) -> BoxFuture<'a, Result<(), anyhow::Error>>,
{
    let mut updated_items = BTreeMap::new();
    let items = tx.get_items();
    for mut item in items {
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

    rewrite_ast_items(tx, |tx, _id, stmt| {
        let _catalog_version = catalog_version.clone();
        Box::pin(async move {
            // Add per-item AST migrations below.
            //
            // Each migration should be a function that takes `stmt` (the AST
            // representing the creation SQL for the item) as input. Any
            // mutations to `stmt` will be staged for commit to the catalog.
            //
            // Migration functions may also take `tx` as input to stage
            // arbitrary changes to the catalog.
            ast_rewrite_create_subsource_options(tx, stmt)?;
            Ok(())
        })
    })
    .await?;

    // Load up a temporary catalog.
    let mut state = state.clone();
    // The catalog is temporary, so we can throw out the builtin updates.
    let item_updates = tx
        .get_items()
        .map(|item| {
            let item = mz_catalog::durable::objects::Item::from(item);
            StateUpdate {
                kind: StateUpdateKind::Item(item),
                ts: tx.commit_ts(),
                diff: StateDiff::Addition,
            }
        })
        .collect();
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

/// Copies options from relevant `CREATE SOURCE` statements to any relevant `CREATE SUBSOURCE`
/// statements. This preps for the eventual removal of these options from the `CREATE SOURCE`
/// statement.
fn ast_rewrite_create_subsource_options(
    tx: &mut Transaction,
    stmt: &mut Statement<Raw>,
) -> Result<(), anyhow::Error> {
    use mz_sql::ast::{
        CreateSourceConnection, CreateSubsourceOption, CreateSubsourceOptionName,
        CreateSubsourceStatement, MySqlConfigOptionName, PgConfigOptionName, RawItemName, Value,
        WithOptionValue,
    };
    use mz_storage_types::sources::load_generator::ProtoLoadGeneratorSourceExportStatementDetails;
    use mz_storage_types::sources::mysql::{
        ProtoMySqlSourceDetails, ProtoMySqlSourceExportStatementDetails,
    };
    use mz_storage_types::sources::postgres::{
        ProtoPostgresSourceExportStatementDetails, ProtoPostgresSourcePublicationDetails,
    };
    use mz_storage_types::sources::proto_source_export_statement_details;
    use mz_storage_types::sources::ProtoSourceExportStatementDetails;
    use prost::Message;

    struct Rewriter<'a, 'b> {
        tx: &'a Transaction<'b>,
    }

    impl<'ast> VisitMut<'ast, Raw> for Rewriter<'_, '_> {
        fn visit_create_subsource_statement_mut(
            &mut self,
            node: &'ast mut CreateSubsourceStatement<Raw>,
        ) {
            match &node.of_source {
                // Not a source export subsource
                None => (),
                Some(source) => {
                    let details = node
                        .with_options
                        .iter()
                        .find(|o| o.name == CreateSubsourceOptionName::Details);
                    if details.is_some() {
                        // this subsource has already had its details written, so it's either new or was already
                        // migrated
                        return;
                    }
                    info!("migrate: populating subsource details: {:?}", node);

                    let external_reference = node
                        .with_options
                        .iter()
                        .find(|o| o.name == CreateSubsourceOptionName::ExternalReference)
                        .expect("subsources must have external reference");
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

                    let source = match &source {
                        RawItemName::Name(_) => {
                            panic!("named item reference in durable catalog")
                        }
                        RawItemName::Id(id, _) => {
                            let gid = id
                                .parse()
                                .expect("RawItenName::Id must be uncorrupted GlobalId");
                            self.tx.get_item(&gid).expect("source must exist")
                        }
                    };
                    let source_statement =
                        mz_sql_parser::parser::parse_statements(&source.create_sql)
                            .expect("parsing persisted create_sql must succeed")
                            .into_element()
                            .ast;
                    match source_statement {
                        Statement::CreateSource(stmt) => match &stmt.connection {
                            CreateSourceConnection::Postgres {
                                connection: _,
                                options,
                            } => {
                                // Copy the PostgresTableDesc from the top-level source publication details proto
                                // into the subsource details proto.
                                let details = options
                                    .iter()
                                    .find(|o| o.name == PgConfigOptionName::Details)
                                    .expect("Sources must have details");
                                let details_val = match &details.value {
                                    Some(WithOptionValue::Value(Value::String(details))) => details,
                                    _ => unreachable!("Source details' value must be a string"),
                                };
                                let details = hex::decode(details_val)
                                    .expect("Source details must be a hex-encoded string");
                                let details =
                                    ProtoPostgresSourcePublicationDetails::decode(&*details)
                                        .expect("Source details must be a hex-encoded protobuf");
                                let table = details
                                    .deprecated_tables
                                    .iter()
                                    .find(|t| {
                                        t.namespace == external_schema && t.name == external_table
                                    })
                                    .expect("subsource table must be in source details");
                                let subsource_details = ProtoSourceExportStatementDetails {
                                    kind: Some(
                                        proto_source_export_statement_details::Kind::Postgres(
                                            ProtoPostgresSourceExportStatementDetails {
                                                table: Some(table.clone()),
                                            },
                                        ),
                                    ),
                                };
                                node.with_options.push(CreateSubsourceOption {
                                    name: CreateSubsourceOptionName::Details,
                                    value: Some(WithOptionValue::Value(Value::String(
                                        hex::encode(subsource_details.encode_to_vec()),
                                    ))),
                                });

                                // Copy the relevant Text Columns from the top-level source option into the subsource option
                                let text_columns = options
                                    .iter()
                                    .find(|o| o.name == PgConfigOptionName::TextColumns);
                                if let Some(text_columns) = text_columns {
                                    let table_text_columns = self.columns_for_table(
                                        &external_schema,
                                        &external_table,
                                        &text_columns.value,
                                    );
                                    if table_text_columns.len() > 0 {
                                        node.with_options.push(CreateSubsourceOption {
                                            name: CreateSubsourceOptionName::TextColumns,
                                            value: Some(WithOptionValue::Sequence(
                                                table_text_columns,
                                            )),
                                        });
                                    }
                                }
                            }
                            CreateSourceConnection::MySql {
                                connection: _,
                                options,
                            } => {
                                let details = options
                                    .iter()
                                    .find(|o| o.name == MySqlConfigOptionName::Details)
                                    .expect("Sources must have details");

                                let details_val = match &details.value {
                                    Some(WithOptionValue::Value(Value::String(details))) => details,
                                    _ => unreachable!("Source details' value must be a string"),
                                };

                                let details = hex::decode(details_val)
                                    .expect("Source details must be a hex-encoded string");
                                let details = ProtoMySqlSourceDetails::decode(&*details)
                                    .expect("Source details must be a hex-encoded protobuf");

                                let (table_idx, table) = details
                                    .deprecated_tables
                                    .iter()
                                    .enumerate()
                                    .find(|(_, t)| {
                                        t.schema_name == external_schema && t.name == external_table
                                    })
                                    .expect("subsource table must be in source details");
                                // Handle the 2 versions of the initial_gtid_set fields in the top-level source details
                                let initial_gtid_set = if details.deprecated_initial_gtid_set.len()
                                    == 1
                                {
                                    details.deprecated_initial_gtid_set[0].clone()
                                } else if details.deprecated_initial_gtid_set.len()
                                    == details.deprecated_tables.len()
                                {
                                    details.deprecated_initial_gtid_set[table_idx].clone()
                                } else if details.deprecated_legacy_initial_gtid_set.len() > 0 {
                                    details.deprecated_legacy_initial_gtid_set.clone()
                                } else {
                                    unreachable!("invalid initial GTID set(s) in source details")
                                };

                                let subsource_details = ProtoSourceExportStatementDetails {
                                    kind: Some(proto_source_export_statement_details::Kind::Mysql(
                                        ProtoMySqlSourceExportStatementDetails {
                                            table: Some(table.clone()),
                                            initial_gtid_set,
                                        },
                                    )),
                                };
                                node.with_options.push(CreateSubsourceOption {
                                    name: CreateSubsourceOptionName::Details,
                                    value: Some(WithOptionValue::Value(Value::String(
                                        hex::encode(subsource_details.encode_to_vec()),
                                    ))),
                                });

                                // Copy the relevant Text Columns from the top-level source option into the subsource option
                                let text_columns = options
                                    .iter()
                                    .find(|o| o.name == MySqlConfigOptionName::TextColumns);
                                if let Some(text_columns) = text_columns {
                                    let table_text_columns = self.columns_for_table(
                                        &external_schema,
                                        &external_table,
                                        &text_columns.value,
                                    );
                                    if table_text_columns.len() > 0 {
                                        node.with_options.push(CreateSubsourceOption {
                                            name: CreateSubsourceOptionName::TextColumns,
                                            value: Some(WithOptionValue::Sequence(
                                                table_text_columns,
                                            )),
                                        });
                                    }
                                }
                                // Copy the relevant Ignore Columns from the top-level source option into the subsource option
                                let ignore_columns = options
                                    .iter()
                                    .find(|o| o.name == MySqlConfigOptionName::IgnoreColumns);
                                if let Some(ignore_columns) = ignore_columns {
                                    let table_ignore_columns = self.columns_for_table(
                                        &external_schema,
                                        &external_table,
                                        &ignore_columns.value,
                                    );
                                    if table_ignore_columns.len() > 0 {
                                        node.with_options.push(CreateSubsourceOption {
                                            name: CreateSubsourceOptionName::IgnoreColumns,
                                            value: Some(WithOptionValue::Sequence(
                                                table_ignore_columns,
                                            )),
                                        });
                                    }
                                }
                            }
                            CreateSourceConnection::LoadGenerator { .. } => {
                                // Load generator sources don't have any information we need to copy
                                // to the subsource, so we just create an empty details for the subsource
                                // which is used as a stub to ensure conformity with other source-export subsources
                                let subsource_details = ProtoSourceExportStatementDetails {
                                    kind: Some(
                                        proto_source_export_statement_details::Kind::Loadgen(
                                            ProtoLoadGeneratorSourceExportStatementDetails {},
                                        ),
                                    ),
                                };
                                node.with_options.push(CreateSubsourceOption {
                                    name: CreateSubsourceOptionName::Details,
                                    value: Some(WithOptionValue::Value(Value::String(
                                        hex::encode(subsource_details.encode_to_vec()),
                                    ))),
                                });
                            }
                            _ => unreachable!("unexpected source type for existing subsource"),
                        },
                        _ => unreachable!("source must be a source"),
                    };
                    info!("migrated subsource: {:?}", node);
                }
            }
        }
    }

    impl Rewriter<'_, '_> {
        fn columns_for_table(
            &self,
            external_schema: &str,
            external_table: &str,
            all_columns: &Option<WithOptionValue<Raw>>,
        ) -> Vec<WithOptionValue<Raw>> {
            let all_table_columns = match all_columns {
                Some(WithOptionValue::Sequence(columns)) => columns
                    .into_iter()
                    .map(|column| match column {
                        WithOptionValue::UnresolvedItemName(name) => name,
                        _ => unreachable!("text columns must be UnresolvedItemName"),
                    })
                    .collect::<Vec<_>>(),
                _ => {
                    unreachable!("Source columns value must be a sequence")
                }
            };

            all_table_columns
                .into_iter()
                .filter_map(|name| {
                    // source text columns are an UnresolvedItemName with (schema, table, column) tuples
                    // and we only need to copy the column name for the subsource option
                    if name.0[0].clone().into_string() == external_schema
                        && name.0[1].clone().into_string() == external_table
                    {
                        Some(WithOptionValue::Ident(name.0[2].clone()))
                    } else {
                        None
                    }
                })
                .collect()
        }
    }
    Rewriter { tx }.visit_statement_mut(stmt);

    Ok(())
}

// Durable migrations

/// Migrations that run only on the durable catalog before any data is loaded into memory.
pub(crate) fn durable_migrate(
    tx: &mut Transaction,
    _boot_ts: Timestamp,
) -> Result<(), anyhow::Error> {
    catalog_add_new_unstable_schemas_v_0_106_0(tx)?;
    catalog_remove_wait_catalog_consolidation_on_startup_v_0_108_0(tx);
    catalog_remove_txn_wal_toggle_v_0_109_0(tx)?;
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

/// This migration applies the rename of the built-in `mz_introspection` cluster to
/// `mz_catalog_server` to the durable catalog state.
fn catalog_add_new_unstable_schemas_v_0_106_0(tx: &mut Transaction) -> Result<(), anyhow::Error> {
    use mz_catalog::durable::initialize::{
        MZ_CATALOG_UNSTABLE_SCHEMA_ID, MZ_INTROSPECTION_SCHEMA_ID,
    };
    use mz_pgrepr::oid::{SCHEMA_MZ_CATALOG_UNSTABLE_OID, SCHEMA_MZ_INTROSPECTION_OID};
    use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem};
    use mz_repr::namespaces::{MZ_CATALOG_UNSTABLE_SCHEMA, MZ_INTROSPECTION_SCHEMA};
    use mz_sql::names::SchemaId;
    use mz_sql::rbac;
    use mz_sql::session::user::{MZ_SUPPORT_ROLE_ID, MZ_SYSTEM_ROLE_ID};

    let schema_ids: BTreeSet<_> = tx
        .get_schemas()
        .filter_map(|schema| match schema.id {
            SchemaId::User(_) => None,
            SchemaId::System(id) => Some(id),
        })
        .collect();
    let schema_privileges = vec![
        rbac::default_builtin_object_privilege(mz_sql::catalog::ObjectType::Schema),
        MzAclItem {
            grantee: MZ_SUPPORT_ROLE_ID,
            grantor: MZ_SYSTEM_ROLE_ID,
            acl_mode: AclMode::USAGE,
        },
        rbac::owner_privilege(mz_sql::catalog::ObjectType::Schema, MZ_SYSTEM_ROLE_ID),
    ];

    if !schema_ids.contains(&MZ_CATALOG_UNSTABLE_SCHEMA_ID) {
        tx.insert_system_schema(
            MZ_CATALOG_UNSTABLE_SCHEMA_ID,
            MZ_CATALOG_UNSTABLE_SCHEMA,
            MZ_SYSTEM_ROLE_ID,
            schema_privileges.clone(),
            SCHEMA_MZ_CATALOG_UNSTABLE_OID,
        )?;
    }
    if !schema_ids.contains(&MZ_INTROSPECTION_SCHEMA_ID) {
        tx.insert_system_schema(
            MZ_INTROSPECTION_SCHEMA_ID,
            MZ_INTROSPECTION_SCHEMA,
            MZ_SYSTEM_ROLE_ID,
            schema_privileges.clone(),
            SCHEMA_MZ_INTROSPECTION_OID,
        )?;
    }

    Ok(())
}

/// This migration removes the server configuration parameter
/// "wait_catalog_consolidation_on_startup" which is no longer used.
fn catalog_remove_wait_catalog_consolidation_on_startup_v_0_108_0(tx: &mut Transaction) {
    tx.remove_system_config("wait_catalog_consolidation_on_startup");
}

/// This migration removes the txn wal feature flag.
fn catalog_remove_txn_wal_toggle_v_0_109_0(tx: &mut Transaction) -> Result<(), anyhow::Error> {
    tx.set_config("persist_txn_tables".to_string(), None)?;
    tx.remove_system_config("persist_txn_tables");
    Ok(())
}
