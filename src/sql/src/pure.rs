// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! SQL purification.
//!
//! See the [crate-level documentation](crate) for details.

use std::collections::{BTreeMap, BTreeSet};
use std::iter;
use std::path::Path;
use std::sync::Arc;

use anyhow::anyhow;
use mz_ccsr::{Client, GetByIdError, GetBySubjectError, Schema as CcsrSchema};
use mz_kafka_util::client::MzClientContext;
use mz_ore::error::ErrorExt;
use mz_ore::str::StrExt;
use mz_proto::RustType;
use mz_repr::{strconv, GlobalId};
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{
    AlterSourceAction, AlterSourceStatement, CreateSubsourceOption, CreateSubsourceOptionName,
    CsrConnection, CsrSeedAvro, CsrSeedProtobuf, CsrSeedProtobufSchema, DbzMode, DeferredItemName,
    Envelope, KafkaConfigOption, KafkaConfigOptionName, KafkaConnection, KafkaSourceConnection,
    PgConfigOption, PgConfigOptionName, RawItemName, ReaderSchemaSelectionStrategy, Statement,
    UnresolvedItemName,
};
use mz_storage_client::types::connections::{Connection, ConnectionContext};
use mz_storage_client::types::sources::{
    GenericSourceConnection, PostgresSourcePublicationDetails, SourceConnection,
};
use prost::Message;
use protobuf_native::compiler::{SourceTreeDescriptorDatabase, VirtualSourceTree};
use protobuf_native::MessageLite;
use tracing::info;
use uuid::Uuid;

use crate::ast::{
    AvroSchema, CreateSourceConnection, CreateSourceFormat, CreateSourceStatement,
    CreateSourceSubsource, CreateSubsourceStatement, CsrConnectionAvro, CsrConnectionProtobuf,
    Format, ProtobufSchema, ReferencedSubsources, Value, WithOptionValue,
};
use crate::catalog::{ErsatzCatalog, SessionCatalog};
use crate::kafka_util::KafkaConfigOptionExtracted;
use crate::names::{Aug, RawDatabaseSpecifier};
use crate::plan::error::PlanError;
use crate::plan::statement::ddl::load_generator_ast_to_generator;
use crate::plan::StatementContext;
use crate::{kafka_util, normalize};

mod postgres;

fn subsource_gen<'a, T>(
    selected_subsources: &mut Vec<CreateSourceSubsource<Aug>>,
    catalog: &ErsatzCatalog<'a, T>,
    source_name: &mut UnresolvedItemName,
) -> Result<Vec<(UnresolvedItemName, UnresolvedItemName, &'a T)>, PlanError> {
    let mut validated_requested_subsources = vec![];

    for subsource in selected_subsources {
        let subsource_name = match &subsource.subsource {
            Some(name) => match name {
                DeferredItemName::Deferred(name) => {
                    let partial = normalize::unresolved_item_name(name.clone())?;
                    match partial.schema {
                        Some(_) => name.clone(),
                        // In cases when a prefix is not provided for the deferred name
                        // fallback to using the schema of the source with the given name
                        None => subsource_name_gen(source_name, &partial.item)?,
                    }
                }
                DeferredItemName::Named(..) => {
                    unreachable!("already errored on this condition")
                }
            },
            None => {
                // Use the entered name as the upstream reference, and then use
                // the item as the subsource name to ensure it's created in the
                // current schema or the source's schema if provided, not mirroring
                // the schema of the reference.
                subsource_name_gen(
                    source_name,
                    &normalize::unresolved_item_name(subsource.reference.clone())?.item,
                )?
            }
        };

        let (qualified_upstream_name, desc) = catalog.resolve(subsource.reference.clone())?;

        validated_requested_subsources.push((qualified_upstream_name, subsource_name, desc));
    }

    Ok(validated_requested_subsources)
}

// Convenience function to ensure subsources are not named.
fn named_subsource_err(name: &Option<DeferredItemName<Aug>>) -> Result<(), PlanError> {
    match name {
        Some(DeferredItemName::Named(_)) => {
            sql_bail!("Cannot manually ID qualify subsources")
        }
        _ => Ok(()),
    }
}

/// Generates a subsource name by prepending source schema name if present
///
/// For eg. if source is `a.b`, then `a` will be prepended to the subsource name
/// so that it's generated in the same schema as source
fn subsource_name_gen(
    source_name: &UnresolvedItemName,
    subsource_name: &String,
) -> Result<UnresolvedItemName, PlanError> {
    let mut partial = normalize::unresolved_item_name(source_name.clone())?;
    partial.item = subsource_name.to_string();
    Ok(UnresolvedItemName::from(partial))
}

/// Purifies a statement, removing any dependencies on external state.
///
/// See the section on [purification](crate#purification) in the crate
/// documentation for details.
pub async fn purify_statement(
    catalog: Box<dyn SessionCatalog>,
    now: u64,
    stmt: Statement<Aug>,
    connection_context: ConnectionContext,
) -> Result<
    (
        Vec<(GlobalId, CreateSubsourceStatement<Aug>)>,
        Statement<Aug>,
    ),
    PlanError,
> {
    match stmt {
        Statement::CreateSource(stmt) => {
            purify_create_source(catalog, now, stmt, connection_context).await
        }
        Statement::AlterSource(stmt) => {
            purify_alter_source(catalog, stmt, connection_context).await
        }
        o => unreachable!("{:?} does not need to be purified", o),
    }
}

async fn purify_create_source(
    catalog: Box<dyn SessionCatalog>,
    now: u64,
    mut stmt: CreateSourceStatement<Aug>,
    connection_context: ConnectionContext,
) -> Result<
    (
        Vec<(GlobalId, CreateSubsourceStatement<Aug>)>,
        Statement<Aug>,
    ),
    PlanError,
> {
    let CreateSourceStatement {
        name: source_name,
        connection,
        format,
        envelope,
        include_metadata: _,
        referenced_subsources,
        progress_subsource,
        ..
    } = &mut stmt;

    // Disallow manually targetting subsources, this syntax is reserved for purification only
    named_subsource_err(progress_subsource)?;
    if let Some(ReferencedSubsources::SubsetTables(subsources)) = referenced_subsources {
        for CreateSourceSubsource {
            subsource,
            reference: _,
        } in subsources
        {
            named_subsource_err(subsource)?;
        }
    }

    let mut subsource_id_counter = 0;
    let mut get_transient_subsource_id = move || {
        subsource_id_counter += 1;
        subsource_id_counter
    };

    let mut subsources = vec![];

    let progress_desc = match &connection {
        CreateSourceConnection::Kafka(_) => &mz_storage_client::types::sources::KAFKA_PROGRESS_DESC,
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

    match &connection {
        CreateSourceConnection::Kafka(_) | CreateSourceConnection::TestScript { .. } => {
            if let Some(referenced_subsources) = &referenced_subsources {
                sql_bail!(
                    "{} is only valid for multi-output sources",
                    referenced_subsources.to_ast_string()
                );
            }
        }
        CreateSourceConnection::Postgres { .. } | CreateSourceConnection::LoadGenerator { .. } => {}
    }

    match connection {
        CreateSourceConnection::Kafka(KafkaSourceConnection {
            connection:
                KafkaConnection {
                    connection,
                    options: base_with_options,
                },
            ..
        }) => {
            let scx = StatementContext::new(None, &*catalog);
            let mut connection = {
                let item = scx.get_item_by_resolved_name(connection)?;
                // Get Kafka connection
                match item.connection()? {
                    Connection::Kafka(connection) => connection.clone(),
                    _ => sql_bail!(
                        "{} is not a kafka connection",
                        scx.catalog.resolve_full_name(item.name())
                    ),
                }
            };

            let extracted_options: KafkaConfigOptionExtracted =
                base_with_options.clone().try_into()?;

            let offset_type =
                Option::<kafka_util::KafkaStartOffsetType>::try_from(&extracted_options)?;

            for (k, v) in kafka_util::LibRdKafkaConfig::try_from(&extracted_options)?.0 {
                connection.options.insert(k, v);
            }

            let topic = extracted_options
                .topic
                .ok_or_else(|| sql_err!("KAFKA CONNECTION without TOPIC"))?;

            let consumer = connection
                .create_with_context(&connection_context, MzClientContext, &BTreeMap::new())
                .await
                .map_err(|e| {
                    anyhow!(
                        "Failed to create and connect Kafka consumer: {}",
                        e.display_with_causes()
                    )
                })?;
            let consumer = Arc::new(consumer);

            if let Some(offset_type) = offset_type {
                // Translate `START TIMESTAMP` to a start offset
                match kafka_util::lookup_start_offsets(
                    Arc::clone(&consumer),
                    &topic,
                    offset_type,
                    now,
                )
                .await?
                {
                    Some(start_offsets) => {
                        // Drop the value we are purifying
                        base_with_options.retain(|val| match val {
                            KafkaConfigOption {
                                name: KafkaConfigOptionName::StartTimestamp,
                                ..
                            } => false,
                            _ => true,
                        });
                        info!("add start_offset {:?}", start_offsets);
                        base_with_options.push(KafkaConfigOption {
                            name: KafkaConfigOptionName::StartOffset,
                            value: Some(WithOptionValue::Sequence(
                                start_offsets
                                    .iter()
                                    .map(|offset| {
                                        WithOptionValue::Value(Value::Number(offset.to_string()))
                                    })
                                    .collect(),
                            )),
                        });
                    }
                    None => {}
                }
            }
        }
        CreateSourceConnection::TestScript { desc_json: _ } => {
            // TODO: verify valid json and valid schema
        }
        CreateSourceConnection::Postgres {
            connection,
            options,
        } => {
            let scx = StatementContext::new(None, &*catalog);
            let connection = {
                let item = scx.get_item_by_resolved_name(connection)?;
                match item.connection()? {
                    Connection::Postgres(connection) => connection.clone(),
                    _ => sql_bail!(
                        "{} is not a postgres connection",
                        scx.catalog.resolve_full_name(item.name())
                    ),
                }
            };
            let crate::plan::statement::PgConfigOptionExtracted {
                publication,
                mut text_columns,
                ..
            } = options.clone().try_into()?;
            let publication = publication
                .ok_or_else(|| sql_err!("POSTGRES CONNECTION must specify PUBLICATION"))?;

            // verify that we can connect upstream and snapshot publication metadata
            let config = connection
                .config(&*connection_context.secrets_reader)
                .await?;

            let publication_tables =
                mz_postgres_util::publication_info(&config, &publication, None).await?;

            if publication_tables.is_empty() {
                return Err(PlanError::EmptyPublication(publication.to_string()));
            }

            let publication_catalog = postgres::derive_catalog_from_publication_tables(
                &connection.database,
                &publication_tables,
            )?;

            let mut validated_requested_subsources = vec![];
            match referenced_subsources {
                Some(ReferencedSubsources::All) => {
                    for table in &publication_tables {
                        let upstream_name = UnresolvedItemName::qualified(&[
                            &connection.database,
                            &table.namespace,
                            &table.name,
                        ]);
                        let subsource_name = subsource_name_gen(source_name, &table.name)?;
                        validated_requested_subsources.push((upstream_name, subsource_name, table));
                    }
                }
                Some(ReferencedSubsources::SubsetSchemas(schemas)) => {
                    let available_schemas: BTreeSet<_> = mz_postgres_util::get_schemas(&config)
                        .await?
                        .into_iter()
                        .map(|s| s.name)
                        .collect();

                    let requested_schemas: BTreeSet<_> =
                        schemas.iter().map(|s| s.as_str().to_string()).collect();

                    let missing_schemas: Vec<_> = requested_schemas
                        .difference(&available_schemas)
                        .map(|s| s.to_string())
                        .collect();

                    if !missing_schemas.is_empty() {
                        return Err(PlanError::PostgresDatabaseMissingFilteredSchemas {
                            schemas: missing_schemas,
                        });
                    }

                    for table in &publication_tables {
                        if !requested_schemas.contains(table.namespace.as_str()) {
                            continue;
                        }

                        let upstream_name = UnresolvedItemName::qualified(&[
                            &connection.database,
                            &table.namespace,
                            &table.name,
                        ]);
                        let subsource_name = UnresolvedItemName::unqualified(&table.name);
                        validated_requested_subsources.push((upstream_name, subsource_name, table));
                    }
                }
                Some(ReferencedSubsources::SubsetTables(subsources)) => {
                    // The user manually selected a subset of upstream tables so we need to
                    // validate that the names actually exist and are not ambiguous
                    validated_requested_subsources.extend(subsource_gen(
                        subsources,
                        &publication_catalog,
                        source_name,
                    )?);
                }
                None => {
                    sql_bail!("multi-output sources require a FOR TABLES (..), FOR SCHEMAS (..), or FOR ALL TABLES clause");
                }
            };

            if validated_requested_subsources.is_empty() {
                sql_bail!(
                    "[internal error]: Postgres source must ingest at least one table, but {} matched none",
                    referenced_subsources.as_ref().unwrap().to_ast_string()
                );
            }

            postgres::validate_requested_subsources(&config, &validated_requested_subsources)
                .await?;

            let text_cols_dict = postgres::generate_text_columns(
                &publication_catalog,
                &mut text_columns,
                &PgConfigOptionName::TextColumns.to_ast_string(),
            )?;

            // Normalize options to contain full qualified values.
            if let Some(text_cols_option) = options
                .iter_mut()
                .find(|option| option.name == PgConfigOptionName::TextColumns)
            {
                let seq = text_columns
                    .into_iter()
                    .map(WithOptionValue::UnresolvedItemName)
                    .collect();
                text_cols_option.value = Some(WithOptionValue::Sequence(seq));
            }

            let (targeted_subsources, new_subsources) = postgres::generate_targeted_subsources(
                &scx,
                validated_requested_subsources,
                text_cols_dict,
                get_transient_subsource_id,
                &publication_tables,
            )?;

            *referenced_subsources = Some(ReferencedSubsources::SubsetTables(targeted_subsources));
            subsources.extend(new_subsources);

            // Remove any old detail references
            options.retain(|PgConfigOption { name, .. }| name != &PgConfigOptionName::Details);
            let details = PostgresSourcePublicationDetails {
                tables: publication_tables,
                slot: format!(
                    "materialize_{}",
                    Uuid::new_v4().to_string().replace('-', "")
                ),
            };
            options.push(PgConfigOption {
                name: PgConfigOptionName::Details,
                value: Some(WithOptionValue::Value(Value::String(hex::encode(
                    details.into_proto().encode_to_vec(),
                )))),
            })
        }
        CreateSourceConnection::LoadGenerator { generator, options } => {
            let scx = StatementContext::new(None, &*catalog);

            let (_load_generator, available_subsources) =
                load_generator_ast_to_generator(generator, options)?;

            let mut targeted_subsources = vec![];

            let mut validated_requested_subsources = vec![];
            match referenced_subsources {
                Some(ReferencedSubsources::All) => {
                    let available_subsources = match &available_subsources {
                        Some(available_subsources) => available_subsources,
                        None => {
                            sql_bail!("FOR ALL TABLES is only valid for multi-output sources")
                        }
                    };
                    for (name, (_, desc)) in available_subsources {
                        let upstream_name = UnresolvedItemName::from(name.clone());
                        let subsource_name = subsource_name_gen(source_name, &name.item)?;
                        validated_requested_subsources.push((upstream_name, subsource_name, desc));
                    }
                }
                Some(ReferencedSubsources::SubsetSchemas(..)) => {
                    sql_bail!("FOR SCHEMAS (..) invalid for LOAD GENERATOR sources");
                }
                Some(ReferencedSubsources::SubsetTables(selected_subsources)) => {
                    let available_subsources = match &available_subsources {
                        Some(available_subsources) => available_subsources,
                        None => {
                            sql_bail!("FOR TABLES (..) is only valid for multi-output sources")
                        }
                    };
                    // The user manually selected a subset of upstream tables so we need to
                    // validate that the names actually exist and are not ambiguous

                    // An index from table name -> schema name -> database name -> PostgresTableDesc
                    let mut tables_by_name = BTreeMap::new();
                    for (subsource_name, (_, desc)) in available_subsources {
                        let database = match &subsource_name.database {
                            RawDatabaseSpecifier::Name(database) => database.clone(),
                            RawDatabaseSpecifier::Ambient => unreachable!(),
                        };
                        tables_by_name
                            .entry(subsource_name.item.clone())
                            .or_insert_with(BTreeMap::new)
                            .entry(subsource_name.schema.clone())
                            .or_insert_with(BTreeMap::new)
                            .entry(database)
                            .or_insert(desc);
                    }

                    validated_requested_subsources.extend(subsource_gen(
                        selected_subsources,
                        &ErsatzCatalog(tables_by_name),
                        source_name,
                    )?);
                }
                None => {
                    if available_subsources.is_some() {
                        sql_bail!("multi-output sources require a FOR TABLES (..) or FOR ALL TABLES statement");
                    }
                }
            };

            // Now that we have an explicit list of validated requested subsources we can create them
            for (upstream_name, subsource_name, desc) in validated_requested_subsources.into_iter()
            {
                let (columns, table_constraints) = scx.relation_desc_into_table_defs(desc)?;

                // Create the targeted AST node for the original CREATE SOURCE statement
                let transient_id = GlobalId::Transient(get_transient_subsource_id());

                let subsource =
                    scx.allocate_resolved_item_name(transient_id, subsource_name.clone())?;

                targeted_subsources.push(CreateSourceSubsource {
                    reference: upstream_name,
                    subsource: Some(DeferredItemName::Named(subsource)),
                });

                // Create the subsource statement
                let subsource = CreateSubsourceStatement {
                    name: subsource_name,
                    columns,
                    // unlike sources that come from an external upstream, we
                    // have more leniency to introduce different constraints
                    // every time the load generator is run; i.e. we are not as
                    // worried about introducing junk data.
                    constraints: table_constraints,
                    if_not_exists: false,
                    with_options: vec![CreateSubsourceOption {
                        name: CreateSubsourceOptionName::References,
                        value: Some(WithOptionValue::Value(Value::Boolean(true))),
                    }],
                };
                subsources.push((transient_id, subsource));
            }
            if available_subsources.is_some() {
                *referenced_subsources =
                    Some(ReferencedSubsources::SubsetTables(targeted_subsources));
            }
        }
    }

    // Generate progress subsource

    // Create the targeted AST node for the original CREATE SOURCE statement
    let transient_id = GlobalId::Transient(subsource_id_counter);

    let scx = StatementContext::new(None, &*catalog);

    // Take name from input or generate name
    let (name, subsource) = match progress_subsource {
        Some(name) => match name {
            DeferredItemName::Deferred(name) => (
                name.clone(),
                scx.allocate_resolved_item_name(transient_id, name.clone())?,
            ),
            DeferredItemName::Named(_) => unreachable!("already checked for this value"),
        },
        None => {
            let (item, prefix) = source_name.0.split_last().unwrap();
            let mut suggested_name = prefix.to_vec();
            suggested_name.push(format!("{}_progress", item).into());

            let partial = normalize::unresolved_item_name(UnresolvedItemName(suggested_name))?;
            let qualified = scx.allocate_qualified_name(partial)?;
            let found_name = scx.catalog.find_available_name(qualified);
            let full_name = scx.catalog.resolve_full_name(&found_name);

            (
                UnresolvedItemName::from(full_name.clone()),
                crate::names::ResolvedItemName::Item {
                    id: transient_id,
                    qualifiers: found_name.qualifiers,
                    full_name,
                    print_id: true,
                },
            )
        }
    };

    let (columns, constraints) = scx.relation_desc_into_table_defs(progress_desc)?;

    *progress_subsource = Some(DeferredItemName::Named(subsource));

    // Create the subsource statement
    let subsource = CreateSubsourceStatement {
        name,
        columns,
        constraints,
        if_not_exists: false,
        with_options: vec![CreateSubsourceOption {
            name: CreateSubsourceOptionName::Progress,
            value: Some(WithOptionValue::Value(Value::Boolean(true))),
        }],
    };
    subsources.push((transient_id, subsource));

    purify_source_format(&*catalog, format, connection, envelope, &connection_context).await?;

    Ok((subsources, Statement::CreateSource(stmt)))
}

/// Equivalent to `purify_create_source` but for `AlterSourceStatement`.
///
/// On success, returns the `GlobalId` and `CreateSubsourceStatement`s for any
/// subsources created by this statement, in addition to the
/// `AlterSourceStatement` with any modifications that are only accessible while
/// we are permitted to use async code.
async fn purify_alter_source(
    catalog: Box<dyn SessionCatalog>,
    mut stmt: AlterSourceStatement<Aug>,
    connection_context: ConnectionContext,
) -> Result<
    (
        Vec<(GlobalId, CreateSubsourceStatement<Aug>)>,
        Statement<Aug>,
    ),
    PlanError,
> {
    let scx = StatementContext::new(None, &*catalog);
    let AlterSourceStatement {
        source_name,
        action,
        if_exists,
    } = &mut stmt;

    // Get connection
    let pg_source_connection = {
        // Get name.
        let item = match scx.resolve_item(RawItemName::Name(source_name.clone())) {
            Ok(item) => item,
            Err(_) if *if_exists => {
                return Ok((vec![], Statement::AlterSource(stmt)));
            }
            Err(e) => return Err(e),
        };

        // Ensure it's an ingestion-based and alterable source.
        let desc = match item.source_desc()? {
            Some(desc) => desc,
            None => {
                sql_bail!("cannot ALTER this type of source")
            }
        };

        // If there's no further work to do here, early return.
        if !matches!(action, AlterSourceAction::AddSubsources { .. }) {
            return Ok((vec![], Statement::AlterSource(stmt)));
        }

        match &desc.connection {
            GenericSourceConnection::Postgres(pg_connection) => pg_connection.clone(),
            _ => sql_bail!(
                "{} is a {} source, which does not support ALTER TABLE...ADD SUBSOURCES",
                scx.catalog.minimal_qualification(item.name()),
                desc.connection.name()
            ),
        }
    };

    let (targeted_subsources, details) = match action {
        AlterSourceAction::AddSubsources {
            subsources,
            details,
        } => (subsources, details),
        _ => unreachable!(),
    };

    assert!(
        details.is_none(),
        "details cannot be set before purification"
    );

    for CreateSourceSubsource {
        subsource,
        reference: _,
    } in targeted_subsources.iter()
    {
        named_subsource_err(subsource)?;
    }

    // Get PostgresConnection for generating subsources.
    let pg_connection = &pg_source_connection.connection;

    let config = pg_connection
        .config(&*connection_context.secrets_reader)
        .await?;

    let mut publication_tables =
        mz_postgres_util::publication_info(&config, &pg_source_connection.publication, None)
            .await?;

    if publication_tables.is_empty() {
        return Err(PlanError::EmptyPublication(
            pg_source_connection.publication.to_string(),
        ));
    }

    let publication_catalog = postgres::derive_catalog_from_publication_tables(
        &pg_connection.database,
        &publication_tables,
    )?;

    let validated_requested_subsources =
        subsource_gen(targeted_subsources, &publication_catalog, source_name)?;

    // Determine duplicate references to tables by cross-referencing the table
    // positions in the current publication info to thei
    let mut current_subsources = BTreeMap::new();
    for idx in pg_source_connection.table_casts.keys() {
        // Table casts all have their values increased by to accommodate for the
        // primary source--this means that to look them up in the publication
        // tables you must subtract one.
        let native_idx = *idx - 1;
        let table_desc = &pg_source_connection.publication_details.tables[native_idx];
        current_subsources.insert(
            UnresolvedItemName(vec![
                pg_connection.database.clone().into(),
                table_desc.namespace.clone().into(),
                table_desc.name.clone().into(),
            ]),
            native_idx,
        );
    }

    for (upstream_name, _, _) in validated_requested_subsources.iter() {
        if current_subsources.contains_key(upstream_name) {
            sql_bail!(
                "cannot create multiple subsources in the same source that refer to upstream table {}",
                upstream_name
            );
        }
    }

    postgres::validate_requested_subsources(&config, &validated_requested_subsources).await?;

    // TODO: text columns

    let mut subsource_id_counter = 0;
    let get_transient_subsource_id = move || {
        subsource_id_counter += 1;
        subsource_id_counter
    };

    let (named_subsources, new_subsources) = postgres::generate_targeted_subsources(
        &scx,
        validated_requested_subsources,
        BTreeMap::new(),
        get_transient_subsource_id,
        &publication_tables,
    )?;

    *targeted_subsources = named_subsources;

    // An index from table name -> output index.
    let mut new_name_to_output_map = BTreeMap::new();
    for (i, table) in publication_tables.iter().enumerate() {
        new_name_to_output_map.insert(
            UnresolvedItemName(vec![
                pg_connection.database.clone().into(),
                table.namespace.clone().into(),
                table.name.clone().into(),
            ]),
            i,
        );
    }

    // Fixup the publication info
    for (name, idx) in current_subsources {
        let table = pg_source_connection.publication_details.tables[idx].clone();

        // Determine if this current subsource is in the new publication tables.
        match new_name_to_output_map.get(&name) {
            // These are tables that were previously defined; we want to
            // duplicate their definition to the new `publication_tables`
            // because this command is meant only to add new tables, not update
            // the schema of existing tables.
            Some(cur_idx) => publication_tables[*cur_idx] = table,
            // These are tables that no longer exist in the publication but the
            // user has kept around. When the ingestion restarts after adding
            // the new table, they will error out, but that is not the problem
            // or scope of this function.
            None => publication_tables.push(table),
        }
    }

    // TODO: Options
    let new_details = PostgresSourcePublicationDetails {
        tables: publication_tables,
        slot: pg_source_connection.publication_details.slot.clone(),
    };

    *details = Some(WithOptionValue::Value(Value::String(hex::encode(
        new_details.into_proto().encode_to_vec(),
    ))));

    Ok((new_subsources, Statement::AlterSource(stmt)))
}

async fn purify_source_format(
    catalog: &dyn SessionCatalog,
    format: &mut CreateSourceFormat<Aug>,
    connection: &mut CreateSourceConnection<Aug>,
    envelope: &Option<Envelope>,
    connection_context: &ConnectionContext,
) -> Result<(), PlanError> {
    if matches!(format, CreateSourceFormat::KeyValue { .. })
        && !matches!(
            connection,
            CreateSourceConnection::Kafka { .. } | CreateSourceConnection::TestScript { .. }
        )
    {
        // We don't mention `TestScript` to users here
        sql_bail!("Kafka sources are the only source type that can provide KEY/VALUE formats")
    }

    match format {
        CreateSourceFormat::None => {}
        CreateSourceFormat::Bare(format) => {
            purify_source_format_single(catalog, format, connection, envelope, connection_context)
                .await?;
        }

        CreateSourceFormat::KeyValue { key, value: val } => {
            purify_source_format_single(catalog, key, connection, envelope, connection_context)
                .await?;
            purify_source_format_single(catalog, val, connection, envelope, connection_context)
                .await?;
        }
    }
    Ok(())
}

async fn purify_source_format_single(
    catalog: &dyn SessionCatalog,
    format: &mut Format<Aug>,
    connection: &mut CreateSourceConnection<Aug>,
    envelope: &Option<Envelope>,
    connection_context: &ConnectionContext,
) -> Result<(), PlanError> {
    match format {
        Format::Avro(schema) => match schema {
            AvroSchema::Csr { csr_connection } => {
                purify_csr_connection_avro(
                    catalog,
                    connection,
                    csr_connection,
                    envelope,
                    connection_context,
                )
                .await?
            }
            AvroSchema::InlineSchema { .. } => {}
        },
        Format::Protobuf(schema) => match schema {
            ProtobufSchema::Csr { csr_connection } => {
                purify_csr_connection_proto(
                    catalog,
                    connection,
                    csr_connection,
                    envelope,
                    connection_context,
                )
                .await?;
            }
            ProtobufSchema::InlineSchema { .. } => {}
        },
        Format::Bytes | Format::Regex(_) | Format::Json | Format::Text | Format::Csv { .. } => (),
    }
    Ok(())
}

async fn purify_csr_connection_proto(
    catalog: &dyn SessionCatalog,
    connection: &mut CreateSourceConnection<Aug>,
    csr_connection: &mut CsrConnectionProtobuf<Aug>,
    envelope: &Option<Envelope>,
    connection_context: &ConnectionContext,
) -> Result<(), PlanError> {
    let topic = if let CreateSourceConnection::Kafka(KafkaSourceConnection {
        connection: KafkaConnection { options, .. },
        ..
    }) = connection
    {
        let KafkaConfigOptionExtracted { topic, .. } = options
            .clone()
            .try_into()
            .expect("already verified options valid provided");
        topic.expect("already validated topic provided")
    } else {
        sql_bail!("Confluent Schema Registry is only supported with Kafka sources")
    };

    let CsrConnectionProtobuf {
        seed,
        connection: CsrConnection {
            connection,
            options: _,
        },
    } = csr_connection;
    match seed {
        None => {
            let scx = StatementContext::new(None, &*catalog);

            let ccsr_connection = match scx.get_item_by_resolved_name(connection)?.connection()? {
                Connection::Csr(connection) => connection.clone(),
                _ => sql_bail!("{} is not a schema registry connection", connection),
            };

            let ccsr_client = ccsr_connection.connect(connection_context).await?;

            let value = compile_proto(&format!("{}-value", topic), &ccsr_client).await?;
            let key = compile_proto(&format!("{}-key", topic), &ccsr_client)
                .await
                .ok();

            if matches!(envelope, Some(Envelope::Debezium(DbzMode::Plain))) && key.is_none() {
                sql_bail!("Key schema is required for ENVELOPE DEBEZIUM");
            }

            *seed = Some(CsrSeedProtobuf { value, key });
        }
        Some(_) => (),
    }

    Ok(())
}

async fn purify_csr_connection_avro(
    catalog: &dyn SessionCatalog,
    connection: &mut CreateSourceConnection<Aug>,
    csr_connection: &mut CsrConnectionAvro<Aug>,
    envelope: &Option<Envelope>,
    connection_context: &ConnectionContext,
) -> Result<(), PlanError> {
    let topic = if let CreateSourceConnection::Kafka(KafkaSourceConnection {
        connection: KafkaConnection { options, .. },
        ..
    }) = connection
    {
        let KafkaConfigOptionExtracted { topic, .. } = options
            .clone()
            .try_into()
            .expect("already verified options valid provided");
        topic.expect("already validated topic provided")
    } else {
        sql_bail!("Confluent Schema Registry is only supported with Kafka sources")
    };

    let CsrConnectionAvro {
        connection: CsrConnection { connection, .. },
        seed,
        key_strategy,
        value_strategy,
    } = csr_connection;
    if seed.is_none() {
        let scx = StatementContext::new(None, &*catalog);
        let csr_connection = match scx.get_item_by_resolved_name(connection)?.connection()? {
            Connection::Csr(connection) => connection.clone(),
            _ => sql_bail!("{} is not a schema registry connection", connection),
        };
        let ccsr_client = csr_connection.connect(connection_context).await?;

        let Schema {
            key_schema,
            value_schema,
        } = get_remote_csr_schema(
            &ccsr_client,
            key_strategy.clone().unwrap_or_default(),
            value_strategy.clone().unwrap_or_default(),
            topic,
        )
        .await?;
        if matches!(envelope, Some(Envelope::Debezium(DbzMode::Plain))) && key_schema.is_none() {
            sql_bail!("Key schema is required for ENVELOPE DEBEZIUM");
        }

        *seed = Some(CsrSeedAvro {
            key_schema,
            value_schema,
        })
    }

    Ok(())
}

#[derive(Debug)]
pub struct Schema {
    pub key_schema: Option<String>,
    pub value_schema: String,
}

async fn get_schema_with_strategy(
    client: &Client,
    strategy: ReaderSchemaSelectionStrategy,
    subject: &str,
) -> Result<Option<String>, PlanError> {
    match strategy {
        ReaderSchemaSelectionStrategy::Latest => {
            match client.get_schema_by_subject(subject).await {
                Ok(CcsrSchema { raw, .. }) => Ok(Some(raw)),
                Err(GetBySubjectError::SubjectNotFound) => Ok(None),
                Err(e) => Err(PlanError::FetchingCsrSchemaFailed {
                    schema_lookup: format!("subject {}", subject.quoted()),
                    cause: Arc::new(e),
                }),
            }
        }
        ReaderSchemaSelectionStrategy::Inline(raw) => Ok(Some(raw)),
        ReaderSchemaSelectionStrategy::ById(id) => match client.get_schema_by_id(id).await {
            Ok(CcsrSchema { raw, .. }) => Ok(Some(raw)),
            Err(GetByIdError::SchemaNotFound) => Ok(None),
            Err(e) => Err(PlanError::FetchingCsrSchemaFailed {
                schema_lookup: format!("ID {}", id),
                cause: Arc::new(e),
            }),
        },
    }
}

async fn get_remote_csr_schema(
    ccsr_client: &mz_ccsr::Client,
    key_strategy: ReaderSchemaSelectionStrategy,
    value_strategy: ReaderSchemaSelectionStrategy,
    topic: String,
) -> Result<Schema, PlanError> {
    let value_schema_name = format!("{}-value", topic);
    let value_schema =
        get_schema_with_strategy(ccsr_client, value_strategy, &value_schema_name).await?;
    let value_schema = value_schema.ok_or_else(|| anyhow!("No value schema found"))?;
    let subject = format!("{}-key", topic);
    let key_schema = get_schema_with_strategy(ccsr_client, key_strategy, &subject).await?;
    Ok(Schema {
        key_schema,
        value_schema,
    })
}

/// Collect protobuf message descriptor from CSR and compile the descriptor.
async fn compile_proto(
    subject_name: &String,
    ccsr_client: &Client,
) -> Result<CsrSeedProtobufSchema, PlanError> {
    let (primary_subject, dependency_subjects) = ccsr_client
        .get_subject_and_references(subject_name)
        .await
        .map_err(|e| PlanError::FetchingCsrSchemaFailed {
            schema_lookup: format!("subject {}", subject_name.quoted()),
            cause: Arc::new(e),
        })?;

    // Compile .proto files into a file descriptor set.
    let mut source_tree = VirtualSourceTree::new();
    for subject in iter::once(&primary_subject).chain(dependency_subjects.iter()) {
        source_tree.as_mut().add_file(
            Path::new(&subject.name),
            subject.schema.raw.as_bytes().to_vec(),
        );
    }
    let mut db = SourceTreeDescriptorDatabase::new(source_tree.as_mut());
    let fds = db
        .as_mut()
        .build_file_descriptor_set(&[Path::new(&primary_subject.name)])
        .map_err(|cause| PlanError::InvalidProtobufSchema { cause })?;

    // Ensure there is exactly one message in the file.
    let primary_fd = fds.file(0);
    let message_name = match primary_fd.message_type_size() {
        1 => String::from_utf8_lossy(primary_fd.message_type(0).name()).into_owned(),
        0 => bail_unsupported!(9598, "Protobuf schemas with no messages"),
        _ => bail_unsupported!(9598, "Protobuf schemas with multiple messages"),
    };

    // Encode the file descriptor set into a SQL byte string.
    let bytes = &fds
        .serialize()
        .map_err(|cause| PlanError::InvalidProtobufSchema { cause })?;
    let mut schema = String::new();
    strconv::format_bytes(&mut schema, bytes);

    Ok(CsrSeedProtobufSchema {
        schema,
        message_name,
    })
}
