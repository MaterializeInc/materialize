// Copyright Materialize, Inc. All rights reserved.
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

use std::collections::BTreeMap;

use anyhow::{anyhow, bail, Context};
use aws_arn::ARN;
use tokio::fs::File;
use tokio::io::AsyncBufReadExt;
use tokio::task;
use tokio::time::Duration;

use repr::strconv;
use sql_parser::ast::{display::AstDisplay, UnresolvedObjectName};
use sql_parser::ast::{
    AvroSchema, ColumnDef, ColumnOption, ColumnOptionDef, Connector, CreateSourceStatement,
    CreateSourcesStatement, CsrSeed, DbzMode, Envelope, Format, Ident, MultiConnector, Raw,
    Statement,
};
use sql_parser::parser::parse_columns;

use crate::kafka_util;
use crate::normalize;

/// Purifies a statement, removing any dependencies on external state.
///
/// See the section on [purification](crate#purification) in the crate
/// documentation for details.
///
/// Note that purification is asynchronous, and may take an unboundedly long
/// time to complete. As a result purification does *not* have access to a
/// [`Catalog`](crate::catalog::Catalog), as that would require locking access
/// to the catalog for an unbounded amount of time.
pub async fn purify(mut stmt: Statement<Raw>) -> Result<Statement<Raw>, anyhow::Error> {
    if let Statement::CreateSource(CreateSourceStatement {
        col_names,
        connector,
        format,
        with_options,
        envelope,
        ..
    }) = &mut stmt
    {
        let mut with_options_map = normalize::options(with_options);
        let mut config_options = BTreeMap::new();

        let mut file = None;
        match connector {
            Connector::Kafka { broker, .. } => {
                if !broker.contains(':') {
                    *broker += ":9092";
                }

                // Verify that the provided security options are valid and then test them.
                config_options = kafka_util::extract_config(&mut with_options_map)?;
                kafka_util::test_config(&broker, &config_options).await?;
            }
            Connector::AvroOcf { path, .. } => {
                let path = path.clone();
                task::block_in_place(|| {
                    // mz_avro::Reader has no async equivalent, so we're stuck
                    // using blocking calls here.
                    let f = std::fs::File::open(path)?;
                    let r = mz_avro::Reader::new(f)?;
                    if !with_options_map.contains_key("reader_schema") {
                        let schema = serde_json::to_string(r.writer_schema()).unwrap();
                        with_options.push(sql_parser::ast::SqlOption::Value {
                            name: sql_parser::ast::Ident::new("reader_schema"),
                            value: sql_parser::ast::Value::String(schema),
                        });
                    }
                    Ok::<_, anyhow::Error>(())
                })?;
            }
            // Report an error if a file cannot be opened, or if it is a directory.
            Connector::File { path, .. } => {
                let f = File::open(&path).await?;
                if f.metadata().await?.is_dir() {
                    bail!("Expected a regular file, but {} is a directory.", path);
                }
                file = Some(f);
            }
            Connector::S3 { .. } => {
                let aws_info = normalize::aws_connect_info(&mut with_options_map, None)?;
                aws_util::aws::validate_credentials(aws_info.clone(), Duration::from_secs(1))
                    .await?;
            }
            Connector::Kinesis { arn } => {
                let region = arn
                    .parse::<ARN>()
                    .map_err(|e| anyhow!("Unable to parse provided ARN: {:#?}", e))?
                    .region
                    .ok_or_else(|| anyhow!("Provided ARN does not include an AWS region"))?;

                let aws_info =
                    normalize::aws_connect_info(&mut with_options_map, Some(region.into()))?;
                aws_util::aws::validate_credentials(aws_info, Duration::from_secs(1)).await?;
            }
            Connector::Postgres {
                conn,
                namespace,
                table,
                columns,
                ..
            } => purify_postgres_table(conn, namespace, table, columns).await?,
            Connector::PubNub { .. } => (),
        }

        purify_format(
            format,
            connector,
            &envelope,
            col_names,
            file,
            &config_options,
        )
        .await?;
        if let sql_parser::ast::Envelope::Upsert(_) = envelope {
            // TODO(bwm): this will be removed with the upcoming upsert rationalization
            //
            // The `format` argument is mutated in the call to purify_format, so it must be the
            // original format from the outer envelope. The envelope is just matched against, and
            // can be cloned safely.
            let envelope_dupe = envelope.clone();
            if let sql_parser::ast::Envelope::Upsert(format) = envelope {
                purify_format(
                    format,
                    connector,
                    &envelope_dupe,
                    col_names,
                    None,
                    &config_options,
                )
                .await?;
            }
        }
    }
    if let Statement::CreateSources(CreateSourcesStatement { connector, stmts }) = &mut stmt {
        match connector {
            MultiConnector::Postgres {
                conn,
                publication,
                namespace,
                tables,
            } => {
                let mut create_stmts = Vec::with_capacity(tables.len());
                for table in tables.into_iter() {
                    purify_postgres_table(&conn, &namespace, &table.name, &mut table.columns)
                        .await?;
                    create_stmts.push(CreateSourceStatement {
                        name: UnresolvedObjectName(vec![Ident::from(table.name.as_str())]),
                        col_names: table.columns.iter().map(|c| c.name.clone()).collect(),
                        connector: Connector::Postgres {
                            conn: conn.to_owned(),
                            publication: publication.to_owned(),
                            namespace: namespace.to_owned(),
                            table: table.name.to_owned(),
                            columns: table.columns.clone(),
                        },
                        format: None,
                        with_options: vec![],
                        envelope: Envelope::None,
                        if_not_exists: false,
                        materialized: false,
                    });
                }
                *stmts = create_stmts;
            }
        }
    }
    Ok(stmt)
}

async fn purify_postgres_table(
    conn: &str,
    namespace: &str,
    table: &str,
    columns: &mut Vec<ColumnDef<Raw>>,
) -> Result<(), anyhow::Error> {
    let fetched_columns = postgres_util::table_info(conn, namespace, table)
        .await?
        .schema
        .iter()
        .map(|c| c.to_ast_string())
        .collect::<Vec<String>>()
        .join(", ");
    let (upstream_columns, _constraints) = parse_columns(&format!("({})", fetched_columns))?;
    if columns.is_empty() {
        *columns = upstream_columns;
    } else {
        if columns != &upstream_columns {
            if columns.len() != upstream_columns.len() {
                bail!(
                    "incorrect column specification: {} columns were specified, upstream has {}: {}",
                    columns.len(),
                    upstream_columns.len(),
                    upstream_columns
                        .iter()
                        .map(|u| u.name.to_string())
                        .collect::<Vec<String>>()
                        .join(", ")
                )
            }
            for (c, u) in columns.into_iter().zip(upstream_columns) {
                // By default, Postgres columns are nullable. This means that both
                // of the following column definitions are nullable:
                //     example_col bool
                //     example_col bool NULL
                // Fetched upstream column information, on the other hand, will always
                // include NULL if a column is nullable. In order to compare the user-provided
                // columns with the fetched columns, we need to append a NULL constraint
                // to any user-provided columns that are implicitly NULL.
                if !c.options.contains(&ColumnOptionDef {
                    name: None,
                    option: ColumnOption::NotNull,
                }) && !c.options.contains(&ColumnOptionDef {
                    name: None,
                    option: ColumnOption::Null,
                }) {
                    c.options.push(ColumnOptionDef {
                        name: None,
                        option: ColumnOption::Null,
                    });
                }
                if c != &u {
                    bail!("incorrect column specification: specified column does not match upstream source, specified: {}, upstream: {}", c, u);
                }
            }
        }
    }
    //TODO(petrosagg): attempt to connect with a replication connection and create a temporary
    //                 logical replication slot to ensure the server has the correct WAL settings
    Ok(())
}

async fn purify_format(
    format: &mut Option<Format<Raw>>,
    connector: &mut Connector<Raw>,
    envelope: &Envelope<Raw>,
    col_names: &mut Vec<Ident>,
    file: Option<File>,
    connector_options: &BTreeMap<String, String>,
) -> Result<(), anyhow::Error> {
    match format {
        Some(Format::Avro(schema)) => match schema {
            AvroSchema::CsrUrl {
                url,
                seed,
                with_options: ccsr_options,
            } => {
                let topic = if let Connector::Kafka { topic, .. } = connector {
                    topic
                } else {
                    bail!("Confluent Schema Registry is only supported with Kafka sources")
                };
                if seed.is_none() {
                    let url = url.parse()?;

                    let ccsr_config = task::block_in_place(|| {
                        kafka_util::generate_ccsr_client_config(
                            url,
                            &connector_options,
                            normalize::options(ccsr_options),
                        )
                    })?;

                    let Schema {
                        key_schema,
                        value_schema,
                        ..
                    } = get_remote_avro_schema(ccsr_config, topic.clone()).await?;
                    if matches!(envelope, Envelope::Debezium(DbzMode::Upsert))
                        && key_schema.is_none()
                    {
                        bail!("Key schema is required for ENVELOPE DEBEZIUM UPSERT");
                    }
                    *seed = Some(CsrSeed {
                        key_schema,
                        value_schema,
                    });
                }
            }
            AvroSchema::Schema {
                schema: sql_parser::ast::Schema::File(path),
                with_options,
            } => {
                if matches!(envelope, Envelope::Debezium(DbzMode::Upsert)) {
                    // TODO(bwm): Support key schemas everywhere, something like
                    // https://github.com/MaterializeInc/materialize/pull/6286
                    bail!(
                        "ENVELOPE DEBEZIUM UPSERT can only be used with schemas from \
                           the confluent schema registry, and requires a key schema"
                    );
                }
                let value_schema = tokio::fs::read_to_string(path).await?;
                *schema = AvroSchema::Schema {
                    schema: sql_parser::ast::Schema::Inline(value_schema),
                    with_options: with_options.clone(),
                };
            }
            _ => {
                if matches!(envelope, Envelope::Debezium(DbzMode::Upsert)) {
                    // TODO(bwm): Support key schemas everywhere, something like
                    // https://github.com/MaterializeInc/materialize/pull/6286
                    bail!(
                        "ENVELOPE DEBEZIUM UPSERT can only be used with schemas from \
                           the confluent schema registry, and requires a key schema"
                    );
                }
            }
        },
        Some(Format::Protobuf { schema, .. }) => {
            if let sql_parser::ast::Schema::File(path) = schema {
                let descriptors = tokio::fs::read(path).await?;
                let mut buf = String::new();
                strconv::format_bytes(&mut buf, &descriptors);
                *schema = sql_parser::ast::Schema::Inline(buf);
            }
        }
        Some(Format::Csv {
            header_row,
            delimiter,
            ..
        }) => {
            if *header_row && col_names.is_empty() {
                if let Some(file) = file {
                    let file = tokio::io::BufReader::new(file);
                    let csv_header = file.lines().next_line().await?;
                    match csv_header {
                        Some(csv_header) => {
                            csv_header
                                .split(*delimiter as char)
                                .for_each(|v| col_names.push(Ident::from(v)));
                        }
                        None => bail!("CSV file expected header line, but is empty"),
                    }
                } else {
                    bail!("CSV format with headers only works with file connectors")
                }
            }
        }
        _ => (),
    }
    Ok(())
}

#[derive(Debug)]
pub struct Schema {
    pub key_schema: Option<String>,
    pub value_schema: String,
    pub schema_registry_config: Option<ccsr::ClientConfig>,
    pub confluent_wire_format: bool,
}

async fn get_remote_avro_schema(
    schema_registry_config: ccsr::ClientConfig,
    topic: String,
) -> Result<Schema, anyhow::Error> {
    let ccsr_client = schema_registry_config.clone().build()?;

    let value_schema_name = format!("{}-value", topic);
    let value_schema = ccsr_client
        .get_schema_by_subject(&value_schema_name)
        .await
        .with_context(|| {
            format!(
                "fetching latest schema for subject '{}' from registry",
                value_schema_name
            )
        })?;
    let subject = format!("{}-key", topic);
    let key_schema = ccsr_client.get_schema_by_subject(&subject).await.ok();
    Ok(Schema {
        key_schema: key_schema.map(|s| s.raw),
        value_schema: value_schema.raw,
        schema_registry_config: Some(schema_registry_config),
        confluent_wire_format: true,
    })
}
