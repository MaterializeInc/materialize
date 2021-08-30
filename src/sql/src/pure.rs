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

use std::collections::{BTreeMap, HashMap};
use std::future::Future;

use anyhow::{anyhow, bail, ensure, Context};
use aws_arn::ARN;
use aws_util::aws;
use csv::ReaderBuilder;
use itertools::Itertools;
use tokio::fs::File;
use tokio::io::AsyncBufReadExt;
use tokio::task;
use uuid::Uuid;

use dataflow_types::{ExternalSourceConnector, PostgresSourceConnector, SourceConnector};
use repr::strconv;
use sql_parser::ast::{
    display::AstDisplay, AvroSchema, CreateSourceConnector, CreateSourceFormat,
    CreateSourceStatement, CreateViewsDefinitions, CreateViewsSourceTarget, CreateViewsStatement,
    CsrConnector, CsrSeed, CsvColumns, DbzMode, Envelope, Expr, Format, Ident, ProtobufSchema,
    Query, Raw, RawName, Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins,
    UnresolvedObjectName, Value, ViewDefinition, WithOption, WithOptionValue,
};
use sql_parser::parser::parse_columns;

use crate::catalog::Catalog;
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
pub fn purify(
    catalog: &dyn Catalog,
    mut stmt: Statement<Raw>,
) -> impl Future<Output = Result<Statement<Raw>, anyhow::Error>> {
    // If we're dealing with a CREATE VIEWS statement we need to query the catalog for the
    // corresponding source connector and store it before we enter the async section.
    let source_connector = if let Statement::CreateViews(CreateViewsStatement {
        definitions: CreateViewsDefinitions::Source { name, .. },
        ..
    }) = &stmt
    {
        normalize::unresolved_object_name(name.clone())
            .map_err(anyhow::Error::new)
            .and_then(|name| {
                catalog
                    .resolve_item(&name)
                    .and_then(|item| item.source_connector())
                    .map(|s| s.clone())
                    .map_err(anyhow::Error::new)
            })
    } else {
        Err(anyhow!("SQL statement does not refer to a source"))
    };

    let now = catalog.now();

    async move {
        if let Statement::CreateSource(CreateSourceStatement {
            connector,
            format,
            envelope,
            key_envelope,
            with_options,
            ..
        }) = &mut stmt
        {
            let mut with_options_map = normalize::options(with_options);
            let mut config_options = BTreeMap::new();

            let mut file = None;
            match connector {
                CreateSourceConnector::Kafka { broker, topic, .. } => {
                    if !broker.contains(':') {
                        *broker += ":9092";
                    }

                    // Verify that the provided security options are valid and then test them.
                    config_options = kafka_util::extract_config(&mut with_options_map)?;
                    let consumer = kafka_util::create_consumer(&broker, &topic, &config_options)
                        .await
                        .map_err(|e| {
                            anyhow!(
                                "Cannot create Kafka Consumer for determining start offsets: {}",
                                e
                            )
                        })?;

                    // Translate `kafka_time_offset` to `start_offset`.
                    match kafka_util::lookup_start_offsets(
                        consumer.clone(),
                        &topic,
                        &with_options_map,
                        now,
                    )
                    .await?
                    {
                        Some(start_offsets) => {
                            // Drop `kafka_time_offset`
                            with_options.retain(|val| match val {
                                sql_parser::ast::SqlOption::Value { name, .. } => {
                                    name.as_str() != "kafka_time_offset"
                                }
                                _ => true,
                            });

                            // Add `start_offset`
                            with_options.push(sql_parser::ast::SqlOption::Value {
                                name: sql_parser::ast::Ident::new("start_offset"),
                                value: sql_parser::ast::Value::Array(
                                    start_offsets
                                        .iter()
                                        .map(|offset| Value::Number(offset.to_string()))
                                        .collect(),
                                ),
                            });
                        }
                        _ => {}
                    }
                }
                CreateSourceConnector::AvroOcf { path, .. } => {
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
                CreateSourceConnector::File { path, .. } => {
                    let f = File::open(&path).await?;
                    if f.metadata().await?.is_dir() {
                        bail!("Expected a regular file, but {} is a directory.", path);
                    }
                    file = Some(f);
                }
                CreateSourceConnector::S3 { .. } => {
                    let aws_info = normalize::aws_connect_info(&mut with_options_map, None)?;
                    aws::validate_credentials(aws_info.clone(), aws::AUTH_TIMEOUT).await?;
                }
                CreateSourceConnector::Kinesis { arn } => {
                    let region = arn
                        .parse::<ARN>()
                        .map_err(|e| anyhow!("Unable to parse provided ARN: {:#?}", e))?
                        .region
                        .ok_or_else(|| anyhow!("Provided ARN does not include an AWS region"))?;

                    let aws_info =
                        normalize::aws_connect_info(&mut with_options_map, Some(region.into()))?;
                    aws::validate_credentials(aws_info, aws::AUTH_TIMEOUT).await?;
                }
                CreateSourceConnector::Postgres {
                    conn,
                    publication,
                    slot,
                } => {
                    slot.get_or_insert_with(|| {
                        format!(
                            "materialize_{}",
                            Uuid::new_v4().to_string().replace('-', "")
                        )
                    });

                    // verify that we can connect upstream
                    // TODO(petrosagg): store this info along with the source for better error
                    // detection
                    let _ = postgres_util::publication_info(&conn, &publication).await?;
                }
                CreateSourceConnector::PubNub { .. } => (),
            }

            purify_source_format(format, connector, &envelope, file, &config_options).await?;

            if key_envelope.is_present() && !matches!(format, CreateSourceFormat::KeyValue { .. }) {
                bail!(
                    "INCLUDE KEY requires specifying KEY FORMAT .. VALUE FORMAT, got bare FORMAT"
                );
            }
        }

        if let Statement::CreateViews(CreateViewsStatement { definitions, .. }) = &mut stmt {
            if let CreateViewsDefinitions::Source {
                name: source_name,
                targets,
            } = definitions
            {
                match source_connector? {
                    SourceConnector::External {
                        connector:
                            ExternalSourceConnector::Postgres(PostgresSourceConnector {
                                conn,
                                publication,
                                ..
                            }),
                        ..
                    } => {
                        let pub_info = postgres_util::publication_info(&conn, &publication).await?;

                        // If the user didn't specify targets we'll generate views for all of them
                        let targets = targets.clone().unwrap_or_else(|| {
                            pub_info
                                .iter()
                                .map(|table_info| {
                                    let name = UnresolvedObjectName::qualified(&[
                                        &table_info.namespace,
                                        &table_info.name,
                                    ]);
                                    CreateViewsSourceTarget {
                                        name: name.clone(),
                                        alias: Some(name),
                                    }
                                })
                                .collect()
                        });

                        let mut views = Vec::with_capacity(pub_info.len());

                        // An index from table_name -> schema_name -> table_info
                        let mut pub_info_idx: HashMap<_, HashMap<_, _>> = HashMap::new();
                        for table in pub_info {
                            pub_info_idx
                                .entry(table.name.clone())
                                .or_default()
                                .entry(table.namespace.clone())
                                .or_insert(table);
                        }

                        for target in targets {
                            let name = normalize::unresolved_object_name(target.name.clone())
                                .map_err(anyhow::Error::new)?;

                            let schemas = pub_info_idx.get(&name.item).ok_or_else(|| {
                                anyhow!("table {} does not exist in upstream database", name)
                            })?;

                            let table_info = if let Some(schema) = &name.schema {
                                schemas.get(schema).ok_or_else(|| {
                                    anyhow!("schema {} does not exist in upstream database", schema)
                                })?
                            } else {
                                schemas.values().exactly_one().or_else(|_| {
                                    Err(anyhow!(
                                        "table {} is ambiguous, consider specifying the schema",
                                        name
                                    ))
                                })?
                            };

                            let view_name =
                                target.alias.clone().unwrap_or_else(|| target.name.clone());

                            let col_schema = table_info
                                .schema
                                .iter()
                                .map(|c| c.to_ast_string())
                                .collect::<Vec<String>>()
                                .join(", ");
                            let (columns, _constraints) =
                                parse_columns(&format!("({})", col_schema))?;

                            let mut projection = vec![];
                            for (i, column) in columns.iter().enumerate() {
                                projection.push(SelectItem::Expr {
                                    expr: Expr::Cast {
                                        expr: Box::new(Expr::SubscriptIndex {
                                            expr: Box::new(Expr::Identifier(vec![Ident::new(
                                                "row_data",
                                            )])),
                                            subscript: Box::new(Expr::Value(Value::Number(
                                                // LIST is one based
                                                (i + 1).to_string(),
                                            ))),
                                        }),
                                        data_type: column.data_type.clone(),
                                    },
                                    alias: Some(column.name.clone()),
                                });
                            }

                            let query = Query {
                                ctes: vec![],
                                body: SetExpr::Select(Box::new(Select {
                                    distinct: None,
                                    projection,
                                    from: vec![TableWithJoins {
                                        relation: TableFactor::Table {
                                            name: RawName::Name(source_name.clone()),
                                            alias: None,
                                        },
                                        joins: vec![],
                                    }],
                                    selection: Some(Expr::Op {
                                        op: "=".to_string(),
                                        expr1: Box::new(Expr::Identifier(vec![Ident::new("oid")])),
                                        expr2: Some(Box::new(Expr::Value(Value::Number(
                                            table_info.rel_id.to_string(),
                                        )))),
                                    }),
                                    group_by: vec![],
                                    having: None,
                                    options: vec![],
                                })),
                                order_by: vec![],
                                limit: None,
                                offset: None,
                            };

                            views.push(ViewDefinition {
                                name: view_name,
                                columns: columns.iter().map(|c| c.name.clone()).collect(),
                                with_options: vec![],
                                query,
                            });
                        }
                        *definitions = CreateViewsDefinitions::Literal(views);
                    }
                    SourceConnector::External { connector, .. } => {
                        bail!("cannot generate views from {} sources", connector.name())
                    }
                    SourceConnector::Local { .. } => {
                        bail!("cannot generate views from local sources")
                    }
                }
            }
        }
        Ok(stmt)
    }
}

async fn purify_source_format(
    format: &mut CreateSourceFormat<Raw>,
    connector: &mut CreateSourceConnector,
    envelope: &Envelope,
    file: Option<File>,
    connector_options: &BTreeMap<String, String>,
) -> Result<(), anyhow::Error> {
    if matches!(format, CreateSourceFormat::KeyValue { .. })
        && !matches!(connector, CreateSourceConnector::Kafka { .. })
    {
        bail!("Kafka sources are the only source type that can provide KEY/VALUE formats")
    }

    // the existing semantics of Upsert is that specifying a simple bare format
    // duplicates the format into the key.
    //
    // TODO(bwm): We should either make this the semantics everywhere, or deprecate
    // this.
    if matches!(connector, CreateSourceConnector::Kafka { .. })
        && matches!(envelope, Envelope::Upsert)
        && format.is_simple()
    {
        let value = format.value().map(|f| f.clone());
        if let Some(value) = value {
            *format = CreateSourceFormat::KeyValue {
                key: value.clone(),
                value,
            }
        } else {
            bail!("Upsert requires either a VALUE FORMAT or a bare TEXT or BYTES format");
        };
    }

    match format {
        CreateSourceFormat::None => {}
        CreateSourceFormat::Bare(format) => {
            purify_source_format_single(format, connector, envelope, file, connector_options)
                .await?
        }

        CreateSourceFormat::KeyValue { key, value: val } => {
            ensure!(
                file.is_none(),
                anyhow!("[internal-error] File sources cannot be key-value sources")
            );

            purify_source_format_single(key, connector, envelope, None, connector_options).await?;
            purify_source_format_single(val, connector, envelope, None, connector_options).await?;
        }
    }
    Ok(())
}

async fn purify_source_format_single(
    format: &mut Format<Raw>,
    connector: &mut CreateSourceConnector,
    envelope: &Envelope,
    file: Option<File>,
    connector_options: &BTreeMap<String, String>,
) -> Result<(), anyhow::Error> {
    match format {
        Format::Avro(schema) => match schema {
            AvroSchema::Csr { csr_connector } => {
                purify_csr_connector(connector, connector_options, envelope, csr_connector).await?
            }
            AvroSchema::InlineSchema {
                schema: sql_parser::ast::Schema::File(path),
                with_options,
            } => {
                let file_schema = tokio::fs::read_to_string(path).await?;
                // Explicitly inject `confluent_wire_format = true`, if unset.
                // This, in combination with the catalog migration that sets
                // this option to true for sources created before this option
                // existed, will make it easy to flip the default to `false`
                // in the future, if we like.
                if !with_options
                    .iter()
                    .any(|option| option.key.as_str() == "confluent_wire_format")
                {
                    with_options.push(WithOption {
                        key: Ident::new("confluent_wire_format"),
                        value: Some(WithOptionValue::Value(Value::Boolean(true))),
                    });
                }
                *schema = AvroSchema::InlineSchema {
                    schema: sql_parser::ast::Schema::Inline(file_schema),
                    with_options: with_options.clone(),
                };
            }
            _ => {}
        },
        Format::Protobuf(schema) => match schema {
            ProtobufSchema::Csr { csr_connector } => {
                purify_csr_connector(connector, connector_options, envelope, csr_connector).await?
            }
            ProtobufSchema::InlineSchema { schema, .. } => {
                if let sql_parser::ast::Schema::File(path) = schema {
                    let descriptors = tokio::fs::read(path).await?;
                    let mut buf = String::new();
                    strconv::format_bytes(&mut buf, &descriptors);
                    *schema = sql_parser::ast::Schema::Inline(buf);
                }
            }
        },
        Format::Csv {
            delimiter,
            ref mut columns,
        } => {
            purify_csv(file, connector, *delimiter, columns).await?;
        }
        Format::Bytes | Format::Regex(_) | Format::Json | Format::Text => (),
    }
    Ok(())
}

async fn purify_csr_connector(
    connector: &mut CreateSourceConnector,
    connector_options: &BTreeMap<String, String>,
    envelope: &Envelope,
    csr_connector: &mut CsrConnector<Raw>,
) -> Result<(), anyhow::Error> {
    let topic = if let CreateSourceConnector::Kafka { topic, .. } = connector {
        topic
    } else {
        bail!("Confluent Schema Registry is only supported with Kafka sources")
    };

    let CsrConnector {
        url,
        seed,
        with_options: ccsr_options,
    } = csr_connector;
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
        } = get_remote_csr_schema(ccsr_config, topic.clone()).await?;
        if matches!(envelope, Envelope::Debezium(DbzMode::Upsert)) && key_schema.is_none() {
            bail!("Key schema is required for ENVELOPE DEBEZIUM UPSERT");
        }

        *seed = Some(CsrSeed {
            key_schema,
            value_schema,
        })
    }

    Ok(())
}

pub async fn purify_csv(
    file: Option<File>,
    connector: &CreateSourceConnector,
    delimiter: char,
    columns: &mut CsvColumns,
) -> anyhow::Result<()> {
    if matches!(columns, CsvColumns::Header { .. })
        && !matches!(connector, CreateSourceConnector::File { .. })
    {
        bail_unsupported!("CSV WITH HEADER with non-file sources");
    }

    let first_row = if let Some(file) = file {
        let file = tokio::io::BufReader::new(file);
        let csv_header = file.lines().next_line().await;
        if !delimiter.is_ascii() {
            bail!("CSV delimiter must be ascii");
        }
        match csv_header {
            Ok(Some(csv_header)) => {
                let mut reader = ReaderBuilder::new()
                    .delimiter(delimiter as u8)
                    .has_headers(false)
                    .from_reader(csv_header.as_bytes());

                if let Some(result) = reader.records().next() {
                    match result {
                        Ok(headers) => Some(headers),
                        Err(e) => bail!("Unable to parse header row: {}", e),
                    }
                } else {
                    None
                }
            }
            Ok(None) => {
                if let CsvColumns::Header { names } = columns {
                    if names.is_empty() {
                        bail!(
                            "CSV file expected to have at least one line \
                             to determine column names, but is empty"
                        );
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            Err(e) => {
                // TODO(#7562): support compressed files
                if let CsvColumns::Header { names } = columns {
                    if names.is_empty() {
                        bail!("Cannot determine header by reading CSV file: {}", e);
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
        }
    } else {
        None
    };

    match (&columns, first_row) {
        (CsvColumns::Header { names }, Some(headers)) if names.is_empty() => {
            *columns = CsvColumns::Header {
                names: headers.into_iter().map(Ident::from).collect(),
            };
        }
        (CsvColumns::Header { names }, Some(headers)) => {
            if names.len() != headers.len() {
                bail!(
                    "Named column count ({}) does not match \
                     number of columns discovered ({})",
                    names.len(),
                    headers.len()
                );
            } else if let Some((sql, csv)) = names
                .iter()
                .zip(headers.iter())
                .find(|(sql, csv)| sql.as_str() != &**csv)
            {
                bail!(
                    "Header columns do not match named columns from CREATE SOURCE statement. \
                               First mismatched columns: {} != {}",
                    sql,
                    csv
                );
            }
        }
        (CsvColumns::Header { names }, None) if names.is_empty() => {
            bail!(
                "WITH HEADER requires a way to determine the header row, but file does not exist"
            );
        }
        (CsvColumns::Header { names }, None) => {
            // we don't need to do any verification if we are told the names of the headers
            assert!(
                !names.is_empty(),
                "empty names should be caught in a previous match arm"
            );
        }

        (CsvColumns::Count(n), first_line) => {
            if let Some(columns) = first_line {
                if *n != columns.len() {
                    bail!(
                        "Specified column count (WITH {} COLUMNS) \
                                 does not match number of columns in CSV file ({})",
                        n,
                        columns.len()
                    );
                }
            }
        }
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

async fn get_remote_csr_schema(
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
