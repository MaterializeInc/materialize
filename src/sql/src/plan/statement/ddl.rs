// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Data definition language (DDL).
//!
//! This module houses the handlers for statements that modify the catalog, like
//! `ALTER`, `CREATE`, and `DROP`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::time::{Duration, UNIX_EPOCH};

use anyhow::{anyhow, bail};

use aws_arn::{Resource, ARN};
use dataflow_types::{
    AvroEncoding, AvroOcfEncoding, AvroOcfSinkConnectorBuilder, Consistency, CsvEncoding,
    DataEncoding, Envelope, ExternalSourceConnector, FileSourceConnector,
    KafkaSinkConnectorBuilder, KafkaSourceConnector, KinesisSourceConnector, ProtobufEncoding,
    RegexEncoding, SinkConnectorBuilder, SourceConnector,
};
use expr::GlobalId;
use interchange::avro::{self, DebeziumDeduplicationStrategy, Encoder};
use itertools::Itertools;
use ore::collections::CollectionExt;
use ore::iter::IteratorExt;
use regex::Regex;
use repr::{strconv, RelationDesc, RelationType, ScalarType};
use reqwest::Url;
use rusoto_core::Region;

use crate::ast::display::AstDisplay;
use crate::ast::{
    AlterIndexOptionsList, AlterIndexOptionsStatement, AlterObjectRenameStatement, AvroSchema,
    ColumnOption, Connector, CreateDatabaseStatement, CreateIndexStatement, CreateSchemaStatement,
    CreateSinkStatement, CreateSourceStatement, CreateTableStatement, CreateTypeAs,
    CreateTypeStatement, CreateViewStatement, DataType, DropDatabaseStatement,
    DropObjectsStatement, Expr, Format, Ident, IfExistsBehavior, ObjectName, ObjectType, SqlOption,
    Statement, Value,
};
use crate::catalog::{CatalogItem, CatalogItemType};
use crate::kafka_util;
use crate::names::{DatabaseSpecifier, FullName, SchemaName};
use crate::normalize;
use crate::plan::query::QueryLifetime;
use crate::plan::statement::with_options::aws_connect_info;
use crate::plan::statement::{StatementContext, StatementDesc};
use crate::plan::{
    self, plan_utils, query, AlterIndexLogicalCompactionWindow, Index, LogicalCompactionWindow,
    Params, Plan, Sink, Source, Table, Type, TypeInner, View,
};
use crate::pure::Schema;

pub fn describe_create_database(
    _: &StatementContext,
    _: CreateDatabaseStatement,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_database(
    _: &StatementContext,
    CreateDatabaseStatement {
        name,
        if_not_exists,
    }: CreateDatabaseStatement,
) -> Result<Plan, anyhow::Error> {
    Ok(Plan::CreateDatabase {
        name: normalize::ident(name),
        if_not_exists,
    })
}

pub fn describe_create_schema(
    _: &StatementContext,
    _: CreateSchemaStatement,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_schema(
    scx: &StatementContext,
    CreateSchemaStatement {
        mut name,
        if_not_exists,
    }: CreateSchemaStatement,
) -> Result<Plan, anyhow::Error> {
    if name.0.len() > 2 {
        bail!("schema name {} has more than two components", name);
    }
    let schema_name = normalize::ident(
        name.0
            .pop()
            .expect("names always have at least one component"),
    );
    let database_name = match name.0.pop() {
        None => DatabaseSpecifier::Name(scx.catalog.default_database().into()),
        Some(n) => DatabaseSpecifier::Name(normalize::ident(n)),
    };
    Ok(Plan::CreateSchema {
        database_name,
        schema_name,
        if_not_exists,
    })
}

pub fn describe_create_table(
    _: &StatementContext,
    _: CreateTableStatement,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_table(
    scx: &StatementContext,
    stmt: CreateTableStatement,
) -> Result<Plan, anyhow::Error> {
    let CreateTableStatement {
        name,
        columns,
        constraints,
        with_options,
        if_not_exists,
    } = &stmt;

    if !with_options.is_empty() {
        unsupported!("WITH options");
    }
    if !constraints.is_empty() {
        unsupported!("CREATE TABLE with constraints")
    }

    let names: Vec<_> = columns
        .iter()
        .map(|c| normalize::column_name(c.name.clone()))
        .collect();

    if let Some(dup) = names.iter().duplicates().next() {
        bail!(
            "cannot CREATE TABLE: column \"{}\" specified more than once",
            dup
        );
    }

    // Build initial relation type that handles declared data types
    // and NOT NULL constraints.
    let mut column_types = Vec::with_capacity(columns.len());
    let mut defaults = Vec::with_capacity(columns.len());

    for c in columns {
        let ty = plan::scalar_type_from_sql(scx, &c.data_type)?;
        let mut nullable = true;
        let mut default = Expr::null();
        for option in &c.options {
            match &option.option {
                ColumnOption::NotNull => nullable = false,
                ColumnOption::Default(expr) => {
                    // Ensure expression can be planned and yields the correct
                    // type.
                    query::plan_default_expr(scx, expr, &ty)?;
                    default = expr.clone();
                }
                other => unsupported!(format!("CREATE TABLE with column constraint: {}", other)),
            }
        }
        column_types.push(ty.nullable(nullable));
        defaults.push(default);
    }

    let typ = RelationType::new(column_types);

    let name = scx.allocate_name(normalize::object_name(name.clone())?);
    let desc = RelationDesc::new(typ, names.into_iter().map(Some));

    let create_sql = normalize::create_statement(&scx, Statement::CreateTable(stmt.clone()))?;
    let table = Table {
        create_sql,
        desc,
        defaults,
    };
    Ok(Plan::CreateTable {
        name,
        table,
        if_not_exists: *if_not_exists,
    })
}

pub fn describe_create_source(
    _: &StatementContext,
    _: CreateSourceStatement,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_source(
    scx: &StatementContext,
    stmt: CreateSourceStatement,
) -> Result<Plan, anyhow::Error> {
    let CreateSourceStatement {
        name,
        col_names,
        connector,
        with_options,
        format,
        envelope,
        if_not_exists,
        materialized,
    } = &stmt;
    let get_encoding = |format: &Option<Format>| {
        let format = format
            .as_ref()
            .ok_or_else(|| anyhow!("Source format must be specified"))?;

        Ok(match format {
            Format::Bytes => DataEncoding::Bytes,
            Format::Avro(schema) => {
                let Schema {
                    key_schema,
                    value_schema,
                    schema_registry_config,
                } = match schema {
                    // TODO(jldlaughlin): we need a way to pass in primary key information
                    // when building a source from a string or file.
                    AvroSchema::Schema(sql_parser::ast::Schema::Inline(schema)) => Schema {
                        key_schema: None,
                        value_schema: schema.clone(),
                        schema_registry_config: None,
                    },
                    AvroSchema::Schema(sql_parser::ast::Schema::File(_)) => {
                        unreachable!("File schema should already have been inlined")
                    }
                    AvroSchema::CsrUrl {
                        url,
                        seed,
                        with_options: ccsr_options,
                    } => {
                        let url: Url = url.parse()?;
                        let kafka_options =
                            kafka_util::extract_config(&mut normalize::options(with_options))?;
                        let ccsr_config = kafka_util::generate_ccsr_client_config(
                            url,
                            &kafka_options,
                            normalize::options(ccsr_options),
                        )?;

                        if let Some(seed) = seed {
                            Schema {
                                key_schema: seed.key_schema.clone(),
                                value_schema: seed.value_schema.clone(),
                                schema_registry_config: Some(ccsr_config),
                            }
                        } else {
                            unreachable!("CSR seed resolution should already have been called")
                        }
                    }
                };

                DataEncoding::Avro(AvroEncoding {
                    key_schema,
                    value_schema,
                    schema_registry_config,
                })
            }
            Format::Protobuf {
                message_name,
                schema,
            } => {
                let descriptors = match schema {
                    sql_parser::ast::Schema::Inline(bytes) => strconv::parse_bytes(&bytes)?,
                    sql_parser::ast::Schema::File(_) => {
                        unreachable!("File schema should already have been inlined")
                    }
                };

                DataEncoding::Protobuf(ProtobufEncoding {
                    descriptors,
                    message_name: message_name.to_owned(),
                })
            }
            Format::Regex(regex) => {
                let regex = Regex::new(regex)?;
                DataEncoding::Regex(RegexEncoding { regex })
            }
            Format::Csv {
                header_row,
                n_cols,
                delimiter,
            } => {
                let n_cols = if col_names.is_empty() {
                    match n_cols {
                        Some(n) => *n,
                        None => bail!(
                            "Cannot determine number of columns in CSV source; specify using \
                            CREATE SOURCE...FORMAT CSV WITH X COLUMNS"
                        ),
                    }
                } else {
                    col_names.len()
                };
                DataEncoding::Csv(CsvEncoding {
                    header_row: *header_row,
                    n_cols,
                    delimiter: match *delimiter as u32 {
                        0..=127 => *delimiter as u8,
                        _ => bail!("CSV delimiter must be an ASCII character"),
                    },
                })
            }
            Format::Json => unsupported!("JSON sources"),
            Format::Text => DataEncoding::Text,
        })
    };

    let mut with_options = normalize::options(with_options);

    let mut consistency = Consistency::RealTime;
    let mut ts_frequency = Duration::from_secs(1);

    let (external_connector, mut encoding) = match connector {
        Connector::Kafka { broker, topic, .. } => {
            let config_options = kafka_util::extract_config(&mut with_options)?;

            consistency = match with_options.remove("consistency") {
                None => Consistency::RealTime,
                Some(Value::String(topic)) => Consistency::BringYourOwn(topic),
                Some(_) => bail!("consistency must be a string"),
            };

            let group_id_prefix = match with_options.remove("group_id_prefix") {
                None => None,
                Some(Value::String(s)) => Some(s),
                Some(_) => bail!("group_id_prefix must be a string"),
            };

            ts_frequency = extract_timestamp_frequency_option(&mut with_options)?;

            // THIS IS EXPERIMENTAL - DO NOT DOCUMENT IT
            // until we have had time to think about what the right UX/design is on a non-urgent timeline!
            // In particular, we almost certainly want the offsets to be specified per-partition.
            // The other major caveat is that by using this feature, you are opting in to
            // not using updates or deletes in CDC sources, and accepting panics if that constraint is violated.
            let start_offset_err = "start_offset must be a nonnegative integer";
            let start_offset = match with_options.remove("start_offset") {
                None => 0,
                Some(Value::Number(n)) => match n.parse::<i64>() {
                    Ok(n) if n >= 0 => n,
                    _ => bail!(start_offset_err),
                },
                Some(_) => bail!(start_offset_err),
            };

            if start_offset != 0 && consistency != Consistency::RealTime {
                bail!("`start_offset` is not yet implemented for BYO consistency sources.")
            }

            let enable_caching = match with_options.remove("cache") {
                None => false,
                Some(Value::Boolean(b)) => b,
                Some(_) => bail!("cache must be a bool!"),
            };

            if enable_caching && consistency != Consistency::RealTime {
                unsupported!("BYO source caching")
            }

            let mut start_offsets = HashMap::new();
            start_offsets.insert(0, start_offset);

            let connector = ExternalSourceConnector::Kafka(KafkaSourceConnector {
                addrs: broker.parse()?,
                topic: topic.clone(),
                config_options,
                start_offsets,
                group_id_prefix,
                cluster_id: scx.catalog.config().cluster_id,
                enable_caching,
                cached_files: None,
            });
            let encoding = get_encoding(format)?;
            (connector, encoding)
        }
        Connector::Kinesis { arn, .. } => {
            let arn: ARN = match arn.parse() {
                Ok(arn) => arn,
                Err(e) => bail!("Unable to parse provided ARN: {:#?}", e),
            };
            let stream_name = match arn.resource {
                Resource::Path(path) => {
                    if let Some(path) = path.strip_prefix("stream/") {
                        path.to_owned()
                    } else {
                        bail!("Unable to parse stream name from resource path: {}", path);
                    }
                }
                _ => unsupported!(format!("AWS Resource type: {:#?}", arn.resource)),
            };

            let region: Region = match arn.region {
                Some(region) => match region.parse() {
                    Ok(region) => {
                        // ignore the endpoint option if we're pointing at a
                        // valid, non-custom AWS region
                        with_options.remove("endpoint");
                        region
                    }
                    Err(e) => {
                        // Region's fromstr doesn't support parsing custom regions.
                        // If a Kinesis stream's ARN indicates it exists in a custom
                        // region, support it iff a valid endpoint for the stream
                        // is also provided.
                        match with_options.remove("endpoint") {
                            Some(Value::String(endpoint)) => Region::Custom {
                                name: region,
                                endpoint,
                            },
                            _ => bail!(
                                "Unable to parse AWS region: {}. If providing a custom \
                                        region, an `endpoint` option must also be provided",
                                e
                            ),
                        }
                    }
                },
                None => bail!("Provided ARN does not include an AWS region"),
            };

            let connector = ExternalSourceConnector::Kinesis(KinesisSourceConnector {
                stream_name,
                aws_info: aws_connect_info(&mut with_options, Some(region))?,
            });
            let encoding = get_encoding(format)?;
            (connector, encoding)
        }
        Connector::File { path, .. } => {
            let tail = match with_options.remove("tail") {
                None => false,
                Some(Value::Boolean(b)) => b,
                Some(_) => bail!("tail must be a boolean"),
            };
            consistency = match with_options.remove("consistency") {
                None => Consistency::RealTime,
                Some(Value::String(topic)) => Consistency::BringYourOwn(topic),
                Some(_) => bail!("consistency must be a string"),
            };
            ts_frequency = extract_timestamp_frequency_option(&mut with_options)?;

            let connector = ExternalSourceConnector::File(FileSourceConnector {
                path: path.clone().into(),
                tail,
            });
            let encoding = get_encoding(format)?;
            (connector, encoding)
        }
        Connector::AvroOcf { path, .. } => {
            let tail = match with_options.remove("tail") {
                None => false,
                Some(Value::Boolean(b)) => b,
                Some(_) => bail!("tail must be a boolean"),
            };
            consistency = match with_options.remove("consistency") {
                None => Consistency::RealTime,
                Some(Value::String(topic)) => Consistency::BringYourOwn(topic),
                Some(_) => bail!("consistency must be a string"),
            };

            ts_frequency = extract_timestamp_frequency_option(&mut with_options)?;

            let connector = ExternalSourceConnector::AvroOcf(FileSourceConnector {
                path: path.clone().into(),
                tail,
            });
            if format.is_some() {
                bail!("avro ocf sources cannot specify a format");
            }
            let reader_schema = match with_options
                .remove("reader_schema")
                .expect("purification guarantees presence of reader_schema")
            {
                Value::String(s) => s,
                _ => bail!("reader_schema option must be a string"),
            };
            let encoding = DataEncoding::AvroOcf(AvroOcfEncoding { reader_schema });
            (connector, encoding)
        }
    };

    // TODO (materialize#2537): cleanup format validation
    // Avro format validation is different for the Debezium envelope
    // vs the Upsert envelope.
    //
    // For the Debezium envelope, the key schema is not meant to be
    // used to decode records; it is meant to be a subset of the
    // value schema so we can identify what the primary key is.
    //
    // When using the Upsert envelope, we delete the key schema
    // from the value encoding because the key schema is not
    // necessarily a subset of the value schema. Also, we shift
    // the key schema, if it exists, over to the value schema position
    // in the Upsert envelope's key_format so it can be validated like
    // a schema used to decode records.

    // TODO: remove bails as more support for upsert is added.
    let envelope = match &envelope {
        sql_parser::ast::Envelope::None => dataflow_types::Envelope::None,
        sql_parser::ast::Envelope::Debezium => {
            let dedup_strat = match with_options.remove("deduplication") {
                None => DebeziumDeduplicationStrategy::Ordered,
                Some(Value::String(s)) => {
                    match s.as_str() {
                        "full" => DebeziumDeduplicationStrategy::Full,
                        "ordered" => DebeziumDeduplicationStrategy::Ordered,
                        "full_in_range" => {
                            match (
                                with_options.remove("deduplication_start"),
                                with_options.remove("deduplication_end"),
                            ) {
                                (Some(Value::String(start)), Some(Value::String(end))) => {
                                    let deduplication_pad_start = match with_options.remove("deduplication_pad_start") {
                                        Some(Value::String(start)) => Some(start),
                                        Some(v) => bail!("Expected string for deduplication_pad_start, got: {:?}", v),
                                        None => None
                                    };
                                    DebeziumDeduplicationStrategy::full_in_range(
                                        &start,
                                        &end,
                                        deduplication_pad_start.as_deref(),
                                    )
                                    .map_err(|e| {
                                        anyhow!("Unable to create deduplication strategy: {}", e)
                                    })?
                                }
                                (_, _) => bail!(
                                    "deduplication full_in_range requires both \
                                 'deduplication_start' and 'deduplication_end' parameters"
                                ),
                            }
                        }
                        _ => bail!(
                            "deduplication must be one of 'ordered' 'full', or 'full_in_range'."
                        ),
                    }
                }
                _ => bail!("deduplication must be one of 'ordered', 'full' or 'full_in_range'."),
            };
            dataflow_types::Envelope::Debezium(dedup_strat)
        }
        sql_parser::ast::Envelope::Upsert(key_format) => match connector {
            Connector::Kafka { .. } => {
                let mut key_encoding = if key_format.is_some() {
                    get_encoding(key_format)?
                } else {
                    encoding.clone()
                };
                match &mut key_encoding {
                    DataEncoding::Avro(AvroEncoding {
                        key_schema,
                        value_schema,
                        ..
                    }) => {
                        if key_schema.is_some() {
                            *value_schema = key_schema.take().unwrap();
                        }
                    }
                    DataEncoding::Bytes | DataEncoding::Text => {}
                    _ => unsupported!("format for upsert key"),
                }
                dataflow_types::Envelope::Upsert(key_encoding)
            }
            _ => unsupported!("upsert envelope for non-Kafka sources"),
        },
        sql_parser::ast::Envelope::CdcV2 => {
            scx.require_experimental_mode("ENVELOPE MATERIALIZE")?;
            if let Connector::AvroOcf { .. } = connector {
                // TODO[btv] - there is no fundamental reason not to support this eventually,
                // but OCF goes through a separate pipeline that it hasn't been implemented for.
                unsupported!("ENVELOPE MATERIALIZE over OCF (Avro files)")
            }
            match format {
                Some(Format::Avro(_)) => {}
                _ => unsupported!("non-Avro-encoded ENVELOPE MATERIALIZE"),
            }
            dataflow_types::Envelope::CdcV2
        }
    };

    if let dataflow_types::Envelope::Upsert(key_encoding) = &envelope {
        match &mut encoding {
            DataEncoding::Avro(AvroEncoding { key_schema, .. }) => {
                *key_schema = None;
            }
            DataEncoding::Bytes | DataEncoding::Text => {
                if let DataEncoding::Avro(_) = &key_encoding {
                    unsupported!("Avro key for this format");
                }
            }
            _ => unsupported!("upsert envelope for this format"),
        }
    }

    let mut desc = encoding.desc(&envelope)?;
    let ignore_source_keys = match with_options.remove("ignore_source_keys") {
        None => false,
        Some(Value::Boolean(b)) => b,
        Some(_) => bail!("ignore_source_keys must be a boolean"),
    };
    if ignore_source_keys {
        desc = desc.without_keys();
    }

    desc = plan_utils::maybe_rename_columns(format!("source {}", name), desc, &col_names)?;

    // TODO(benesch): the available metadata columns should not depend
    // on the format.
    //
    // TODO(brennan): They should not depend on the envelope either. Figure out a way to
    // make all of this more tasteful.
    match (&encoding, &envelope) {
        (DataEncoding::Avro { .. }, _)
        | (DataEncoding::Protobuf { .. }, _)
        | (_, Envelope::Debezium(_)) => (),
        _ => {
            for (name, ty) in external_connector.metadata_columns() {
                desc = desc.with_column(name, ty);
            }
        }
    }

    let if_not_exists = *if_not_exists;
    let materialized = *materialized;
    let name = scx.allocate_name(normalize::object_name(name.clone())?);
    let create_sql = normalize::create_statement(&scx, Statement::CreateSource(stmt))?;

    let source = Source {
        create_sql,
        connector: SourceConnector::External {
            connector: external_connector,
            encoding,
            envelope,
            consistency,
            ts_frequency,
        },
        desc,
    };

    if !with_options.is_empty() {
        bail!(
            "unexpected parameters for CREATE SOURCE: {}",
            with_options.keys().join(",")
        )
    }

    Ok(Plan::CreateSource {
        name,
        source,
        if_not_exists,
        materialized,
    })
}

pub fn describe_create_view(
    _: &StatementContext,
    _: CreateViewStatement,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_view(
    scx: &StatementContext,
    mut stmt: CreateViewStatement,
    params: &Params,
) -> Result<Plan, anyhow::Error> {
    let create_sql = normalize::create_statement(scx, Statement::CreateView(stmt.clone()))?;
    let CreateViewStatement {
        name,
        columns,
        query,
        temporary,
        materialized,
        if_exists,
        with_options,
    } = &mut stmt;
    if !with_options.is_empty() {
        unsupported!("WITH options");
    }
    let name = if *temporary {
        scx.allocate_temporary_name(normalize::object_name(name.to_owned())?)
    } else {
        scx.allocate_name(normalize::object_name(name.to_owned())?)
    };
    let replace = if *if_exists == IfExistsBehavior::Replace {
        if let Ok(item) = scx.catalog.resolve_item(&name.clone().into()) {
            let cascade = false;
            plan_drop_item(scx, ObjectType::View, item, cascade)?
        } else {
            None
        }
    } else {
        None
    };
    let (mut relation_expr, mut desc, finishing) =
        query::plan_root_query(scx, query.clone(), QueryLifetime::Static)?;
    relation_expr.bind_parameters(&params)?;
    //TODO: materialize#724 - persist finishing information with the view?
    relation_expr.finish(finishing);
    let relation_expr = relation_expr.decorrelate();
    desc = plan_utils::maybe_rename_columns(format!("view {}", name), desc, columns)?;
    let temporary = *temporary;
    let materialize = *materialized; // Normalize for `raw_sql` below.
    let if_not_exists = *if_exists == IfExistsBehavior::Skip;
    Ok(Plan::CreateView {
        name,
        view: View {
            create_sql,
            expr: relation_expr,
            column_names: desc.iter_names().map(|n| n.cloned()).collect(),
            temporary,
        },
        replace,
        materialize,
        if_not_exists,
    })
}

fn kafka_sink_builder(
    format: Option<Format>,
    with_options: &mut BTreeMap<String, Value>,
    broker: String,
    topic_prefix: String,
    desc: RelationDesc,
    topic_suffix: String,
    key_indices: Option<Vec<usize>>,
) -> Result<SinkConnectorBuilder, anyhow::Error> {
    let (schema_registry_url, ccsr_with_options) = match format {
        Some(Format::Avro(AvroSchema::CsrUrl {
            url,
            seed,
            with_options,
        })) => {
            if seed.is_some() {
                bail!("SEED option does not make sense with sinks");
            }
            (url.parse::<Url>()?, normalize::options(&with_options))
        }
        _ => unsupported!("non-confluent schema registry avro sinks"),
    };

    let broker_addrs = broker.parse()?;

    let include_consistency = match with_options.remove("consistency") {
        Some(Value::Boolean(b)) => b,
        None => false,
        Some(_) => bail!("consistency must be a boolean"),
    };

    let encoder = Encoder::new(desc, include_consistency, key_indices.clone());
    let value_schema = encoder.writer_schema().canonical_form();
    let key_schema = encoder
        .key_writer_schema()
        .map(|key_schema| key_schema.canonical_form());

    // Use the user supplied value for replication factor, or default to 1
    let replication_factor = match with_options.remove("replication_factor") {
        None => 1,
        Some(Value::Number(n)) => n.parse::<u32>()?,
        Some(_) => bail!("replication factor for sink topics has to be a positive integer"),
    };

    if replication_factor == 0 {
        bail!("replication factor for sink topics has to be greater than zero");
    }

    let consistency_value_schema = if include_consistency {
        Some(avro::get_debezium_transaction_schema().canonical_form())
    } else {
        None
    };

    let config_options = kafka_util::extract_config(with_options)?;
    let ccsr_config = kafka_util::generate_ccsr_client_config(
        schema_registry_url.clone(),
        &config_options,
        ccsr_with_options,
    )?;
    Ok(SinkConnectorBuilder::Kafka(KafkaSinkConnectorBuilder {
        broker_addrs,
        schema_registry_url,
        value_schema,
        topic_prefix,
        topic_suffix,
        replication_factor,
        fuel: 10000,
        consistency_value_schema,
        config_options,
        ccsr_config,
        key_indices,
        key_schema,
    }))
}

fn avro_ocf_sink_builder(
    format: Option<Format>,
    path: String,
    file_name_suffix: String,
) -> Result<SinkConnectorBuilder, anyhow::Error> {
    if format.is_some() {
        bail!("avro ocf sinks cannot specify a format");
    }

    let path = PathBuf::from(path);

    if path.is_dir() {
        bail!("avro ocf sink cannot write to a directory");
    }

    Ok(SinkConnectorBuilder::AvroOcf(AvroOcfSinkConnectorBuilder {
        path,
        file_name_suffix,
    }))
}

pub fn describe_create_sink(
    _: &StatementContext,
    _: CreateSinkStatement,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_sink(
    scx: &StatementContext,
    stmt: CreateSinkStatement,
) -> Result<Plan, anyhow::Error> {
    let create_sql = normalize::create_statement(scx, Statement::CreateSink(stmt.clone()))?;
    let CreateSinkStatement {
        name,
        from,
        connector,
        with_options,
        format,
        with_snapshot,
        as_of,
        if_not_exists,
    } = stmt;
    let name = scx.allocate_name(normalize::object_name(name)?);
    let from = scx.resolve_item(from)?;
    let suffix = format!(
        "{}-{}",
        scx.catalog
            .config()
            .startup_time
            .duration_since(UNIX_EPOCH)?
            .as_secs(),
        scx.catalog.config().nonce
    );

    let mut with_options = normalize::options(&with_options);

    let as_of = as_of.map(|e| query::eval_as_of(scx, e)).transpose()?;
    let connector_builder = match connector {
        Connector::File { .. } => unsupported!("file sinks"),
        Connector::Kafka { broker, topic, key } => {
            let desc = from.desc()?;
            let key_indices = if let Some(key) = key {
                let key = key
                    .into_iter()
                    .map(normalize::column_name)
                    .collect::<Vec<_>>();
                let mut uniq = HashSet::new();
                for col in key.iter() {
                    if !uniq.insert(col) {
                        bail!("Repeated column name in sink key: {}", col);
                    }
                }
                let indices = key
                    .into_iter()
                    .map(|col| -> anyhow::Result<usize> {
                        let name_idx = desc
                            .get_by_name(&col)
                            .map(|(idx, _type)| idx)
                            .ok_or_else(|| anyhow!("No such column: {}", col))?;
                        if desc.get_unambiguous_name(name_idx).is_none() {
                            bail!("Ambiguous column: {}", col);
                        }
                        Ok(name_idx)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Some(indices)
            } else {
                None
            };
            kafka_sink_builder(
                format,
                &mut with_options,
                broker,
                topic,
                desc.clone(),
                suffix,
                key_indices,
            )?
        }
        Connector::Kinesis { .. } => unsupported!("Kinesis sinks"),
        Connector::AvroOcf { path } => avro_ocf_sink_builder(format, path, suffix)?,
    };

    if !with_options.is_empty() {
        bail!(
            "unexpected parameters for CREATE SINK: {}",
            with_options.keys().join(",")
        )
    }

    Ok(Plan::CreateSink {
        name,
        sink: Sink {
            create_sql,
            from: from.id(),
            connector_builder,
        },
        with_snapshot,
        as_of,
        if_not_exists,
    })
}

pub fn describe_create_index(
    _: &StatementContext,
    _: CreateIndexStatement,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_index(
    scx: &StatementContext,
    mut stmt: CreateIndexStatement,
) -> Result<Plan, anyhow::Error> {
    let CreateIndexStatement {
        name,
        on_name,
        key_parts,
        if_not_exists,
    } = &mut stmt;
    let on = scx.resolve_item(on_name.clone())?;

    if CatalogItemType::View != on.item_type()
        && CatalogItemType::Source != on.item_type()
        && CatalogItemType::Table != on.item_type()
    {
        bail!(
            "index cannot be created on {} because it is a {}",
            on.name(),
            on.item_type()
        )
    }

    let on_desc = on.desc()?;

    let filled_key_parts = match key_parts {
        Some(kp) => kp.to_vec(),
        None => {
            // `key_parts` is None if we're creating a "default" index, i.e.
            // creating the index as if the index had been created alongside the
            // view source, e.g. `CREATE MATERIALIZED...`
            on.desc()?
                .typ()
                .default_key()
                .iter()
                .map(|i| match on_desc.get_unambiguous_name(*i) {
                    Some(n) => Expr::Identifier(vec![Ident::new(n.to_string())]),
                    _ => Expr::Value(Value::Number((i + 1).to_string())),
                })
                .collect()
        }
    };
    let keys = query::plan_index_exprs(scx, on_desc, filled_key_parts.clone())?;

    let index_name = if let Some(name) = name {
        FullName {
            database: on.name().database.clone(),
            schema: on.name().schema.clone(),
            item: normalize::ident(name.clone()),
        }
    } else {
        let mut idx_name_base = on.name().clone();
        if key_parts.is_none() {
            // We're trying to create the "default" index.
            idx_name_base.item += "_primary_idx";
        } else {
            // Use PG schema for automatically naming indexes:
            // `<table>_<_-separated indexed expressions>_idx`
            let index_name_col_suffix = keys
                .iter()
                .map(|k| match k {
                    expr::ScalarExpr::Column(i) => match on_desc.get_unambiguous_name(*i) {
                        Some(col_name) => col_name.to_string(),
                        None => format!("{}", i + 1),
                    },
                    _ => "expr".to_string(),
                })
                .join("_");
            idx_name_base.item += &format!("_{}_idx", index_name_col_suffix);
            idx_name_base.item = normalize::ident(Ident::new(idx_name_base.item))
        }

        let mut index_name = idx_name_base.clone();
        let mut i = 0;

        let schema = SchemaName {
            database: on.name().database.clone(),
            schema: on.name().schema.clone(),
        };
        let mut cat_schema_iter = scx.catalog.list_items(&schema);

        // Search for an unused version of the name unless `if_not_exists`.
        while cat_schema_iter.any(|i| *i.name() == index_name) && !*if_not_exists {
            i += 1;
            index_name = idx_name_base.clone();
            index_name.item += &i.to_string();
            cat_schema_iter = scx.catalog.list_items(&schema);
        }

        index_name
    };

    // Normalize `stmt`.
    *name = Some(Ident::new(index_name.item.clone()));
    *key_parts = Some(filled_key_parts);
    let if_not_exists = *if_not_exists;
    let create_sql = normalize::create_statement(scx, Statement::CreateIndex(stmt))?;

    Ok(Plan::CreateIndex {
        name: index_name,
        index: Index {
            create_sql,
            on: on.id(),
            keys,
        },
        if_not_exists,
    })
}

pub fn describe_create_type(
    _: &StatementContext,
    _: CreateTypeStatement,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_type(
    scx: &StatementContext,
    stmt: CreateTypeStatement,
) -> Result<Plan, anyhow::Error> {
    let create_sql = normalize::create_statement(scx, Statement::CreateType(stmt.clone()))?;
    let CreateTypeStatement {
        name,
        as_type,
        with_options,
    } = stmt;

    let mut with_options = normalize::option_objects(&with_options);

    let option_keys = match as_type {
        CreateTypeAs::List => vec!["element_type"],
        CreateTypeAs::Map => vec!["key_type", "value_type"],
    };

    let mut ids = vec![];
    for key in option_keys {
        let item_name = match with_options.remove(&key.to_string()) {
            Some(SqlOption::DataType { data_type, .. }) => match data_type {
                DataType::Other { name, typ_mod } => {
                    if !typ_mod.is_empty() {
                        bail!(
                            "CREATE TYPE ... AS {}option \"{}\" cannot accept type modifier on \
                            {}, you must use the default type",
                            as_type,
                            key,
                            name
                        )
                    }
                    query::canonicalize_type_name_internal(&name)
                }
                d => bail!(
                    "CREATE TYPE ... AS {}option \"{}\" can only use named data types, but \
                    found unnamed data type {}. Use CREATE TYPE to create a named type first",
                    as_type,
                    key,
                    d.to_ast_string(),
                ),
            },
            Some(_) => bail!("{} must be a data type", key),
            None => bail!("{} parameter required", key),
        };
        let item = scx
            .catalog
            .resolve_item(&normalize::object_name(item_name.clone())?)?;
        let item_id = item.id();
        if scx
            .catalog
            .try_get_lossy_scalar_type_by_id(&item_id)
            .is_none()
        {
            bail!(
                "{} must be of class type, but received {} which is of class {}",
                key,
                item.name(),
                item.item_type()
            );
        }
        ids.push(item_id);
    }

    if !with_options.is_empty() {
        bail!(
            "unexpected parameters for CREATE TYPE: {}",
            with_options.keys().join(",")
        )
    }

    let name = scx.allocate_name(normalize::object_name(name)?);
    if scx.catalog.item_exists(&name) {
        bail!("catalog item \"{}\" already exists", name.to_string());
    }

    let inner = match as_type {
        CreateTypeAs::List => TypeInner::List {
            element_id: ids.remove(0),
        },
        CreateTypeAs::Map => {
            let key_id = ids.remove(0);
            match scx.catalog.try_get_lossy_scalar_type_by_id(&key_id) {
                Some(ScalarType::String) => {}
                Some(t) => bail!(
                    "key_type must be text, got {}",
                    scx.humanize_scalar_type(&t)
                ),
                None => unreachable!("already guaranteed id correlates to a type"),
            }

            TypeInner::Map {
                key_id,
                value_id: ids.remove(0),
            }
        }
    };

    assert!(ids.is_empty());

    Ok(Plan::CreateType {
        name,
        typ: Type { create_sql, inner },
    })
}

fn extract_timestamp_frequency_option(
    with_options: &mut BTreeMap<String, Value>,
) -> Result<Duration, anyhow::Error> {
    match with_options.remove("timestamp_frequency_ms") {
        None => Ok(Duration::from_secs(1)),
        Some(Value::Number(n)) => match n.parse::<u64>() {
            Ok(n) => Ok(Duration::from_millis(n)),
            _ => bail!("timestamp_frequency_ms must be an u64"),
        },
        Some(_) => bail!("timestamp_frequency_ms must be an u64"),
    }
}

pub fn describe_drop_database(
    _: &StatementContext,
    _: DropDatabaseStatement,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_drop_database(
    scx: &StatementContext,
    DropDatabaseStatement { name, if_exists }: DropDatabaseStatement,
) -> Result<Plan, anyhow::Error> {
    let name = match scx.resolve_database_ident(name) {
        Ok(database) => database.name().into(),
        Err(_) if if_exists => {
            // TODO(benesch): generate a notice indicating that the database
            // does not exist.
            //
            // TODO(benesch): adjust the type here so we can more clearly
            // indicate that we don't want to drop any database at all.
            String::new()
        }
        Err(err) => return Err(err.into()),
    };
    Ok(Plan::DropDatabase { name })
}

pub fn describe_drop_objects(
    _: &StatementContext,
    _: DropObjectsStatement,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_drop_objects(
    scx: &StatementContext,
    DropObjectsStatement {
        object_type,
        if_exists,
        names,
        cascade,
    }: DropObjectsStatement,
) -> Result<Plan, anyhow::Error> {
    match object_type {
        ObjectType::Schema => plan_drop_schema(scx, if_exists, names, cascade),
        ObjectType::Source
        | ObjectType::Table
        | ObjectType::View
        | ObjectType::Index
        | ObjectType::Sink
        | ObjectType::Type => plan_drop_items(scx, object_type, if_exists, names, cascade),
        ObjectType::Object => unreachable!("cannot drop generic OBJECT, must provide object type"),
    }
}

pub fn plan_drop_schema(
    scx: &StatementContext,
    if_exists: bool,
    names: Vec<ObjectName>,
    cascade: bool,
) -> Result<Plan, anyhow::Error> {
    if names.len() != 1 {
        unsupported!("DROP SCHEMA with multiple schemas");
    }
    match scx.resolve_schema(names.into_element()) {
        Ok(schema) => {
            if let DatabaseSpecifier::Ambient = schema.name().database {
                bail!(
                    "cannot drop schema {} because it is required by the database system",
                    schema.name()
                );
            }
            let mut items = scx.catalog.list_items(schema.name());
            if !cascade && items.next().is_some() {
                bail!(
                    "schema '{}' cannot be dropped without CASCADE while it contains objects",
                    schema.name(),
                );
            }
            Ok(Plan::DropSchema {
                name: schema.name().clone(),
            })
        }
        Err(_) if if_exists => {
            // TODO(benesch): generate a notice indicating that the
            // database does not exist.
            // TODO(benesch): adjust the types here properly, rather than making
            // up a nonexistent database.
            Ok(Plan::DropSchema {
                name: SchemaName {
                    database: DatabaseSpecifier::Ambient,
                    schema: "noexist".into(),
                },
            })
        }
        Err(e) => Err(e.into()),
    }
}

pub fn plan_drop_items(
    scx: &StatementContext,
    object_type: ObjectType,
    if_exists: bool,
    names: Vec<ObjectName>,
    cascade: bool,
) -> Result<Plan, anyhow::Error> {
    let items = names
        .into_iter()
        .map(|n| scx.resolve_item(n))
        .collect::<Vec<_>>();
    let mut ids = vec![];
    for item in items {
        match item {
            Ok(item) => ids.extend(plan_drop_item(scx, object_type, item, cascade)?),
            Err(_) if if_exists => {
                // TODO(benesch): generate a notice indicating this
                // item does not exist.
            }
            Err(err) => return Err(err.into()),
        }
    }
    Ok(Plan::DropItems {
        items: ids,
        ty: object_type,
    })
}

pub fn plan_drop_item(
    scx: &StatementContext,
    object_type: ObjectType,
    catalog_entry: &dyn CatalogItem,
    cascade: bool,
) -> Result<Option<GlobalId>, anyhow::Error> {
    if catalog_entry.id().is_system() {
        bail!(
            "cannot drop item {} because it is required by the database system",
            catalog_entry.name(),
        );
    }
    if object_type != catalog_entry.item_type() {
        bail!("{} is not of type {}", catalog_entry.name(), object_type);
    }
    if !cascade {
        for id in catalog_entry.used_by() {
            let dep = scx.catalog.get_item_by_id(id);
            match dep.item_type() {
                CatalogItemType::Table
                | CatalogItemType::Source
                | CatalogItemType::View
                | CatalogItemType::Sink
                | CatalogItemType::Type => {
                    bail!(
                        "cannot drop {}: still depended upon by catalog item '{}'",
                        catalog_entry.name(),
                        dep.name()
                    );
                }
                CatalogItemType::Index => (),
            }
        }
    }
    Ok(Some(catalog_entry.id()))
}

pub fn describe_alter_index_options(
    _: &StatementContext,
    _: AlterIndexOptionsStatement,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_alter_index_options(
    scx: &StatementContext,
    AlterIndexOptionsStatement {
        index_name,
        if_exists,
        options,
    }: AlterIndexOptionsStatement,
) -> Result<Plan, anyhow::Error> {
    let alter_index = match scx.resolve_item(index_name) {
        Ok(entry) => {
            if entry.item_type() != CatalogItemType::Index {
                bail!("{} is a {} not a index", entry.name(), entry.item_type())
            }

            let logical_compaction_window = match options {
                AlterIndexOptionsList::Reset(o) => {
                    let mut options: HashSet<_> =
                        o.iter().map(|x| normalize::ident(x.clone())).collect();
                    // Follow Postgres and don't complain if unknown parameters
                    // are passed into ALTER INDEX ... RESET
                    if options.remove("logical_compaction_window") {
                        Some(LogicalCompactionWindow::Default)
                    } else {
                        None
                    }
                }
                AlterIndexOptionsList::Set(o) => {
                    let mut options = normalize::options(&o);

                    let logical_compaction_window = match options
                        .remove("logical_compaction_window")
                    {
                        Some(Value::String(window)) => match window.as_str() {
                            "off" => Some(LogicalCompactionWindow::Off),
                            s => Some(LogicalCompactionWindow::Custom(parse_duration::parse(s)?)),
                        },
                        Some(_) => bail!("\"logical_compaction_window\" must be a string"),
                        None => None,
                    };

                    if !options.is_empty() {
                        bail!("unrecognized parameter: \"{}\". Only \"logical_compaction_window\" is currently supported.",
                              options.keys().next().expect("known to exist"))
                    }

                    logical_compaction_window
                }
            };

            if let Some(logical_compaction_window) = logical_compaction_window {
                Some(AlterIndexLogicalCompactionWindow {
                    index: entry.id(),
                    logical_compaction_window,
                })
            } else {
                None
            }
        }
        Err(_) if if_exists => {
            // TODO(rkhaitan): better message indicating that the index does not exist.
            None
        }
        Err(e) => return Err(e.into()),
    };

    Ok(Plan::AlterIndexLogicalCompactionWindow(alter_index))
}

pub fn describe_alter_object_rename(
    _: &StatementContext,
    _: AlterObjectRenameStatement,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_alter_object_rename(
    scx: &StatementContext,
    AlterObjectRenameStatement {
        name,
        object_type,
        if_exists,
        to_item_name,
    }: AlterObjectRenameStatement,
) -> Result<Plan, anyhow::Error> {
    let id = match scx.resolve_item(name.clone()) {
        Ok(entry) => {
            if entry.item_type() != object_type {
                bail!("{} is a {} not a {}", name, entry.item_type(), object_type)
            }
            let mut proposed_name = name.0;
            let last = proposed_name.last_mut().unwrap();
            *last = to_item_name.clone();
            if scx.resolve_item(ObjectName(proposed_name)).is_ok() {
                bail!("{} is already taken by item in schema", to_item_name)
            }
            Some(entry.id())
        }
        Err(_) if if_exists => {
            // TODO(benesch): generate a notice indicating this
            // item does not exist.
            None
        }
        Err(err) => return Err(err.into()),
    };

    Ok(Plan::AlterItemRename {
        id,
        to_name: normalize::ident(to_item_name),
        object_type,
    })
}
