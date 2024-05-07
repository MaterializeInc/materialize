// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Planning functions related to running ingestions (e.g. Kafka, PostgreSQL,
//! MySQL).

use std::collections::BTreeSet;
use std::time::Duration;

use itertools::Itertools;
use mz_ore::cast::CastFrom;

use mz_ore::str::StrExt;

use mz_repr::{strconv, ColumnName, RelationDesc, RelationType, ScalarType};
use mz_sql_parser::ast::display::comma_separated;
use mz_sql_parser::ast::{
    self, AvroSchema, AvroSchemaOption, AvroSchemaOptionName, ColumnOption, CreateSourceConnection,
    CreateSourceFormat, CreateSourceOption, CreateSourceOptionName, CreateSourceStatement,
    CreateSubsourceOption, CreateSubsourceOptionName, CreateSubsourceStatement, CsrConnection,
    CsrConnectionAvro, CsrConnectionProtobuf, CsrSeedProtobuf, CsvColumns, DeferredItemName,
    Format, Ident, KeyConstraint, ProtobufSchema, SourceIncludeMetadata, Statement,
    TableConstraint, UnresolvedItemName,
};
use mz_storage_types::connections::inline::{ConnectionAccess, ReferencedConnection};
use mz_storage_types::connections::Connection;
use mz_storage_types::sources::encoding::{
    included_column_desc, AvroEncoding, ColumnSpec, CsvEncoding, DataEncoding, ProtobufEncoding,
    RegexEncoding, SourceDataEncoding,
};
use mz_storage_types::sources::envelope::{
    KeyEnvelope, SourceEnvelope, UnplannedSourceEnvelope, UpsertStyle,
};
use mz_storage_types::sources::load_generator::{LoadGenerator, LoadGeneratorSourceConnection};
use mz_storage_types::sources::{GenericSourceConnection, SourceConnection, Timeline};

use crate::ast::display::AstDisplay;
use crate::names::{Aug, PartialItemName, ResolvedItemName};
use crate::normalize;
use crate::plan::error::PlanError;
use crate::plan::statement::StatementContext;
use crate::plan::with_options::TryFromValue;
use crate::plan::{
    plan_utils, query, CreateSourcePlan, DataSourceDesc, Ingestion, Plan, Source, StatementDesc,
};
use crate::session::vars;

mod kafka;
mod load_generator;
mod mysql;
mod postgres;

pub(crate) use load_generator::load_generator_ast_to_generator;
pub(crate) use mysql::MySqlConfigOptionExtracted;
pub use postgres::PgConfigOptionExtracted;

pub fn describe_create_source(
    _: &StatementContext,
    _: CreateSourceStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn describe_create_subsource(
    _: &StatementContext,
    _: CreateSubsourceStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

/// The fields of [`CreateSourceStatement`] which should be exposed to generate
/// [`mz_storage_types::sources::SourceDesc`].
///
/// This struct in conjunction with [`CreateSourceCommonFields`] tries to make
/// concrete the abstractions around planning sources.
struct CreateSourceDescFields<'a> {
    connection: &'a CreateSourceConnection<Aug>,
    envelope: &'a Option<ast::SourceEnvelope>,
    format: &'a Option<CreateSourceFormat<Aug>>,
    include_metadata: &'a [SourceIncludeMetadata],
}

impl<'a> From<&'a CreateSourceStatement<Aug>> for CreateSourceDescFields<'a> {
    fn from(stmt: &'a CreateSourceStatement<Aug>) -> Self {
        let CreateSourceStatement {
            name: _,
            in_cluster: _,
            col_names: _,
            connection,
            include_metadata,
            format,
            envelope,
            if_not_exists: _,
            key_constraint: _,
            with_options: _,
            referenced_subsources: _,
            progress_subsource: _,
        } = stmt;
        CreateSourceDescFields {
            connection,
            envelope,
            format,
            include_metadata,
        }
    }
}

/// The fields of [`CreateSourceStatement`] which should be exposed to generate
/// the fields of [`Plan::CreateSource`] other than
/// [`mz_storage_types::sources::SourceDesc`].
///
/// This struct in conjunction with [`CreateSourceDescFields`] tries to make
/// concrete the abstractions around planning sources.
struct CreateSourceCommonFields<'a> {
    name: &'a UnresolvedItemName,
    if_not_exists: bool,
    with_options: &'a [CreateSourceOption<Aug>],
    progress_subsource: &'a Option<DeferredItemName<Aug>>,
    key_constraint: &'a Option<KeyConstraint>,
    col_names: &'a [Ident],
}

impl<'a> From<&'a CreateSourceStatement<Aug>> for CreateSourceCommonFields<'a> {
    fn from(stmt: &'a CreateSourceStatement<Aug>) -> Self {
        let CreateSourceStatement {
            name,
            in_cluster: _,
            col_names,
            connection: _,
            include_metadata: _,
            format: _,
            envelope: _,
            if_not_exists,
            key_constraint,
            with_options,
            referenced_subsources: _,
            progress_subsource,
        } = stmt;

        CreateSourceCommonFields {
            name,
            if_not_exists: *if_not_exists,
            with_options,
            progress_subsource,
            key_constraint,
            col_names,
        }
    }
}

generate_extracted_config!(
    CreateSourceOption,
    (IgnoreKeys, bool),
    (Timeline, String),
    (TimestampInterval, Duration),
    (RetainHistory, Duration)
);

pub fn plan_create_source(
    scx: &StatementContext,
    mut stmt: CreateSourceStatement<Aug>,
) -> Result<Plan, PlanError> {
    mz_ore::soft_assert_or_log!(
        stmt.referenced_subsources.is_none(),
        "referenced subsources must be cleared in purification"
    );

    let allowed_with_options = vec![
        CreateSourceOptionName::TimestampInterval,
        CreateSourceOptionName::RetainHistory,
    ];
    if let Some(op) = stmt
        .with_options
        .iter()
        .find(|op| !allowed_with_options.contains(&op.name))
    {
        scx.require_feature_flag_w_dynamic_desc(
            &vars::ENABLE_CREATE_SOURCE_DENYLIST_WITH_OPTIONS,
            format!("CREATE SOURCE...WITH ({}..)", op.name.to_ast_string()),
            format!(
                "permitted options are {}",
                comma_separated(&allowed_with_options)
            ),
        )?;
    }

    let desc_fields = CreateSourceDescFields::from(&stmt);
    let (source_desc, mut desc) = match stmt.connection {
        CreateSourceConnection::Kafka { .. } => {
            kafka::plan_create_source_desc_kafka(scx, desc_fields)?
        }
        CreateSourceConnection::LoadGenerator { .. } => {
            load_generator::plan_create_source_desc_load_gen(scx, desc_fields)?
        }
        CreateSourceConnection::MySql { .. } => {
            mysql::plan_create_source_desc_mysql(scx, desc_fields)?
        }
        CreateSourceConnection::Postgres { .. } => {
            postgres::plan_create_source_desc_postgres(scx, desc_fields)?
        }
    };

    let CreateSourceCommonFields {
        name,
        if_not_exists,
        with_options,
        progress_subsource,
        key_constraint,
        col_names,
    } = (&stmt).into();

    let CreateSourceOptionExtracted {
        timeline,
        timestamp_interval,
        ignore_keys,
        retain_history,
        seen: _,
    } = CreateSourceOptionExtracted::try_from(with_options.to_vec())?;

    if ignore_keys.unwrap_or(false) {
        desc = desc.without_keys();
    }

    plan_utils::maybe_rename_columns(format!("source {}", name), &mut desc, col_names)?;

    let names: Vec<_> = desc.iter_names().cloned().collect();
    if let Some(dup) = names.iter().duplicates().next() {
        sql_bail!("column {} specified more than once", dup.as_str().quoted());
    }

    // Apply user-specified key constraint
    if let Some(KeyConstraint::PrimaryKeyNotEnforced { columns }) = key_constraint.clone() {
        // Don't remove this without addressing
        // https://github.com/MaterializeInc/materialize/issues/15272.
        scx.require_feature_flag(&vars::ENABLE_PRIMARY_KEY_NOT_ENFORCED)?;

        let key_columns = columns
            .into_iter()
            .map(normalize::column_name)
            .collect::<Vec<_>>();

        let mut uniq = BTreeSet::new();
        for col in key_columns.iter() {
            if !uniq.insert(col) {
                sql_bail!("Repeated column name in source key constraint: {}", col);
            }
        }

        let key_indices = key_columns
            .iter()
            .map(|col| {
                let name_idx = desc
                    .get_by_name(col)
                    .map(|(idx, _type)| idx)
                    .ok_or_else(|| sql_err!("No such column in source key constraint: {}", col))?;
                if desc.get_unambiguous_name(name_idx).is_none() {
                    sql_bail!("Ambiguous column in source key constraint: {}", col);
                }
                Ok(name_idx)
            })
            .collect::<Result<Vec<_>, _>>()?;

        if !desc.typ().keys.is_empty() {
            return Err(key_constraint_err(&desc, &key_columns));
        } else {
            desc = desc.with_key(key_indices);
        }
    }

    let progress_subsource = match progress_subsource {
        Some(name) => match name {
            DeferredItemName::Named(name) => match name {
                ResolvedItemName::Item { id, .. } => *id,
                ResolvedItemName::Cte { .. } | ResolvedItemName::Error => {
                    sql_bail!("[internal error] invalid target id")
                }
            },
            DeferredItemName::Deferred(_) => {
                sql_bail!("[internal error] progress subsource must be named during purification")
            }
        },
        _ => sql_bail!("[internal error] progress subsource must be named during purification"),
    };

    let name = scx.allocate_qualified_name(normalize::unresolved_item_name(name.clone())?)?;

    // Check for an object in the catalog with this same name
    let full_name = scx.catalog.resolve_full_name(&name);
    let partial_name = PartialItemName::from(full_name.clone());
    // For PostgreSQL compatibility, we need to prevent creating sources when
    // there is an existing object *or* type of the same name.
    if let (false, Ok(item)) = (
        if_not_exists,
        scx.catalog.resolve_item_or_type(&partial_name),
    ) {
        return Err(PlanError::ItemAlreadyExists {
            name: full_name.to_string(),
            item_type: item.item_type(),
        });
    }

    // We will rewrite the cluster if one is not provided, so we must use the
    // `in_cluster` value we plan to normalize when we canonicalize the create
    // statement.
    let in_cluster = super::source_sink_cluster_config(scx, "source", &mut stmt.in_cluster)?;

    let create_sql = normalize::create_statement(scx, Statement::CreateSource(stmt))?;

    // Allow users to specify a timeline. If they do not, determine a default
    // timeline for the source.
    let timeline = match timeline {
        None => match &source_desc.envelope {
            SourceEnvelope::CdcV2 => {
                Timeline::External(scx.catalog.resolve_full_name(&name).to_string())
            }
            _ => Timeline::EpochMilliseconds,
        },
        // TODO(benesch): if we stabilize this, can we find a better name than
        // `mz_epoch_ms`? Maybe just `mz_system`?
        Some(timeline) if timeline == "mz_epoch_ms" => Timeline::EpochMilliseconds,
        Some(timeline) if timeline.starts_with("mz_") => {
            return Err(PlanError::UnacceptableTimelineName(timeline));
        }
        Some(timeline) => Timeline::User(timeline),
    };

    let compaction_window = retain_history
        .map(|cw| {
            scx.require_feature_flag(&vars::ENABLE_LOGICAL_COMPACTION_WINDOW)?;
            Ok::<_, PlanError>(cw.try_into()?)
        })
        .transpose()?;

    let timestamp_interval = match timestamp_interval {
        Some(duration) => {
            let min = scx.catalog.system_vars().min_timestamp_interval();
            let max = scx.catalog.system_vars().max_timestamp_interval();
            if duration < min || duration > max {
                return Err(PlanError::InvalidTimestampInterval {
                    min,
                    max,
                    requested: duration,
                });
            }
            duration
        }
        // Use this pattern instead of unwrap in case we want to set the config
        // to be out of the bounds of the min and max timestamp intervals.
        None => scx.catalog.config().timestamp_interval,
    };

    let source = Source {
        create_sql,
        data_source: DataSourceDesc::Ingestion(Ingestion {
            desc: source_desc,
            progress_subsource,
            timestamp_interval,
        }),
        desc,
        compaction_window,
    };

    Ok(Plan::CreateSource(CreateSourcePlan {
        name,
        source,
        if_not_exists,
        timeline,
        in_cluster: Some(in_cluster),
    }))
}

fn plan_encoding_envelope(
    scx: &StatementContext,
    external_connection: &GenericSourceConnection<ReferencedConnection>,
    format: &Option<CreateSourceFormat<Aug>>,
    envelope: &ast::SourceEnvelope,
    include_metadata: &[SourceIncludeMetadata],
) -> Result<
    (
        Option<SourceDataEncoding<ReferencedConnection>>,
        SourceEnvelope,
        RelationDesc,
    ),
    PlanError,
> {
    let encoding = match format {
        Some(format) => Some(get_encoding(scx, format, envelope)?),
        None => None,
    };

    let (key_desc, value_desc) = match &encoding {
        Some(encoding) => {
            // If we are applying an encoding we need to ensure that the incoming value_desc is a
            // single column of type bytes.
            match external_connection.value_desc().typ().columns() {
                [typ] => match typ.scalar_type {
                    ScalarType::Bytes => {}
                    _ => sql_bail!(
                        "The schema produced by the source is incompatible with format decoding"
                    ),
                },
                _ => sql_bail!(
                    "The schema produced by the source is incompatible with format decoding"
                ),
            }

            let (key_desc, value_desc) = encoding.desc()?;

            // TODO(petrosagg): This piece of code seems to be making a statement about the
            // nullability of the NONE envelope when the source is Kafka. As written, the code
            // misses opportunities to mark columns as not nullable and is over conservative. For
            // example in the case of `FORMAT BYTES ENVELOPE NONE` the output is indeed
            // non-nullable but we will mark it as nullable anyway. This kind of crude reasoning
            // should be replaced with precise type-level reasoning.
            let key_desc = key_desc.map(|desc| {
                let is_kafka = external_connection.name() == "kafka";
                let is_envelope_none = matches!(envelope, ast::SourceEnvelope::None);
                if is_kafka && is_envelope_none {
                    RelationDesc::from_names_and_types(
                        desc.into_iter()
                            .map(|(name, typ)| (name, typ.nullable(true))),
                    )
                } else {
                    desc
                }
            });
            (key_desc, value_desc)
        }
        None => (
            Some(external_connection.key_desc()),
            external_connection.value_desc(),
        ),
    };

    // KEY VALUE load generators are the only UPSERT source that
    // has no encoding but defaults to `INCLUDE KEY`.
    //
    // As discussed
    // <https://github.com/MaterializeInc/materialize/pull/26246#issuecomment-2023558097>,
    // removing this special case amounts to deciding how to handle null keys
    // from sources, in a holistic way. We aren't yet prepared to do this, so we leave
    // this special case in.
    //
    // Note that this is safe because this generator is
    // 1. The only source with no encoding that can have its key included.
    // 2. Never produces null keys (or values, for that matter).
    let key_envelope_no_encoding = matches!(
        external_connection,
        GenericSourceConnection::LoadGenerator(LoadGeneratorSourceConnection {
            load_generator: LoadGenerator::KeyValue(_),
            ..
        })
    );
    let mut key_envelope = get_key_envelope(
        include_metadata,
        encoding.as_ref(),
        key_envelope_no_encoding,
    )?;

    match (&envelope, &key_envelope) {
        (ast::SourceEnvelope::Debezium, KeyEnvelope::None) => {}
        (ast::SourceEnvelope::Debezium, _) => sql_bail!(
            "Cannot use INCLUDE KEY with ENVELOPE DEBEZIUM: Debezium values include all keys."
        ),
        _ => {}
    };

    // Not all source envelopes are compatible with all source connections.
    // Whoever constructs the source ingestion pipeline is responsible for
    // choosing compatible envelopes and connections.
    //
    // TODO(guswynn): ambiguously assert which connections and envelopes are
    // compatible in typechecking
    //
    // TODO: remove bails as more support for upsert is added.
    let envelope = match &envelope {
        // TODO: fixup key envelope
        ast::SourceEnvelope::None => UnplannedSourceEnvelope::None(key_envelope),
        ast::SourceEnvelope::Debezium => {
            //TODO check that key envelope is not set
            let after_idx = match typecheck_debezium(&value_desc) {
                Ok((_before_idx, after_idx)) => Ok(after_idx),
                Err(type_err) => match encoding.as_ref().map(|e| &e.value) {
                    Some(DataEncoding::Avro(_)) => Err(type_err),
                    _ => Err(sql_err!(
                        "ENVELOPE DEBEZIUM requires that VALUE FORMAT is set to AVRO"
                    )),
                },
            }?;

            UnplannedSourceEnvelope::Upsert {
                style: UpsertStyle::Debezium { after_idx },
            }
        }
        ast::SourceEnvelope::Upsert => {
            let key_encoding = match encoding.as_ref().and_then(|e| e.key.as_ref()) {
                None => {
                    if !key_envelope_no_encoding {
                        bail_unsupported!(format!(
                            "UPSERT requires a key/value format: {:?}",
                            format
                        ))
                    }
                    None
                }
                Some(key_encoding) => Some(key_encoding),
            };
            // `ENVELOPE UPSERT` implies `INCLUDE KEY`, if it is not explicitly
            // specified.
            if key_envelope == KeyEnvelope::None {
                key_envelope = get_unnamed_key_envelope(key_encoding)?;
            }
            UnplannedSourceEnvelope::Upsert {
                style: UpsertStyle::Default(key_envelope),
            }
        }
        ast::SourceEnvelope::CdcV2 => {
            scx.require_feature_flag(&vars::ENABLE_ENVELOPE_MATERIALIZE)?;
            //TODO check that key envelope is not set
            match format {
                Some(CreateSourceFormat::Bare(Format::Avro(_))) => {}
                _ => bail_unsupported!("non-Avro-encoded ENVELOPE MATERIALIZE"),
            }
            UnplannedSourceEnvelope::CdcV2
        }
    };

    let metadata_columns = external_connection.metadata_columns();
    let metadata_desc = included_column_desc(metadata_columns.clone());
    let (envelope, desc) = envelope.desc(key_desc, value_desc, metadata_desc)?;

    Ok((encoding, envelope, desc))
}

fn typecheck_debezium(value_desc: &RelationDesc) -> Result<(Option<usize>, usize), PlanError> {
    let before = value_desc.get_by_name(&"before".into());
    let (after_idx, after_ty) = value_desc
        .get_by_name(&"after".into())
        .ok_or_else(|| sql_err!("'after' column missing from debezium input"))?;
    let before_idx = if let Some((before_idx, before_ty)) = before {
        if !matches!(before_ty.scalar_type, ScalarType::Record { .. }) {
            sql_bail!("'before' column must be of type record");
        }
        if before_ty != after_ty {
            sql_bail!("'before' type differs from 'after' column");
        }
        Some(before_idx)
    } else {
        None
    };
    Ok((before_idx, after_idx))
}

fn get_encoding(
    scx: &StatementContext,
    format: &CreateSourceFormat<Aug>,
    envelope: &ast::SourceEnvelope,
) -> Result<SourceDataEncoding<ReferencedConnection>, PlanError> {
    let encoding = match format {
        CreateSourceFormat::Bare(format) => get_encoding_inner(scx, format)?,
        CreateSourceFormat::KeyValue { key, value } => {
            let key = {
                let encoding = get_encoding_inner(scx, key)?;
                Some(encoding.key.unwrap_or(encoding.value))
            };
            let value = get_encoding_inner(scx, value)?.value;
            SourceDataEncoding { key, value }
        }
    };

    let requires_keyvalue = matches!(
        envelope,
        ast::SourceEnvelope::Debezium | ast::SourceEnvelope::Upsert
    );
    let is_keyvalue = encoding.key.is_some();
    if requires_keyvalue && !is_keyvalue {
        sql_bail!("ENVELOPE [DEBEZIUM] UPSERT requires that KEY FORMAT be specified");
    };

    Ok(encoding)
}

/// Extract the key envelope, if it is requested
fn get_key_envelope(
    included_items: &[SourceIncludeMetadata],
    encoding: Option<&SourceDataEncoding<ReferencedConnection>>,
    key_envelope_no_encoding: bool,
) -> Result<KeyEnvelope, PlanError> {
    let key_definition = included_items
        .iter()
        .find(|i| matches!(i, SourceIncludeMetadata::Key { .. }));
    if let Some(SourceIncludeMetadata::Key { alias }) = key_definition {
        match (alias, encoding.and_then(|e| e.key.as_ref())) {
            (Some(name), Some(_)) => Ok(KeyEnvelope::Named(name.as_str().to_string())),
            (None, Some(key)) => get_unnamed_key_envelope(Some(key)),
            (Some(name), _) if key_envelope_no_encoding => {
                Ok(KeyEnvelope::Named(name.as_str().to_string()))
            }
            (None, _) if key_envelope_no_encoding => get_unnamed_key_envelope(None),
            (_, None) => {
                // `kd.alias` == `None` means `INCLUDE KEY`
                // `kd.alias` == `Some(_) means INCLUDE KEY AS ___`
                // These both make sense with the same error message
                sql_bail!(
                    "INCLUDE KEY requires specifying KEY FORMAT .. VALUE FORMAT, \
                        got bare FORMAT"
                );
            }
        }
    } else {
        Ok(KeyEnvelope::None)
    }
}

/// Gets the key envelope for a given key encoding when no name for the key has
/// been requested by the user.
fn get_unnamed_key_envelope(
    key: Option<&DataEncoding<ReferencedConnection>>,
) -> Result<KeyEnvelope, PlanError> {
    // If the key is requested but comes from an unnamed type then it gets the name "key"
    //
    // Otherwise it gets the names of the columns in the type
    let is_composite = match key {
        Some(DataEncoding::Bytes | DataEncoding::Json | DataEncoding::Text) => false,
        Some(
            DataEncoding::Avro(_)
            | DataEncoding::Csv(_)
            | DataEncoding::Protobuf(_)
            | DataEncoding::Regex { .. },
        ) => true,
        None => false,
    };

    if is_composite {
        Ok(KeyEnvelope::Flattened)
    } else {
        Ok(KeyEnvelope::Named("key".to_string()))
    }
}

fn get_encoding_inner(
    scx: &StatementContext,
    format: &Format<Aug>,
) -> Result<SourceDataEncoding<ReferencedConnection>, PlanError> {
    let value = match format {
        Format::Bytes => DataEncoding::Bytes,
        Format::Avro(schema) => {
            let Schema {
                key_schema,
                value_schema,
                csr_connection,
                confluent_wire_format,
            } = match schema {
                // TODO(jldlaughlin): we need a way to pass in primary key information
                // when building a source from a string or file.
                AvroSchema::InlineSchema {
                    schema: ast::Schema { schema },
                    with_options,
                } => {
                    let AvroSchemaOptionExtracted {
                        confluent_wire_format,
                        ..
                    } = with_options.clone().try_into()?;

                    Schema {
                        key_schema: None,
                        value_schema: schema.clone(),
                        csr_connection: None,
                        confluent_wire_format,
                    }
                }
                AvroSchema::Csr {
                    csr_connection:
                        CsrConnectionAvro {
                            connection,
                            seed,
                            key_strategy: _,
                            value_strategy: _,
                        },
                } => {
                    let item = scx.get_item_by_resolved_name(&connection.connection)?;
                    let csr_connection = match item.connection()? {
                        Connection::Csr(_) => item.id(),
                        _ => {
                            sql_bail!(
                                "{} is not a schema registry connection",
                                scx.catalog
                                    .resolve_full_name(item.name())
                                    .to_string()
                                    .quoted()
                            )
                        }
                    };

                    if let Some(seed) = seed {
                        Schema {
                            key_schema: seed.key_schema.clone(),
                            value_schema: seed.value_schema.clone(),
                            csr_connection: Some(csr_connection),
                            confluent_wire_format: true,
                        }
                    } else {
                        unreachable!("CSR seed resolution should already have been called: Avro")
                    }
                }
            };

            if let Some(key_schema) = key_schema {
                return Ok(SourceDataEncoding {
                    key: Some(DataEncoding::Avro(AvroEncoding {
                        schema: key_schema,
                        csr_connection: csr_connection.clone(),
                        confluent_wire_format,
                    })),
                    value: DataEncoding::Avro(AvroEncoding {
                        schema: value_schema,
                        csr_connection,
                        confluent_wire_format,
                    }),
                });
            } else {
                DataEncoding::Avro(AvroEncoding {
                    schema: value_schema,
                    csr_connection,
                    confluent_wire_format,
                })
            }
        }
        Format::Protobuf(schema) => match schema {
            ProtobufSchema::Csr {
                csr_connection:
                    CsrConnectionProtobuf {
                        connection:
                            CsrConnection {
                                connection,
                                options,
                            },
                        seed,
                    },
            } => {
                if let Some(CsrSeedProtobuf { key, value }) = seed {
                    let item = scx.get_item_by_resolved_name(connection)?;
                    let _ = match item.connection()? {
                        Connection::Csr(connection) => connection,
                        _ => {
                            sql_bail!(
                                "{} is not a schema registry connection",
                                scx.catalog
                                    .resolve_full_name(item.name())
                                    .to_string()
                                    .quoted()
                            )
                        }
                    };

                    if !options.is_empty() {
                        sql_bail!("Protobuf CSR connections do not support any options");
                    }

                    let value = DataEncoding::Protobuf(ProtobufEncoding {
                        descriptors: strconv::parse_bytes(&value.schema)?,
                        message_name: value.message_name.clone(),
                        confluent_wire_format: true,
                    });
                    if let Some(key) = key {
                        return Ok(SourceDataEncoding {
                            key: Some(DataEncoding::Protobuf(ProtobufEncoding {
                                descriptors: strconv::parse_bytes(&key.schema)?,
                                message_name: key.message_name.clone(),
                                confluent_wire_format: true,
                            })),
                            value,
                        });
                    }
                    value
                } else {
                    unreachable!("CSR seed resolution should already have been called: Proto")
                }
            }
            ProtobufSchema::InlineSchema {
                message_name,
                schema: ast::Schema { schema },
            } => {
                let descriptors = strconv::parse_bytes(schema)?;

                DataEncoding::Protobuf(ProtobufEncoding {
                    descriptors,
                    message_name: message_name.to_owned(),
                    confluent_wire_format: false,
                })
            }
        },
        Format::Regex(regex) => DataEncoding::Regex(RegexEncoding {
            regex: mz_repr::adt::regex::Regex::new(regex.clone(), false)
                .map_err(|e| sql_err!("parsing regex: {e}"))?,
        }),
        Format::Csv { columns, delimiter } => {
            let columns = match columns {
                CsvColumns::Header { names } => {
                    if names.is_empty() {
                        sql_bail!("[internal error] column spec should get names in purify")
                    }
                    ColumnSpec::Header {
                        names: names.iter().cloned().map(|n| n.into_string()).collect(),
                    }
                }
                CsvColumns::Count(n) => ColumnSpec::Count(usize::cast_from(*n)),
            };
            DataEncoding::Csv(CsvEncoding {
                columns,
                delimiter: u8::try_from(*delimiter)
                    .map_err(|_| sql_err!("CSV delimiter must be an ASCII character"))?,
            })
        }
        Format::Json { array: false } => DataEncoding::Json,
        Format::Json { array: true } => bail_unsupported!("JSON ARRAY format in sources"),
        Format::Text => DataEncoding::Text,
    };
    Ok(SourceDataEncoding { key: None, value })
}

generate_extracted_config!(AvroSchemaOption, (ConfluentWireFormat, bool, Default(true)));

#[derive(Debug)]
pub struct Schema {
    pub key_schema: Option<String>,
    pub value_schema: String,
    pub csr_connection: Option<<ReferencedConnection as ConnectionAccess>::Csr>,
    pub confluent_wire_format: bool,
}

generate_extracted_config!(
    CreateSubsourceOption,
    (Progress, bool, Default(false)),
    (ExternalReference, UnresolvedItemName)
);

pub fn plan_create_subsource(
    scx: &StatementContext,
    stmt: CreateSubsourceStatement<Aug>,
) -> Result<Plan, PlanError> {
    let CreateSubsourceStatement {
        name,
        columns,
        of_source,
        constraints,
        if_not_exists,
        with_options,
    } = &stmt;

    let CreateSubsourceOptionExtracted {
        progress,
        external_reference,
        ..
    } = with_options.clone().try_into()?;

    // This invariant is enforced during purification; we are responsible for
    // creating the AST for subsources as a response to CREATE SOURCE
    // statements, so this would fire in integration testing if we failed to
    // uphold it.
    assert!(
        progress ^ (external_reference.is_some() && of_source.is_some()),
        "CREATE SUBSOURCE statement must specify either PROGRESS or REFERENCES option"
    );

    let names: Vec<_> = columns
        .iter()
        .map(|c| normalize::column_name(c.name.clone()))
        .collect();

    if let Some(dup) = names.iter().duplicates().next() {
        sql_bail!("column {} specified more than once", dup.as_str().quoted());
    }

    // Build initial relation type that handles declared data types
    // and NOT NULL constraints.
    let mut column_types = Vec::with_capacity(columns.len());
    let mut keys = Vec::new();

    for (i, c) in columns.into_iter().enumerate() {
        let aug_data_type = &c.data_type;
        let ty = query::scalar_type_from_sql(scx, aug_data_type)?;
        let mut nullable = true;
        for option in &c.options {
            match &option.option {
                ColumnOption::NotNull => nullable = false,
                ColumnOption::Default(_) => {
                    bail_unsupported!("Subsources cannot have default values")
                }
                ColumnOption::Unique { is_primary } => {
                    keys.push(vec![i]);
                    if *is_primary {
                        nullable = false;
                    }
                }
                other => {
                    bail_unsupported!(format!(
                        "CREATE SUBSOURCE with column constraint: {}",
                        other
                    ))
                }
            }
        }
        column_types.push(ty.nullable(nullable));
    }

    let mut seen_primary = false;
    'c: for constraint in constraints {
        match constraint {
            TableConstraint::Unique {
                name: _,
                columns,
                is_primary,
                nulls_not_distinct,
            } => {
                if seen_primary && *is_primary {
                    sql_bail!(
                        "multiple primary keys for source {} are not allowed",
                        name.to_ast_string_stable()
                    );
                }
                seen_primary = *is_primary || seen_primary;

                let mut key = vec![];
                for column in columns {
                    let column = normalize::column_name(column.clone());
                    match names.iter().position(|name| *name == column) {
                        None => sql_bail!("unknown column in constraint: {}", column),
                        Some(i) => {
                            let nullable = &mut column_types[i].nullable;
                            if *is_primary {
                                if *nulls_not_distinct {
                                    sql_bail!(
                                        "[internal error] PRIMARY KEY does not support NULLS NOT DISTINCT"
                                    );
                                }
                                *nullable = false;
                            } else if !(*nulls_not_distinct || !*nullable) {
                                // Non-primary key unique constraints are only keys if all of their
                                // columns are `NOT NULL` or the constraint is `NULLS NOT DISTINCT`.
                                break 'c;
                            }

                            key.push(i);
                        }
                    }
                }

                if *is_primary {
                    keys.insert(0, key);
                } else {
                    keys.push(key);
                }
            }
            TableConstraint::ForeignKey { .. } => {
                bail_unsupported!("CREATE SUBSOURCE with a foreign key")
            }
            TableConstraint::Check { .. } => {
                bail_unsupported!("CREATE SUBSOURCE with a check constraint")
            }
        }
    }

    let data_source = if let Some(source_reference) = of_source {
        // This is a subsource with the "natural" dependency order, i.e. it is
        // not a legacy subsource with the inverted structure.
        let ingestion_id = *source_reference.item_id();
        let external_reference = external_reference.unwrap();

        DataSourceDesc::IngestionExport {
            ingestion_id,
            external_reference,
        }
    } else if progress {
        DataSourceDesc::Progress
    } else {
        panic!("subsources must specify one of `external_reference`, `progress`, or `references`")
    };

    let if_not_exists = *if_not_exists;
    let name = scx.allocate_qualified_name(normalize::unresolved_item_name(name.clone())?)?;

    let create_sql = normalize::create_statement(scx, Statement::CreateSubsource(stmt))?;

    let typ = RelationType::new(column_types).with_keys(keys);
    let desc = RelationDesc::new(typ, names);

    let source = Source {
        create_sql,
        data_source,
        desc,
        compaction_window: None,
    };

    Ok(Plan::CreateSource(CreateSourcePlan {
        name,
        source,
        if_not_exists,
        timeline: Timeline::EpochMilliseconds,
        in_cluster: None,
    }))
}

fn key_constraint_err(desc: &RelationDesc, user_keys: &[ColumnName]) -> PlanError {
    let user_keys = user_keys.iter().map(|column| column.as_str()).join(", ");

    let existing_keys = desc
        .typ()
        .keys
        .iter()
        .map(|key_columns| {
            key_columns
                .iter()
                .map(|col| desc.get_name(*col).as_str())
                .join(", ")
        })
        .join(", ");

    sql_err!(
        "Key constraint ({}) conflicts with existing key ({})",
        user_keys,
        existing_keys
    )
}
