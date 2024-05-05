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

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use itertools::Itertools;
use mz_ore::cast::CastFrom;
use mz_ore::cast::TryCastFrom;
use mz_ore::str::StrExt;
use mz_proto::RustType;
use mz_repr::adt::system::Oid;
use mz_repr::{strconv, ColumnName, ColumnType, RelationDesc, RelationType, ScalarType};
use mz_sql_parser::ast::display::comma_separated;
use mz_sql_parser::ast::{
    self, AvroSchema, AvroSchemaOption, AvroSchemaOptionName, ColumnOption, CreateSourceConnection,
    CreateSourceFormat, CreateSourceOption, CreateSourceOptionName, CreateSourceStatement,
    CreateSubsourceOption, CreateSubsourceOptionName, CreateSubsourceStatement, CsrConnection,
    CsrConnectionAvro, CsrConnectionProtobuf, CsrSeedProtobuf, CsvColumns, DeferredItemName,
    Format, Ident, KeyConstraint, LoadGeneratorOption, LoadGeneratorOptionName, MySqlConfigOption,
    MySqlConfigOptionName, PgConfigOption, PgConfigOptionName, ProtobufSchema,
    SourceIncludeMetadata, Statement, TableConstraint, UnresolvedItemName,
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
use mz_storage_types::sources::kafka::{KafkaMetadataKind, KafkaSourceConnection};
use mz_storage_types::sources::load_generator::{
    KeyValueLoadGenerator, LoadGenerator, LoadGeneratorSourceConnection,
    LOAD_GENERATOR_KEY_VALUE_OFFSET_DEFAULT,
};
use mz_storage_types::sources::mysql::{
    MySqlSourceConnection, MySqlSourceDetails, ProtoMySqlSourceDetails,
};
use mz_storage_types::sources::postgres::{
    CastType, PostgresSourceConnection, PostgresSourcePublicationDetails,
    ProtoPostgresSourcePublicationDetails,
};
use mz_storage_types::sources::{GenericSourceConnection, SourceConnection, SourceDesc, Timeline};
use prost::Message;

use crate::ast::display::AstDisplay;
use crate::kafka_util::KafkaSourceConfigOptionExtracted;
use crate::names::{Aug, FullItemName, PartialItemName, RawDatabaseSpecifier, ResolvedItemName};
use crate::normalize;
use crate::plan::error::PlanError;
use crate::plan::expr::ColumnRef;
use crate::plan::query::{ExprContext, QueryLifetime};
use crate::plan::scope::Scope;
use crate::plan::statement::StatementContext;
use crate::plan::typeconv::{plan_cast, CastContext};
use crate::plan::with_options::TryFromValue;
use crate::plan::{
    plan_utils, query, CreateSourcePlan, DataSourceDesc, HirScalarExpr, Ingestion, Plan,
    QueryContext, Source, StatementDesc,
};
use crate::session::vars;

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

generate_extracted_config!(
    PgConfigOption,
    (Details, String),
    (Publication, String),
    (TextColumns, Vec::<UnresolvedItemName>, Default(vec![]))
);

fn plan_create_source_desc_postgres(
    scx: &StatementContext,
    stmt: &CreateSourceStatement<Aug>,
) -> Result<(SourceDesc<ReferencedConnection>, RelationDesc), PlanError> {
    mz_ore::soft_assert_or_log!(
        stmt.referenced_subsources.is_none(),
        "referenced subsources must be cleared in purification"
    );

    let CreateSourceStatement {
        name: _,
        in_cluster: _,
        col_names: _,
        connection,
        envelope,
        if_not_exists: _,
        format,
        key_constraint: _,
        include_metadata,
        with_options: _,
        referenced_subsources: _,
        progress_subsource: _,
    } = &stmt;

    for (check, feature) in [
        (
            matches!(envelope, Some(ast::SourceEnvelope::None) | None),
            "ENVELOPE other than NONE",
        ),
        (format.is_none(), "FORMAT"),
        (include_metadata.is_empty(), "INCLUDE metadata"),
    ] {
        if !check {
            bail_never_supported!(format!("{} with PostgreSQL source", feature));
        }
    }

    let CreateSourceConnection::Postgres {
        connection,
        options,
    } = connection
    else {
        panic!("must be PG connection")
    };

    let connection_item = scx.get_item_by_resolved_name(connection)?;
    let connection = match connection_item.connection()? {
        Connection::Postgres(connection) => connection.clone(),
        _ => sql_bail!(
            "{} is not a postgres connection",
            scx.catalog.resolve_full_name(connection_item.name())
        ),
    };

    let PgConfigOptionExtracted {
        details,
        publication,
        text_columns,
        seen: _,
    } = options.clone().try_into()?;

    let details = details
        .as_ref()
        .ok_or_else(|| sql_err!("internal error: Postgres source missing details"))?;
    let details = hex::decode(details).map_err(|e| sql_err!("{}", e))?;
    let details =
        ProtoPostgresSourcePublicationDetails::decode(&*details).map_err(|e| sql_err!("{}", e))?;

    // Create a "catalog" of the tables in the PG details.
    let mut tables_by_name = BTreeMap::new();
    for table in details.tables.iter() {
        tables_by_name
            .entry(table.name.clone())
            .or_insert_with(BTreeMap::new)
            .entry(table.namespace.clone())
            .or_insert_with(BTreeMap::new)
            .entry(connection.database.clone())
            .or_insert(table);
    }

    let publication_catalog = crate::catalog::SubsourceCatalog(tables_by_name);

    let mut text_cols: BTreeMap<Oid, BTreeSet<String>> = BTreeMap::new();

    // Look up the referenced text_columns in the publication_catalog.
    for name in text_columns {
        let (qual, col) = match name.0.split_last().expect("must have at least one element") {
            (col, qual) => (UnresolvedItemName(qual.to_vec()), col.as_str().to_string()),
        };

        let (_name, table_desc) = publication_catalog
            .resolve(qual)
            .expect("known to exist from purification");

        assert!(
            table_desc
                .columns
                .iter()
                .find(|column| column.name == col)
                .is_some(),
            "validated column exists in table during purification"
        );

        text_cols
            .entry(Oid(table_desc.oid))
            .or_default()
            .insert(col);
    }

    // Here we will generate the cast expressions required to convert the text encoded
    // columns into the appropriate target types, creating a Vec<MirScalarExpr> per table.
    // The postgres source reader will then eval each of those on the incoming rows based
    // on the target table
    let mut table_casts = BTreeMap::new();

    for (i, table) in details.tables.iter().enumerate() {
        // First, construct an expression context where the expression is evaluated on an
        // imaginary row which has the same number of columns as the upstream table but all
        // of the types are text
        let mut cast_scx = scx.clone();
        cast_scx.param_types = Default::default();
        let cast_qcx = QueryContext::root(&cast_scx, QueryLifetime::Source);
        let mut column_types = vec![];
        for column in table.columns.iter() {
            column_types.push(ColumnType {
                nullable: column.nullable,
                scalar_type: ScalarType::String,
            });
        }

        let cast_ecx = ExprContext {
            qcx: &cast_qcx,
            name: "plan_postgres_source_cast",
            scope: &Scope::empty(),
            relation_type: &RelationType {
                column_types,
                keys: vec![],
            },
            allow_aggregates: false,
            allow_subqueries: false,
            allow_parameters: false,
            allow_windows: false,
        };

        // Then, for each column we will generate a MirRelationExpr that extracts the nth
        // column and casts it to the appropriate target type
        let mut column_casts = vec![];
        for (i, column) in table.columns.iter().enumerate() {
            let (cast_type, ty) = match text_cols.get(&Oid(table.oid)) {
                // Treat the column as text if it was referenced in
                // `TEXT COLUMNS`. This is the only place we need to
                // perform this logic; even if the type is unsupported,
                // we'll be able to ingest its values as text in
                // storage.
                Some(names) if names.contains(&column.name) => {
                    (CastType::Text, mz_pgrepr::Type::Text)
                }
                _ => {
                    match mz_pgrepr::Type::from_oid_and_typmod(column.type_oid, column.type_mod) {
                        Ok(t) => (CastType::Natural, t),
                        // If this reference survived purification, we
                        // do not expect it to be from a table that the
                        // user will consume., i.e. expect this table to
                        // be filtered out of table casts.
                        Err(_) => {
                            column_casts.push((
                                CastType::Natural,
                                HirScalarExpr::CallVariadic {
                                    func: mz_expr::VariadicFunc::ErrorIfNull,
                                    exprs: vec![
                                        HirScalarExpr::literal_null(ScalarType::String),
                                        HirScalarExpr::literal(
                                            mz_repr::Datum::from(
                                                format!(
                                                    "Unsupported type with OID {}",
                                                    column.type_oid
                                                )
                                                .as_str(),
                                            ),
                                            ScalarType::String,
                                        ),
                                    ],
                                }
                                .lower_uncorrelated()
                                .expect("no correlation"),
                            ));
                            continue;
                        }
                    }
                }
            };

            let data_type = scx.resolve_type(ty)?;
            let scalar_type = query::scalar_type_from_sql(scx, &data_type)?;

            let col_expr = HirScalarExpr::Column(ColumnRef {
                level: 0,
                column: i,
            });

            let cast_expr = plan_cast(&cast_ecx, CastContext::Explicit, col_expr, &scalar_type)?;

            let cast = if column.nullable {
                cast_expr
            } else {
                // We must enforce nullability constraint on cast
                // because PG replication stream does not propagate
                // constraint changes and we want to error subsource if
                // e.g. the constraint is dropped and we don't notice
                // it.
                HirScalarExpr::CallVariadic {
                        func: mz_expr::VariadicFunc::ErrorIfNull,
                        exprs: vec![
                            cast_expr,
                            HirScalarExpr::literal(
                                mz_repr::Datum::from(
                                    format!(
                                        "PG column {}.{}.{} contained NULL data, despite having NOT NULL constraint",
                                        table.namespace.clone(),
                                        table.name.clone(),
                                        column.name.clone())
                                        .as_str(),
                                ),
                                ScalarType::String,
                            ),
                        ],
                    }
            };

            // We expect only reg* types to encounter this issue. Users
            // can ingest the data as text if they need to ingest it.
            // This is acceptable because we don't expect the OIDs from
            // an external PG source to be unilaterally usable in
            // resolving item names in MZ.
            let mir_cast = cast.lower_uncorrelated().map_err(|_e| {
                tracing::info!(
                    "cannot ingest {:?} data from PG source because cast is correlated",
                    scalar_type
                );

                let publication = match publication.clone() {
                    Some(publication) => publication,
                    None => return sql_err!("[internal error]: missing publication"),
                };

                PlanError::PublicationContainsUningestableTypes {
                    publication,
                    type_: scx.humanize_scalar_type(&scalar_type),
                    column: UnresolvedItemName::qualified(&[
                        Ident::new_unchecked(table.name.to_string()),
                        Ident::new_unchecked(column.name.to_string()),
                    ])
                    .to_ast_string(),
                }
            })?;

            column_casts.push((cast_type, mir_cast));
        }
        let r = table_casts.insert(i + 1, column_casts);
        assert!(r.is_none(), "cannot have table defined multiple times");
    }

    let publication_details =
        PostgresSourcePublicationDetails::from_proto(details).map_err(|e| sql_err!("{}", e))?;

    let connection =
        GenericSourceConnection::<ReferencedConnection>::from(PostgresSourceConnection {
            connection: connection_item.id(),
            connection_id: connection_item.id(),
            table_casts,
            publication: publication.expect("validated exists during purification"),
            publication_details,
        });

    mz_ore::soft_assert_no_log!(
        connection.metadata_columns().is_empty(),
        "PG connections do not contain metadata columns"
    );

    let (envelope, relation_desc) = UnplannedSourceEnvelope::None(KeyEnvelope::None).desc(
        None,
        RelationDesc::empty(),
        RelationDesc::empty(),
    )?;

    mz_ore::soft_assert_eq_or_log!(
        relation_desc,
        RelationDesc::empty(),
        "PostgreSQL source's primary source must have an empty relation desc"
    );

    let source_desc = SourceDesc::<ReferencedConnection> {
        connection,
        encoding: None,
        envelope,
    };

    Ok((source_desc, relation_desc))
}

generate_extracted_config!(
    MySqlConfigOption,
    (Details, String),
    (TextColumns, Vec::<UnresolvedItemName>, Default(vec![])),
    (IgnoreColumns, Vec::<UnresolvedItemName>, Default(vec![]))
);

fn plan_create_source_desc_mysql(
    scx: &StatementContext,
    stmt: &CreateSourceStatement<Aug>,
) -> Result<(SourceDesc<ReferencedConnection>, RelationDesc), PlanError> {
    mz_ore::soft_assert_or_log!(
        stmt.referenced_subsources.is_none(),
        "referenced subsources must be cleared in purification"
    );

    let CreateSourceStatement {
        name: _,
        in_cluster: _,
        col_names: _,
        connection,
        envelope,
        if_not_exists: _,
        format,
        key_constraint: _,
        include_metadata,
        with_options: _,
        referenced_subsources: _,
        progress_subsource: _,
    } = &stmt;

    for (check, feature) in [
        (
            matches!(envelope, Some(ast::SourceEnvelope::None) | None),
            "ENVELOPE other than NONE",
        ),
        (format.is_none(), "FORMAT"),
        (include_metadata.is_empty(), "INCLUDE metadata"),
    ] {
        if !check {
            bail_never_supported!(format!("{} with MySQL source", feature));
        }
    }

    let CreateSourceConnection::MySql {
        connection,
        options,
    } = connection
    else {
        panic!("must be MySQL connection")
    };

    let connection_item = scx.get_item_by_resolved_name(connection)?;
    match connection_item.connection()? {
        Connection::MySql(connection) => connection,
        _ => sql_bail!(
            "{} is not a MySQL connection",
            scx.catalog.resolve_full_name(connection_item.name())
        ),
    };
    let MySqlConfigOptionExtracted {
        details,
        text_columns,
        ignore_columns,
        seen: _,
    } = options.clone().try_into()?;

    let details = details
        .as_ref()
        .ok_or_else(|| sql_err!("internal error: MySQL source missing details"))?;
    let details = hex::decode(details).map_err(|e| sql_err!("{}", e))?;
    let details = ProtoMySqlSourceDetails::decode(&*details).map_err(|e| sql_err!("{}", e))?;
    let details = MySqlSourceDetails::from_proto(details).map_err(|e| sql_err!("{}", e))?;

    let text_columns = text_columns
        .into_iter()
        .map(|name| name.try_into().map_err(|e| sql_err!("{}", e)))
        .collect::<Result<Vec<_>, _>>()?;
    let ignore_columns = ignore_columns
        .into_iter()
        .map(|name| name.try_into().map_err(|e| sql_err!("{}", e)))
        .collect::<Result<Vec<_>, _>>()?;

    let connection = GenericSourceConnection::<ReferencedConnection>::from(MySqlSourceConnection {
        connection: connection_item.id(),
        connection_id: connection_item.id(),
        details,
        text_columns,
        ignore_columns,
    });

    mz_ore::soft_assert_no_log!(
        connection.metadata_columns().is_empty(),
        "PG connections do not contain metadata columns"
    );

    let (envelope, relation_desc) = UnplannedSourceEnvelope::None(KeyEnvelope::None).desc(
        None,
        RelationDesc::empty(),
        RelationDesc::empty(),
    )?;

    mz_ore::soft_assert_eq_or_log!(
        relation_desc,
        RelationDesc::empty(),
        "PostgreSQL source's primary source must have an empty relation desc"
    );

    let source_desc = SourceDesc::<ReferencedConnection> {
        connection,
        encoding: None,
        envelope,
    };

    Ok((source_desc, relation_desc))
}

fn plan_create_source_desc_kafka(
    scx: &StatementContext,
    stmt: &CreateSourceStatement<Aug>,
) -> Result<(SourceDesc<ReferencedConnection>, RelationDesc), PlanError> {
    let CreateSourceStatement {
        name: _,
        in_cluster: _,
        col_names: _,
        connection,
        envelope,
        if_not_exists: _,
        format,
        key_constraint: _,
        include_metadata,
        with_options: _,
        referenced_subsources: _,
        progress_subsource: _,
    } = &stmt;

    let envelope = envelope.clone().unwrap_or(ast::SourceEnvelope::None);

    let CreateSourceConnection::Kafka {
        connection: connection_name,
        options,
    } = connection
    else {
        panic!("must be Kafka connection")
    };

    let connection_item = scx.get_item_by_resolved_name(connection_name)?;
    if !matches!(connection_item.connection()?, Connection::Kafka(_)) {
        sql_bail!(
            "{} is not a kafka connection",
            scx.catalog.resolve_full_name(connection_item.name())
        )
    }

    let KafkaSourceConfigOptionExtracted {
        group_id_prefix,
        topic,
        topic_metadata_refresh_interval,
        start_timestamp: _, // purified into `start_offset`
        start_offset,
        seen: _,
    }: KafkaSourceConfigOptionExtracted = options.clone().try_into()?;

    let topic = topic.expect("validated exists during purification");

    let mut start_offsets = BTreeMap::new();
    if let Some(offsets) = start_offset {
        for (part, offset) in offsets.iter().enumerate() {
            if *offset < 0 {
                sql_bail!("START OFFSET must be a nonnegative integer");
            }
            start_offsets.insert(i32::try_from(part)?, *offset);
        }
    }

    if !start_offsets.is_empty() && envelope.requires_all_input() {
        sql_bail!("START OFFSET is not supported with ENVELOPE {}", envelope)
    }

    if topic_metadata_refresh_interval > Duration::from_secs(60 * 60) {
        // This is a librdkafka-enforced restriction that, if violated,
        // would result in a runtime error for the source.
        sql_bail!("TOPIC METADATA REFRESH INTERVAL cannot be greater than 1 hour");
    }

    if !include_metadata.is_empty()
        && !matches!(
            envelope,
            ast::SourceEnvelope::Upsert | ast::SourceEnvelope::None | ast::SourceEnvelope::Debezium
        )
    {
        // TODO(guswynn): should this be `bail_unsupported!`?
        sql_bail!("INCLUDE <metadata> requires ENVELOPE (NONE|UPSERT|DEBEZIUM)");
    }

    let metadata_columns = include_metadata
        .into_iter()
        .flat_map(|item| match item {
            SourceIncludeMetadata::Timestamp { alias } => {
                let name = match alias {
                    Some(name) => name.to_string(),
                    None => "timestamp".to_owned(),
                };
                Some((name, KafkaMetadataKind::Timestamp))
            }
            SourceIncludeMetadata::Partition { alias } => {
                let name = match alias {
                    Some(name) => name.to_string(),
                    None => "partition".to_owned(),
                };
                Some((name, KafkaMetadataKind::Partition))
            }
            SourceIncludeMetadata::Offset { alias } => {
                let name = match alias {
                    Some(name) => name.to_string(),
                    None => "offset".to_owned(),
                };
                Some((name, KafkaMetadataKind::Offset))
            }
            SourceIncludeMetadata::Headers { alias } => {
                let name = match alias {
                    Some(name) => name.to_string(),
                    None => "headers".to_owned(),
                };
                Some((name, KafkaMetadataKind::Headers))
            }
            SourceIncludeMetadata::Header {
                alias,
                key,
                use_bytes,
            } => Some((
                alias.to_string(),
                KafkaMetadataKind::Header {
                    key: key.clone(),
                    use_bytes: *use_bytes,
                },
            )),
            SourceIncludeMetadata::Key { .. } => {
                // handled below
                None
            }
        })
        .collect();

    let connection = KafkaSourceConnection::<ReferencedConnection> {
        connection: connection_item.id(),
        connection_id: connection_item.id(),
        topic,
        start_offsets,
        group_id_prefix,
        topic_metadata_refresh_interval,
        metadata_columns,
    };

    let external_connection = GenericSourceConnection::Kafka(connection);

    let (encoding, envelope, topic_desc) = plan_encoding_envelope(
        scx,
        &external_connection,
        format,
        &envelope,
        include_metadata,
    )?;

    let source_desc = SourceDesc::<ReferencedConnection> {
        connection: external_connection,
        encoding,
        envelope,
    };

    Ok((source_desc, topic_desc))
}

generate_extracted_config!(
    LoadGeneratorOption,
    (TickInterval, Duration),
    (ScaleFactor, f64),
    (MaxCardinality, u64),
    (Keys, u64),
    (SnapshotRounds, u64),
    (TransactionalSnapshot, bool),
    (ValueSize, u64),
    (Seed, u64),
    (Partitions, u64),
    (BatchSize, u64)
);

impl LoadGeneratorOptionExtracted {
    pub(super) fn ensure_only_valid_options(
        &self,
        loadgen: &ast::LoadGenerator,
    ) -> Result<(), PlanError> {
        use mz_sql_parser::ast::LoadGeneratorOptionName::*;

        let mut options = self.seen.clone();

        let permitted_options: &[_] = match loadgen {
            ast::LoadGenerator::Auction => &[TickInterval],
            ast::LoadGenerator::Counter => &[TickInterval, MaxCardinality],
            ast::LoadGenerator::Marketing => &[TickInterval],
            ast::LoadGenerator::Datums => &[TickInterval],
            ast::LoadGenerator::Tpch => &[TickInterval, ScaleFactor],
            ast::LoadGenerator::KeyValue => &[
                TickInterval,
                Keys,
                SnapshotRounds,
                TransactionalSnapshot,
                ValueSize,
                Seed,
                Partitions,
                BatchSize,
            ],
        };

        for o in permitted_options {
            options.remove(o);
        }

        if !options.is_empty() {
            sql_bail!(
                "{} load generators do not support {} values",
                loadgen,
                options.iter().join(", ")
            )
        }

        Ok(())
    }
}

pub(crate) fn load_generator_ast_to_generator(
    scx: &StatementContext,
    loadgen: &ast::LoadGenerator,
    options: &[LoadGeneratorOption<Aug>],
    include_metadata: &[SourceIncludeMetadata],
) -> Result<
    (
        LoadGenerator,
        Option<BTreeMap<FullItemName, (usize, RelationDesc)>>,
    ),
    PlanError,
> {
    let extracted: LoadGeneratorOptionExtracted = options.to_vec().try_into()?;
    extracted.ensure_only_valid_options(loadgen)?;

    if loadgen != &ast::LoadGenerator::KeyValue && !include_metadata.is_empty() {
        sql_bail!("INCLUDE metadata only supported with `KEY VALUE` load generators");
    }

    let load_generator = match loadgen {
        ast::LoadGenerator::Auction => LoadGenerator::Auction,
        ast::LoadGenerator::Counter => {
            let LoadGeneratorOptionExtracted {
                max_cardinality, ..
            } = extracted;
            LoadGenerator::Counter { max_cardinality }
        }
        ast::LoadGenerator::Marketing => LoadGenerator::Marketing,
        ast::LoadGenerator::Datums => LoadGenerator::Datums,
        ast::LoadGenerator::Tpch => {
            let LoadGeneratorOptionExtracted { scale_factor, .. } = extracted;

            // Default to 0.01 scale factor (=10MB).
            let sf: f64 = scale_factor.unwrap_or(0.01);
            if !sf.is_finite() || sf < 0.0 {
                sql_bail!("unsupported scale factor {sf}");
            }

            let f_to_i = |multiplier: f64| -> Result<i64, PlanError> {
                let total = (sf * multiplier).floor();
                let mut i = i64::try_cast_from(total)
                    .ok_or_else(|| sql_err!("unsupported scale factor {sf}"))?;
                if i < 1 {
                    i = 1;
                }
                Ok(i)
            };

            // The multiplications here are safely unchecked because they will
            // overflow to infinity, which will be caught by f64_to_i64.
            let count_supplier = f_to_i(10_000f64)?;
            let count_part = f_to_i(200_000f64)?;
            let count_customer = f_to_i(150_000f64)?;
            let count_orders = f_to_i(150_000f64 * 10f64)?;
            let count_clerk = f_to_i(1_000f64)?;

            LoadGenerator::Tpch {
                count_supplier,
                count_part,
                count_customer,
                count_orders,
                count_clerk,
            }
        }
        mz_sql_parser::ast::LoadGenerator::KeyValue => {
            scx.require_feature_flag(&vars::ENABLE_LOAD_GENERATOR_KEY_VALUE)?;
            let LoadGeneratorOptionExtracted {
                keys,
                snapshot_rounds,
                transactional_snapshot,
                value_size,
                tick_interval,
                seed,
                partitions,
                batch_size,
                ..
            } = extracted;

            let mut include_offset = None;
            for im in include_metadata {
                match im {
                    SourceIncludeMetadata::Offset { alias } => {
                        include_offset = match alias {
                            Some(alias) => Some(alias.to_string()),
                            None => Some(LOAD_GENERATOR_KEY_VALUE_OFFSET_DEFAULT.to_string()),
                        }
                    }
                    SourceIncludeMetadata::Key { .. } => continue,

                    _ => {
                        sql_bail!("only `INCLUDE OFFSET` and `INCLUDE KEY` is supported");
                    }
                };
            }

            let lgkv = KeyValueLoadGenerator {
                keys: keys.ok_or_else(|| sql_err!("LOAD GENERATOR KEY VALUE requires KEYS"))?,
                snapshot_rounds: snapshot_rounds
                    .ok_or_else(|| sql_err!("LOAD GENERATOR KEY VALUE requires SNAPSHOT ROUNDS"))?,
                // Defaults to true.
                transactional_snapshot: transactional_snapshot.unwrap_or(true),
                value_size: value_size
                    .ok_or_else(|| sql_err!("LOAD GENERATOR KEY VALUE requires VALUE SIZE"))?,
                partitions: partitions
                    .ok_or_else(|| sql_err!("LOAD GENERATOR KEY VALUE requires PARTITIONS"))?,
                tick_interval,
                batch_size: batch_size
                    .ok_or_else(|| sql_err!("LOAD GENERATOR KEY VALUE requires BATCH SIZE"))?,
                seed: seed.ok_or_else(|| sql_err!("LOAD GENERATOR KEY VALUE requires SEED"))?,
                include_offset,
            };

            if lgkv.keys == 0
                || lgkv.partitions == 0
                || lgkv.value_size == 0
                || lgkv.batch_size == 0
            {
                sql_bail!("LOAD GENERATOR KEY VALUE options must be non-zero")
            }

            if lgkv.keys % lgkv.partitions != 0 {
                sql_bail!("KEYS must be a multiple of PARTITIONS")
            }

            if lgkv.batch_size > lgkv.keys {
                sql_bail!("KEYS must be larger than BATCH SIZE")
            }

            // This constraints simplifies the source implementation.
            // We can lift it later.
            if (lgkv.keys / lgkv.partitions) % lgkv.batch_size != 0 {
                sql_bail!("PARTITIONS * BATCH SIZE must be a divisor of KEYS")
            }

            if lgkv.snapshot_rounds == 0 {
                sql_bail!("SNAPSHOT ROUNDS must be larger than 0")
            }

            LoadGenerator::KeyValue(lgkv)
        }
    };

    let mut available_subsources = BTreeMap::new();
    for (i, (name, desc)) in load_generator.views().iter().enumerate() {
        let name = FullItemName {
            database: RawDatabaseSpecifier::Name(
                mz_storage_types::sources::load_generator::LOAD_GENERATOR_DATABASE_NAME.to_owned(),
            ),
            schema: load_generator.schema_name().into(),
            item: name.to_string(),
        };
        // The zero-th output is the main output
        // TODO(petrosagg): these plus ones are an accident waiting to happen. Find a way
        // to handle the main source and the subsources uniformly
        available_subsources.insert(name, (i + 1, desc.clone()));
    }
    let available_subsources = if available_subsources.is_empty() {
        None
    } else {
        Some(available_subsources)
    };

    Ok((load_generator, available_subsources))
}

fn plan_create_source_desc_load_gen(
    scx: &StatementContext,
    stmt: &CreateSourceStatement<Aug>,
) -> Result<(SourceDesc<ReferencedConnection>, RelationDesc), PlanError> {
    let CreateSourceStatement {
        name,
        in_cluster: _,
        col_names,
        connection,
        envelope,
        if_not_exists: _,
        format,
        key_constraint: _,
        include_metadata,
        with_options: _,
        referenced_subsources: _,
        progress_subsource: _,
    } = &stmt;

    let CreateSourceConnection::LoadGenerator { generator, options } = connection else {
        panic!("must be Load Generator connection")
    };

    for (check, feature) in [
        (
            matches!(envelope, Some(ast::SourceEnvelope::None) | None)
                || mz_sql_parser::ast::LoadGenerator::KeyValue == *generator,
            "ENVELOPE other than NONE (except for KEY VALUE)",
        ),
        (format.is_none(), "FORMAT"),
    ] {
        if !check {
            bail_never_supported!(format!("{} with load generator source", feature));
        }
    }

    let envelope = envelope.clone().unwrap_or(ast::SourceEnvelope::None);

    let (load_generator, _available_subsources) =
        load_generator_ast_to_generator(scx, generator, options, include_metadata)?;

    let LoadGeneratorOptionExtracted { tick_interval, .. } = options.clone().try_into()?;
    let tick_micros = match tick_interval {
        Some(interval) => Some(interval.as_micros().try_into()?),
        None => None,
    };

    let connection =
        GenericSourceConnection::<ReferencedConnection>::from(LoadGeneratorSourceConnection {
            load_generator,
            tick_micros,
        });

    let (encoding, envelope, mut desc) =
        plan_encoding_envelope(scx, &connection, format, &envelope, include_metadata)?;

    plan_utils::maybe_rename_columns(format!("source {}", name), &mut desc, col_names)?;

    let names: Vec<_> = desc.iter_names().cloned().collect();
    if let Some(dup) = names.iter().duplicates().next() {
        sql_bail!("column {} specified more than once", dup.as_str().quoted());
    }

    let source_desc = SourceDesc::<ReferencedConnection> {
        connection,
        encoding,
        envelope,
    };

    Ok((source_desc, desc))
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

    let (source_desc, mut desc) = match stmt.connection {
        CreateSourceConnection::Kafka { .. } => plan_create_source_desc_kafka(scx, &stmt)?,
        CreateSourceConnection::LoadGenerator { .. } => {
            // Load generator sources' primary source's output can be empty or
            // not––the inner impl here determines which it is.
            plan_create_source_desc_load_gen(scx, &stmt)?
        }
        CreateSourceConnection::MySql { .. } => plan_create_source_desc_mysql(scx, &stmt)?,
        CreateSourceConnection::Postgres { .. } => plan_create_source_desc_postgres(scx, &stmt)?,
    };

    // The are the common elements for all sources.
    let CreateSourceStatement {
        name,
        if_not_exists,
        with_options,
        progress_subsource,
        key_constraint,
        col_names,
        ..
    } = &stmt;

    let CreateSourceOptionExtracted {
        timeline,
        timestamp_interval,
        ignore_keys,
        retain_history,
        seen: _,
    } = CreateSourceOptionExtracted::try_from(with_options.clone())?;

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

    let if_not_exists = *if_not_exists;
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
