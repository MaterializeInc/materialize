// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Planning functions related to running PostgreSQL ingestions.

use std::collections::{BTreeMap, BTreeSet};

use mz_proto::RustType;
use mz_repr::adt::system::Oid;
use mz_repr::{ColumnType, RelationDesc, RelationType, ScalarType};

use mz_sql_parser::ast::{
    CreateSourceConnection, CreateSourceStatement, Ident, PgConfigOption, PgConfigOptionName,
    SourceEnvelope, UnresolvedItemName,
};
use mz_storage_types::connections::inline::ReferencedConnection;
use mz_storage_types::connections::Connection;

use mz_storage_types::sources::envelope::{KeyEnvelope, UnplannedSourceEnvelope};
use mz_storage_types::sources::postgres::{
    CastType, PostgresSourceConnection, PostgresSourcePublicationDetails,
    ProtoPostgresSourcePublicationDetails,
};
use mz_storage_types::sources::{GenericSourceConnection, SourceConnection, SourceDesc};
use prost::Message;

use crate::ast::display::AstDisplay;
use crate::names::Aug;
use crate::plan::error::PlanError;
use crate::plan::expr::ColumnRef;
use crate::plan::query::{ExprContext, QueryLifetime};
use crate::plan::scope::Scope;
use crate::plan::statement::StatementContext;
use crate::plan::typeconv::{plan_cast, CastContext};
use crate::plan::with_options::TryFromValue;
use crate::plan::{query, HirScalarExpr, QueryContext};

generate_extracted_config!(
    PgConfigOption,
    (Details, String),
    (Publication, String),
    (TextColumns, Vec::<UnresolvedItemName>, Default(vec![]))
);

pub(super) fn plan_create_source_desc_postgres(
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
            matches!(envelope, Some(SourceEnvelope::None) | None),
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
