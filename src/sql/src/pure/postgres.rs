// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Postgres utilities for SQL purification.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use mz_expr::MirScalarExpr;
use mz_postgres_util::Config;
use mz_postgres_util::desc::PostgresTableDesc;
use mz_proto::RustType;
use mz_repr::{SqlColumnType, SqlRelationType, SqlScalarType};
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{
    ColumnDef, CreateSubsourceOption, CreateSubsourceOptionName, CreateSubsourceStatement,
    ExternalReferences, Ident, TableConstraint, UnresolvedItemName, Value, WithOptionValue,
};
use mz_storage_types::sources::SourceExportStatementDetails;
use mz_storage_types::sources::postgres::CastType;
use prost::Message;
use tokio_postgres::Client;
use tokio_postgres::types::Oid;

use crate::names::{Aug, ResolvedItemName};
use crate::normalize;
use crate::plan::hir::ColumnRef;
use crate::plan::typeconv::{CastContext, plan_cast};
use crate::plan::{
    ExprContext, HirScalarExpr, PlanError, QueryContext, QueryLifetime, Scope, StatementContext,
};

use super::error::PgSourcePurificationError;
use super::references::RetrievedSourceReferences;
use super::{PartialItemName, PurifiedExportDetails, PurifiedSourceExport, SourceReferencePolicy};

/// Ensure that we have select permissions on all tables; we have to do this before we
/// start snapshotting because if we discover we cannot `COPY` from a table while
/// snapshotting, we break the entire source.
pub(super) async fn validate_requested_references_privileges(
    config: &Config,
    client: &Client,
    table_oids: &[Oid],
) -> Result<(), PlanError> {
    privileges::check_table_privileges(config, client, table_oids).await?;
    replica_identity::check_replica_identity_full(client, table_oids).await?;

    Ok(())
}

/// Generate a mapping of `Oid`s to column names that should be ingested as text
/// (rather than their type in the upstream database).
///
/// Additionally, modify `text_columns` so that they contain database-qualified
/// references to the columns.
pub(super) fn generate_text_columns(
    retrieved_references: &RetrievedSourceReferences,
    text_columns: &mut [UnresolvedItemName],
) -> Result<BTreeMap<u32, BTreeSet<String>>, PlanError> {
    let mut text_cols_dict: BTreeMap<u32, BTreeSet<String>> = BTreeMap::new();

    for name in text_columns {
        let (qual, col) = match name.0.split_last().expect("must have at least one element") {
            (col, qual) if qual.is_empty() => {
                return Err(PlanError::InvalidOptionValue {
                    option_name: "TEXT COLUMNS".to_string(),
                    err: Box::new(PlanError::UnderqualifiedColumnName(
                        col.as_str().to_string(),
                    )),
                });
            }
            (col, qual) => (qual.to_vec(), col.as_str().to_string()),
        };

        let resolved_reference = retrieved_references.resolve_name(&qual)?;
        let mut fully_qualified_name =
            resolved_reference
                .external_reference()
                .map_err(|e| PlanError::InvalidOptionValue {
                    option_name: "TEXT COLUMNS".to_string(),
                    err: Box::new(e.into()),
                })?;

        let desc = resolved_reference
            .postgres_desc()
            .expect("known to be postgres");

        if !desc.columns.iter().any(|column| column.name == col) {
            let column = mz_repr::ColumnName::from(col);
            let similar = desc
                .columns
                .iter()
                .filter_map(|c| {
                    let c_name = mz_repr::ColumnName::from(c.name.clone());
                    c_name.is_similar(&column).then_some(c_name)
                })
                .collect();
            return Err(PlanError::InvalidOptionValue {
                option_name: "TEXT COLUMNS".to_string(),
                err: Box::new(PlanError::UnknownColumn {
                    table: Some(
                        normalize::unresolved_item_name(fully_qualified_name)
                            .expect("known to be of valid len"),
                    ),
                    column,
                    similar,
                }),
            });
        }

        // Rewrite fully qualified name.
        let col_ident = Ident::new(col.as_str().to_string())?;
        fully_qualified_name.0.push(col_ident);
        *name = fully_qualified_name;

        let new = text_cols_dict
            .entry(desc.oid)
            .or_default()
            .insert(col.as_str().to_string());

        if !new {
            return Err(PlanError::InvalidOptionValue {
                option_name: "TEXT COLUMNS".to_string(),
                err: Box::new(PlanError::UnexpectedDuplicateReference { name: name.clone() }),
            });
        }
    }

    Ok(text_cols_dict)
}

pub fn generate_create_subsource_statements(
    scx: &StatementContext,
    source_name: ResolvedItemName,
    requested_subsources: BTreeMap<UnresolvedItemName, PurifiedSourceExport>,
) -> Result<Vec<CreateSubsourceStatement<Aug>>, PlanError> {
    // Aggregate all unrecognized types.
    let mut unsupported_cols = vec![];

    // Now that we have an explicit list of validated requested subsources we can create them
    let mut subsources = Vec::with_capacity(requested_subsources.len());

    for (subsource_name, purified_export) in requested_subsources {
        let PostgresExportStatementValues {
            columns,
            constraints,
            text_columns,
            details,
            external_reference,
        } = generate_source_export_statement_values(scx, purified_export, &mut unsupported_cols)?;

        let mut with_options = vec![
            CreateSubsourceOption {
                name: CreateSubsourceOptionName::ExternalReference,
                value: Some(WithOptionValue::UnresolvedItemName(external_reference)),
            },
            CreateSubsourceOption {
                name: CreateSubsourceOptionName::Details,
                value: Some(WithOptionValue::Value(Value::String(hex::encode(
                    details.into_proto().encode_to_vec(),
                )))),
            },
        ];

        if let Some(text_columns) = text_columns {
            with_options.push(CreateSubsourceOption {
                name: CreateSubsourceOptionName::TextColumns,
                value: Some(WithOptionValue::Sequence(text_columns)),
            });
        }

        // Create the subsource statement
        let subsource = CreateSubsourceStatement {
            name: subsource_name,
            columns,
            // We might not know the primary source's `GlobalId` yet; if not,
            // we'll fill it in once we generate it.
            of_source: Some(source_name.clone()),
            // TODO(petrosagg): nothing stops us from getting the constraints of the
            // upstream tables and mirroring them here which will lead to more optimization
            // opportunities if for example there is a primary key or an index.
            //
            // If we ever do that we must triple check that we will get notified *in the
            // replication stream*, if our assumptions change. Failure to do that could
            // mean that an upstream table that started with an index was then altered to
            // one without and now we're producing garbage data.
            constraints,
            if_not_exists: false,
            with_options,
        };
        subsources.push(subsource);
    }

    if !unsupported_cols.is_empty() {
        unsupported_cols.sort();
        Err(PgSourcePurificationError::UnrecognizedTypes {
            cols: unsupported_cols,
        })?;
    }

    Ok(subsources)
}

pub(super) struct PostgresExportStatementValues {
    pub(super) columns: Vec<ColumnDef<Aug>>,
    pub(super) constraints: Vec<TableConstraint<Aug>>,
    pub(super) text_columns: Option<Vec<WithOptionValue<Aug>>>,
    pub(super) details: SourceExportStatementDetails,
    pub(super) external_reference: UnresolvedItemName,
}

pub(super) fn generate_source_export_statement_values(
    scx: &StatementContext,
    purified_export: PurifiedSourceExport,
    unsupported_cols: &mut Vec<(String, mz_repr::adt::system::Oid)>,
) -> Result<PostgresExportStatementValues, PlanError> {
    let (text_columns, table) = match purified_export.details {
        PurifiedExportDetails::Postgres {
            text_columns,
            table,
        } => (text_columns, table),
        _ => unreachable!("purified export details must be postgres"),
    };

    let text_column_set = text_columns
        .as_ref()
        .map(|v| BTreeSet::from_iter(v.iter().map(Ident::as_str)));

    // Figure out the schema of the subsource
    let mut columns = vec![];
    for c in table.columns.iter() {
        let name = Ident::new(c.name.clone())?;

        let ty = match text_column_set {
            Some(ref names) if names.contains(c.name.as_str()) => mz_pgrepr::Type::Text,
            _ => match mz_pgrepr::Type::from_oid_and_typmod(c.type_oid, c.type_mod) {
                Ok(t) => t,
                Err(_) => {
                    let mut full_name = purified_export.external_reference.0.clone();
                    full_name.push(name);
                    unsupported_cols.push((
                        UnresolvedItemName(full_name).to_ast_string_simple(),
                        mz_repr::adt::system::Oid(c.type_oid),
                    ));
                    continue;
                }
            },
        };

        let data_type = scx.resolve_type(ty)?;
        let mut options = vec![];

        if !c.nullable {
            options.push(mz_sql_parser::ast::ColumnOptionDef {
                name: None,
                option: mz_sql_parser::ast::ColumnOption::NotNull,
            });
        }

        columns.push(ColumnDef {
            name,
            data_type,
            collation: None,
            options,
        });
    }

    let mut constraints = vec![];
    for key in table.keys.clone() {
        let mut key_columns = vec![];

        for col_num in key.cols {
            let ident = Ident::new(
                table
                    .columns
                    .iter()
                    .find(|col| col.col_num == col_num)
                    .expect("key exists as column")
                    .name
                    .clone(),
            )?;
            key_columns.push(ident);
        }

        let constraint = mz_sql_parser::ast::TableConstraint::Unique {
            name: Some(Ident::new(key.name)?),
            columns: key_columns,
            is_primary: key.is_primary,
            nulls_not_distinct: key.nulls_not_distinct,
        };

        // We take the first constraint available to be the primary key.
        if key.is_primary {
            constraints.insert(0, constraint);
        } else {
            constraints.push(constraint);
        }
    }
    let details = SourceExportStatementDetails::Postgres { table };

    let text_columns = text_columns.map(|mut columns| {
        columns.sort();
        columns
            .into_iter()
            .map(WithOptionValue::Ident::<Aug>)
            .collect()
    });

    Ok(PostgresExportStatementValues {
        columns,
        constraints,
        text_columns,
        details,
        external_reference: purified_export.external_reference,
    })
}

pub(super) struct PurifiedSourceExports {
    pub(super) source_exports: BTreeMap<UnresolvedItemName, PurifiedSourceExport>,
    // NOTE(roshan): The text columns are already part of their
    // appropriate `source_exports` above, but these are returned to allow
    // round-tripping a `CREATE SOURCE` statement while we still allow creating
    // implicit subsources from `CREATE SOURCE`. Remove once
    // fully deprecating that feature and forcing users to use explicit
    // `CREATE TABLE .. FROM SOURCE` statements.
    pub(super) normalized_text_columns: Vec<WithOptionValue<Aug>>,
}

// Purify the requested external references, returning a set of purified
// source exports corresponding to external tables, and and additional
// fields necessary to generate relevant statements and update statement options
pub(super) async fn purify_source_exports(
    client: &Client,
    config: &mz_postgres_util::Config,
    retrieved_references: &RetrievedSourceReferences,
    requested_references: &Option<ExternalReferences>,
    mut text_columns: Vec<UnresolvedItemName>,
    unresolved_source_name: &UnresolvedItemName,
    reference_policy: &SourceReferencePolicy,
) -> Result<PurifiedSourceExports, PlanError> {
    let requested_exports = match requested_references.as_ref() {
        Some(requested) if matches!(reference_policy, SourceReferencePolicy::NotAllowed) => {
            Err(PlanError::UseTablesForSources(requested.to_string()))?
        }
        Some(requested) => retrieved_references
            .requested_source_exports(Some(requested), unresolved_source_name)?,
        None => {
            if matches!(reference_policy, SourceReferencePolicy::Required) {
                Err(PgSourcePurificationError::RequiresExternalReferences)?
            }

            // If no external reference is specified, it does not make sense to include
            // text columns.
            if !text_columns.is_empty() {
                Err(
                    PgSourcePurificationError::UnnecessaryOptionsWithoutReferences(
                        "TEXT COLUMNS".to_string(),
                    ),
                )?
            }

            return Ok(PurifiedSourceExports {
                source_exports: BTreeMap::new(),
                normalized_text_columns: vec![],
            });
        }
    };

    if requested_exports.is_empty() {
        sql_bail!(
            "[internal error]: Postgres reference {} did not match any tables",
            requested_references
                .as_ref()
                .unwrap()
                .to_ast_string_simple()
        );
    }

    super::validate_source_export_names(&requested_exports)?;

    let table_oids: Vec<_> = requested_exports
        .iter()
        .map(|r| r.meta.postgres_desc().expect("is postgres").oid)
        .collect();

    validate_requested_references_privileges(config, client, &table_oids).await?;

    let mut text_column_map = generate_text_columns(retrieved_references, &mut text_columns)?;

    // Normalize options to contain full qualified values.
    text_columns.sort();
    text_columns.dedup();
    let normalized_text_columns: Vec<_> = text_columns
        .into_iter()
        .map(WithOptionValue::UnresolvedItemName)
        .collect();

    let source_exports = requested_exports
        .into_iter()
        .map(|r| {
            let desc = r.meta.postgres_desc().expect("known postgres");
            (
                r.name,
                PurifiedSourceExport {
                    external_reference: r.external_reference,
                    details: PurifiedExportDetails::Postgres {
                        text_columns: text_column_map.remove(&desc.oid).map(|v| {
                            v.into_iter()
                                .map(|s| Ident::new(s).expect("validated above"))
                                .collect()
                        }),
                        table: desc.clone(),
                    },
                },
            )
        })
        .collect();

    if !text_column_map.is_empty() {
        // If any any item was not removed from the text_column_map, it wasn't being
        // added.
        let mut dangling_text_column_refs = vec![];
        let all_references = retrieved_references.all_references();

        for id in text_column_map.keys() {
            let desc = all_references
                .iter()
                .find_map(|reference| {
                    let desc = reference.postgres_desc().expect("is postgres");
                    if desc.oid == *id { Some(desc) } else { None }
                })
                .expect("validated when generating text columns");

            dangling_text_column_refs.push(PartialItemName {
                database: None,
                schema: Some(desc.namespace.clone()),
                item: desc.name.clone(),
            });
        }

        dangling_text_column_refs.sort();
        Err(PgSourcePurificationError::DanglingTextColumns {
            items: dangling_text_column_refs,
        })?;
    }

    Ok(PurifiedSourceExports {
        source_exports,
        normalized_text_columns,
    })
}

pub(crate) fn generate_column_casts(
    scx: &StatementContext,
    table: &PostgresTableDesc,
    text_columns: &Vec<Ident>,
) -> Result<Vec<(CastType, MirScalarExpr)>, PlanError> {
    // Generate the cast expressions required to convert the text encoded columns into
    // the appropriate target types, creating a Vec<MirScalarExpr>
    // The postgres source reader will then eval each of those on the incoming rows
    // First, construct an expression context where the expression is evaluated on an
    // imaginary row which has the same number of columns as the upstream table but all
    // of the types are text
    let mut cast_scx = scx.clone();
    cast_scx.param_types = Default::default();
    let cast_qcx = QueryContext::root(&cast_scx, QueryLifetime::Source);
    let mut column_types = vec![];
    for column in table.columns.iter() {
        column_types.push(SqlColumnType {
            nullable: column.nullable,
            scalar_type: SqlScalarType::String,
        });
    }

    let cast_ecx = ExprContext {
        qcx: &cast_qcx,
        name: "plan_postgres_source_cast",
        scope: &Scope::empty(),
        relation_type: &SqlRelationType {
            column_types,
            keys: vec![],
        },
        allow_aggregates: false,
        allow_subqueries: false,
        allow_parameters: false,
        allow_windows: false,
    };

    let text_columns = BTreeSet::from_iter(text_columns.iter().map(Ident::as_str));

    // Then, for each column we will generate a MirRelationExpr that extracts the nth
    // column and casts it to the appropriate target type
    let mut table_cast = vec![];
    for (i, column) in table.columns.iter().enumerate() {
        let (cast_type, ty) = if text_columns.contains(column.name.as_str()) {
            // Treat the column as text if it was referenced in
            // `TEXT COLUMNS`. This is the only place we need to
            // perform this logic; even if the type is unsupported,
            // we'll be able to ingest its values as text in
            // storage.
            (CastType::Text, mz_pgrepr::Type::Text)
        } else {
            match mz_pgrepr::Type::from_oid_and_typmod(column.type_oid, column.type_mod) {
                Ok(t) => (CastType::Natural, t),
                // If this reference survived purification, we
                // do not expect it to be from a table that the
                // user will consume., i.e. expect this table to
                // be filtered out of table casts.
                Err(_) => {
                    table_cast.push((
                        CastType::Natural,
                        HirScalarExpr::call_variadic(
                            mz_expr::VariadicFunc::ErrorIfNull,
                            vec![
                                HirScalarExpr::literal_null(SqlScalarType::String),
                                HirScalarExpr::literal(
                                    mz_repr::Datum::from(
                                        format!("Unsupported type with OID {}", column.type_oid)
                                            .as_str(),
                                    ),
                                    SqlScalarType::String,
                                ),
                            ],
                        )
                        .lower_uncorrelated()
                        .expect("no correlation"),
                    ));
                    continue;
                }
            }
        };

        let data_type = scx.resolve_type(ty)?;
        let scalar_type = crate::plan::query::scalar_type_from_sql(scx, &data_type)?;

        let col_expr = HirScalarExpr::named_column(
            ColumnRef {
                level: 0,
                column: i,
            },
            Arc::from(column.name.as_str()),
        );

        let cast_expr = plan_cast(&cast_ecx, CastContext::Explicit, col_expr, &scalar_type)?;

        let cast = if column.nullable {
            cast_expr
        } else {
            // We must enforce nullability constraint on cast
            // because PG replication stream does not propagate
            // constraint changes and we want to error subsource if
            // e.g. the constraint is dropped and we don't notice
            // it.
            HirScalarExpr::call_variadic(mz_expr::VariadicFunc::ErrorIfNull, vec![
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
                                    SqlScalarType::String,
                                ),
                            ],
                            )
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

            PlanError::TableContainsUningestableTypes {
                name: table.name.to_string(),
                type_: scx.humanize_scalar_type(&scalar_type, false),
                column: column.name.to_string(),
            }
        })?;

        table_cast.push((cast_type, mir_cast));
    }
    Ok(table_cast)
}

mod privileges {
    use mz_postgres_util::{Config, PostgresError};

    use super::*;
    use crate::plan::PlanError;
    use crate::pure::PgSourcePurificationError;

    async fn check_schema_privileges(
        config: &Config,
        client: &Client,
        table_oids: &[Oid],
    ) -> Result<(), PlanError> {
        let invalid_schema_privileges_rows = client
            .query(
                "
                WITH distinct_namespace AS (
                    SELECT
                        DISTINCT n.oid, n.nspname AS schema_name
                    FROM unnest($1::OID[]) AS oids (oid)
                    JOIN pg_class AS c ON c.oid = oids.oid
                    JOIN pg_namespace AS n ON c.relnamespace = n.oid
                )
                SELECT d.schema_name
                FROM distinct_namespace AS d
                WHERE
                    NOT has_schema_privilege($2::TEXT, d.oid, 'usage')",
                &[
                    &table_oids,
                    &config.get_user().expect("connection specifies user"),
                ],
            )
            .await
            .map_err(PostgresError::from)?;

        let mut invalid_schema_privileges = invalid_schema_privileges_rows
            .into_iter()
            .map(|row| row.get("schema_name"))
            .collect::<Vec<String>>();

        if invalid_schema_privileges.is_empty() {
            Ok(())
        } else {
            invalid_schema_privileges.sort();
            Err(PgSourcePurificationError::UserLacksUsageOnSchemas {
                user: config
                    .get_user()
                    .expect("connection specifies user")
                    .to_string(),
                schemas: invalid_schema_privileges,
            })?
        }
    }

    /// Ensure that the user specified in `config` has:
    ///
    /// -`SELECT` privileges for the identified `tables`.
    ///
    ///  `tables`'s elements should be of the structure `[<schema name>, <table name>]`.
    ///
    /// - `USAGE` privileges on the schemas references in `tables`.
    ///
    /// # Panics
    /// If `config` does not specify a user.
    pub async fn check_table_privileges(
        config: &Config,
        client: &Client,
        table_oids: &[Oid],
    ) -> Result<(), PlanError> {
        check_schema_privileges(config, client, table_oids).await?;

        let invalid_table_privileges_rows = client
            .query(
                "
            SELECT
                format('%I.%I', n.nspname, c.relname) AS schema_qualified_table_name
             FROM unnest($1::oid[]) AS oids (oid)
             JOIN
                 pg_class c ON c.oid = oids.oid
             JOIN
                 pg_namespace n ON c.relnamespace = n.oid
             WHERE NOT has_table_privilege($2::text, c.oid, 'select')",
                &[
                    &table_oids,
                    &config.get_user().expect("connection specifies user"),
                ],
            )
            .await
            .map_err(PostgresError::from)?;

        let mut invalid_table_privileges = invalid_table_privileges_rows
            .into_iter()
            .map(|row| row.get("schema_qualified_table_name"))
            .collect::<Vec<String>>();

        if invalid_table_privileges.is_empty() {
            Ok(())
        } else {
            invalid_table_privileges.sort();
            Err(PgSourcePurificationError::UserLacksSelectOnTables {
                user: config
                    .get_user()
                    .expect("connection must specify user")
                    .to_string(),
                tables: invalid_table_privileges,
            })?
        }
    }
}

mod replica_identity {
    use mz_postgres_util::PostgresError;

    use super::*;
    use crate::plan::PlanError;
    use crate::pure::PgSourcePurificationError;

    /// Ensures that all provided OIDs are tables with `REPLICA IDENTITY FULL`.
    pub async fn check_replica_identity_full(
        client: &Client,
        table_oids: &[Oid],
    ) -> Result<(), PlanError> {
        let invalid_replica_identity_rows = client
            .query(
                "
            SELECT
                format('%I.%I', n.nspname, c.relname) AS schema_qualified_table_name
             FROM unnest($1::oid[]) AS oids (oid)
             JOIN
                 pg_class c ON c.oid = oids.oid
             JOIN
                 pg_namespace n ON c.relnamespace = n.oid
             WHERE relreplident != 'f' OR relreplident IS NULL;",
                &[&table_oids],
            )
            .await
            .map_err(PostgresError::from)?;

        let mut invalid_replica_identity = invalid_replica_identity_rows
            .into_iter()
            .map(|row| row.get("schema_qualified_table_name"))
            .collect::<Vec<String>>();

        if invalid_replica_identity.is_empty() {
            Ok(())
        } else {
            invalid_replica_identity.sort();
            Err(PgSourcePurificationError::NotTablesWReplicaIdentityFull {
                items: invalid_replica_identity,
            })?
        }
    }
}
