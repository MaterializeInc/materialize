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

use mz_postgres_util::desc::PostgresTableDesc;
use mz_postgres_util::Config;
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{
    ColumnDef, CreateSubsourceOption, CreateSubsourceOptionName, CreateSubsourceStatement, Ident,
    WithOptionValue,
};
use mz_sql_parser::ast::{ExternalReferences, UnresolvedItemName};
use mz_storage_types::connections::PostgresConnection;
use mz_storage_types::sources::SourceReferenceResolver;
use tokio_postgres::types::Oid;
use tokio_postgres::Client;

use crate::names::{Aug, ResolvedItemName};
use crate::normalize;
use crate::plan::{PlanError, StatementContext};

use super::error::PgSourcePurificationError;
use super::{PartialItemName, PurifiedExportDetails, PurifiedSourceExport, RequestedSourceExport};

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
    reference_resolver: &SourceReferenceResolver,
    references: &[PostgresTableDesc],
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

        let qual_name = UnresolvedItemName(qual);

        let (mut fully_qualified_name, idx) =
            reference_resolver.resolve(&qual_name.0, 3).map_err(|e| {
                PlanError::InvalidOptionValue {
                    option_name: "TEXT COLUMNS".to_string(),
                    err: Box::new(e.into()),
                }
            })?;

        let desc = &references[idx];

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
        let (text_columns, table) = match &purified_export.details {
            PurifiedExportDetails::Postgres {
                text_columns,
                table,
            } => (text_columns, table),
            _ => unreachable!("purified export details must be postgres"),
        };

        // Figure out the schema of the subsource
        let mut columns = vec![];
        for c in table.columns.iter() {
            let name = Ident::new(c.name.clone())?;

            let ty = match text_columns {
                Some(names) if names.contains(&c.name) => mz_pgrepr::Type::Text,
                _ => match mz_pgrepr::Type::from_oid_and_typmod(c.type_oid, c.type_mod) {
                    Ok(t) => t,
                    Err(_) => {
                        let mut full_name = purified_export.external_reference.0.clone();
                        full_name.push(name);
                        unsupported_cols.push((
                            UnresolvedItemName(full_name).to_ast_string(),
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
            with_options: vec![CreateSubsourceOption {
                name: CreateSubsourceOptionName::ExternalReference,
                value: Some(WithOptionValue::UnresolvedItemName(
                    purified_export.external_reference,
                )),
            }],
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

pub(super) struct PurifiedSourceExports {
    pub(super) source_exports: BTreeMap<UnresolvedItemName, PurifiedSourceExport>,
    pub(super) referenced_tables: Vec<PostgresTableDesc>,
    pub(super) normalized_text_columns: Vec<WithOptionValue<Aug>>,
}

// Purify the requested external references, returning a set of purified
// source exports corresponding to external tables, and and additional
// fields necessary to generate relevant statements and update statement options
pub(super) async fn purify_source_exports(
    client: &Client,
    config: &mz_postgres_util::Config,
    publication: &str,
    connection: &PostgresConnection,
    external_references: &mut Option<ExternalReferences>,
    mut text_columns: Vec<UnresolvedItemName>,
    unresolved_source_name: &UnresolvedItemName,
) -> Result<PurifiedSourceExports, PlanError> {
    let mut publication_tables = mz_postgres_util::publication_info(client, publication).await?;

    if publication_tables.is_empty() {
        Err(PgSourcePurificationError::EmptyPublication(
            publication.to_string(),
        ))?;
    }

    let reference_resolver =
        SourceReferenceResolver::new(&connection.database, &publication_tables)?;

    let mut validated_references = vec![];
    match external_references
        .as_mut()
        .ok_or(PgSourcePurificationError::RequiresExternalReferences)?
    {
        ExternalReferences::All => {
            for table in &publication_tables {
                let external_reference = UnresolvedItemName::qualified(&[
                    Ident::new(&connection.database)?,
                    Ident::new(&table.namespace)?,
                    Ident::new(&table.name)?,
                ]);
                let subsource_name =
                    super::source_export_name_gen(unresolved_source_name, &table.name)?;
                validated_references.push(RequestedSourceExport {
                    external_reference,
                    name: subsource_name,
                    table,
                });
            }
        }
        ExternalReferences::SubsetSchemas(schemas) => {
            let available_schemas: BTreeSet<_> = mz_postgres_util::get_schemas(client)
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
                Err(PgSourcePurificationError::DatabaseMissingFilteredSchemas {
                    database: connection.database.clone(),
                    schemas: missing_schemas,
                })?;
            }

            for table in &publication_tables {
                if !requested_schemas.contains(table.namespace.as_str()) {
                    continue;
                }

                let external_reference = UnresolvedItemName::qualified(&[
                    Ident::new(&connection.database)?,
                    Ident::new(&table.namespace)?,
                    Ident::new(&table.name)?,
                ]);
                let subsource_name =
                    super::source_export_name_gen(unresolved_source_name, &table.name)?;
                validated_references.push(RequestedSourceExport {
                    external_reference,
                    name: subsource_name,
                    table,
                });
            }
        }
        ExternalReferences::SubsetTables(references) => {
            // The user manually selected a subset of upstream tables so we need to
            // validate that the names actually exist and are not ambiguous
            validated_references.extend(super::source_export_gen(
                references,
                &reference_resolver,
                &publication_tables,
                3,
                unresolved_source_name,
            )?);
        }
    };

    // TODO: Remove this check once we allow creating a source with no exports and adding
    // source-fed tables to that source later.
    if validated_references.is_empty() {
        sql_bail!(
            "[internal error]: Postgres source must ingest at least one table, but {} matched none",
            external_references.as_ref().unwrap().to_ast_string()
        );
    }

    super::validate_source_export_names(&validated_references)?;

    let table_oids: Vec<_> = validated_references.iter().map(|r| r.table.oid).collect();

    validate_requested_references_privileges(config, client, &table_oids).await?;

    let mut text_column_map =
        generate_text_columns(&reference_resolver, &publication_tables, &mut text_columns)?;

    // Normalize options to contain full qualified values.
    text_columns.sort();
    text_columns.dedup();
    let normalized_text_columns: Vec<_> = text_columns
        .into_iter()
        .map(WithOptionValue::UnresolvedItemName)
        .collect();

    let requested_subsources = validated_references
        .into_iter()
        .map(|r| {
            (
                r.name,
                PurifiedSourceExport {
                    external_reference: r.external_reference,
                    details: PurifiedExportDetails::Postgres {
                        table: r.table.clone(),
                        text_columns: text_column_map.remove(&r.table.oid),
                    },
                },
            )
        })
        .collect();

    // If any any item was not removed from the text_column_map, it wasn't being
    // added.
    let mut dangling_text_column_refs = vec![];

    for id in text_column_map.keys() {
        let desc = publication_tables
            .iter()
            .find(|t| t.oid == *id)
            .expect("validated when generating text columns");

        dangling_text_column_refs.push(PartialItemName {
            database: None,
            schema: Some(desc.namespace.clone()),
            item: desc.name.clone(),
        });
    }

    if !dangling_text_column_refs.is_empty() {
        dangling_text_column_refs.sort();
        Err(PgSourcePurificationError::DanglingTextColumns {
            items: dangling_text_column_refs,
        })?;
    }

    // Trim any un-referred-to tables
    publication_tables.retain(|t| table_oids.contains(&t.oid));

    Ok(PurifiedSourceExports {
        source_exports: requested_subsources,
        referenced_tables: publication_tables,
        normalized_text_columns,
    })
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
