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
use mz_sql_parser::ast::UnresolvedItemName;
use mz_sql_parser::ast::{
    ColumnDef, CreateSubsourceOption, CreateSubsourceOptionName, CreateSubsourceStatement, Ident,
    WithOptionValue,
};
use mz_storage_types::sources::SubsourceResolver;
use tokio_postgres::types::Oid;
use tokio_postgres::Client;

use crate::names::{Aug, PartialItemName, ResolvedItemName};
use crate::normalize;
use crate::plan::{PlanError, StatementContext};

use super::error::PgSourcePurificationError;
use super::RequestedSubsource;

/// Ensure that we have select permissions on all tables; we have to do this before we
/// start snapshotting because if we discover we cannot `COPY` from a table while
/// snapshotting, we break the entire source.
pub(super) async fn validate_requested_subsources_privileges(
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
    subsource_resolver: &SubsourceResolver,
    references: &[PostgresTableDesc],
    text_columns: &mut [UnresolvedItemName],
    option_name: &str,
) -> Result<BTreeMap<u32, BTreeSet<String>>, PlanError> {
    let mut text_cols_dict: BTreeMap<u32, BTreeSet<String>> = BTreeMap::new();

    for name in text_columns {
        let (qual, col) = match name.0.split_last().expect("must have at least one element") {
            (col, qual) if qual.is_empty() => {
                return Err(PlanError::InvalidOptionValue {
                    option_name: option_name.to_string(),
                    err: Box::new(PlanError::UnderqualifiedColumnName(
                        col.as_str().to_string(),
                    )),
                });
            }
            (col, qual) => (qual.to_vec(), col.as_str().to_string()),
        };

        let qual_name = UnresolvedItemName(qual);

        let (mut fully_qualified_name, idx) =
            subsource_resolver.resolve(&qual_name.0, 3).map_err(|e| {
                PlanError::InvalidOptionValue {
                    option_name: option_name.to_string(),
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
                option_name: option_name.to_string(),
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
                option_name: option_name.to_string(),
                err: Box::new(PlanError::UnexpectedDuplicateReference { name: name.clone() }),
            });
        }
    }

    Ok(text_cols_dict)
}

pub(crate) fn generate_targeted_subsources(
    scx: &StatementContext,
    source_name: Option<ResolvedItemName>,
    validated_requested_subsources: Vec<RequestedSubsource<'_, PostgresTableDesc>>,
    mut text_cols_dict: BTreeMap<u32, BTreeSet<String>>,
    publication_tables: &[PostgresTableDesc],
) -> Result<
    (
        Vec<CreateSubsourceStatement<Aug>>,
        // These are the tables referenced by the subsources. We want this set
        // of tables separately so we can retain only table definitions that are
        // referenced by subsources. This helps avoid issues when generating PG
        // source table casts.
        Vec<PostgresTableDesc>,
    ),
    PlanError,
> {
    let mut subsources = vec![];
    let mut referenced_tables = vec![];

    // Aggregate all unrecognized types.
    let mut unsupported_cols = vec![];

    // Now that we have an explicit list of validated requested subsources we can create them
    for RequestedSubsource {
        upstream_name,
        subsource_name,
        table,
    } in validated_requested_subsources.into_iter()
    {
        // Figure out the schema of the subsource
        let mut columns = vec![];
        let text_cols_dict = text_cols_dict.remove(&table.oid);
        for c in table.columns.iter() {
            let name = Ident::new(c.name.clone())?;
            let ty = match &text_cols_dict {
                Some(names) if names.contains(&c.name) => mz_pgrepr::Type::Text,
                _ => match mz_pgrepr::Type::from_oid_and_typmod(c.type_oid, c.type_mod) {
                    Ok(t) => t,
                    Err(_) => {
                        let mut full_name = upstream_name.0.clone();
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
            of_source: source_name.clone(),
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
                value: Some(WithOptionValue::UnresolvedItemName(upstream_name)),
            }],
        };
        subsources.push(subsource);
        referenced_tables.push(table.clone())
    }

    if !unsupported_cols.is_empty() {
        unsupported_cols.sort();
        Err(PgSourcePurificationError::UnrecognizedTypes {
            cols: unsupported_cols,
        })?;
    }

    // If any any item was not removed from the text_cols dict, it wasn't being
    // added.
    let mut dangling_text_column_refs = vec![];

    for id in text_cols_dict.keys() {
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

    Ok((subsources, referenced_tables))
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
