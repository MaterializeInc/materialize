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
use mz_repr::adt::system::Oid;
use mz_repr::GlobalId;
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{
    ColumnDef, CreateSubsourceOption, CreateSubsourceOptionName, CreateSubsourceStatement,
    DeferredItemName, Ident, Value, WithOptionValue,
};
use mz_sql_parser::ast::{CreateSourceSubsource, UnresolvedItemName};
use mz_ssh_util::tunnel_manager::SshTunnelManager;

use crate::catalog::SubsourceCatalog;
use crate::names::{Aug, PartialItemName};
use crate::normalize;
use crate::plan::{PlanError, StatementContext};

use super::error::PgSourcePurificationError;
use super::RequestedSubsource;

pub(super) fn derive_catalog_from_publication_tables<'a>(
    database: &'a str,
    publication_tables: &'a [PostgresTableDesc],
) -> Result<SubsourceCatalog<&'a PostgresTableDesc>, PlanError> {
    // An index from table name -> schema name -> database name -> PostgresTableDesc
    let mut tables_by_name = BTreeMap::new();
    for table in publication_tables.iter() {
        tables_by_name
            .entry(table.name.clone())
            .or_insert_with(BTreeMap::new)
            .entry(table.namespace.clone())
            .or_insert_with(BTreeMap::new)
            .entry(database.to_string())
            .or_insert(table);
    }

    Ok(SubsourceCatalog(tables_by_name))
}

pub(super) async fn validate_requested_subsources_privileges(
    config: &Config,
    requested_subsources: &[RequestedSubsource<'_, PostgresTableDesc>],
    ssh_tunnel_manager: &SshTunnelManager,
) -> Result<(), PlanError> {
    // Ensure that we have select permissions on all tables; we have to do this before we
    // start snapshotting because if we discover we cannot `COPY` from a table while
    // snapshotting, we break the entire source.
    let tables_to_check_permissions = requested_subsources
        .iter()
        .map(
            |RequestedSubsource {
                 upstream_name: UnresolvedItemName(inner),
                 ..
             }| [inner[1].as_str(), inner[2].as_str()],
        )
        .collect();

    privileges::check_table_privileges(config, tables_to_check_permissions, ssh_tunnel_manager)
        .await?;

    let oids: Vec<_> = requested_subsources
        .iter()
        .map(|RequestedSubsource { table, .. }| table.oid)
        .collect();

    replica_identity::check_replica_identity_full(config, oids, ssh_tunnel_manager).await?;

    Ok(())
}

pub(super) fn generate_text_columns(
    publication_catalog: &SubsourceCatalog<&PostgresTableDesc>,
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

        let (mut fully_qualified_name, desc) =
            publication_catalog
                .resolve(qual_name)
                .map_err(|e| PlanError::InvalidOptionValue {
                    option_name: option_name.to_string(),
                    err: Box::new(e),
                })?;

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

pub(crate) fn generate_targeted_subsources<F>(
    scx: &StatementContext,
    validated_requested_subsources: Vec<RequestedSubsource<'_, PostgresTableDesc>>,
    mut text_cols_dict: BTreeMap<u32, BTreeSet<String>>,
    mut get_transient_subsource_id: F,
    publication_tables: &[PostgresTableDesc],
) -> Result<
    (
        Vec<CreateSourceSubsource<Aug>>,
        Vec<(GlobalId, CreateSubsourceStatement<Aug>)>,
    ),
    PlanError,
>
where
    F: FnMut() -> u64,
{
    let mut targeted_subsources = vec![];
    let mut subsources = vec![];

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
                            Oid(c.type_oid),
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

        // Create the targeted AST node for the original CREATE SOURCE statement
        let transient_id = GlobalId::Transient(get_transient_subsource_id());

        let subsource = scx.allocate_resolved_item_name(transient_id, subsource_name.clone())?;

        targeted_subsources.push(CreateSourceSubsource {
            reference: upstream_name,
            subsource: Some(DeferredItemName::Named(subsource)),
        });

        // Create the subsource statement
        let subsource = CreateSubsourceStatement {
            name: subsource_name,
            columns,
            of_source: None,
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
                name: CreateSubsourceOptionName::References,
                value: Some(WithOptionValue::Value(Value::Boolean(true))),
            }],
        };
        subsources.push((transient_id, subsource));
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

    targeted_subsources.sort();

    Ok((targeted_subsources, subsources))
}

mod privileges {
    use postgres_array::{Array, Dimension};

    use mz_postgres_util::{Config, PostgresError};

    use super::SshTunnelManager;
    use crate::plan::PlanError;
    use crate::pure::PgSourcePurificationError;

    async fn check_schema_privileges(
        config: &Config,
        schemas: Vec<&str>,
        ssh_tunnel_manager: &SshTunnelManager,
    ) -> Result<(), PlanError> {
        let client = config
            .connect("check_schema_privileges", ssh_tunnel_manager)
            .await?;

        let schemas_len = schemas.len();

        let schemas = Array::from_parts(
            schemas,
            vec![Dimension {
                len: i32::try_from(schemas_len).expect("fewer than i32::MAX schemas"),
                lower_bound: 0,
            }],
        );

        let invalid_schema_privileges = client
            .query(
                "
            SELECT
                s, has_schema_privilege($2::text, s, 'usage') AS p
            FROM
                (SELECT unnest($1::text[])) AS o (s);",
                &[
                    &schemas,
                    &config.get_user().expect("connection specifies user"),
                ],
            )
            .await
            .map_err(PostgresError::from)?
            .into_iter()
            .filter_map(|row| {
                // Only get rows that do not have sufficient privileges.
                let privileges: bool = row.get("p");
                if !privileges {
                    Some(row.get("s"))
                } else {
                    None
                }
            })
            .collect::<Vec<String>>();

        if invalid_schema_privileges.is_empty() {
            Ok(())
        } else {
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
        tables: Vec<[&str; 2]>,
        ssh_tunnel_manager: &SshTunnelManager,
    ) -> Result<(), PlanError> {
        let schemas = tables.iter().map(|t| t[0]).collect();
        check_schema_privileges(config, schemas, ssh_tunnel_manager).await?;

        let client = config
            .connect("check_table_privileges", ssh_tunnel_manager)
            .await?;

        let tables_len = tables.len();

        let tables = Array::from_parts(
            tables.into_iter().map(|i| i.to_vec()).flatten().collect(),
            vec![
                Dimension {
                    len: i32::try_from(tables_len).expect("fewer than i32::MAX tables"),
                    lower_bound: 1,
                },
                Dimension {
                    len: 2,
                    lower_bound: 1,
                },
            ],
        );

        let mut invalid_table_privileges = client
            .query(
                "
            WITH
                data AS (SELECT $1::text[] AS arr)
            SELECT
                t, has_table_privilege($2::text, t, 'select') AS p
            FROM
                (
                    SELECT
                        format('%I.%I', arr[i][1], arr[i][2]) AS t
                    FROM
                        data, ROWS FROM (generate_subscripts((SELECT arr FROM data), 1)) AS i
                )
                    AS o (t);",
                &[
                    &tables,
                    &config.get_user().expect("connection specifies user"),
                ],
            )
            .await
            .map_err(PostgresError::from)?
            .into_iter()
            .filter_map(|row| {
                // Only get rows that do not have sufficient privileges.
                let privileges: bool = row.get("p");
                if !privileges {
                    Some(row.get("t"))
                } else {
                    None
                }
            })
            .collect::<Vec<String>>();

        if invalid_table_privileges.is_empty() {
            Ok(())
        } else {
            invalid_table_privileges.sort();
            Err(PgSourcePurificationError::UserLacksSelectOnTables {
                user: config
                    .get_user()
                    .expect("connection specifies user")
                    .to_string(),
                tables: invalid_table_privileges,
            })?
        }
    }
}

mod replica_identity {
    use postgres_array::{Array, Dimension};
    use tokio_postgres::types::Oid;

    use mz_postgres_util::{Config, PostgresError};

    use super::SshTunnelManager;
    use crate::plan::PlanError;
    use crate::pure::PgSourcePurificationError;

    /// Ensures that all provided OIDs are tables with `REPLICA IDENTITY FULL`.
    pub async fn check_replica_identity_full(
        config: &Config,
        oids: Vec<Oid>,
        ssh_tunnel_manager: &SshTunnelManager,
    ) -> Result<(), PlanError> {
        let client = config
            .connect("check_replica_identity_full", ssh_tunnel_manager)
            .await?;

        let oids_len = oids.len();

        let oids = Array::from_parts(
            oids,
            vec![Dimension {
                len: i32::try_from(oids_len).expect("fewer than i32::MAX schemas"),
                lower_bound: 0,
            }],
        );

        let mut invalid_replica_identity = client
            .query(
                "
            SELECT
                input.oid::REGCLASS::TEXT AS name
            FROM
                (SELECT unnest($1::OID[]) AS oid) AS input
                LEFT JOIN pg_class ON input.oid = pg_class.oid
            WHERE
                relreplident != 'f' OR relreplident IS NULL;",
                &[&oids],
            )
            .await
            .map_err(PostgresError::from)?
            .into_iter()
            .map(|row| row.get("name"))
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
