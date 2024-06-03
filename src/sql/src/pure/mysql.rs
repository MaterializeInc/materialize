// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! MySQL utilities for SQL purification.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::DerefMut;

use mz_mysql_util::{validate_source_privileges, MySqlError, MySqlTableDesc, QualifiedTableRef};
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::UnresolvedItemName;
use mz_sql_parser::ast::{
    ColumnDef, CreateSubsourceOption, CreateSubsourceOptionName, CreateSubsourceStatement, Ident,
    IdentError, MySqlConfigOptionName, ReferencedSubsources, WithOptionValue,
};

use crate::catalog::{SessionCatalog, SubsourceCatalog};
use crate::names::Aug;
use crate::plan::{PlanError, StatementContext};
use crate::pure::{MySqlSourcePurificationError, ResolvedItemName};

use super::RequestedSubsource;

/// The name of the fake database that we use for MySQL sources
/// to fit our model of a 3-layer catalog. MySQL doesn't have a concept
/// of databases AND schemas, it treats both as the same thing.
pub(crate) static MYSQL_DATABASE_FAKE_NAME: &str = "mysql";

pub(super) fn mysql_upstream_name(
    table: &MySqlTableDesc,
) -> Result<UnresolvedItemName, IdentError> {
    Ok(UnresolvedItemName::qualified(&[
        Ident::new(&table.schema_name)?,
        Ident::new(&table.name)?,
    ]))
}

/// Reverses the `mysql_upstream_name` function.
fn upstream_name_to_table(
    name: &UnresolvedItemName,
) -> Result<QualifiedTableRef, MySqlSourcePurificationError> {
    if name.0.len() != 2 {
        Err(MySqlSourcePurificationError::InvalidTableReference(name.to_string()).into())?
    }
    Ok(QualifiedTableRef {
        schema_name: name.0[0].as_str(),
        table_name: name.0[1].as_str(),
    })
}

pub(super) fn derive_catalog_from_tables<'a>(
    tables: &'a [MySqlTableDesc],
) -> Result<SubsourceCatalog<&'a MySqlTableDesc>, PlanError> {
    // An index from table name -> schema name -> MySqlTableDesc
    let mut tables_by_name = BTreeMap::new();
    for table in tables.iter() {
        tables_by_name
            .entry(table.name.clone())
            .or_insert_with(BTreeMap::new)
            .entry(table.schema_name.clone())
            .or_insert_with(BTreeMap::new)
            .entry(MYSQL_DATABASE_FAKE_NAME.to_string())
            .or_insert(table);
    }

    Ok(SubsourceCatalog(tables_by_name))
}

pub(super) fn generate_targeted_subsources(
    scx: &StatementContext,
    source_name: Option<ResolvedItemName>,
    validated_requested_subsources: Vec<RequestedSubsource<MySqlTableDesc>>,
) -> Result<Vec<CreateSubsourceStatement<Aug>>, PlanError> {
    let mut subsources = vec![];

    // Now that we have an explicit list of validated requested subsources we can create them
    for RequestedSubsource {
        upstream_name,
        subsource_name,
        table,
    } in validated_requested_subsources.into_iter()
    {
        // Figure out the schema of the subsource
        let mut columns = vec![];
        for c in table.columns.iter() {
            match c.column_type {
                // This column is intentionally ignored, so we don't generate a column for it in
                // the subsource.
                None => {}
                Some(ref column_type) => {
                    let name = Ident::new(&c.name)?;

                    let ty = mz_pgrepr::Type::from(&column_type.scalar_type);
                    let data_type = scx.resolve_type(ty)?;
                    let mut col_options = vec![];

                    if !column_type.nullable {
                        col_options.push(mz_sql_parser::ast::ColumnOptionDef {
                            name: None,
                            option: mz_sql_parser::ast::ColumnOption::NotNull,
                        });
                    }
                    columns.push(ColumnDef {
                        name,
                        data_type,
                        collation: None,
                        options: col_options,
                    });
                }
            }
        }

        let mut constraints = vec![];
        for key in table.keys.iter() {
            let columns: Result<Vec<Ident>, _> = key.columns.iter().map(Ident::new).collect();

            let constraint = mz_sql_parser::ast::TableConstraint::Unique {
                name: Some(Ident::new(&key.name)?),
                columns: columns?,
                is_primary: key.is_primary,
                // MySQL always permits multiple nulls values in unique indexes.
                nulls_not_distinct: false,
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
            constraints,
            if_not_exists: false,
            with_options: vec![CreateSubsourceOption {
                name: CreateSubsourceOptionName::ExternalReference,
                value: Some(WithOptionValue::UnresolvedItemName(upstream_name)),
            }],
        };
        subsources.push(subsource);
    }

    Ok(subsources)
}

/// Map a list of column references to a map of table references to column names.
pub(super) fn map_column_refs<'a>(
    cols: &'a [UnresolvedItemName],
    option_type: MySqlConfigOptionName,
) -> Result<BTreeMap<QualifiedTableRef<'a>, BTreeSet<&'a str>>, PlanError> {
    let mut table_to_cols = BTreeMap::new();
    for name in cols.iter() {
        // We only support fully qualified references for now (e.g. `schema_name.table_name.column_name`)
        if name.0.len() == 3 {
            let key = mz_mysql_util::QualifiedTableRef {
                schema_name: name.0[0].as_str(),
                table_name: name.0[1].as_str(),
            };
            let new = table_to_cols
                .entry(key)
                .or_insert_with(BTreeSet::new)
                .insert(name.0[2].as_str());
            if !new {
                return Err(PlanError::InvalidOptionValue {
                    option_name: option_type.to_ast_string(),
                    err: Box::new(PlanError::UnexpectedDuplicateReference { name: name.clone() }),
                });
            }
        } else {
            return Err(PlanError::InvalidOptionValue {
                option_name: option_type.to_ast_string(),
                err: Box::new(PlanError::UnderqualifiedColumnName(name.to_string())),
            });
        }
    }
    Ok(table_to_cols)
}

/// Normalize column references to a sorted, deduplicated options list of column names.
pub(super) fn normalize_column_refs<'a>(
    cols: Vec<UnresolvedItemName>,
    catalog: &'a SubsourceCatalog<&'a MySqlTableDesc>,
) -> Result<Vec<WithOptionValue<Aug>>, MySqlSourcePurificationError> {
    let (seq, unknown): (Vec<_>, Vec<_>) = cols.into_iter().partition(|name| {
        let (column_name, qual) = name.0.split_last().expect("non-empty");
        match catalog.resolve(UnresolvedItemName::qualified(qual)) {
            // TODO: this needs to also introduce the maximum qualification on
            // the columns, i.e. ensure they have the schema name.
            Ok((_, desc)) => desc.columns.iter().any(|n| &n.name == column_name.as_str()),
            Err(_) => false,
        }
    });

    if !unknown.is_empty() {
        return Err(MySqlSourcePurificationError::DanglingColumns { items: unknown });
    }

    let mut seq: Vec<_> = seq
        .into_iter()
        .map(WithOptionValue::UnresolvedItemName)
        .collect();
    seq.sort();
    seq.dedup();
    Ok(seq)
}

pub(super) async fn validate_requested_subsources_privileges(
    requested_subsources: &[RequestedSubsource<'_, MySqlTableDesc>],
    conn: &mut mz_mysql_util::MySqlConn,
) -> Result<(), PlanError> {
    // Ensure that we have correct privileges on all tables; we have to do this before we
    // start snapshotting because if we discover we cannot `SELECT` from a table while
    // snapshotting, we break the entire source.
    let tables_to_check_permissions = requested_subsources
        .iter()
        .map(|RequestedSubsource { upstream_name, .. }| upstream_name_to_table(upstream_name))
        .collect::<Result<Vec<_>, _>>()?;

    validate_source_privileges(&mut *conn, &tables_to_check_permissions)
        .await
        .map_err(|err| match err {
            MySqlError::MissingPrivileges(missing_privileges) => {
                MySqlSourcePurificationError::UserLacksPrivileges(
                    missing_privileges
                        .into_iter()
                        .map(|mp| (mp.privilege, mp.qualified_table_name))
                        .collect(),
                )
                .into()
            }
            _ => PlanError::MySqlConnectionErr { cause: err.into() },
        })?;

    Ok(())
}

pub(super) struct PurifiedSubsources {
    pub(super) new_subsources: Vec<CreateSubsourceStatement<Aug>>,
    pub(super) tables: Vec<MySqlTableDesc>,
    pub(super) normalized_text_columns: Vec<WithOptionValue<Aug>>,
    pub(super) normalized_ignore_columns: Vec<WithOptionValue<Aug>>,
}

// Purify the referenced subsources, return the list of subsource statements,
// corresponding tables, and and additional fields necessary to update the source
// options
pub(super) async fn purify_subsources(
    conn: &mut mz_mysql_util::MySqlConn,
    referenced_subsources: &mut Option<ReferencedSubsources>,
    text_columns: Vec<UnresolvedItemName>,
    ignore_columns: Vec<UnresolvedItemName>,
    resolved_source_name: Option<ResolvedItemName>,
    unresolved_source_name: &UnresolvedItemName,
    catalog: &impl SessionCatalog,
) -> Result<PurifiedSubsources, PlanError> {
    // Determine which table schemas to request from mysql. Note that in mysql
    // a 'schema' is the same as a 'database', and a fully qualified table
    // name is 'schema_name.table_name' (there is no db_name)
    let table_schema_request = match referenced_subsources
        .as_mut()
        .ok_or(MySqlSourcePurificationError::RequiresReferencedSubsources)?
    {
        ReferencedSubsources::All => mz_mysql_util::SchemaRequest::All,
        ReferencedSubsources::SubsetSchemas(schemas) => mz_mysql_util::SchemaRequest::Schemas(
            schemas.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        ),
        ReferencedSubsources::SubsetTables(tables) => mz_mysql_util::SchemaRequest::Tables(
            tables
                .iter()
                .map(|t| {
                    let idents = &t.reference.0;
                    // We only support fully qualified table names for now
                    if idents.len() != 2 {
                        Err(MySqlSourcePurificationError::InvalidTableReference(
                            t.reference.to_ast_string(),
                        ))?;
                    }
                    Ok((idents[0].as_str(), idents[1].as_str()))
                })
                .collect::<Result<Vec<_>, MySqlSourcePurificationError>>()?,
        ),
    };

    let text_cols_map = map_column_refs(&text_columns, MySqlConfigOptionName::TextColumns)?;
    let ignore_cols_map = map_column_refs(&ignore_columns, MySqlConfigOptionName::IgnoreColumns)?;

    // Retrieve schemas for all requested tables
    // NOTE: mysql will only expose the schemas of tables we have at least one privilege on
    // and we can't tell if a table exists without a privilege, so in some cases we may
    // return an EmptyDatabase error in the case of privilege issues.
    let tables = mz_mysql_util::schema_info(
        conn.deref_mut(),
        &table_schema_request,
        &text_cols_map,
        &ignore_cols_map,
    )
    .await
    .map_err(|err| match err {
        mz_mysql_util::MySqlError::UnsupportedDataTypes { columns } => {
            PlanError::from(MySqlSourcePurificationError::UnrecognizedTypes {
                cols: columns
                    .into_iter()
                    .map(|c| (c.qualified_table_name, c.column_name, c.column_type))
                    .collect(),
            })
        }
        mz_mysql_util::MySqlError::DuplicatedColumnNames {
            qualified_table_name,
            columns,
        } => PlanError::from(MySqlSourcePurificationError::DuplicatedColumnNames(
            qualified_table_name,
            columns,
        )),
        _ => err.into(),
    })?;

    if tables.is_empty() {
        Err(MySqlSourcePurificationError::EmptyDatabase)?;
    }

    let mysql_catalog = derive_catalog_from_tables(&tables)?;

    let mut validated_requested_subsources = vec![];
    match referenced_subsources
        .as_mut()
        .ok_or(MySqlSourcePurificationError::RequiresReferencedSubsources)?
    {
        ReferencedSubsources::All => {
            for table in &tables {
                let upstream_name = mysql_upstream_name(table)?;
                let subsource_name =
                    super::subsource_name_gen(unresolved_source_name, &table.name)?;
                validated_requested_subsources.push(RequestedSubsource {
                    upstream_name,
                    subsource_name,
                    table,
                });
            }
        }
        ReferencedSubsources::SubsetSchemas(schemas) => {
            let available_schemas: BTreeSet<_> =
                tables.iter().map(|t| t.schema_name.as_str()).collect();
            let requested_schemas: BTreeSet<_> = schemas.iter().map(|s| s.as_str()).collect();
            let missing_schemas: Vec<_> = requested_schemas
                .difference(&available_schemas)
                .map(|s| s.to_string())
                .collect();
            if !missing_schemas.is_empty() {
                Err(MySqlSourcePurificationError::NoTablesFoundForSchemas(
                    missing_schemas,
                ))?;
            }

            for table in &tables {
                if !requested_schemas.contains(table.schema_name.as_str()) {
                    continue;
                }

                let upstream_name = mysql_upstream_name(table)?;
                let subsource_name =
                    super::subsource_name_gen(unresolved_source_name, &table.name)?;
                validated_requested_subsources.push(RequestedSubsource {
                    upstream_name,
                    subsource_name,
                    table,
                });
            }
        }
        ReferencedSubsources::SubsetTables(subsources) => {
            // The user manually selected a subset of upstream tables so we need to
            // validate that the names actually exist and are not ambiguous
            validated_requested_subsources =
                super::subsource_gen(subsources, &mysql_catalog, unresolved_source_name)?;

            // subsource_gen automatically inserts the "fully qualified
            // name," which for MySQL sources includes the fake database
            // name.
            for requested_subsource in validated_requested_subsources.iter_mut() {
                let fake_database_name = requested_subsource.upstream_name.0.remove(0);
                if fake_database_name.as_str() != MYSQL_DATABASE_FAKE_NAME {
                    sql_bail!(
                            "[internal error]: expected first element of upstream reference to be {}, but is {}",
                            MYSQL_DATABASE_FAKE_NAME,
                            fake_database_name.as_str()
                        );
                }
            }
        }
    }

    if validated_requested_subsources.is_empty() {
        sql_bail!(
            "[internal error]: MySQL source must ingest at least one table, but {} matched none",
            referenced_subsources.as_ref().unwrap().to_ast_string()
        );
    }

    super::validate_subsource_names(&validated_requested_subsources)?;

    validate_requested_subsources_privileges(&validated_requested_subsources, conn).await?;

    let scx = StatementContext::new(None, catalog);
    let new_subsources =
        generate_targeted_subsources(&scx, resolved_source_name, validated_requested_subsources)?;

    Ok(PurifiedSubsources {
        new_subsources,
        // Normalize column options and remove unused column references.
        normalized_text_columns: normalize_column_refs(text_columns, &mysql_catalog)?,
        normalized_ignore_columns: normalize_column_refs(ignore_columns, &mysql_catalog)?,
        tables,
    })
}
