// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Symbiosis mode.
//!
//! In symbiosis mode, Materialize will conjoin with an OLTP database to
//! masquerade as a HTAP system. All DDL statements and writes will be routed to
//! the OLTP database (like `CREATE TABLE`, `INSERT`, etc.), while reads will be
//! routed through Materialize. Changes to the tables in the OLTP database are
//! automatically streamed through Materialize.
//!
//! The only supported OLTP database at the moment is PostgreSQL. Supporting
//! other databases is complicated by the fact that we roughly followe
//! Postgres's SQL semantics; using, say, MySQL, would be rather confusing,
//! because `INSERT`, `UPDATE`, and `DELETE` statements would be subject to a
//! wildly different set of SQL semantics than `SELECT` statements.
//!
//! Symbiosis mode is only suitable for development. It is likely to be
//! extremely slow and inefficient on large data sets.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::env;
use std::rc::Rc;

use anyhow::{anyhow, bail};
use chrono::Utc;
use tokio_postgres::types::FromSql;
use uuid::Uuid;

use pgrepr::Jsonb;
use repr::adt::numeric;
use repr::{Datum, RelationDesc, RelationType, Row};
use sql::ast::{
    ColumnOption, CreateSchemaStatement, CreateTableStatement, DataType, DeleteStatement,
    DropObjectsStatement, Expr, InsertStatement, ObjectType, Raw, Statement, TableConstraint,
    UpdateStatement,
};
use sql::catalog::SessionCatalog;
use sql::names::{DatabaseSpecifier, FullName};
use sql::normalize;
use sql::plan::{
    plan_default_expr, resolve_names_data_type, Aug, CreateSchemaPlan, CreateTablePlan,
    DropItemsPlan, MutationKind, Plan, PlanContext, SendDiffsPlan, StatementContext, Table,
};

pub struct Postgres {
    client: tokio_postgres::Client,
    table_types: HashMap<FullName, (Vec<DataType<Aug>>, RelationDesc)>,
}

impl Postgres {
    pub async fn open_and_erase(url: &str) -> Result<Self, anyhow::Error> {
        let mut config: tokio_postgres::Config = url.parse()?;
        let username = whoami::username();
        if config.get_user().is_none() {
            config.user(env::var("PGUSER").ok().as_deref().unwrap_or(&username));
        }
        if config.get_password().is_none() {
            if let Ok(password) = env::var("PGPASSWORD") {
                config.password(password);
            }
        }
        if config.get_dbname().is_none() {
            if let Ok(dbname) = env::var("PGDATABASE") {
                config.dbname(&dbname);
            }
        }
        if config.get_hosts().is_empty() {
            config.host(env::var("PGHOST").ok().as_deref().unwrap_or("localhost"));
        }
        let (client, conn) = config
            .connect(tokio_postgres::NoTls)
            .await
            .map_err(|err| anyhow!("Postgres connection failed: {}", err))?;

        tokio::spawn(async move {
            if let Err(e) = conn.await {
                panic!("connection error: {}", e);
            }
        });

        // Some postgres servers (or clients?) don't default to UTC, which is the
        // only value materialize supports. Enforce that here because otherwise the
        // sqllogictest results can change when dealing with timestamptz types.
        client.execute("SET TIMEZONE TO UTC", &[]).await?;

        // drop all tables
        client
            .execute(
                r#"
DO $$ DECLARE
    r RECORD;
BEGIN
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = current_schema()) LOOP
        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
    END LOOP;
    -- DROP all schemas (and any tables within) non-default schemas.
    FOR r IN (
        SELECT nspname
        FROM pg_namespace
        WHERE
            nspname !~ '^pg_' AND
            nspname <> 'information_schema' AND
            nspname <> current_schema()
        ) LOOP
        EXECUTE 'DROP SCHEMA IF EXISTS ' || quote_ident(r.nspname) || ' CASCADE';
    END LOOP;
END $$;
"#,
                &[],
            )
            .await?;
        Ok(Self {
            client,
            table_types: HashMap::new(),
        })
    }

    pub fn can_handle(&self, stmt: &Statement<Raw>) -> bool {
        matches!(
            stmt,
            Statement::CreateTable { .. }
                | Statement::CreateSchema { .. }
                | Statement::DropObjects { .. }
                | Statement::Delete { .. }
                | Statement::Insert { .. }
                | Statement::Update { .. }
        )
    }

    pub async fn execute(
        &mut self,
        pcx: &PlanContext,
        catalog: &dyn SessionCatalog,
        stmt: &Statement<Raw>,
    ) -> Result<Plan, anyhow::Error> {
        let scx = StatementContext::new(Some(pcx), catalog, Rc::new(RefCell::new(BTreeMap::new())));
        Ok(match stmt {
            Statement::CreateTable(CreateTableStatement {
                name,
                columns,
                constraints,
                if_not_exists,
                temporary,
                ..
            }) => {
                let names: Vec<_> = columns
                    .iter()
                    .map(|c| Some(sql::normalize::column_name(c.name.clone())))
                    .collect();

                // Build initial relation type that handles declared data types
                // and NOT NULL, UNIQUE, and PRIMARY KEY constraints.
                let mut column_types = Vec::with_capacity(columns.len());
                let mut defaults = Vec::with_capacity(columns.len());
                let mut depends_on = Vec::new();
                let mut keys = vec![];

                let mut sql_types: Vec<_> = Vec::with_capacity(columns.len());
                for (index, column) in columns.iter().enumerate() {
                    let (aug_data_type, ids) =
                        resolve_names_data_type(&scx, column.data_type.clone())?;
                    let ty = sql::plan::scalar_type_from_sql(&scx, &aug_data_type)?;
                    sql_types.push(aug_data_type);
                    let mut nullable = true;
                    let mut default = Expr::null();

                    for option in &column.options {
                        match &option.option {
                            ColumnOption::NotNull => nullable = false,
                            ColumnOption::Default(expr) => {
                                let (_, expr_depends_on) = plan_default_expr(&scx, expr, &ty)?;
                                depends_on.extend(expr_depends_on);
                                default = expr.clone();
                            }
                            // PRIMARY KEY implies UNIQUE and NOT NULL.
                            ColumnOption::Unique { is_primary } => {
                                keys.push(vec![index]);
                                if *is_primary {
                                    nullable = false;
                                }
                            }
                            other => bail!("unsupported column constraint: {}", other),
                        }
                    }
                    column_types.push(ty.nullable(nullable));
                    defaults.push(default);
                    depends_on.extend(ids);
                }
                let mut typ = RelationType { column_types, keys };

                // Handle table-level UNIQUE and PRIMARY KEY constraints.
                // PRIMARY KEY implies UNIQUE and NOT NULL.
                for constraint in constraints {
                    if let TableConstraint::Unique {
                        name: _,
                        columns,
                        is_primary,
                    } = constraint
                    {
                        let mut key = vec![];
                        for column in columns {
                            let name = normalize::column_name(column.clone());
                            match names.iter().position(|n| n.as_ref() == Some(&name)) {
                                None => bail!("unknown column {} in unique constraint", name),
                                Some(i) => key.push(i),
                            }
                        }
                        if *is_primary {
                            for i in key.iter() {
                                typ.column_types[*i].nullable = false;
                            }
                        }
                        typ = typ.with_key(key);
                    }
                }

                self.client.execute(&*stmt.to_string(), &[]).await?;
                let name = scx.allocate_name(normalize::unresolved_object_name(name.clone())?);
                let desc = RelationDesc::new(typ, names);
                self.table_types
                    .insert(name.clone(), (sql_types, desc.clone()));

                let temporary = *temporary;
                let table = Table {
                    create_sql: stmt.to_string(),
                    desc,
                    defaults,
                    temporary,
                    depends_on,
                };
                Plan::CreateTable(CreateTablePlan {
                    name,
                    table,
                    if_not_exists: *if_not_exists,
                })
            }
            Statement::CreateSchema(CreateSchemaStatement {
                name,
                if_not_exists,
            }) => {
                self.client.execute(&*stmt.to_string(), &[]).await?;
                if name.0.len() > 2 {
                    bail!("schema name {} has more than two components", name);
                }
                let mut name = name.0.clone();
                let schema_name = normalize::ident(
                    name.pop()
                        .expect("names always have at least one component"),
                );
                let database_name = match name.pop() {
                    None => DatabaseSpecifier::Name(scx.catalog.default_database().into()),
                    Some(n) => DatabaseSpecifier::Name(normalize::ident(n)),
                };
                Plan::CreateSchema(CreateSchemaPlan {
                    database_name,
                    schema_name,
                    if_not_exists: *if_not_exists,
                })
            }
            Statement::DropObjects(DropObjectsStatement {
                names,
                object_type: ObjectType::Table,
                if_exists,
                ..
            }) => {
                self.client.execute(&*stmt.to_string(), &[]).await?;
                let mut items = vec![];
                for name in names {
                    match scx.resolve_item(name.clone()) {
                        Ok(item) => items.push(item.id()),
                        Err(err) => {
                            if *if_exists {
                                continue;
                            } else {
                                return Err(err.into());
                            }
                        }
                    }
                }
                Plan::DropItems(DropItemsPlan {
                    items,
                    ty: ObjectType::Table,
                })
            }
            Statement::Delete(DeleteStatement { table_name, .. }) => {
                let mut updates = vec![];
                let table = scx.resolve_item(table_name.name().clone())?;
                let sql = format!("{} RETURNING *", stmt.to_string());
                for row in self.run_query(table.name(), sql, 0).await? {
                    updates.push((row, -1));
                }
                Plan::SendDiffs(SendDiffsPlan {
                    id: table.id(),
                    updates,
                    kind: MutationKind::Delete,
                })
            }
            Statement::Insert(InsertStatement { table_name, .. }) => {
                let mut updates = vec![];
                let table = scx.resolve_item(table_name.clone())?;
                // RETURNING cannot return zero columns, but we might be
                // executing INSERT INTO t DEFAULT VALUES where t is a zero
                // arity table. So use a time-honored trick of always including
                // a junk column in RETURNING, then stripping that column out.
                let sql = format!("{} RETURNING *, 1", stmt.to_string());
                for row in self.run_query(table.name(), sql, 1).await? {
                    updates.push((row, 1));
                }
                Plan::SendDiffs(SendDiffsPlan {
                    id: table.id(),
                    updates,
                    kind: MutationKind::Insert,
                })
            }
            Statement::Update(UpdateStatement {
                table_name,
                selection,
                ..
            }) => {
                let mut updates = vec![];
                let mut sql = format!("SELECT * FROM {}", &table_name.name());
                let table = scx.resolve_item(table_name.name().clone())?;
                if let Some(selection) = selection {
                    sql += &format!(" WHERE {}", selection);
                }
                for row in self.run_query(table.name(), sql, 0).await? {
                    updates.push((row, -1))
                }
                let sql = format!("{} RETURNING *", stmt.to_string());
                for row in self.run_query(table.name(), sql, 0).await? {
                    updates.push((row, 1));
                }
                Plan::SendDiffs(SendDiffsPlan {
                    id: table.id(),
                    updates,
                    kind: MutationKind::Update,
                })
            }
            _ => bail!("Unsupported symbiosis statement: {:?}", stmt),
        })
    }

    async fn run_query(
        &mut self,
        table_name: &FullName,
        query: String,
        junk: usize,
    ) -> Result<Vec<Row>, anyhow::Error> {
        let (sql_types, desc) = self
            .table_types
            .get(table_name)
            .ok_or_else(|| anyhow!("Unknown table: {:?}", table_name))?
            .clone();
        let mut rows = vec![];
        let postgres_rows = self.client.query(&*query, &[]).await?;
        let mut row = Row::default();
        for postgres_row in postgres_rows.iter() {
            for c in 0..postgres_row.len() - junk {
                row = push_column(
                    row,
                    &postgres_row,
                    c,
                    &sql_types[c],
                    desc.typ().column_types[c].nullable,
                )?;
            }
            rows.push(row.finish_and_reuse());
        }
        Ok(rows)
    }
}

fn push_column(
    mut row: Row,
    postgres_row: &tokio_postgres::Row,
    i: usize,
    sql_type: &DataType<Aug>,
    nullable: bool,
) -> Result<Row, anyhow::Error> {
    // NOTE this needs to stay in sync with
    // materialize::sql::scalar_type_from_sql in some cases, we use slightly
    // different representations than postgres does for the same sql types, so
    // we have to be careful about conversions
    match sql_type {
        DataType::Other { name, typ_mod } => match name.raw_name().to_string().as_str() {
            "pg_catalog.bool" => {
                let bool = get_column_inner::<bool>(postgres_row, i, nullable)?;
                row.push(Datum::from(bool));
            }
            "pg_catalog.bytea" => {
                let bytes = get_column_inner::<Vec<u8>>(postgres_row, i, nullable)?;
                row.push(Datum::from(bytes.as_deref()));
            }
            "pg_catalog.text" => {
                let string = get_column_inner::<String>(postgres_row, i, nullable)?;
                row.push(Datum::from(string.as_deref()));
            }
            "pg_catalog.bpchar" | "pg_catalog.char" => {
                let length = repr::adt::char::extract_typ_mod(&typ_mod)?;
                match get_column_inner::<String>(postgres_row, i, nullable)? {
                    None => row.push(Datum::Null),
                    Some(s) => {
                        let s = repr::adt::char::format_str_trim(&s, length, true)?;
                        row.push(Datum::String(&s));
                    }
                }
            }
            "pg_catalog.varchar" => {
                let length = repr::adt::varchar::extract_typ_mod(&typ_mod)?;
                match get_column_inner::<String>(postgres_row, i, nullable)? {
                    None => row.push(Datum::Null),
                    Some(s) => {
                        let s = repr::adt::varchar::format_str(&s, length, true)?;
                        row.push(Datum::String(&s));
                    }
                }
            }
            "pg_catalog.date" => {
                let d: chrono::NaiveDate =
                    get_column_inner::<chrono::NaiveDate>(postgres_row, i, nullable)?.unwrap();
                row.push(Datum::Date(d));
            }
            "pg_catalog.float4" => {
                let f = get_column_inner::<f32>(postgres_row, i, nullable)?.map(f32::from);
                row.push(Datum::from(f));
            }
            "pg_catalog.float8" => {
                let f = get_column_inner::<f64>(postgres_row, i, nullable)?;
                row.push(Datum::from(f));
            }
            "pg_catalog.int2" => {
                let i = get_column_inner::<i16>(postgres_row, i, nullable)?;
                row.push(Datum::from(i));
            }
            "pg_catalog.int4" => {
                let i = get_column_inner::<i32>(postgres_row, i, nullable)?;
                row.push(Datum::from(i));
            }
            "pg_catalog.int8" => {
                let i = get_column_inner::<i64>(postgres_row, i, nullable)?;
                row.push(Datum::from(i));
            }
            "pg_catalog.interval" => {
                let iv = get_column_inner::<pgrepr::Interval>(postgres_row, i, nullable)?.unwrap();
                row.push(Datum::Interval(iv.0));
            }
            "pg_catalog.jsonb" => {
                let jsonb = get_column_inner::<Jsonb>(postgres_row, i, nullable)?;
                if let Some(jsonb) = jsonb {
                    row.extend_by_row(&jsonb.0.into_row())
                } else {
                    row.push(Datum::Null)
                }
            }
            "pg_catalog.numeric" => {
                let desired_scale = repr::adt::numeric::extract_typ_mod(typ_mod)?;
                match get_column_inner::<pgrepr::Numeric>(postgres_row, i, nullable)? {
                    None => row.push(Datum::Null),
                    Some(mut d) => {
                        if let Some(scale) = desired_scale {
                            numeric::rescale(&mut d.0 .0, scale)?;
                        }
                        row.push(Datum::Numeric(d.0));
                    }
                }
            }
            "pg_catalog.timestamp" => {
                let d: chrono::NaiveDateTime =
                    get_column_inner::<chrono::NaiveDateTime>(postgres_row, i, nullable)?.unwrap();
                row.push(Datum::Timestamp(d));
            }
            "pg_catalog.timestamptz" => {
                let d: chrono::DateTime<Utc> =
                    get_column_inner::<chrono::DateTime<Utc>>(postgres_row, i, nullable)?.unwrap();
                row.push(Datum::TimestampTz(d));
            }
            "pg_catalog.uuid" => {
                let u = get_column_inner::<Uuid>(postgres_row, i, nullable)?.unwrap();
                row.push(Datum::Uuid(u));
            }
            _ => bail!(
                "Postgres to materialize conversion not yet supported for {:?}",
                sql_type
            ),
        },
        _ => bail!(
            "Postgres to materialize conversion not yet supported for {:?}",
            sql_type
        ),
    }
    Ok(row)
}

fn get_column_inner<'a, T>(
    postgres_row: &'a tokio_postgres::Row,
    i: usize,
    nullable: bool,
) -> Result<Option<T>, anyhow::Error>
where
    T: FromSql<'a>,
{
    if nullable {
        let value: Option<T> = postgres_row.try_get(i)?;
        Ok(value)
    } else {
        let value: T = postgres_row.try_get(i)?;
        Ok(Some(value))
    }
}
