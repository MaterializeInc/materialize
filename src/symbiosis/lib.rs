// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

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

use std::collections::HashMap;
use std::convert::TryInto;

use chrono::Utc;
use failure::{bail, format_err};
use sql_parser::ast::ColumnOption;
use sql_parser::ast::{DataType, ObjectType, Statement};
use tokio_postgres::types::FromSql;

use catalog::names::FullName;
use catalog::Catalog;
use repr::decimal::Significand;
use repr::jsonb::Jsonb;
use repr::{ColumnType, Datum, RelationDesc, RelationType, Row, RowPacker, ScalarType};
use sql::{normalize, scalar_type_from_sql, MutationKind, Plan, Session, StatementContext};

pub struct Postgres {
    client: tokio_postgres::Client,
    table_types: HashMap<FullName, (Vec<DataType>, RelationDesc)>,
}

impl Postgres {
    pub async fn open_and_erase(url: &str) -> Result<Self, failure::Error> {
        let (client, conn) = tokio_postgres::connect(url, tokio_postgres::NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = conn.await {
                panic!("connection error: {}", e);
            }
        });

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

    pub fn can_handle(&self, stmt: &Statement) -> bool {
        match stmt {
            Statement::CreateTable { .. }
            | Statement::DropObjects { .. }
            | Statement::Delete { .. }
            | Statement::Insert { .. }
            | Statement::Update { .. } => true,
            _ => false,
        }
    }

    pub async fn execute(
        &mut self,
        catalog: &Catalog,
        session: &Session,
        stmt: &Statement,
    ) -> Result<Plan, failure::Error> {
        let scx = StatementContext { catalog, session };
        Ok(match stmt {
            Statement::CreateTable {
                name,
                columns,
                constraints,
                if_not_exists,
                ..
            } => {
                self.client.execute(&*stmt.to_string(), &[]).await?;
                let sql_types = columns
                    .iter()
                    .map(|column| column.data_type.clone())
                    .collect::<Vec<_>>();
                let mut typ = RelationType::new(
                    columns
                        .iter()
                        .map(|column| {
                            Ok(ColumnType {
                                scalar_type: scalar_type_from_sql(&column.data_type)?,
                                nullable: !column
                                    .options
                                    .iter()
                                    .any(|o| o.option == ColumnOption::NotNull),
                            })
                        })
                        .collect::<Result<Vec<_>, failure::Error>>()?,
                );
                let names = columns
                    .iter()
                    .map(|c| Some(sql::normalize::column_name(c.name.clone())));

                for (index, column) in columns.iter().enumerate() {
                    for option in column.options.iter() {
                        if let ColumnOption::Unique { is_primary } = option.option {
                            typ = typ.add_keys(vec![index]);
                            if is_primary {
                                typ.column_types[index] =
                                    typ.column_types[index].clone().nullable(false);
                            }
                        }
                    }
                }

                for constraint in constraints {
                    use sql_parser::ast::TableConstraint;
                    if let TableConstraint::Unique {
                        name: _,
                        columns: cols,
                        is_primary,
                    } = constraint
                    {
                        let keys = cols
                            .iter()
                            .map(|ident| {
                                columns
                                    .iter()
                                    .position(|c| ident == &c.name)
                                    .expect("Column named in UNIQUE constraint not found")
                            })
                            .collect::<Vec<_>>();

                        if *is_primary {
                            for key in keys.iter() {
                                typ.column_types[*key].set_nullable(false);
                            }
                        }
                        typ = typ.add_keys(keys);
                    }
                }

                let name = scx.allocate_name(normalize::object_name(name.clone())?);
                let desc = RelationDesc::new(typ, names);
                self.table_types
                    .insert(name.clone(), (sql_types, desc.clone()));
                Plan::CreateTable {
                    name,
                    desc,
                    if_not_exists: *if_not_exists,
                }
            }
            Statement::DropObjects {
                names,
                object_type: ObjectType::Table,
                if_exists,
                ..
            } => {
                self.client.execute(&*stmt.to_string(), &[]).await?;
                let mut items = vec![];
                for name in names {
                    let name = match scx.resolve_name(name.clone()) {
                        Ok(name) => name,
                        Err(err) => {
                            if *if_exists {
                                continue;
                            } else {
                                return Err(err);
                            }
                        }
                    };
                    match catalog.try_get(&name) {
                        None => {
                            if !if_exists {
                                bail!("internal error: table {} missing from catalog", name);
                            }
                        }
                        Some(entry) => {
                            items.push(entry.id());
                        }
                    }
                }
                Plan::DropItems {
                    items,
                    ty: ObjectType::Table,
                }
            }
            Statement::Delete { table_name, .. } => {
                let mut updates = vec![];
                let table_name = scx.resolve_name(table_name.clone())?;
                let sql = format!("{} RETURNING *", stmt.to_string());
                for row in self.run_query(&table_name, sql).await? {
                    updates.push((row, -1));
                }
                let affected_rows = updates.len();
                Plan::SendDiffs {
                    id: catalog.get(&table_name)?.id(),
                    updates,
                    affected_rows,
                    kind: MutationKind::Delete,
                }
            }
            Statement::Insert { table_name, .. } => {
                let mut updates = vec![];
                let table_name = scx.resolve_name(table_name.clone())?;
                let sql = format!("{} RETURNING *", stmt.to_string());
                for row in self.run_query(&table_name, sql).await? {
                    updates.push((row, 1));
                }
                let affected_rows = updates.len();
                Plan::SendDiffs {
                    id: catalog.get(&table_name)?.id(),
                    updates,
                    affected_rows,
                    kind: MutationKind::Insert,
                }
            }
            Statement::Update {
                table_name,
                selection,
                ..
            } => {
                let mut updates = vec![];
                let mut sql = format!("SELECT * FROM {}", table_name);
                let table_name = scx.resolve_name(table_name.clone())?;
                if let Some(selection) = selection {
                    sql += &format!(" WHERE {}", selection);
                }
                for row in self.run_query(&table_name, sql).await? {
                    updates.push((row, -1))
                }
                let affected_rows = updates.len();
                let sql = format!("{} RETURNING *", stmt.to_string());
                for row in self.run_query(&table_name, sql).await? {
                    updates.push((row, 1));
                }
                assert_eq!(affected_rows * 2, updates.len());
                Plan::SendDiffs {
                    id: catalog.get(&table_name)?.id(),
                    updates,
                    affected_rows,
                    kind: MutationKind::Update,
                }
            }
            _ => bail!("Unsupported symbiosis statement: {:?}", stmt),
        })
    }

    async fn run_query(
        &mut self,
        table_name: &FullName,
        query: String,
    ) -> Result<Vec<Row>, failure::Error> {
        let (sql_types, desc) = self
            .table_types
            .get(table_name)
            .ok_or_else(|| format_err!("Unknown table: {:?}", table_name))?
            .clone();
        let mut rows = vec![];
        let postgres_rows = self.client.query(&*query, &[]).await?;
        for postgres_row in postgres_rows.iter() {
            // NOTE We can't use Row::pack here because PostgresRow::get_opt insists on allocating data for strings,
            // which has to live somewhere while the iterator is running.
            let mut row = RowPacker::new();
            for c in 0..postgres_row.len() {
                row = push_column(
                    row,
                    &postgres_row,
                    c,
                    &sql_types[c],
                    desc.typ().column_types[c].nullable,
                )?;
            }
            rows.push(row.finish());
        }
        Ok(rows)
    }
}

fn push_column(
    mut row: RowPacker,
    postgres_row: &tokio_postgres::Row,
    i: usize,
    sql_type: &DataType,
    nullable: bool,
) -> Result<RowPacker, failure::Error> {
    // NOTE this needs to stay in sync with materialize::sql::scalar_type_from_sql
    // in some cases, we use slightly different representations than postgres does for the same sql types, so we have to be careful about conversions
    match sql_type {
        DataType::Boolean => {
            let bool = get_column_inner::<bool>(postgres_row, i, nullable)?;
            row.push(bool.into());
        }
        DataType::Char(_) | DataType::Varchar(_) | DataType::Text => {
            let string = get_column_inner::<String>(postgres_row, i, nullable)?;
            row.push(string.as_deref().into());
        }
        DataType::SmallInt => {
            let i = get_column_inner::<i16>(postgres_row, i, nullable)?.map(|i| i32::from(i));
            row.push(i.into());
        }
        DataType::Int => {
            let i = get_column_inner::<i32>(postgres_row, i, nullable)?;
            row.push(i.into());
        }
        DataType::BigInt => {
            let i = get_column_inner::<i64>(postgres_row, i, nullable)?;
            row.push(i.into());
        }
        DataType::Float(p) => {
            if p.unwrap_or(53) <= 24 {
                let f = get_column_inner::<f32>(postgres_row, i, nullable)?.map(|f| f64::from(f));
                row.push(f.into());
            } else {
                let f = get_column_inner::<f64>(postgres_row, i, nullable)?;
                row.push(f.into());
            }
        }
        DataType::Real => {
            let f = get_column_inner::<f32>(postgres_row, i, nullable)?.map(|f| f64::from(f));
            row.push(f.into());
        }
        DataType::Double => {
            let f = get_column_inner::<f64>(postgres_row, i, nullable)?;
            row.push(f.into());
        }
        DataType::Date => {
            let d: chrono::NaiveDate =
                get_column_inner::<chrono::NaiveDate>(postgres_row, i, nullable)?.unwrap();
            row.push(Datum::Date(d));
        }
        DataType::Timestamp => {
            let d: chrono::NaiveDateTime =
                get_column_inner::<chrono::NaiveDateTime>(postgres_row, i, nullable)?.unwrap();
            row.push(Datum::Timestamp(d));
        }
        DataType::TimestampTz => {
            let d: chrono::DateTime<Utc> =
                get_column_inner::<chrono::DateTime<Utc>>(postgres_row, i, nullable)?.unwrap();
            row.push(Datum::TimestampTz(d));
        }
        DataType::Interval => {
            let iv = get_column_inner::<pgrepr::Interval>(postgres_row, i, nullable)?.unwrap();
            row.push(Datum::Interval(iv.0));
        }
        DataType::Decimal(_, _) => {
            let desired_scale = match scalar_type_from_sql(sql_type).unwrap() {
                ScalarType::Decimal(_precision, desired_scale) => desired_scale,
                _ => unreachable!(),
            };
            match get_column_inner::<pgrepr::Numeric>(postgres_row, i, nullable)? {
                None => row.push(Datum::Null),
                Some(d) => {
                    let mut significand = d.0.significand();
                    // TODO(jamii) lots of potential for unchecked edge cases here eg 10^scale_correction could overflow
                    // current representation is `significand * 10^current_scale`
                    // want to get to `significand2 * 10^desired_scale`
                    // so `significand2 = significand * 10^(current_scale - desired_scale)`
                    let scale_correction = (d.0.scale() as isize) - (desired_scale as isize);
                    if scale_correction > 0 {
                        significand /= 10i128.pow(scale_correction.try_into()?);
                    } else {
                        significand *= 10i128.pow((-scale_correction).try_into()?);
                    };
                    row.push(Significand::new(significand).into());
                }
            }
        }
        DataType::Bytea => {
            let bytes = get_column_inner::<Vec<u8>>(postgres_row, i, nullable)?;
            row.push(bytes.as_deref().into());
        }
        DataType::Jsonb => {
            let serde = get_column_inner::<serde_json::Value>(postgres_row, i, nullable)?;
            if let Some(serde) = serde {
                row = Jsonb::new(serde)?.pack_into(row)
            } else {
                row.push(Datum::Null)
            }
        }
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
) -> Result<Option<T>, failure::Error>
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
