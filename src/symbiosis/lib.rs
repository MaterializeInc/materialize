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
use std::convert::{TryFrom, TryInto};
use std::env;

use ::postgres::rows::Row as PostgresRow;
use ::postgres::types::FromSql;
use ::postgres::{Connection, TlsMode};
use failure::{bail, format_err};
use postgres::params::{ConnectParams, IntoConnectParams};
use rust_decimal::Decimal;
use sqlparser::ast::ColumnOption;
use sqlparser::ast::{DataType, ObjectType, Statement};

use ore::option::OptionExt;
use repr::decimal::Significand;
use repr::{
    ColumnType, Datum, Interval, PackableRow, QualName, RelationDesc, RelationType, Row, RowPacker,
    ScalarType,
};
use sql::{scalar_type_from_sql, MutationKind, Plan};

pub struct Postgres {
    conn: Connection,
    table_types: HashMap<QualName, (Vec<DataType>, RelationDesc)>,
    packer: RowPacker,
}

impl Postgres {
    pub fn open_and_erase(url: &str) -> Result<Self, failure::Error> {
        let params = url
            .into_connect_params()
            .map_err(failure::Error::from_boxed_compat)?;

        // Fill in default values for parameters using the same logic that libpq
        // uses, which involves reading various environment variables. The
        // `postgres` crate makes this maximally annoying.
        let params = {
            let mut new_params = ConnectParams::builder();

            if let Some(user) = params.user() {
                let password = user
                    .password()
                    .owned()
                    .or_else(|| env::var("PGPASSWORD").ok());
                new_params.user(user.name(), password.mz_as_deref());
            } else {
                let name = env::var("PGUSER").unwrap_or_else(|_| whoami::username());
                let password = env::var("PGPASSWORD").ok();
                new_params.user(&name, password.mz_as_deref());
            }

            if let Some(database) = params
                .database()
                .owned()
                .or_else(|| env::var("PGDATABASE").ok())
            {
                new_params.database(&database);
            }
            new_params.port(params.port());

            let mut host = params.host().clone();
            if let postgres::params::Host::Tcp(hostname) = &host {
                if hostname == "" {
                    let host_str = env::var("PGHOST").unwrap_or_else(|_| "localhost".into());
                    host = postgres::params::Host::Tcp(host_str);
                }
            }

            new_params.build(host)
        };

        let conn = Connection::connect(params, TlsMode::None)?;
        // drop all tables
        conn.execute(
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
        )?;
        Ok(Self {
            conn,
            table_types: HashMap::new(),
            packer: RowPacker::new(),
        })
    }

    pub fn can_handle(&self, stmt: &Statement) -> bool {
        match stmt {
            Statement::CreateTable { .. }
            | Statement::Drop { .. }
            | Statement::Delete { .. }
            | Statement::Insert { .. }
            | Statement::Update { .. } => true,
            _ => false,
        }
    }

    pub fn execute(&mut self, stmt: &Statement) -> Result<Plan, failure::Error> {
        Ok(match stmt {
            Statement::CreateTable {
                name,
                columns,
                constraints,
                ..
            } => {
                self.conn.execute(&stmt.to_string(), &[])?;
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
                for constraint in constraints {
                    use sqlparser::ast::TableConstraint;
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

                let names = columns
                    .iter()
                    .cloned()
                    .map(|column| QualName::try_from(column.name).map(Some))
                    .collect::<Result<Vec<_>, _>>()?;

                let desc = RelationDesc::new(typ, names);
                self.table_types
                    .insert(name.try_into()?, (sql_types, desc.clone()));
                Plan::CreateTable {
                    name: name.try_into()?,
                    desc,
                }
            }
            Statement::Drop {
                names,
                object_type: ObjectType::Table,
                ..
            } => {
                self.conn.execute(&stmt.to_string(), &[])?;
                Plan::DropItems(
                    names
                        .iter()
                        .map(|name| match name.try_into() {
                            Ok(val) => Ok(val),
                            Err(e) => {
                                failure::bail!("unable to drop invalid name {}: {}", name, e);
                            }
                        })
                        .collect::<Result<_, _>>()?,
                    ObjectType::Table,
                )
            }
            Statement::Delete { table_name, .. } => {
                let mut updates = vec![];
                let table_name = QualName::try_from(table_name)?;
                let sql = format!("{} RETURNING *", stmt.to_string());
                for row in self.run_query(&table_name, sql)? {
                    updates.push((row, -1));
                }
                let affected_rows = updates.len();
                Plan::SendDiffs {
                    name: table_name,
                    updates,
                    affected_rows,
                    kind: MutationKind::Delete,
                }
            }
            Statement::Insert { table_name, .. } => {
                let mut updates = vec![];
                let table_name = table_name.try_into()?;
                let sql = format!("{} RETURNING *", stmt.to_string());
                for row in self.run_query(&table_name, sql)? {
                    updates.push((row, 1));
                }
                let affected_rows = updates.len();
                Plan::SendDiffs {
                    name: table_name,
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
                let table_name = QualName::try_from(table_name)?;
                let mut sql = format!("SELECT * FROM {}", table_name);
                if let Some(selection) = selection {
                    sql += &format!(" WHERE {}", selection);
                }
                for row in self.run_query(&table_name, sql)? {
                    updates.push((row, -1))
                }
                let affected_rows = updates.len();
                let sql = format!("{} RETURNING *", stmt.to_string());
                for row in self.run_query(&table_name, sql)? {
                    updates.push((row, 1));
                }
                assert_eq!(affected_rows * 2, updates.len());
                Plan::SendDiffs {
                    name: table_name.try_into()?,
                    updates,
                    affected_rows,
                    kind: MutationKind::Update,
                }
            }
            _ => bail!("Unsupported symbiosis statement: {:?}", stmt),
        })
    }

    fn run_query(
        &mut self,
        table_name: &QualName,
        query: String,
    ) -> Result<Vec<Row>, failure::Error> {
        let (sql_types, desc) = self
            .table_types
            .get(table_name)
            .ok_or_else(|| format_err!("Unknown table: {:?}", table_name))?
            .clone();
        let mut rows = vec![];
        let postgres_rows = self.conn.query(&*query, &[])?;
        for postgres_row in postgres_rows.iter() {
            // NOTE We can't use RowPacker::pack here because PostgresRow::get_opt insists on allocating data for strings,
            // which has to live somewhere while the iterator is running.
            let mut row = self.packer.packable();
            for c in 0..postgres_row.len() {
                push_column(
                    &mut row,
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
    row: &mut PackableRow,
    postgres_row: &PostgresRow,
    i: usize,
    sql_type: &DataType,
    nullable: bool,
) -> Result<(), failure::Error> {
    // NOTE this needs to stay in sync with materialize::sql::scalar_type_from_sql
    // in some cases, we use slightly different representations than postgres does for the same sql types, so we have to be careful about conversions
    match sql_type {
        DataType::Boolean => {
            let bool = get_column_inner::<bool>(postgres_row, i, nullable)?;
            row.push(bool.into());
        }
        DataType::Custom(name) if QualName::name_equals(name.clone(), "bool") => {
            let bool = get_column_inner::<bool>(postgres_row, i, nullable)?;
            row.push(bool.into());
        }
        DataType::Char(_) | DataType::Varchar(_) | DataType::Text => {
            let string = get_column_inner::<String>(postgres_row, i, nullable)?;
            row.push(string.mz_as_deref().into());
        }
        DataType::Custom(name) if QualName::name_equals(name.clone(), "string") => {
            let string = get_column_inner::<String>(postgres_row, i, nullable)?;
            row.push(string.mz_as_deref().into());
        }
        DataType::SmallInt => {
            let i = get_column_inner::<i16>(postgres_row, i, nullable)?.map(|i| i32::from(i));
            row.push(i.into());
        }
        DataType::Int => {
            let i = get_column_inner::<i32>(postgres_row, i, nullable)?.map(|i| i64::from(i));
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
        DataType::Interval => {
            let pgi =
                get_column_inner::<pg_interval::Interval>(postgres_row, i, nullable)?.unwrap();
            if pgi.months != 0 && (pgi.days != 0 || pgi.microseconds != 0) {
                bail!("can't handle pg intervals that have both months and times");
            }
            if pgi.months != 0 {
                row.push(Datum::Interval(Interval::Months(pgi.months.into())));
            } else {
                // TODO(quodlibetor): I can't find documentation about how
                // microseconds and days are supposed to sum before the epoch.
                // Hopefully we only ever end up with one. Since we don't
                // actually use pg in prod anywhere so I'm not digging further
                // until this breaks.
                if pgi.days > 0 && pgi.microseconds < 0 || pgi.days < 0 && pgi.microseconds > 0 {
                    panic!(
                        "postgres interval parts do not agree on sign days={} microseconds={}",
                        pgi.days, pgi.microseconds
                    );
                }
                let seconds = i64::from(pgi.days) * 86_400 + pgi.microseconds / 1_000_000;
                let nanos = (pgi.microseconds.abs() % 1_000_000) as u32 * 1_000;

                row.push(Datum::Interval(Interval::Duration {
                    is_positive: seconds >= 0,
                    duration: std::time::Duration::new(seconds.abs() as u64, nanos),
                }));
            }
        }
        DataType::Decimal(_, _) => {
            let desired_scale = match scalar_type_from_sql(sql_type).unwrap() {
                ScalarType::Decimal(_precision, desired_scale) => desired_scale,
                _ => unreachable!(),
            };
            match get_column_inner::<Decimal>(postgres_row, i, nullable)? {
                None => row.push(Datum::Null),
                Some(d) => {
                    let d = d.unpack();
                    let mut significand =
                        i128::from(d.lo) + (i128::from(d.mid) << 32) + (i128::from(d.hi) << 64);
                    // TODO(jamii) lots of potential for unchecked edge cases here eg 10^scale_correction could overflow
                    // current representation is `significand * 10^current_scale`
                    // want to get to `significand2 * 10^desired_scale`
                    // so `significand2 = significand * 10^(current_scale - desired_scale)`
                    let scale_correction = (d.scale as isize) - (desired_scale as isize);
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
            row.push(bytes.mz_as_deref().into());
        }
        _ => bail!(
            "Postgres to materialize conversion not yet supported for {:?}",
            sql_type
        ),
    }
    Ok(())
}

fn get_column_inner<T>(
    postgres_row: &PostgresRow,
    i: usize,
    nullable: bool,
) -> Result<Option<T>, failure::Error>
where
    T: FromSql,
{
    if nullable {
        let value: Option<T> = get_column_raw(postgres_row, i)?;
        Ok(value)
    } else {
        let value: T = get_column_raw(postgres_row, i)?;
        Ok(Some(value))
    }
}

fn get_column_raw<T>(postgres_row: &PostgresRow, i: usize) -> Result<T, failure::Error>
where
    T: FromSql,
{
    match postgres_row.get_opt(i) {
        None => bail!("missing column at index {}", i),
        Some(Ok(t)) => Ok(t),
        Some(Err(err)) => Err(err.into()),
    }
}
