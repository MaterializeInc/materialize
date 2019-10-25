// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Postgres glue for sqllogictest.

use std::collections::HashMap;
use std::convert::TryInto;
use std::env;

use ::postgres::rows::Row as PostgresRow;
use ::postgres::types::FromSql;
use ::postgres::{Connection, TlsMode};
use failure::{bail, format_err};
use rust_decimal::Decimal;
use sqlparser::ast::ColumnOption;
use sqlparser::ast::{DataType, ObjectType, Statement};

use ore::option::OptionExt;
use repr::decimal::Significand;
use repr::{ColumnType, Datum, Interval, RelationDesc, RelationType, Row, ScalarType};
use sql::scalar_type_from_sql;

pub struct Postgres {
    conn: Connection,
    table_types: HashMap<String, (Vec<DataType>, RelationDesc)>,
}

pub type Diff = Vec<(Row, isize)>;

#[derive(Debug, Clone)]
pub enum Outcome {
    Created(String, RelationDesc),
    Dropped(Vec<String>),
    Changed {
        table_name: String,
        updates: Diff,
        affected: usize,
    },
}

impl Postgres {
    pub fn open_and_erase() -> Result<Self, failure::Error> {
        // This roughly matches the environment variables that the official C
        // library for PostgreSQL, libpq, uses [0]. It would be nice if this
        // hunk of code lived in the Rust PostgreSQL library.
        //
        // [0]: https://www.postgresql.org/docs/current/libpq-envars.html
        let user = env::var("PGUSER").unwrap_or_else(|_| whoami::username());
        let password = env::var("PGPASSWORD").unwrap_or_else(|_| "".into());
        let dbname = env::var("PGDATABASE").unwrap_or_else(|_| user.clone());
        let port = match env::var("PGPORT") {
            Ok(port) => port.parse()?,
            Err(_) => 5432,
        };
        let host = env::var("PGHOST").unwrap_or_else(|_| "localhost".into());
        let url = format!(
            "postgresql://{}:{}@{}:{}/{}",
            user, password, host, port, dbname
        );
        let conn = Connection::connect(url, TlsMode::None)?;
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
        })
    }

    pub fn run_statement(
        &mut self,
        sql: &str,
        parsed: &Statement,
    ) -> Result<Outcome, failure::Error> {
        Ok(match parsed {
            Statement::CreateTable {
                name,
                columns,
                constraints,
                ..
            } => {
                self.conn.execute(sql, &[])?;
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
                let names = columns.iter().map(|column| Some(column.name.value.clone()));

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

                let desc = RelationDesc::new(typ, names);
                self.table_types
                    .insert(name.to_string(), (sql_types, desc.clone()));
                Outcome::Created(name.to_string(), desc)
            }
            Statement::Drop {
                names,
                object_type: ObjectType::Table,
                ..
            } => {
                self.conn.execute(sql, &[])?;
                Outcome::Dropped(names.iter().map(|name| name.to_string()).collect())
            }
            Statement::Delete { table_name, .. } => {
                let mut updates = vec![];
                let table_name = table_name.to_string();
                let sql = format!("{} RETURNING *", parsed.to_string());
                for row in self.run_query(&table_name, sql)? {
                    updates.push((row, -1));
                }
                let affected = updates.len();
                Outcome::Changed {
                    table_name,
                    updates,
                    affected,
                }
            }
            Statement::Insert { table_name, .. } => {
                let mut updates = vec![];
                let table_name = table_name.to_string();
                let sql = format!("{} RETURNING *", parsed.to_string());
                for row in self.run_query(&table_name, sql)? {
                    updates.push((row, 1));
                }
                let affected = updates.len();
                Outcome::Changed {
                    table_name,
                    updates,
                    affected,
                }
            }
            Statement::Update {
                table_name,
                selection,
                ..
            } => {
                let mut updates = vec![];
                let table_name = table_name.to_string();
                let mut sql = format!("SELECT * FROM {}", table_name);
                if let Some(selection) = selection {
                    sql += &format!(" WHERE {}", selection);
                }
                for row in self.run_query(&table_name, sql)? {
                    updates.push((row, -1))
                }
                let affected = updates.len();
                let sql = format!("{} RETURNING *", parsed.to_string());
                for row in self.run_query(&table_name, sql)? {
                    updates.push((row, 1));
                }
                assert_eq!(affected * 2, updates.len());
                Outcome::Changed {
                    table_name,
                    updates,
                    affected,
                }
            }
            _ => bail!("Unsupported statement: {:?}", parsed),
        })
    }

    fn run_query(&mut self, table_name: &str, query: String) -> Result<Vec<Row>, failure::Error> {
        let (sql_types, desc) = self
            .table_types
            .get(table_name)
            .ok_or_else(|| format_err!("Unknown table: {:?}", table_name))?
            .clone();
        let mut rows = vec![];
        let postgres_rows = self.conn.query(&*query, &[])?;
        for postgres_row in postgres_rows.iter() {
            let mut row = Row::new();
            for c in 0..postgres_row.len() {
                push_column(
                    &mut row,
                    &postgres_row,
                    c,
                    &sql_types[c],
                    desc.typ().column_types[c].nullable,
                )?;
            }
            rows.push(row);
        }
        Ok(rows)
    }
}

fn push_column(
    row: &mut Row,
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
        DataType::Custom(name) if name.to_string().to_lowercase() == "bool" => {
            let bool = get_column_inner::<bool>(postgres_row, i, nullable)?;
            row.push(bool.into());
        }
        DataType::Char(_) | DataType::Varchar(_) | DataType::Text => {
            let string = get_column_inner::<String>(postgres_row, i, nullable)?;
            row.push(string.mz_as_deref().into());
        }
        DataType::Custom(name) if name.to_string().to_lowercase() == "string" => {
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
