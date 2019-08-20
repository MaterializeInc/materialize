// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::collections::HashMap;
use std::convert::TryInto;
use std::env;
use std::io::Cursor;

use ::postgres::row::Row as PostgresRow;
use ::postgres::types::{FromSql, Type as PostgresType};
use ::postgres::{Client, NoTls};
use byteorder::{NetworkEndian, ReadBytesExt};
use failure::{bail, ensure, format_err};
use sqlparser::ast::{DataType, ObjectType, Statement};

use repr::decimal::Significand;
use repr::{ColumnType, Datum, RelationType, ScalarType};
use sql::scalar_type_from_sql;

pub struct Postgres {
    client: Client,
    table_types: HashMap<String, (Vec<DataType>, RelationType)>,
}

pub type Row = Vec<Datum>;
pub type Diff = Vec<(Row, isize)>;

#[derive(Debug, Clone)]
pub enum Outcome {
    Created(String, RelationType),
    Dropped(Vec<String>),
    Changed(String, RelationType, Diff),
}

impl Postgres {
    pub fn open_and_erase() -> Result<Self, failure::Error> {
        // This roughly matches the environment variables that the official C
        // library for PostgreSQL, libpq, uses [0]. It would be nice if this
        // hunk of code lived in the Rust PostgreSQL library.
        //
        // [0]: https://www.postgresql.org/docs/current/libpq-envars.html
        let user = env::var("PGUSER").unwrap_or_else(|_| whoami::username());
        let mut client = postgres::config::Config::new()
            .user(&user)
            .password(env::var("PGPASSWORD").unwrap_or_else(|_| "".into()))
            .dbname(&env::var("PGDATABASE").unwrap_or(user))
            .port(match env::var("PGPORT") {
                Ok(port) => port.parse()?,
                Err(_) => 5432,
            })
            .host(&env::var("PGHOST").unwrap_or_else(|_| "localhost".into()))
            .connect(NoTls)?;
        // drop all tables
        client.execute(
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
            client,
            table_types: HashMap::new(),
        })
    }

    pub fn run_statement(
        &mut self,
        sql: &str,
        parsed: &Statement,
    ) -> Result<Outcome, failure::Error> {
        Ok(match parsed {
            Statement::CreateTable { name, columns, .. } => {
                self.client.execute(sql, &[])?;
                let sql_types = columns
                    .iter()
                    .map(|column| column.data_type.clone())
                    .collect::<Vec<_>>();
                let typ = RelationType {
                    column_types: columns
                        .iter()
                        .map(|column| {
                            Ok(ColumnType {
                                name: Some(column.name.clone()),
                                scalar_type: scalar_type_from_sql(&column.data_type)?,
                                nullable: true,
                            })
                        })
                        .collect::<Result<Vec<_>, failure::Error>>()?,
                };
                self.table_types
                    .insert(name.to_string(), (sql_types, typ.clone()));
                Outcome::Created(name.to_string(), typ)
            }
            Statement::Drop {
                names,
                object_type: ObjectType::Table,
                ..
            } => {
                self.client.execute(sql, &[])?;
                Outcome::Dropped(names.iter().map(|name| name.to_string()).collect())
            }
            Statement::Delete { table_name, .. }
            | Statement::Insert { table_name, .. }
            | Statement::Update { table_name, .. } => {
                let table_name = table_name.to_string();
                let (sql_types, typ) = self
                    .table_types
                    .get(&table_name)
                    .ok_or_else(|| format_err!("Unknown table: {:?}", table_name))?
                    .clone();
                let before = self.select_all(&table_name, &sql_types, &typ)?;
                self.client.execute(sql, &[])?;
                let after = self.select_all(&table_name, &sql_types, &typ)?;
                let mut update = HashMap::new();
                for row in before {
                    *update.entry(row).or_insert(0) -= 1;
                }
                for row in after {
                    *update.entry(row).or_insert(0) += 1;
                }
                update.retain(|_, count| *count != 0);
                Outcome::Changed(table_name, typ, update.into_iter().collect())
            }
            _ => bail!("Unsupported statement: {:?}", parsed),
        })
    }

    fn select_all(
        &mut self,
        table_name: &str,
        sql_types: &[DataType],
        typ: &RelationType,
    ) -> Result<Vec<Row>, failure::Error> {
        let mut rows = vec![];
        let postgres_rows = self
            .client
            .query(&*format!("SELECT * FROM {}", table_name), &[])?;
        for postgres_row in postgres_rows.iter() {
            let row = (0..postgres_row.len())
                .map(|c| {
                    let datum = get_column(
                        &postgres_row,
                        c,
                        &sql_types[c],
                        typ.column_types[c].nullable,
                    )?;
                    ensure!(
                        datum.is_instance_of(&typ.column_types[c]),
                        "Expected value of type {:?}, got {:?}",
                        typ.column_types[c],
                        datum
                    );
                    Ok(datum)
                })
                .collect::<Result<_, _>>()?;
            rows.push(row);
        }
        Ok(rows)
    }
}

fn get_column(
    postgres_row: &PostgresRow,
    i: usize,
    sql_type: &DataType,
    nullable: bool,
) -> Result<Datum, failure::Error> {
    // NOTE this needs to stay in sync with materialize::sql::scalar_type_from_sql
    // in some cases, we use slightly different representations than postgres does for the same sql types, so we have to be careful about conversions
    Ok(match sql_type {
        DataType::Boolean => get_column_inner::<bool>(postgres_row, i, nullable)?.into(),
        DataType::Custom(name) if name.to_string().to_lowercase() == "bool" => {
            get_column_inner::<bool>(postgres_row, i, nullable)?.into()
        }
        DataType::Char(_) | DataType::Varchar(_) | DataType::Text => {
            get_column_inner::<String>(postgres_row, i, nullable)?.into()
        }
        DataType::Custom(name) if name.to_string().to_lowercase() == "string" => {
            get_column_inner::<String>(postgres_row, i, nullable)?.into()
        }
        DataType::SmallInt => get_column_inner::<i16>(postgres_row, i, nullable)?
            .map(|i| i32::from(i))
            .into(),
        DataType::Int => get_column_inner::<i32>(postgres_row, i, nullable)?
            .map(|i| i64::from(i))
            .into(),
        DataType::BigInt => get_column_inner::<i64>(postgres_row, i, nullable)?.into(),
        DataType::Float(p) => {
            if p.unwrap_or(53) <= 24 {
                get_column_inner::<f32>(postgres_row, i, nullable)?
                    .map(|f| f64::from(f))
                    .into()
            } else {
                get_column_inner::<f64>(postgres_row, i, nullable)?.into()
            }
        }
        DataType::Real => get_column_inner::<f32>(postgres_row, i, nullable)?
            .map(|f| f64::from(f))
            .into(),
        DataType::Double => get_column_inner::<f64>(postgres_row, i, nullable)?.into(),
        DataType::Date => {
            let d: chrono::NaiveDate =
                get_column_inner::<chrono::NaiveDate>(postgres_row, i, nullable)?.unwrap();
            Datum::Date(d)
        }
        DataType::Decimal(_, _) => {
            let desired_scale = match scalar_type_from_sql(sql_type).unwrap() {
                ScalarType::Decimal(_precision, desired_scale) => desired_scale,

                _ => unreachable!(),
            };
            match get_column_inner::<DecimalWrapper>(postgres_row, i, nullable)? {
                None => Datum::Null,
                Some(DecimalWrapper {
                    mut significand,
                    scale: current_scale,
                }) => {
                    // TODO(jamii) lots of potential for unchecked edge cases here eg 10^scale_correction could overflow
                    // current representation is `significand * 10^current_scale`
                    // want to get to `significand2 * 10^desired_scale`
                    // so `significand2 = significand * 10^(current_scale - desired_scale)`
                    let scale_correction = current_scale - (i64::from(desired_scale));
                    if scale_correction > 0 {
                        significand /= 10i128.pow(scale_correction.try_into()?);
                    } else {
                        significand *= 10i128.pow((-scale_correction).try_into()?);
                    };
                    Significand::new(significand).into()
                }
            }
        }
        DataType::Bytea => get_column_inner::<Vec<u8>>(postgres_row, i, nullable)?.into(),
        _ => bail!(
            "Postgres to materialize conversion not yet supported for {:?}",
            sql_type
        ),
    })
}

fn get_column_inner<'a, T>(
    postgres_row: &'a PostgresRow,
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

struct DecimalWrapper {
    significand: i128,
    scale: i64,
}

impl FromSql<'_> for DecimalWrapper {
    fn from_sql(
        _ty: &PostgresType,
        raw: &[u8],
    ) -> Result<Self, Box<dyn std::error::Error + 'static + Send + Sync>> {
        // TODO(jamii) how do we attribute this?
        // based on:
        //   https://docs.diesel.rs/src/diesel/pg/types/floats/mod.rs.html#55-82
        //   https://docs.diesel.rs/src/diesel/pg/types/numeric.rs.html#41-73

        let mut raw = Cursor::new(raw);
        let digit_count = raw.read_u16::<NetworkEndian>()?;
        let mut digits = Vec::with_capacity(digit_count as usize);
        let weight = raw.read_i16::<NetworkEndian>()?;
        let sign = raw.read_u16::<NetworkEndian>()?;
        let _scale = raw.read_u16::<NetworkEndian>()?;
        for _ in 0..digit_count {
            digits.push(raw.read_i16::<NetworkEndian>()?);
        }

        let mut significand: i128 = 0;
        let count = digits.len() as i64;
        for digit in digits {
            significand *= 10_000i128;
            significand += i128::from(digit);
        }
        significand *= match sign {
            0 => 1,
            0x4000 => -1,
            0xC000 => Err(format_err!("Got a decimal NaN"))?,
            _ => Err(format_err!("Got an invalid sign byte: {:?}", sign))?,
        };

        // first digit got factor 10_000^(digits.len() - 1), but should get 10_000^weight
        let current_scale = -(4 * (i64::from(weight) - count + 1));

        Ok(DecimalWrapper {
            significand,
            scale: current_scale,
        })
    }

    fn accepts(ty: &PostgresType) -> bool {
        match *ty {
            PostgresType::NUMERIC => true,
            _ => false,
        }
    }
}
