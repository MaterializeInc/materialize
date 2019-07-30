use std::collections::HashMap;
use std::convert::TryInto;
use std::error::Error;
use std::io::Cursor;

use ::postgres::rows::Row as PostgresRow;
use ::postgres::types::{FromSql, Type as PostgresType};
use ::postgres::{Connection, TlsMode};
use byteorder::{NetworkEndian, ReadBytesExt};
use failure::{bail, ensure, format_err};
use sqlparser::ast::{ObjectType, Statement};

use materialize::repr::decimal::Significand;
use materialize::repr::{ColumnType, Datum, RelationType, ScalarType};

#[derive(Debug)]
pub struct Postgres {
    connection: Connection,
    table_types: HashMap<String, RelationType>,
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
        // TODO(jamii) figure out CI setup
        // "alter role postgres password password"
        let connection = Connection::connect(
            "postgresql://postgres:password@localhost/sqllogictest_throwaway_database",
            TlsMode::None,
        )?;
        // drop all tables
        connection.execute(
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
            connection,
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
                self.connection.execute(sql, &[])?;
                let typ = RelationType {
                    column_types: columns
                        .iter()
                        .map(|column| {
                            Ok(ColumnType {
                                name: Some(column.name.clone()),
                                scalar_type: ScalarType::from_sql(&column.data_type)?,
                                nullable: true,
                            })
                        })
                        .collect::<Result<Vec<_>, failure::Error>>()?,
                };
                self.table_types.insert(name.to_string(), typ.clone());
                Outcome::Created(name.to_string(), typ)
            }
            Statement::Drop {
                names,
                object_type: ObjectType::Table,
                ..
            } => {
                self.connection.execute(sql, &[])?;
                Outcome::Dropped(names.iter().map(|name| name.to_string()).collect())
            }
            Statement::Delete { table_name, .. }
            | Statement::Insert { table_name, .. }
            | Statement::Update { table_name, .. } => {
                let table_name = table_name.to_string();
                let typ = self
                    .table_types
                    .get(&table_name)
                    .ok_or_else(|| format_err!("Unknown table: {:?}", table_name))?
                    .clone();
                let before = self.select_all(&table_name, &typ)?;
                self.connection.execute(sql, &[])?;
                let after = self.select_all(&table_name, &typ)?;
                let mut diff = HashMap::new();
                for row in before {
                    *diff.entry(row).or_insert(0) -= 1;
                }
                for row in after {
                    *diff.entry(row).or_insert(0) += 1;
                }
                diff.retain(|_, count| *count != 0);
                Outcome::Changed(table_name, typ, diff.into_iter().collect())
            }
            _ => bail!("Unsupported statement: {:?}", parsed),
        })
    }

    fn select_all(
        &mut self,
        table_name: &str,
        typ: &RelationType,
    ) -> Result<Vec<Row>, failure::Error> {
        let mut rows = vec![];
        let postgres_rows = self
            .connection
            .query(&format!("select * from {}", table_name), &[])?;
        for postgres_row in postgres_rows.iter() {
            let row = (0..postgres_row.len())
                .map(|c| get_column(&postgres_row, c, &typ.column_types[c]))
                .collect::<Result<_, _>>()?;
            rows.push(row);
        }
        Ok(rows)
    }
}

fn get_column(
    postgres_row: &PostgresRow,
    i: usize,
    column_type: &ColumnType,
) -> Result<Datum, failure::Error> {
    Ok(match &column_type.scalar_type {
        ScalarType::Bool => get_column_inner::<bool>(postgres_row, i, column_type.nullable)?.into(),
        ScalarType::Bytes => {
            get_column_inner::<Vec<u8>>(postgres_row, i, column_type.nullable)?.into()
        }
        ScalarType::Float32 => {
            get_column_inner::<f32>(postgres_row, i, column_type.nullable)?.into()
        }
        ScalarType::Float64 => {
            get_column_inner::<f64>(postgres_row, i, column_type.nullable)?.into()
        }
        ScalarType::Int32 => get_column_inner::<i32>(postgres_row, i, column_type.nullable)?.into(),
        ScalarType::Int64 => get_column_inner::<i64>(postgres_row, i, column_type.nullable)?.into(),
        ScalarType::Null => {
            postgres_row.get_opt::<usize, Null>(i).unwrap()?;
            Datum::Null
        }
        ScalarType::String => {
            get_column_inner::<String>(postgres_row, i, column_type.nullable)?.into()
        }
        ScalarType::Decimal(_, type_scale) => {
            match get_column_inner::<DecimalWrapper>(postgres_row, i, column_type.nullable)? {
                None => Datum::Null,
                Some(DecimalWrapper {
                    mut significand,
                    scale: current_scale,
                }) => {
                    // TODO(jamii) lots of potential for unchecked edge cases here eg 10^scale_correction could overflow
                    // Current representation is `significand * 10^current_scale`
                    // Want to get to `significand2 * 10^type_scale`
                    // So `significand2 = significand * 10^(current_scale - type_scale)`
                    let scale_correction = current_scale - (*type_scale as i64);
                    if scale_correction > 0 {
                        significand *= 10i128.pow(scale_correction.try_into()?);
                    } else {
                        significand /= 10i128.pow((-scale_correction).try_into()?);
                    };
                    Significand::new(significand).into()
                }
            }
        }
        ScalarType::Regex | ScalarType::Time | ScalarType::Date | ScalarType::Timestamp => {
            // TODO(jamii)
            bail!(
                "Postgres to materialize conversion not yet supported for {:?}",
                column_type.scalar_type
            )
        }
    })
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
        let value: Option<T> = postgres_row.get_opt(i).unwrap()?;
        Ok(value)
    } else {
        let value: T = postgres_row.get_opt(i).unwrap()?;
        Ok(Some(value))
    }
}

struct DecimalWrapper {
    significand: i128,
    scale: i64,
}

impl FromSql for DecimalWrapper {
    fn from_sql(
        _ty: &PostgresType,
        raw: &[u8],
    ) -> Result<Self, Box<std::error::Error + 'static + Send + Sync>> {
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
            significand += digit as i128;
        }
        significand *= match sign {
            0 => 1,
            0x4000 => -1,
            0xC000 => Err(format_err!("Got a decimal NaN"))?,
            _ => Err(format_err!("Got an invalid sign byte: {:?}", sign))?,
        };

        // First digit got factor 10_000^(digits.len() - 1), but should get 10_000^weight
        let current_scale = -(4 * (i64::from(weight) - count + 1));

        Ok(DecimalWrapper {
            significand,
            scale: current_scale,
        })
    }

    fn accepts(ty: &PostgresType) -> bool {
        match ty {
            &postgres::types::NUMERIC => true,
            _ => false,
        }
    }
}

struct Null();

impl FromSql for Null {
    fn from_sql(
        ty: &PostgresType,
        _raw: &[u8],
    ) -> Result<Self, Box<std::error::Error + Sync + Send>> {
        Err(format_err!("Expected null, got a value of type: {:?}", ty))?
    }

    fn from_sql_null(_: &PostgresType) -> Result<Self, Box<std::error::Error + Sync + Send>> {
        Ok(Null())
    }

    fn accepts(_ty: &PostgresType) -> bool {
        true
    }
}
