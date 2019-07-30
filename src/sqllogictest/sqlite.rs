use std::collections::HashMap;

use failure::{bail, ensure, format_err};
use rusqlite::{types::ValueRef, Connection, NO_PARAMS};
use sqlparser::ast::{ObjectType, Statement};

use materialize::repr::{ColumnType, Datum, RelationType, ScalarType};

#[derive(Debug)]
pub struct Sqlite {
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

impl Sqlite {
    pub fn open() -> Result<Self, failure::Error> {
        let connection = Connection::open_in_memory()?;
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
                self.connection.execute(sql, NO_PARAMS)?;
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
                self.connection.execute(sql, NO_PARAMS)?;
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
                let before = self.select_all(&table_name)?;
                self.connection.execute(sql, NO_PARAMS)?;
                let after = self.select_all(&table_name)?;
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
            _ => bail!("Unsupported statement sent to sqlite: {:?}", parsed),
        })
    }

    fn select_all(&mut self, table_name: &str) -> Result<Vec<Row>, failure::Error> {
        let mut prepared = self
            .connection
            .prepare(&format!("select * from {}", table_name))?;
        let typ = self
            .table_types
            .get(table_name)
            .ok_or_else(|| format_err!("Unknown table: {:?}", table_name))?;
        ensure!(
            prepared.column_count() == typ.column_types.len(),
            "In table {:?} expected {} columns, received {}",
            table_name,
            typ.column_types.len(),
            prepared.column_count()
        );
        let mut sqlite_rows = prepared.query(NO_PARAMS)?;
        let mut rows = vec![];
        while let Some(sqlite_row) = sqlite_rows.next()? {
            let row = (0..sqlite_row.column_count())
                .map(|c| {
                    let column_type = &typ.column_types[c];
                    Ok(match (sqlite_row.get_raw(c), &column_type.scalar_type) {
                        (ValueRef::Null, _) if column_type.nullable => Datum::Null,
                        (ValueRef::Integer(i), ScalarType::Int64) => Datum::Int64(i),
                        (ValueRef::Real(r), ScalarType::Float64) => {
                            Datum::Float64(ordered_float::OrderedFloat(r))
                        }
                        (ValueRef::Text(t), ScalarType::String) => {
                            Datum::String(String::from_utf8(t.to_vec())?)
                        }
                        (ValueRef::Integer(0), ScalarType::Bool) => Datum::False,
                        (ValueRef::Integer(1), ScalarType::Bool) => Datum::True,
                        (ValueRef::Real(r), ScalarType::Decimal(_precision, scale)) => {
                            // TODO(jamii) lolwut
                            let scaled_r = r * (10.0 as f64).powi(*scale as i32);
                            Datum::from(scaled_r as i128)
                        }
                        // TODO(jamii) handle dates, decimals etc
                        (other, _) => bail!(
                            "Unsupported sqlite->materialize conversion: {:?} to {:?}",
                            other,
                            column_type
                        ),
                    })
                })
                .collect::<Result<_, _>>()?;
            rows.push(row);
        }
        Ok(rows)
    }
}
