use std::collections::HashMap;

use failure::{bail, ensure, format_err};
use rusqlite::{types::ValueRef, Connection, NO_PARAMS};
use sqlparser::ast::{DataType, Statement};

use materialize::repr::{Datum, ScalarType};

pub struct Sqlite {
    connection: Connection,
    table_types: HashMap<String, Vec<DataType>>,
}

pub type Row = Vec<Datum>;
pub type Diff = Vec<(Row, isize)>;

pub enum Outcome {
    Created(String),
    Dropped(String),
    Changed(String, Diff),
}

impl Sqlite {
    pub fn open() -> Result<Self, failure::Error> {
        let connection = Connection::open_in_memory()?;
        Ok(Self {
            connection,
            table_types: HashMap::new(),
        })
    }

    pub fn run_statement(&mut self, statement: &Statement) -> Result<Vec<Outcome>, failure::Error> {
        Ok(match statement {
            Statement::CreateTable { name, columns, .. } => {
                self.connection.execute(&statement.to_string(), NO_PARAMS)?;
                self.table_types.insert(
                    name.to_string(),
                    columns
                        .iter()
                        .map(|column| column.data_type.clone())
                        .collect(),
                );
                vec![Outcome::Created(name.to_string())]
            }
            Statement::Drop { names, .. } => {
                self.connection.execute(&statement.to_string(), NO_PARAMS)?;
                names
                    .iter()
                    .map(|name| Outcome::Dropped(name.to_string()))
                    .collect()
            }
            Statement::Delete { table_name, .. }
            | Statement::Insert { table_name, .. }
            | Statement::Update { table_name, .. } => {
                let table_name = table_name.to_string();
                let before = self.select_all(&table_name)?;
                self.connection.execute(&statement.to_string(), NO_PARAMS)?;
                let after = self.select_all(&table_name)?;
                let mut diff = HashMap::new();
                for row in before {
                    *diff.entry(row).or_insert(0) -= 1;
                }
                for row in after {
                    *diff.entry(row).or_insert(0) += 1;
                }
                diff.retain(|_, count| *count != 0);
                vec![Outcome::Changed(table_name, diff.into_iter().collect())]
            }
            _ => bail!("Unsupported statement sent to sqlite: {:?}", statement),
        })
    }

    fn select_all(&mut self, table_name: &str) -> Result<Vec<Row>, failure::Error> {
        let mut prepared = self
            .connection
            .prepare(&format!("select * from {}", table_name))?;
        let types = self
            .table_types
            .get(table_name)
            .ok_or_else(|| format_err!("Unknown table: {:?}", table_name))?;
        ensure!(
            prepared.column_count() == types.len(),
            "In table {:?} expected {} columns, received {}",
            table_name,
            types.len(),
            prepared.column_count()
        );
        let mut sqlite_rows = prepared.query(NO_PARAMS)?;
        let mut rows = vec![];
        while let Some(sqlite_row) = sqlite_rows.next()? {
            let row = (0..sqlite_row.column_count())
                .map(|c| {
                    Ok(
                        match (sqlite_row.get_raw(c), ScalarType::from_sql(&types[c])?) {
                            (ValueRef::Null, _) => Datum::Null,
                            (ValueRef::Integer(i), ScalarType::Int64) => Datum::Int64(i),
                            (ValueRef::Real(r), ScalarType::Float64) => {
                                Datum::Float64(ordered_float::OrderedFloat(r))
                            }
                            (ValueRef::Text(t), ScalarType::String) => {
                                Datum::String(String::from_utf8(t.to_vec())?)
                            }
                            // TODO(jamii) handle dates, decimals etc
                            other => bail!("Unsupported sqlite type: {:?}", other),
                        },
                    )
                })
                .collect::<Result<_, _>>()?;
            rows.push(row);
        }
        Ok(rows)
    }
}
