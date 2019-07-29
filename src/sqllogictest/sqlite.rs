use std::collections::HashMap;

use failure::bail;
use rusqlite::{types::ValueRef, Connection, NO_PARAMS};
use sqlparser::ast::Statement;
use sqlparser::dialect::AnsiDialect;
use sqlparser::parser::{Parser as SqlParser, ParserError as SqlParserError};

use materialize::repr::Datum;

pub type Row = Vec<Datum>;
pub type Diff = Vec<(Row, isize)>;

pub enum Outcome {
    Created(String),
    Dropped(String),
    Changed(String, Diff),
}

pub fn open() -> Result<Connection, failure::Error> {
    Ok(Connection::open_in_memory()?)
}

pub fn run_statement(
    connection: &mut Connection,
    statement: &Statement,
) -> Result<Vec<Outcome>, failure::Error> {
    Ok(match statement {
        Statement::CreateTable { name, .. } | Statement::CreateView { name, .. } => {
            connection.execute(&statement.to_string(), NO_PARAMS)?;
            vec![Outcome::Created(name.to_string())]
        }
        Statement::Drop { names, .. } => {
            connection.execute(&statement.to_string(), NO_PARAMS)?;
            names
                .iter()
                .map(|name| Outcome::Dropped(name.to_string()))
                .collect()
        }
        Statement::Delete { table_name, .. }
        | Statement::Insert { table_name, .. }
        | Statement::Update { table_name, .. } => {
            let table_name = table_name.to_string();
            let before = select_all(connection, &table_name)?;
            connection.execute(&statement.to_string(), NO_PARAMS)?;
            let after = select_all(connection, &table_name)?;
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

fn select_all(connection: &mut Connection, table_name: &str) -> Result<Vec<Row>, failure::Error> {
    let sqlite_rows = connection
        .prepare(&format!("select * from {}", table_name))?
        .query(NO_PARAMS)?;
    let mut rows = vec![];
    // TODO gonna need sql types in here
    while let Some(sqlite_row) = sqlite_rows.next()? {
        let row = (0..sqlite_row.column_count())
            .map(|c| {
                Ok(match sqlite_row.get_raw(c) {
                    ValueRef::Null => Datum::Null,
                    ValueRef::Integer(i) => Datum::Int64(i),
                    ValueRef::Real(r) => Datum::Float64(r),
                    ValueRef::Text(t) => Datum::String(String::from_utf8(t.to_vec())?),
                    other => bail!("Unsupported sqlite type: {:?}", other),
                })
            })
            .collect::<Result<_, _>>()?;
        rows.push(row);
    }
    Ok(rows)
}
