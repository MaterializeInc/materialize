// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! DDL that recreates a recorded column schema as a relation.
//!
//! Used both by the in-process typechecking catalog and by the container the
//! `explain` command stages against, so it lives beside the contract rather
//! than inside either consumer.
//!
//! A schema of ordinary types is a `CREATE TABLE`. A record has no data-type
//! syntax, so a schema containing one becomes a view over helper tables: a
//! relation alias in expression position plans to a record with that relation's
//! field names and nullability (`mz_sql::plan::query::plan_identifier`), which
//! is the only way to spell an anonymous record in SQL.

use crate::client::quote_identifier;
use crate::project::ir::object_id::ObjectId;
use crate::types::{ColumnType, DataType, RecordField};
use std::collections::BTreeMap;
use thiserror::Error;

/// A recorded type that cannot be turned back into a relation.
#[derive(Error, Debug)]
pub(crate) enum StubError {
    #[error(
        "`{object}`.`{column}` is recorded as the pseudo-type `{found}`, which does not describe the column's real type; re-run `mz-deploy lock` to capture it"
    )]
    UnreconstructibleType {
        object: ObjectId,
        column: String,
        found: String,
    },
    #[error(
        "`{object}`.`{column}` has type `{found}`; a record inside a container is not supported"
    )]
    RecordInContainer {
        object: ObjectId,
        column: String,
        found: String,
    },
}

/// Where a stub and its helper relations are created.
pub(crate) struct StubTarget {
    /// Fully-qualified, quoted name of the stub relation itself.
    pub name: String,
    /// Quoted, dot-terminated qualification for helper relations, e.g.
    /// `"db"."schema".`.
    pub helper_prefix: String,
    /// Distinguishes this stub's helpers from another's in the same schema.
    pub helper_stem: String,
}

/// Allocates names for the helper relations and aliases of one stub.
///
/// A nested derived table shares the enclosing scope, so every alias must be
/// distinct across the whole stub, not just within one nesting level. Drawing
/// all of them from one counter guarantees that.
struct StubNames<'a> {
    target: &'a StubTarget,
    next: usize,
}

impl StubNames<'_> {
    fn take(&mut self) -> usize {
        let n = self.next;
        self.next += 1;
        n
    }

    fn helper(&mut self) -> String {
        let n = self.take();
        format!(
            "{}{}",
            self.target.helper_prefix,
            quote_identifier(&format!("{}_h{}", self.target.helper_stem, n))
        )
    }
}

/// The statements that recreate `columns` as `target.name`.
///
/// Helper relations come first and the stub itself is always last, so callers
/// execute the sequence in order.
pub(crate) fn build_stub_statements(
    object_id: &ObjectId,
    target: &StubTarget,
    columns: &BTreeMap<String, ColumnType>,
) -> Result<Vec<String>, StubError> {
    // `columns` is keyed by name, so iterating it directly yields alphabetical
    // order. The schema's real column order lives in `ColumnType::position`.
    let mut ordered: Vec<_> = columns.iter().collect();
    ordered.sort_by_key(|(_, ct)| ct.position);

    for (name, column) in &ordered {
        check_reconstructible(object_id, name, &column.r#type)?;
    }

    if !ordered.iter().any(|(_, ct)| ct.r#type.contains_record()) {
        let defs: Vec<String> = ordered
            .iter()
            .map(|(name, ct)| column_def(name, &ct.r#type, ct.nullable))
            .collect();
        return Ok(vec![format!(
            "CREATE TABLE {} ({})",
            target.name,
            defs.join(", ")
        )]);
    }

    let fields: Vec<RecordField> = ordered
        .iter()
        .map(|(name, ct)| RecordField {
            name: (*name).clone(),
            r#type: ct.r#type.clone(),
            nullable: ct.nullable,
        })
        .collect();

    let mut names = StubNames { target, next: 0 };
    let mut statements = Vec::new();
    let select = build_select(&fields, &mut names, &mut statements);
    statements.push(format!("CREATE VIEW {} AS {}", target.name, select));
    Ok(statements)
}

/// Reject a type the builder cannot express, naming the column that carries it.
fn check_reconstructible(
    object_id: &ObjectId,
    column: &str,
    r#type: &DataType,
) -> Result<(), StubError> {
    if r#type.contains_pseudo_token() {
        return Err(StubError::UnreconstructibleType {
            object: object_id.clone(),
            column: column.to_string(),
            found: r#type.to_string(),
        });
    }
    match r#type {
        DataType::Array(inner) | DataType::List(inner) | DataType::Map(inner)
            if inner.contains_record() =>
        {
            Err(StubError::RecordInContainer {
                object: object_id.clone(),
                column: column.to_string(),
                found: r#type.to_string(),
            })
        }
        DataType::Record(fields) => fields
            .iter()
            .try_for_each(|field| check_reconstructible(object_id, column, &field.r#type)),
        _ => Ok(()),
    }
}

/// `"name" <type>[ NOT NULL]` for a table column.
fn column_def(name: &str, r#type: &DataType, nullable: bool) -> String {
    format!(
        "{} {}{}",
        quote_identifier(name),
        r#type,
        if nullable { "" } else { " NOT NULL" }
    )
}

/// A `SELECT` whose output columns are exactly `fields`.
///
/// Appends the helper tables it needs to `statements`, always before the
/// statement that will reference them.
fn build_select(
    fields: &[RecordField],
    names: &mut StubNames<'_>,
    statements: &mut Vec<String>,
) -> String {
    let scalars: Vec<&RecordField> = fields
        .iter()
        .filter(|f| !f.r#type.contains_record())
        .collect();

    // A helper table is what carries exact field nullability, which a `SELECT`
    // of literals cannot express. It is only needed when there is a scalar
    // field to put in it.
    let mut from = Vec::new();
    let base = if scalars.is_empty() {
        String::new()
    } else {
        let helper = names.helper();
        let defs: Vec<String> = scalars
            .iter()
            .map(|f| column_def(&f.name, &f.r#type, f.nullable))
            .collect();
        statements.push(format!("CREATE TABLE {} ({})", helper, defs.join(", ")));
        let base = quote_identifier(&format!("__b{}", names.take()));
        from.push(format!("{} AS {}", helper, base));
        base
    };

    let mut projection = Vec::new();
    for field in fields {
        let alias = quote_identifier(&field.name);
        match &field.r#type {
            DataType::Record(inner) => {
                let inner_select = build_select(inner, names, statements);
                let record = quote_identifier(&format!("__r{}", names.take()));
                if field.nullable {
                    // A scalar subquery is nullable; a plain FROM alias is not.
                    projection.push(format!(
                        "(SELECT {record} FROM ({inner_select}) AS {record} LIMIT 1) AS {alias}"
                    ));
                } else {
                    from.push(format!("({}) AS {}", inner_select, record));
                    projection.push(format!("{} AS {}", record, alias));
                }
            }
            _ => projection.push(format!("{}.{} AS {}", base, alias, alias)),
        }
    }

    if from.is_empty() {
        return format!("SELECT {}", projection.join(", "));
    }
    format!("SELECT {} FROM {}", projection.join(", "), from.join(", "))
}
