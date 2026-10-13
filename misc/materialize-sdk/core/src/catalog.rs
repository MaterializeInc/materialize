// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The catalog queries a durable subscription runs before it starts, and what
//! their rows mean.
//!
//! Each query reads only catalog relations that every role may read. Names
//! are inlined as string literals, which is safe because Materialize keeps
//! `standard_conforming_strings` on and refuses to turn it off, so a backslash
//! is never an escape.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::decode::{Envelope, Row};
use crate::error::{ProtocolError, UsageError};
use crate::frontier::Frontier;
use crate::identity::{Column, Fingerprint};
use crate::statement::ObjectRef;
use crate::wire;

/// A catalog object's name, with every part given, so that the lookup does
/// not depend on the session's database or search path.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectName {
    /// The database, or `None` for an ambient schema such as `mz_catalog`.
    pub database: Option<String>,
    /// The schema.
    pub schema: String,
    /// The object's name.
    pub name: String,
}

/// What kind of object a name resolved to.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectKind {
    /// A table.
    Table,
    /// A source.
    Source,
    /// A materialized view.
    MaterializedView,
    /// Any other kind, by its `mz_objects.type`.
    Other(String),
}

/// What the catalog says about an object.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogObject {
    /// The object, with its catalog id.
    pub object: ObjectRef,
    /// The object's kind.
    pub kind: ObjectKind,
    /// The storage shard, or `None` for an object without storage, such as a
    /// view.
    pub shard: Option<String>,
    /// The object's history window: its `RETAIN HISTORY`, or the one-second
    /// default, which the catalog reports the same way. `None` for an object
    /// without history, such as a view.
    #[serde(default, with = "wire::opt_u64_string")]
    pub retention_ms: Option<u64>,
    /// The output columns, in order.
    pub columns: Vec<Column>,
}

impl CatalogObject {
    /// The fingerprint a durable subscription to the object in `envelope`
    /// records.
    ///
    /// Fails unless the object is a user's table, source with columns, or
    /// materialized view. A source without columns is the primary object of a
    /// source with subsources, which never emits data. A subscription to a
    /// system object can be routed to the `mz_catalog_server` cluster and read
    /// an index there, which the index check on the session's cluster misses.
    pub fn fingerprint(&self, envelope: Envelope) -> Result<Fingerprint, UsageError> {
        let refuse = |why: &str| {
            Err(UsageError::new(format!(
                "a durable subscription cannot read {:?}: {why}",
                self.object.name
            )))
        };
        if !self.object.id.starts_with('u') {
            return refuse("it is a system object");
        }
        match (&self.kind, &self.shard) {
            (ObjectKind::Source, Some(_)) if self.columns.is_empty() => {
                refuse("it is a source without columns")
            }
            (
                ObjectKind::Table | ObjectKind::Source | ObjectKind::MaterializedView,
                Some(shard),
            ) => Ok(Fingerprint {
                object: self.object.clone(),
                shard: shard.clone(),
                envelope,
                columns: self.columns.clone(),
            }),
            _ => refuse("it is not a table, source, or materialized view"),
        }
    }
}

const LOOKUP_COLUMNS: [&str; 12] = [
    "id",
    "type",
    "database",
    "schema",
    "name",
    "shards",
    "shard",
    "retention_ms",
    "column",
    "type_oid",
    "type_mod",
    "nullable",
];

/// The query that looks up `name`. Pass its result to [`parse_lookup`].
///
/// A catalog id has a global id per version of the object, and the shard and
/// frontiers are keyed by global id, so the shard is found through
/// `mz_object_global_ids`. Every version of an object shares one shard.
pub fn lookup_sql(name: &ObjectName) -> String {
    let shards = "FROM mz_internal.mz_object_global_ids AS g \
                  JOIN mz_internal.mz_storage_shards AS sh ON sh.object_id = g.global_id \
                  WHERE g.id = o.id";
    let database = match &name.database {
        Some(database) => format!("d.name = {}", quote_literal(database)),
        None => "s.database_id IS NULL".to_string(),
    };
    format!(
        "SELECT o.id, o.type, d.name AS database, s.name AS schema, o.name, \
         (SELECT count(DISTINCT sh.shard_id) {shards}) AS shards, \
         (SELECT min(sh.shard_id) {shards}) AS shard, \
         (SELECT r.value::text FROM mz_internal.mz_history_retention_strategies AS r \
         WHERE r.id = o.id AND r.strategy = 'FOR') AS retention_ms, \
         c.name AS column, c.type_oid, c.type_mod, c.nullable \
         FROM mz_catalog.mz_objects AS o \
         JOIN mz_catalog.mz_schemas AS s ON s.id = o.schema_id \
         LEFT JOIN mz_catalog.mz_databases AS d ON d.id = s.database_id \
         LEFT JOIN mz_catalog.mz_columns AS c ON c.id = o.id \
         WHERE o.type IN ('table', 'source', 'view', 'materialized-view') \
         AND o.name = {} AND s.name = {} AND {database} \
         ORDER BY c.position",
        quote_literal(&name.name),
        quote_literal(&name.schema),
    )
}

/// Reads the result of [`lookup_sql`]: the object, or `None` if the name
/// resolves to nothing.
pub fn parse_lookup<S: AsRef<str>>(
    columns: &[S],
    rows: Vec<Row>,
) -> Result<Option<CatalogObject>, ProtocolError> {
    check_columns(columns, &LOOKUP_COLUMNS)?;
    let mut object: Option<CatalogObject> = None;
    for row in rows {
        let [
            id,
            kind,
            database,
            schema,
            name,
            shards,
            shard,
            retention,
            column,
            type_oid,
            type_mod,
            nullable,
        ]: [Option<String>; 12] = row
            .try_into()
            .map_err(|_| ProtocolError::new("a catalog row has the wrong width"))?;
        let id = required(id, "id")?;
        let current = match &mut object {
            Some(current) if current.object.id == id => current,
            Some(current) => {
                return Err(ProtocolError::new(format!(
                    "the name resolves to both {} and {id}",
                    current.object.id
                )));
            }
            None => {
                let shards = parse(&required(shards, "shards")?, "shards", wire::parse_u64)?;
                let shard = match (shards, shard) {
                    (0, None) => None,
                    (1, Some(shard)) => Some(shard),
                    _ => {
                        return Err(ProtocolError::new(format!(
                            "object {id} has {shards} storage shards"
                        )));
                    }
                };
                let kind = match required(kind, "type")?.as_str() {
                    "table" => ObjectKind::Table,
                    "source" => ObjectKind::Source,
                    "materialized-view" => ObjectKind::MaterializedView,
                    other => ObjectKind::Other(other.to_string()),
                };
                let retention_ms = retention
                    .map(|ms| parse(&ms, "retention_ms", wire::parse_u64))
                    .transpose()?;
                object.insert(CatalogObject {
                    object: ObjectRef {
                        id,
                        database,
                        schema: required(schema, "schema")?,
                        name: required(name, "name")?,
                    },
                    kind,
                    shard,
                    retention_ms,
                    columns: Vec::new(),
                })
            }
        };
        // An object without columns comes back as one row with no column.
        if let Some(column) = column {
            current.columns.push(Column {
                name: column,
                type_oid: parse(&required(type_oid, "type_oid")?, "type_oid", |s| {
                    wire::parse_u64(s).and_then(|v| u32::try_from(v).ok())
                })?,
                type_mod: parse(&required(type_mod, "type_mod")?, "type_mod", |s| {
                    wire::parse_i64(s).and_then(|v| i32::try_from(v).ok())
                })?,
                nullable: parse(&required(nullable, "nullable")?, "nullable", parse_bool)?,
            });
        }
    }
    Ok(object)
}

/// The query that lists the indexes on the object with catalog id
/// `object_id` in `cluster`, the cluster the subscription runs on. Pass its
/// result to [`parse_indexes`].
///
/// A subscription reads such an index instead of the object's storage, and an
/// index keeps its own history, one second by default, whatever the object's
/// `RETAIN HISTORY`. An index on another cluster does not matter. An index on
/// an older version of a table is listed too, although a subscription reads
/// the latest version, so the check can refuse an object it need not.
pub fn indexes_sql(object_id: &str, cluster: &str) -> String {
    format!(
        "SELECT i.name FROM mz_catalog.mz_indexes AS i \
         JOIN mz_catalog.mz_clusters AS c ON c.id = i.cluster_id \
         WHERE i.on_id = {} AND c.name = {} ORDER BY i.name",
        quote_literal(object_id),
        quote_literal(cluster),
    )
}

/// Reads the result of [`indexes_sql`]: the index names.
pub fn parse_indexes<S: AsRef<str>>(
    columns: &[S],
    rows: Vec<Row>,
) -> Result<Vec<String>, ProtocolError> {
    check_columns(columns, &["name"])?;
    rows.into_iter()
        .map(|row| match <[Option<String>; 1]>::try_from(row) {
            Ok([Some(name)]) => Ok(name),
            _ => Err(ProtocolError::new("an index row is not one name")),
        })
        .collect()
}

/// The query for the earliest timestamp at which a subscription can still read
/// the object with catalog id `object_id`. Pass its result to
/// [`parse_read_frontier`].
///
/// An object can have several global ids with different read frontiers. After
/// `APPLY REPLACEMENT` the old one keeps its own retention, and a subscription
/// reads the new one, whose frontier is not lower. After `ADD COLUMN` an older
/// table version's frontier is not lower than the newer one's. So the highest
/// frontier is the one a resume must clear.
pub fn read_frontier_sql(object_id: &str) -> String {
    format!(
        "SELECT max(f.read_frontier) AS read_frontier \
         FROM mz_internal.mz_object_global_ids AS g \
         JOIN mz_internal.mz_frontiers AS f ON f.object_id = g.global_id \
         WHERE g.id = {}",
        quote_literal(object_id),
    )
}

/// Reads the result of [`read_frontier_sql`], or `None` if the object has no
/// readable time.
pub fn parse_read_frontier<S: AsRef<str>>(
    columns: &[S],
    rows: Vec<Row>,
) -> Result<Option<u64>, ProtocolError> {
    check_columns(columns, &["read_frontier"])?;
    match <[Row; 1]>::try_from(rows) {
        Ok([row]) => match <[Option<String>; 1]>::try_from(row) {
            Ok([cell]) => cell
                .map(|t| parse(&t, "read_frontier", wire::parse_u64))
                .transpose(),
            Err(_) => Err(ProtocolError::new("a frontier row is not one value")),
        },
        Err(_) => Err(ProtocolError::new(
            "the frontier query returned no single row",
        )),
    }
}

/// How far a resume from `frontier` is from losing history, given the
/// object's `read_frontier`, or `None` if it already has.
///
/// The resume reads `AS OF F - 1`, which must not be below the read frontier.
/// Timestamps on the default timeline are milliseconds.
pub fn resume_margin(frontier: Frontier, read_frontier: u64) -> Option<Duration> {
    frontier
        .resume_as_of()
        .checked_sub(read_frontier)
        .map(Duration::from_millis)
}

/// Quotes a string literal, doubling any `'` it contains.
fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn check_columns<S: AsRef<str>>(columns: &[S], expected: &[&str]) -> Result<(), ProtocolError> {
    if columns
        .iter()
        .map(AsRef::as_ref)
        .eq(expected.iter().copied())
    {
        Ok(())
    } else {
        let got: Vec<&str> = columns.iter().map(AsRef::as_ref).collect();
        Err(ProtocolError::new(format!(
            "expected the catalog columns {expected:?}, got {got:?}"
        )))
    }
}

fn required(cell: Option<String>, column: &str) -> Result<String, ProtocolError> {
    cell.ok_or_else(|| ProtocolError::new(format!("the catalog returned NULL {column}")))
}

fn parse<T>(text: &str, column: &str, f: impl Fn(&str) -> Option<T>) -> Result<T, ProtocolError> {
    f(text).ok_or_else(|| ProtocolError::new(format!("invalid catalog {column} {text:?}")))
}

fn parse_bool(text: &str) -> Option<bool> {
    match text {
        "t" => Some(true),
        "f" => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cell(s: &str) -> Option<String> {
        Some(s.to_string())
    }

    fn column_row(object: [Option<&str>; 8], column: [Option<&str>; 4]) -> Row {
        object
            .into_iter()
            .chain(column)
            .map(|c| c.map(str::to_string))
            .collect()
    }

    const TABLE: [Option<&str>; 8] = [
        Some("u1"),
        Some("table"),
        Some("materialize"),
        Some("public"),
        Some("orders"),
        Some("1"),
        Some("s1"),
        Some("1000"),
    ];

    #[test]
    fn looks_up_an_object_by_every_part_of_its_name() {
        let sql = lookup_sql(&ObjectName {
            database: Some("db".into()),
            schema: "public".into(),
            name: "it's".into(),
        });
        assert!(
            sql.contains("o.name = 'it''s' AND s.name = 'public' AND d.name = 'db'"),
            "{sql}"
        );
        let ambient = lookup_sql(&ObjectName {
            database: None,
            schema: "mz_catalog".into(),
            name: "mz_tables".into(),
        });
        assert!(ambient.contains("AND s.database_id IS NULL"), "{ambient}");
    }

    #[test]
    fn parses_an_object_with_its_columns_in_order() {
        let rows = vec![
            column_row(TABLE, [Some("id"), Some("20"), Some("-1"), Some("f")]),
            column_row(
                TABLE,
                [Some("total"), Some("1700"), Some("655366"), Some("t")],
            ),
        ];
        let object = parse_lookup(&LOOKUP_COLUMNS, rows).unwrap().unwrap();
        assert_eq!(object.kind, ObjectKind::Table);
        assert_eq!(object.shard.as_deref(), Some("s1"));
        assert_eq!(object.retention_ms, Some(1000));
        assert_eq!(
            object.columns,
            [
                Column {
                    name: "id".into(),
                    type_oid: 20,
                    type_mod: -1,
                    nullable: false
                },
                Column {
                    name: "total".into(),
                    type_oid: 1700,
                    type_mod: 655366,
                    nullable: true
                },
            ]
        );
        let fingerprint = object.fingerprint(Envelope::Diff).unwrap();
        assert_eq!(fingerprint.shard, "s1");
    }

    #[test]
    fn a_name_that_resolves_to_nothing_is_none() {
        assert_eq!(parse_lookup(&LOOKUP_COLUMNS, vec![]), Ok(None));
    }

    #[test]
    fn an_object_without_columns_has_one_row_with_no_column() {
        let rows = vec![column_row(TABLE, [None, None, None, None])];
        let object = parse_lookup(&LOOKUP_COLUMNS, rows).unwrap().unwrap();
        assert!(object.columns.is_empty());
    }

    #[test]
    fn a_view_has_no_shard_and_no_durable_fingerprint() {
        let view = [
            Some("u5"),
            Some("view"),
            Some("materialize"),
            Some("public"),
            Some("v"),
            Some("0"),
            None,
            None,
        ];
        let rows = vec![column_row(
            view,
            [Some("x"), Some("23"), Some("-1"), Some("f")],
        )];
        let object = parse_lookup(&LOOKUP_COLUMNS, rows).unwrap().unwrap();
        assert_eq!(object.kind, ObjectKind::Other("view".into()));
        assert_eq!(object.shard, None);
        assert!(object.fingerprint(Envelope::Diff).is_err());
    }

    #[test]
    fn system_objects_and_sources_without_columns_are_not_durable() {
        let object = |id: &str, kind: ObjectKind, columns: usize| CatalogObject {
            object: ObjectRef {
                id: id.into(),
                database: None,
                schema: "s".into(),
                name: "o".into(),
            },
            kind,
            shard: Some("s1".into()),
            retention_ms: Some(1000),
            columns: vec![
                Column {
                    name: "c".into(),
                    type_oid: 25,
                    type_mod: -1,
                    nullable: true,
                };
                columns
            ],
        };
        assert!(
            object("s493", ObjectKind::MaterializedView, 1)
                .fingerprint(Envelope::Diff)
                .is_err()
        );
        assert!(
            object("u1", ObjectKind::Source, 0)
                .fingerprint(Envelope::Diff)
                .is_err()
        );
        assert!(
            object("u1", ObjectKind::Source, 1)
                .fingerprint(Envelope::Diff)
                .is_ok()
        );
        assert!(
            object("u1", ObjectKind::Table, 0)
                .fingerprint(Envelope::Diff)
                .is_ok()
        );
    }

    #[test]
    fn refuses_results_the_query_cannot_return() {
        let other_id = column_row(
            [
                Some("u2"),
                Some("table"),
                Some("materialize"),
                Some("public"),
                Some("orders"),
                Some("1"),
                Some("s2"),
                None,
            ],
            [Some("id"), Some("20"), Some("-1"), Some("f")],
        );
        let first = column_row(TABLE, [Some("id"), Some("20"), Some("-1"), Some("f")]);
        assert!(parse_lookup(&LOOKUP_COLUMNS, vec![first, other_id]).is_err());
        let two_shards = column_row(
            [
                Some("u1"),
                Some("table"),
                Some("materialize"),
                Some("public"),
                Some("orders"),
                Some("2"),
                Some("s1"),
                None,
            ],
            [None, None, None, None],
        );
        assert!(parse_lookup(&LOOKUP_COLUMNS, vec![two_shards]).is_err());
        assert!(parse_lookup(&["id"], vec![]).is_err());
        assert!(parse_lookup(&LOOKUP_COLUMNS, vec![vec![cell("u1")]]).is_err());
    }

    #[test]
    fn indexes_and_read_frontier() {
        assert_eq!(
            parse_indexes(&["name"], vec![vec![cell("orders_idx")]]),
            Ok(vec!["orders_idx".to_string()])
        );
        assert_eq!(
            parse_read_frontier(&["read_frontier"], vec![vec![cell("1700")]]),
            Ok(Some(1700))
        );
        assert_eq!(
            parse_read_frontier(&["read_frontier"], vec![vec![None]]),
            Ok(None)
        );
        assert!(parse_read_frontier(&["read_frontier"], vec![]).is_err());
    }

    #[test]
    fn resume_margin_is_how_far_as_of_is_above_the_read_frontier() {
        let at = |f| Frontier::new(f).unwrap();
        assert_eq!(resume_margin(at(1_001), 0), Some(Duration::from_secs(1)));
        assert_eq!(resume_margin(at(1_001), 1_000), Some(Duration::ZERO));
        assert_eq!(resume_margin(at(1_001), 1_001), None);
    }
}
