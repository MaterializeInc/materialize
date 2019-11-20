// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! The types for the dataflow crate.
//!
//! These are extracted into their own crate so that crates that only depend
//! on the interface of the dataflow crate, and not its implementation, can
//! avoid the dependency, as the dataflow crate is very slow to compile.

// Clippy doesn't understand `as_of` and complains.
#![allow(clippy::wrong_self_convention)]
use std::cmp::Ordering;

use serde::{Deserialize, Serialize};
use url::Url;

use expr::{ColumnOrder, RelationExpr, ScalarExpr};
use repr::{Datum, QualName, RelationDesc, RelationType, Row};

/// System-wide update type.
pub type Diff = isize;

/// System-wide timestamp type.
pub type Timestamp = u64;

/// Specifies when a `Peek` should occur.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PeekWhen {
    /// The peek should occur at the latest possible timestamp that allows the
    /// peek to complete immediately.
    Immediately,
    /// The peek should occur at the latest possible timestamp that has been
    /// accepted by each input source.
    EarliestSource,
    /// The peek should occur at the specified timestamp.
    AtTimestamp(Timestamp),
}

/// The response from a `Peek`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PeekResponse {
    Rows(Vec<Row>),
    Canceled,
}

impl PeekResponse {
    pub fn unwrap_rows(self) -> Vec<Row> {
        match self {
            PeekResponse::Rows(rows) => rows,
            PeekResponse::Canceled => {
                panic!("PeekResponse::unwrap_rows called on PeekResponse::Canceled")
            }
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
/// A batch of updates to be fed to a local input
pub struct Update {
    pub row: Row,
    pub timestamp: u64,
    pub diff: isize,
}

pub fn compare_columns(order: &[ColumnOrder], left: &[Datum], right: &[Datum]) -> Ordering {
    for order in order {
        let (lval, rval) = (left[order.column].clone(), right[order.column].clone());
        let cmp = if order.desc {
            rval.cmp(&lval)
        } else {
            lval.cmp(&rval)
        };
        if cmp != Ordering::Equal {
            return cmp;
        }
    }
    Ordering::Equal
}

/// Instructions for finishing the result of a query.
///
/// The primary reason for the existence of this structure and attendant code
/// is that SQL's ORDER BY requires sorting rows (as already implied by the
/// keywords), whereas much of the rest of SQL is defined in terms of unordered
/// multisets. But as it turns out, the same idea can be used to optimize
/// trivial peeks.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RowSetFinishing {
    /// Order rows by the given columns.
    pub order_by: Vec<ColumnOrder>,
    /// Include only as many rows (after offset).
    pub limit: Option<usize>,
    /// Omit as many rows.
    pub offset: usize,
    /// Include only given columns.
    pub project: Vec<usize>,
}

impl RowSetFinishing {
    pub fn is_trivial(&self) -> bool {
        (self.limit == None) && self.order_by.is_empty() && self.offset == 0
    }
}

/// A description of a dataflow to construct and results to surface.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Default)]
pub struct DataflowDesc {
    /// Named sources used by the dataflow.
    pub sources: Vec<Source>,
    /// Named views produced by the dataflow.
    pub views: Vec<View>,
    /// Named sinks internal to the dataflow.
    pub sinks: Vec<Sink>,
    /// Named indexes used by the dataflow.
    pub indexes: Vec<Index>,
    /// An optional frontier to which inputs should be advanced.
    ///
    /// This is logically equivalent to a timely dataflow `Antichain`,
    /// which should probably be used here instead.
    pub as_of: Option<Vec<Timestamp>>,
}

impl DataflowDesc {
    pub fn new() -> Self {
        Default::default()
    }

    /// Collects the names of the dataflows that this dataflow depends upon.
    pub fn uses(&self) -> Vec<&QualName> {
        let mut out = Vec::new();
        for view in self.views.iter() {
            view.relation_expr.unbound_uses(&mut out);
        }
        for index in self.indexes.iter() {
            out.push(&index.on_name);
        }
        out.sort();
        out.dedup();
        out
    }

    pub fn add_source(mut self, source: Source) -> Self {
        self.sources.push(source);
        self
    }
    pub fn add_view(mut self, view: View) -> Self {
        self.views.push(view);
        self
    }
    pub fn add_sink(mut self, sink: Sink) -> Self {
        self.sinks.push(sink);
        self
    }
    pub fn add_index(mut self, index: Index) -> Self {
        self.indexes.push(index);
        self
    }
    pub fn as_of(mut self, as_of: Option<Vec<Timestamp>>) -> Self {
        self.as_of = as_of;
        self
    }
}

impl From<Source> for DataflowDesc {
    fn from(s: Source) -> Self {
        DataflowDesc::new().add_source(s)
    }
}

impl From<View> for DataflowDesc {
    fn from(v: View) -> Self {
        DataflowDesc::new().add_view(v)
    }
}

impl From<Sink> for DataflowDesc {
    fn from(s: Sink) -> Self {
        DataflowDesc::new().add_sink(s)
    }
}

impl From<Index> for DataflowDesc {
    fn from(i: Index) -> Self {
        DataflowDesc::new().add_index(i)
    }
}

/// A source of updates for a relational collection.
///
/// A source contains enough information to instantiate a stream of changes,
/// as well as related metadata about the columns, their types, and properties
/// of the collection.
#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Source {
    pub name: QualName,
    pub connector: SourceConnector,
    pub desc: RelationDesc,
}

/// A sink for updates to a relational collection.
#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Sink {
    pub name: QualName,
    pub from: (QualName, RelationDesc),
    pub connector: SinkConnector,
}

/// A description of a relational collection in terms of input collections.
#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct View {
    pub name: QualName,
    pub raw_sql: String,
    pub relation_expr: RelationExpr,
    pub desc: RelationDesc,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SourceConnector {
    Local,
    Kafka(KafkaSourceConnector),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KafkaSourceConnector {
    pub addr: std::net::SocketAddr,
    pub topic: String,
    pub raw_schema: String,
    #[serde(with = "url_serde")]
    pub schema_registry_url: Option<Url>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SinkConnector {
    Kafka(KafkaSinkConnector),
    Tail(TailSinkConnector),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KafkaSinkConnector {
    pub addr: std::net::SocketAddr,
    pub topic: String,
    pub schema_id: i32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TailSinkConnector {
    pub tx: comm::mpsc::Sender<Vec<Update>>,
    pub since: Timestamp,
}

/// An index is an arrangement of a dataflow
#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Index {
    /// Name of Index
    pub name: QualName,
    /// Name of collection the index is on
    pub on_name: QualName,
    /// Types of the columns of the `on_name` collection
    pub relation_type: RelationType,
    /// Numbers of the columns to be arranged, in order of decreasing primacy
    /// Includes the numbers of extra columns defined in `fxns`
    pub keys: Vec<usize>,
    /// Functions of the columns to evaluate and arrange on
    pub funcs: Vec<ScalarExpr>,
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use std::error::Error;

    use super::*;
    use repr::{ColumnType, LiteralName, RelationType, ScalarType};

    /// Verify that a basic relation_expr serializes and deserializes to JSON sensibly.
    #[test]
    fn test_roundtrip() -> Result<(), Box<dyn Error>> {
        let dataflow = DataflowDesc::new().add_view(View {
            name: "report".lit(),
            raw_sql: "<none>".into(),
            relation_expr: RelationExpr::Project {
                outputs: vec![1, 2],
                input: Box::new(RelationExpr::join(
                    vec![
                        RelationExpr::Get {
                            name: "orders".lit(),
                            typ: RelationType::new(vec![ColumnType::new(ScalarType::Int64)]),
                        },
                        Box::new(RelationExpr::Union {
                            left: Box::new(RelationExpr::Get {
                                name: "customers2018".lit(),
                                typ: RelationType::new(vec![ColumnType::new(ScalarType::Int64)]),
                            }),
                            right: Box::new(RelationExpr::Get {
                                name: "customers2019".lit(),
                                typ: RelationType::new(vec![ColumnType::new(ScalarType::Int64)]),
                            }),
                        })
                        .distinct(),
                    ],
                    vec![vec![(0, 0), (1, 0)]],
                )),
            },
            desc: RelationDesc::empty()
                .add_column("name", ScalarType::String)
                .add_column("quantity", ScalarType::String),
        });

        let decoded: DataflowDesc =
            serde_json::from_str(&serde_json::to_string_pretty(&dataflow)?)?;
        assert_eq!(decoded, dataflow);

        Ok(())
    }
}
