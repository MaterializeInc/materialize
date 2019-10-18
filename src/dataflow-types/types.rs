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

use expr::{ColumnOrder, RelationExpr, ScalarExpr};
use repr::{Datum, RelationDesc};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use url::Url;
use uuid::Uuid;

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
    Rows(Vec<Vec<Datum>>),
    Canceled,
}

impl PeekResponse {
    pub fn unwrap_rows(self) -> Vec<Vec<Datum>> {
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
    pub row: Vec<Datum>,
    pub timestamp: u64,
    pub diff: isize,
}

pub fn compare_columns(order: &[ColumnOrder], left: &[Datum], right: &[Datum]) -> Ordering {
    for order in order {
        let (lval, rval) = (&left[order.column], &right[order.column]);
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RowSetFinishing {
    /// Include only rows matching all predicates.
    pub filter: Vec<ScalarExpr>,
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

#[derive(Debug, Clone)]
pub enum LocalInput {
    /// Send a batch of updates to the input
    Updates(Vec<Update>),
    /// All future updates will have timestamps >= this timestamp
    Watermark(u64),
}

// /// A named stream of data.
// #[serde(rename_all = "snake_case")]
// #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
// pub enum Dataflow {
//     Source(Source),
//     Sink(Sink),
//     View(View),
// }

// impl Dataflow {
//     pub fn name(&self) -> &str {
//         match self {
//             Dataflow::Source(source) => &source.name,
//             Dataflow::View(view) => &view.name,
//             Dataflow::Sink(sink) => &sink.name,
//         }
//     }

//     /// Collects the names of the dataflows that this dataflow depends upon.
//     pub fn uses(&self) -> Vec<&str> {
//         match self {
//             Dataflow::Source(_src) => Vec::new(),
//             Dataflow::Sink(sink) => vec![&sink.from.0],
//             Dataflow::View(view) => {
//                 let mut out = Vec::new();
//                 view.relation_expr.unbound_uses(&mut out);
//                 out
//             }
//         }
//     }
// }

/// A description of a dataflow to construct and results to surface.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DataflowDescription {
    /// Named sources used by the dataflow.
    pub sources: Vec<Source>,
    /// Named views produced by the dataflow.
    pub views: Vec<View>,
    /// Named sinks internal to the dataflow.
    pub sinks: Vec<Sink>,
    /// An optional frontier to which inputs should be advanced.
    ///
    /// This is logically equivalent to a timely dataflow `Antichain`,
    /// which should probably be used here instead.
    pub as_of: Option<Vec<Timestamp>>,
}

impl DataflowDescription {
    pub fn new(as_of: Option<Vec<Timestamp>>) -> Self {
        Self {
            sources: Vec::new(),
            views: Vec::new(),
            sinks: Vec::new(),
            as_of,
        }
    }

    /// Collects the names of the dataflows that this dataflow depends upon.
    pub fn uses(&self) -> Vec<&str> {
        let mut out = Vec::new();
        for view in self.views.iter() {
            view.relation_expr.unbound_uses(&mut out);
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
    pub fn as_of(mut self, as_of: Option<Vec<Timestamp>>) -> Self {
        self.as_of = as_of;
        self
    }
}

impl From<Source> for DataflowDescription {
    fn from(s: Source) -> Self {
        DataflowDescription::new(None).add_source(s)
    }
}

impl From<View> for DataflowDescription {
    fn from(v: View) -> Self {
        DataflowDescription::new(None).add_view(v)
    }
}

impl From<Sink> for DataflowDescription {
    fn from(s: Sink) -> Self {
        DataflowDescription::new(None).add_sink(s)
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
    pub name: String,
    pub connector: SourceConnector,
    pub desc: RelationDesc,
}

/// A sink for updates to a relational collection.
#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Sink {
    pub name: String,
    pub from: (String, RelationDesc),
    pub connector: SinkConnector,
}

/// A description of a relational collection in terms of input collections.
#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct View {
    pub name: String,
    pub relation_expr: RelationExpr,
    pub desc: RelationDesc,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SourceConnector {
    Kafka(KafkaSourceConnector),
    Local(LocalSourceConnector),
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
pub struct LocalSourceConnector {
    pub uuid: Uuid,
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
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use std::error::Error;

    use super::*;
    use repr::{ColumnType, RelationType, ScalarType};

    /// Verify that a basic relation_expr serializes and deserializes to JSON sensibly.
    #[test]
    fn test_roundtrip() -> Result<(), Box<dyn Error>> {
        let dataflow = DataflowDescription::new(None).add_view(View {
            name: "report".into(),
            relation_expr: RelationExpr::Project {
                outputs: vec![1, 2],
                input: Box::new(RelationExpr::Join {
                    inputs: vec![
                        RelationExpr::Get {
                            name: "orders".into(),
                            typ: RelationType::new(vec![ColumnType::new(ScalarType::Int64)]),
                        },
                        Box::new(RelationExpr::Union {
                            left: Box::new(RelationExpr::Get {
                                name: "customers2018".into(),
                                typ: RelationType::new(vec![ColumnType::new(ScalarType::Int64)]),
                            }),
                            right: Box::new(RelationExpr::Get {
                                name: "customers2019".into(),
                                typ: RelationType::new(vec![ColumnType::new(ScalarType::Int64)]),
                            }),
                        })
                        .distinct(),
                    ],
                    variables: vec![vec![(0, 0), (1, 0)]],
                }),
            },
            desc: RelationDesc::empty()
                .add_column("name", ScalarType::String)
                .add_column("quantity", ScalarType::String),
        });

        let decoded: DataflowDescription =
            serde_json::from_str(&serde_json::to_string_pretty(&dataflow)?)?;
        assert_eq!(decoded, dataflow);

        Ok(())
    }
}
