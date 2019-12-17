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
use std::path::PathBuf;
use url::Url;

use expr::{ColumnOrder, EvalEnv, GlobalId, RelationExpr, ScalarExpr};
use repr::{Datum, RelationDesc, RelationType, Row};

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

/// Compare `left` and `right` using `order`. If that doesn't produce a strict ordering, call `tiebreaker`.
pub fn compare_columns<F>(
    order: &[ColumnOrder],
    left: &[Datum],
    right: &[Datum],
    tiebreaker: F,
) -> Ordering
where
    F: Fn() -> Ordering,
{
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
    tiebreaker()
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
    /// True if the finishing does nothing to any result set.
    pub fn is_trivial(&self) -> bool {
        (self.limit == None) && self.order_by.is_empty() && self.offset == 0
    }
    /// Applies finishing actions to a row set.
    pub fn finish(&self, rows: &mut Vec<Row>) {
        use repr::{RowPacker, RowUnpacker};

        let mut left_unpacker = RowUnpacker::new();
        let mut right_unpacker = RowUnpacker::new();
        let mut sort_by = |left: &Row, right: &Row| {
            compare_columns(
                &self.order_by,
                &left_unpacker.unpack(left),
                &right_unpacker.unpack(right),
                || left.cmp(right),
            )
        };
        let offset = self.offset;
        if offset > rows.len() {
            *rows = Vec::new();
        } else {
            if let Some(limit) = self.limit {
                let offset_plus_limit = offset + limit;
                if rows.len() > offset_plus_limit {
                    pdqselect::select_by(rows, offset_plus_limit, &mut sort_by);
                    rows.truncate(offset_plus_limit);
                }
            }
            if offset > 0 {
                pdqselect::select_by(rows, offset, &mut sort_by);
                rows.drain(..offset);
            }
            rows.sort_by(&mut sort_by);
            let mut unpacker = RowUnpacker::new();
            let mut packer = RowPacker::new();
            for row in rows {
                let datums = unpacker.unpack(&*row);
                let new_row = packer.pack(self.project.iter().map(|i| &datums[*i]));
                drop(datums);
                *row = new_row;
            }
        }
    }
}

/// A description of a dataflow to construct and results to surface.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Default)]
pub struct DataflowDesc {
    /// Sources used by the dataflow.
    pub sources: Vec<(GlobalId, Source)>,
    /// Views produced by the dataflow.
    pub views: Vec<(GlobalId, View)>,
    /// Sinks internal to the dataflow.
    pub sinks: Vec<(GlobalId, Sink)>,
    /// Indexes used by the dataflow.
    pub indexes: Vec<(GlobalId, IndexDesc)>,
    /// An optional frontier to which inputs should be advanced.
    ///
    /// This is logically equivalent to a timely dataflow `Antichain`,
    /// which should probably be used here instead.
    pub as_of: Option<Vec<Timestamp>>,
    /// Human readable name
    pub debug_name: String,
}

impl DataflowDesc {
    pub fn new(name: String) -> Self {
        let mut dd = DataflowDesc::default();
        dd.debug_name = name;
        dd
    }

    /// Collects the IDs of the dataflows that this dataflow depends upon.
    pub fn uses(&self) -> Vec<GlobalId> {
        let mut out = Vec::new();
        for (_id, view) in self.views.iter() {
            view.relation_expr.global_uses(&mut out);
        }
        for (_id, index) in self.indexes.iter() {
            out.push(index.on_id);
        }
        out.sort();
        out.dedup();
        out
    }

    pub fn add_source(mut self, id: GlobalId, source: Source) -> Self {
        self.sources.push((id, source));
        self
    }

    pub fn add_view(mut self, id: GlobalId, view: View) -> Self {
        self.views.push((id, view));
        self
    }

    pub fn add_sink(mut self, id: GlobalId, sink: Sink) -> Self {
        self.sinks.push((id, sink));
        self
    }

    pub fn add_index(mut self, id: GlobalId, index: IndexDesc) -> Self {
        self.indexes.push((id, index));
        self
    }

    pub fn as_of(mut self, as_of: Option<Vec<Timestamp>>) -> Self {
        self.as_of = as_of;
        self
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
    pub connector: SourceConnector,
    pub desc: RelationDesc,
}

/// A sink for updates to a relational collection.
#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Sink {
    pub from: (GlobalId, RelationDesc),
    pub connector: SinkConnector,
}

/// A description of a relational collection in terms of input collections.
#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct View {
    pub raw_sql: String,
    pub relation_expr: RelationExpr,
    pub eval_env: EvalEnv,
    pub desc: RelationDesc,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SourceConnector {
    Local,
    Kafka(KafkaSourceConnector),
    File(FileSourceConnector),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KafkaSourceConnector {
    pub addr: std::net::SocketAddr,
    pub topic: String,
    pub raw_schema: String,
    pub schema_registry_url: Option<Url>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FileSourceConnector {
    pub path: PathBuf,
    pub format: FileFormat,
    pub tail: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum FileFormat {
    Csv(usize),
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KeySql {
    pub raw_sql: String,
    /// true if the raw_sql is a column name, false if it is a function
    pub is_column_name: bool,
    /// true if the raw_sql evaluates to a nullable expression
    pub nullable: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IndexDesc {
    /// Identity of the collection the index is on.
    pub on_id: GlobalId,
    /// Types of the columns of the `on_id` collection.
    pub relation_type: RelationType,
    /// Numbers of the columns to be arranged, in order of decreasing primacy.
    /// the columns to evaluate and arrange on.
    pub keys: Vec<ScalarExpr>,
    /// The evaluation environment for the expressions in `funcs`.
    pub eval_env: EvalEnv,
}

/// An index is an arrangement of a dataflow
#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Index {
    pub desc: IndexDesc,
    /// the human-friendly description on each column for `SHOW INDEXES` to show
    pub raw_keys: Vec<KeySql>,
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use std::error::Error;

    use super::*;
    use expr::Id;
    use repr::{ColumnType, RelationType, ScalarType};

    /// Verify that a basic relation_expr serializes and deserializes to JSON sensibly.
    #[test]
    fn test_roundtrip() -> Result<(), Box<dyn Error>> {
        let dataflow = DataflowDesc::new("none".to_string()).add_view(
            GlobalId::user(4),
            View {
                raw_sql: "<none>".into(),
                relation_expr: RelationExpr::Project {
                    outputs: vec![1, 2],
                    input: Box::new(RelationExpr::join(
                        vec![
                            RelationExpr::Get {
                                id: Id::Global(GlobalId::user(1)),
                                typ: RelationType::new(vec![ColumnType::new(ScalarType::Int64)]),
                            },
                            Box::new(RelationExpr::Union {
                                left: Box::new(RelationExpr::Get {
                                    id: Id::Global(GlobalId::user(2)),
                                    typ: RelationType::new(vec![ColumnType::new(
                                        ScalarType::Int64,
                                    )]),
                                }),
                                right: Box::new(RelationExpr::Get {
                                    id: Id::Global(GlobalId::user(3)),
                                    typ: RelationType::new(vec![ColumnType::new(
                                        ScalarType::Int64,
                                    )]),
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
                eval_env: EvalEnv::default(),
            },
        );

        let decoded: DataflowDesc =
            serde_json::from_str(&serde_json::to_string_pretty(&dataflow)?)?;
        assert_eq!(decoded, dataflow);

        Ok(())
    }
}
