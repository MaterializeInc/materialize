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
use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use url::Url;

use expr::{ColumnOrder, EvalEnv, GlobalId, RelationExpr, ScalarExpr};
use regex::Regex;
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
        let mut sort_by = |left: &Row, right: &Row| {
            compare_columns(&self.order_by, &left.unpack(), &right.unpack(), || {
                left.cmp(right)
            })
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
            for row in rows {
                let datums = row.unpack();
                let new_row = Row::pack(self.project.iter().map(|i| &datums[*i]));
                *row = new_row;
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BuildDesc {
    pub id: GlobalId,
    pub relation_expr: RelationExpr,
    pub eval_env: EvalEnv,
    /// is_some if building a view, none otherwise
    pub typ: Option<RelationType>,
}

/// A description of a dataflow to construct and results to surface.
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct DataflowDesc {
    pub source_imports: HashMap<GlobalId, Source>,
    pub index_imports: HashMap<GlobalId, (IndexDesc, RelationType)>,
    /// Views and indexes to be built and stored in the local context.
    /// Objects must be built in the specific order as the Vec
    pub objects_to_build: Vec<BuildDesc>,
    pub index_exports: Vec<(GlobalId, IndexDesc, RelationType)>,
    pub sink_exports: Vec<(GlobalId, Sink)>,
    /// Maps views to views + indexes needed to generate that view
    pub dependent_objects: HashMap<GlobalId, Vec<GlobalId>>,
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

    pub fn add_index_import(
        &mut self,
        id: GlobalId,
        index: IndexDesc,
        typ: RelationType,
        requesting_view: GlobalId,
    ) {
        self.index_imports.insert(id, (index, typ));
        self.add_dependency(requesting_view, id);
    }

    pub fn add_dependency(&mut self, view_id: GlobalId, dependent_id: GlobalId) {
        self.dependent_objects
            .entry(view_id)
            .or_insert_with(|| Vec::new())
            .push(dependent_id);
    }

    pub fn add_source_import(&mut self, id: GlobalId, source: Source) {
        self.source_imports.insert(id, source);
    }

    pub fn add_view_to_build(&mut self, id: GlobalId, view: View) {
        self.objects_to_build.push(BuildDesc {
            id,
            relation_expr: view.relation_expr,
            eval_env: view.eval_env,
            typ: Some(view.desc.typ().clone()),
        });
    }

    pub fn add_index_to_build(&mut self, id: GlobalId, index: Index) {
        self.objects_to_build.push(BuildDesc {
            id,
            relation_expr: RelationExpr::ArrangeBy {
                input: Box::new(RelationExpr::global_get(
                    index.desc.on_id,
                    index.relation_type,
                )),
                keys: index.desc.keys,
            },
            eval_env: index.eval_env,
            typ: None,
        });
    }

    pub fn add_index_export(&mut self, id: GlobalId, index: IndexDesc, typ: RelationType) {
        self.index_exports.push((id, index, typ));
    }

    pub fn add_sink_export(&mut self, id: GlobalId, sink: Sink) {
        self.sink_exports.push((id, sink));
    }

    pub fn as_of(&mut self, as_of: Option<Vec<Timestamp>>) {
        self.as_of = as_of;
    }

    /// Gets index ids of all indexes require to construct a particular view
    /// If `id` is None, returns all indexes required to construct all views
    /// required by the exports
    pub fn get_imports(&self, id: Option<&GlobalId>) -> HashSet<GlobalId> {
        if let Some(id) = id {
            self.get_imports_inner(id)
        } else {
            let mut result = HashSet::new();
            for (_, desc, _) in &self.index_exports {
                result.extend(self.get_imports_inner(&desc.on_id))
            }
            for (_, sink) in &self.sink_exports {
                result.extend(self.get_imports_inner(&sink.from.0))
            }
            result
        }
    }

    pub fn get_imports_inner(&self, id: &GlobalId) -> HashSet<GlobalId> {
        let mut result = HashSet::new();
        if let Some(dependents) = self.dependent_objects.get(id) {
            for id in dependents {
                result.extend(self.get_imports_inner(id));
            }
        } else {
            result.insert(*id);
        }
        result
    }
}

/// A description of how each row should be decoded, from a string of bytes to a sequence of
/// Differential updates.
#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DataEncoding {
    Avro(AvroEncoding),
    Csv(CsvEncoding),
    Regex {
        #[serde(with = "serde_regex")]
        regex: Regex,
    },
    Protobuf(ProtobufEncoding),
}

/// Encoding in Avro format.
///
/// Assumes Debezium-style `before: ..., after: ...` structure.
#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AvroEncoding {
    pub raw_schema: String,
    pub schema_registry_url: Option<Url>,
}

/// Encoding in CSV format, with no headers, and `n_cols` columns per row.
#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CsvEncoding {
    pub n_cols: usize,
}

/// Encoding in Protobuf format
#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProtobufEncoding {
    pub descriptor_file: String,
    pub message_name: String,
}

/// A source of updates for a relational collection.
///
/// A source contains enough information to instantiate a stream of changes,
/// as well as related metadata about the columns, their types, and properties
/// of the collection.
#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Serialize, Deserialize)]
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

impl View {
    pub fn auto_generate_primary_idx(&self, view_id: GlobalId) -> Index {
        let keys = if let Some(keys) = self.desc.typ().keys.first() {
            keys.clone()
        } else {
            (0..self.desc.typ().column_types.len()).collect()
        };
        Index::new_from_cols(view_id, keys, &self.desc)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceConnector {
    pub connector: ExternalSourceConnector,
    pub encoding: DataEncoding,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ExternalSourceConnector {
    Kafka(KafkaSourceConnector),
    File(FileSourceConnector),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KafkaSourceConnector {
    pub addr: std::net::SocketAddr,
    pub topic: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FileSourceConnector {
    pub path: PathBuf,
    pub tail: bool,
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct IndexDesc {
    /// Identity of the collection the index is on.
    pub on_id: GlobalId,
    /// Numbers of the columns to be arranged, in order of decreasing primacy.
    /// the columns to evaluate and arrange on.
    pub keys: Vec<ScalarExpr>,
}

/// An index is an arrangement of a dataflow
#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Index {
    pub desc: IndexDesc,
    /// the human-friendly description on each column for `SHOW INDEXES` to show
    pub raw_keys: Vec<KeySql>,
    /// Types of the columns of the `on_id` collection.
    pub relation_type: RelationType,
    /// The evaluation environment for the expressions in `funcs`.
    pub eval_env: EvalEnv,
}

impl Index {
    pub fn new(
        on_id: GlobalId,
        keys: Vec<ScalarExpr>,
        key_strings: Vec<String>,
        desc: &RelationDesc,
    ) -> Self {
        let on_relation_type = desc.typ();
        let nullables = keys
            .iter()
            .map(|key| key.typ(on_relation_type).nullable)
            .collect::<Vec<_>>();
        let raw_keys = key_strings
            .into_iter()
            .zip(keys.iter().zip(nullables))
            .map(|(key_string, (key, nullable))| KeySql {
                raw_sql: key_string,
                is_column_name: match key {
                    ScalarExpr::Column(_i) => true,
                    _ => false,
                },
                nullable,
            })
            .collect();
        Index {
            desc: IndexDesc { on_id, keys },
            relation_type: on_relation_type.clone(),
            eval_env: EvalEnv::default(),
            raw_keys,
        }
    }

    pub fn new_from_cols(on_id: GlobalId, keys: Vec<usize>, desc: &RelationDesc) -> Self {
        Index::new(
            on_id,
            keys.iter()
                .map(|c| ScalarExpr::Column(*c))
                .collect::<Vec<_>>(),
            keys.into_iter()
                .map(|c| {
                    desc.get_name(&c)
                        .as_ref()
                        .map(|name| name.to_string())
                        .unwrap_or_else(|| c.to_string())
                })
                .collect::<Vec<_>>(),
            desc,
        )
    }
}
