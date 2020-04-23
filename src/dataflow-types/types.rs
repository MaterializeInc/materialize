// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The types for the dataflow crate.
//!
//! These are extracted into their own crate so that crates that only depend
//! on the interface of the dataflow crate, and not its implementation, can
//! avoid the dependency, as the dataflow crate is very slow to compile.

use std::collections::{HashMap, HashSet};

use failure::ResultExt;
use rusoto_core::Region;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use url::Url;

use expr::{GlobalId, Id, OptimizedRelationExpr, RelationExpr, ScalarExpr, SourceInstanceId};
use interchange::avro;
use interchange::protobuf::{decode_descriptors, validate_descriptors};
use regex::Regex;
use repr::{ColumnType, RelationDesc, RelationType, Row, ScalarType};

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
    /// The peek should occur at the specified timestamp.
    AtTimestamp(Timestamp),
}

/// The response from a `Peek`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum PeekResponse {
    Rows(Vec<Row>),
    Error(String),
    Canceled,
}

impl PeekResponse {
    pub fn unwrap_rows(self) -> Vec<Row> {
        match self {
            PeekResponse::Rows(rows) => rows,
            PeekResponse::Error(_) | PeekResponse::Canceled => {
                panic!("PeekResponse::unwrap_rows called on {:?}", self)
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BuildDesc {
    pub id: GlobalId,
    pub relation_expr: OptimizedRelationExpr,
    /// is_some if building a view, none otherwise
    pub typ: Option<RelationType>,
}

/// A description of a dataflow to construct and results to surface.
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct DataflowDesc {
    pub source_imports: HashMap<SourceInstanceId, SourceDesc>,
    pub index_imports: HashMap<GlobalId, (IndexDesc, RelationType)>,
    /// Views and indexes to be built and stored in the local context.
    /// Objects must be built in the specific order as the Vec
    pub objects_to_build: Vec<BuildDesc>,
    /// Indexes to be made available to be shared with other dataflows
    /// (id of new index, description of index, relationtype of base source/view)
    pub index_exports: Vec<(GlobalId, IndexDesc, RelationType)>,
    /// sinks to be created
    /// (id of new sink, description of sink)
    pub sink_exports: Vec<(GlobalId, SinkDesc)>,
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
            .or_insert_with(Vec::new)
            .push(dependent_id);
    }

    pub fn add_source_import(
        &mut self,
        id: SourceInstanceId,
        connector: SourceConnector,
        desc: RelationDesc,
    ) {
        let source_description = SourceDesc {
            connector,
            operators: None,
            desc,
        };
        self.source_imports.insert(id, source_description);
    }

    pub fn add_view_to_build(
        &mut self,
        id: GlobalId,
        expr: OptimizedRelationExpr,
        typ: RelationType,
    ) {
        self.objects_to_build.push(BuildDesc {
            id,
            relation_expr: expr,
            typ: Some(typ),
        });
    }

    pub fn add_index_to_build(
        &mut self,
        id: GlobalId,
        on_id: GlobalId,
        on_type: RelationType,
        keys: Vec<ScalarExpr>,
    ) {
        self.objects_to_build.push(BuildDesc {
            id,
            relation_expr: OptimizedRelationExpr::declare_optimized(RelationExpr::ArrangeBy {
                input: Box::new(RelationExpr::global_get(on_id, on_type)),
                keys: vec![keys],
            }),
            typ: None,
        });
    }

    pub fn add_index_export(
        &mut self,
        id: GlobalId,
        on_id: GlobalId,
        on_type: RelationType,
        keys: Vec<ScalarExpr>,
    ) {
        self.index_exports
            .push((id, IndexDesc { on_id, keys }, on_type));
    }

    pub fn add_sink_export(
        &mut self,
        id: GlobalId,
        from_id: GlobalId,
        from_desc: RelationDesc,
        connector: SinkConnector,
    ) {
        self.sink_exports.push((
            id,
            SinkDesc {
                from: (from_id, from_desc),
                connector,
            },
        ));
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
    Bytes,
    Text,
    AvroOcf {
        reader_schema: String,
    },
}

impl DataEncoding {
    pub fn desc(&self, envelope: &Envelope) -> Result<RelationDesc, failure::Error> {
        let mut full_desc = if let Envelope::Upsert(key_encoding) = envelope {
            let key_desc = key_encoding.desc(&Envelope::None)?;
            //rename key columns to "key" something if the encoding is not Avro
            let key_desc = match key_encoding {
                DataEncoding::Avro(_) => key_desc,
                _ => RelationDesc::new(
                    key_desc.typ().clone(),
                    key_desc
                        .iter_names()
                        .enumerate()
                        .map(|(i, _)| Some(format!("key{}", i))),
                ),
            };
            let keys = key_desc
                .iter_names()
                .enumerate()
                .map(|(i, _)| i)
                .collect::<Vec<_>>();
            key_desc.add_keys(keys)
        } else {
            RelationDesc::empty()
        };

        let desc = match self {
            DataEncoding::Bytes => RelationDesc::from_cols(vec![(
                ColumnType::new(ScalarType::Bytes),
                Some("data".to_owned()),
            )]),
            DataEncoding::AvroOcf { reader_schema } => {
                avro::validate_value_schema(&*reader_schema, envelope.get_avro_envelope_type())
                    .with_context(|e| format!("validating avro ocf reader schema: {}", e))?
            }
            DataEncoding::Avro(AvroEncoding {
                value_schema,
                key_schema,
                ..
            }) => {
                let mut desc =
                    avro::validate_value_schema(value_schema, envelope.get_avro_envelope_type())
                        .with_context(|e| format!("validating avro value schema: {}", e))?;
                if let Some(key_schema) = key_schema {
                    let keys = avro::validate_key_schema(key_schema, &desc)
                        .with_context(|e| format!("validating avro key schema: {}", e))?;
                    desc = desc.add_keys(keys);
                }
                desc
            }
            DataEncoding::Protobuf(ProtobufEncoding {
                descriptors,
                message_name,
            }) => {
                let d = decode_descriptors(descriptors)?;
                validate_descriptors(message_name, &d)?
            }
            DataEncoding::Regex { regex } => {
                RelationDesc::from_cols(
                    regex
                        .capture_names()
                        .enumerate()
                        // The first capture is the entire matched string.
                        // This will often not be useful, so skip it.
                        // If people want it they can just surround their
                        // entire regex in an explicit capture group.
                        .skip(1)
                        .map(|(i, ocn)| {
                            (
                                ColumnType::new(ScalarType::String).nullable(true),
                                match ocn {
                                    None => Some(format!("column{}", i)),
                                    Some(ocn) => Some(String::from(ocn)),
                                },
                            )
                        })
                        .collect(),
                )
            }
            DataEncoding::Csv(CsvEncoding { n_cols, .. }) => RelationDesc::from_cols(
                (1..=*n_cols)
                    .map(|i| {
                        (
                            ColumnType::new(ScalarType::String),
                            Some(format!("column{}", i)),
                        )
                    })
                    .collect(),
            ),
            DataEncoding::Text => RelationDesc::from_cols(vec![(
                ColumnType::new(ScalarType::String),
                Some("text".to_owned()),
            )]),
        };
        full_desc.add_cols(
            desc.iter()
                .map(|(name, typ)| (name.to_owned(), typ.to_owned())),
        );
        Ok(full_desc)
    }

    pub fn op_name(&self) -> &str {
        match self {
            DataEncoding::Bytes => "Bytes",
            DataEncoding::AvroOcf { .. } => "AvroOcf",
            DataEncoding::Avro(_) => "Avro",
            DataEncoding::Protobuf(_) => "Protobuf",
            DataEncoding::Regex { .. } => "Regex",
            DataEncoding::Csv(_) => "Csv",
            DataEncoding::Text => "Text",
        }
    }
}

/// Encoding in Avro format.
#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AvroEncoding {
    pub key_schema: Option<String>,
    pub value_schema: String,
    pub schema_registry_url: Option<Url>,
}

/// Encoding in CSV format, with `n_cols` columns per row, with an optional header.
#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CsvEncoding {
    pub header_row: bool,
    pub n_cols: usize,
    pub delimiter: u8,
}

/// Encoding in Protobuf format.
#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProtobufEncoding {
    pub descriptors: Vec<u8>,
    pub message_name: String,
}

/// A source of updates for a relational collection.
///
/// A source contains enough information to instantiate a stream of changes,
/// as well as related metadata about the columns, their types, and properties
/// of the collection.
#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceDesc {
    pub connector: SourceConnector,
    /// Optionally, filtering and projection to optionally apply.
    pub operators: Option<LinearOperator>,
    pub desc: RelationDesc,
}

/// A sink for updates to a relational collection.
#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SinkDesc {
    pub from: (GlobalId, RelationDesc),
    pub connector: SinkConnector,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Envelope {
    None,
    Debezium,
    Upsert(DataEncoding),
}

impl Envelope {
    pub fn get_avro_envelope_type(&self) -> avro::EnvelopeType {
        match self {
            Envelope::None => avro::EnvelopeType::None,
            Envelope::Debezium => avro::EnvelopeType::Debezium,
            Envelope::Upsert(_) => avro::EnvelopeType::Upsert,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SourceConnector {
    External {
        connector: ExternalSourceConnector,
        encoding: DataEncoding,
        envelope: Envelope,
        consistency: Consistency,
    },
    Local,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ExternalSourceConnector {
    Kafka(KafkaSourceConnector),
    Kinesis(KinesisSourceConnector),
    File(FileSourceConnector),
    AvroOcf(FileSourceConnector),
}

impl ExternalSourceConnector {
    pub fn metadata_columns(&self) -> Vec<(Option<String>, ColumnType)> {
        match self {
            Self::Kafka(_) => vec![(Some("mz_offset".into()), ColumnType::new(ScalarType::Int64))],
            Self::File(_) => vec![(
                Some("mz_line_no".into()),
                ColumnType::new(ScalarType::Int64),
            )],
            Self::Kinesis(_) => vec![],
            Self::AvroOcf(_) => {
                vec![(Some("mz_obj_no".into()), ColumnType::new(ScalarType::Int64))]
            }
        }
    }

    /// Returns the name of the external source connector.
    pub fn name(&self) -> &'static str {
        match self {
            ExternalSourceConnector::Kafka(_) => "kafka",
            ExternalSourceConnector::Kinesis(_) => "kinesis",
            ExternalSourceConnector::File(_) => "file",
            ExternalSourceConnector::AvroOcf(_) => "avro-ocf",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Consistency {
    BringYourOwn(String),
    RealTime,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KafkaSourceConnector {
    pub url: Url,
    pub topic: String,
    // Represents options specified by user when creating the source, e.g.
    // security settings.
    pub config_options: Vec<(String, String)>,
    // FIXME (brennan) - in the very near future, this should be made into a hashmap of partition |-> offset.
    // #2736
    pub start_offset: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KinesisSourceConnector {
    pub stream_name: String,
    pub region: Region,
    pub access_key: String,
    pub secret_access_key: String,
    pub token: Option<String>,
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
    AvroOcf(AvroOcfSinkConnector),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KafkaSinkConnector {
    pub url: Url,
    pub topic: String,
    pub schema_id: i32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AvroOcfSinkConnector {
    pub path: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TailSinkConnector {
    pub tx: comm::mpsc::Sender<Vec<Update>>,
    pub since: Timestamp,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SinkConnectorBuilder {
    Kafka(KafkaSinkConnectorBuilder),
    AvroOcf(AvroOcfSinkConnectorBuilder),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AvroOcfSinkConnectorBuilder {
    pub path: PathBuf,
    pub file_name_suffix: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KafkaSinkConnectorBuilder {
    pub broker_url: Url,
    pub schema_registry_url: Url,
    pub value_schema: String,
    pub topic_prefix: String,
    pub topic_suffix: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct IndexDesc {
    /// Identity of the collection the index is on.
    pub on_id: GlobalId,
    /// Expressions to be arranged, in order of decreasing primacy.
    pub keys: Vec<ScalarExpr>,
}

/// In-place restrictions that can be made to rows.
///
/// These fields indicate *optional* information that may applied to
/// streams of rows. Any row that does not satisfy all predicates may
/// be discarded, and any column not listed in the projection may be
/// replaced by a default value.
///
/// The intended order of operations is that the predicates are first
/// applied, and columns not in projection can then be overwritten with
/// default values. This allows the projection to avoid capturing columns
/// used by the predicates but not otherwise required.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct LinearOperator {
    /// Rows that do not pass all predicates may be discarded.
    pub predicates: Vec<ScalarExpr>,
    /// Columns not present in `projection` may be replaced with
    /// default values.
    pub projection: Vec<usize>,
}

impl DataflowDesc {
    /// Optimizes the implementation of each dataflow.
    pub fn optimize(&mut self) {
        // This method is currently limited in scope to propagating filtering and
        // projection information, though it could certainly generalize beyond.

        // 1. Propagate demand information from outputs back to sources.
        let mut demand = HashMap::new();

        // Demand all columns of inputs to sinks.
        for (_id, sink) in self.sink_exports.iter() {
            let input_id = sink.from.0;
            demand
                .entry(Id::Global(input_id))
                .or_insert_with(HashSet::new)
                .extend(0..self.arity_of(&input_id));
        }

        // Demand all columns of inputs to exported indexes.
        for (_id, desc, _typ) in self.index_exports.iter() {
            let input_id = desc.on_id;
            demand
                .entry(Id::Global(input_id))
                .or_insert_with(HashSet::new)
                .extend(0..self.arity_of(&input_id));
        }

        // Propagate demand information from outputs to inputs.
        for build_desc in self.objects_to_build.iter_mut().rev() {
            let transform = expr::transform::demand::Demand;
            if let Some(columns) = demand.get(&Id::Global(build_desc.id)).clone() {
                transform.action(
                    build_desc.relation_expr.as_mut(),
                    columns.clone(),
                    &mut demand,
                );
            }
        }

        // Push demand information into the SourceDesc.
        for (source_id, source_desc) in self.source_imports.iter_mut() {
            if let Some(columns) = demand.get(&Id::Global(source_id.sid)).clone() {
                // Install no-op demand information if none exists.
                if source_desc.operators.is_none() {
                    source_desc.operators = Some(LinearOperator {
                        predicates: Vec::new(),
                        projection: (0..source_desc.desc.typ().arity()).collect(),
                    })
                }
                // Restrict required columns by those identified as demanded.
                if let Some(operator) = &mut source_desc.operators {
                    operator.projection.retain(|col| columns.contains(col));
                }
            }
        }
    }

    /// The number of columns associated with an identifier in the dataflow.
    fn arity_of(&self, id: &GlobalId) -> usize {
        for (source, desc) in self.source_imports.iter() {
            if &source.sid == id {
                return desc.desc.typ().arity();
            }
        }
        for (_index_id, (desc, typ)) in self.index_imports.iter() {
            if &desc.on_id == id {
                return typ.arity();
            }
        }
        for desc in self.objects_to_build.iter() {
            if &desc.id == id {
                return desc.relation_expr.as_ref().arity();
            }
        }
        panic!("GlobalId {} not found in DataflowDesc", id);
    }
}
