// Copyright Materialize, Inc. and contributors. All rights reserved.
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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::ops::Add;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use globset::Glob;
use log::warn;
use regex::Regex;
use serde::{Deserialize, Serialize};
use timely::progress::frontier::Antichain;
use url::Url;
use uuid::Uuid;

use aws_util::aws;
use expr::{GlobalId, MirRelationExpr, MirScalarExpr, OptimizedMirRelationExpr, PartitionId};
use interchange::avro::{self, DebeziumDeduplicationStrategy};
use interchange::protobuf;
use kafka_util::KafkaAddrs;
use repr::{ColumnName, ColumnType, Diff, RelationDesc, RelationType, Row, ScalarType, Timestamp};

/// The response from a `Peek`.
///
/// Note that each `Peek` expects to generate exactly one `PeekResponse`, i.e.
/// we expect a 1:1 contract between `Peek` and `PeekResponse`.
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

/// Various responses that can be communicated about the progress of a TAIL command.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum TailResponse {
    /// Rows that should be returned in order to the client.
    Rows(Vec<Row>),
    /// Sent once the stream is complete. Indicates the end.
    Complete,
    /// The TAIL dataflow was dropped before completing. Indicates the end.
    Dropped,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
/// A batch of updates to be fed to a local input
pub struct Update {
    pub row: Row,
    pub timestamp: u64,
    pub diff: Diff,
}

/// A commonly used name for dataflows contain MIR expressions.
pub type DataflowDesc = DataflowDescription<OptimizedMirRelationExpr>;

/// An association of a global identifier to an expression.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BuildDesc<View> {
    pub id: GlobalId,
    pub view: View,
}

/// A description of a dataflow to construct and results to surface.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DataflowDescription<View> {
    /// Sources made available to the dataflow.
    pub source_imports: BTreeMap<GlobalId, (SourceDesc, GlobalId)>,
    /// Indexes made available to the dataflow.
    pub index_imports: BTreeMap<GlobalId, (IndexDesc, RelationType)>,
    /// Views and indexes to be built and stored in the local context.
    /// Objects must be built in the specific order, as there may be
    /// dependencies of later objects on prior identifiers.
    pub objects_to_build: Vec<BuildDesc<View>>,
    /// Indexes to be made available to be shared with other dataflows
    /// (id of new index, description of index, relationtype of base source/view)
    pub index_exports: Vec<(GlobalId, IndexDesc, RelationType)>,
    /// sinks to be created
    /// (id of new sink, description of sink)
    pub sink_exports: Vec<(GlobalId, SinkDesc)>,
    /// Maps views to views + indexes needed to generate that view
    pub dependent_objects: BTreeMap<GlobalId, Vec<GlobalId>>,
    /// An optional frontier to which inputs should be advanced.
    ///
    /// If this is set, it should override the default setting determined by
    /// the upper bound of `since` frontiers contributing to the dataflow.
    /// It is an error for this to be set to a frontier not beyond that default.
    pub as_of: Option<Antichain<Timestamp>>,
    /// Human readable name
    pub debug_name: String,
}

impl DataflowDescription<OptimizedMirRelationExpr> {
    /// Creates a new dataflow description with a human-readable name.
    pub fn new(name: String) -> Self {
        Self {
            source_imports: Default::default(),
            index_imports: Default::default(),
            objects_to_build: Vec::new(),
            index_exports: Default::default(),
            sink_exports: Default::default(),
            dependent_objects: Default::default(),
            as_of: Default::default(),
            debug_name: name,
        }
    }

    /// Imports a previously exported index.
    ///
    /// This method makes available an index previously exported as `id`, identified
    /// to the query by `description` (which names the view the index arranges, and
    /// the keys by which it is arranged).
    ///
    /// The `requesting_view` argument is currently necessary to correctly track the
    /// dependencies of views on indexes.
    pub fn import_index(
        &mut self,
        id: GlobalId,
        description: IndexDesc,
        typ: RelationType,
        requesting_view: GlobalId,
    ) {
        self.index_imports.insert(id, (description, typ));
        self.record_depends_on(requesting_view, id);
    }

    /// Imports a source and makes it available as `id`.
    ///
    /// The `orig_id` identifier must be set correctly, and is used to index various
    /// internal data structures. Little else is known about it.
    pub fn import_source(&mut self, id: GlobalId, description: SourceDesc, orig_id: GlobalId) {
        self.source_imports.insert(id, (description, orig_id));
    }

    /// Binds to `id` the relation expression `view`.
    pub fn insert_view(&mut self, id: GlobalId, view: OptimizedMirRelationExpr) {
        for get_id in view.global_uses() {
            self.record_depends_on(id, get_id);
        }
        self.objects_to_build.push(BuildDesc { id, view });
    }

    /// Exports as `id` an index on `on_id`.
    ///
    /// Future uses of `import_index` in other dataflow descriptions may use `id`,
    /// as long as this dataflow has not been terminated in the meantime.
    pub fn export_index(&mut self, id: GlobalId, description: IndexDesc, on_type: RelationType) {
        // We first create a "view" named `id` that ensures that the
        // data are correctly arranged and available for export.
        self.insert_view(
            id,
            OptimizedMirRelationExpr::declare_optimized(MirRelationExpr::ArrangeBy {
                input: Box::new(MirRelationExpr::global_get(
                    description.on_id,
                    on_type.clone(),
                )),
                keys: vec![description.keys.clone()],
            }),
        );
        self.index_exports.push((id, description, on_type));
    }

    /// Exports as `id` a sink described by `description`.
    pub fn export_sink(&mut self, id: GlobalId, description: SinkDesc) {
        self.sink_exports.push((id, description));
    }

    /// Records a dependency of `view_id` on `depended_upon`.
    // TODO(#7267): This information should ideally be automatically extracted
    // from the imported sources and indexes, rather than relying on the caller
    // to correctly specify them.
    fn record_depends_on(&mut self, view_id: GlobalId, depended_upon: GlobalId) {
        self.dependent_objects
            .entry(view_id)
            .or_insert_with(Vec::new)
            .push(depended_upon);
    }

    /// Returns true iff `id` is already imported.
    pub fn is_imported(&self, id: &GlobalId) -> bool {
        self.objects_to_build.iter().any(|bd| &bd.id == id)
            || self.source_imports.iter().any(|(i, _)| i == id)
    }

    /// Assigns the `as_of` frontier to the supplied argument.
    ///
    /// This method allows the dataflow to indicate a frontier up through
    /// which all times should be advanced. This can be done for at least
    /// two reasons: 1. correctness and 2. performance.
    ///
    /// Correctness may require an `as_of` to ensure that historical detail
    /// is consolidated at representative times that do not present specific
    /// detail that is not specifically correct. For example, updates may be
    /// compacted to times that are no longer the source times, but instead
    /// some byproduct of when compaction was executed; we should not present
    /// those specific times as meaningfully different from other equivalent
    /// times.
    ///
    /// Performance may benefit from an aggressive `as_of` as it reduces the
    /// number of distinct moments at which collections vary. Differential
    /// dataflow will refresh its outputs at each time its inputs change and
    /// to moderate that we can minimize the volume of distinct input times
    /// as much as possible.
    ///
    /// Generally, one should consider setting `as_of` at least to the `since`
    /// frontiers of contributing data sources and as aggressively as the
    /// computation permits.
    pub fn set_as_of(&mut self, as_of: Antichain<Timestamp>) {
        self.as_of = Some(as_of);
    }

    /// The number of columns associated with an identifier in the dataflow.
    pub fn arity_of(&self, id: &GlobalId) -> usize {
        for (source_id, (desc, _orig_id)) in self.source_imports.iter() {
            if source_id == id {
                return desc.bare_desc.arity();
            }
        }
        for (_index_id, (desc, typ)) in self.index_imports.iter() {
            if &desc.on_id == id {
                return typ.arity();
            }
        }
        for desc in self.objects_to_build.iter() {
            if &desc.id == id {
                return desc.view.arity();
            }
        }
        panic!("GlobalId {} not found in DataflowDesc", id);
    }
}

impl<View> DataflowDescription<View> {
    /// Collects all indexes required to construct all exports.
    pub fn get_all_imports(&self) -> HashSet<GlobalId> {
        let mut result = HashSet::new();
        for (_, desc, _) in &self.index_exports {
            result.extend(self.get_imports(&desc.on_id))
        }
        for (_, sink) in &self.sink_exports {
            result.extend(self.get_imports(&sink.from))
        }
        result
    }

    /// Collects all transitively dependent identifiers that do not have their own dependencies.
    pub fn get_imports(&self, id: &GlobalId) -> HashSet<GlobalId> {
        let mut result = HashSet::new();
        let mut worklist = vec![id];
        while let Some(id) = worklist.pop() {
            result.insert(*id);
            if let Some(dependents) = self.dependent_objects.get(id) {
                for id in dependents.iter() {
                    if !result.contains(id) {
                        worklist.push(id);
                    }
                }
            }
        }
        result.retain(|id| self.dependent_objects.get(id).is_none());
        result
    }
}

/// A description of how to interpret data from various sources
///
/// Almost all sources only present values as part of their records, but Kafka allows a key to be
/// associated with each record, which has a possibly independent encoding.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SourceDataEncoding {
    Single(DataEncoding),
    KeyValue {
        key: DataEncoding,
        value: DataEncoding,
    },
}

/// A description of how each row should be decoded, from a string of bytes to a sequence of
/// Differential updates.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DataEncoding {
    Avro(AvroEncoding),
    AvroOcf(AvroOcfEncoding),
    Protobuf(ProtobufEncoding),
    Csv(CsvEncoding),
    Regex(RegexEncoding),
    Postgres,
    Bytes,
    Text,
}

impl SourceDataEncoding {
    /// Return either the Single encoding if this was a `SourceDataEncoding::Single`, else return the value encoding
    pub fn value(self) -> DataEncoding {
        match self {
            SourceDataEncoding::Single(encoding) => encoding,
            SourceDataEncoding::KeyValue { value, .. } => value,
        }
    }

    pub fn value_ref(&self) -> &DataEncoding {
        match self {
            SourceDataEncoding::Single(encoding) => encoding,
            SourceDataEncoding::KeyValue { value, .. } => value,
        }
    }

    pub fn desc(
        &self,
        envelope: &SourceEnvelope,
        key_envelope: Option<&KeyEnvelope>,
    ) -> Result<RelationDesc, anyhow::Error> {
        match self {
            SourceDataEncoding::Single(enc) => enc.desc(envelope, RelationDesc::empty(), None),
            SourceDataEncoding::KeyValue { key, value } => match key_envelope {
                None => value.desc(envelope, RelationDesc::empty(), None),
                Some(KeyEnvelope::Flattened) => insert_flattened_key(key, value, envelope, false),
                Some(KeyEnvelope::LegacyUpsert) => insert_flattened_key(key, value, envelope, true),
                Some(KeyEnvelope::Named(key_name)) => {
                    insert_named_key(key_name, key, value, envelope)
                }
            },
        }
    }
}

/// modify the desc to include the key using the appropriate name
fn insert_named_key(
    key_name: &str,
    key: &DataEncoding,
    value: &DataEncoding,
    envelope: &SourceEnvelope,
) -> Result<RelationDesc, anyhow::Error> {
    let key_desc = {
        let key_desc = key.desc(&SourceEnvelope::None, RelationDesc::empty(), None)?;

        // It doesn't make sense for the key to have keys.
        assert!(key_desc.typ().keys.is_empty());

        // if the key has multiple objects, nest them as a record inside of a single name
        if key_desc.arity() > 1 {
            let key_type = key_desc.typ();
            let key_as_record = RelationType::new(vec![ColumnType {
                nullable: false,
                scalar_type: ScalarType::Record {
                    fields: key_desc
                        .iter_names()
                        .enumerate()
                        .map(|(i, name)| {
                            name.map(Clone::clone)
                                .unwrap_or_else(|| format!("key{}", i).into())
                        })
                        .zip(key_type.column_types.iter())
                        .map(|(name, typ)| (name, typ.clone()))
                        .collect(),
                    custom_oid: None,
                    custom_name: None,
                },
            }]);

            RelationDesc::new(key_as_record, vec![Some(key_name.to_string())])
        } else {
            key_desc.with_names(vec![Some(key_name.to_string())])
        }
    };
    // In all cases the first column is the key
    let key_desc = key_desc.with_key(vec![0]);

    value.desc(envelope, key_desc, None)
}

fn insert_flattened_key(
    key: &DataEncoding,
    value: &DataEncoding,
    envelope: &SourceEnvelope,
    is_legacy_upsert: bool,
) -> Result<RelationDesc, anyhow::Error> {
    let key_desc = {
        let key_desc = key.desc(&SourceEnvelope::None, RelationDesc::empty(), None)?;

        // It doesn't make sense for the key to have keys.
        assert!(key_desc.typ().keys.is_empty());

        // Add the key columns as a key.
        let key_indices = (0..key_desc.arity()).collect();
        let key_desc = key_desc.with_key(key_indices);

        if let DataEncoding::Avro(_) = key {
            key_desc
        } else if is_legacy_upsert {
            // Rename key columns to "keyN" if the encoding is not Avro and we're in unadorned
            // upsert mode
            let names = (0..key_desc.arity()).map(|i| Some(format!("key{}", i)));
            key_desc.with_names(names)
        } else {
            key_desc
        }
    };
    let key_schema = if let DataEncoding::Avro(AvroEncoding { schema, .. }) = key {
        Some(&**schema)
    } else {
        None
    };
    value.desc(envelope, key_desc, key_schema)
}

impl DataEncoding {
    /// Computes the [`RelationDesc`] for the relation specified by this
    /// data encoding and envelope.
    ///
    /// If a key desc is provided it will be prepended to the returned desc
    fn desc(
        &self,
        envelope: &SourceEnvelope,
        key_desc: RelationDesc,
        key_schema: Option<&str>,
    ) -> Result<RelationDesc, anyhow::Error> {
        // Add columns for the data, based on the encoding format.
        Ok(match self {
            DataEncoding::Bytes => {
                key_desc.with_named_column("data", ScalarType::Bytes.nullable(false))
            }
            DataEncoding::AvroOcf(AvroOcfEncoding { .. })
            | DataEncoding::Avro(AvroEncoding { .. }) => {
                let value_schema = match self {
                    DataEncoding::AvroOcf(AvroOcfEncoding { reader_schema }) => (reader_schema),
                    DataEncoding::Avro(AvroEncoding { schema, .. }) => schema,
                    _ => unreachable!(),
                };
                let mut columns =
                    avro::validate_value_schema(value_schema, envelope.get_avro_envelope_type())
                        .context("validating avro value schema")?
                        .into_iter()
                        .collect::<Vec<_>>();
                if envelope.get_avro_envelope_type() == avro::EnvelopeType::Debezium {
                    // TODO [btv] - Get rid of this, when we can. Right now source processing is still not fully orthogonal,
                    // so we have some special logic in the debezium processor that only passes on
                    // the first two columns ("before" and "after"), and uses the information in the other ones itself
                    columns.truncate(2);
                }
                let desc = columns.into_iter().fold(key_desc, |desc, (name, ty)| {
                    desc.with_named_column(name, ty)
                });
                let key_schema_indices = key_schema.as_ref().and_then(|key_schema| {
                    avro::validate_key_schema(key_schema, &desc)
                        .map(Some)
                        .unwrap_or_else(|e| {
                            warn!("Not using key due to error: {}", e);
                            None
                        })
                });
                if envelope.get_avro_envelope_type() == avro::EnvelopeType::Debezium {
                    desc
                } else {
                    if let Some(key) = key_schema_indices {
                        desc.with_key(key)
                    } else {
                        desc
                    }
                }
            }
            DataEncoding::Protobuf(ProtobufEncoding {
                descriptors,
                message_name,
            }) => {
                protobuf::decode::DecodedDescriptors::from_bytes(descriptors, message_name.into())?
                    .validate()?
                    .into_iter()
                    .fold(key_desc, |desc, (name, ty)| {
                        desc.with_named_column(name.unwrap(), ty)
                    })
            }
            DataEncoding::Regex(RegexEncoding { regex }) => regex
                .capture_names()
                .enumerate()
                // The first capture is the entire matched string. This will
                // often not be useful, so skip it. If people want it they can
                // just surround their entire regex in an explicit capture
                // group.
                .skip(1)
                .fold(key_desc, |desc, (i, name)| {
                    let name = match name {
                        None => format!("column{}", i),
                        Some(name) => name.to_owned(),
                    };
                    let ty = ScalarType::String.nullable(true);
                    desc.with_named_column(name, ty)
                }),
            DataEncoding::Csv(CsvEncoding { columns, .. }) => match columns {
                ColumnSpec::Count(n) => (1..=*n).into_iter().fold(key_desc, |desc, i| {
                    desc.with_named_column(
                        format!("column{}", i),
                        ScalarType::String.nullable(false),
                    )
                }),
                ColumnSpec::Header { names } => {
                    names.iter().map(|s| &**s).fold(key_desc, |desc, name| {
                        desc.with_named_column(name, ScalarType::String.nullable(false))
                    })
                }
            },
            DataEncoding::Text => {
                key_desc.with_named_column("text", ScalarType::String.nullable(false))
            }
            DataEncoding::Postgres => key_desc
                .with_named_column("oid", ScalarType::Int32.nullable(false))
                .with_named_column(
                    "row_data",
                    ScalarType::List {
                        element_type: Box::new(ScalarType::String),
                        custom_oid: None,
                    }
                    .nullable(false),
                ),
        })
    }

    pub fn op_name(&self) -> &'static str {
        match self {
            DataEncoding::Bytes => "Bytes",
            DataEncoding::AvroOcf { .. } => "AvroOcf",
            DataEncoding::Avro(_) => "Avro",
            DataEncoding::Protobuf(_) => "Protobuf",
            DataEncoding::Regex { .. } => "Regex",
            DataEncoding::Csv(_) => "Csv",
            DataEncoding::Text => "Text",
            DataEncoding::Postgres => "Postgres",
        }
    }
}

/// Encoding in Avro format.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AvroEncoding {
    pub schema: String,
    pub schema_registry_config: Option<ccsr::ClientConfig>,
    pub confluent_wire_format: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AvroOcfEncoding {
    pub reader_schema: String,
}

/// Encoding in Protobuf format.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProtobufEncoding {
    pub descriptors: Vec<u8>,
    pub message_name: String,
}

/// Arguments necessary to define how to decode from CSV format
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CsvEncoding {
    pub columns: ColumnSpec,
    pub delimiter: u8,
}

/// Determines the RelationDesc and decoding of CSV objects
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ColumnSpec {
    /// The first row is not a header row, and all columns get default names like `columnN`.
    Count(usize),
    /// The first row is a header row and therefore does become data
    ///
    /// Each of the values in `names` becomes the default name of a column in the dataflow.
    Header { names: Vec<String> },
}

impl ColumnSpec {
    /// The number of columns described by the column spec.
    pub fn arity(&self) -> usize {
        match self {
            ColumnSpec::Count(n) => *n,
            ColumnSpec::Header { names } => names.len(),
        }
    }

    pub fn into_header_names(self) -> Option<Vec<String>> {
        match self {
            ColumnSpec::Count(_) => None,
            ColumnSpec::Header { names } => Some(names),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegexEncoding {
    #[serde(with = "serde_regex")]
    pub regex: Regex,
}

/// A source of updates for a relational collection.
///
/// A source contains enough information to instantiate a stream of changes,
/// as well as related metadata about the columns, their types, and properties
/// of the collection.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceDesc {
    pub name: String,
    pub connector: SourceConnector,
    /// Optionally, filtering and projection that may optimistically be applied
    /// to the output of the source.
    pub operators: Option<LinearOperator>,
    pub bare_desc: RelationDesc,
    pub persisted_name: Option<String>,
}

/// A sink for updates to a relational collection.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SinkDesc {
    pub from: GlobalId,
    pub from_desc: RelationDesc,
    pub connector: SinkConnector,
    pub envelope: Option<SinkEnvelope>,
    pub as_of: SinkAsOf,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SinkEnvelope {
    Debezium,
    Upsert,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SinkAsOf {
    pub frontier: Antichain<Timestamp>,
    pub strict: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SourceEnvelope {
    None,
    Debezium(DebeziumDeduplicationStrategy, DebeziumMode),
    Upsert,
    CdcV2,
}

impl SourceEnvelope {
    pub fn get_avro_envelope_type(&self) -> avro::EnvelopeType {
        match self {
            SourceEnvelope::None => avro::EnvelopeType::None,
            SourceEnvelope::Debezium { .. } => avro::EnvelopeType::Debezium,
            SourceEnvelope::Upsert => avro::EnvelopeType::Upsert,
            SourceEnvelope::CdcV2 => avro::EnvelopeType::CdcV2,
        }
    }
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum DebeziumMode {
    Plain,
    /// Keep track of keys from upstream and discard retractions for new keys
    Upsert,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Compression {
    Gzip,
    None,
}

/// The meaning of the timestamp number produced by data sources. This type
/// is not concerned with the source of the timestamp (like if the data came
/// from a Debezium consistency topic or a CDCv2 stream), instead only what the
/// timestamp number means.
///
/// Some variants here have attached data used to differentiate incomparable
/// instantiations. These attached data types should be expanded in the future
/// if we need to tell apart more kinds of sources.
#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum Timeline {
    /// EpochMilliseconds means the timestamp is the number of milliseconds since
    /// the Unix epoch.
    EpochMilliseconds,
    /// Counter means the timestamp starts at 1 and is incremented for each
    /// transaction. It holds the BYO source so different instantiations can be
    /// differentiated.
    Counter(BringYourOwn),
    /// External means the timestamp comes from an external data source and we
    /// don't know what the number means. The attached String is the source's name,
    /// which will result in different sources being incomparable.
    External(String),
    /// User means the user has manually specified a timeline. The attached
    /// String is specified by the user, allowing them to decide sources that are
    /// joinable.
    User(String),
}

/// A struct to hold more specific information about where a BYO source
/// came from so we can differentiate between topics of the same name across
/// different brokers.
#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct BringYourOwn {
    pub broker: String,
    pub topic: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SourceConnector {
    External {
        connector: ExternalSourceConnector,
        encoding: SourceDataEncoding,
        envelope: SourceEnvelope,
        consistency: Consistency,
        ts_frequency: Duration,
        timeline: Timeline,
    },
    Local {
        timeline: Timeline,
    },
}

impl SourceConnector {
    /// Returns `true` if this connector yields input data (including
    /// timestamps) that is stable across restarts. This is important for
    /// exactly-once Sinks that need to ensure that the same data is written,
    /// even when failures/restarts happen.
    pub fn yields_stable_input(&self) -> bool {
        if let SourceConnector::External { connector, .. } = self {
            // Conservatively, set all Kafka (BYO or RT), File, or AvroOcf sources as having stable inputs because
            // we know they will be read in a known, repeatable offset order (modulo compaction for some Kafka sources).
            match connector {
                ExternalSourceConnector::Kafka(_)
                | ExternalSourceConnector::File(_)
                | ExternalSourceConnector::AvroOcf(_) => true,
                // Currently, the Kinesis connector assigns "offsets" by counting the message in the order it was received
                // and this order is not replayable across different reads of the same Kinesis stream.
                ExternalSourceConnector::Kinesis(_) => false,
                _ => false,
            }
        } else {
            false
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            SourceConnector::External { connector, .. } => connector.name(),
            SourceConnector::Local { .. } => "local",
        }
    }

    pub fn timeline(&self) -> Timeline {
        match self {
            SourceConnector::External { timeline, .. } => timeline.clone(),
            SourceConnector::Local { timeline, .. } => timeline.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ExternalSourceConnector {
    Kafka(KafkaSourceConnector),
    Kinesis(KinesisSourceConnector),
    File(FileSourceConnector),
    AvroOcf(FileSourceConnector),
    S3(S3SourceConnector),
    Postgres(PostgresSourceConnector),
    PubNub(PubNubSourceConnector),
}

impl ExternalSourceConnector {
    /// Returns the name and type of each additional metadata column that
    /// Materialize will automatically append to the source's inherent columns.
    ///
    /// Presently, each source type exposes precisely one metadata column that
    /// corresponds to some source-specific record counter. For example, file
    /// sources use a line number, while Kafka sources use a topic offset.
    ///
    /// The columns declared here must be kept in sync with the actual source
    /// implementations that produce these columns.
    pub fn metadata_columns(&self) -> Vec<(ColumnName, ColumnType)> {
        match self {
            Self::Kafka(_) => vec![("mz_offset".into(), ScalarType::Int64.nullable(false))],
            Self::File(_) => vec![("mz_line_no".into(), ScalarType::Int64.nullable(false))],
            Self::Kinesis(_) => vec![("mz_offset".into(), ScalarType::Int64.nullable(false))],
            Self::AvroOcf(_) => vec![("mz_obj_no".into(), ScalarType::Int64.nullable(false))],
            // TODO: should we include object key and possibly object-internal offset here?
            Self::S3(_) => vec![("mz_record".into(), ScalarType::Int64.nullable(false))],
            Self::Postgres(_) => vec![],
            Self::PubNub(_) => vec![],
        }
    }

    /// Returns the name of the external source connector.
    pub fn name(&self) -> &'static str {
        match self {
            ExternalSourceConnector::Kafka(_) => "kafka",
            ExternalSourceConnector::Kinesis(_) => "kinesis",
            ExternalSourceConnector::File(_) => "file",
            ExternalSourceConnector::AvroOcf(_) => "avro-ocf",
            ExternalSourceConnector::S3(_) => "s3",
            ExternalSourceConnector::Postgres(_) => "postgres",
            ExternalSourceConnector::PubNub(_) => "pubnub",
        }
    }

    /// Optionally returns the name of the upstream resource this source corresponds to.
    /// (Currently only implemented for Kafka and Kinesis, to match old-style behavior
    ///  TODO: decide whether we want file paths and other upstream names to show up in metrics too.
    pub fn upstream_name(&self) -> Option<&str> {
        match self {
            ExternalSourceConnector::Kafka(KafkaSourceConnector { topic, .. }) => {
                Some(topic.as_str())
            }
            ExternalSourceConnector::Kinesis(KinesisSourceConnector { stream_name, .. }) => {
                Some(stream_name.as_str())
            }
            ExternalSourceConnector::File(_) => None,
            ExternalSourceConnector::AvroOcf(_) => None,
            ExternalSourceConnector::S3(_) => None,
            ExternalSourceConnector::Postgres(_) => None,
            ExternalSourceConnector::PubNub(_) => None,
        }
    }

    pub fn is_delimited(&self) -> bool {
        match self {
            ExternalSourceConnector::AvroOcf(_) => false,
            ExternalSourceConnector::File(_) => false,
            ExternalSourceConnector::S3(_) => false,
            ExternalSourceConnector::Kafka(_) => true,
            ExternalSourceConnector::Kinesis(_) => true,
            ExternalSourceConnector::Postgres(_) => true,
            ExternalSourceConnector::PubNub(_) => true,
        }
    }

    pub fn key_envelope(&self) -> Option<&KeyEnvelope> {
        if let ExternalSourceConnector::Kafka(KafkaSourceConnector { include_key, .. }) = &self {
            include_key.as_ref()
        } else {
            None
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Consistency {
    BringYourOwn(BringYourOwn),
    RealTime,
}

/// Universal language for describing message positions in Materialize, in a source independent
/// way. Invidual sources like Kafka or File sources should explicitly implement their own offset
/// type that converts to/From MzOffsets. A 0-MzOffset denotes an empty stream.
#[derive(Copy, Clone, Debug, PartialEq, PartialOrd, Eq, Hash, Serialize, Deserialize)]
pub struct MzOffset {
    pub offset: i64,
}

impl fmt::Display for MzOffset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.offset)
    }
}

impl Add<i64> for MzOffset {
    type Output = MzOffset;

    fn add(self, x: i64) -> MzOffset {
        MzOffset {
            offset: self.offset + x,
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct KafkaOffset {
    pub offset: i64,
}

/// Structure wrapping a timestamp update from a source
/// If RT, contains a partition count
/// If BYO, contains a tuple (PartitionCount, PartitionID, Timestamp, Offset),
/// which informs workers that messages with Offset on PartititionId will be timestamped
/// with Timestamp.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TimestampSourceUpdate {
    /// Update for an RT source: contains a new partition to add to this source.
    RealTime(PartitionId),
    /// Timestamp update for a BYO source: contains a PartitionID, Timestamp,
    /// MzOffset tuple. This tuple informs workers that messages with Offset on
    /// PartitionId will be timestamped with Timestamp.
    BringYourOwn(PartitionId, u64, MzOffset),
}

/// Convert from KafkaOffset to MzOffset (1-indexed)
impl From<KafkaOffset> for MzOffset {
    fn from(kafka_offset: KafkaOffset) -> Self {
        MzOffset {
            offset: kafka_offset.offset + 1,
        }
    }
}

/// Convert from MzOffset to Kafka::Offset as long as
/// the offset is not negative
impl Into<KafkaOffset> for MzOffset {
    fn into(self) -> KafkaOffset {
        KafkaOffset {
            offset: self.offset - 1,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KafkaSourceConnector {
    pub addrs: KafkaAddrs,
    pub topic: String,
    // Represents options specified by user when creating the source, e.g.
    // security settings.
    pub config_options: BTreeMap<String, String>,
    // Map from partition -> starting offset
    pub start_offsets: HashMap<i32, i64>,
    pub group_id_prefix: Option<String>,
    pub cluster_id: Uuid,
    /// If present, include the key columns as an output column of the source with the given properties.
    pub include_key: Option<KeyEnvelope>,
    /// If present, include the timestamp as an output column of the source with the given name
    pub include_timestamp: Option<String>,
    /// If present, include the partition as an output column of the source with the given name.
    pub include_partition: Option<String>,
    /// If present, include the topic as an output column of the source with the given name.
    pub include_topic: Option<String>,
}

/// Whether and how to include the key portion of a stream in dataflows
///
/// Currently only Kafka streams have Key parts of messages, but there do exist other streaming
/// systems which we do not yet integrate with that have a similar concept.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum KeyEnvelope {
    /// For composite key encodings, pull the fields from the encoding into columns.
    Flattened,
    /// Upsert is identical to Flattened but differs for non-avro sources, for which key names are overwritten.
    LegacyUpsert,
    /// Always use the given name for the key.
    ///
    /// * For a single-field key, this means that the column will get the given name.
    /// * For a multi-column key, the columns will get packed into a [`ScalarType::Record`], and
    ///   that Record will get the given name.
    Named(String),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KinesisSourceConnector {
    pub stream_name: String,
    pub aws_info: aws::ConnectInfo,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FileSourceConnector {
    pub path: PathBuf,
    pub tail: bool,
    pub compression: Compression,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PostgresSourceConnector {
    pub conn: String,
    pub publication: String,
    pub slot_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PubNubSourceConnector {
    pub subscribe_key: String,
    pub channel: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct S3SourceConnector {
    pub key_sources: Vec<S3KeySource>,
    pub pattern: Option<Glob>,
    pub aws_info: aws::ConnectInfo,
    pub compression: Compression,
}

/// A Source of Object Key names, the argument of the `DISCOVER OBJECTS` clause
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum S3KeySource {
    /// Scan the S3 Bucket to discover keys to download
    Scan { bucket: String },
    /// Load object keys based on the contents of an S3 Notifications channel
    ///
    /// S3 notifications channels can be configured to go to SQS, which is the
    /// only target we currently support.
    SqsNotifications { queue: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SinkConnector {
    Kafka(KafkaSinkConnector),
    Tail(TailSinkConnector),
    AvroOcf(AvroOcfSinkConnector),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KafkaSinkConsistencyConnector {
    pub topic: String,
    pub schema_id: i32,
    // gate_ts is the most recent high watermark tailed from the consistency topic
    // Exactly-once sinks use this to determine when they should start publishing again. This
    // tells them when they have caught up to where the previous materialize instance stopped.
    pub gate_ts: Option<Timestamp>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KafkaSinkConnector {
    pub addrs: KafkaAddrs,
    pub topic: String,
    pub topic_prefix: String,
    pub key_desc_and_indices: Option<(RelationDesc, Vec<usize>)>,
    pub relation_key_indices: Option<Vec<usize>>,
    pub value_desc: RelationDesc,
    pub published_schema_info: Option<PublishedSchemaInfo>,
    pub consistency: Option<KafkaSinkConsistencyConnector>,
    pub exactly_once: bool,
    // Source dependencies for exactly-once sinks.
    pub transitive_source_dependencies: Vec<GlobalId>,
    // Maximum number of records the sink will attempt to send each time it is
    // invoked
    pub fuel: usize,
    pub config_options: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AvroOcfSinkConnector {
    pub value_desc: RelationDesc,
    pub path: PathBuf,
}

impl SinkConnector {
    /// Returns the name of the sink connector.
    pub fn name(&self) -> &'static str {
        match self {
            SinkConnector::AvroOcf(_) => "avro-ocf",
            SinkConnector::Kafka(_) => "kafka",
            SinkConnector::Tail(_) => "tail",
        }
    }

    /// Returns `true` if this sink requires sources to block timestamp binding
    /// compaction until all sinks that depend on a given source have finished
    /// writing out that timestamp.
    ///
    /// To achieve that, each sink will hold a `AntichainToken` for all of
    /// the sources it depends on, and will advance all of its source
    /// dependencies' compaction frontiers as it completes writes.
    ///
    /// Sinks that do need to hold back compaction need to insert an
    /// [`Antichain`] into `RenderState.sink_write_frontiers` that they update
    /// in order to advance the frontier that holds back upstream compaction
    /// of timestamp bindings.
    ///
    /// See also [`transitive_source_dependencies`](SinkConnector::transitive_source_dependencies).
    pub fn requires_source_compaction_holdback(&self) -> bool {
        match self {
            SinkConnector::Kafka(k) => k.exactly_once,
            SinkConnector::AvroOcf(_) => false,
            SinkConnector::Tail(_) => false,
        }
    }

    /// Returns the [`GlobalIds`](GlobalId) of the transitive sources of this
    /// sink.
    pub fn transitive_source_dependencies(&self) -> &[GlobalId] {
        match self {
            SinkConnector::Kafka(k) => &k.transitive_source_dependencies,
            SinkConnector::AvroOcf(_) => &[],
            SinkConnector::Tail(_) => &[],
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TailSinkConnector {
    pub emit_progress: bool,
    pub object_columns: usize,
    pub value_desc: RelationDesc,
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
    pub value_desc: RelationDesc,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KafkaSinkConnectorBuilder {
    pub broker_addrs: KafkaAddrs,
    pub format: KafkaSinkFormat,
    /// A natural key of the sinked relation (view or source).
    pub relation_key_indices: Option<Vec<usize>>,
    /// The user-specified key for the sink.
    pub key_desc_and_indices: Option<(RelationDesc, Vec<usize>)>,
    pub value_desc: RelationDesc,
    pub topic_prefix: String,
    pub consistency_topic_prefix: Option<String>,
    pub consistency_format: Option<KafkaSinkFormat>,
    pub topic_suffix_nonce: String,
    pub partition_count: i32,
    pub replication_factor: i32,
    pub fuel: usize,
    pub config_options: BTreeMap<String, String>,
    // Forces the sink to always write to the same topic across restarts instead
    // of picking a new topic each time.
    pub reuse_topic: bool,
    // Source dependencies for exactly-once sinks.
    pub transitive_source_dependencies: Vec<GlobalId>,
    pub retention: KafkaSinkConnectorRetention,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct KafkaSinkConnectorRetention {
    pub retention_ms: Option<i64>,
    pub retention_bytes: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum KafkaSinkFormat {
    Avro {
        schema_registry_url: Url,
        key_schema: Option<String>,
        value_schema: String,
        ccsr_config: ccsr::ClientConfig,
    },
    Json,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PublishedSchemaInfo {
    pub key_schema_id: Option<i32>,
    pub value_schema_id: i32,
}

/// An index storing processed updates so they can be queried
/// or reused in other computations
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct IndexDesc {
    /// Identity of the collection the index is on.
    pub on_id: GlobalId,
    /// Expressions to be arranged, in order of decreasing primacy.
    pub keys: Vec<MirScalarExpr>,
}

// TODO: change contract to ensure that the operator is always applied to
// streams of rows
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
    pub predicates: Vec<MirScalarExpr>,
    /// Columns not present in `projection` may be replaced with
    /// default values.
    pub projection: Vec<usize>,
}

impl LinearOperator {
    /// Reports whether this linear operator is trivial when applied to an
    /// input of the specified arity.
    pub fn is_trivial(&self, arity: usize) -> bool {
        self.predicates.is_empty() && self.projection.iter().copied().eq(0..arity)
    }
}
