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

use failure::{bail, ResultExt};
use rusoto_core::Region;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use url::Url;

use expr::{EvalEnv, GlobalId, OptimizedRelationExpr, RelationExpr, ScalarExpr, SourceInstanceId};
use interchange::avro;
use interchange::protobuf::{decode_descriptors, validate_descriptors};
use rdkafka::consumer::BaseConsumer;
use rdkafka::ClientConfig;
use regex::Regex;
use repr::{ColumnType, RelationDesc, RelationType, Row, ScalarType};
use sql_parser::ast::Value;

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
    pub eval_env: EvalEnv,
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
    pub index_exports: Vec<(GlobalId, IndexDesc, RelationType)>,
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
            .or_insert_with(|| Vec::new())
            .push(dependent_id);
    }

    pub fn add_source_import(
        &mut self,
        id: SourceInstanceId,
        connector: SourceConnector,
        desc: RelationDesc,
    ) {
        self.source_imports
            .insert(id, SourceDesc { connector, desc });
    }

    pub fn add_view_to_build(
        &mut self,
        id: GlobalId,
        expr: OptimizedRelationExpr,
        eval_env: EvalEnv,
        typ: RelationType,
    ) {
        self.objects_to_build.push(BuildDesc {
            id,
            relation_expr: expr,
            eval_env,
            typ: Some(typ),
        });
    }

    pub fn add_index_to_build(
        &mut self,
        id: GlobalId,
        on_id: GlobalId,
        on_type: RelationType,
        keys: Vec<ScalarExpr>,
        eval_env: EvalEnv,
    ) {
        self.objects_to_build.push(BuildDesc {
            id,
            relation_expr: OptimizedRelationExpr::declare_optimized(RelationExpr::ArrangeBy {
                input: Box::new(RelationExpr::global_get(on_id, on_type)),
                keys: vec![keys],
            }),
            eval_env,
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
    pub fn desc(&self, envelope: Envelope) -> Result<RelationDesc, failure::Error> {
        let desc = match self {
            DataEncoding::Bytes => RelationDesc::from_cols(vec![(
                ColumnType::new(ScalarType::Bytes),
                Some("data".to_owned()),
            )]),
            DataEncoding::AvroOcf { reader_schema } => {
                avro::validate_value_schema(&*reader_schema, envelope == Envelope::Debezium)
                    .with_context(|e| format!("validating avro ocf reader schema: {}", e))?
            }
            DataEncoding::Avro(AvroEncoding {
                value_schema,
                key_schema,
                ..
            }) => {
                let mut desc =
                    avro::validate_value_schema(value_schema, envelope == Envelope::Debezium)
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
        Ok(desc)
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
    pub desc: RelationDesc,
}

/// A sink for updates to a relational collection.
#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SinkDesc {
    pub from: (GlobalId, RelationDesc),
    pub connector: SinkConnector,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Envelope {
    None,
    Debezium,
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
    pub fn metadata_columns(&self) -> Vec<(ColumnType, Option<String>)> {
        match self {
            Self::Kafka(_) => vec![(ColumnType::new(ScalarType::Int64), Some("mz_offset".into()))],
            Self::File(_) => vec![(
                ColumnType::new(ScalarType::Int64),
                Some("mz_line_no".into()),
            )],
            Self::Kinesis(_) => vec![],
            Self::AvroOcf(_) => {
                vec![(ColumnType::new(ScalarType::Int64), Some("mz_obj_no".into()))]
            }
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
    pub auth: Option<KafkaAuth>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum KafkaAuth {
    /// Authenticate the Kafka broker using the CA file at `path`.
    SSL(PathBuf),
    /// Connect to the Kafka broker with sasl_plaintext, configuring the client with the
    /// embedded key-value pairs.
    SASLPlaintext(Vec<(String, String)>),
}

impl KafkaAuth {
    /// Return a list of key-value pairs to authenaticate `rdkafka` to connect
    /// to a Kerberized Kafka cluster.
    ///
    /// # Arguments
    ///
    /// - `with_options` should be the `with_options` field of
    ///   `sql_parser::ast::Statement::CreateSource`, where the user has passed
    ///   in their options to connect to the Kerberized Kafka cluster.
    ///
    /// # Errors
    ///
    /// - If the `rdkafka` does not have sufficient information to create a
    ///   Kafka consumer. This case covers when the user doesn't have a local
    ///   keytab cache configured and doesn't provide sufficient detail to
    ///   `rdkafka` to establish a connection.
    ///
    pub fn sasl_palintext_kerberos_settings(
        with_options: &mut std::collections::HashMap<String, Value>,
    ) -> Result<Self, failure::Error> {
        // Represents valid with_option keys to connect to Kerberized Kafka
        // cluster through SASL based on
        // https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md.
        // Currently all of these keys can be converted to their respective
        // client config settings by replacing underscores with dots.
        let allowed_configs = vec![
            "sasl_kerberos_keytab",
            "sasl_kerberos_kinit_cmd",
            "sasl_kerberos_min_time_before_relogin",
            "sasl_kerberos_principal",
            "sasl_kerberos_service_name",
            "sasl_mechanisms",
        ];

        let mut client_config: Vec<(String, String)> = vec![];
        for config in allowed_configs {
            match with_options.remove(&config.to_string()) {
                Some(Value::SingleQuotedString(v)) => {
                    client_config.push((config.replace("_", "."), v));
                }
                Some(_) => bail!("{} must be a string", config),
                None => {}
            };
        }

        // Perform a dry run to see if we have the necessary credentials to
        // connect. Concretely, this is checking to see if the local keytab
        // cache is present, and if not, notifying the user which additional
        // paramters need to be set.
        let mut config = ClientConfig::new();
        config.set("security.protocol", "sasl_plaintext");
        for s in client_config.clone() {
            config.set(s.0.as_str(), s.1.as_str());
        }

        if let Err(e) = config.create::<BaseConsumer>() {
            match e {
                rdkafka::error::KafkaError::ClientCreation(s) => {
                    println!("{}", s);
                    // Catch the one error we know about.
                    if s == "Invalid sasl.kerberos.kinit.cmd value: Property \
                    not available: \"sasl.kerberos.keytab\""
                    {
                        bail!(
                            "Invalid SASL Auth: Can't seem to find local keytab \
                            cache. You must provide explicit sasl_kerberos_keytab \
                            or sasl_kerberos_kinit_cmd option."
                        )
                    } else {
                        // Pass existing error back up.
                        bail!(rdkafka::error::KafkaError::ClientCreation(s))
                    }
                }
                _ => bail!(e),
            }
        }

        Ok(KafkaAuth::SASLPlaintext(client_config))
    }
    /// Add the appropriate settings to the `rdkafka` client's config based on
    /// the authentication method detailed when creating the Kafka source.
    pub fn configure_client(&self, config: &mut ClientConfig) {
        match self {
            KafkaAuth::SSL(path) => {
                // See https://github.com/edenhill/librdkafka/wiki/Using-SSL-with-librdkafka
                // for more details on this librdkafka option
                config.set("security.protocol", "ssl");
                config.set(
                    "ssl.ca.location",
                    path.to_str()
                        .expect("Converting ssl certificate file path failed"),
                );
            }
            KafkaAuth::SASLPlaintext(settings) => {
                config.set("security.protocol", "sasl_plaintext");
                for s in settings {
                    config.set(s.0.as_str(), s.1.as_str());
                }
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KinesisSourceConnector {
    pub stream_name: String,
    pub region: Region,
    pub access_key: String,
    pub secret_access_key: String,
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
