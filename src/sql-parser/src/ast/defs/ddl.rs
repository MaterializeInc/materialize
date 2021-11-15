// Copyright 2018 sqlparser-rs contributors. All rights reserved.
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// This file is derived from the sqlparser-rs project, available at
// https://github.com/andygrove/sqlparser-rs. It was incorporated
// directly into Materialize on December 21, 2019.
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

//! AST types specific to CREATE/ALTER variants of [crate::ast::Statement]
//! (commonly referred to as Data Definition Language, or DDL)

use std::fmt;
use std::path::PathBuf;

use enum_kinds::EnumKind;

use crate::ast::display::{self, AstDisplay, AstFormatter};
use crate::ast::{AstInfo, DataType, Expr, Ident, SqlOption, UnresolvedObjectName, WithOption};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Schema {
    File(PathBuf),
    Inline(String),
}

impl AstDisplay for Schema {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            Self::File(path) => {
                f.write_str("SCHEMA FILE '");
                f.write_node(&display::escape_single_quote_string(
                    &path.display().to_string(),
                ));
                f.write_str("'");
            }
            Self::Inline(inner) => {
                f.write_str("SCHEMA '");
                f.write_node(&display::escape_single_quote_string(inner));
                f.write_str("'");
            }
        }
    }
}
impl_display!(Schema);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AvroSchema<T: AstInfo> {
    Csr {
        csr_connector: CsrConnectorAvro<T>,
    },
    InlineSchema {
        schema: Schema,
        with_options: Vec<WithOption>,
    },
}

impl<T: AstInfo> AstDisplay for AvroSchema<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            Self::Csr { csr_connector } => {
                f.write_node(csr_connector);
            }
            Self::InlineSchema {
                schema,
                with_options,
            } => {
                f.write_str("USING ");
                schema.fmt(f);
                if !with_options.is_empty() {
                    f.write_str(" WITH (");
                    f.write_node(&display::comma_separated(with_options));
                    f.write_str(")");
                }
            }
        }
    }
}
impl_display_t!(AvroSchema);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ProtobufSchema<T: AstInfo> {
    Csr {
        csr_connector: CsrConnectorProto<T>,
    },
    InlineSchema {
        message_name: String,
        schema: Schema,
    },
}

impl<T: AstInfo> AstDisplay for ProtobufSchema<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            Self::Csr { csr_connector } => {
                f.write_node(csr_connector);
            }
            Self::InlineSchema {
                message_name,
                schema,
            } => {
                f.write_str("MESSAGE '");
                f.write_node(&display::escape_single_quote_string(message_name));
                f.write_str("' USING ");
                f.write_str(schema);
            }
        }
    }
}
impl_display_t!(ProtobufSchema);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CsrConnectorAvro<T: AstInfo> {
    pub url: String,
    pub seed: Option<CsrSeed>,
    pub with_options: Vec<SqlOption<T>>,
}

impl<T: AstInfo> AstDisplay for CsrConnectorAvro<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("USING CONFLUENT SCHEMA REGISTRY '");
        f.write_node(&display::escape_single_quote_string(&self.url));
        f.write_str("'");
        if let Some(seed) = &self.seed {
            f.write_str(" ");
            f.write_node(seed);
        }
        if !self.with_options.is_empty() {
            f.write_str(" WITH (");
            f.write_node(&display::comma_separated(&self.with_options));
            f.write_str(")");
        }
    }
}
impl_display_t!(CsrConnectorAvro);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CsrConnectorProto<T: AstInfo> {
    pub url: String,
    pub seed: Option<CsrSeedCompiled>,
    pub with_options: Vec<SqlOption<T>>,
}

impl<T: AstInfo> AstDisplay for CsrConnectorProto<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("USING CONFLUENT SCHEMA REGISTRY '");
        f.write_node(&display::escape_single_quote_string(&self.url));
        f.write_str("'");
        if let Some(seed) = &self.seed {
            f.write_str(" ");
            f.write_node(seed);
        }
        if !self.with_options.is_empty() {
            f.write_str(" WITH (");
            f.write_node(&display::comma_separated(&self.with_options));
            f.write_str(")");
        }
    }
}
impl_display_t!(CsrConnectorProto);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CsrSeed {
    pub key_schema: Option<String>,
    pub value_schema: String,
}

impl AstDisplay for CsrSeed {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("SEED");
        if let Some(key_schema) = &self.key_schema {
            f.write_str(" KEY SCHEMA '");
            f.write_node(&display::escape_single_quote_string(key_schema));
            f.write_str("'");
        }
        f.write_str(" VALUE SCHEMA '");
        f.write_node(&display::escape_single_quote_string(&self.value_schema));
        f.write_str("'");
    }
}
impl_display!(CsrSeed);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CsrSeedCompiled {
    pub key: Option<CsrSeedCompiledEncoding>,
    pub value: CsrSeedCompiledEncoding,
}
impl AstDisplay for CsrSeedCompiled {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("SEED");
        if let Some(key) = &self.key {
            f.write_str(" KEY ");
            f.write_node(key);
        }
        f.write_str(" VALUE ");
        f.write_node(&self.value);
    }
}
impl_display!(CsrSeedCompiled);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CsrSeedCompiledEncoding {
    pub schema: Vec<u8>,
    pub message_name: String,
}
impl AstDisplay for CsrSeedCompiledEncoding {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str(" SCHEMA '");
        f.write_str(&self.message_name);
        f.write_str("' ");
        f.write_node(&display::comma_separated(&self.schema));
        f.write_str(" ");
    }
}
impl_display!(CsrSeedCompiledEncoding);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CreateSourceFormat<T: AstInfo> {
    None,
    /// `CREATE SOURCE .. FORMAT`
    Bare(Format<T>),
    /// `CREATE SOURCE .. KEY FORMAT .. VALUE FORMAT`
    ///
    /// Also the destination for the legacy `ENVELOPE UPSERT FORMAT ...`
    KeyValue {
        key: Format<T>,
        value: Format<T>,
    },
}

impl<T: AstInfo> AstDisplay for CreateSourceFormat<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            CreateSourceFormat::None => {}
            CreateSourceFormat::Bare(format) => {
                f.write_str(" FORMAT ");
                f.write_node(format)
            }
            CreateSourceFormat::KeyValue { key, value } => {
                f.write_str(" KEY FORMAT ");
                f.write_node(key);
                f.write_str(" VALUE FORMAT ");
                f.write_node(value);
            }
        }
    }
}
impl_display_t!(CreateSourceFormat);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Format<T: AstInfo> {
    Bytes,
    Avro(AvroSchema<T>),
    Protobuf(ProtobufSchema<T>),
    Regex(String),
    Csv {
        columns: CsvColumns,
        delimiter: char,
    },
    Json,
    Text,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CsvColumns {
    /// `WITH count COLUMNS`
    Count(usize),
    /// `WITH HEADER (ident, ...)?`: `names` is empty if there are no names specified
    Header { names: Vec<Ident> },
}

impl AstDisplay for CsvColumns {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            CsvColumns::Count(n) => {
                f.write_str(n);
                f.write_str(" COLUMNS")
            }
            CsvColumns::Header { names } => {
                f.write_str("HEADER");
                if !names.is_empty() {
                    f.write_str(" (");
                    f.write_node(&display::comma_separated(&names));
                    f.write_str(")");
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SourceIncludeMetadataType {
    Key,
    Timestamp,
    Partition,
    Topic,
    Offset,
}

impl AstDisplay for SourceIncludeMetadataType {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            SourceIncludeMetadataType::Key => f.write_str("KEY"),
            SourceIncludeMetadataType::Timestamp => f.write_str("TIMESTAMP"),
            SourceIncludeMetadataType::Partition => f.write_str("PARTITION"),
            SourceIncludeMetadataType::Topic => f.write_str("TOPIC"),
            SourceIncludeMetadataType::Offset => f.write_str("OFFSET"),
        }
    }
}
impl_display!(SourceIncludeMetadataType);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SourceIncludeMetadata {
    pub ty: SourceIncludeMetadataType,
    pub alias: Option<Ident>,
}

impl AstDisplay for SourceIncludeMetadata {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_node(&self.ty);
        if let Some(alias) = &self.alias {
            f.write_str(" AS ");
            f.write_node(alias);
        }
    }
}
impl_display!(SourceIncludeMetadata);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Envelope {
    None,
    Debezium(DbzMode),
    Upsert,
    CdcV2,
}

impl AstDisplay for Envelope {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            Self::None => {
                // this is unreachable as long as the default is None, but include it in case we ever change that
                f.write_str("NONE");
            }
            Self::Debezium(mode) => {
                f.write_str("DEBEZIUM");
                f.write_node(mode);
            }
            Self::Upsert => {
                f.write_str("UPSERT");
            }
            Self::CdcV2 => {
                f.write_str("MATERIALIZE");
            }
        }
    }
}
impl_display!(Envelope);

impl<T: AstInfo> AstDisplay for Format<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            Self::Bytes => f.write_str("BYTES"),
            Self::Avro(inner) => {
                f.write_str("AVRO ");
                f.write_node(inner);
            }
            Self::Protobuf(inner) => {
                f.write_str("PROTOBUF ");
                f.write_node(inner);
            }
            Self::Regex(regex) => {
                f.write_str("REGEX '");
                f.write_node(&display::escape_single_quote_string(regex));
                f.write_str("'");
            }
            Self::Csv { columns, delimiter } => {
                f.write_str("CSV WITH ");
                f.write_node(columns);

                if *delimiter != ',' {
                    f.write_str(" DELIMITED BY '");
                    f.write_node(&display::escape_single_quote_string(&delimiter.to_string()));
                    f.write_str("'");
                }
            }
            Self::Json => f.write_str("JSON"),
            Self::Text => f.write_str("TEXT"),
        }
    }
}
impl_display_t!(Format);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Compression {
    Gzip,
    None,
}

impl AstDisplay for Compression {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            Self::Gzip => f.write_str("GZIP"),
            Self::None => f.write_str("NONE"),
        }
    }
}
impl_display!(Compression);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DbzMode {
    /// `ENVELOPE DEBEZIUM` with no suffix
    Plain,
    /// `ENVELOPE DEBEZIUM UPSERT`
    Upsert,
}

impl AstDisplay for DbzMode {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            Self::Plain => f.write_str(""),
            Self::Upsert => f.write_str(" UPSERT"),
        }
    }
}
impl_display!(DbzMode);

#[derive(Debug, Clone, PartialEq, Eq, Hash, EnumKind)]
#[enum_kind(ConnectorType)]
pub enum CreateSourceConnector {
    File {
        path: String,
        compression: Compression,
    },
    Kafka {
        broker: String,
        topic: String,
        key: Option<Vec<Ident>>,
    },
    Kinesis {
        arn: String,
    },
    /// Avro Object Container File
    AvroOcf {
        path: String,
    },
    S3 {
        /// The arguments to `DISCOVER OBJECTS USING`: `BUCKET SCAN` or `SQS NOTIFICATIONS`
        key_sources: Vec<S3KeySource>,
        /// The argument to the MATCHING clause: `MATCHING 'a/**/*.json'`
        pattern: Option<String>,
        compression: Compression,
    },
    Postgres {
        /// The postgres connection string
        conn: String,
        /// The name of the publication to sync
        publication: String,
        /// The replication slot name that will be created upstream
        slot: Option<String>,
    },
    PubNub {
        /// PubNub's subscribe key
        subscribe_key: String,
        /// The PubNub channel to subscribe to
        channel: String,
    },
}

impl AstDisplay for CreateSourceConnector {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            CreateSourceConnector::File { path, compression } => {
                f.write_str("FILE '");
                f.write_node(&display::escape_single_quote_string(path));
                f.write_str("'");
                f.write_str(" COMPRESSION ");
                f.write_node(compression);
            }
            CreateSourceConnector::Kafka { broker, topic, key } => {
                f.write_str("KAFKA BROKER '");
                f.write_node(&display::escape_single_quote_string(broker));
                f.write_str("'");
                f.write_str(" TOPIC '");
                f.write_node(&display::escape_single_quote_string(topic));
                f.write_str("'");
                if let Some(key) = key.as_ref() {
                    f.write_str(" KEY (");
                    f.write_node(&display::comma_separated(&key));
                    f.write_str(")");
                }
            }
            CreateSourceConnector::Kinesis { arn } => {
                f.write_str("KINESIS ARN '");
                f.write_node(&display::escape_single_quote_string(arn));
                f.write_str("'");
            }
            CreateSourceConnector::AvroOcf { path } => {
                f.write_str("AVRO OCF '");
                f.write_node(&display::escape_single_quote_string(path));
                f.write_str("'");
            }
            CreateSourceConnector::S3 {
                key_sources,
                pattern,
                compression,
            } => {
                f.write_str("S3 DISCOVER OBJECTS");
                if let Some(pattern) = pattern {
                    f.write_str(" MATCHING '");
                    f.write_str(&display::escape_single_quote_string(pattern));
                    f.write_str("'");
                }
                f.write_str(" USING");
                f.write_node(&display::comma_separated(key_sources));
                f.write_str(" COMPRESSION ");
                f.write_node(compression);
            }
            CreateSourceConnector::Postgres {
                conn,
                publication,
                slot,
            } => {
                f.write_str("POSTGRES CONNECTION '");
                f.write_str(&display::escape_single_quote_string(conn));
                f.write_str("' PUBLICATION '");
                f.write_str(&display::escape_single_quote_string(publication));
                if let Some(slot) = slot {
                    f.write_str("' SLOT '");
                    f.write_str(&display::escape_single_quote_string(slot));
                }
                f.write_str("'");
            }
            CreateSourceConnector::PubNub {
                subscribe_key,
                channel,
            } => {
                f.write_str("PUBNUB SUBSCRIBE KEY '");
                f.write_str(&display::escape_single_quote_string(subscribe_key));
                f.write_str("' CHANNEL '");
                f.write_str(&display::escape_single_quote_string(channel));
                f.write_str("'");
            }
        }
    }
}
impl_display!(CreateSourceConnector);

impl<T: AstInfo> From<&CreateSinkConnector<T>> for ConnectorType {
    fn from(connector: &CreateSinkConnector<T>) -> ConnectorType {
        match connector {
            CreateSinkConnector::Kafka { .. } => ConnectorType::Kafka,
            CreateSinkConnector::AvroOcf { .. } => ConnectorType::AvroOcf,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EnumKind)]
#[enum_kind(CreateSinkConnectorKind)]
pub enum CreateSinkConnector<T: AstInfo> {
    Kafka {
        broker: String,
        topic: String,
        key: Option<Vec<Ident>>,
        consistency: Option<KafkaConsistency<T>>,
    },
    /// Avro Object Container File
    AvroOcf { path: String },
}

impl<T: AstInfo> AstDisplay for CreateSinkConnector<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            CreateSinkConnector::Kafka {
                broker,
                topic,
                key,
                consistency,
            } => {
                f.write_str("KAFKA BROKER '");
                f.write_node(&display::escape_single_quote_string(broker));
                f.write_str("'");
                f.write_str(" TOPIC '");
                f.write_node(&display::escape_single_quote_string(topic));
                f.write_str("'");
                if let Some(key) = key.as_ref() {
                    f.write_str(" KEY (");
                    f.write_node(&display::comma_separated(&key));
                    f.write_str(")");
                }
                if let Some(consistency) = consistency.as_ref() {
                    f.write_node(consistency);
                }
            }
            CreateSinkConnector::AvroOcf { path } => {
                f.write_str("AVRO OCF '");
                f.write_node(&display::escape_single_quote_string(path));
                f.write_str("'");
            }
        }
    }
}
impl_display_t!(CreateSinkConnector);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KafkaConsistency<T: AstInfo> {
    pub topic: String,
    pub topic_format: Option<Format<T>>,
}

impl<T: AstInfo> AstDisplay for KafkaConsistency<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str(" CONSISTENCY (TOPIC '");
        f.write_node(&display::escape_single_quote_string(&self.topic));
        f.write_str("'");

        if let Some(format) = self.topic_format.as_ref() {
            f.write_str(" FORMAT ");
            f.write_node(format);
        }
        f.write_str(")");
    }
}
impl_display_t!(KafkaConsistency);

/// Information about upstream Postgres tables used for replication sources
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PgTable<T: AstInfo> {
    /// The name of the table to sync
    pub name: UnresolvedObjectName,
    /// The name for the table in Materialize
    pub alias: T::ObjectName,
    /// The expected column schema of the synced table
    pub columns: Vec<ColumnDef<T>>,
}

impl<T: AstInfo> AstDisplay for PgTable<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_node(&self.name);
        f.write_str(" AS ");
        f.write_str(self.alias.to_ast_string());
        if !self.columns.is_empty() {
            f.write_str(" (");
            f.write_node(&display::comma_separated(&self.columns));
            f.write_str(")");
        }
    }
}
impl_display_t!(PgTable);

/// The key sources specified in the S3 source's `DISCOVER OBJECTS` clause.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum S3KeySource {
    /// `SCAN BUCKET '<bucket>'`
    Scan { bucket: String },
    /// `SQS NOTIFICATIONS '<queue-name>'`
    SqsNotifications { queue: String },
}

impl AstDisplay for S3KeySource {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            S3KeySource::Scan { bucket } => {
                f.write_str(" BUCKET SCAN '");
                f.write_str(&display::escape_single_quote_string(bucket));
                f.write_str("'");
            }
            S3KeySource::SqsNotifications { queue } => {
                f.write_str(" SQS NOTIFICATIONS '");
                f.write_str(&display::escape_single_quote_string(queue));
                f.write_str("'");
            }
        }
    }
}
impl_display!(S3KeySource);

/// A table-level constraint, specified in a `CREATE TABLE` or an
/// `ALTER TABLE ADD <constraint>` statement.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TableConstraint<T: AstInfo> {
    /// `[ CONSTRAINT <name> ] { PRIMARY KEY | UNIQUE } (<columns>)`
    Unique {
        name: Option<Ident>,
        columns: Vec<Ident>,
        /// Whether this is a `PRIMARY KEY` or just a `UNIQUE` constraint
        is_primary: bool,
    },
    /// A referential integrity constraint (`[ CONSTRAINT <name> ] FOREIGN KEY (<columns>)
    /// REFERENCES <foreign_table> (<referred_columns>)`)
    ForeignKey {
        name: Option<Ident>,
        columns: Vec<Ident>,
        foreign_table: UnresolvedObjectName,
        referred_columns: Vec<Ident>,
    },
    /// `[ CONSTRAINT <name> ] CHECK (<expr>)`
    Check {
        name: Option<Ident>,
        expr: Box<Expr<T>>,
    },
}

impl<T: AstInfo> AstDisplay for TableConstraint<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            TableConstraint::Unique {
                name,
                columns,
                is_primary,
            } => {
                f.write_node(&display_constraint_name(name));
                if *is_primary {
                    f.write_str("PRIMARY KEY ");
                } else {
                    f.write_str("UNIQUE ");
                }
                f.write_str("(");
                f.write_node(&display::comma_separated(columns));
                f.write_str(")");
            }
            TableConstraint::ForeignKey {
                name,
                columns,
                foreign_table,
                referred_columns,
            } => {
                f.write_node(&display_constraint_name(name));
                f.write_str("FOREIGN KEY (");
                f.write_node(&display::comma_separated(columns));
                f.write_str(") REFERENCES ");
                f.write_node(foreign_table);
                f.write_str("(");
                f.write_node(&display::comma_separated(referred_columns));
                f.write_str(")");
            }
            TableConstraint::Check { name, expr } => {
                f.write_node(&display_constraint_name(name));
                f.write_str("CHECK (");
                f.write_node(&expr);
                f.write_str(")");
            }
        }
    }
}
impl_display_t!(TableConstraint);

/// A key constraint, specified in a `CREATE SOURCE`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum KeyConstraint {
    // PRIMARY KEY (<columns>) NOT ENFORCED
    PrimaryKeyNotEnforced { columns: Vec<Ident> },
}

impl AstDisplay for KeyConstraint {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            KeyConstraint::PrimaryKeyNotEnforced { columns } => {
                f.write_str("PRIMARY KEY ");
                f.write_str("(");
                f.write_node(&display::comma_separated(columns));
                f.write_str(") ");
                f.write_str("NOT ENFORCED");
            }
        }
    }
}
impl_display!(KeyConstraint);

/// SQL column definition
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnDef<T: AstInfo> {
    pub name: Ident,
    pub data_type: DataType<T>,
    pub collation: Option<UnresolvedObjectName>,
    pub options: Vec<ColumnOptionDef<T>>,
}

impl<T: AstInfo> AstDisplay for ColumnDef<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_node(&self.name);
        f.write_str(" ");
        f.write_node(&self.data_type);
        for option in &self.options {
            f.write_str(" ");
            f.write_node(option);
        }
    }
}
impl_display_t!(ColumnDef);

/// An optionally-named `ColumnOption`: `[ CONSTRAINT <name> ] <column-option>`.
///
/// Note that implementations are substantially more permissive than the ANSI
/// specification on what order column options can be presented in, and whether
/// they are allowed to be named. The specification distinguishes between
/// constraints (NOT NULL, UNIQUE, PRIMARY KEY, and CHECK), which can be named
/// and can appear in any order, and other options (DEFAULT, GENERATED), which
/// cannot be named and must appear in a fixed order. PostgreSQL, however,
/// allows preceding any option with `CONSTRAINT <name>`, even those that are
/// not really constraints, like NULL and DEFAULT. MSSQL is less permissive,
/// allowing DEFAULT, UNIQUE, PRIMARY KEY and CHECK to be named, but not NULL or
/// NOT NULL constraints (the last of which is in violation of the spec).
///
/// For maximum flexibility, we don't distinguish between constraint and
/// non-constraint options, lumping them all together under the umbrella of
/// "column options," and we allow any column option to be named.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnOptionDef<T: AstInfo> {
    pub name: Option<Ident>,
    pub option: ColumnOption<T>,
}

impl<T: AstInfo> AstDisplay for ColumnOptionDef<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_node(&display_constraint_name(&self.name));
        f.write_node(&self.option);
    }
}
impl_display_t!(ColumnOptionDef);

/// `ColumnOption`s are modifiers that follow a column definition in a `CREATE
/// TABLE` statement.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ColumnOption<T: AstInfo> {
    /// `NULL`
    Null,
    /// `NOT NULL`
    NotNull,
    /// `DEFAULT <restricted-expr>`
    Default(Expr<T>),
    /// `{ PRIMARY KEY | UNIQUE }`
    Unique {
        is_primary: bool,
    },
    /// A referential integrity constraint (`[FOREIGN KEY REFERENCES
    /// <foreign_table> (<referred_columns>)`).
    ForeignKey {
        foreign_table: UnresolvedObjectName,
        referred_columns: Vec<Ident>,
    },
    // `CHECK (<expr>)`
    Check(Expr<T>),
}

impl<T: AstInfo> AstDisplay for ColumnOption<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        use ColumnOption::*;
        match self {
            Null => f.write_str("NULL"),
            NotNull => f.write_str("NOT NULL"),
            Default(expr) => {
                f.write_str("DEFAULT ");
                f.write_node(expr);
            }
            Unique { is_primary } => {
                if *is_primary {
                    f.write_str("PRIMARY KEY");
                } else {
                    f.write_str("UNIQUE");
                }
            }
            ForeignKey {
                foreign_table,
                referred_columns,
            } => {
                f.write_str("REFERENCES ");
                f.write_node(foreign_table);
                f.write_str(" (");
                f.write_node(&display::comma_separated(referred_columns));
                f.write_str(")");
            }
            Check(expr) => {
                f.write_str("CHECK (");
                f.write_node(expr);
                f.write_str(")");
            }
        }
    }
}
impl_display_t!(ColumnOption);

fn display_constraint_name<'a>(name: &'a Option<Ident>) -> impl AstDisplay + 'a {
    struct ConstraintName<'a>(&'a Option<Ident>);
    impl<'a> AstDisplay for ConstraintName<'a> {
        fn fmt<W>(&self, f: &mut AstFormatter<W>)
        where
            W: fmt::Write,
        {
            if let Some(name) = self.0 {
                f.write_str("CONSTRAINT ");
                f.write_node(name);
                f.write_str(" ");
            }
        }
    }
    ConstraintName(name)
}
