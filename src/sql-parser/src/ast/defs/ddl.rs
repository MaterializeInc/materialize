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

use crate::ast::display::{self, AstDisplay, AstFormatter, WithOptionName};
use crate::ast::{
    AstInfo, ColumnName, Expr, Ident, OrderByExpr, UnresolvedItemName, Version, WithOptionValue,
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MaterializedViewOptionName {
    /// The `ASSERT NOT NULL [=] <ident>` option.
    AssertNotNull,
    PartitionBy,
    RetainHistory,
    /// The `REFRESH [=] ...` option.
    Refresh,
}

impl AstDisplay for MaterializedViewOptionName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            MaterializedViewOptionName::AssertNotNull => f.write_str("ASSERT NOT NULL"),
            MaterializedViewOptionName::PartitionBy => f.write_str("PARTITION BY"),
            MaterializedViewOptionName::RetainHistory => f.write_str("RETAIN HISTORY"),
            MaterializedViewOptionName::Refresh => f.write_str("REFRESH"),
        }
    }
}

impl WithOptionName for MaterializedViewOptionName {
    /// # WARNING
    ///
    /// Whenever implementing this trait consider very carefully whether or not
    /// this value could contain sensitive user data. If you're uncertain, err
    /// on the conservative side and return `true`.
    fn redact_value(&self) -> bool {
        match self {
            MaterializedViewOptionName::AssertNotNull
            | MaterializedViewOptionName::PartitionBy
            | MaterializedViewOptionName::RetainHistory
            | MaterializedViewOptionName::Refresh => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MaterializedViewOption<T: AstInfo> {
    pub name: MaterializedViewOptionName,
    pub value: Option<WithOptionValue<T>>,
}
impl_display_for_with_option!(MaterializedViewOption);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ContinualTaskOptionName {
    /// The `SNAPSHOT [=] ...` option.
    Snapshot,
}

impl AstDisplay for ContinualTaskOptionName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            ContinualTaskOptionName::Snapshot => f.write_str("SNAPSHOT"),
        }
    }
}

impl WithOptionName for ContinualTaskOptionName {
    /// # WARNING
    ///
    /// Whenever implementing this trait consider very carefully whether or not
    /// this value could contain sensitive user data. If you're uncertain, err
    /// on the conservative side and return `true`.
    fn redact_value(&self) -> bool {
        match self {
            ContinualTaskOptionName::Snapshot => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContinualTaskOption<T: AstInfo> {
    pub name: ContinualTaskOptionName,
    pub value: Option<WithOptionValue<T>>,
}
impl_display_for_with_option!(ContinualTaskOption);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Schema {
    pub schema: String,
}

impl AstDisplay for Schema {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("SCHEMA '");
        f.write_node(&display::escape_single_quote_string(&self.schema));
        f.write_str("'");
    }
}
impl_display!(Schema);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AvroSchemaOptionName {
    /// The `CONFLUENT WIRE FORMAT [=] <bool>` option.
    ConfluentWireFormat,
}

impl AstDisplay for AvroSchemaOptionName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            AvroSchemaOptionName::ConfluentWireFormat => f.write_str("CONFLUENT WIRE FORMAT"),
        }
    }
}

impl WithOptionName for AvroSchemaOptionName {
    /// # WARNING
    ///
    /// Whenever implementing this trait consider very carefully whether or not
    /// this value could contain sensitive user data. If you're uncertain, err
    /// on the conservative side and return `true`.
    fn redact_value(&self) -> bool {
        match self {
            Self::ConfluentWireFormat => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AvroSchemaOption<T: AstInfo> {
    pub name: AvroSchemaOptionName,
    pub value: Option<WithOptionValue<T>>,
}
impl_display_for_with_option!(AvroSchemaOption);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AvroSchema<T: AstInfo> {
    Csr {
        csr_connection: CsrConnectionAvro<T>,
    },
    InlineSchema {
        schema: Schema,
        with_options: Vec<AvroSchemaOption<T>>,
    },
}

impl<T: AstInfo> AstDisplay for AvroSchema<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            Self::Csr { csr_connection } => {
                f.write_node(csr_connection);
            }
            Self::InlineSchema {
                schema,
                with_options,
            } => {
                f.write_str("USING ");
                schema.fmt(f);
                if !with_options.is_empty() {
                    f.write_str(" (");
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
        csr_connection: CsrConnectionProtobuf<T>,
    },
    InlineSchema {
        message_name: String,
        schema: Schema,
    },
}

impl<T: AstInfo> AstDisplay for ProtobufSchema<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            Self::Csr { csr_connection } => {
                f.write_node(csr_connection);
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CsrConfigOptionName<T: AstInfo> {
    AvroKeyFullname,
    AvroValueFullname,
    NullDefaults,
    AvroDocOn(AvroDocOn<T>),
    KeyCompatibilityLevel,
    ValueCompatibilityLevel,
}

impl<T: AstInfo> WithOptionName for CsrConfigOptionName<T> {
    /// # WARNING
    ///
    /// Whenever implementing this trait consider very carefully whether or not
    /// this value could contain sensitive user data. If you're uncertain, err
    /// on the conservative side and return `true`.
    fn redact_value(&self) -> bool {
        match self {
            Self::AvroKeyFullname
            | Self::AvroValueFullname
            | Self::NullDefaults
            | Self::AvroDocOn(_)
            | Self::KeyCompatibilityLevel
            | Self::ValueCompatibilityLevel => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AvroDocOn<T: AstInfo> {
    pub identifier: DocOnIdentifier<T>,
    pub for_schema: DocOnSchema,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DocOnSchema {
    KeyOnly,
    ValueOnly,
    All,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DocOnIdentifier<T: AstInfo> {
    Column(ColumnName<T>),
    Type(T::ItemName),
}

impl<T: AstInfo> AstDisplay for AvroDocOn<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match &self.for_schema {
            DocOnSchema::KeyOnly => f.write_str("KEY "),
            DocOnSchema::ValueOnly => f.write_str("VALUE "),
            DocOnSchema::All => {}
        }
        match &self.identifier {
            DocOnIdentifier::Column(name) => {
                f.write_str("DOC ON COLUMN ");
                f.write_node(name);
            }
            DocOnIdentifier::Type(name) => {
                f.write_str("DOC ON TYPE ");
                f.write_node(name);
            }
        }
    }
}
impl_display_t!(AvroDocOn);

impl<T: AstInfo> AstDisplay for CsrConfigOptionName<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            CsrConfigOptionName::AvroKeyFullname => f.write_str("AVRO KEY FULLNAME"),
            CsrConfigOptionName::AvroValueFullname => f.write_str("AVRO VALUE FULLNAME"),
            CsrConfigOptionName::NullDefaults => f.write_str("NULL DEFAULTS"),
            CsrConfigOptionName::AvroDocOn(doc_on) => f.write_node(doc_on),
            CsrConfigOptionName::KeyCompatibilityLevel => f.write_str("KEY COMPATIBILITY LEVEL"),
            CsrConfigOptionName::ValueCompatibilityLevel => {
                f.write_str("VALUE COMPATIBILITY LEVEL")
            }
        }
    }
}
impl_display_t!(CsrConfigOptionName);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
/// An option in a `{FROM|INTO} CONNECTION ...` statement.
pub struct CsrConfigOption<T: AstInfo> {
    pub name: CsrConfigOptionName<T>,
    pub value: Option<WithOptionValue<T>>,
}
impl_display_for_with_option!(CsrConfigOption);
impl_display_t!(CsrConfigOption);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CsrConnection<T: AstInfo> {
    pub connection: T::ItemName,
    pub options: Vec<CsrConfigOption<T>>,
}

impl<T: AstInfo> AstDisplay for CsrConnection<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("CONNECTION ");
        f.write_node(&self.connection);
        if !self.options.is_empty() {
            f.write_str(" (");
            f.write_node(&display::comma_separated(&self.options));
            f.write_str(")");
        }
    }
}
impl_display_t!(CsrConnection);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ReaderSchemaSelectionStrategy {
    Latest,
    Inline(String),
    ById(i32),
}

impl Default for ReaderSchemaSelectionStrategy {
    fn default() -> Self {
        Self::Latest
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CsrConnectionAvro<T: AstInfo> {
    pub connection: CsrConnection<T>,
    pub key_strategy: Option<ReaderSchemaSelectionStrategy>,
    pub value_strategy: Option<ReaderSchemaSelectionStrategy>,
    pub seed: Option<CsrSeedAvro>,
}

impl<T: AstInfo> AstDisplay for CsrConnectionAvro<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("USING CONFLUENT SCHEMA REGISTRY ");
        f.write_node(&self.connection);
        if let Some(seed) = &self.seed {
            f.write_str(" ");
            f.write_node(seed);
        }
    }
}
impl_display_t!(CsrConnectionAvro);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CsrConnectionProtobuf<T: AstInfo> {
    pub connection: CsrConnection<T>,
    pub seed: Option<CsrSeedProtobuf>,
}

impl<T: AstInfo> AstDisplay for CsrConnectionProtobuf<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("USING CONFLUENT SCHEMA REGISTRY ");
        f.write_node(&self.connection);

        if let Some(seed) = &self.seed {
            f.write_str(" ");
            f.write_node(seed);
        }
    }
}
impl_display_t!(CsrConnectionProtobuf);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CsrSeedAvro {
    pub key_schema: Option<String>,
    pub value_schema: String,
}

impl AstDisplay for CsrSeedAvro {
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
impl_display!(CsrSeedAvro);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CsrSeedProtobuf {
    pub key: Option<CsrSeedProtobufSchema>,
    pub value: CsrSeedProtobufSchema,
}

impl AstDisplay for CsrSeedProtobuf {
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
impl_display!(CsrSeedProtobuf);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CsrSeedProtobufSchema {
    // Hex encoded string.
    pub schema: String,
    pub message_name: String,
}
impl AstDisplay for CsrSeedProtobufSchema {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("SCHEMA '");
        f.write_str(&display::escape_single_quote_string(&self.schema));
        f.write_str("' MESSAGE '");
        f.write_str(&self.message_name);
        f.write_str("'");
    }
}
impl_display!(CsrSeedProtobufSchema);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FormatSpecifier<T: AstInfo> {
    /// `CREATE SOURCE/SINK .. FORMAT`
    Bare(Format<T>),
    /// `CREATE SOURCE/SINK .. KEY FORMAT .. VALUE FORMAT`
    KeyValue { key: Format<T>, value: Format<T> },
}

impl<T: AstInfo> AstDisplay for FormatSpecifier<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            FormatSpecifier::Bare(format) => {
                f.write_str("FORMAT ");
                f.write_node(format)
            }
            FormatSpecifier::KeyValue { key, value } => {
                f.write_str("KEY FORMAT ");
                f.write_node(key);
                f.write_str(" VALUE FORMAT ");
                f.write_node(value);
            }
        }
    }
}
impl_display_t!(FormatSpecifier);

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
    Json {
        array: bool,
    },
    Text,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CsvColumns {
    /// `WITH count COLUMNS`
    Count(u64),
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
                    f.write_node(&display::comma_separated(names));
                    f.write_str(")");
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SourceIncludeMetadata {
    Key {
        alias: Option<Ident>,
    },
    Timestamp {
        alias: Option<Ident>,
    },
    Partition {
        alias: Option<Ident>,
    },
    Offset {
        alias: Option<Ident>,
    },
    Headers {
        alias: Option<Ident>,
    },
    Header {
        key: String,
        alias: Ident,
        use_bytes: bool,
    },
}

impl AstDisplay for SourceIncludeMetadata {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        let print_alias = |f: &mut AstFormatter<W>, alias: &Option<Ident>| {
            if let Some(alias) = alias {
                f.write_str(" AS ");
                f.write_node(alias);
            }
        };

        match self {
            SourceIncludeMetadata::Key { alias } => {
                f.write_str("KEY");
                print_alias(f, alias);
            }
            SourceIncludeMetadata::Timestamp { alias } => {
                f.write_str("TIMESTAMP");
                print_alias(f, alias);
            }
            SourceIncludeMetadata::Partition { alias } => {
                f.write_str("PARTITION");
                print_alias(f, alias);
            }
            SourceIncludeMetadata::Offset { alias } => {
                f.write_str("OFFSET");
                print_alias(f, alias);
            }
            SourceIncludeMetadata::Headers { alias } => {
                f.write_str("HEADERS");
                print_alias(f, alias);
            }
            SourceIncludeMetadata::Header {
                alias,
                key,
                use_bytes,
            } => {
                f.write_str("HEADER '");
                f.write_str(&display::escape_single_quote_string(key));
                f.write_str("'");
                print_alias(f, &Some(alias.clone()));
                if *use_bytes {
                    f.write_str(" BYTES");
                }
            }
        }
    }
}
impl_display!(SourceIncludeMetadata);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SourceErrorPolicy {
    Inline {
        /// The alias to use for the error column. If unspecified will be `error`.
        alias: Option<Ident>,
    },
}

impl AstDisplay for SourceErrorPolicy {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            Self::Inline { alias } => {
                f.write_str("INLINE");
                if let Some(alias) = alias {
                    f.write_str(" AS ");
                    f.write_node(alias);
                }
            }
        }
    }
}
impl_display!(SourceErrorPolicy);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SourceEnvelope {
    None,
    Debezium,
    Upsert {
        value_decode_err_policy: Vec<SourceErrorPolicy>,
    },
    CdcV2,
}

impl SourceEnvelope {
    /// `true` iff Materialize is expected to crash or exhibit UB
    /// when attempting to ingest data starting at an offset other than zero.
    pub fn requires_all_input(&self) -> bool {
        match self {
            SourceEnvelope::None => false,
            SourceEnvelope::Debezium => false,
            SourceEnvelope::Upsert { .. } => false,
            SourceEnvelope::CdcV2 => true,
        }
    }
}

impl AstDisplay for SourceEnvelope {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            Self::None => {
                // this is unreachable as long as the default is None, but include it in case we ever change that
                f.write_str("NONE");
            }
            Self::Debezium => {
                f.write_str("DEBEZIUM");
            }
            Self::Upsert {
                value_decode_err_policy,
            } => {
                if value_decode_err_policy.is_empty() {
                    f.write_str("UPSERT");
                } else {
                    f.write_str("UPSERT (VALUE DECODING ERRORS = (");
                    f.write_node(&display::comma_separated(value_decode_err_policy));
                    f.write_str("))")
                }
            }
            Self::CdcV2 => {
                f.write_str("MATERIALIZE");
            }
        }
    }
}
impl_display!(SourceEnvelope);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SinkEnvelope {
    Debezium,
    Upsert,
}

impl AstDisplay for SinkEnvelope {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            Self::Upsert => {
                f.write_str("UPSERT");
            }
            Self::Debezium => {
                f.write_str("DEBEZIUM");
            }
        }
    }
}
impl_display!(SinkEnvelope);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SubscribeOutput<T: AstInfo> {
    Diffs,
    WithinTimestampOrderBy { order_by: Vec<OrderByExpr<T>> },
    EnvelopeUpsert { key_columns: Vec<Ident> },
    EnvelopeDebezium { key_columns: Vec<Ident> },
}

impl<T: AstInfo> AstDisplay for SubscribeOutput<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            Self::Diffs => {}
            Self::WithinTimestampOrderBy { order_by } => {
                f.write_str(" WITHIN TIMESTAMP ORDER BY ");
                f.write_node(&display::comma_separated(order_by));
            }
            Self::EnvelopeUpsert { key_columns } => {
                f.write_str(" ENVELOPE UPSERT (KEY (");
                f.write_node(&display::comma_separated(key_columns));
                f.write_str("))");
            }
            Self::EnvelopeDebezium { key_columns } => {
                f.write_str(" ENVELOPE DEBEZIUM (KEY (");
                f.write_node(&display::comma_separated(key_columns));
                f.write_str("))");
            }
        }
    }
}
impl_display_t!(SubscribeOutput);

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
            Self::Json { array } => {
                f.write_str("JSON");
                if *array {
                    f.write_str(" ARRAY");
                }
            }
            Self::Text => f.write_str("TEXT"),
        }
    }
}
impl_display_t!(Format);

// All connection options are bundled together to allow us to parse `ALTER
// CONNECTION` without specifying the type of connection we're altering. Il faut
// souffrir pour être belle.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ConnectionOptionName {
    AccessKeyId,
    AssumeRoleArn,
    AssumeRoleSessionName,
    AvailabilityZones,
    AwsConnection,
    AwsPrivatelink,
    Broker,
    Brokers,
    Database,
    Endpoint,
    Host,
    Password,
    Port,
    ProgressTopic,
    ProgressTopicReplicationFactor,
    PublicKey1,
    PublicKey2,
    Region,
    SaslMechanisms,
    SaslPassword,
    SaslUsername,
    SecretAccessKey,
    SecurityProtocol,
    ServiceName,
    SshTunnel,
    SslCertificate,
    SslCertificateAuthority,
    SslKey,
    SslMode,
    SessionToken,
    CatalogType,
    Url,
    User,
    Warehouse,
}

impl AstDisplay for ConnectionOptionName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str(match self {
            ConnectionOptionName::AccessKeyId => "ACCESS KEY ID",
            ConnectionOptionName::AvailabilityZones => "AVAILABILITY ZONES",
            ConnectionOptionName::AwsConnection => "AWS CONNECTION",
            ConnectionOptionName::AwsPrivatelink => "AWS PRIVATELINK",
            ConnectionOptionName::Broker => "BROKER",
            ConnectionOptionName::Brokers => "BROKERS",
            ConnectionOptionName::Database => "DATABASE",
            ConnectionOptionName::Endpoint => "ENDPOINT",
            ConnectionOptionName::Host => "HOST",
            ConnectionOptionName::Password => "PASSWORD",
            ConnectionOptionName::Port => "PORT",
            ConnectionOptionName::ProgressTopic => "PROGRESS TOPIC",
            ConnectionOptionName::ProgressTopicReplicationFactor => {
                "PROGRESS TOPIC REPLICATION FACTOR"
            }
            ConnectionOptionName::PublicKey1 => "PUBLIC KEY 1",
            ConnectionOptionName::PublicKey2 => "PUBLIC KEY 2",
            ConnectionOptionName::Region => "REGION",
            ConnectionOptionName::AssumeRoleArn => "ASSUME ROLE ARN",
            ConnectionOptionName::AssumeRoleSessionName => "ASSUME ROLE SESSION NAME",
            ConnectionOptionName::SaslMechanisms => "SASL MECHANISMS",
            ConnectionOptionName::SaslPassword => "SASL PASSWORD",
            ConnectionOptionName::SaslUsername => "SASL USERNAME",
            ConnectionOptionName::SecurityProtocol => "SECURITY PROTOCOL",
            ConnectionOptionName::SecretAccessKey => "SECRET ACCESS KEY",
            ConnectionOptionName::ServiceName => "SERVICE NAME",
            ConnectionOptionName::SshTunnel => "SSH TUNNEL",
            ConnectionOptionName::SslCertificate => "SSL CERTIFICATE",
            ConnectionOptionName::SslCertificateAuthority => "SSL CERTIFICATE AUTHORITY",
            ConnectionOptionName::SslKey => "SSL KEY",
            ConnectionOptionName::SslMode => "SSL MODE",
            ConnectionOptionName::SessionToken => "SESSION TOKEN",
            ConnectionOptionName::CatalogType => "TYPE",
            ConnectionOptionName::Url => "URL",
            ConnectionOptionName::User => "USER",
            ConnectionOptionName::Warehouse => "WAREHOUSE",
        })
    }
}
impl_display!(ConnectionOptionName);

impl WithOptionName for ConnectionOptionName {
    /// # WARNING
    ///
    /// Whenever implementing this trait consider very carefully whether or not
    /// this value could contain sensitive user data. If you're uncertain, err
    /// on the conservative side and return `true`.
    fn redact_value(&self) -> bool {
        match self {
            ConnectionOptionName::AccessKeyId
            | ConnectionOptionName::AvailabilityZones
            | ConnectionOptionName::AwsConnection
            | ConnectionOptionName::AwsPrivatelink
            | ConnectionOptionName::Broker
            | ConnectionOptionName::Brokers
            | ConnectionOptionName::Database
            | ConnectionOptionName::Endpoint
            | ConnectionOptionName::Host
            | ConnectionOptionName::Password
            | ConnectionOptionName::Port
            | ConnectionOptionName::ProgressTopic
            | ConnectionOptionName::ProgressTopicReplicationFactor
            | ConnectionOptionName::PublicKey1
            | ConnectionOptionName::PublicKey2
            | ConnectionOptionName::Region
            | ConnectionOptionName::AssumeRoleArn
            | ConnectionOptionName::AssumeRoleSessionName
            | ConnectionOptionName::SaslMechanisms
            | ConnectionOptionName::SaslPassword
            | ConnectionOptionName::SaslUsername
            | ConnectionOptionName::SecurityProtocol
            | ConnectionOptionName::SecretAccessKey
            | ConnectionOptionName::ServiceName
            | ConnectionOptionName::SshTunnel
            | ConnectionOptionName::SslCertificate
            | ConnectionOptionName::SslCertificateAuthority
            | ConnectionOptionName::SslKey
            | ConnectionOptionName::SslMode
            | ConnectionOptionName::SessionToken
            | ConnectionOptionName::CatalogType
            | ConnectionOptionName::Url
            | ConnectionOptionName::User
            | ConnectionOptionName::Warehouse => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
/// An option in a `CREATE CONNECTION`.
pub struct ConnectionOption<T: AstInfo> {
    pub name: ConnectionOptionName,
    pub value: Option<WithOptionValue<T>>,
}
impl_display_for_with_option!(ConnectionOption);
impl_display_t!(ConnectionOption);

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum CreateConnectionType {
    Aws,
    AwsPrivatelink,
    Kafka,
    Csr,
    Postgres,
    Ssh,
    SqlServer,
    MySql,
    Yugabyte,
    IcebergCatalog,
}

impl AstDisplay for CreateConnectionType {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            Self::Kafka => {
                f.write_str("KAFKA");
            }
            Self::Csr => {
                f.write_str("CONFLUENT SCHEMA REGISTRY");
            }
            Self::Postgres => {
                f.write_str("POSTGRES");
            }
            Self::Aws => {
                f.write_str("AWS");
            }
            Self::AwsPrivatelink => {
                f.write_str("AWS PRIVATELINK");
            }
            Self::Ssh => {
                f.write_str("SSH TUNNEL");
            }
            Self::SqlServer => {
                f.write_str("SQL SERVER");
            }
            Self::MySql => {
                f.write_str("MYSQL");
            }
            Self::Yugabyte => {
                f.write_str("YUGABYTE");
            }
            Self::IcebergCatalog => {
                f.write_str("ICEBERG CATALOG");
            }
        }
    }
}
impl_display!(CreateConnectionType);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CreateConnectionOptionName {
    Validate,
}

impl AstDisplay for CreateConnectionOptionName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str(match self {
            CreateConnectionOptionName::Validate => "VALIDATE",
        })
    }
}
impl_display!(CreateConnectionOptionName);

impl WithOptionName for CreateConnectionOptionName {
    /// # WARNING
    ///
    /// Whenever implementing this trait consider very carefully whether or not
    /// this value could contain sensitive user data. If you're uncertain, err
    /// on the conservative side and return `true`.
    fn redact_value(&self) -> bool {
        match self {
            CreateConnectionOptionName::Validate => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
/// An option in a `CREATE CONNECTION...` statement.
pub struct CreateConnectionOption<T: AstInfo> {
    pub name: CreateConnectionOptionName,
    pub value: Option<WithOptionValue<T>>,
}
impl_display_for_with_option!(CreateConnectionOption);
impl_display_t!(CreateConnectionOption);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum KafkaSourceConfigOptionName {
    GroupIdPrefix,
    Topic,
    TopicMetadataRefreshInterval,
    StartTimestamp,
    StartOffset,
}

impl AstDisplay for KafkaSourceConfigOptionName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str(match self {
            KafkaSourceConfigOptionName::GroupIdPrefix => "GROUP ID PREFIX",
            KafkaSourceConfigOptionName::Topic => "TOPIC",
            KafkaSourceConfigOptionName::TopicMetadataRefreshInterval => {
                "TOPIC METADATA REFRESH INTERVAL"
            }
            KafkaSourceConfigOptionName::StartOffset => "START OFFSET",
            KafkaSourceConfigOptionName::StartTimestamp => "START TIMESTAMP",
        })
    }
}
impl_display!(KafkaSourceConfigOptionName);

impl WithOptionName for KafkaSourceConfigOptionName {
    /// # WARNING
    ///
    /// Whenever implementing this trait consider very carefully whether or not
    /// this value could contain sensitive user data. If you're uncertain, err
    /// on the conservative side and return `true`.
    fn redact_value(&self) -> bool {
        match self {
            KafkaSourceConfigOptionName::GroupIdPrefix
            | KafkaSourceConfigOptionName::Topic
            | KafkaSourceConfigOptionName::TopicMetadataRefreshInterval
            | KafkaSourceConfigOptionName::StartOffset
            | KafkaSourceConfigOptionName::StartTimestamp => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KafkaSourceConfigOption<T: AstInfo> {
    pub name: KafkaSourceConfigOptionName,
    pub value: Option<WithOptionValue<T>>,
}
impl_display_for_with_option!(KafkaSourceConfigOption);
impl_display_t!(KafkaSourceConfigOption);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum KafkaSinkConfigOptionName {
    CompressionType,
    PartitionBy,
    ProgressGroupIdPrefix,
    Topic,
    TransactionalIdPrefix,
    LegacyIds,
    TopicConfig,
    TopicMetadataRefreshInterval,
    TopicPartitionCount,
    TopicReplicationFactor,
}

impl AstDisplay for KafkaSinkConfigOptionName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str(match self {
            KafkaSinkConfigOptionName::CompressionType => "COMPRESSION TYPE",
            KafkaSinkConfigOptionName::PartitionBy => "PARTITION BY",
            KafkaSinkConfigOptionName::ProgressGroupIdPrefix => "PROGRESS GROUP ID PREFIX",
            KafkaSinkConfigOptionName::Topic => "TOPIC",
            KafkaSinkConfigOptionName::TransactionalIdPrefix => "TRANSACTIONAL ID PREFIX",
            KafkaSinkConfigOptionName::LegacyIds => "LEGACY IDS",
            KafkaSinkConfigOptionName::TopicConfig => "TOPIC CONFIG",
            KafkaSinkConfigOptionName::TopicMetadataRefreshInterval => {
                "TOPIC METADATA REFRESH INTERVAL"
            }
            KafkaSinkConfigOptionName::TopicPartitionCount => "TOPIC PARTITION COUNT",
            KafkaSinkConfigOptionName::TopicReplicationFactor => "TOPIC REPLICATION FACTOR",
        })
    }
}
impl_display!(KafkaSinkConfigOptionName);

impl WithOptionName for KafkaSinkConfigOptionName {
    /// # WARNING
    ///
    /// Whenever implementing this trait consider very carefully whether or not
    /// this value could contain sensitive user data. If you're uncertain, err
    /// on the conservative side and return `true`.
    fn redact_value(&self) -> bool {
        match self {
            KafkaSinkConfigOptionName::CompressionType
            | KafkaSinkConfigOptionName::ProgressGroupIdPrefix
            | KafkaSinkConfigOptionName::Topic
            | KafkaSinkConfigOptionName::TopicMetadataRefreshInterval
            | KafkaSinkConfigOptionName::TransactionalIdPrefix
            | KafkaSinkConfigOptionName::LegacyIds
            | KafkaSinkConfigOptionName::TopicConfig
            | KafkaSinkConfigOptionName::TopicPartitionCount
            | KafkaSinkConfigOptionName::TopicReplicationFactor => false,
            KafkaSinkConfigOptionName::PartitionBy => true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KafkaSinkConfigOption<T: AstInfo> {
    pub name: KafkaSinkConfigOptionName,
    pub value: Option<WithOptionValue<T>>,
}
impl_display_for_with_option!(KafkaSinkConfigOption);
impl_display_t!(KafkaSinkConfigOption);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PgConfigOptionName {
    /// Hex encoded string of binary serialization of
    /// `mz_storage_types::sources::postgres::PostgresSourcePublicationDetails`
    Details,
    /// The name of the publication to sync
    Publication,
    /// Columns whose types you want to unconditionally format as text
    /// NOTE(roshan): This value is kept around to allow round-tripping a
    /// `CREATE SOURCE` statement while we still allow creating implicit
    /// subsources from `CREATE SOURCE`, but will be removed once
    /// fully deprecating that feature and forcing users to use explicit
    /// `CREATE TABLE .. FROM SOURCE` statements
    TextColumns,
}

impl AstDisplay for PgConfigOptionName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str(match self {
            PgConfigOptionName::Details => "DETAILS",
            PgConfigOptionName::Publication => "PUBLICATION",
            PgConfigOptionName::TextColumns => "TEXT COLUMNS",
        })
    }
}
impl_display!(PgConfigOptionName);

impl WithOptionName for PgConfigOptionName {
    /// # WARNING
    ///
    /// Whenever implementing this trait consider very carefully whether or not
    /// this value could contain sensitive user data. If you're uncertain, err
    /// on the conservative side and return `true`.
    fn redact_value(&self) -> bool {
        match self {
            PgConfigOptionName::Details
            | PgConfigOptionName::Publication
            | PgConfigOptionName::TextColumns => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
/// An option in a `{FROM|INTO} CONNECTION ...` statement.
pub struct PgConfigOption<T: AstInfo> {
    pub name: PgConfigOptionName,
    pub value: Option<WithOptionValue<T>>,
}
impl_display_for_with_option!(PgConfigOption);
impl_display_t!(PgConfigOption);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MySqlConfigOptionName {
    /// Hex encoded string of binary serialization of
    /// `mz_storage_types::sources::mysql::MySqlSourceDetails`
    Details,
    /// Columns whose types you want to unconditionally format as text
    /// NOTE(roshan): This value is kept around to allow round-tripping a
    /// `CREATE SOURCE` statement while we still allow creating implicit
    /// subsources from `CREATE SOURCE`, but will be removed once
    /// fully deprecating that feature and forcing users to use explicit
    /// `CREATE TABLE .. FROM SOURCE` statements
    TextColumns,
    /// Columns you want to exclude
    /// NOTE(roshan): This value is kept around to allow round-tripping a
    /// `CREATE SOURCE` statement while we still allow creating implicit
    /// subsources from `CREATE SOURCE`, but will be removed once
    /// fully deprecating that feature and forcing users to use explicit
    /// `CREATE TABLE .. FROM SOURCE` statements
    ExcludeColumns,
}

impl AstDisplay for MySqlConfigOptionName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str(match self {
            MySqlConfigOptionName::Details => "DETAILS",
            MySqlConfigOptionName::TextColumns => "TEXT COLUMNS",
            MySqlConfigOptionName::ExcludeColumns => "EXCLUDE COLUMNS",
        })
    }
}
impl_display!(MySqlConfigOptionName);

impl WithOptionName for MySqlConfigOptionName {
    /// # WARNING
    ///
    /// Whenever implementing this trait consider very carefully whether or not
    /// this value could contain sensitive user data. If you're uncertain, err
    /// on the conservative side and return `true`.
    fn redact_value(&self) -> bool {
        match self {
            MySqlConfigOptionName::Details
            | MySqlConfigOptionName::TextColumns
            | MySqlConfigOptionName::ExcludeColumns => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
/// An option in a `{FROM|INTO} CONNECTION ...` statement.
pub struct MySqlConfigOption<T: AstInfo> {
    pub name: MySqlConfigOptionName,
    pub value: Option<WithOptionValue<T>>,
}
impl_display_for_with_option!(MySqlConfigOption);
impl_display_t!(MySqlConfigOption);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SqlServerConfigOptionName {
    /// Hex encoded string of binary serialization of
    /// `mz_storage_types::sources::sql_server::SqlServerSourceDetails`.
    Details,
    /// Columns whose types you want to unconditionally format as text.
    ///
    /// NOTE(roshan): This value is kept around to allow round-tripping a
    /// `CREATE SOURCE` statement while we still allow creating implicit
    /// subsources from `CREATE SOURCE`, but will be removed once
    /// fully deprecating that feature and forcing users to use explicit
    /// `CREATE TABLE .. FROM SOURCE` statements
    TextColumns,
    /// Columns you want to exclude.
    ///
    /// NOTE(roshan): This value is kept around to allow round-tripping a
    /// `CREATE SOURCE` statement while we still allow creating implicit
    /// subsources from `CREATE SOURCE`, but will be removed once
    /// fully deprecating that feature and forcing users to use explicit
    /// `CREATE TABLE .. FROM SOURCE` statements
    ExcludeColumns,
}

impl AstDisplay for SqlServerConfigOptionName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str(match self {
            SqlServerConfigOptionName::Details => "DETAILS",
            SqlServerConfigOptionName::TextColumns => "TEXT COLUMNS",
            SqlServerConfigOptionName::ExcludeColumns => "EXCLUDE COLUMNS",
        })
    }
}
impl_display!(SqlServerConfigOptionName);

impl WithOptionName for SqlServerConfigOptionName {
    /// # WARNING
    ///
    /// Whenever implementing this trait consider very carefully whether or not
    /// this value could contain sensitive user data. If you're uncertain, err
    /// on the conservative side and return `true`.
    fn redact_value(&self) -> bool {
        match self {
            SqlServerConfigOptionName::Details
            | SqlServerConfigOptionName::TextColumns
            | SqlServerConfigOptionName::ExcludeColumns => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
/// An option in a `{FROM|INTO} CONNECTION ...` statement.
pub struct SqlServerConfigOption<T: AstInfo> {
    pub name: SqlServerConfigOptionName,
    pub value: Option<WithOptionValue<T>>,
}
impl_display_for_with_option!(SqlServerConfigOption);
impl_display_t!(SqlServerConfigOption);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CreateSourceConnection<T: AstInfo> {
    Kafka {
        connection: T::ItemName,
        options: Vec<KafkaSourceConfigOption<T>>,
    },
    Postgres {
        connection: T::ItemName,
        options: Vec<PgConfigOption<T>>,
    },
    Yugabyte {
        connection: T::ItemName,
        options: Vec<PgConfigOption<T>>,
    },
    SqlServer {
        connection: T::ItemName,
        options: Vec<SqlServerConfigOption<T>>,
    },
    MySql {
        connection: T::ItemName,
        options: Vec<MySqlConfigOption<T>>,
    },
    LoadGenerator {
        generator: LoadGenerator,
        options: Vec<LoadGeneratorOption<T>>,
    },
}

impl<T: AstInfo> AstDisplay for CreateSourceConnection<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            CreateSourceConnection::Kafka {
                connection,
                options,
            } => {
                f.write_str("KAFKA CONNECTION ");
                f.write_node(connection);
                if !options.is_empty() {
                    f.write_str(" (");
                    f.write_node(&display::comma_separated(options));
                    f.write_str(")");
                }
            }
            CreateSourceConnection::Postgres {
                connection,
                options,
            } => {
                f.write_str("POSTGRES CONNECTION ");
                f.write_node(connection);
                if !options.is_empty() {
                    f.write_str(" (");
                    f.write_node(&display::comma_separated(options));
                    f.write_str(")");
                }
            }
            CreateSourceConnection::Yugabyte {
                connection,
                options,
            } => {
                f.write_str("YUGABYTE CONNECTION ");
                f.write_node(connection);
                if !options.is_empty() {
                    f.write_str(" (");
                    f.write_node(&display::comma_separated(options));
                    f.write_str(")");
                }
            }
            CreateSourceConnection::SqlServer {
                connection,
                options,
            } => {
                f.write_str("SQL SERVER CONNECTION ");
                f.write_node(connection);
                if !options.is_empty() {
                    f.write_str(" (");
                    f.write_node(&display::comma_separated(options));
                    f.write_str(")");
                }
            }
            CreateSourceConnection::MySql {
                connection,
                options,
            } => {
                f.write_str("MYSQL CONNECTION ");
                f.write_node(connection);
                if !options.is_empty() {
                    f.write_str(" (");
                    f.write_node(&display::comma_separated(options));
                    f.write_str(")");
                }
            }
            CreateSourceConnection::LoadGenerator { generator, options } => {
                f.write_str("LOAD GENERATOR ");
                f.write_node(generator);
                if !options.is_empty() {
                    f.write_str(" (");
                    f.write_node(&display::comma_separated(options));
                    f.write_str(")");
                }
            }
        }
    }
}
impl_display_t!(CreateSourceConnection);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LoadGenerator {
    Clock,
    Counter,
    Marketing,
    Auction,
    Datums,
    Tpch,
    KeyValue,
}

impl AstDisplay for LoadGenerator {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            Self::Counter => f.write_str("COUNTER"),
            Self::Clock => f.write_str("CLOCK"),
            Self::Marketing => f.write_str("MARKETING"),
            Self::Auction => f.write_str("AUCTION"),
            Self::Datums => f.write_str("DATUMS"),
            Self::Tpch => f.write_str("TPCH"),
            Self::KeyValue => f.write_str("KEY VALUE"),
        }
    }
}
impl_display!(LoadGenerator);

impl LoadGenerator {
    /// Corresponds with the same mapping on the `LoadGenerator` enum defined in
    /// src/storage-types/src/sources/load_generator.rs, but re-defined here for
    /// cases where we only have the AST representation. This can be removed once
    /// the `ast_rewrite_sources_to_tables` migration is removed.
    pub fn schema_name(&self) -> &'static str {
        match self {
            LoadGenerator::Counter => "counter",
            LoadGenerator::Clock => "clock",
            LoadGenerator::Marketing => "marketing",
            LoadGenerator::Auction => "auction",
            LoadGenerator::Datums => "datums",
            LoadGenerator::Tpch => "tpch",
            LoadGenerator::KeyValue => "key_value",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LoadGeneratorOptionName {
    ScaleFactor,
    TickInterval,
    AsOf,
    UpTo,
    MaxCardinality,
    Keys,
    SnapshotRounds,
    TransactionalSnapshot,
    ValueSize,
    Seed,
    Partitions,
    BatchSize,
}

impl AstDisplay for LoadGeneratorOptionName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str(match self {
            LoadGeneratorOptionName::ScaleFactor => "SCALE FACTOR",
            LoadGeneratorOptionName::TickInterval => "TICK INTERVAL",
            LoadGeneratorOptionName::AsOf => "AS OF",
            LoadGeneratorOptionName::UpTo => "UP TO",
            LoadGeneratorOptionName::MaxCardinality => "MAX CARDINALITY",
            LoadGeneratorOptionName::Keys => "KEYS",
            LoadGeneratorOptionName::SnapshotRounds => "SNAPSHOT ROUNDS",
            LoadGeneratorOptionName::TransactionalSnapshot => "TRANSACTIONAL SNAPSHOT",
            LoadGeneratorOptionName::ValueSize => "VALUE SIZE",
            LoadGeneratorOptionName::Seed => "SEED",
            LoadGeneratorOptionName::Partitions => "PARTITIONS",
            LoadGeneratorOptionName::BatchSize => "BATCH SIZE",
        })
    }
}
impl_display!(LoadGeneratorOptionName);

impl WithOptionName for LoadGeneratorOptionName {
    /// # WARNING
    ///
    /// Whenever implementing this trait consider very carefully whether or not
    /// this value could contain sensitive user data. If you're uncertain, err
    /// on the conservative side and return `true`.
    fn redact_value(&self) -> bool {
        match self {
            LoadGeneratorOptionName::ScaleFactor
            | LoadGeneratorOptionName::TickInterval
            | LoadGeneratorOptionName::AsOf
            | LoadGeneratorOptionName::UpTo
            | LoadGeneratorOptionName::MaxCardinality
            | LoadGeneratorOptionName::Keys
            | LoadGeneratorOptionName::SnapshotRounds
            | LoadGeneratorOptionName::TransactionalSnapshot
            | LoadGeneratorOptionName::ValueSize
            | LoadGeneratorOptionName::Partitions
            | LoadGeneratorOptionName::BatchSize
            | LoadGeneratorOptionName::Seed => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
/// An option in a `CREATE CONNECTION...SSH`.
pub struct LoadGeneratorOption<T: AstInfo> {
    pub name: LoadGeneratorOptionName,
    pub value: Option<WithOptionValue<T>>,
}
impl_display_for_with_option!(LoadGeneratorOption);
impl_display_t!(LoadGeneratorOption);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CreateSinkConnection<T: AstInfo> {
    Kafka {
        connection: T::ItemName,
        options: Vec<KafkaSinkConfigOption<T>>,
        key: Option<KafkaSinkKey>,
        headers: Option<Ident>,
    },
}

impl<T: AstInfo> AstDisplay for CreateSinkConnection<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            CreateSinkConnection::Kafka {
                connection,
                options,
                key,
                headers,
            } => {
                f.write_str("KAFKA CONNECTION ");
                f.write_node(connection);
                if !options.is_empty() {
                    f.write_str(" (");
                    f.write_node(&display::comma_separated(options));
                    f.write_str(")");
                }
                if let Some(key) = key.as_ref() {
                    f.write_node(key);
                }
                if let Some(headers) = headers {
                    f.write_str(" HEADERS ");
                    f.write_node(headers);
                }
            }
        }
    }
}
impl_display_t!(CreateSinkConnection);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KafkaSinkKey {
    pub key_columns: Vec<Ident>,
    pub not_enforced: bool,
}

impl AstDisplay for KafkaSinkKey {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str(" KEY (");
        f.write_node(&display::comma_separated(&self.key_columns));
        f.write_str(")");
        if self.not_enforced {
            f.write_str(" NOT ENFORCED");
        }
    }
}

/// A table-level constraint, specified in a `CREATE TABLE` or an
/// `ALTER TABLE ADD <constraint>` statement.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TableConstraint<T: AstInfo> {
    /// `[ CONSTRAINT <name> ] { PRIMARY KEY | UNIQUE (NULLS NOT DISTINCT)? } (<columns>)`
    Unique {
        name: Option<Ident>,
        columns: Vec<Ident>,
        /// Whether this is a `PRIMARY KEY` or just a `UNIQUE` constraint
        is_primary: bool,
        // Where this constraint treats each NULL value as distinct; only available on `UNIQUE`
        // constraints.
        nulls_not_distinct: bool,
    },
    /// A referential integrity constraint (`[ CONSTRAINT <name> ] FOREIGN KEY (<columns>)
    /// REFERENCES <foreign_table> (<referred_columns>)`)
    ForeignKey {
        name: Option<Ident>,
        columns: Vec<Ident>,
        foreign_table: T::ItemName,
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
                nulls_not_distinct,
            } => {
                f.write_node(&display_constraint_name(name));
                if *is_primary {
                    f.write_str("PRIMARY KEY ");
                } else {
                    f.write_str("UNIQUE ");
                    if *nulls_not_distinct {
                        f.write_str("NULLS NOT DISTINCT ");
                    }
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CreateSourceOptionName {
    TimestampInterval,
    RetainHistory,
}

impl AstDisplay for CreateSourceOptionName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str(match self {
            CreateSourceOptionName::TimestampInterval => "TIMESTAMP INTERVAL",
            CreateSourceOptionName::RetainHistory => "RETAIN HISTORY",
        })
    }
}
impl_display!(CreateSourceOptionName);

impl WithOptionName for CreateSourceOptionName {
    /// # WARNING
    ///
    /// Whenever implementing this trait consider very carefully whether or not
    /// this value could contain sensitive user data. If you're uncertain, err
    /// on the conservative side and return `true`.
    fn redact_value(&self) -> bool {
        match self {
            CreateSourceOptionName::TimestampInterval | CreateSourceOptionName::RetainHistory => {
                false
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
/// An option in a `CREATE SOURCE...` statement.
pub struct CreateSourceOption<T: AstInfo> {
    pub name: CreateSourceOptionName,
    pub value: Option<WithOptionValue<T>>,
}
impl_display_for_with_option!(CreateSourceOption);
impl_display_t!(CreateSourceOption);

/// SQL column definition
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnDef<T: AstInfo> {
    pub name: Ident,
    pub data_type: T::DataType,
    pub collation: Option<UnresolvedItemName>,
    pub options: Vec<ColumnOptionDef<T>>,
}

impl<T: AstInfo> AstDisplay for ColumnDef<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_node(&self.name);
        f.write_str(" ");
        f.write_node(&self.data_type);
        if let Some(collation) = &self.collation {
            f.write_str(" COLLATE ");
            f.write_node(collation);
        }
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
    Unique { is_primary: bool },
    /// A referential integrity constraint (`[FOREIGN KEY REFERENCES
    /// <foreign_table> (<referred_columns>)`).
    ForeignKey {
        foreign_table: UnresolvedItemName,
        referred_columns: Vec<Ident>,
    },
    /// `CHECK (<expr>)`
    Check(Expr<T>),
    /// `VERSION <action> <version>`
    Versioned {
        action: ColumnVersioned,
        version: Version,
    },
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
            Versioned { action, version } => {
                f.write_str("VERSION ");
                f.write_node(action);
                f.write_str(" ");
                f.write_node(version);
            }
        }
    }
}
impl_display_t!(ColumnOption);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ColumnVersioned {
    Added,
}

impl AstDisplay for ColumnVersioned {
    fn fmt<W>(&self, f: &mut AstFormatter<W>)
    where
        W: fmt::Write,
    {
        match self {
            // TODO(alter_table): Support dropped columns.
            ColumnVersioned::Added => f.write_str("ADDED"),
        }
    }
}
impl_display!(ColumnVersioned);

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
