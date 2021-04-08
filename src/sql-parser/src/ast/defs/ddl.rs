// Copyright 2018 sqlparser-rs contributors. All rights reserved.
// Copyright Materialize, Inc. All rights reserved.
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

use std::path::PathBuf;

use crate::ast::display::{self, AstDisplay, AstFormatter};
use crate::ast::{AstInfo, DataType, Expr, Ident, SqlOption, UnresolvedObjectName, WithOption};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Schema {
    File(PathBuf),
    Inline(String),
}

impl AstDisplay for Schema {
    fn fmt(&self, f: &mut AstFormatter) {
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
    CsrUrl {
        url: String,
        seed: Option<CsrSeed>,
        with_options: Vec<SqlOption<T>>,
    },
    Schema {
        schema: Schema,
        with_options: Vec<WithOption>,
    },
}

impl<T: AstInfo> AstDisplay for AvroSchema<T> {
    fn fmt(&self, f: &mut AstFormatter) {
        match self {
            Self::CsrUrl {
                url,
                seed,
                with_options,
            } => {
                f.write_str("CONFLUENT SCHEMA REGISTRY '");
                f.write_node(&display::escape_single_quote_string(url));
                f.write_str("'");
                if let Some(seed) = seed {
                    f.write_str(" ");
                    f.write_node(seed);
                }
                if !with_options.is_empty() {
                    f.write_str(" WITH (");
                    f.write_node(&display::comma_separated(with_options));
                    f.write_str(")");
                }
            }
            Self::Schema {
                schema,
                with_options,
            } => {
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
pub struct CsrSeed {
    pub key_schema: Option<String>,
    pub value_schema: String,
}

impl AstDisplay for CsrSeed {
    fn fmt(&self, f: &mut AstFormatter) {
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
pub enum Format<T: AstInfo> {
    Bytes,
    Avro(AvroSchema<T>),
    Protobuf {
        message_name: String,
        schema: Schema,
    },
    Regex(String),
    Csv {
        header_row: bool,
        n_cols: Option<usize>,
        delimiter: char,
    },
    Json,
    Text,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Envelope<T: AstInfo> {
    None,
    Debezium(DbzMode),
    Upsert(Option<Format<T>>),
    CdcV2,
}

impl<T: AstInfo> Default for Envelope<T> {
    fn default() -> Self {
        Self::None
    }
}

impl<T: AstInfo> AstDisplay for Envelope<T> {
    fn fmt(&self, f: &mut AstFormatter) {
        match self {
            Self::None => {
                // this is unreachable as long as the default is None, but include it in case we ever change that
                f.write_str("NONE");
            }
            Self::Debezium(mode) => {
                f.write_str("DEBEZIUM");
                f.write_node(mode);
            }
            Self::Upsert(format) => {
                f.write_str("UPSERT");
                if let Some(format) = format {
                    f.write_str(" FORMAT ");
                    f.write_node(format);
                }
            }
            Self::CdcV2 => {
                f.write_str("MATERIALIZE");
            }
        }
    }
}
impl_display_t!(Envelope);

impl<T: AstInfo> AstDisplay for Format<T> {
    fn fmt(&self, f: &mut AstFormatter) {
        match self {
            Self::Bytes => f.write_str("BYTES"),
            Self::Avro(inner) => {
                f.write_str("AVRO USING ");
                f.write_node(inner);
            }
            Self::Protobuf {
                message_name,
                schema,
            } => {
                f.write_str("PROTOBUF MESSAGE '");
                f.write_node(&display::escape_single_quote_string(message_name));
                f.write_str("' USING ");
                f.write_str(schema);
            }
            Self::Regex(regex) => {
                f.write_str("REGEX '");
                f.write_node(&display::escape_single_quote_string(regex));
                f.write_str("'");
            }
            Self::Csv {
                header_row,
                n_cols,
                delimiter,
            } => {
                f.write_str("CSV WITH ");
                if *header_row {
                    f.write_str("HEADER");
                } else {
                    f.write_str(n_cols.unwrap());
                    f.write_str(" COLUMNS");
                }
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

impl Default for Compression {
    fn default() -> Self {
        Self::None
    }
}

impl AstDisplay for Compression {
    fn fmt(&self, f: &mut AstFormatter) {
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

impl Default for DbzMode {
    fn default() -> Self {
        Self::Plain
    }
}

impl AstDisplay for DbzMode {
    fn fmt(&self, f: &mut AstFormatter) {
        match self {
            Self::Plain => f.write_str(""),
            Self::Upsert => f.write_str(" UPSERT"),
        }
    }
}
impl_display!(DbzMode);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Connector<T: AstInfo> {
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
        /// The namespace the synced table belongs to
        namespace: String,
        /// The name of the table to sync
        table: String,
        /// The expected column schema of the synced table
        columns: Vec<ColumnDef<T>>,
    },
    PubNub {
        /// PubNub's subscribe key
        subscribe_key: String,
        /// The PubNub channel to subscribe to
        channel: String,
    },
}

impl<T: AstInfo> AstDisplay for Connector<T> {
    fn fmt(&self, f: &mut AstFormatter) {
        match self {
            Connector::File { path, compression } => {
                f.write_str("FILE '");
                f.write_node(&display::escape_single_quote_string(path));
                f.write_str("'");
                if compression != &Default::default() {
                    f.write_str(" COMPRESSION ");
                    f.write_node(compression);
                }
            }
            Connector::Kafka { broker, topic, key } => {
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
            Connector::Kinesis { arn } => {
                f.write_str("KINESIS ARN '");
                f.write_node(&display::escape_single_quote_string(arn));
                f.write_str("'");
            }
            Connector::AvroOcf { path } => {
                f.write_str("AVRO OCF '");
                f.write_node(&display::escape_single_quote_string(path));
                f.write_str("'");
            }
            Connector::S3 {
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
                if compression != &Default::default() {
                    f.write_str(" COMPRESSION ");
                    f.write_node(compression);
                }
            }
            Connector::Postgres {
                conn,
                publication,
                namespace,
                table,
                columns,
            } => {
                f.write_str("POSTGRES HOST '");
                f.write_str(&display::escape_single_quote_string(conn));
                f.write_str("' PUBLICATION '");
                f.write_str(&display::escape_single_quote_string(publication));
                f.write_str("' NAMESPACE '");
                f.write_str(&display::escape_single_quote_string(namespace));
                f.write_str("' TABLE '");
                f.write_str(&display::escape_single_quote_string(table));
                f.write_str("' (");
                f.write_node(&display::comma_separated(columns));
                f.write_str(")");
            }
            Connector::PubNub {
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
impl_display_t!(Connector);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MultiConnector<T: AstInfo> {
    Postgres {
        /// The postgres connection string
        conn: String,
        /// The name of the publication to sync
        publication: String,
        /// The namespace the synced table belongs to
        namespace: String,
        /// The tables to sync
        tables: Vec<PgTable<T>>,
    },
}

impl<T: AstInfo> AstDisplay for MultiConnector<T> {
    fn fmt(&self, f: &mut AstFormatter) {
        match self {
            MultiConnector::Postgres {
                conn,
                publication,
                namespace,
                tables,
            } => {
                f.write_str("POSTGRES HOST '");
                f.write_str(&display::escape_single_quote_string(conn));
                f.write_str("' PUBLICATION '");
                f.write_str(&display::escape_single_quote_string(publication));
                f.write_str("' NAMESPACE '");
                f.write_str(&display::escape_single_quote_string(namespace));
                f.write_str("' TABLES (");
                f.write_node(&display::comma_separated(tables));
                f.write_str(")");
            }
        }
    }
}
impl_display_t!(MultiConnector);

/// Information about upstream Postgres tables used for replication sources
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PgTable<T: AstInfo> {
    /// The name of the table to sync
    pub name: String,
    /// The name for the table in Materialize
    pub alias: T::ObjectName,
    /// The expected column schema of the synced table
    pub columns: Vec<ColumnDef<T>>,
}

impl<T: AstInfo> AstDisplay for PgTable<T> {
    fn fmt(&self, f: &mut AstFormatter) {
        f.write_str("'");
        f.write_str(&display::escape_single_quote_string(&self.name));
        f.write_str("'");
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
    fn fmt(&self, f: &mut AstFormatter) {
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
    fn fmt(&self, f: &mut AstFormatter) {
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

/// SQL column definition
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnDef<T: AstInfo> {
    pub name: Ident,
    pub data_type: DataType<T>,
    pub collation: Option<UnresolvedObjectName>,
    pub options: Vec<ColumnOptionDef<T>>,
}

impl<T: AstInfo> AstDisplay for ColumnDef<T> {
    fn fmt(&self, f: &mut AstFormatter) {
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
    fn fmt(&self, f: &mut AstFormatter) {
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
    fn fmt(&self, f: &mut AstFormatter) {
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
        fn fmt(&self, f: &mut AstFormatter) {
            if let Some(name) = self.0 {
                f.write_str("CONSTRAINT ");
                f.write_node(name);
                f.write_str(" ");
            }
        }
    }
    ConstraintName(name)
}
