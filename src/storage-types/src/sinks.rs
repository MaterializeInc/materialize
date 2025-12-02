// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types and traits related to reporting changing collections out of `dataflow`.

use std::borrow::Cow;
use std::fmt::Debug;
use std::time::Duration;

use mz_dyncfg::ConfigSet;
use mz_expr::MirScalarExpr;
use mz_pgcopy::CopyFormatParams;
use mz_repr::bytes::ByteSize;
use mz_repr::{CatalogItemId, GlobalId, RelationDesc};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use timely::PartialOrder;
use timely::progress::frontier::Antichain;

use crate::AlterCompatible;
use crate::connections::inline::{
    ConnectionAccess, ConnectionResolver, InlinedConnection, IntoInlineConnection,
    ReferencedConnection,
};
use crate::connections::{ConnectionContext, KafkaConnection, KafkaTopicOptions};
use crate::controller::AlterError;

pub mod s3_oneshot_sink;

/// A sink for updates to a relational collection.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct StorageSinkDesc<S, T = mz_repr::Timestamp> {
    pub from: GlobalId,
    pub from_desc: RelationDesc,
    pub connection: StorageSinkConnection,
    pub with_snapshot: bool,
    pub version: u64,
    pub envelope: SinkEnvelope,
    pub as_of: Antichain<T>,
    pub from_storage_metadata: S,
    pub to_storage_metadata: S,
    /// The interval at which to commit data to the sink.
    /// This isn't universally supported by all sinks
    /// yet, so it is optional. Even for sinks that might
    /// support it in the future (ahem, kafka) users might
    /// not want to set it.
    pub commit_interval: Option<Duration>,
}

impl<S: Debug + PartialEq, T: Debug + PartialEq + PartialOrder> AlterCompatible
    for StorageSinkDesc<S, T>
{
    /// Determines if `self` is compatible with another `StorageSinkDesc`, in
    /// such a way that it is possible to turn `self` into `other` through a
    /// valid series of transformations.
    ///
    /// Currently, the only "valid transformation" is the passage of time such
    /// that the sink's as ofs may differ. However, this will change once we
    /// support `ALTER CONNECTION` or `ALTER SINK`.
    fn alter_compatible(
        &self,
        id: GlobalId,
        other: &StorageSinkDesc<S, T>,
    ) -> Result<(), AlterError> {
        if self == other {
            return Ok(());
        }
        let StorageSinkDesc {
            from,
            from_desc,
            connection,
            envelope,
            version: _,
            // The as-of of the descriptions may differ.
            as_of: _,
            from_storage_metadata,
            with_snapshot,
            to_storage_metadata,
            commit_interval: _,
        } = self;

        let compatibility_checks = [
            (from == &other.from, "from"),
            (from_desc == &other.from_desc, "from_desc"),
            (
                connection.alter_compatible(id, &other.connection).is_ok(),
                "connection",
            ),
            (envelope == &other.envelope, "envelope"),
            // This can legally change from true to false once the snapshot has been
            // written out.
            (*with_snapshot || !other.with_snapshot, "with_snapshot"),
            (
                from_storage_metadata == &other.from_storage_metadata,
                "from_storage_metadata",
            ),
            (
                to_storage_metadata == &other.to_storage_metadata,
                "to_storage_metadata",
            ),
        ];

        for (compatible, field) in compatibility_checks {
            if !compatible {
                tracing::warn!(
                    "StorageSinkDesc incompatible at {field}:\nself:\n{:#?}\n\nother\n{:#?}",
                    self,
                    other
                );

                return Err(AlterError { id });
            }
        }

        Ok(())
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SinkEnvelope {
    Debezium,
    Upsert,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum StorageSinkConnection<C: ConnectionAccess = InlinedConnection> {
    Kafka(KafkaSinkConnection<C>),
    Iceberg(IcebergSinkConnection<C>),
}

impl<C: ConnectionAccess> StorageSinkConnection<C> {
    /// Determines if `self` is compatible with another `StorageSinkConnection`,
    /// in such a way that it is possible to turn `self` into `other` through a
    /// valid series of transformations (e.g. no transformation or `ALTER
    /// CONNECTION`).
    pub fn alter_compatible(
        &self,
        id: GlobalId,
        other: &StorageSinkConnection<C>,
    ) -> Result<(), AlterError> {
        if self == other {
            return Ok(());
        }
        match (self, other) {
            (StorageSinkConnection::Kafka(s), StorageSinkConnection::Kafka(o)) => {
                s.alter_compatible(id, o)?
            }
            (StorageSinkConnection::Iceberg(s), StorageSinkConnection::Iceberg(o)) => {
                s.alter_compatible(id, o)?
            }
            _ => {
                tracing::warn!(
                    "StorageSinkConnection incompatible:\nself:\n{:#?}\n\nother\n{:#?}",
                    self,
                    other
                );
                return Err(AlterError { id });
            }
        }

        Ok(())
    }
}

impl<R: ConnectionResolver> IntoInlineConnection<StorageSinkConnection, R>
    for StorageSinkConnection<ReferencedConnection>
{
    fn into_inline_connection(self, r: R) -> StorageSinkConnection {
        match self {
            Self::Kafka(conn) => StorageSinkConnection::Kafka(conn.into_inline_connection(r)),
            Self::Iceberg(conn) => StorageSinkConnection::Iceberg(conn.into_inline_connection(r)),
        }
    }
}

impl<C: ConnectionAccess> StorageSinkConnection<C> {
    /// returns an option to not constrain ourselves in the future
    pub fn connection_id(&self) -> Option<CatalogItemId> {
        use StorageSinkConnection::*;
        match self {
            Kafka(KafkaSinkConnection { connection_id, .. }) => Some(*connection_id),
            Iceberg(IcebergSinkConnection {
                catalog_connection_id: connection_id,
                ..
            }) => Some(*connection_id),
        }
    }

    /// Returns the name of the sink connection.
    pub fn name(&self) -> &'static str {
        use StorageSinkConnection::*;
        match self {
            Kafka(_) => "kafka",
            Iceberg(_) => "iceberg",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum KafkaSinkCompressionType {
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

impl KafkaSinkCompressionType {
    /// Format the compression type as expected by `compression.type` librdkafka
    /// setting.
    pub fn to_librdkafka_option(&self) -> &'static str {
        match self {
            KafkaSinkCompressionType::None => "none",
            KafkaSinkCompressionType::Gzip => "gzip",
            KafkaSinkCompressionType::Snappy => "snappy",
            KafkaSinkCompressionType::Lz4 => "lz4",
            KafkaSinkCompressionType::Zstd => "zstd",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KafkaSinkConnection<C: ConnectionAccess = InlinedConnection> {
    pub connection_id: CatalogItemId,
    pub connection: C::Kafka,
    pub format: KafkaSinkFormat<C>,
    /// A natural key of the sinked relation (view or source).
    pub relation_key_indices: Option<Vec<usize>>,
    /// The user-specified key for the sink.
    pub key_desc_and_indices: Option<(RelationDesc, Vec<usize>)>,
    /// The index of the column containing message headers value, if any.
    pub headers_index: Option<usize>,
    pub value_desc: RelationDesc,
    /// An expression that, if present, computes a hash value that should be
    /// used to determine the partition for each message.
    pub partition_by: Option<MirScalarExpr>,
    pub topic: String,
    /// Options to use when creating the topic if it doesn't already exist.
    pub topic_options: KafkaTopicOptions,
    pub compression_type: KafkaSinkCompressionType,
    pub progress_group_id: KafkaIdStyle,
    pub transactional_id: KafkaIdStyle,
    pub topic_metadata_refresh_interval: Duration,
}

impl KafkaSinkConnection {
    /// Returns the client ID to register with librdkafka with.
    ///
    /// The caller is responsible for providing the sink ID as it is not known
    /// to `KafkaSinkConnection`.
    pub fn client_id(
        &self,
        configs: &ConfigSet,
        connection_context: &ConnectionContext,
        sink_id: GlobalId,
    ) -> String {
        let mut client_id =
            KafkaConnection::id_base(connection_context, self.connection_id, sink_id);
        self.connection.enrich_client_id(configs, &mut client_id);
        client_id
    }

    /// Returns the name of the progress topic to use for the sink.
    pub fn progress_topic(&self, connection_context: &ConnectionContext) -> Cow<'_, str> {
        self.connection
            .progress_topic(connection_context, self.connection_id)
    }

    /// Returns the ID for the consumer group the sink will use to read the
    /// progress topic on resumption.
    ///
    /// The caller is responsible for providing the sink ID as it is not known
    /// to `KafkaSinkConnection`.
    pub fn progress_group_id(
        &self,
        connection_context: &ConnectionContext,
        sink_id: GlobalId,
    ) -> String {
        match self.progress_group_id {
            KafkaIdStyle::Prefix(ref prefix) => format!(
                "{}{}",
                prefix.as_deref().unwrap_or(""),
                KafkaConnection::id_base(connection_context, self.connection_id, sink_id),
            ),
            KafkaIdStyle::Legacy => format!("materialize-bootstrap-sink-{sink_id}"),
        }
    }

    /// Returns the transactional ID to use for the sink.
    ///
    /// The caller is responsible for providing the sink ID as it is not known
    /// to `KafkaSinkConnection`.
    pub fn transactional_id(
        &self,
        connection_context: &ConnectionContext,
        sink_id: GlobalId,
    ) -> String {
        match self.transactional_id {
            KafkaIdStyle::Prefix(ref prefix) => format!(
                "{}{}",
                prefix.as_deref().unwrap_or(""),
                KafkaConnection::id_base(connection_context, self.connection_id, sink_id)
            ),
            KafkaIdStyle::Legacy => format!("mz-producer-{sink_id}-0"),
        }
    }
}

impl<C: ConnectionAccess> KafkaSinkConnection<C> {
    /// Determines if `self` is compatible with another `StorageSinkConnection`,
    /// in such a way that it is possible to turn `self` into `other` through a
    /// valid series of transformations (e.g. no transformation or `ALTER
    /// CONNECTION`).
    pub fn alter_compatible(
        &self,
        id: GlobalId,
        other: &KafkaSinkConnection<C>,
    ) -> Result<(), AlterError> {
        if self == other {
            return Ok(());
        }
        let KafkaSinkConnection {
            connection_id,
            connection,
            format,
            relation_key_indices,
            key_desc_and_indices,
            headers_index,
            value_desc,
            partition_by,
            topic,
            compression_type,
            progress_group_id,
            transactional_id,
            topic_options,
            topic_metadata_refresh_interval,
        } = self;

        let compatibility_checks = [
            (connection_id == &other.connection_id, "connection_id"),
            (
                connection.alter_compatible(id, &other.connection).is_ok(),
                "connection",
            ),
            (format.alter_compatible(id, &other.format).is_ok(), "format"),
            (
                relation_key_indices == &other.relation_key_indices,
                "relation_key_indices",
            ),
            (
                key_desc_and_indices == &other.key_desc_and_indices,
                "key_desc_and_indices",
            ),
            (headers_index == &other.headers_index, "headers_index"),
            (value_desc == &other.value_desc, "value_desc"),
            (partition_by == &other.partition_by, "partition_by"),
            (topic == &other.topic, "topic"),
            (
                compression_type == &other.compression_type,
                "compression_type",
            ),
            (
                progress_group_id == &other.progress_group_id,
                "progress_group_id",
            ),
            (
                transactional_id == &other.transactional_id,
                "transactional_id",
            ),
            (topic_options == &other.topic_options, "topic_config"),
            (
                topic_metadata_refresh_interval == &other.topic_metadata_refresh_interval,
                "topic_metadata_refresh_interval",
            ),
        ];
        for (compatible, field) in compatibility_checks {
            if !compatible {
                tracing::warn!(
                    "KafkaSinkConnection incompatible at {field}:\nself:\n{:#?}\n\nother\n{:#?}",
                    self,
                    other
                );

                return Err(AlterError { id });
            }
        }

        Ok(())
    }
}

impl<R: ConnectionResolver> IntoInlineConnection<KafkaSinkConnection, R>
    for KafkaSinkConnection<ReferencedConnection>
{
    fn into_inline_connection(self, r: R) -> KafkaSinkConnection {
        let KafkaSinkConnection {
            connection_id,
            connection,
            format,
            relation_key_indices,
            key_desc_and_indices,
            headers_index,
            value_desc,
            partition_by,
            topic,
            compression_type,
            progress_group_id,
            transactional_id,
            topic_options,
            topic_metadata_refresh_interval,
        } = self;
        KafkaSinkConnection {
            connection_id,
            connection: r.resolve_connection(connection).unwrap_kafka(),
            format: format.into_inline_connection(r),
            relation_key_indices,
            key_desc_and_indices,
            headers_index,
            value_desc,
            partition_by,
            topic,
            compression_type,
            progress_group_id,
            transactional_id,
            topic_options,
            topic_metadata_refresh_interval,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum KafkaIdStyle {
    /// A new-style id that is optionally prefixed.
    Prefix(Option<String>),
    /// A legacy style id.
    Legacy,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KafkaSinkFormat<C: ConnectionAccess = InlinedConnection> {
    pub key_format: Option<KafkaSinkFormatType<C>>,
    pub value_format: KafkaSinkFormatType<C>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum KafkaSinkFormatType<C: ConnectionAccess = InlinedConnection> {
    Avro {
        schema: String,
        compatibility_level: Option<mz_ccsr::CompatibilityLevel>,
        csr_connection: C::Csr,
    },
    Json,
    Text,
    Bytes,
}

impl<C: ConnectionAccess> KafkaSinkFormatType<C> {
    pub fn get_format_name(&self) -> &str {
        match self {
            Self::Avro { .. } => "avro",
            Self::Json => "json",
            Self::Text => "text",
            Self::Bytes => "bytes",
        }
    }
}

impl<C: ConnectionAccess> KafkaSinkFormat<C> {
    pub fn get_format_name<'a>(&'a self) -> Cow<'a, str> {
        // For legacy reasons, if the key-format is none or the key & value formats are
        // both the same (either avro or json), we return the value format name,
        // otherwise we return a composite name.
        match &self.key_format {
            None => self.value_format.get_format_name().into(),
            Some(key_format) => match (key_format, &self.value_format) {
                (KafkaSinkFormatType::Avro { .. }, KafkaSinkFormatType::Avro { .. }) => {
                    "avro".into()
                }
                (KafkaSinkFormatType::Json, KafkaSinkFormatType::Json) => "json".into(),
                (keyf, valuef) => format!(
                    "key-{}-value-{}",
                    keyf.get_format_name(),
                    valuef.get_format_name()
                )
                .into(),
            },
        }
    }

    fn alter_compatible(&self, id: GlobalId, other: &Self) -> Result<(), AlterError> {
        if self == other {
            return Ok(());
        }

        match (&self.value_format, &other.value_format) {
            (
                KafkaSinkFormatType::Avro {
                    schema,
                    compatibility_level: _,
                    csr_connection,
                },
                KafkaSinkFormatType::Avro {
                    schema: other_schema,
                    compatibility_level: _,
                    csr_connection: other_csr_connection,
                },
            ) => {
                if schema != other_schema
                    || csr_connection
                        .alter_compatible(id, other_csr_connection)
                        .is_err()
                {
                    tracing::warn!(
                        "KafkaSinkFormat::Avro incompatible at value_format:\nself:\n{:#?}\n\nother\n{:#?}",
                        self,
                        other
                    );

                    return Err(AlterError { id });
                }
            }
            (s, o) => {
                if s != o {
                    tracing::warn!(
                        "KafkaSinkFormat incompatible at value_format:\nself:\n{:#?}\n\nother:{:#?}",
                        s,
                        o
                    );
                    return Err(AlterError { id });
                }
            }
        }

        match (&self.key_format, &other.key_format) {
            (
                Some(KafkaSinkFormatType::Avro {
                    schema,
                    compatibility_level: _,
                    csr_connection,
                }),
                Some(KafkaSinkFormatType::Avro {
                    schema: other_schema,
                    compatibility_level: _,
                    csr_connection: other_csr_connection,
                }),
            ) => {
                if schema != other_schema
                    || csr_connection
                        .alter_compatible(id, other_csr_connection)
                        .is_err()
                {
                    tracing::warn!(
                        "KafkaSinkFormat::Avro incompatible at key_format:\nself:\n{:#?}\n\nother\n{:#?}",
                        self,
                        other
                    );

                    return Err(AlterError { id });
                }
            }
            (s, o) => {
                if s != o {
                    tracing::warn!(
                        "KafkaSinkFormat incompatible at key_format\nself:\n{:#?}\n\nother:{:#?}",
                        s,
                        o
                    );
                    return Err(AlterError { id });
                }
            }
        }

        Ok(())
    }
}

impl<R: ConnectionResolver> IntoInlineConnection<KafkaSinkFormat, R>
    for KafkaSinkFormat<ReferencedConnection>
{
    fn into_inline_connection(self, r: R) -> KafkaSinkFormat {
        KafkaSinkFormat {
            key_format: self.key_format.map(|f| f.into_inline_connection(&r)),
            value_format: self.value_format.into_inline_connection(&r),
        }
    }
}

impl<R: ConnectionResolver> IntoInlineConnection<KafkaSinkFormatType, R>
    for KafkaSinkFormatType<ReferencedConnection>
{
    fn into_inline_connection(self, r: R) -> KafkaSinkFormatType {
        match self {
            KafkaSinkFormatType::Avro {
                schema,
                compatibility_level,
                csr_connection,
            } => KafkaSinkFormatType::Avro {
                schema,
                compatibility_level,
                csr_connection: r.resolve_connection(csr_connection).unwrap_csr(),
            },
            KafkaSinkFormatType::Json => KafkaSinkFormatType::Json,
            KafkaSinkFormatType::Text => KafkaSinkFormatType::Text,
            KafkaSinkFormatType::Bytes => KafkaSinkFormatType::Bytes,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum S3SinkFormat {
    /// Encoded using the PG `COPY` protocol, with one of its supported formats.
    PgCopy(CopyFormatParams<'static>),
    /// Encoded as Parquet.
    Parquet,
}

/// Info required to copy the data to s3.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct S3UploadInfo {
    /// The s3 uri path to write the data to.
    pub uri: String,
    /// The max file size of each file uploaded to S3.
    pub max_file_size: u64,
    /// The relation desc of the data to be uploaded to S3.
    pub desc: RelationDesc,
    /// The selected sink format.
    pub format: S3SinkFormat,
}

pub const MIN_S3_SINK_FILE_SIZE: ByteSize = ByteSize::mb(16);
pub const MAX_S3_SINK_FILE_SIZE: ByteSize = ByteSize::gb(4);

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IcebergSinkConnection<C: ConnectionAccess = InlinedConnection> {
    pub catalog_connection_id: CatalogItemId,
    pub catalog_connection: C::IcebergCatalog,
    pub aws_connection_id: CatalogItemId,
    pub aws_connection: C::Aws,
    /// A natural key of the sinked relation (view or source).
    pub relation_key_indices: Option<Vec<usize>>,
    /// The user-specified key for the sink.
    pub key_desc_and_indices: Option<(RelationDesc, Vec<usize>)>,
    pub namespace: String,
    pub table: String,
}

impl<C: ConnectionAccess> IcebergSinkConnection<C> {
    /// Determines if `self` is compatible with another `StorageSinkConnection`,
    /// in such a way that it is possible to turn `self` into `other` through a
    /// valid series of transformations (e.g. no transformation or `ALTER
    /// CONNECTION`).
    pub fn alter_compatible(&self, id: GlobalId, other: &Self) -> Result<(), AlterError> {
        if self == other {
            return Ok(());
        }
        let IcebergSinkConnection {
            catalog_connection_id: connection_id,
            catalog_connection,
            aws_connection_id,
            aws_connection,
            relation_key_indices,
            key_desc_and_indices,
            namespace,
            table,
        } = self;

        let compatibility_checks = [
            (
                connection_id == &other.catalog_connection_id,
                "connection_id",
            ),
            (
                catalog_connection
                    .alter_compatible(id, &other.catalog_connection)
                    .is_ok(),
                "catalog_connection",
            ),
            (
                aws_connection_id == &other.aws_connection_id,
                "aws_connection_id",
            ),
            (
                aws_connection
                    .alter_compatible(id, &other.aws_connection)
                    .is_ok(),
                "aws_connection",
            ),
            (
                relation_key_indices == &other.relation_key_indices,
                "relation_key_indices",
            ),
            (
                key_desc_and_indices == &other.key_desc_and_indices,
                "key_desc_and_indices",
            ),
            (namespace == &other.namespace, "namespace"),
            (table == &other.table, "table"),
        ];
        for (compatible, field) in compatibility_checks {
            if !compatible {
                tracing::warn!(
                    "IcebergSinkConnection incompatible at {field}:\nself:\n{:#?}\n\nother\n{:#?}",
                    self,
                    other
                );

                return Err(AlterError { id });
            }
        }

        Ok(())
    }
}

impl<R: ConnectionResolver> IntoInlineConnection<IcebergSinkConnection, R>
    for IcebergSinkConnection<ReferencedConnection>
{
    fn into_inline_connection(self, r: R) -> IcebergSinkConnection {
        let IcebergSinkConnection {
            catalog_connection_id,
            catalog_connection,
            aws_connection_id,
            aws_connection,
            relation_key_indices,
            key_desc_and_indices,
            namespace,
            table,
        } = self;
        IcebergSinkConnection {
            catalog_connection_id,
            catalog_connection: r
                .resolve_connection(catalog_connection)
                .unwrap_iceberg_catalog(),
            aws_connection_id,
            aws_connection: r.resolve_connection(aws_connection).unwrap_aws(),
            relation_key_indices,
            key_desc_and_indices,
            namespace,
            table,
        }
    }
}
