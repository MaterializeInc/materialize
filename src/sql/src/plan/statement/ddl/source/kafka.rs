// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Planning functions related to running Kafka
//!  ingestions.
//!
use std::collections::BTreeMap;
use std::time::Duration;

use mz_repr::RelationDesc;
use mz_sql_parser::ast::{self, CreateSourceConnection, SourceIncludeMetadata};
use mz_storage_types::connections::inline::ReferencedConnection;
use mz_storage_types::connections::Connection;
use mz_storage_types::sources::kafka::{KafkaMetadataKind, KafkaSourceConnection};
use mz_storage_types::sources::{GenericSourceConnection, SourceDesc};

use crate::kafka_util::KafkaSourceConfigOptionExtracted;
use crate::plan::error::PlanError;
use crate::plan::statement::StatementContext;

pub(super) fn plan_create_source_desc_kafka(
    scx: &StatementContext,
    stmt: super::CreateSourceDescFields,
) -> Result<(SourceDesc<ReferencedConnection>, RelationDesc), PlanError> {
    let super::CreateSourceDescFields {
        connection,
        envelope,
        format,
        include_metadata,
    } = stmt;

    let envelope = envelope.clone().unwrap_or(ast::SourceEnvelope::None);

    let CreateSourceConnection::Kafka {
        connection: connection_name,
        options,
    } = connection
    else {
        panic!("must be Kafka connection")
    };

    let connection_item = scx.get_item_by_resolved_name(connection_name)?;
    if !matches!(connection_item.connection()?, Connection::Kafka(_)) {
        sql_bail!(
            "{} is not a kafka connection",
            scx.catalog.resolve_full_name(connection_item.name())
        )
    }

    let KafkaSourceConfigOptionExtracted {
        group_id_prefix,
        topic,
        topic_metadata_refresh_interval,
        start_timestamp: _, // purified into `start_offset`
        start_offset,
        seen: _,
    }: KafkaSourceConfigOptionExtracted = options.clone().try_into()?;

    let topic = topic.expect("validated exists during purification");

    let mut start_offsets = BTreeMap::new();
    if let Some(offsets) = start_offset {
        for (part, offset) in offsets.iter().enumerate() {
            if *offset < 0 {
                sql_bail!("START OFFSET must be a nonnegative integer");
            }
            start_offsets.insert(i32::try_from(part)?, *offset);
        }
    }

    if !start_offsets.is_empty() && envelope.requires_all_input() {
        sql_bail!("START OFFSET is not supported with ENVELOPE {}", envelope)
    }

    if topic_metadata_refresh_interval > Duration::from_secs(60 * 60) {
        // This is a librdkafka-enforced restriction that, if violated,
        // would result in a runtime error for the source.
        sql_bail!("TOPIC METADATA REFRESH INTERVAL cannot be greater than 1 hour");
    }

    if !include_metadata.is_empty()
        && !matches!(
            envelope,
            ast::SourceEnvelope::Upsert | ast::SourceEnvelope::None | ast::SourceEnvelope::Debezium
        )
    {
        // TODO(guswynn): should this be `bail_unsupported!`?
        sql_bail!("INCLUDE <metadata> requires ENVELOPE (NONE|UPSERT|DEBEZIUM)");
    }

    let metadata_columns = include_metadata
        .into_iter()
        .flat_map(|item| match item {
            SourceIncludeMetadata::Timestamp { alias } => {
                let name = match alias {
                    Some(name) => name.to_string(),
                    None => "timestamp".to_owned(),
                };
                Some((name, KafkaMetadataKind::Timestamp))
            }
            SourceIncludeMetadata::Partition { alias } => {
                let name = match alias {
                    Some(name) => name.to_string(),
                    None => "partition".to_owned(),
                };
                Some((name, KafkaMetadataKind::Partition))
            }
            SourceIncludeMetadata::Offset { alias } => {
                let name = match alias {
                    Some(name) => name.to_string(),
                    None => "offset".to_owned(),
                };
                Some((name, KafkaMetadataKind::Offset))
            }
            SourceIncludeMetadata::Headers { alias } => {
                let name = match alias {
                    Some(name) => name.to_string(),
                    None => "headers".to_owned(),
                };
                Some((name, KafkaMetadataKind::Headers))
            }
            SourceIncludeMetadata::Header {
                alias,
                key,
                use_bytes,
            } => Some((
                alias.to_string(),
                KafkaMetadataKind::Header {
                    key: key.clone(),
                    use_bytes: *use_bytes,
                },
            )),
            SourceIncludeMetadata::Key { .. } => {
                // handled below
                None
            }
        })
        .collect();

    let connection = KafkaSourceConnection::<ReferencedConnection> {
        connection: connection_item.id(),
        connection_id: connection_item.id(),
        topic,
        start_offsets,
        group_id_prefix,
        topic_metadata_refresh_interval,
        metadata_columns,
    };

    let external_connection = GenericSourceConnection::Kafka(connection);

    let (encoding, envelope, topic_desc) = super::plan_encoding_envelope(
        scx,
        &external_connection,
        format,
        &envelope,
        include_metadata,
    )?;

    let source_desc = SourceDesc::<ReferencedConnection> {
        connection: external_connection,
        encoding,
        envelope,
    };

    Ok((source_desc, topic_desc))
}
