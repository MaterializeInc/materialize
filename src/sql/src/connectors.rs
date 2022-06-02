// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_sql_parser::ast::{
    AvroSchema, CreateSourceConnector, CreateSourceFormat, CreateSourceStatement, CsrConnector,
    CsrConnectorAvro, CsrConnectorProto, Format, KafkaConnector, KafkaSourceConnector,
    ProtobufSchema, Raw, RawObjectName,
};

use crate::catalog::SessionCatalog;
use crate::names;
use crate::plan::StatementContext;

/// Uses the provided catalog to populate all Connector references with the values of the connector
/// it references allowing it to be used as if there was no indirection
pub fn populate_connectors(
    mut stmt: CreateSourceStatement<Raw>,
    catalog: &dyn SessionCatalog,
) -> Result<CreateSourceStatement<Raw>, anyhow::Error> {
    let scx = StatementContext::new(None, catalog);

    if let CreateSourceStatement {
        connector:
            CreateSourceConnector::Kafka(
                KafkaSourceConnector {
                    connector: kafka_connector @ KafkaConnector::Reference { .. },
                    ..
                },
                ..,
            ),
        ..
    } = &mut stmt
    {
        let name = match kafka_connector {
            KafkaConnector::Reference { connector, .. } => connector,
            _ => unreachable!(),
        };
        let resolved_name = names::resolve_object_name(&scx, name.clone())?;
        let conn = scx.get_item_by_resolved_name(&resolved_name)?;
        let resolved_source_connector = conn.catalog_connector()?;
        *kafka_connector = KafkaConnector::Reference {
            connector: name.to_owned(),
            broker: Some(resolved_source_connector.uri()),
            with_options: Some(resolved_source_connector.options()),
        };
    };

    if let CreateSourceStatement {
        format: CreateSourceFormat::Bare(format),
        ..
    } = &mut stmt
    {
        populate_connector_for_format(&scx, format)?;
        return Ok(stmt);
    };
    if let CreateSourceStatement {
        format: CreateSourceFormat::KeyValue { key, value },
        ..
    } = &mut stmt
    {
        populate_connector_for_format(&scx, key)?;
        populate_connector_for_format(&scx, value)?;
    };
    Ok(stmt)
}

/// Helper function which reifies any connectors in a single [`Format`]
fn populate_connector_for_format(
    scx: &StatementContext,
    format: &mut Format<Raw>,
) -> Result<(), anyhow::Error> {
    Ok(match format {
        Format::Avro(avro_schema) => match avro_schema {
            AvroSchema::Csr {
                csr_connector:
                    CsrConnectorAvro {
                        connector: csr_connector @ CsrConnector::Reference { .. },
                        ..
                    },
            } => {
                let name = match csr_connector {
                    CsrConnector::Reference { connector, .. } => connector,
                    _ => unreachable!(),
                };
                *csr_connector = populate_csr_connector(scx, name.clone())?;
            }
            _ => {}
        },
        Format::Protobuf(proto_schema) => match proto_schema {
            ProtobufSchema::Csr {
                csr_connector:
                    CsrConnectorProto {
                        connector: csr_connector @ CsrConnector::Reference { .. },
                        ..
                    },
            } => {
                let name = match csr_connector {
                    CsrConnector::Reference { connector, .. } => connector,
                    _ => unreachable!(),
                };
                *csr_connector = populate_csr_connector(scx, name.clone())?;
            }
            _ => {}
        },
        _ => {}
    })
}

/// Helper function which reifies individual [`CsrConnector::Reference`] instances
fn populate_csr_connector(
    scx: &StatementContext,
    name: RawObjectName,
) -> Result<CsrConnector<Raw>, anyhow::Error> {
    let resolved_name = names::resolve_object_name(&scx, name.clone())?;
    let conn = scx.get_item_by_resolved_name(&resolved_name)?;
    let resolved_csr_connector = conn.catalog_connector()?;
    Ok(CsrConnector::Reference {
        connector: name,
        url: Some(resolved_csr_connector.uri()),
        with_options: Some(resolved_csr_connector.options()),
    })
}
