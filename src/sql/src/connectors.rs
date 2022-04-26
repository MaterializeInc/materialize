// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_repr::GlobalId;
use mz_sql_parser::ast::{
    AstInfo, AvroSchema, CreateSourceConnector, CreateSourceFormat, CreateSourceStatement,
    CsrConnector, CsrConnectorAvro, CsrConnectorProto, Format, KafkaConnector,
    KafkaSourceConnector, ProtobufSchema, UnresolvedObjectName,
};

use crate::normalize::unresolve;
use crate::normalize::unresolved_object_name;
use crate::plan::PlanError;
use crate::plan::StatementContext;
use crate::{catalog::SessionCatalog, normalize};

/// Uses the provided catalog to populate all Connector references with the values of the connector
/// it references allowing it to be used as if there was no indirection
pub fn reify_connectors<T: AstInfo>(
    mut stmt: CreateSourceStatement<T>,
    catalog: &dyn SessionCatalog,
    depends_on: &mut Vec<GlobalId>,
) -> Result<CreateSourceStatement<T>, anyhow::Error> {
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
        let resolved_source_connector = normalize::unresolved_object_name(name.clone())
            .map_err(anyhow::Error::new)
            .and_then(|name| catalog.resolve_item(&name).map_err(anyhow::Error::new))
            .and_then(|conn| {
                let ctr = conn.catalog_connector()?;
                depends_on.push(conn.id());
                Ok(ctr)
            })?;
        *kafka_connector = KafkaConnector::Reference {
            connector: name.to_owned(),
            broker: Some(resolved_source_connector.uri()),
            with_options: Some(
                resolved_source_connector
                    .options()
                    .iter()
                    .flat_map(|(k, v)| vec![k.to_owned(), v.to_owned()])
                    .collect::<Vec<String>>(),
            ),
        };
    };

    if let CreateSourceStatement {
        format: CreateSourceFormat::Bare(ref mut format),
        ..
    } = stmt
    {
        reify_connector_for_format(format, catalog, depends_on)?;
        return Ok(stmt);
    };
    if let CreateSourceStatement {
        format:
            CreateSourceFormat::KeyValue {
                ref mut key,
                ref mut value,
            },
        ..
    } = stmt
    {
        reify_connector_for_format(key, catalog, depends_on)?;
        reify_connector_for_format(value, catalog, depends_on)?;
    };
    Ok(stmt)
}

/// Helper function which reifies any connectors in a single [`Format`]
fn reify_connector_for_format<T: AstInfo>(
    format: &mut Format<T>,
    catalog: &dyn SessionCatalog,
    depends_on: &mut Vec<GlobalId>,
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
                *csr_connector = reify_csr_connector(name, catalog, depends_on)?;
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
                *csr_connector = reify_csr_connector(name, catalog, depends_on)?;
            }
            _ => {}
        },
        _ => {}
    })
}

/// Helper function which reifies individual [`CsrConnector::Reference`] instances
fn reify_csr_connector(
    name: &UnresolvedObjectName,
    catalog: &dyn SessionCatalog,
    depends_on: &mut Vec<GlobalId>,
) -> Result<CsrConnector, anyhow::Error> {
    let resolved_avro_connector = normalize::unresolved_object_name(name.clone())
        .map_err(anyhow::Error::new)
        .and_then(|name| catalog.resolve_item(&name).map_err(anyhow::Error::new))
        .and_then(|conn| {
            depends_on.push(conn.id());
            let ctr = conn.catalog_connector()?;
            Ok(ctr)
        })?;
    Ok(CsrConnector::Reference {
        connector: name.to_owned(),
        url: Some(resolved_avro_connector.uri()),
        with_options: Some(
            resolved_avro_connector
                .options()
                .iter()
                .flat_map(|(k, v)| [k.to_owned(), v.to_owned()])
                .collect::<Vec<String>>(),
        ),
    })
}

/// Turn all [`UnresolvedObjectName`]s in [`CsrConnector::Reference`]s within the statement into fully qualified names
/// so that they can be persisted in the catalog safely
pub fn qualify_csr_connector_names<T: AstInfo>(
    format: &mut CreateSourceFormat<T>,
    scx: &StatementContext,
) -> Result<(), anyhow::Error> {
    match format {
        mz_sql_parser::ast::CreateSourceFormat::None => {}
        mz_sql_parser::ast::CreateSourceFormat::Bare(fmt) => {
            qualify_connector_in_single_schema(fmt, scx)?
        }
        mz_sql_parser::ast::CreateSourceFormat::KeyValue { key, value } => {
            qualify_connector_in_single_schema(key, scx)?;
            qualify_connector_in_single_schema(value, scx)?;
        }
    }

    Ok(())
}

/// Helper function to resolve names for connectors in a single [`Format`]
fn qualify_connector_in_single_schema<T: AstInfo>(
    schema: &mut Format<T>,
    scx: &StatementContext,
) -> Result<(), anyhow::Error> {
    let allocate_name = |name: &UnresolvedObjectName| -> Result<_, PlanError> {
        Ok(unresolve(scx.allocate_full_name(
            unresolved_object_name(name.clone())?,
        )?))
    };
    match schema {
        mz_sql_parser::ast::Format::Avro(avro_schema) => match avro_schema {
            mz_sql_parser::ast::AvroSchema::Csr {
                csr_connector:
                    CsrConnectorAvro {
                        connector: CsrConnector::Reference { connector, .. },
                        ..
                    },
            } => {
                *connector = allocate_name(connector)?;
            }
            _ => {}
        },
        mz_sql_parser::ast::Format::Protobuf(proto_schema) => match proto_schema {
            mz_sql_parser::ast::ProtobufSchema::Csr {
                csr_connector:
                    CsrConnectorProto {
                        connector: CsrConnector::Reference { connector, .. },
                        ..
                    },
            } => {
                *connector = allocate_name(connector)?;
            }
            _ => {}
        },
        _ => {}
    }
    Ok(())
}
