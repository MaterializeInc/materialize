// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use mz_dataflow_types::sources::MaybeStringId;
use mz_repr::GlobalId;
use mz_sql_parser::ast::{
    AstInfo, AvroSchema, CreateSourceConnector, CreateSourceFormat, CreateSourceStatement,
    CsrConnector, CsrConnectorAvro, CsrConnectorProto, Format, KafkaConnector,
    KafkaSourceConnector, ProtobufSchema, UnresolvedObjectName, WithOption,
};

use crate::catalog::SessionCatalog;
use crate::normalize::{self, SqlMaybeValueId};
use crate::plan::{PlanError, StatementContext};

/// Uses the provided catalog to populate all Connector references with the values of the connector
/// it references allowing it to be used as if there was no indirection
pub fn populate_connectors<T: AstInfo>(
    mut stmt: CreateSourceStatement<T>,
    catalog: &dyn SessionCatalog,
    depends_on: &mut Vec<GlobalId>,
    secrets_reader: (),
) -> Result<CreateSourceStatement<T>, anyhow::Error> {
    if let CreateSourceStatement {
        connector:
            CreateSourceConnector::Kafka(
                KafkaSourceConnector {
                    connector: kafka_connector,
                    ..
                },
                ..,
            ),
        with_options: stmt_with_options,
        ..
    } = &mut stmt
    {
        match kafka_connector {
            KafkaConnector::Reference {
                connector: name, ..
            } => {
                let p_o_name = normalize::unresolved_object_name(name.clone())?;
                let conn = catalog.resolve_item(&p_o_name)?;
                let resolved_source_connector = conn.catalog_connector()?;
                depends_on.push(conn.id());
                *kafka_connector = KafkaConnector::Reference {
                    connector: name.to_owned(),
                    broker: Some(resolved_source_connector.uri()),
                    with_options: Some(resolved_source_connector.options(secrets_reader)),
                };
            }
            KafkaConnector::Inline {
                broker: _,
                with_options,
            } => {
                let new_options = crate::kafka_util::read_secrets_config(
                    crate::kafka_util::extract_config(&mut normalize::options_catalog(
                        stmt_with_options,
                        catalog,
                    )?)?,
                    secrets_reader,
                )?;
                *with_options = Some(new_options);
            }
        };
    };

    if let CreateSourceStatement {
        format: CreateSourceFormat::Bare(ref mut format),
        ref mut with_options,
        ..
    } = stmt
    {
        populate_connector_for_format(format, catalog, depends_on, with_options, secrets_reader)?;
        return Ok(stmt);
    };
    if let CreateSourceStatement {
        format:
            CreateSourceFormat::KeyValue {
                ref mut key,
                ref mut value,
            },
        ref mut with_options,
        ..
    } = stmt
    {
        populate_connector_for_format(key, catalog, depends_on, with_options, secrets_reader)?;
        populate_connector_for_format(value, catalog, depends_on, with_options, secrets_reader)?;
    };
    Ok(stmt)
}

/// Helper function which reifies any connectors in a single [`Format`]
fn populate_connector_for_format<T: AstInfo>(
    format: &mut Format<T>,
    catalog: &dyn SessionCatalog,
    depends_on: &mut Vec<GlobalId>,
    stmt_with_options: &mut Vec<WithOption<T>>,
    secrets_reader: (),
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
                match csr_connector {
                    CsrConnector::Reference {
                        connector: name, ..
                    } => {
                        *csr_connector = populate_csr_connector_reference(
                            name,
                            catalog,
                            depends_on,
                            secrets_reader,
                        )?;
                    }
                    CsrConnector::Inline { with_options, .. } => {
                        *with_options = Some(generate_csr_connector_inline(
                            stmt_with_options,
                            catalog,
                            secrets_reader,
                        )?)
                    }
                };
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
                match csr_connector {
                    CsrConnector::Reference {
                        connector: name, ..
                    } => {
                        *csr_connector = populate_csr_connector_reference(
                            name,
                            catalog,
                            depends_on,
                            secrets_reader,
                        )?;
                    }
                    CsrConnector::Inline { with_options, .. } => {
                        *with_options = Some(generate_csr_connector_inline(
                            stmt_with_options,
                            catalog,
                            secrets_reader,
                        )?)
                    }
                };
            }
            _ => {}
        },
        _ => {}
    })
}

/// Helper function which reifies individual [`CsrConnector::Reference`] instances
fn populate_csr_connector_reference(
    name: &UnresolvedObjectName,
    catalog: &dyn SessionCatalog,
    depends_on: &mut Vec<GlobalId>,
    secrets_reader: (),
) -> Result<CsrConnector, anyhow::Error> {
    let p_o_name = normalize::unresolved_object_name(name.clone())?;
    let conn = catalog.resolve_item(&p_o_name)?;
    let resolved_csr_connector = conn.catalog_connector()?;
    depends_on.push(conn.id());
    Ok(CsrConnector::Reference {
        connector: name.to_owned(),
        url: Some(resolved_csr_connector.uri()),
        with_options: Some(resolved_csr_connector.options(secrets_reader)),
    })
}

fn generate_csr_connector_inline<T: AstInfo>(
    with_options: &mut Vec<WithOption<T>>,
    catalog: &dyn SessionCatalog,
    secrets_reader: (),
) -> Result<BTreeMap<String, String>, anyhow::Error> {
    crate::kafka_util::read_secrets_config(
        normalize::options_catalog(with_options, catalog)?
            .into_iter()
            // XXX(chae): this probably isn't right?
            .map(|(k, v)| match v {
                SqlMaybeValueId::Value(v) => (k, MaybeStringId::Value(v.to_string())),
                SqlMaybeValueId::Secret(id) => (k, MaybeStringId::Secret(id)),
            })
            .collect::<BTreeMap<_, _>>(),
        secrets_reader,
    )
}

/// Turn all [`UnresolvedObjectName`]s in [`CsrConnector::Reference`]s within the statement into fully qualified names
/// so that they can be persisted in the catalog safely
pub fn qualify_csr_connector_names<T: AstInfo>(
    format: &mut CreateSourceFormat<T>,
    scx: &StatementContext,
) -> Result<(), anyhow::Error> {
    match format {
        CreateSourceFormat::None => {}
        CreateSourceFormat::Bare(fmt) => qualify_connector_in_format(fmt, scx)?,
        CreateSourceFormat::KeyValue { key, value } => {
            qualify_connector_in_format(key, scx)?;
            qualify_connector_in_format(value, scx)?;
        }
    }

    Ok(())
}

/// Helper function to resolve names for connectors in a single [`Format`]
fn qualify_connector_in_format<T: AstInfo>(
    format: &mut Format<T>,
    scx: &StatementContext,
) -> Result<(), anyhow::Error> {
    let allocate_name = |name: &UnresolvedObjectName| -> Result<_, PlanError> {
        Ok(normalize::unresolve(scx.allocate_full_name(
            normalize::unresolved_object_name(name.clone())?,
        )?))
    };
    match format {
        Format::Avro(avro_schema) => match avro_schema {
            AvroSchema::Csr {
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
        Format::Protobuf(proto_schema) => match proto_schema {
            ProtobufSchema::Csr {
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
