// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use lazy_static::lazy_static;
use mz_repr::GlobalId;
use mz_secrets::SecretsReader;
use mz_secrets_filesystem::FilesystemSecretsController;
use mz_sql_parser::ast::{
    AstInfo, AvroSchema, CreateSourceConnector, CreateSourceFormat, CreateSourceStatement,
    CsrConnector, CsrConnectorAvro, CsrConnectorProto, Format, KafkaConnector,
    KafkaSourceConnector, ProtobufSchema, UnresolvedObjectName,
};

use crate::catalog::SessionCatalog;
use crate::normalize;
use crate::plan::{PlanError, StatementContext};

lazy_static! {
    static ref SECRETS: Arc<Box<dyn SecretsReader>> = default_secrets_reader();
}

fn default_secrets_reader() -> Arc<Box<dyn SecretsReader>> {
    // This is a clumsy way of checking but based on the structop configuration in materialized/main.rs this looks accurate for detecting being in k8s
    match env::var("MZ_POD_NAME") {
        Ok(_) => {
            // A Kubernetes compatible SecretsReader implementer should go here
            unreachable!()
        }
        Err(_) => {
            let reader: Arc<Box<dyn SecretsReader>> = Arc::new(Box::new(
                FilesystemSecretsController::new(match env::var("MZ_DATA_DIRECTORY") {
                    Ok(d) => PathBuf::new().join(d),
                    Err(_) => PathBuf::new().join("/share/mzdata/secrets"),
                }),
            ));
            reader
        }
    }
}

/// Uses the provided catalog to populate all Connector references with the values of the connector
/// it references allowing it to be used as if there was no indirection
pub fn populate_connectors<T: AstInfo>(
    stmt: CreateSourceStatement<T>,
    catalog: &dyn SessionCatalog,
    depends_on: &mut Vec<GlobalId>,
    secrets: Option<Arc<Box<dyn SecretsReader>>>,
) -> Result<CreateSourceStatement<T>, anyhow::Error> {
    let secrets_reader = if let Some(secret_reader) = secrets {
        secret_reader
    } else {
        Arc::clone(&*SECRETS)
    };
    populate_connectors_with_secrets(stmt, catalog, depends_on, secrets_reader)
}

/// Same as populate_connectors except it converts secret fields into either paths or string values as appropriate using the supplied SecretsReader
pub fn populate_connectors_with_secrets<T: AstInfo>(
    mut stmt: CreateSourceStatement<T>,
    catalog: &dyn SessionCatalog,
    depends_on: &mut Vec<GlobalId>,
    secrets_reader: Arc<Box<dyn SecretsReader>>,
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
        let p_o_name = normalize::unresolved_object_name(name.clone())?;
        let conn = catalog.resolve_item(&p_o_name)?;
        let resolved_source_connector = conn.catalog_connector()?;
        depends_on.push(conn.id());
        *kafka_connector = KafkaConnector::Reference {
            connector: name.to_owned(),
            broker: Some(resolved_source_connector.uri()),
            with_options: Some(resolved_source_connector.options(Arc::clone(&secrets_reader))?),
        };
    };

    if let CreateSourceStatement {
        format: CreateSourceFormat::Bare(ref mut format),
        ..
    } = stmt
    {
        populate_connector_for_format_with_secrets(
            format,
            catalog,
            depends_on,
            Arc::clone(&secrets_reader),
        )?;
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
        populate_connector_for_format_with_secrets(
            key,
            catalog,
            depends_on,
            Arc::clone(&secrets_reader),
        )?;
        populate_connector_for_format_with_secrets(
            value,
            catalog,
            depends_on,
            Arc::clone(&secrets_reader),
        )?;
    };
    Ok(stmt)
}

/// Helper function which populates any connectors in a single [`Format`]
fn populate_connector_for_format_with_secrets<T: AstInfo>(
    format: &mut Format<T>,
    catalog: &dyn SessionCatalog,
    depends_on: &mut Vec<GlobalId>,
    secrets_reader: Arc<Box<dyn SecretsReader>>,
) -> Result<(), anyhow::Error> {
    Ok(match format {
        Format::Avro(AvroSchema::Csr {
            csr_connector:
                CsrConnectorAvro {
                    connector: connector @ CsrConnector::Reference { .. },
                    ..
                },
        }) => {
            *connector = match connector {
                CsrConnector::Inline { .. } => unreachable!(),
                CsrConnector::Reference { connector, .. } => populate_csr_connector_with_secrets(
                    connector,
                    catalog,
                    depends_on,
                    Arc::clone(&secrets_reader),
                )?,
            }
        }
        Format::Protobuf(ProtobufSchema::Csr {
            csr_connector: CsrConnectorProto { connector, .. },
        }) => {
            let name = match connector {
                CsrConnector::Reference { connector, .. } => connector,
                _ => unreachable!(),
            };
            *connector = populate_csr_connector_with_secrets(
                &name,
                catalog,
                depends_on,
                Arc::clone(&secrets_reader),
            )?;
        }
        _ => {}
    })
}

/// Helper function which populates individual [`CsrConnector::Reference`] instances
fn populate_csr_connector_with_secrets(
    name: &UnresolvedObjectName,
    catalog: &dyn SessionCatalog,
    depends_on: &mut Vec<GlobalId>,
    secrets_reader: Arc<Box<dyn SecretsReader>>,
) -> Result<CsrConnector, anyhow::Error> {
    let p_o_name = normalize::unresolved_object_name(name.clone())?;
    let conn = catalog.resolve_item(&p_o_name)?;
    let resolved_csr_connector = conn.catalog_connector()?;
    depends_on.push(conn.id());
    Ok(CsrConnector::Reference {
        connector: name.to_owned(),
        url: Some(resolved_csr_connector.uri()),
        with_options: Some(resolved_csr_connector.options(secrets_reader)?),
    })
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
