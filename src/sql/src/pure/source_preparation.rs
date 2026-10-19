// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use mz_storage_types::configuration::StorageConfiguration;

use super::{
    AlterSourcePreparation, CreateSourcePreparation, CsrAvroPreparation, CsrProtobufPreparation,
    GlueAvroPreparation, PreparedCreateSource, PurifiedSourceExport, PurifiedStatement,
    SourceFormatOptions, TableFromSourcePreparation, execute_alter_source_preparation,
    execute_create_source_preparation, execute_table_from_source_preparation,
    finish_table_from_source, prepare_alter_source, prepare_csr_connection_avro,
    prepare_csr_connection_proto, prepare_glue_connection_avro, prepare_table_from_source,
};
use crate::ast::{
    AvroSchema, CsrSeedAvro, CsrSeedProtobuf, Format, FormatSpecifier, GlueAvroSeed,
    ProtobufSchema, SourceEnvelope, Statement,
};
use crate::catalog::SessionCatalog;
use crate::names::Aug;
use crate::plan::PlanError;

/// Statement-local external results, reusable only for matching current requests.
#[derive(Clone, Debug)]
pub struct SourcePreparation {
    now: u64,
    clock_as_of: OnceLock<u64>,
    inputs: BTreeMap<usize, PreparedSourceInput>,
}

/// The result of resolving source preparation against a catalog.
#[derive(Debug)]
pub enum SourcePreparationStatus {
    Ready(Box<PurifiedStatement>),
    Pending(SourcePreparationRequest),
}

/// An external request derived from the current catalog and submitted statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourcePreparationRequest {
    position: usize,
    kind: RequestKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum RequestKind {
    Alter(Box<AlterSourcePreparation>),
    Source(Box<CreateSourcePreparation>),
    Table(Box<TableFromSourcePreparation>),
    Protobuf(Box<CsrProtobufPreparation>),
    Avro(Box<CsrAvroPreparation>),
    Glue(Box<GlueAvroPreparation>),
}

/// An external result paired with the exact request that produced it.
#[derive(Clone, Debug)]
struct PreparedSourceInput {
    request: SourcePreparationRequest,
    value: Arc<PreparationValue>,
}

/// An external completion whose success or failure must be checked against current inputs.
#[derive(Debug)]
#[must_use = "pass completions to SourcePreparation::finish to validate their inputs"]
pub struct SourcePreparationResponse {
    request: SourcePreparationRequest,
    result: Result<Arc<PreparationValue>, PlanError>,
}

#[derive(Debug)]
enum PreparationValue {
    Alter(Box<PurifiedStatement>),
    Source(Box<PreparedCreateSource>),
    Table(Box<(PurifiedSourceExport, SourceFormatOptions)>),
    Protobuf(CsrSeedProtobuf),
    Avro(CsrSeedAvro),
    Glue(GlueAvroSeed),
}

enum ResolveError {
    Plan(Box<PlanError>),
    Pending(SourcePreparationRequest),
}

impl From<PlanError> for ResolveError {
    fn from(error: PlanError) -> Self {
        Self::Plan(Box::new(error))
    }
}

impl SourcePreparation {
    pub fn new(now: u64) -> Self {
        Self {
            now,
            clock_as_of: OnceLock::new(),
            inputs: BTreeMap::new(),
        }
    }

    /// Recompute catalog decisions, requesting external work when its inputs do not match.
    pub fn resolve(
        &self,
        catalog: &dyn SessionCatalog,
        statement: Statement<Aug>,
    ) -> Result<SourcePreparationStatus, PlanError> {
        let mut cursor = Cursor {
            preparation: self,
            next: 0,
        };
        match cursor.resolve(catalog, statement) {
            Ok(statement) => Ok(SourcePreparationStatus::Ready(Box::new(statement))),
            Err(ResolveError::Pending(request)) => Ok(SourcePreparationStatus::Pending(request)),
            Err(ResolveError::Plan(error)) => Err(*error),
        }
    }

    /// Accept completed work, reporting a failure only if its request is still required.
    pub fn finish(
        &mut self,
        catalog: &dyn SessionCatalog,
        statement: Statement<Aug>,
        response: SourcePreparationResponse,
    ) -> Result<SourcePreparationStatus, PlanError> {
        match response.result {
            Ok(value) => {
                // A changed earlier request can change the requests that follow it.
                self.inputs
                    .retain(|position, _| *position < response.request.position);
                self.inputs.insert(
                    response.request.position,
                    PreparedSourceInput {
                        request: response.request,
                        value,
                    },
                );
                self.resolve(catalog, statement)
            }
            Err(error) => {
                let status = self.resolve(catalog, statement)?;
                match status {
                    SourcePreparationStatus::Pending(request) if request == response.request => {
                        Err(error)
                    }
                    status => Ok(status),
                }
            }
        }
    }
}

impl SourcePreparationRequest {
    /// Execute external reads without access to the catalog.
    pub async fn execute(
        &self,
        storage_configuration: &StorageConfiguration,
    ) -> SourcePreparationResponse {
        SourcePreparationResponse {
            request: self.clone(),
            result: self.execute_inner(storage_configuration).await,
        }
    }

    async fn execute_inner(
        &self,
        storage_configuration: &StorageConfiguration,
    ) -> Result<Arc<PreparationValue>, PlanError> {
        let value = match &self.kind {
            RequestKind::Alter(request) => PreparationValue::Alter(Box::new(
                execute_alter_source_preparation((**request).clone(), storage_configuration)
                    .await?,
            )),
            RequestKind::Source(request) => PreparationValue::Source(Box::new(
                execute_create_source_preparation((**request).clone(), storage_configuration)
                    .await?,
            )),
            RequestKind::Table(request) => PreparationValue::Table(Box::new(
                execute_table_from_source_preparation((**request).clone(), storage_configuration)
                    .await?,
            )),
            RequestKind::Protobuf(request) => PreparationValue::Protobuf(
                (**request).clone().execute(storage_configuration).await?,
            ),
            RequestKind::Avro(request) => {
                PreparationValue::Avro((**request).clone().execute(storage_configuration).await?)
            }
            RequestKind::Glue(request) => {
                PreparationValue::Glue((**request).clone().execute(storage_configuration).await?)
            }
        };
        Ok(Arc::new(value))
    }
}

struct Cursor<'a> {
    preparation: &'a SourcePreparation,
    next: usize,
}

impl<'a> Cursor<'a> {
    fn request(&mut self, kind: RequestKind) -> Result<&'a PreparationValue, ResolveError> {
        let request = SourcePreparationRequest {
            position: self.next,
            kind,
        };
        self.next += 1;
        match self.preparation.inputs.get(&request.position) {
            Some(input) if input.request == request => Ok(input.value.as_ref()),
            _ => Err(ResolveError::Pending(request)),
        }
    }

    fn resolve(
        &mut self,
        catalog: &dyn SessionCatalog,
        statement: Statement<Aug>,
    ) -> Result<PurifiedStatement, ResolveError> {
        match statement {
            Statement::AlterSource(statement) => {
                let Some(request) = prepare_alter_source(catalog, &statement)? else {
                    return Ok(PurifiedStatement::PurifiedAlterSource {
                        alter_source_stmt: statement,
                    });
                };
                let PreparationValue::Alter(prepared) =
                    self.request(RequestKind::Alter(Box::new(request)))?
                else {
                    unreachable!("the result kind is determined by its request");
                };
                Ok((**prepared).clone())
            }
            Statement::CreateSource(statement) => {
                let request = CreateSourcePreparation::new(
                    catalog,
                    self.preparation.now,
                    || *self.preparation.clock_as_of.get_or_init(|| catalog.now()),
                    statement,
                )?;
                let PreparationValue::Source(prepared) =
                    self.request(RequestKind::Source(Box::new(request)))?
                else {
                    unreachable!("the result kind is determined by its request")
                };
                let PreparedCreateSource {
                    create_progress_subsource,
                    mut create_source_stmt,
                    subsources,
                    available_source_references,
                    format_options,
                } = (**prepared).clone();
                self.format(
                    catalog,
                    &mut create_source_stmt.format,
                    &format_options,
                    &create_source_stmt.envelope,
                )?;
                Ok(PurifiedStatement::PurifiedCreateSource {
                    create_progress_subsource,
                    create_source_stmt,
                    subsources,
                    available_source_references,
                })
            }
            Statement::CreateTableFromSource(mut statement) => {
                let request = prepare_table_from_source(catalog, &statement)?;
                let PreparationValue::Table(prepared) =
                    self.request(RequestKind::Table(Box::new(request)))?
                else {
                    unreachable!("the result kind is determined by its request")
                };
                let (export, options) = (**prepared).clone();
                self.format(
                    catalog,
                    &mut statement.format,
                    &options,
                    &statement.envelope,
                )?;
                Ok(finish_table_from_source(catalog, statement, export)?)
            }
            _ => Err(internal_err!(
                "source preparation requires CREATE SOURCE or CREATE TABLE FROM SOURCE"
            )
            .into()),
        }
    }

    fn format(
        &mut self,
        catalog: &dyn SessionCatalog,
        format: &mut Option<FormatSpecifier<Aug>>,
        options: &SourceFormatOptions,
        envelope: &Option<SourceEnvelope>,
    ) -> Result<(), ResolveError> {
        if matches!(format, Some(FormatSpecifier::KeyValue { .. }))
            && !matches!(options, SourceFormatOptions::Kafka { .. })
        {
            return Err(sql_err!(
                "Kafka sources are the only source type that can provide KEY/VALUE formats"
            )
            .into());
        }
        match format {
            None => {}
            Some(FormatSpecifier::Bare(format)) => {
                self.single_format(catalog, format, options, envelope)?;
            }
            Some(FormatSpecifier::KeyValue { key, value }) => {
                self.single_format(catalog, key, options, envelope)?;
                self.single_format(catalog, value, options, envelope)?;
            }
        }
        Ok(())
    }

    fn single_format(
        &mut self,
        catalog: &dyn SessionCatalog,
        format: &mut Format<Aug>,
        options: &SourceFormatOptions,
        envelope: &Option<SourceEnvelope>,
    ) -> Result<(), ResolveError> {
        match format {
            Format::Avro(AvroSchema::Csr { csr_connection }) => {
                if let Some(request) =
                    prepare_csr_connection_avro(catalog, options, csr_connection, envelope)?
                {
                    let PreparationValue::Avro(seed) =
                        self.request(RequestKind::Avro(Box::new(request)))?
                    else {
                        unreachable!("the result kind is determined by its request")
                    };
                    csr_connection.seed = Some(seed.clone());
                }
            }
            Format::Avro(AvroSchema::Glue {
                connection,
                with_options,
                seed,
            }) => {
                if let Some(request) =
                    prepare_glue_connection_avro(catalog, options, connection, with_options, seed)?
                {
                    let PreparationValue::Glue(prepared) =
                        self.request(RequestKind::Glue(Box::new(request)))?
                    else {
                        unreachable!("the result kind is determined by its request")
                    };
                    *seed = Some(prepared.clone());
                }
            }
            Format::Protobuf(ProtobufSchema::Csr { csr_connection }) => {
                if let Some(request) =
                    prepare_csr_connection_proto(catalog, options, csr_connection, envelope)?
                {
                    let PreparationValue::Protobuf(seed) =
                        self.request(RequestKind::Protobuf(Box::new(request)))?
                    else {
                        unreachable!("the result kind is determined by its request")
                    };
                    csr_connection.seed = Some(seed.clone());
                }
            }
            Format::Avro(AvroSchema::InlineSchema { .. })
            | Format::Protobuf(ProtobufSchema::InlineSchema { .. })
            | Format::Bytes
            | Format::Regex(_)
            | Format::Json { .. }
            | Format::Text
            | Format::Csv { .. } => {}
        }
        Ok(())
    }
}

pub(super) async fn purify(
    catalog: &dyn SessionCatalog,
    now: u64,
    statement: Statement<Aug>,
    storage_configuration: &StorageConfiguration,
) -> Result<PurifiedStatement, PlanError> {
    let mut preparation = SourcePreparation::new(now);
    let mut status = preparation.resolve(catalog, statement.clone())?;
    loop {
        match status {
            SourcePreparationStatus::Ready(statement) => return Ok(*statement),
            SourcePreparationStatus::Pending(request) => {
                let response = request.execute(storage_configuration).await;
                status = preparation.finish(catalog, statement.clone(), response)?;
            }
        }
    }
}
