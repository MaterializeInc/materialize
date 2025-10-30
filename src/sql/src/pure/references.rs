// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::DerefMut;
use std::sync::Arc;

use mz_ore::now::SYSTEM_TIME;
use mz_repr::RelationDesc;
use mz_sql_parser::ast::{ExternalReferences, Ident, IdentError, UnresolvedItemName};
use mz_sql_server_util::desc::SqlServerTableRaw;
use mz_storage_types::sources::load_generator::{LoadGenerator, LoadGeneratorOutput};
use mz_storage_types::sources::{ExternalReferenceResolutionError, SourceReferenceResolver};

use crate::names::{FullItemName, RawDatabaseSpecifier};
use crate::plan::{PlanError, SourceReference, SourceReferences};

use super::{RequestedSourceExport, error::PgSourcePurificationError};

/// A client that allows determining all available source references and resolving
/// them to a user-specified source reference during purification.
pub(super) enum SourceReferenceClient<'a> {
    Postgres {
        client: &'a mz_postgres_util::Client,
        publication: &'a str,
        database: &'a str,
    },
    MySql {
        conn: &'a mut mz_mysql_util::MySqlConn,
        /// Sets whether to include tables from the built-in system schemas in the
        /// retrieved references.
        include_system_schemas: bool,
    },
    SqlServer {
        client: &'a mut mz_sql_server_util::Client,
        database: Arc<str>,
    },
    Kafka {
        topic: &'a str,
    },
    LoadGenerator {
        generator: &'a LoadGenerator,
    },
}

/// Metadata about an available source reference retrieved from the upstream system.
#[derive(Clone, Debug)]
pub(super) enum ReferenceMetadata {
    Postgres {
        table: mz_postgres_util::desc::PostgresTableDesc,
        database: String,
    },
    MySql(mz_mysql_util::MySqlTableSchema),
    SqlServer {
        table: mz_sql_server_util::desc::SqlServerTableDesc,
        database: Arc<str>,
        capture_instance: Arc<str>,
    },
    Kafka(String),
    LoadGenerator {
        name: String,
        desc: Option<RelationDesc>,
        namespace: String,
        output: LoadGeneratorOutput,
    },
}

impl ReferenceMetadata {
    fn namespace(&self) -> Option<&str> {
        match self {
            ReferenceMetadata::Postgres { table, .. } => Some(&table.namespace),
            ReferenceMetadata::MySql(table) => Some(&table.schema_name),
            ReferenceMetadata::SqlServer { table, .. } => Some(table.schema_name.as_ref()),
            ReferenceMetadata::Kafka(_) => None,
            ReferenceMetadata::LoadGenerator { namespace, .. } => Some(namespace),
        }
    }

    fn name(&self) -> &str {
        match self {
            ReferenceMetadata::Postgres { table, .. } => &table.name,
            ReferenceMetadata::MySql(table) => &table.name,
            ReferenceMetadata::SqlServer { table, .. } => table.name.as_ref(),
            ReferenceMetadata::Kafka(topic) => topic,
            ReferenceMetadata::LoadGenerator { name, .. } => name,
        }
    }

    pub(super) fn postgres_desc(&self) -> Option<&mz_postgres_util::desc::PostgresTableDesc> {
        match self {
            ReferenceMetadata::Postgres { table, .. } => Some(table),
            _ => None,
        }
    }

    pub(super) fn mysql_table(&self) -> Option<&mz_mysql_util::MySqlTableSchema> {
        match self {
            ReferenceMetadata::MySql(table) => Some(table),
            _ => None,
        }
    }

    pub(super) fn sql_server_table(&self) -> Option<&mz_sql_server_util::desc::SqlServerTableDesc> {
        match self {
            ReferenceMetadata::SqlServer { table, .. } => Some(table),
            _ => None,
        }
    }

    pub(super) fn sql_server_capture_instance(&self) -> Option<&Arc<str>> {
        match self {
            ReferenceMetadata::SqlServer {
                capture_instance, ..
            } => Some(capture_instance),
            _ => None,
        }
    }

    pub(super) fn load_generator_desc(&self) -> Option<&Option<RelationDesc>> {
        match self {
            ReferenceMetadata::LoadGenerator { desc, .. } => Some(desc),
            _ => None,
        }
    }

    pub(super) fn load_generator_output(&self) -> Option<&LoadGeneratorOutput> {
        match self {
            ReferenceMetadata::LoadGenerator { output, .. } => Some(output),
            _ => None,
        }
    }

    /// Convert the reference metadata into an `UnresolvedItemName` representing the
    /// external reference, normalized for each source type to be stored as part of
    /// the relevant statement in the catalog.
    pub(super) fn external_reference(&self) -> Result<UnresolvedItemName, IdentError> {
        match self {
            ReferenceMetadata::Postgres { table, database } => {
                Ok(UnresolvedItemName::qualified(&[
                    Ident::new(database)?,
                    Ident::new(&table.namespace)?,
                    Ident::new(&table.name)?,
                ]))
            }
            ReferenceMetadata::MySql(table) => Ok(UnresolvedItemName::qualified(&[
                Ident::new(&table.schema_name)?,
                Ident::new(&table.name)?,
            ])),
            ReferenceMetadata::SqlServer {
                table,
                database,
                capture_instance: _,
            } => Ok(UnresolvedItemName::qualified(&[
                Ident::new(database.as_ref())?,
                Ident::new(table.schema_name.as_ref())?,
                Ident::new(table.name.as_ref())?,
            ])),
            ReferenceMetadata::Kafka(topic) => {
                Ok(UnresolvedItemName::qualified(&[Ident::new(topic)?]))
            }
            ReferenceMetadata::LoadGenerator {
                name, namespace, ..
            } => {
                let name = FullItemName {
                    database: RawDatabaseSpecifier::Name(
                        mz_storage_types::sources::load_generator::LOAD_GENERATOR_DATABASE_NAME
                            .to_owned(),
                    ),
                    schema: namespace.to_string(),
                    item: name.to_string(),
                };
                Ok(UnresolvedItemName::from(name))
            }
        }
    }
}

/// A set of resolved source references.
#[derive(Clone, Debug)]
pub(super) struct RetrievedSourceReferences {
    updated_at: u64,
    references: Vec<ReferenceMetadata>,
    resolver: SourceReferenceResolver,
}

/// The name of the fake database that we use for non-Postgres sources
/// to fit the model of a 3-layer catalog used to resolve references
/// in the `SourceReferenceResolver`. This isn't actually stored in
/// the catalog since the `ReferenceMetadata::external_reference`
/// method only includes the database name for Postgres sources.
pub(crate) static DATABASE_FAKE_NAME: &str = "database";

impl<'a> SourceReferenceClient<'a> {
    /// Get all available source references from the upstream system
    /// and return a `RetrievedSourceReferences` object that can be used
    /// to resolve user-specified source references and create `SourceReferences`
    /// for storage in the catalog.
    pub(super) async fn get_source_references(
        mut self,
    ) -> Result<RetrievedSourceReferences, PlanError> {
        let references = match self {
            SourceReferenceClient::Postgres {
                client,
                publication,
                database,
            } => {
                let tables = mz_postgres_util::publication_info(client, publication, None).await?;

                if tables.is_empty() {
                    Err(PgSourcePurificationError::EmptyPublication(
                        publication.to_string(),
                    ))?;
                }

                tables
                    .into_iter()
                    .map(|(_oid, desc)| ReferenceMetadata::Postgres {
                        table: desc,
                        database: database.to_string(),
                    })
                    .collect()
            }
            SourceReferenceClient::MySql {
                ref mut conn,
                include_system_schemas,
            } => {
                let request = if include_system_schemas {
                    mz_mysql_util::SchemaRequest::AllWithSystemSchemas
                } else {
                    mz_mysql_util::SchemaRequest::All
                };
                // NOTE: mysql will only expose the schemas of tables we have at least one privilege on
                // and we can't tell if a table exists without a privilege
                let tables = mz_mysql_util::schema_info((*conn).deref_mut(), &request).await?;

                tables.into_iter().map(ReferenceMetadata::MySql).collect()
            }
            SourceReferenceClient::SqlServer {
                ref mut client,
                ref database,
            } => {
                let tables = mz_sql_server_util::inspect::get_tables(client).await?;

                let mut unique_tables: BTreeMap<(Arc<str>, Arc<str>), SqlServerTableRaw> =
                    BTreeMap::default();
                for table in tables {
                    let key = (Arc::clone(&table.schema_name), Arc::clone(&table.name));

                    unique_tables
                        .entry(key)
                        .and_modify(|chosen_table: &mut SqlServerTableRaw| {
                            // When multiple capture instances exist for the same table,
                            // we select deterministically based on:
                            // 1. Most recent create_date (newer capture instance)
                            // 2. If dates are equal, lexicographically greatest capture_instance name
                            if chosen_table.capture_instance.create_date
                                < table.capture_instance.create_date
                                || (chosen_table.capture_instance.create_date
                                    == table.capture_instance.create_date
                                    && chosen_table.capture_instance.name
                                        < table.capture_instance.name)
                            {
                                *chosen_table = table.clone();
                            }
                        })
                        .or_insert(table);
                }

                let mut result = Vec::with_capacity(unique_tables.len());
                for raw_table in unique_tables.into_values() {
                    let constraints = mz_sql_server_util::inspect::get_constraints_for_table(
                        client,
                        &raw_table.schema_name,
                        &raw_table.name,
                    )
                    .await?;
                    let capture_instance = Arc::clone(&raw_table.capture_instance.name);
                    let database = Arc::clone(database);
                    let table =
                        mz_sql_server_util::desc::SqlServerTableDesc::new(raw_table, constraints)?;
                    result.push(ReferenceMetadata::SqlServer {
                        table,
                        database,
                        capture_instance,
                    });
                }
                result
            }
            SourceReferenceClient::Kafka { topic } => {
                vec![ReferenceMetadata::Kafka(topic.to_string())]
            }
            SourceReferenceClient::LoadGenerator { generator } => {
                let mut references = generator
                    .views()
                    .into_iter()
                    .map(
                        |(view, relation, output)| ReferenceMetadata::LoadGenerator {
                            name: view.to_string(),
                            desc: Some(relation),
                            namespace: generator.schema_name().to_string(),
                            output,
                        },
                    )
                    .collect::<Vec<_>>();

                if references.is_empty() {
                    // If there are no views then this load-generator just has a single output
                    // that uses the load-generator's schema name.
                    references.push(ReferenceMetadata::LoadGenerator {
                        name: generator.schema_name().to_string(),
                        desc: None,
                        namespace: generator.schema_name().to_string(),
                        output: LoadGeneratorOutput::Default,
                    });
                }
                references
            }
        };

        let reference_names: Vec<(&str, &str)> = references
            .iter()
            .map(|reference| {
                (
                    reference.namespace().unwrap_or_else(|| reference.name()),
                    reference.name(),
                )
            })
            .collect();
        let resolver = match self {
            SourceReferenceClient::Postgres { database, .. } => {
                SourceReferenceResolver::new(database, &reference_names)
            }
            _ => SourceReferenceResolver::new(DATABASE_FAKE_NAME, &reference_names),
        }?;

        Ok(RetrievedSourceReferences {
            updated_at: SYSTEM_TIME(),
            references,
            resolver,
        })
    }
}

impl RetrievedSourceReferences {
    /// Convert the resolved source references into a `SourceReferences` object
    /// for storage in the catalog.
    pub(super) fn available_source_references(self) -> SourceReferences {
        SourceReferences {
            updated_at: self.updated_at,
            references: self
                .references
                .into_iter()
                .map(|reference| match reference {
                    ReferenceMetadata::Postgres { table, .. } => SourceReference {
                        name: table.name,
                        namespace: Some(table.namespace),
                        columns: table.columns.into_iter().map(|c| c.name).collect(),
                    },
                    ReferenceMetadata::MySql(table) => SourceReference {
                        name: table.name,
                        namespace: Some(table.schema_name),
                        columns: table
                            .columns
                            .into_iter()
                            .map(|column| column.name())
                            .collect(),
                    },
                    ReferenceMetadata::SqlServer { table, .. } => SourceReference {
                        name: table.name.to_string(),
                        namespace: Some(table.schema_name.to_string()),
                        columns: table
                            .columns
                            .into_iter()
                            .map(|c| c.name.to_string())
                            .collect(),
                    },
                    ReferenceMetadata::Kafka(topic) => SourceReference {
                        name: topic,
                        namespace: None,
                        columns: vec![],
                    },
                    ReferenceMetadata::LoadGenerator {
                        name,
                        desc,
                        namespace,
                        ..
                    } => SourceReference {
                        name,
                        namespace: Some(namespace),
                        columns: desc
                            .map(|desc| desc.iter_names().map(|n| n.to_string()).collect())
                            .unwrap_or_default(),
                    },
                })
                .collect(),
        }
    }

    /// Resolve the requested external references to their appropriate source exports.
    pub(super) fn requested_source_exports<'a>(
        &'a self,
        requested: Option<&ExternalReferences>,
        source_name: &UnresolvedItemName,
    ) -> Result<Vec<RequestedSourceExport<&'a ReferenceMetadata>>, PlanError> {
        // Filter all available references to those requested by the `ExternalReferences`
        // specification and include any alias that the user has specified.
        // TODO(database-issues#8620): The alias handling can be removed once subsources are removed.
        let filtered: Vec<(&ReferenceMetadata, Option<&UnresolvedItemName>)> = match requested {
            Some(ExternalReferences::All) => self.references.iter().map(|r| (r, None)).collect(),
            Some(ExternalReferences::SubsetSchemas(schemas)) => {
                let available_schemas: BTreeSet<&str> = self
                    .references
                    .iter()
                    .filter_map(|r| r.namespace())
                    .collect();
                let requested_schemas: BTreeSet<&str> =
                    schemas.iter().map(|s| s.as_str()).collect();

                let missing_schemas: Vec<_> = requested_schemas
                    .difference(&available_schemas)
                    .map(|s| s.to_string())
                    .collect();

                if !missing_schemas.is_empty() {
                    Err(PlanError::NoTablesFoundForSchemas(missing_schemas))?
                }

                self.references
                    .iter()
                    .filter_map(|reference| {
                        reference
                            .namespace()
                            .map(|namespace| {
                                requested_schemas
                                    .contains(namespace)
                                    .then_some((reference, None))
                            })
                            .flatten()
                    })
                    .collect()
            }
            Some(ExternalReferences::SubsetTables(requested_tables)) => {
                // Use the `SourceReferenceResolver` to resolve the requested tables to their
                // appropriate index in the available references.
                requested_tables
                    .into_iter()
                    .map(|requested_table| {
                        let idx = self.resolver.resolve_idx(&requested_table.reference.0)?;
                        Ok((&self.references[idx], requested_table.alias.as_ref()))
                    })
                    .collect::<Result<Vec<_>, PlanError>>()?
            }
            None => {
                // If no reference is requested we must validate that only one reference is
                // available, else we cannot determine which reference the user is referring to.
                if self.references.len() != 1 {
                    Err(ExternalReferenceResolutionError::Ambiguous {
                        name: "".to_string(),
                    })?
                }
                vec![(&self.references[0], None)]
            }
        };

        // Convert the filtered references to their appropriate `RequestedSourceExport` form.
        filtered
            .into_iter()
            .map(|(reference, alias)| {
                let name = match alias {
                    Some(alias_name) => {
                        let partial = crate::normalize::unresolved_item_name(alias_name.clone())?;
                        match partial.schema {
                            Some(_) => alias_name.clone(),
                            // In cases when a prefix is not provided for the aliased name
                            // fallback to using the schema of the source with the given name
                            None => super::source_export_name_gen(source_name, &partial.item)?,
                        }
                    }
                    None => {
                        // Just use the item name for this reference and ensure it's created in
                        // the current schema or the source's schema if provided, not mirroring
                        // the schema of the reference.
                        super::source_export_name_gen(source_name, reference.name())?
                    }
                };

                Ok(RequestedSourceExport {
                    external_reference: reference.external_reference()?,
                    name,
                    meta: reference,
                })
            })
            .collect::<Result<Vec<_>, PlanError>>()
    }

    pub(super) fn resolve_name(&self, name: &[Ident]) -> Result<&ReferenceMetadata, PlanError> {
        let idx = self.resolver.resolve_idx(name)?;
        Ok(&self.references[idx])
    }

    pub(super) fn all_references(&self) -> &Vec<ReferenceMetadata> {
        &self.references
    }
}
