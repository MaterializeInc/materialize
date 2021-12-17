// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Materialize-specific persistence configuration.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use timely::progress::Timestamp;

use build_info::BuildInfo;
use dataflow_types::{
    EnvelopePersistDesc, ExternalSourceConnector, PersistStreamDesc, SourceConnector,
    SourceEnvelope, SourcePersistDesc,
};
use itertools::Itertools;
use ore::metrics::MetricsRegistry;
use persist::error::{Error, ErrorLog};
use persist::indexed::encoding::Id as PersistId;
use persist::s3::{S3Blob, S3BlobConfig};
use persist::storage::{Blob, LockInfo};
use repr::Row;
use serde::Serialize;
use tokio::runtime::Runtime;
use url::Url;

use expr::GlobalId;
use persist::file::FileBlob;
use persist::indexed::runtime::{
    self, MultiWriteHandle, RuntimeClient, RuntimeConfig, StreamWriteHandle,
};
use uuid::Uuid;

use crate::catalog::{SerializedEnvelopePersistDetails, SerializedSourcePersistDetails};

#[derive(Clone, Debug)]
pub enum PersistStorage {
    File(PersistFileStorage),
    S3(PersistS3Storage),
}

impl TryFrom<String> for PersistStorage {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let url = Url::parse(&value).map_err(|err| {
            format!(
                "failed to parse storage location {} as a url: {}",
                &value, err
            )
        })?;
        match url.scheme() {
            "s3" => {
                let bucket = url
                    .host()
                    .ok_or_else(|| format!("missing bucket: {}", &url))?
                    .to_string();
                let prefix = url
                    .path()
                    .strip_prefix('/')
                    .unwrap_or_else(|| url.path())
                    .to_string();
                let mut query_params = url.query_pairs().collect::<HashMap<_, _>>();
                let role_arn = query_params.remove("aws_role_arn").map(|x| x.into_owned());
                if !query_params.is_empty() {
                    return Err(format!(
                        "unknown storage location params: {}",
                        query_params
                            .keys()
                            .map(|x| x.to_owned())
                            .collect::<Vec<_>>()
                            .join(" ")
                    )
                    .into());
                }
                Ok(PersistStorage::S3(PersistS3Storage {
                    bucket,
                    prefix,
                    role_arn,
                }))
            }
            p => Err(Error::from(format!("unknown storage provider: {}", p))),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct PersistFileStorage {
    /// A directory under which larger batches of indexed data are stored.
    pub blob_path: PathBuf,
}

#[derive(Clone, Debug)]
pub struct PersistS3Storage {
    /// An S3 bucket in which larger batches of indexed data are stored.
    pub bucket: String,
    /// A prefix prepended for all S3 object keys used by this storage. Empty is
    /// fine.
    pub prefix: String,
    /// An AWS role ARN to assume.
    pub role_arn: Option<String>,
}

/// Configuration of the persistence runtime and features.
#[derive(Clone, Debug)]
pub struct PersistConfig {
    /// A runtime used for IO and cpu heavy work. The None case should only be
    /// used for [PersistConfig::disabled] (which is an awkward artifact of us
    /// currently initializing persistence in the Catalog startup).
    pub runtime: Option<Arc<Runtime>>,
    /// Where to store persisted data.
    pub storage: PersistStorage,
    /// Whether to persist all user tables. This is extremely experimental and
    /// should not even be tried by users. It's initially here for end-to-end
    /// testing.
    pub user_table_enabled: bool,
    /// Whether to persist certain system tables that have opted in. This is
    /// extremely experimental and should not even be tried by users. It's
    /// initially here for end-to-end testing.
    pub system_table_enabled: bool,
    /// Whether to make Kafka Upserts persistent for fast restarts. This is
    /// extremely experimental and should not even be tried by users.
    pub kafka_upsert_source_enabled: bool,
    /// Unstructured information stored in the "lock" files created by the
    /// log and blob to ensure that they are exclusive writers to those
    /// locations. This should contain whatever information might be useful to
    /// investigating an unexpected lock file (e.g. hostname and materialize
    /// version of the creating process).
    pub lock_info: String,
    pub min_step_interval: Duration,
}

impl PersistConfig {
    pub fn disabled() -> Self {
        PersistConfig {
            runtime: None,
            storage: PersistStorage::File(PersistFileStorage::default()),
            user_table_enabled: false,
            system_table_enabled: false,
            kafka_upsert_source_enabled: false,
            lock_info: Default::default(),
            min_step_interval: Duration::default(),
        }
    }

    /// Initializes the persistence runtime and returns a clone-able handle for
    /// interacting with it. Returns None and does not start the runtime if all
    /// persistence features are disabled.
    pub async fn init(
        &self,
        catalog_id: Uuid,
        build: BuildInfo,
        reg: &MetricsRegistry,
    ) -> Result<PersisterWithConfig, Error> {
        let persister = if self.user_table_enabled
            || self.system_table_enabled
            || self.kafka_upsert_source_enabled
        {
            let lock_reentrance_id = catalog_id.to_string();
            let lock_info = LockInfo::new(lock_reentrance_id, self.lock_info.clone())?;
            let log = ErrorLog;
            let persister = match &self.storage {
                PersistStorage::File(s) => {
                    let mut blob = FileBlob::open_exclusive((&s.blob_path).into(), lock_info)?;
                    persist::storage::check_meta_version_maybe_delete_data(&mut blob)?;
                    runtime::start(
                        RuntimeConfig::with_min_step_interval(self.min_step_interval),
                        log,
                        blob,
                        build,
                        reg,
                        self.runtime.clone(),
                    )
                }
                PersistStorage::S3(s) => {
                    let config =
                        S3BlobConfig::new(s.bucket.clone(), s.prefix.clone(), s.role_arn.clone())
                            .await?;
                    let mut blob = S3Blob::open_exclusive(config, lock_info)?;
                    persist::storage::check_meta_version_maybe_delete_data(&mut blob)?;
                    runtime::start(
                        RuntimeConfig::with_min_step_interval(self.min_step_interval),
                        log,
                        blob,
                        build,
                        reg,
                        self.runtime.clone(),
                    )
                }
            }?;
            Some(persister)
        } else {
            None
        };
        Ok(PersisterWithConfig {
            persister,
            config: self.clone(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct PersisterWithConfig {
    pub config: PersistConfig,
    pub persister: Option<RuntimeClient>,
}

impl PersisterWithConfig {
    fn stream_name(&self, id: GlobalId, pretty: &str) -> Option<String> {
        match id {
            GlobalId::User(id) if self.config.user_table_enabled => {
                // NB: This gets written down in the catalog, so it should be
                // safe to change the naming, if necessary. See
                // Catalog::deserialize_item.
                Some(format!("user-table-{:?}-{}", id, pretty))
            }
            GlobalId::System(id) if self.config.system_table_enabled => {
                // NB: Until the end of our persisted system tables experiment, give
                // persist team a heads up if you change this, please!
                Some(format!("system-table-{:?}-{}", id, pretty))
            }
            _ => None,
        }
    }

    pub fn table_details(
        &self,
        id: GlobalId,
        pretty: &str,
    ) -> Result<Option<TablePersistDetails>, Error> {
        self.table_details_from_name(self.stream_name(id, pretty))
    }

    pub fn table_details_from_name(
        &self,
        stream_name: Option<String>,
    ) -> Result<Option<TablePersistDetails>, Error> {
        let stream_name = match stream_name {
            Some(x) => x,
            None => return Ok(None),
        };
        let persister = match self.persister.as_ref() {
            Some(x) => x,
            None => return Ok(None),
        };
        let (write_handle, _) = persister.create_or_load(&stream_name);
        Ok(Some(TablePersistDetails {
            stream_name,
            // We need to get the stream_id now because we cannot get it later since most methods
            // in the coordinator/catalog aren't fallible.
            stream_id: write_handle.stream_id()?,
            write_handle,
        }))
    }

    pub fn source_persist_desc(
        &self,
        id: GlobalId,
        connector: &SourceConnector,
        pretty: &str,
    ) -> Result<Option<SourcePersistDesc>, Error> {
        let serialized_details = match connector {
            SourceConnector::External {
                connector: ExternalSourceConnector::Kafka(_),
                envelope: SourceEnvelope::Upsert(_),
                ..
            } if self.config.kafka_upsert_source_enabled => {
                // NB: This gets written down in the catalog, so it should be
                // safe to change the naming, if necessary. See
                // Catalog::deserialize_item.
                let name_prefix = format!("user-source-{}-{}", id, pretty);
                let primary_stream = format!("{}", name_prefix);
                let timestamp_bindings_stream = format!("{}-timestamp-bindings", name_prefix);

                Some(SerializedSourcePersistDetails {
                    primary_stream,
                    timestamp_bindings_stream,
                    envelope_details: crate::catalog::SerializedEnvelopePersistDetails::Upsert,
                })
            }
            _ => None,
        };

        self.source_persist_desc_from_serialized(connector, serialized_details)
    }

    // NOTE: This is not a From<SerializedSourcePersistDetails> because we also need access to the
    // connector and the persist runtime in future changes.
    pub fn source_persist_desc_from_serialized(
        &self,
        connector: &SourceConnector,
        serialized_details: Option<SerializedSourcePersistDetails>,
    ) -> Result<Option<SourcePersistDesc>, Error> {
        let persister = match self.persister.as_ref() {
            Some(x) => x,
            None => return Ok(None),
        };

        let details = serialized_details.map(|serialized_details| {
            let envelope_desc = match connector {
                SourceConnector::External {
                    envelope: SourceEnvelope::Upsert(_),
                    ..
                } => {
                    assert!(matches!(
                        serialized_details.envelope_details,
                        SerializedEnvelopePersistDetails::Upsert
                    ));
                    EnvelopePersistDesc::Upsert
                }

                SourceConnector::External { envelope, .. } => {
                    return Err(format!("unsupported envelope: {:?}", envelope).into());
                }

                connector => {
                    return Err(format!(
                        "only external sources support persistence, got: {:?}",
                        connector
                    )
                    .into());
                }
            };

            // TODO: We might want to add (or change) a get_description() that allows getting the
            // descriptions for a batch of IDs in one go instead of getting them all separately. It
            // shouldn't be an issue right now, though.
            let primary_stream =
                stream_desc_from_name(serialized_details.primary_stream, persister)?;
            let timestamp_bindings_stream =
                stream_desc_from_name(serialized_details.timestamp_bindings_stream, persister)?;

            Ok(SourcePersistDesc {
                primary_stream,
                timestamp_bindings_stream,
                envelope_desc,
            })
        });

        details.transpose()
    }
}

fn stream_desc_from_name(
    name: String,
    persister: &RuntimeClient,
) -> Result<PersistStreamDesc, Error> {
    let description = persister.get_description(&name);
    let description = match description {
        Ok(description) => description,
        Err(Error::UnknownRegistration(error_name)) if name == error_name => {
            // The stream has not been created yet, so return the initial seal timestamp of
            // streams.
            // TODO: We might want to codify this somewhere?
            return Ok(PersistStreamDesc {
                name,
                upper_seal_ts: u64::minimum(),
                since_ts: u64::minimum(),
            });
        }
        Err(e) => {
            let error_string = format!("Reading upper seal timestamp for {}: {}", name, e);
            return Err(Error::String(error_string));
        }
    };

    // TODO: We have a mismatch here: we know that these frontiers always contains only one
    // element, because we only allow `u64` timestamps, but the return value suggests there could
    // be more. Also: if the frontiers are truly multi-dimensional in the future, the logic that
    // determines a common upper seal timestamp will become a bit more complicated.
    let upper_seal_ts = description
        .upper()
        .iter()
        .exactly_one()
        .map_err(|_| format!("expected exactly one element in the persist upper frontier"))?;
    let since_ts =
        description.since().iter().exactly_one().map_err(|_| {
            format!("expected exactly one element in the persist compaction frontier")
        })?;
    Ok(PersistStreamDesc {
        name,
        upper_seal_ts: *upper_seal_ts,
        since_ts: *since_ts,
    })
}

#[derive(Debug, Clone, Serialize)]
pub struct TablePersistDetails {
    pub stream_name: String,
    pub stream_id: PersistId,
    #[serde(skip)]
    pub write_handle: StreamWriteHandle<Row, ()>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TablePersistMultiDetails {
    pub all_table_ids: Vec<PersistId>,
    pub write_handle: MultiWriteHandle<Row, ()>,
}
