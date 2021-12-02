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

use dataflow_types::{
    EnvelopePersistDesc, ExternalSourceConnector, SourceConnector, SourceEnvelope,
    SourcePersistDesc,
};
use ore::metrics::MetricsRegistry;
use persist::error::{Error, ErrorLog};
use persist::indexed::encoding::Id as PersistId;
use persist::s3::{Config as S3Config, S3Blob};
use persist::storage::LockInfo;
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

use crate::catalog::SerializedSourcePersistDetails;

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
                Ok(PersistStorage::S3(PersistS3Storage { bucket, prefix }))
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
                    let mut blob = FileBlob::new(&s.blob_path, lock_info)?;
                    persist::storage::check_meta_version_maybe_delete_data(&mut blob)?;
                    runtime::start(
                        RuntimeConfig::with_min_step_interval(self.min_step_interval),
                        log,
                        blob,
                        reg,
                        self.runtime.clone(),
                    )
                }
                PersistStorage::S3(s) => {
                    let config = S3Config::new(s.bucket.clone(), s.prefix.clone()).await?;
                    let mut blob = S3Blob::new(config, lock_info)?;
                    persist::storage::check_meta_version_maybe_delete_data(&mut blob)?;
                    runtime::start(
                        RuntimeConfig::with_min_step_interval(self.min_step_interval),
                        log,
                        blob,
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

    pub fn source_details(
        &self,
        id: GlobalId,
        connector: &SourceConnector,
        pretty: &str,
    ) -> Result<Option<SourcePersistDetails>, Error> {
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
                let data_name = format!("{}", name_prefix);
                let timestamp_bindings_name = format!("{}-timestamp-bindings", name_prefix);

                let mut secondary_streams = HashMap::new();
                secondary_streams.insert("timestamp-bindings".to_string(), timestamp_bindings_name);
                Some(SerializedSourcePersistDetails {
                    primary_stream: data_name,
                    secondary_streams,
                })
            }
            _ => None,
        };

        self.source_details_from_stream_names(connector, serialized_details)
    }

    // NOTE: This is not a From<SerializedSourcePersistDetails> because we also need access to the
    // connector and the persist runtime in future changes.
    pub fn source_details_from_stream_names(
        &self,
        connector: &SourceConnector,
        serialized_details: Option<SerializedSourcePersistDetails>,
    ) -> Result<Option<SourcePersistDetails>, Error> {
        let details = serialized_details.map(|serialized_details| {
            let envelope_details = match connector {
                SourceConnector::External {
                    envelope: SourceEnvelope::Upsert(_),
                    ..
                } => EnvelopePersistDetails::Upsert,

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

            let timestamp_bindings_stream = serialized_details
                .secondary_streams
                .get("timestamp-bindings")
                .ok_or("missing timestamp-bindings stream")?
                .to_string();

            Ok(SourcePersistDetails {
                primary_stream: serialized_details.primary_stream,
                timestamp_bindings_stream,
                envelope_details,
            })
        });

        details.transpose()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SourcePersistDetails {
    /// Name of the primary persisted stream of this source. This is what a consumer of the
    /// persisted data would be interested in while the secondary stream(s) of the source are an
    /// internal implementation detail.
    pub primary_stream: String,

    /// Persisted stream of timestamp bindings.
    pub timestamp_bindings_stream: String,

    /// Any additional details that we need to make the envelope logic stateful.
    pub envelope_details: EnvelopePersistDetails,
}

#[derive(Debug, Clone, Serialize)]
pub enum EnvelopePersistDetails {
    // TODO: We only have support for UPSERT right now, which doesn't need any state besides the
    // primary stream (which doubles as the upsert state) and the timestamp bindings.
    //
    // I put this here as a placeholder and we can expand in the future, when we will also have
    // `Debezium` and know what it needs as additional state.
    Upsert,
}

impl From<SourcePersistDetails> for SourcePersistDesc {
    fn from(source_persist_details: SourcePersistDetails) -> Self {
        let primary_stream = source_persist_details.primary_stream;
        let timestamp_bindings_stream = source_persist_details.timestamp_bindings_stream;

        let envelope_desc = match source_persist_details.envelope_details {
            EnvelopePersistDetails::Upsert => EnvelopePersistDesc::Upsert,
        };

        SourcePersistDesc {
            primary_stream,
            timestamp_bindings_stream,
            envelope_desc,
        }
    }
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
