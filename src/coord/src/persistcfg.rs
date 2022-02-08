// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Materialize-specific persistence configuration.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use timely::progress::Timestamp;

use itertools::Itertools;
use mz_build_info::BuildInfo;
use mz_dataflow_types::sources::{
    persistence::{EnvelopePersistDesc, SourcePersistDesc},
    ExternalSourceConnector, SourceConnector, SourceEnvelope,
};
use mz_ore::metrics::MetricsRegistry;
use mz_persist::error::{Error, ErrorLog};
use mz_persist::indexed::encoding::Id as PersistId;
use mz_persist::s3::{S3Blob, S3BlobConfig};
use mz_persist::storage::{Blob, LockInfo};
use mz_repr::Row;
use serde::Serialize;
use tokio::runtime::Runtime as TokioRuntime;
use url::Url;

use mz_expr::GlobalId;
use mz_persist::client::{MultiWriteHandle, RuntimeClient, StreamWriteHandle};
use mz_persist::file::FileBlob;
use mz_persist::runtime::{self, RuntimeConfig};
use uuid::Uuid;

use crate::catalog::{self, SerializedEnvelopePersistDetails, SerializedSourcePersistDetails};

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
    /// used for [PersistConfig::disabled] (which is an awkward historical
    /// artifact).
    ///
    /// TODO(benesch): can probably drop the `Option` now, but I didn't see an
    /// immediately obvious refactor.
    pub async_runtime: Option<Arc<TokioRuntime>>,
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
    /// Whether to make Kafka sources persistent for fast restarts. This is
    /// extremely experimental and should not even be tried by users.
    pub kafka_sources_enabled: bool,
    /// Unstructured information stored in the "lock" files created by the
    /// log and blob to ensure that they are exclusive writers to those
    /// locations. This should contain whatever information might be useful to
    /// investigating an unexpected lock file (e.g. hostname and materialize
    /// version of the creating process).
    pub lock_info: String,
    pub min_step_interval: Duration,
    /// Maximum size of the in-memory blob storage cache, in bytes.
    pub cache_size_limit: Option<usize>,
}

impl PersistConfig {
    pub fn disabled() -> Self {
        PersistConfig {
            async_runtime: None,
            storage: PersistStorage::File(PersistFileStorage::default()),
            user_table_enabled: false,
            system_table_enabled: false,
            kafka_sources_enabled: false,
            lock_info: Default::default(),
            min_step_interval: Duration::default(),
            cache_size_limit: None,
        }
    }

    /// Initializes the persistence runtime and returns a clone-able handle for
    /// interacting with it. Returns None and does not start the runtime if all
    /// persistence features are disabled.
    pub async fn init(
        &self,
        reentrance_id: Uuid,
        build: BuildInfo,
        reg: &MetricsRegistry,
    ) -> Result<PersisterWithConfig, Error> {
        let runtime = if self.user_table_enabled
            || self.system_table_enabled
            || self.kafka_sources_enabled
        {
            let lock_reentrance_id = reentrance_id.to_string();
            let lock_info = LockInfo::new(lock_reentrance_id, self.lock_info.clone())?;
            let log = ErrorLog;
            let runtime = match &self.storage {
                PersistStorage::File(s) => {
                    let mut blob = FileBlob::open_exclusive((&s.blob_path).into(), lock_info)?;
                    mz_persist::storage::check_meta_version_maybe_delete_data(&mut blob)?;
                    runtime::start(
                        RuntimeConfig::new(self.min_step_interval, self.cache_size_limit),
                        log,
                        blob,
                        build,
                        reg,
                        self.async_runtime.clone(),
                    )
                }
                PersistStorage::S3(s) => {
                    let config =
                        S3BlobConfig::new(s.bucket.clone(), s.prefix.clone(), s.role_arn.clone())
                            .await?;
                    let mut blob = S3Blob::open_exclusive(config, lock_info)?;
                    mz_persist::storage::check_meta_version_maybe_delete_data(&mut blob)?;
                    runtime::start(
                        RuntimeConfig::new(self.min_step_interval, self.cache_size_limit),
                        log,
                        blob,
                        build,
                        reg,
                        self.async_runtime.clone(),
                    )
                }
            }?;
            Some(runtime)
        } else {
            None
        };
        Ok(PersisterWithConfig {
            runtime,
            config: self.clone(),
            table_details: BTreeMap::new(),
            table_writer: None,
            all_table_ids: vec![],
        })
    }
}

#[derive(Debug, Clone)]
pub struct PersisterWithConfig {
    /// The configuration of the persistence runtime.
    pub config: PersistConfig,
    /// A client to the persistence runtime.
    pub runtime: Option<RuntimeClient>,
    /// Details about all persisted tables.
    ///
    /// Tables are only stored in the map if persistence is enabled for that
    /// table.
    pub table_details: BTreeMap<GlobalId, TablePersistDetails>,
    /// A writer that can write atomically to all tables in `table_details`.
    pub table_writer: Option<MultiWriteHandle>,
    /// The persist stream IDs for all tables in `table_details`.
    ///
    /// This is a denormalized copy of information stored in `table_details` for
    /// ease of use with some `MultiWriteHandle` APIs.
    pub all_table_ids: Vec<PersistId>,
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

    /// Creates a persist stream name for a new table.
    pub fn new_table_persist_name(&self, id: GlobalId, pretty: &str) -> Option<String> {
        self.stream_name(id, pretty)
    }

    /// Adds the given table to the set of tables managed by the persister.
    pub fn add_table(&mut self, id: GlobalId, table: &catalog::Table) -> Result<(), Error> {
        let stream_name = match &table.persist_name {
            Some(x) => x.clone(),
            None => return Ok(()),
        };
        let persister = match self.runtime.as_ref() {
            Some(x) => x,
            None => return Ok(()),
        };
        let (write_handle, _) = persister.create_or_load(&stream_name);

        let description = persister.get_description(&stream_name);
        let description = match description {
            Ok(description) => description,
            Err(e) => {
                let error_string = format!("Reading description for {}: {}", stream_name, e);
                return Err(Error::String(error_string));
            }
        };

        // TODO: We have a mismatch here: we know that these frontiers always contain only one
        // element, because we only allow `u64` timestamps, but the return value suggests there
        // could be more. Also: if the frontiers are truly multi-dimensional in the future, the
        // logic that determines a common upper seal timestamp will become a bit more complicated.
        let since_ts = description.since.iter().exactly_one().map_err(|_| {
            format!("expected exactly one element in the persist compaction frontier")
        })?;

        let details = TablePersistDetails {
            stream_name,
            // We need to get the stream_id now because we cannot get it later since most methods
            // in the coordinator/catalog aren't fallible.
            stream_id: write_handle.stream_id()?,
            since_ts: *since_ts,
            write_handle,
        };
        self.table_details.insert(id, details);
        self.regenerate_table_metadata();
        Ok(())
    }

    /// Removes the given table from the set of tables managed by the persister.
    pub fn remove_table(&mut self, id: GlobalId) {
        self.table_details.remove(&id);
        self.regenerate_table_metadata();
    }

    fn regenerate_table_metadata(&mut self) {
        self.all_table_ids = self.table_details.values().map(|td| td.stream_id).collect();

        let mut write_handles = self.table_details.values().map(|td| &td.write_handle);
        match write_handles.next() {
            None => self.table_writer = None,
            Some(first_write_handle) => {
                let table_writer = self
                    .table_writer
                    .insert(MultiWriteHandle::new(first_write_handle));
                for write_handle in write_handles {
                    table_writer
                        .add_stream(write_handle)
                        .expect("write handles known to be from the same runtime");
                }
            }
        }
    }

    /// Creates a [`SerializedSourcePersistDetails`] for a new source.
    pub fn new_serialized_source_persist_details(
        &self,
        id: GlobalId,
        connector: &SourceConnector,
        pretty: &str,
    ) -> Option<SerializedSourcePersistDetails> {
        if !self.config.kafka_sources_enabled {
            return None;
        }

        // NB: This gets written down in the catalog, so it should be
        // safe to change the naming, if necessary. See
        // Catalog::deserialize_item.
        let name_prefix = format!("user-source-{}-{}", id, pretty);
        let primary_stream = format!("{}", name_prefix);
        let offsets_stream = format!("{}-offsets", name_prefix);

        match connector {
            SourceConnector::External {
                connector: ExternalSourceConnector::Kafka(_),
                envelope: SourceEnvelope::Upsert(_),
                ..
            } => Some(SerializedSourcePersistDetails {
                primary_stream,
                offsets_stream,
                envelope_details: crate::catalog::SerializedEnvelopePersistDetails::Upsert,
            }),
            SourceConnector::External {
                connector: ExternalSourceConnector::Kafka(_),
                envelope: SourceEnvelope::None(_),
                ..
            } => Some(SerializedSourcePersistDetails {
                primary_stream,
                offsets_stream,
                envelope_details: crate::catalog::SerializedEnvelopePersistDetails::None,
            }),
            SourceConnector::External {
                connector: ExternalSourceConnector::Kafka(_),
                envelope,
                ..
            } => {
                panic!(
                    "Kafka Source persistence not yet implemented for envelope {:?}",
                    envelope
                );
            }
            _ => None,
        }
    }

    /// Loads a source persist descriptor for an existing catalog source that
    /// reflects current frontier information.
    pub fn load_source_persist_desc(
        &self,
        catalog::Source {
            connector,
            persist_details,
            ..
        }: &catalog::Source,
    ) -> Result<Option<SourcePersistDesc>, Error> {
        let runtime = match self.runtime.as_ref() {
            Some(x) => x,
            None => return Ok(None),
        };

        let details = persist_details.as_ref().map(|serialized_details| {
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

                SourceConnector::External {
                    envelope: SourceEnvelope::None(_),
                    ..
                } => {
                    assert!(matches!(
                        serialized_details.envelope_details,
                        SerializedEnvelopePersistDetails::None
                    ));
                    EnvelopePersistDesc::None
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
            let (primary_stream, primary_since, primary_upper) =
                stream_desc_from_name(serialized_details.primary_stream.clone(), runtime)?;
            let (offsets_stream, offsets_since, offsets_upper) =
                stream_desc_from_name(serialized_details.offsets_stream.clone(), runtime)?;

            // Assert invariants!
            assert_eq!(
                primary_since, offsets_since,
                "the since of all involved streams must be the same"
            );
            assert_eq!(
                primary_upper, offsets_upper,
                "the upper of all involved streams must be the same"
            );

            Ok(SourcePersistDesc {
                since_ts: primary_since,
                upper_seal_ts: primary_upper,
                primary_stream,
                offsets_stream,
                envelope_desc,
            })
        });

        details.transpose()
    }
}

fn stream_desc_from_name(
    name: String,
    runtime: &RuntimeClient,
) -> Result<(String, u64, u64), Error> {
    let description = runtime.get_description(&name);
    let description = match description {
        Ok(description) => description,
        Err(Error::UnknownRegistration(error_name)) if name == error_name => {
            // The stream has not been created yet, so return the initial seal timestamp of
            // streams.
            // TODO: We might want to codify this somewhere?
            return Ok((name, u64::minimum(), u64::minimum()));
        }
        Err(e) => {
            let error_string = format!("Reading upper seal timestamp for {}: {}", name, e);
            return Err(Error::String(error_string));
        }
    };

    // TODO: We have a mismatch here: we know that these frontiers always contain only one element,
    // because we only allow `u64` timestamps, but the return value suggests there could be more.
    // Also: if the frontiers are truly multi-dimensional in the future, the logic that determines
    // a common upper seal timestamp will become a bit more complicated.
    let upper_seal_ts = description
        .upper
        .iter()
        .exactly_one()
        .map_err(|_| format!("expected exactly one element in the persist upper frontier"))?;
    let since_ts =
        description.since.iter().exactly_one().map_err(|_| {
            format!("expected exactly one element in the persist compaction frontier")
        })?;
    Ok((name, *since_ts, *upper_seal_ts))
}

#[derive(Debug, Clone, Serialize)]
pub struct TablePersistDetails {
    pub stream_name: String,
    pub stream_id: PersistId,
    /// The _current_ compaction frontier (aka _since_) of the persistent stream that is backing
    /// this table.
    ///
    /// NOTE: This timestamp is determined when the coordinator starts up or when the table is
    /// initially created. When a table is actively being written to when allowing compaction, this
    /// will progress beyond this timestamp. This is ok, though, because we only need to make sure
    /// that we respect the since when starting up. After that, we keep track of the compaction
    /// frontier as we do for other indexes.
    pub since_ts: u64,
    #[serde(skip)]
    pub write_handle: StreamWriteHandle<Row, ()>,
}
