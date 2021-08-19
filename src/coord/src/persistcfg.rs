// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Materialize-specific persistence configuration.

use std::path::PathBuf;

use ore::metrics::MetricsRegistry;
use persist::error::Error;
use persist::indexed::encoding::Id;
use persist::storage::LockInfo;
use repr::Row;
use serde::Serialize;

use expr::GlobalId;
use persist::file::{FileBlob, FileLog};
use persist::indexed::runtime::{self, MultiWriteHandle, RuntimeClient, StreamWriteHandle};
use uuid::Uuid;

/// Configuration of the persistence runtime and features.
#[derive(Clone, Debug)]
pub struct PersistConfig {
    /// A directory under which un-indexed WAL-like writes are quickly stored.
    pub log_path: PathBuf,
    /// A directory under which larger batches of indexed data are stored. This
    /// will eventually be S3 for Cloud.
    pub blob_path: PathBuf,
    /// Whether to persist all user tables. This is extremely experimental and
    /// should not even be tried by users. It's initially here for end-to-end
    /// testing.
    pub user_table_enabled: bool,
    /// Whether to persist certain system tables that have opted in. This is
    /// extremely experimental and should not even be tried by users. It's
    /// initially here for end-to-end testing.
    pub system_table_enabled: bool,
    /// Unstructured information stored in the "lock" files created by the
    /// log and blob to ensure that they are exclusive writers to those
    /// locations. This should contain whatever information might be useful to
    /// investigating an unexpected lock file (e.g. hostname and materialize
    /// version of the creating process).
    pub lock_info: String,
}

impl PersistConfig {
    pub fn disabled() -> Self {
        PersistConfig {
            log_path: Default::default(),
            blob_path: Default::default(),
            user_table_enabled: false,
            system_table_enabled: false,
            lock_info: Default::default(),
        }
    }

    /// Initializes the persistence runtime and returns a clone-able handle for
    /// interacting with it. Returns None and does not start the runtime if all
    /// persistence features are disabled.
    pub fn init(
        &self,
        catalog_id: Uuid,
        reg: &MetricsRegistry,
    ) -> Result<PersisterWithConfig, Error> {
        let persister = if self.user_table_enabled || self.system_table_enabled {
            let lock_reentrance_id = catalog_id.to_string();
            let lock_info = LockInfo::new(lock_reentrance_id, self.lock_info.clone())?;
            let log = FileLog::new(&self.log_path, lock_info.clone())?;
            let blob = FileBlob::new(&self.blob_path, lock_info)?;
            let persister = runtime::start(log, blob, reg)?;
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
                // TODO: This needs to be written down somewhere in the catalog
                // in case we need to change the naming at some point. See
                // related TODO in Catalog::deserialize_item.
                Some(format!("user-table-{:?}-{}", id, pretty))
            }
            GlobalId::System(id) if self.config.system_table_enabled => {
                // TODO: This needs to be written down somewhere in the catalog
                // in case we need to change the naming at some point. See
                // related TODO in Catalog::deserialize_item.
                Some(format!("system-table-{:?}-{}", id, pretty))
            }
            _ => None,
        }
    }

    pub fn details(&self, id: GlobalId, pretty: &str) -> Result<Option<PersistDetails>, Error> {
        let persister = match self.persister.as_ref() {
            Some(x) => x,
            None => return Ok(None),
        };
        let stream_name = match self.stream_name(id, pretty) {
            Some(x) => x,
            None => return Ok(None),
        };
        let (write_handle, _) = persister.create_or_load(&stream_name)?;
        Ok(Some(PersistDetails {
            stream_name,
            write_handle,
        }))
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct PersistDetails {
    pub stream_name: String,
    #[serde(skip)]
    pub write_handle: StreamWriteHandle<Row, ()>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistMultiDetails {
    pub all_table_ids: Vec<Id>,
    pub write_handle: MultiWriteHandle<Row, ()>,
}
