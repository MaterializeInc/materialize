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

use anyhow::anyhow;
use persist::error::Error;
use persist::indexed::encoding::Id;
use serde::Serialize;

use expr::GlobalId;
use persist::file::{FileBlob, FileBuffer};
use persist::indexed::runtime::{self, MultiWriteHandle, RuntimeClient, StreamWriteHandle};

/// Configuration of the persistence runtime and features.
#[derive(Clone, Debug)]
pub struct PersistConfig {
    /// A directory under which un-indexed WAL-like writes are quickly stored.
    pub buffer_path: PathBuf,
    /// A directory under which larger batches of indexed data are stored. This
    /// will eventually be S3 for Cloud.
    pub blob_path: PathBuf,
    /// Whether to persist user tables. This is extremely experimental and
    /// should not even be tried by users. It's initially here for end-to-end
    /// testing.
    pub user_table_enabled: bool,
    /// Information stored in the "lock" files created by the buffer and blob to
    /// ensure that they are exclusive writers to those locations. This should
    /// contain whatever information might be useful to investigating an
    /// unexpected lock file (e.g. hostname and materialize version of the
    /// creating process).
    pub lock_info: String,
}

impl PersistConfig {
    pub fn disabled() -> Self {
        PersistConfig {
            buffer_path: Default::default(),
            blob_path: Default::default(),
            user_table_enabled: false,
            lock_info: Default::default(),
        }
    }

    /// Initializes the persistence runtime and returns a clone-able handle for
    /// interacting with it. Returns None and does not start the runtime if all
    /// persistence features are disabled.
    pub fn init(&self) -> Result<PersisterWithConfig, anyhow::Error> {
        let persister = if self.user_table_enabled {
            let buffer = FileBuffer::new(&self.buffer_path, &self.lock_info)
                .map_err(|err| anyhow!("{}", err))?;
            let blob = FileBlob::new(&self.blob_path, &self.lock_info)
                .map_err(|err| anyhow!("{}", err))?;
            let persister = runtime::start(buffer, blob).map_err(|err| anyhow!("{}", err))?;
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
    pub persister: Option<RuntimeClient<Vec<u8>, ()>>,
}

impl PersisterWithConfig {
    fn stream_name(&self, id: GlobalId) -> Option<String> {
        match id {
            GlobalId::User(id) if self.config.user_table_enabled => {
                // TODO: This needs to be written down somewhere in the catalog in case
                // we need to change the naming at some point.
                Some(format!("user-table-{:?}", id))
            }
            GlobalId::System(id) => {
                // TODO: This needs to be written down somewhere in the catalog in case
                // we need to change the naming at some point.
                Some(format!("system-table-{:?}", id))
            }
            _ => None,
        }
    }

    pub fn details(&self, id: GlobalId) -> Result<Option<PersistDetails>, Error> {
        let persister = match self.persister.as_ref() {
            Some(x) => x,
            None => return Ok(None),
        };
        let stream_name = match self.stream_name(id) {
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
    pub write_handle: StreamWriteHandle<Vec<u8>, ()>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistMultiDetails {
    pub all_table_ids: Vec<Id>,
    pub write_handle: MultiWriteHandle<Vec<u8>, ()>,
}
