// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// `PersistentTables` is a wrapper that encapsulates what the Coordinator needs
/// to do to properly write data to persistent storage.
///
/// The intention here is to wrap the WAL and Compacter interactions in a simple
/// API that insulates the Coordinator against potential errors and allows us to
/// disable table persistence after any error.
use std::path::PathBuf;

use log::{debug, error};
use timely::progress::Antichain;
use tokio::sync::mpsc;

use dataflow_types::Update;
use expr::GlobalId;
use repr::Timestamp;
use storage::{Compacter, CompacterMessage, Message, Trace, WriteAheadLogs};

#[derive(Clone, Debug)]
pub struct PersistenceConfig {
    pub wals_path: PathBuf,
    pub traces_path: PathBuf,
}

pub struct PersistentTables {
    // Map of each table's write-ahead log
    wals: WriteAheadLogs,
    // Channel to send table creations, deletions and other messages to the
    // compacter
    compacter_tx: mpsc::UnboundedSender<CompacterMessage>,
    // Path where all WAL files are stored
    wals_path: PathBuf,
    // Path where all Batch data are stored.
    traces_path: PathBuf,
    // Flag that indicates whether we've encountered an error while trying to
    // persist things. If true, every subsequent operation will be a no-op.
    disabled: bool,
}

impl PersistentTables {
    /// Create the persistent tables subsystem.
    ///
    /// Have to spawn a Compacter task and initialize a map of WALs.
    pub fn new(config: &PersistenceConfig) -> Option<Self> {
        let (compacter_tx, compacter_rx) = mpsc::unbounded_channel();
        let mut compacter = match Compacter::new(
            compacter_rx,
            config.traces_path.clone(),
            config.wals_path.clone(),
        ) {
            Ok(compacter) => compacter,
            Err(e) => {
                error!(
                    "Encountered error while trying to initialize compacter task: {:#}",
                    e
                );
                error!("No further records will be persisted.");
                return None;
            }
        };
        tokio::spawn(async move { compacter.run().await });
        let wals = match WriteAheadLogs::new(config.wals_path.clone()) {
            Ok(wals) => wals,
            Err(e) => {
                error!(
                    "encountered error while trying to initialize write-ahead log: {:#}",
                    e
                );
                error!("No additional records will be persisted.");
                return None;
            }
        };

        Some(Self {
            wals,
            compacter_tx,
            wals_path: config.wals_path.clone(),
            traces_path: config.traces_path.clone(),
            disabled: false,
        })
    }

    /// Permanently shut off table persistence.
    fn disable(&mut self) {
        self.disabled = true;
        error!("No additional records will be persisted");
    }

    /// Reload all of the persisted data for `id` into memory.
    ///
    /// Reinstantiate a Trace, reread all of the batch data and unused WAL segments,
    /// and tell the compacter to resume managing that trace.
    pub fn resume(&mut self, id: GlobalId) -> Option<Vec<Message>> {
        if self.disabled {
            return None;
        }

        let persisted_trace = match Trace::resume(id, &self.traces_path, &self.wals_path) {
            Ok(trace) => trace,
            Err(e) => {
                error!(
                    "encountered error while trying to restart table {}: {:#}",
                    id, e
                );
                self.disable();
                return None;
            }
        };

        let messages = match persisted_trace.read() {
            Ok(messages) => Some(messages),
            Err(e) => {
                error!(
                    "encountered error trying to read from persisted table {}: {:#}",
                    id, e
                );
                self.disable();
                return None;
            }
        };

        if let Err(e) = self.wals.resume(id) {
            error!(
                "encountered error trying to resume write-ahead log for persisted table {}: {:#}",
                id, e
            );
            self.disable();
            return None;
        };

        if let Err(e) = self
            .compacter_tx
            .send(CompacterMessage::Resume(id, persisted_trace))
        {
            debug!("compacter dropped, disabling WAL: {}", e);
            self.disable();
            return None;
        }

        messages
    }

    /// Write progress updates to all table WALs.
    pub fn write_progress(&mut self, timestamp: Timestamp) {
        if self.disabled {
            return;
        }

        if let Err(e) = self.wals.write_progress(timestamp) {
            error!(
                "encountered error trying to write progress at ts {}: {:#}",
                timestamp, e
            );
            self.disable();
        }
    }

    /// Start persisting table `id`.
    ///
    /// Create a new WAL map entry and directory for `id` and tell the compacter
    /// to start managing it.
    pub fn create(&mut self, id: GlobalId) {
        if self.disabled {
            return;
        }

        if let Err(e) = self.wals.create(id) {
            error!(
                "encountered error creating table for relation {}: {:#}",
                id, e
            );
            self.disable();
            return;
        }
        if let Err(e) = self.compacter_tx.send(CompacterMessage::Add(id)) {
            error!("compacter dropped, disabling WAL: {:#}", e);
            self.disable();
        }
    }

    /// Stop persisting table `id`.
    ///
    /// Tell the Compacter to drop relation `id` and also delete the persisted WAL
    /// and batch files. Also, remove the relation from the table from the in-memory
    /// WAL map.
    pub fn destroy(&mut self, id: GlobalId) {
        if self.disabled {
            return;
        }

        if let Err(e) = self.wals.destroy(id) {
            error!(
                "encountered error destroying table for relation {}: {:#}",
                id, e
            );
            self.disable();
            return;
        }
        if let Err(e) = self.compacter_tx.send(CompacterMessage::Drop(id)) {
            error!("compacter dropped, disabling WAL: {:#}", e);
            self.disable();
        }
    }

    /// Write updates to the WAL.
    pub fn write(&mut self, id: GlobalId, updates: &[Update]) {
        if self.disabled {
            return;
        }
        if let Err(e) = self.wals.write(id, updates) {
            error!(
                "encountered error writing to table for relation {}: {:#}",
                id, e
            );
            self.disable();
        }
    }

    /// Tell the Compacter to advance the compaction frontiers.
    ///
    /// Note that we might send frontier updates for relations that are not persisted
    /// and the Compacter knows to ignore that.
    pub fn allow_compaction(&mut self, since_updates: &[(GlobalId, Antichain<Timestamp>)]) {
        if self.disabled {
            return;
        }

        for (id, times) in since_updates {
            let times_list = times.elements();

            if times_list.len() == 1 {
                if let Err(e) = self
                    .compacter_tx
                    .send(CompacterMessage::AllowCompaction(*id, times_list[0]))
                {
                    error!("compacter dropped, disabling WAL: {:#}", e);
                    self.disable();
                }
            }
        }
    }
}
