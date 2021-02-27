// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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
    wals: WriteAheadLogs,
    compacter_tx: mpsc::UnboundedSender<CompacterMessage>,
    wals_path: PathBuf,
    traces_path: PathBuf,
    disabled: bool,
}

impl PersistentTables {
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
                    "Encountered error while trying to initialize compacter task: {}",
                    e
                );
                return None;
            }
        };
        tokio::spawn(async move { compacter.run().await });
        let wals = match WriteAheadLogs::new(config.wals_path.clone()) {
            Ok(wals) => wals,
            Err(e) => {
                error!(
                    "encountered error while trying to initialize write-ahead log: {}",
                    e
                );
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

    pub fn resume(&mut self, id: GlobalId) -> Option<Vec<Message>> {
        if self.disabled {
            return None;
        }

        let persisted_trace = match Trace::resume(id, &self.traces_path, &self.wals_path) {
            Ok(trace) => trace,
            Err(e) => {
                error!(
                    "encountered error while trying to restart table {} {}",
                    id, e
                );
                self.disabled = true;
                return None;
            }
        };

        let messages = match persisted_trace.read() {
            Ok(messages) => Some(messages),
            Err(e) => {
                error!(
                    "encountered error trying to read from persisted table {} {}",
                    id, e
                );
                None
            }
        };

        self.compacter_tx
            .send(CompacterMessage::Resume(id, persisted_trace))
            .unwrap();

        messages
    }

    pub fn write_progress(&mut self, timestamp: Timestamp) {
        if self.disabled {
            return;
        }

        if let Err(e) = self.wals.write_progress(timestamp) {
            error!(
                "encountered error trying to write progress at ts {}: {}",
                timestamp, e
            );
            self.disabled = true;
        }
    }

    pub fn create(&mut self, id: GlobalId) {
        if self.disabled {
            return;
        }

        if let Err(e) = self.wals.create(id) {
            error!(
                "encountered error creating table for relation {}: {}",
                id, e
            );
            self.disabled = true;
            return;
        }
        if let Err(e) = self.compacter_tx.send(CompacterMessage::Add(id)) {
            debug!("compacter dropped, disabling WAL: {}", e);
            self.disabled = true;
        }
    }

    pub fn destroy(&mut self, id: GlobalId) {
        if self.disabled {
            return;
        }

        if let Err(e) = self.wals.destroy(id) {
            error!(
                "encountered error destroying table for relation {}: {}",
                id, e
            );
            self.disabled = true;
            return;
        }
        if let Err(e) = self.compacter_tx.send(CompacterMessage::Drop(id)) {
            debug!("compacter dropped, disabling WAL: {}", e);
            self.disabled = true;
        }
    }

    pub fn write(&mut self, id: GlobalId, updates: &[Update]) {
        if self.disabled {
            return;
        }
        if let Err(e) = self.wals.write(id, updates) {
            error!(
                "encountered error writing to table for relation {}: {}",
                id, e
            );
            self.disabled = true;
        }
    }

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
                    debug!("compacter dropped, disabling WAL: {}", e);
                    self.disabled = true;
                }
            }
        }
    }
}
