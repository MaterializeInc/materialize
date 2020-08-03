// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::thread;
use std::time::Duration;

use futures::executor::block_on;
use futures::stream::StreamExt;
use log::info;

use dataflow_types::{Persistence, WorkerPersistenceData};
use expr::GlobalId;

#[derive(Debug)]
pub enum PersistenceMetadata {
    AddSource(GlobalId, Persistence),
    DropSource(GlobalId),
}

pub struct Persister {
    data_rx: comm::mpsc::Receiver<WorkerPersistenceData>,
    metadata_rx: std::sync::mpsc::Receiver<PersistenceMetadata>,
}

impl Persister {
    pub fn new(
        data_rx: comm::mpsc::Receiver<WorkerPersistenceData>,
        metadata_rx: std::sync::mpsc::Receiver<PersistenceMetadata>,
    ) -> Self {
        Persister {
            data_rx,
            metadata_rx,
        }
    }

    pub fn update(&mut self) {
        loop {
            thread::sleep(Duration::from_secs(5));
            info!("persister thread awoke");
            self.update_persistence();
        }
    }

    fn update_persistence(&mut self) {
        while let Some(_) = block_on(self.data_rx.next()) {
            log::info!("received some data to be persisted");
        }
    }
}
