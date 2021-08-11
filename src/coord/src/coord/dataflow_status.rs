// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types and methods for working with status updates from the dataflow layer.

use std::collections::HashMap;

use dataflow_types::DataflowStatusUpdate;
use expr::GlobalId;

/// A buffer for [`status updates`](DataflowStatusUpdate) coming in from
/// workers/the dataflow layer.
///
/// We need to stash updates on created dataflows until we received the same
/// update from all workers.
pub struct DataflowStatusUpdateBuffer {
    created_views: HashMap<GlobalId, usize>,
    created_sinks: HashMap<GlobalId, usize>,
    num_workers: usize,
}

impl DataflowStatusUpdateBuffer {
    /// Creates a new [`DataflowStatusUpdateBuffer`] that expects updates
    /// from the given number of workers.
    pub fn new(num_workers: usize) -> Self {
        DataflowStatusUpdateBuffer {
            num_workers,
            created_views: HashMap::new(),
            created_sinks: HashMap::new(),
        }
    }

    /// Updates internal state based on the given `update`. If we now have
    /// updates from the expected number of workers for any dataflow, returns
    /// an update that signals "global" updates for those dataflows.
    pub fn add_update(&mut self, update: DataflowStatusUpdate) -> Option<DataflowStatusUpdate> {
        match update {
            DataflowStatusUpdate::CreatedDataflows {
                created_indexes,
                created_sinks,
            } => self.process_created_dataflows(created_indexes, created_sinks),
        }
    }

    fn process_created_dataflows(
        &mut self,
        created_indexes: Vec<GlobalId>,
        created_sinks: Vec<GlobalId>,
    ) -> Option<DataflowStatusUpdate> {
        let mut result_created_indexes = Vec::new();
        let mut result_created_sinks = Vec::new();

        for id in created_indexes {
            let num_updates = self
                .created_views
                .entry(id.clone())
                .and_modify(|count| *count += 1)
                .or_insert(1);

            assert!(
                *num_updates <= self.num_workers,
                "got more updates than expected from workers"
            );
            if *num_updates == self.num_workers {
                result_created_indexes.push(id);
            }
        }
        for id in created_sinks {
            let num_updates = self
                .created_sinks
                .entry(id.clone())
                .and_modify(|count| *count += 1)
                .or_insert(1);

            assert!(
                *num_updates <= self.num_workers,
                "got more updates than expected from workers"
            );
            if *num_updates == self.num_workers {
                result_created_sinks.push(id);
            }
        }

        if !result_created_indexes.is_empty() || !result_created_sinks.is_empty() {
            Some(DataflowStatusUpdate::CreatedDataflows {
                created_indexes: result_created_indexes,
                created_sinks: result_created_sinks,
            })
        } else {
            None
        }
    }
}
