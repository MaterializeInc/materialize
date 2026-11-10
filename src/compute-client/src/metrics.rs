// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Metrics for compute commands and their retained history.

use std::borrow::Borrow;

use crate::protocol::command::ComputeCommand;

/// Metrics keyed by `ComputeCommand` type.
#[derive(Clone, Debug)]
pub struct CommandMetrics<M> {
    /// Metrics for `Hello`.
    pub hello: M,
    /// Metrics for query-connection setup.
    pub hello_query: M,
    /// Metrics for connection-local query result limits.
    pub set_query_max_result_size: M,
    /// Metrics for query-local dataflow creation.
    pub create_query_dataflow: M,
    /// Metrics for lifecycle catalog application.
    pub apply_catalog_position: M,
    /// Metrics for `CreateInstance`.
    pub create_instance: M,
    /// Metrics for `CreateDataflow`.
    pub create_dataflow: M,
    /// Metrics for `Schedule`.
    pub schedule: M,
    /// Metrics for `AllowCompaction`.
    pub allow_compaction: M,
    /// Metrics for `Peek`.
    pub peek: M,
    /// Metrics for `CancelPeek`.
    pub cancel_peek: M,
    /// Metrics for `InitializationComplete`.
    pub initialization_complete: M,
    /// Metrics for `UpdateConfiguration`.
    pub update_configuration: M,
    /// Metrics for `AllowWrites`.
    pub allow_writes: M,
}

impl<M> CommandMetrics<M> {
    /// TODO(database-issues#7533): Add documentation.
    pub fn build<F>(build_metric: F) -> Self
    where
        F: Fn(&str) -> M,
    {
        Self {
            hello: build_metric("hello"),
            hello_query: build_metric("hello_query"),
            set_query_max_result_size: build_metric("set_query_max_result_size"),
            create_query_dataflow: build_metric("create_query_dataflow"),
            apply_catalog_position: build_metric("apply_catalog_position"),
            create_instance: build_metric("create_instance"),
            create_dataflow: build_metric("create_dataflow"),
            schedule: build_metric("schedule"),
            allow_compaction: build_metric("allow_compaction"),
            peek: build_metric("peek"),
            cancel_peek: build_metric("cancel_peek"),
            initialization_complete: build_metric("initialization_complete"),
            update_configuration: build_metric("update_configuration"),
            allow_writes: build_metric("allow_writes"),
        }
    }

    fn for_all<F>(&self, f: F)
    where
        F: Fn(&M),
    {
        f(&self.hello);
        f(&self.hello_query);
        f(&self.set_query_max_result_size);
        f(&self.create_query_dataflow);
        f(&self.apply_catalog_position);
        f(&self.create_instance);
        f(&self.initialization_complete);
        f(&self.update_configuration);
        f(&self.create_dataflow);
        f(&self.schedule);
        f(&self.allow_compaction);
        f(&self.peek);
        f(&self.cancel_peek);
        f(&self.allow_writes);
    }

    /// TODO(database-issues#7533): Add documentation.
    pub fn for_command(&self, command: &ComputeCommand) -> &M {
        use ComputeCommand::*;

        match command {
            Hello { .. } => &self.hello,
            HelloQuery { .. } => &self.hello_query,
            SetQueryMaxResultSize { .. } => &self.set_query_max_result_size,
            CreateQueryDataflow { .. } => &self.create_query_dataflow,
            ApplyCatalogPosition(_) => &self.apply_catalog_position,
            CreateInstance(_) => &self.create_instance,
            InitializationComplete => &self.initialization_complete,
            UpdateConfiguration(_) => &self.update_configuration,
            CreateDataflow(_) => &self.create_dataflow,
            Schedule(_) => &self.schedule,
            AllowCompaction { .. } => &self.allow_compaction,
            Peek(_) => &self.peek,
            CancelPeek { .. } => &self.cancel_peek,
            AllowWrites { .. } => &self.allow_writes,
        }
    }
}

/// Metrics tracked by the command history.
#[derive(Debug)]
pub struct HistoryMetrics<G> {
    /// Metrics tracking command counts.
    pub command_counts: CommandMetrics<G>,
    /// Metric tracking the dataflow count.
    pub dataflow_count: G,
}

impl<G> HistoryMetrics<G>
where
    G: Borrow<mz_ore::metrics::UIntGauge>,
{
    /// Reset all tracked counts to 0.
    pub fn reset(&self) {
        self.command_counts.for_all(|m| m.borrow().set(0));
        self.dataflow_count.borrow().set(0);
    }
}
