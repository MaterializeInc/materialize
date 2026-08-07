// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Configuration parameter types.

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

use mz_dyncfg::ConfigSet;

use crate::{connections::ConnectionContext, parameters::StorageParameters};

/// A struct representing the _entirety_ of configuration required for interacting with storage APIs.
///
/// Portions of this struct are mutable, but it remains _clone-able_ so it can be moved between
/// tasks.
///
/// Usable within clusterd and environmentd.
#[derive(Debug, Clone)]
pub struct StorageConfiguration {
    /// Mutable, LD-controlled parameters related to upstream storage connections,
    /// persist, and rendering of dataflows.
    ///
    /// This type can be serialized and copied from environmentd to clusterd, and can
    /// be merged into a `StorageConfiguration` with `StorageConfiguration::update`.
    pub parameters: StorageParameters,

    /// Immutable, CLI-configured parameters.
    ///
    /// TODO(guswynn): `ConnectionContext` also contains some shared global state that should
    /// eventually be moved up to this struct.
    pub connection_context: ConnectionContext,

    /// A clone-able `mz_dyncfg::ConfigSet` used to access dyncfg values.
    config_set: Arc<ConfigSet>,
}

impl StorageConfiguration {
    /// Instantiate a new `StorageConfiguration` with default parameters and the given context.
    pub fn new(
        connection_context: ConnectionContext,
        config_set: ConfigSet,
    ) -> StorageConfiguration {
        StorageConfiguration {
            parameters: Default::default(),
            connection_context,
            config_set: Arc::new(config_set),
        }
    }

    /// Get a reference to the shared `ConfigSet`.
    pub fn config_set(&self) -> &Arc<ConfigSet> {
        &self.config_set
    }

    pub fn update(&mut self, parameters: StorageParameters) {
        // We serialize the dyncfg updates in StorageParameters, but store the config set
        // top-level. Eventually, all of `StorageParameters` goes away.
        parameters.dyncfg_updates.apply(&self.config_set);
        self.parameters.update(parameters);
    }
}

/// Replica configuration
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageReplicaConfig {
    /// TODO(database-issues#7533): Add documentation.
    pub logging: StorageReplicaLogging,
}

/// Logging configuration of a replica.
#[derive(
    Clone,
    Debug,
    Default,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize
)]
pub struct StorageReplicaLogging {
    /// Whether to enable logging for the logging dataflows.
    pub log_logging: bool,
    /// The interval at which to log.
    ///
    /// A `None` value indicates that logging is disabled.
    pub interval: Option<Duration>,
}

impl StorageReplicaLogging {
    /// Return whether logging is enabled.
    pub fn enabled(&self) -> bool {
        self.interval.is_some()
    }
}
