// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;

use async_trait::async_trait;
use dyn_clonable::clonable;

/// An orchestrator manages services.
///
/// A service is a set of one or more processes running the same image. See
/// [`ServiceConfig`] for details.
///
/// All services live within a namespace. A namespace allows multiple users to
/// share an orchestrator without conflicting: each user can only create,
/// delete, and list the services within their namespace. Namespaces are not
/// isolated at the network level, however: services in one namespace can
/// communicate with services in another namespace with no restrictions.
///
/// The intent is that you can implement `Orchestrator` with pods in Kubernetes,
/// containers in Docker, or processes on your local machine.
#[clonable]
pub trait Orchestrator: fmt::Debug + Clone + Send {
    /// Enter a namespace in the orchestrator.
    fn namespace(&self, namespace: &str) -> Box<dyn NamespacedOrchestrator>;
}

/// An orchestrator restricted to a single namespace.
#[clonable]
#[async_trait]
pub trait NamespacedOrchestrator: fmt::Debug + Clone + Send {
    /// Ensures that a service with the given configuration is running.
    ///
    /// If a service with the same ID already exists, its configuration is
    /// updated to match `config`. This may or may not involve restarting the
    /// service, depending on whether the existing service matches `config`.
    async fn ensure_service(
        &mut self,
        id: &str,
        config: ServiceConfig,
    ) -> Result<Box<dyn Service>, anyhow::Error>;

    /// Drops the identified service, if it exists.
    async fn drop_service(&mut self, id: &str) -> Result<(), anyhow::Error>;

    /// Lists the identifiers of all known services.
    async fn list_services(&self) -> Result<Vec<String>, anyhow::Error>;
}

/// Describes a running service managed by an `Orchestrator`.
pub trait Service: fmt::Debug {
    /// Returns the hostnames for each of the service's processes, in order.
    fn hosts(&self) -> Vec<String>;
}

/// Describes the desired state of a service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceConfig {
    /// An opaque identifier for the executable or container image to run.
    ///
    /// Often names a container on Docker Hub or a path on the local machine.
    pub image: String,
    /// Arguments for the process.
    pub args: Vec<String>,
    /// Ports to expose.
    pub ports: Vec<i32>,
    /// An optional limit on the memory that the service can use.
    pub memory_limit: Option<MemoryLimit>,
    /// An optional limit on the CPU that the service can use.
    pub cpu_limit: Option<CpuLimit>,
    /// The number of processes to run.
    pub processes: usize,
}

/// Describes a limit on memory resources.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryLimit {
    bytes: usize,
}

impl MemoryLimit {
    /// Constructs a new memory limit from a number of bytes.
    pub fn from_bytes(&self, bytes: usize) -> MemoryLimit {
        MemoryLimit { bytes }
    }

    /// Returns the memory limit in bytes.
    pub fn as_bytes(&self) -> usize {
        self.bytes
    }
}

/// Describes a limit on CPU resources.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CpuLimit {
    millicpus: usize,
}

impl CpuLimit {
    /// Constructs a new CPU limit from a number of millicpus.
    pub fn from_millicpus(&self, millicpus: usize) -> CpuLimit {
        CpuLimit { millicpus }
    }

    /// Returns the CPU limit in millicpus.
    pub fn as_millicpus(&self) -> usize {
        self.millicpus
    }
}
