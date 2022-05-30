// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::fmt;
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use bytesize::ByteSize;
use derivative::Derivative;
use serde::de::Unexpected;
use serde::{Deserialize, Deserializer, Serialize};

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
pub trait Orchestrator: fmt::Debug + Send + Sync {
    // Default host used to bind to.
    fn listen_host(&self) -> &str;

    /// Enter a namespace in the orchestrator.
    fn namespace(&self, namespace: &str) -> Arc<dyn NamespacedOrchestrator>;
}

/// An orchestrator restricted to a single namespace.
#[async_trait]
pub trait NamespacedOrchestrator: fmt::Debug + Send + Sync {
    /// Ensures that a service with the given configuration is running.
    ///
    /// If a service with the same ID already exists, its configuration is
    /// updated to match `config`. This may or may not involve restarting the
    /// service, depending on whether the existing service matches `config`.
    async fn ensure_service(
        &self,
        id: &str,
        config: ServiceConfig<'_>,
    ) -> Result<Box<dyn Service>, anyhow::Error>;

    /// Drops the identified service, if it exists.
    async fn drop_service(&self, id: &str) -> Result<(), anyhow::Error>;

    /// Lists the identifiers of all known services.
    async fn list_services(&self) -> Result<Vec<String>, anyhow::Error>;
}

/// Describes a running service managed by an `Orchestrator`.
pub trait Service: fmt::Debug {
    /// Given the name of a port, returns the addresses for each of the
    /// service's processes, in order.
    ///
    /// Panics if `port` does not name a valid port.
    fn addresses(&self, port: &str) -> Vec<String>;
}

/// Describes the desired state of a service.
#[derive(Derivative, Clone)]
#[derivative(Debug)]
pub struct ServiceConfig<'a> {
    /// An opaque identifier for the executable or container image to run.
    ///
    /// Often names a container on Docker Hub or a path on the local machine.
    pub image: String,
    /// A function that generates the arguments for each process of the service
    /// given various information about the process to be configured.
    ///
    /// The first argument is the port mappings for each host; the second mapping is the
    /// port mappings for the host or set of hosts presently being configured.
    ///
    /// The third argument is the index of this process in the service, _if available_.
    /// It is not available from all orchestrators; for example, the Kubernetes orchestrator
    /// configures the arguments for all processes at once.
    #[derivative(Debug = "ignore")]
    pub args: &'a (dyn Fn(
        &[(String, HashMap<String, u16>)],
        &HashMap<String, u16>,
        Option<usize>,
    ) -> Vec<String>
             + Send
             + Sync),
    /// Ports to expose.
    pub ports: Vec<ServicePort>,
    /// An optional limit on the memory that the service can use.
    pub memory_limit: Option<MemoryLimit>,
    /// An optional limit on the CPU that the service can use.
    pub cpu_limit: Option<CpuLimit>,
    /// The number of copies of this service to run.
    pub scale: NonZeroUsize,
    /// Arbitrary key–value pairs to attach to the service in the orchestrator
    /// backend.
    ///
    /// The orchestrator backend may apply a prefix to the key if appropriate.
    pub labels: HashMap<String, String>,
    /// The availability zone the service should be run in. If no availability
    /// zone is specified, the orchestrator is free to choose one.
    pub availability_zone: Option<String>,
}

/// A named port associated with a service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServicePort {
    /// A descriptive name for the port.
    ///
    /// Note that not all orchestrator backends make use of port names.
    pub name: String,
    /// The desired port number.
    ///
    /// Not all orchestrator backends will make use of the hint.
    pub port_hint: u16,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct MemoryLimit(pub ByteSize);

impl<'de> Deserialize<'de> for MemoryLimit {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        <String as Deserialize>::deserialize(deserializer)
            .and_then(|s| {
                ByteSize::from_str(&s).map_err(|_e| {
                    use serde::de::Error;
                    D::Error::invalid_value(serde::de::Unexpected::Str(&s), &"valid size in bytes")
                })
            })
            .map(MemoryLimit)
    }
}

impl Serialize for MemoryLimit {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        <String as Serialize>::serialize(&self.0.to_string(), serializer)
    }
}

/// Describes a limit on CPU resources.
#[derive(Debug, Copy, Clone, Eq, Ord, PartialEq, PartialOrd)]
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

impl<'de> Deserialize<'de> for CpuLimit {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Note -- we just round off any precision beyond 0.001 here.
        let float = f64::deserialize(deserializer)?;
        let millicpus = (float * 1000.).round();
        if millicpus < 0. || millicpus > (std::usize::MAX as f64) {
            use serde::de::Error;
            Err(D::Error::invalid_value(
                Unexpected::Float(float),
                &"a float representing a plausible number of CPUs",
            ))
        } else {
            Ok(Self {
                millicpus: millicpus as usize,
            })
        }
    }
}

impl Serialize for CpuLimit {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        <f64 as Serialize>::serialize(&(self.millicpus as f64 / 1000.0), serializer)
    }
}
