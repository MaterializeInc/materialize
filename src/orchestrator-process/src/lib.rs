// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{Arc, Mutex};

use anyhow::anyhow;
use async_trait::async_trait;
use itertools::Itertools;
use scopeguard::defer;
use tokio::process::{Child, Command};
use tokio::task::JoinHandle;
use tokio::time::{self, Duration};
use tracing::{error, info};

use mz_orchestrator::{NamespacedOrchestrator, Orchestrator, Service, ServiceConfig};
use mz_ore::id_gen::IdAllocator;

/// Configures a [`ProcessOrchestrator`].
#[derive(Debug, Clone)]
pub struct ProcessOrchestratorConfig {
    /// The directory in which the orchestrator should look for executable
    /// images.
    pub image_dir: PathBuf,
    /// The ports to allocate.
    pub port_allocator: Arc<IdAllocator<i32>>,
    /// Whether to supress output from spawned subprocesses.
    pub suppress_output: bool,
    /// The host spawned subprocesses bind to.
    pub process_listen_host: Option<String>,
}

/// An orchestrator backed by processes on the local machine.
///
/// **This orchestrator is for development only.** Due to limitations in the
/// Unix process API, it does not exactly conform to the documented semantics
/// of `Orchestrator`.
#[derive(Debug)]
pub struct ProcessOrchestrator {
    image_dir: PathBuf,
    port_allocator: Arc<IdAllocator<i32>>,
    suppress_output: bool,
    namespaces: Mutex<HashMap<String, Arc<dyn NamespacedOrchestrator>>>,
    process_listen_host: String,
}

impl ProcessOrchestrator {
    const DEFAULT_LISTEN_HOST: &'static str = "127.0.0.1";
    /// Creates a new process orchestrator from the provided configuration.
    pub async fn new(
        ProcessOrchestratorConfig {
            image_dir,
            port_allocator,
            suppress_output,
            process_listen_host,
        }: ProcessOrchestratorConfig,
    ) -> Result<ProcessOrchestrator, anyhow::Error> {
        Ok(ProcessOrchestrator {
            image_dir: fs::canonicalize(image_dir)?,
            port_allocator,
            suppress_output,
            namespaces: Mutex::new(HashMap::new()),
            process_listen_host: process_listen_host
                .unwrap_or_else(|| ProcessOrchestrator::DEFAULT_LISTEN_HOST.to_string()),
        })
    }
}

impl Orchestrator for ProcessOrchestrator {
    fn listen_host(&self) -> &str {
        &self.process_listen_host
    }
    fn namespace(&self, namespace: &str) -> Arc<dyn NamespacedOrchestrator> {
        let mut namespaces = self.namespaces.lock().expect("lock poisoned");
        Arc::clone(namespaces.entry(namespace.into()).or_insert_with(|| {
            Arc::new(NamespacedProcessOrchestrator {
                namespace: namespace.into(),
                image_dir: self.image_dir.clone(),
                port_allocator: Arc::clone(&self.port_allocator),
                suppress_output: self.suppress_output,
                supervisors: Mutex::new(HashMap::new()),
            })
        }))
    }
}

#[derive(Debug)]
struct NamespacedProcessOrchestrator {
    namespace: String,
    image_dir: PathBuf,
    port_allocator: Arc<IdAllocator<i32>>,
    suppress_output: bool,
    supervisors: Mutex<HashMap<String, Vec<AbortOnDrop<()>>>>,
}

#[async_trait]
impl NamespacedOrchestrator for NamespacedProcessOrchestrator {
    async fn ensure_service(
        &self,
        id: &str,
        ServiceConfig {
            image,
            args,
            ports: ports_in,
            memory_limit: _,
            cpu_limit: _,
            processes: processes_in,
            labels: _,
        }: ServiceConfig<'_>,
    ) -> Result<Box<dyn Service>, anyhow::Error> {
        let full_id = format!("{}-{}", self.namespace, id);
        let mut supervisors = self.supervisors.lock().expect("lock poisoned");
        if supervisors.contains_key(id) {
            unimplemented!("ProcessOrchestrator does not yet support updating existing services");
        }
        let path = self.image_dir.join(image);
        let mut processes = vec![];
        let mut handles = vec![];
        for _ in 0..(processes_in.get()) {
            let mut ports = HashMap::new();
            for port in &ports_in {
                let p = self
                    .port_allocator
                    .alloc()
                    .ok_or_else(|| anyhow!("port exhaustion"))?;
                ports.insert(port.name.clone(), p);
            }
            let args = args(&ports);
            processes.push(ports.clone());
            handles.push(AbortOnDrop(mz_ore::task::spawn(
                || format!("service-supervisor: {full_id}"),
                supervise(
                    full_id.clone(),
                    path.clone(),
                    args.clone(),
                    Arc::clone(&self.port_allocator),
                    ports.values().cloned().collect(),
                    self.suppress_output,
                ),
            )));
        }
        supervisors.insert(id.into(), handles);
        Ok(Box::new(ProcessService { processes }))
    }

    async fn drop_service(&self, id: &str) -> Result<(), anyhow::Error> {
        let mut supervisors = self.supervisors.lock().expect("lock poisoned");
        supervisors.remove(id);
        Ok(())
    }

    async fn list_services(&self) -> Result<Vec<String>, anyhow::Error> {
        let supervisors = self.supervisors.lock().expect("lock poisoned");
        Ok(supervisors.keys().cloned().collect())
    }
}

async fn supervise(
    full_id: String,
    path: PathBuf,
    args: Vec<String>,
    port_allocator: Arc<IdAllocator<i32>>,
    ports: Vec<i32>,
    suppress_output: bool,
) {
    defer! {
        for port in ports {
            port_allocator.free(port);
        }
    }
    loop {
        info!(
            "Launching {}: {} {}...",
            full_id,
            path.display(),
            args.iter().join(" ")
        );
        let mut cmd = Command::new(&path);
        cmd.args(&args);
        if suppress_output {
            cmd.stdout(Stdio::null());
            cmd.stderr(Stdio::null());
        }
        match cmd.spawn() {
            Ok(process) => {
                let status = KillOnDropChild(process).0.wait().await;
                error!("{} exited: {:?}; relaunching in 5s", full_id, status);
            }
            Err(e) => {
                error!("{} failed to launch: {}; relaunching in 5s", full_id, e);
            }
        };
        time::sleep(Duration::from_secs(5)).await;
    }
}

struct KillOnDropChild(Child);

impl Drop for KillOnDropChild {
    fn drop(&mut self) {
        let _ = self.0.start_kill();
    }
}

#[derive(Debug)]
struct AbortOnDrop<T>(JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[derive(Debug, Clone)]
struct ProcessService {
    /// For each process in order, the allocated ports by name.
    processes: Vec<HashMap<String, i32>>,
}

impl Service for ProcessService {
    fn addresses(&self, port: &str) -> Vec<String> {
        self.processes
            .iter()
            .map(|p| format!("localhost:{}", p[port]))
            .collect()
    }
}
