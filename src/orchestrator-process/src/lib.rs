// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod port_metadata_file;

use std::collections::HashMap;
use std::fmt::Debug;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};

use anyhow::anyhow;
use async_trait::async_trait;
use itertools::Itertools;
use scopeguard::defer;
use sysinfo::{ProcessExt, ProcessStatus, SystemExt};
use tokio::process::{Child, Command};
use tokio::task::JoinHandle;
use tokio::time::{self, Duration};
use tracing::{error, info};

use crate::port_metadata_file::PortMetadataFile;
use mz_orchestrator::{NamespacedOrchestrator, Orchestrator, Service, ServiceConfig};
use mz_ore::id_gen::IdAllocator;
use mz_pid_file::PidFile;

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
    /// The directory in which the orchestrator should look for process
    /// lock files.
    pub data_dir: PathBuf,
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
    data_dir: PathBuf,
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
            data_dir,
        }: ProcessOrchestratorConfig,
    ) -> Result<ProcessOrchestrator, anyhow::Error> {
        Ok(ProcessOrchestrator {
            image_dir: fs::canonicalize(image_dir)?,
            port_allocator,
            suppress_output,
            namespaces: Mutex::new(HashMap::new()),
            process_listen_host: process_listen_host
                .unwrap_or_else(|| ProcessOrchestrator::DEFAULT_LISTEN_HOST.to_string()),
            data_dir: fs::canonicalize(data_dir)?,
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
                data_dir: self.data_dir.clone(),
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
    supervisors: Mutex<HashMap<String, Vec<AbortOnDrop>>>,
    data_dir: PathBuf,
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
            scale: scale_in,
            labels: _,
            availability_zone: _,
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

        let system = sysinfo::System::new_all();

        for i in 0..(scale_in.get()) {
            let process_file_name = format!("{}-{}-{}", self.namespace, id, i);
            let pid_file_location = self.data_dir.join(format!("{}.pid", process_file_name));
            let port_metadata_file_location =
                self.data_dir.join(format!("{}.ports", process_file_name));

            if pid_file_location.exists() {
                let pid = PidFile::read(&pid_file_location)?;
                let port_metadata = PortMetadataFile::read(&port_metadata_file_location)?;
                if let Some(process) = system.process(pid.into()) {
                    if process.exe() == path {
                        if process.status() == ProcessStatus::Dead
                            || process.status() == ProcessStatus::Zombie
                        {
                            // Existing dead process, so we try and kill it and create a new one later
                            process.kill();
                        } else {
                            // Existing non-dead process, so we don't create a new one
                            processes.push(port_metadata);
                            handles.push(AbortOnDrop(Box::new(ExternalProcess {
                                pid,
                                _port_metadata_file: PortMetadataFile::open_existing(
                                    port_metadata_file_location,
                                ),
                            })));
                            continue;
                        }
                    }
                }
            }

            let mut ports = HashMap::new();
            for port in &ports_in {
                let p = self
                    .port_allocator
                    .alloc()
                    .ok_or_else(|| anyhow!("port exhaustion"))?;
                ports.insert(port.name.clone(), p);
            }
            let mut args = args(&ports);
            args.push(format!(
                "--pid-file-location={}",
                pid_file_location.display()
            ));
            processes.push(ports.clone());
            handles.push(AbortOnDrop(Box::new(mz_ore::task::spawn(
                || format!("service-supervisor: {full_id}"),
                supervise(
                    full_id.clone(),
                    path.clone(),
                    args.clone(),
                    Arc::clone(&self.port_allocator),
                    ports,
                    self.suppress_output,
                    port_metadata_file_location,
                ),
            ))));
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
    ports: HashMap<String, i32>,
    suppress_output: bool,
    port_metadata_file_location: PathBuf,
) {
    defer! {
        for port in ports.values() {
            port_allocator.free(*port);
        }
    }
    let _port_metadata_file = PortMetadataFile::open(&port_metadata_file_location, &ports)
        .unwrap_or_else(|_| {
            panic!(
                "unable to create port metadata file {}",
                port_metadata_file_location.as_os_str().to_str().unwrap()
            )
        });
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

trait Abortable: Send + Sync + Debug {
    fn abort_process(&self);
}

impl<T: Send + Sync + Debug> Abortable for JoinHandle<T> {
    fn abort_process(&self) {
        self.abort();
    }
}

#[derive(Debug)]
struct ExternalProcess<P>
where
    P: AsRef<Path> + Debug,
{
    pid: i32,
    _port_metadata_file: PortMetadataFile<P>,
}

impl<P> Abortable for ExternalProcess<P>
where
    P: AsRef<Path> + Debug + Send + Sync,
{
    fn abort_process(&self) {
        let system = sysinfo::System::new_all();
        if let Some(process) = system.process(self.pid.into()) {
            process.kill();
        }
    }
}

#[derive(Debug)]
struct AbortOnDrop(Box<dyn Abortable>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort_process();
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
