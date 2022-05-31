// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::fmt::Debug;
use std::fs;
use std::net::{IpAddr, Ipv4Addr};
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

use mz_orchestrator::{
    NamespacedOrchestrator, Orchestrator, Service, ServiceAssignments, ServiceConfig,
};
use mz_ore::id_gen::PortAllocator;
use mz_pid_file::PidFile;

use crate::port_metadata_file::PortMetadataFile;

pub mod port_metadata_file;

/// Configures a [`ProcessOrchestrator`].
#[derive(Debug, Clone)]
pub struct ProcessOrchestratorConfig {
    /// The directory in which the orchestrator should look for executable
    /// images.
    pub image_dir: PathBuf,
    /// The ports to allocate.
    pub port_allocator: Arc<PortAllocator>,
    /// Whether to supress output from spawned subprocesses.
    pub suppress_output: bool,
    /// The directory in which the orchestrator should look for process
    /// lock files.
    pub data_dir: PathBuf,
    /// A command to wrap the child command invocation
    pub command_wrapper: Vec<String>,
}

/// An orchestrator backed by processes on the local machine.
///
/// **This orchestrator is for development only.** Due to limitations in the
/// Unix process API, it does not exactly conform to the documented semantics
/// of `Orchestrator`.
///
/// Processes launched by this orchestrator must support a `--pid-file-location`
/// command line flag which causes a PID file to be emitted at the specified
/// path.
#[derive(Debug)]
pub struct ProcessOrchestrator {
    image_dir: PathBuf,
    port_allocator: Arc<PortAllocator>,
    suppress_output: bool,
    namespaces: Mutex<HashMap<String, Arc<dyn NamespacedOrchestrator>>>,
    data_dir: PathBuf,
    command_wrapper: Vec<String>,
}

impl ProcessOrchestrator {
    /// Creates a new process orchestrator from the provided configuration.
    pub async fn new(
        ProcessOrchestratorConfig {
            image_dir,
            port_allocator,
            suppress_output,
            data_dir,
            command_wrapper,
        }: ProcessOrchestratorConfig,
    ) -> Result<ProcessOrchestrator, anyhow::Error> {
        Ok(ProcessOrchestrator {
            image_dir: fs::canonicalize(image_dir)?,
            port_allocator,
            suppress_output,
            namespaces: Mutex::new(HashMap::new()),
            data_dir: fs::canonicalize(data_dir)?,
            command_wrapper,
        })
    }
}

impl Orchestrator for ProcessOrchestrator {
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
                command_wrapper: self.command_wrapper.clone(),
            })
        }))
    }
}

#[derive(Debug)]
struct NamespacedProcessOrchestrator {
    namespace: String,
    image_dir: PathBuf,
    port_allocator: Arc<PortAllocator>,
    suppress_output: bool,
    supervisors: Mutex<HashMap<String, Vec<AbortOnDrop>>>,
    data_dir: PathBuf,
    command_wrapper: Vec<String>,
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

        // Decide on port mappings, and detect any non-dead processes that still exist.
        // Any such processes will be left alone; any others will be (re-)created.
        let mut processes_exist = vec![true; scale_in.get()];
        let mut pid_file_locations = vec![None; scale_in.get()];
        let mut port_metadata_file_locations = vec![None; scale_in.get()];
        for i in 0..(scale_in.get()) {
            let process_file_name = format!("{}-{}-{}", self.namespace, id, i);
            let pid_file_location = self.data_dir.join(format!("{}.pid", process_file_name));
            pid_file_locations[i] = Some(pid_file_location.clone());
            let port_metadata_file_location =
                self.data_dir.join(format!("{}.ports", process_file_name));
            port_metadata_file_locations[i] = Some(port_metadata_file_location.clone());

            if pid_file_location.exists() && port_metadata_file_location.exists() {
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
                            for port in port_metadata.values() {
                                if !self.port_allocator.mark_allocated(*port) {
                                    // Somehow we've re-allocated an already used port which
                                    // shouldn't be possible. So we just kill the process and panic.
                                    process.kill();
                                    panic!("port re-use");
                                }
                            }
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
            // If we got here, we didn't find evidence of a live process, so we must (re-)create it later.
            processes_exist[i] = false;

            let mut ports = HashMap::new();
            for port in &ports_in {
                let p = self
                    .port_allocator
                    .alloc()
                    .ok_or_else(|| anyhow!("port exhaustion"))?;
                ports.insert(port.name.clone(), p);
            }
            processes.push(ports.clone());
        }

        // Now create all the processes that weren't detected as being still alive
        let peers = processes
            .iter()
            .map(|ports| ("localhost".to_string(), ports.clone()))
            .collect::<Vec<_>>();
        for i in 0..(scale_in.get()) {
            if !processes_exist[i] {
                let mut args = args(&ServiceAssignments {
                    listen_host: IpAddr::V4(Ipv4Addr::LOCALHOST),
                    ports: &peers[i].1,
                    index: Some(i),
                    peers: &peers,
                });
                args.push(format!(
                    "--pid-file-location={}",
                    pid_file_locations[i].as_ref().unwrap().display()
                ));

                let command_wrapper = self.command_wrapper.clone();
                handles.push(AbortOnDrop(Box::new(mz_ore::task::spawn(
                    || format!("service-supervisor: {full_id}"),
                    supervise(
                        full_id.clone(),
                        path.clone(),
                        args.clone(),
                        command_wrapper,
                        Arc::clone(&self.port_allocator),
                        processes[i].clone(),
                        self.suppress_output,
                        std::mem::take(port_metadata_file_locations[i].as_mut().unwrap()),
                    ),
                ))));
            }
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
    path: impl AsRef<OsStr>,
    args: Vec<impl AsRef<OsStr>>,
    command_wrapper: Vec<String>,
    port_allocator: Arc<PortAllocator>,
    ports: HashMap<String, u16>,
    suppress_output: bool,
    port_metadata_file_location: PathBuf,
) {
    fn interpolate_command(
        command_part: &str,
        full_id: &str,
        ports: &HashMap<String, u16>,
    ) -> String {
        let mut command_part = command_part.replace("%N", full_id);
        for (endpoint, port) in ports {
            command_part = command_part.replace(&format!("%P:{endpoint}"), &format!("{port}"));
        }
        command_part
    }

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
        let mut cmd = if command_wrapper.is_empty() {
            let mut cmd = Command::new(&path);
            cmd.args(&args);
            cmd
        } else {
            let mut cmd = Command::new(&command_wrapper[0]);
            let path = path.as_ref();
            cmd.args(
                command_wrapper[1..]
                    .iter()
                    .map(|part| interpolate_command(part, &full_id, &ports)),
            );
            cmd.arg(path);
            cmd.args(args.iter().map(AsRef::as_ref));
            cmd
        };
        info!(
            "Launching {}: {}...",
            full_id,
            cmd.as_std()
                .get_args()
                .map(|arg| arg.to_string_lossy())
                .join(" ")
        );
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
    processes: Vec<HashMap<String, u16>>,
}

impl Service for ProcessService {
    fn addresses(&self, port: &str) -> Vec<String> {
        self.processes
            .iter()
            .map(|p| format!("localhost:{}", p[port]))
            .collect()
    }
}
