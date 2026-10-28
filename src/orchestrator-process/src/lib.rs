// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::ffi::OsStr;
use std::fmt::Debug;
use std::fs::Permissions;
use std::future::Future;
use std::net::{IpAddr, SocketAddr, TcpListener as StdTcpListener};
use std::num::NonZero;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use anyhow::{Context, anyhow, bail};
use async_stream::stream;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use futures::stream::{BoxStream, FuturesUnordered};
use maplit::btreemap;
use mz_orchestrator::scheduling_config::ServiceSchedulingConfig;
use mz_orchestrator::{
    CpuLimit, DiskLimit, MemoryLimit, NamespacedOrchestrator, Orchestrator, Service,
    ServiceAssignments, ServiceConfig, ServiceEvent, ServicePort, ServiceProcessMetrics,
    ServiceStatus,
};
use mz_ore::cast::{CastFrom, TryCastFrom};
use mz_ore::error::ErrorExt;
use mz_ore::netio::UnixSocketAddr;
use mz_ore::result::ResultExt;
use mz_ore::task::AbortOnDropHandle;
use nix::sys::signal::Signal;
use scopeguard::defer;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use sysinfo::{Pid, Process, ProcessRefreshKind, ProcessesToUpdate, System};
use tokio::fs::remove_dir_all;
use tokio::net::{TcpListener, UnixStream};
use tokio::process::{Child, Command};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::time::{self, Duration};
use tokio::{fs, io, select};
use tracing::{debug, error, info, warn};

pub mod secrets;

#[cfg(test)]
mod lifetime_tests;

/// Configures a [`ProcessOrchestrator`].
#[derive(Debug, Clone)]
pub struct ProcessOrchestratorConfig {
    /// The directory in which the orchestrator should look for executable
    /// images.
    pub image_dir: PathBuf,
    /// Whether to supress output from spawned subprocesses.
    pub suppress_output: bool,
    /// The ID of the environment under orchestration.
    pub environment_id: String,
    /// The directory in which to store secrets.
    pub secrets_dir: PathBuf,
    /// A command to wrap the child command invocation
    pub command_wrapper: Vec<String>,
    /// Whether to crash this process if a child process crashes.
    pub propagate_crashes: bool,
    /// Leave children running when this orchestrator stops supervising them.
    /// Explicit service deletion still terminates both spawned and adopted children.
    pub retain_on_shutdown: bool,
    /// TCP proxy configuration.
    ///
    /// When enabled, for each named port of each created service, the process
    /// orchestrator will bind a TCP listener that proxies incoming connections
    /// to the underlying Unix domain socket. Each bound TCP address will be
    /// emitted as a tracing event.
    ///
    /// The primary use is live debugging the running child services via tools
    /// that do not support Unix domain sockets (e.g., Prometheus, web
    /// browsers).
    pub tcp_proxy: Option<ProcessOrchestratorTcpProxyConfig>,
    /// A scratch directory that orchestrated processes can use for ephemeral storage.
    pub scratch_directory: PathBuf,
}

/// Configures the TCP proxy for a [`ProcessOrchestrator`].
///
/// See [`ProcessOrchestratorConfig::tcp_proxy`].
#[derive(Debug, Clone)]
pub struct ProcessOrchestratorTcpProxyConfig {
    /// The IP address on which to bind TCP listeners.
    pub listen_addr: IpAddr,
    /// A directory in which to write Prometheus scrape targets, for use with
    /// Prometheus's file-based service discovery.
    ///
    /// Each [`NamespacedOrchestrator`] will maintain a single JSON file into
    /// the directory named `NAMESPACE.json` containing the scrape targets for
    /// all extant services. The scrape targets will use the TCP proxy address,
    /// as Prometheus does not support scraping over Unix domain sockets.
    ///
    /// See also: <https://prometheus.io/docs/guides/file-sd/>
    pub prometheus_service_discovery_dir: Option<PathBuf>,
}

/// An orchestrator backed by processes on the local machine.
///
/// **This orchestrator is for development only.** Due to limitations in the
/// Unix process API, it does not exactly conform to the documented semantics
/// of `Orchestrator`.
#[derive(Debug)]
pub struct ProcessOrchestrator {
    image_dir: PathBuf,
    suppress_output: bool,
    namespaces: Mutex<BTreeMap<String, Arc<dyn NamespacedOrchestrator>>>,
    metadata_dir: PathBuf,
    secrets_dir: PathBuf,
    command_wrapper: Vec<String>,
    propagate_crashes: bool,
    retain_on_shutdown: bool,
    tcp_proxy: Option<ProcessOrchestratorTcpProxyConfig>,
    scratch_directory: PathBuf,
    launch_spec: LaunchSpec,
    // Latest synchronous ensures, shared across namespaces. Queued drops use
    // this to preserve records already published by a subsequent ensure.
    // As with PID adoption, a metadata directory has one lifecycle owner.
    tcp_assignments: Arc<Mutex<BTreeMap<PathBuf, TcpAssignments>>>,
}

#[derive(Debug, Clone, Copy)]
enum LaunchSpec {
    /// Directly execute the provided binary
    Direct,
    /// Use Systemd to start the binary
    Systemd,
}

impl LaunchSpec {
    fn determine_implementation() -> Result<Self, anyhow::Error> {
        // According to https://www.freedesktop.org/software/systemd/man/latest/sd_booted.html
        // checking for `/run/systemd/system/` is the canonical way to determine if the system
        // was booted up with systemd.
        match Path::new("/run/systemd/system/").try_exists()? {
            true => Ok(Self::Systemd),
            false => Ok(Self::Direct),
        }
    }

    fn refine_command(
        &self,
        image: impl AsRef<OsStr>,
        args: &[impl AsRef<OsStr>],
        wrapper: &[String],
        memory_limit: Option<&MemoryLimit>,
        cpu_limit: Option<&CpuLimit>,
    ) -> Command {
        let mut cmd = match self {
            Self::Direct => {
                if let Some((program, wrapper_args)) = wrapper.split_first() {
                    let mut cmd = Command::new(program);
                    cmd.args(wrapper_args);
                    cmd.arg(image);
                    cmd
                } else {
                    Command::new(image)
                }
            }
            Self::Systemd => {
                let mut cmd = Command::new("systemd-run");
                cmd.args(["--user", "--scope", "--quiet"]);
                if let Some(memory_limit) = memory_limit {
                    let memory_limit = memory_limit.0.as_u64();
                    cmd.args(["-p", &format!("MemoryMax={memory_limit}")]);
                    // TODO: We could set `-p MemorySwapMax=0` here to disable regular swap.
                }
                if let Some(cpu_limit) = cpu_limit {
                    let cpu_limit = (cpu_limit.as_millicpus() + 9) / 10;
                    cmd.args(["-p", &format!("CPUQuota={cpu_limit}%")]);
                }

                cmd.args(wrapper);
                cmd.arg(image);
                cmd
            }
        };
        cmd.args(args);
        cmd
    }
}

impl ProcessOrchestrator {
    /// Creates a new process orchestrator from the provided configuration.
    pub async fn new(
        ProcessOrchestratorConfig {
            image_dir,
            suppress_output,
            environment_id,
            secrets_dir,
            command_wrapper,
            propagate_crashes,
            retain_on_shutdown,
            tcp_proxy,
            scratch_directory,
        }: ProcessOrchestratorConfig,
    ) -> Result<ProcessOrchestrator, anyhow::Error> {
        let metadata_dir = env::temp_dir().join(format!("environmentd-{environment_id}"));
        fs::create_dir_all(&metadata_dir)
            .await
            .context("creating metadata directory")?;
        fs::create_dir_all(&secrets_dir)
            .await
            .context("creating secrets directory")?;
        fs::set_permissions(&secrets_dir, Permissions::from_mode(0o700))
            .await
            .context("setting secrets directory permissions")?;
        if let Some(prometheus_dir) = tcp_proxy
            .as_ref()
            .and_then(|p| p.prometheus_service_discovery_dir.as_ref())
        {
            fs::create_dir_all(&prometheus_dir)
                .await
                .context("creating prometheus directory")?;
        }

        let launch_spec = LaunchSpec::determine_implementation()?;
        info!(driver = ?launch_spec, "Process orchestrator launch spec");

        Ok(ProcessOrchestrator {
            image_dir: fs::canonicalize(image_dir).await?,
            suppress_output,
            namespaces: Mutex::new(BTreeMap::new()),
            metadata_dir: fs::canonicalize(metadata_dir).await?,
            secrets_dir: fs::canonicalize(secrets_dir).await?,
            command_wrapper,
            propagate_crashes,
            retain_on_shutdown,
            tcp_proxy,
            scratch_directory,
            launch_spec,
            tcp_assignments: Default::default(),
        })
    }
}

impl Orchestrator for ProcessOrchestrator {
    fn namespace(&self, namespace: &str) -> Arc<dyn NamespacedOrchestrator> {
        let mut namespaces = self.namespaces.lock().expect("lock poisoned");
        Arc::clone(namespaces.entry(namespace.into()).or_insert_with(|| {
            let config = Arc::new(NamespacedProcessOrchestratorConfig {
                namespace: namespace.into(),
                image_dir: self.image_dir.clone(),
                suppress_output: self.suppress_output,
                metadata_dir: self.metadata_dir.clone(),
                command_wrapper: self.command_wrapper.clone(),
                propagate_crashes: self.propagate_crashes,
                retain_on_shutdown: self.retain_on_shutdown,
                tcp_proxy: self.tcp_proxy.clone(),
                scratch_directory: self.scratch_directory.clone(),
                launch_spec: self.launch_spec,
                tcp_assignments: Arc::clone(&self.tcp_assignments),
            });

            let services = Arc::new(Mutex::new(BTreeMap::new()));
            let (service_event_tx, service_event_rx) = broadcast::channel(16384);
            let (command_tx, command_rx) = mpsc::unbounded_channel();

            let worker = OrchestratorWorker {
                config: Arc::clone(&config),
                services: Arc::clone(&services),
                service_event_tx,
                system: System::new(),
                command_rx,
            }
            .spawn();

            Arc::new(NamespacedProcessOrchestrator {
                config,
                services,
                service_event_rx,
                command_tx,
                scheduling_config: Default::default(),
                _worker: worker,
            })
        }))
    }
}

/// Configuration for a [`NamespacedProcessOrchestrator`].
#[derive(Debug)]
struct NamespacedProcessOrchestratorConfig {
    namespace: String,
    image_dir: PathBuf,
    suppress_output: bool,
    metadata_dir: PathBuf,
    command_wrapper: Vec<String>,
    propagate_crashes: bool,
    retain_on_shutdown: bool,
    tcp_proxy: Option<ProcessOrchestratorTcpProxyConfig>,
    scratch_directory: PathBuf,
    launch_spec: LaunchSpec,
    tcp_assignments: Arc<Mutex<BTreeMap<PathBuf, TcpAssignments>>>,
}

impl NamespacedProcessOrchestratorConfig {
    fn full_id(&self, id: &str) -> String {
        format!("{}-{}", self.namespace, id)
    }

    fn service_run_dir(&self, id: &str) -> PathBuf {
        self.metadata_dir.join(&self.full_id(id))
    }

    fn service_scratch_dir(&self, id: &str) -> PathBuf {
        self.scratch_directory.join(&self.full_id(id))
    }
}

#[derive(Debug)]
struct NamespacedProcessOrchestrator {
    config: Arc<NamespacedProcessOrchestratorConfig>,
    services: Arc<Mutex<BTreeMap<String, Vec<ProcessState>>>>,
    service_event_rx: broadcast::Receiver<ServiceEvent>,
    command_tx: mpsc::UnboundedSender<WorkerCommand>,
    scheduling_config: std::sync::RwLock<ServiceSchedulingConfig>,
    _worker: AbortOnDropHandle<()>,
}

impl NamespacedProcessOrchestrator {
    fn send_command(&self, cmd: WorkerCommand) {
        self.command_tx.send(cmd).expect("worker task not dropped");
    }
}

#[async_trait]
impl NamespacedOrchestrator for NamespacedProcessOrchestrator {
    fn service_addresses(
        &self,
        id: &str,
        scale: NonZero<u16>,
        port: &ServicePort,
    ) -> Result<Vec<String>, anyhow::Error> {
        let run_dir = self.config.service_run_dir(id);
        if port.requires_tcp {
            let assignments = read_tcp_assignments(&run_dir)?;
            (0..usize::from(scale.get()))
                .map(|i| {
                    assignments
                        .get(i)
                        .and_then(|a| a.get(&port.name))
                        .map(ToString::to_string)
                        .ok_or_else(|| {
                            anyhow!(
                                "TCP port {} for service {id} process {i} is not assigned",
                                port.name
                            )
                        })
                })
                .collect()
        } else {
            Ok((0..scale.get())
                .map(|i| socket_path(&run_dir, &port.name, i))
                .collect())
        }
    }

    fn ensure_service(
        &self,
        id: &str,
        config: ServiceConfig,
    ) -> Result<Box<dyn Service>, anyhow::Error> {
        // Serialize assignment and enqueueing with drops across all namespaces.
        let mut assignments = self.config.tcp_assignments.lock().expect("lock poisoned");
        let run_dir = self.config.service_run_dir(id);
        let mut tcp = assign_tcp_ports(&self.config.metadata_dir, &run_dir, &config)?;
        if tcp.iter().any(|ports| !ports.is_empty()) {
            write_tcp_assignments(&run_dir, &tcp)?;
            assignments.insert(run_dir.clone(), tcp.clone());
        }
        for ports in &mut tcp {
            ports.retain(|name, _| {
                config
                    .ports
                    .iter()
                    .any(|p| p.name == *name && p.requires_tcp)
            });
        }
        let service = ProcessService {
            run_dir,
            scale: config.scale,
            tcp,
        };
        let peer_addrs = (0..config.scale.get())
            .map(|i| {
                config
                    .ports
                    .iter()
                    .map(|port| (port.name.clone(), service.address(&port.name, i)))
                    .collect()
            })
            .collect();

        // Enable disk if the size does not disable it.
        let disk = config.disk_limit != Some(DiskLimit::ZERO);

        let config = EnsureServiceConfig {
            process_restart_on_image_change: config.process_restart_on_image_change,
            image: config.image,
            args: config.args,
            ports: config.ports,
            peer_addrs,
            memory_limit: config.memory_limit,
            cpu_limit: config.cpu_limit,
            scale: config.scale,
            labels: config.labels,
            disk,
        };

        self.send_command(WorkerCommand::EnsureService {
            id: id.to_string(),
            config,
        });

        Ok(Box::new(service))
    }

    fn drop_service(&self, id: &str) -> Result<(), anyhow::Error> {
        let mut assignments = self.config.tcp_assignments.lock().expect("lock poisoned");
        assignments.remove(&self.config.service_run_dir(id));
        self.send_command(WorkerCommand::DropService { id: id.to_string() });
        Ok(())
    }

    async fn list_services(&self) -> Result<Vec<String>, anyhow::Error> {
        let (result_tx, result_rx) = oneshot::channel();
        self.send_command(WorkerCommand::ListServices { result_tx });

        result_rx.await.expect("worker task not dropped")
    }

    async fn flush(&self) -> Result<(), anyhow::Error> {
        let (result_tx, result_rx) = oneshot::channel();
        self.send_command(WorkerCommand::Flush { result_tx });
        result_rx.await.expect("worker task not dropped");
        Ok(())
    }

    fn watch_services(&self) -> BoxStream<'static, Result<ServiceEvent, anyhow::Error>> {
        let mut initial_events = vec![];
        let mut service_event_rx = {
            let services = self.services.lock().expect("lock poisoned");
            for (service_id, process_states) in &*services {
                for (process_id, process_state) in process_states.iter().enumerate() {
                    initial_events.push(ServiceEvent {
                        service_id: service_id.clone(),
                        process_id: u64::cast_from(process_id),
                        status: process_state.status.into(),
                        restart_count: process_state.restart_count,
                        time: process_state.status_time,
                    });
                }
            }
            self.service_event_rx.resubscribe()
        };
        Box::pin(stream! {
            for event in initial_events {
                yield Ok(event);
            }
            loop {
                yield service_event_rx.recv().await.err_into();
            }
        })
    }

    async fn fetch_service_metrics(
        &self,
        id: &str,
    ) -> Result<Vec<ServiceProcessMetrics>, anyhow::Error> {
        let (result_tx, result_rx) = oneshot::channel();
        self.send_command(WorkerCommand::FetchServiceMetrics {
            id: id.to_string(),
            result_tx,
        });

        result_rx.await.expect("worker task not dropped")
    }

    fn update_scheduling_config(
        &self,
        config: mz_orchestrator::scheduling_config::ServiceSchedulingConfig,
    ) {
        *self.scheduling_config.write().expect("poisoned") = config;
    }
}

/// Commands sent from a [`NamespacedProcessOrchestrator`] to its
/// [`OrchestratorWorker`].
///
/// Commands for which the caller expects a result include a `result_tx` on which the
/// [`OrchestratorWorker`] will deliver the result.
enum WorkerCommand {
    EnsureService {
        id: String,
        config: EnsureServiceConfig,
    },
    DropService {
        id: String,
    },
    ListServices {
        result_tx: oneshot::Sender<Result<Vec<String>, anyhow::Error>>,
    },
    Flush {
        result_tx: oneshot::Sender<()>,
    },
    FetchServiceMetrics {
        id: String,
        result_tx: oneshot::Sender<Result<Vec<ServiceProcessMetrics>, anyhow::Error>>,
    },
}

/// Describes the desired state of a process.
struct EnsureServiceConfig {
    process_restart_on_image_change: bool,
    /// An opaque identifier for the executable or container image to run.
    ///
    /// Often names a container on Docker Hub or a path on the local machine.
    pub image: String,
    /// A function that generates the arguments for each process of the service
    /// given the assigned listen addresses for each named port.
    pub args: Box<dyn Fn(ServiceAssignments) -> Vec<String> + Send + Sync>,
    /// Ports to expose.
    pub ports: Vec<ServicePort>,
    /// Addresses selected before the lifecycle command was queued.
    pub peer_addrs: Vec<BTreeMap<String, String>>,
    /// An optional limit on the memory that the service can use.
    pub memory_limit: Option<MemoryLimit>,
    /// An optional limit on the CPU that the service can use.
    pub cpu_limit: Option<CpuLimit>,
    /// The number of copies of this service to run.
    pub scale: NonZero<u16>,
    /// Arbitrary key–value pairs to attach to the service in the orchestrator
    /// backend.
    ///
    /// The orchestrator backend may apply a prefix to the key if appropriate.
    pub labels: BTreeMap<String, String>,
    /// Whether scratch disk space should be allocated for the service.
    pub disk: bool,
}

/// A task executing blocking work for a [`NamespacedProcessOrchestrator`] in the background.
///
/// Required TCP endpoint assignment and metadata publication happen synchronously.
/// Process lifecycle work remains queued.
///
/// This keeps process startup and termination off the caller's path, allowing
/// lifecycle requests from latency-sensitive contexts.
///
/// Note that, apart from `ensure_service` and `drop_service`, this worker also handles blocking
/// orchestrator calls that query service state (such as `list_services`). These need to be
/// sequenced through the worker loop to ensure they linearize as expected. For example, we want to
/// ensure that a `list_services` result contains exactly those services that were previously
/// created with `ensure_service` and not yet dropped with `drop_service`.
struct OrchestratorWorker {
    config: Arc<NamespacedProcessOrchestratorConfig>,
    services: Arc<Mutex<BTreeMap<String, Vec<ProcessState>>>>,
    service_event_tx: broadcast::Sender<ServiceEvent>,
    system: System,
    command_rx: mpsc::UnboundedReceiver<WorkerCommand>,
}

impl OrchestratorWorker {
    fn spawn(self) -> AbortOnDropHandle<()> {
        let name = format!("process-orchestrator:{}", self.config.namespace);
        mz_ore::task::spawn(|| name, self.run()).abort_on_drop()
    }

    async fn run(mut self) {
        while let Some(cmd) = self.command_rx.recv().await {
            use WorkerCommand::*;
            let result = match cmd {
                EnsureService { id, config } => self.ensure_service(id, config).await,
                DropService { id } => self.drop_service(&id).await,
                ListServices { result_tx } => {
                    let _ = result_tx.send(self.list_services().await);
                    Ok(())
                }
                Flush { result_tx } => {
                    let _ = result_tx.send(());
                    Ok(())
                }
                FetchServiceMetrics { id, result_tx } => {
                    let _ = result_tx.send(self.fetch_service_metrics(&id));
                    Ok(())
                }
            };

            if let Err(error) = result {
                panic!("process orchestrator worker failed: {error}");
            }
        }
    }

    fn fetch_service_metrics(
        &mut self,
        id: &str,
    ) -> Result<Vec<ServiceProcessMetrics>, anyhow::Error> {
        let pids: Vec<_> = {
            let services = self.services.lock().expect("lock poisoned");
            let Some(service) = services.get(id) else {
                bail!("unknown service {id}")
            };
            service.iter().map(|p| p.pid()).collect()
        };

        let mut metrics = vec![];
        for pid in pids {
            let (cpu_nano_cores, memory_bytes) = match pid {
                None => (None, None),
                Some(pid) => {
                    self.system.refresh_processes_specifics(
                        ProcessesToUpdate::Some(&[pid]),
                        true,
                        ProcessRefreshKind::nothing().with_cpu().with_memory(),
                    );
                    match self.system.process(pid) {
                        None => (None, None),
                        Some(process) => {
                            // Justification for `unwrap`:
                            //
                            // `u64::try_cast_from(f: f64)`
                            // will always succeed if 0 <= f < 2^64.
                            // Since the max value of `process.cpu_usage()` is
                            // 100.0 * num_of_cores, this will be true whenever there
                            // are less than 2^64 / 10^9 logical cores, or about
                            // 18 billion.
                            let cpu = u64::try_cast_from(
                                (f64::from(process.cpu_usage()) * 10_000_000.0).trunc(),
                            )
                            .expect("sane value of process.cpu_usage()");
                            let memory = process.memory();
                            (Some(cpu), Some(memory))
                        }
                    }
                }
            };
            metrics.push(ServiceProcessMetrics {
                cpu_nano_cores,
                memory_bytes,
                // Process orchestrator does not support the remaining fields right now.
                disk_bytes: None,
                heap_bytes: None,
                heap_limit: None,
                swap_bytes: None,
            });
        }
        Ok(metrics)
    }

    async fn ensure_service(
        &self,
        id: String,
        EnsureServiceConfig {
            process_restart_on_image_change,
            image,
            args,
            ports: ports_in,
            peer_addrs,
            memory_limit,
            cpu_limit,
            scale,
            labels,
            disk,
        }: EnsureServiceConfig,
    ) -> Result<(), anyhow::Error> {
        let full_id = self.config.full_id(&id);

        let run_dir = self.config.service_run_dir(&id);
        fs::create_dir_all(&run_dir)
            .await
            .context("creating run directory")?;
        let scratch_dir = if disk {
            let scratch_dir = self.config.service_scratch_dir(&id);
            fs::create_dir_all(&scratch_dir)
                .await
                .context("creating scratch directory")?;
            Some(fs::canonicalize(&scratch_dir).await?)
        } else {
            None
        };

        // Only opted-in services reconcile executable changes. In particular,
        // settings-derived replica arguments must not trigger replacement.
        let image_identity = process_restart_on_image_change
            .then(|| ImageIdentity::read(self.config.image_dir.join(&image)))
            .transpose()?;
        let existing = {
            let services = self.services.lock().expect("poisoned");
            services.get(&id).map(|states| {
                states.len() == usize::cast_from(scale)
                    && (!process_restart_on_image_change
                        || states.iter().all(|state| {
                            state
                                .image_identity
                                .as_ref()
                                .is_none_or(|actual| Some(actual) == image_identity.as_ref())
                        }))
            })
        };
        match existing {
            Some(true) => return Ok(()),
            Some(false) => self.drop_service(&id).await?,
            None => (),
        }

        {
            let mut services = self.services.lock().expect("lock poisoned");

            // Create the state for new processes.
            let mut process_states = vec![];
            for i in 0..usize::cast_from(scale) {
                let listen_addrs = &peer_addrs[i];

                // Fill out placeholders in the command wrapper for this process.
                let mut command_wrapper = self.config.command_wrapper.clone();
                if let Some(parts) = command_wrapper.get_mut(1..) {
                    for part in parts {
                        *part = interpolate_command(&part[..], &full_id, listen_addrs);
                    }
                }

                // Allocate listeners for each TCP proxy, if requested.
                let mut ports = vec![];
                let mut tcp_proxy_addrs = BTreeMap::new();
                for port in &ports_in {
                    let tcp_proxy_listener = match self
                        .config
                        .tcp_proxy
                        .as_ref()
                        .filter(|_| !port.requires_tcp)
                    {
                        None => None,
                        Some(tcp_proxy) => {
                            let listener = StdTcpListener::bind((tcp_proxy.listen_addr, 0))
                                .with_context(|| format!("binding to {}", tcp_proxy.listen_addr))?;
                            listener.set_nonblocking(true)?;
                            let listener = TcpListener::from_std(listener)?;
                            let local_addr = listener.local_addr()?;
                            tcp_proxy_addrs.insert(port.name.clone(), local_addr);
                            Some(AddressedTcpListener {
                                listener,
                                local_addr,
                            })
                        }
                    };
                    ports.push(ServiceProcessPort {
                        name: port.name.clone(),
                        listen_addr: listen_addrs[&port.name].clone(),
                        tcp_proxy_listener,
                    });
                }

                let mut args = args(ServiceAssignments {
                    listen_addrs,
                    peer_addrs: &peer_addrs,
                });
                args.push(format!("--process={i}"));
                if disk {
                    if let Some(scratch) = &scratch_dir {
                        args.push(format!("--scratch-directory={}", scratch.display()));
                    } else {
                        panic!(
                            "internal error: service requested disk but no scratch directory was configured"
                        );
                    }
                }

                let terminate_on_drop = Arc::new(AtomicBool::new(!self.config.retain_on_shutdown));

                // Launch supervisor process.
                let handle = mz_ore::task::spawn(
                    || format!("process-orchestrator:{full_id}-{i}"),
                    self.supervise_service_process(ServiceProcessConfig {
                        id: id.to_string(),
                        run_dir: run_dir.clone(),
                        i,
                        image: image.clone(),
                        image_identity: image_identity.clone(),
                        args,
                        command_wrapper,
                        ports,
                        memory_limit,
                        cpu_limit,
                        launch_spec: self.config.launch_spec,
                        terminate_on_drop: Arc::clone(&terminate_on_drop),
                    }),
                );

                process_states.push(ProcessState {
                    image_identity: None,
                    _handle: handle.abort_on_drop(),
                    terminate_on_drop,
                    status: ProcessStatus::NotReady,
                    status_time: Utc::now(),
                    restart_count: 0,
                    labels: labels.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
                    tcp_proxy_addrs,
                });
            }

            // Update the in-memory process state. We do this after we've created
            // all process states to avoid partially updating our in-memory state.
            services.insert(id, process_states);
        }

        self.maybe_write_prometheus_service_discovery_file().await;

        Ok(())
    }

    async fn drop_service(&self, id: &str) -> Result<(), anyhow::Error> {
        let full_id = self.config.full_id(id);
        let run_dir = self.config.service_run_dir(id);
        let scratch_dir = self.config.service_scratch_dir(id);

        // Drop the supervisor for the service, if it exists. If this service
        // was under supervision, this will kill all processes associated with
        // it.
        let supervisors = self.services.lock().expect("lock poisoned").remove(id);
        let supervised = supervisors.is_some();
        if let Some(supervisors) = supervisors {
            // Request termination before cancellation, including for a spawn that
            // has not yet published its PID. Wait before removing its directory,
            // so a cancelled supervisor cannot recreate metadata after deletion.
            for supervisor in &supervisors {
                supervisor.terminate_on_drop.store(true, Ordering::SeqCst);
            }
            futures::future::join_all(
                supervisors
                    .into_iter()
                    .map(|supervisor| supervisor._handle.abort_and_wait()),
            )
            .await;
        }

        // If the service was orphaned by a prior incarnation of the
        // orchestrator, it won't have been under supervision and therefore will
        // still be running. So kill any process that we have state for in the
        // run directory.
        if let Ok(mut entries) = fs::read_dir(&run_dir).await {
            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                if path.extension() == Some(OsStr::new("pid")) {
                    let mut system = System::new();
                    let Some(process) = find_process_from_pid_file(&mut system, &path).await else {
                        // Dropping the supervisor may already have killed a
                        // supervised process. An orphan that cannot be found
                        // either exited or runs in a PID namespace this
                        // orchestrator cannot see, e.g., another container
                        // sharing the data directory. Its run directory is
                        // deleted below, so warn while there is a trace.
                        if !supervised {
                            warn!(
                                "not terminating orphaned process for {full_id}: no live process \
                                 matches {}",
                                path.display()
                            );
                        }
                        continue;
                    };
                    let pid = process.pid();
                    info!("terminating orphaned process for {full_id} with PID {pid}");
                    process.kill();
                }
            }
        }

        // Clean up the on-disk state of the service.
        {
            let assignments = self.config.tcp_assignments.lock().expect("lock poisoned");
            // A later ensure has already returned this endpoint. Keep its
            // assignment file continuously visible to read-only discoverers,
            // including those in other processes that do not share this mutex.
            let retain_assignment = assignments.contains_key(&run_dir);
            if let Err(e) = clean_service_run_dir(&run_dir, retain_assignment) {
                if e.kind() != io::ErrorKind::NotFound {
                    warn!(
                        "error cleaning up run directory for {full_id}: {}",
                        e.display_with_causes()
                    );
                }
            }
        }
        if let Err(e) = remove_dir_all(scratch_dir).await {
            if e.kind() != io::ErrorKind::NotFound {
                warn!(
                    "error cleaning up scratch directory for {full_id}: {}",
                    e.display_with_causes()
                );
            }
        }

        self.maybe_write_prometheus_service_discovery_file().await;
        Ok(())
    }

    async fn list_services(&self) -> Result<Vec<String>, anyhow::Error> {
        let mut services = vec![];
        let namespace_prefix = format!("{}-", self.config.namespace);
        let mut entries = fs::read_dir(&self.config.metadata_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let filename = entry
                .file_name()
                .into_string()
                .map_err(|_| anyhow!("unable to convert filename to string"))?;
            if let Some(id) = filename.strip_prefix(&namespace_prefix) {
                services.push(id.to_string());
            }
        }
        Ok(services)
    }

    fn supervise_service_process(
        &self,
        ServiceProcessConfig {
            image_identity,
            id,
            run_dir,
            i,
            image,
            args,
            command_wrapper,
            ports,
            memory_limit,
            cpu_limit,
            launch_spec,
            terminate_on_drop,
        }: ServiceProcessConfig,
    ) -> impl Future<Output = ()> + use<> {
        let suppress_output = self.config.suppress_output;
        let propagate_crashes = self.config.propagate_crashes;
        let image = self.config.image_dir.join(image);
        let pid_file = run_dir.join(format!("{i}.pid"));
        let full_id = self.config.full_id(&id);

        let state_updater = ProcessStateUpdater {
            namespace: self.config.namespace.clone(),
            id,
            i,
            services: Arc::downgrade(&self.services),
            service_event_tx: self.service_event_tx.clone(),
        };

        async move {
            // Holds AbortOnDropHandles to keep proxy tasks alive.
            #[allow(clippy::collection_is_never_read)]
            let mut proxy_handles = vec![];
            for port in ports {
                if let Some(tcp_listener) = port.tcp_proxy_listener {
                    info!(
                        "{full_id}-{i}: {} tcp proxy listening on {}",
                        port.name, tcp_listener.local_addr,
                    );
                    let uds_path = port.listen_addr;
                    let handle = mz_ore::task::spawn(
                        || format!("{full_id}-{i}-proxy-{}", port.name),
                        tcp_proxy(TcpProxyConfig {
                            name: format!("{full_id}-{i}-{}", port.name),
                            tcp_listener,
                            uds_path: uds_path.clone(),
                        }),
                    );
                    proxy_handles.push(handle.abort_on_drop());
                }
            }

            supervise_existing_process(
                &state_updater,
                &pid_file,
                &terminate_on_drop,
                image_identity.as_ref(),
                !command_wrapper.is_empty(),
            )
            .await;

            loop {
                let mut cmd = launch_spec.refine_command(
                    &image,
                    &args,
                    &command_wrapper,
                    memory_limit.as_ref(),
                    cpu_limit.as_ref(),
                );
                // Launch arguments can contain credentials, including through
                // wrappers. Log only the executable, never the argument vector.
                info!(
                    "launching {full_id}-{i} via {}",
                    cmd.as_std().get_program().to_string_lossy(),
                );
                if suppress_output {
                    cmd.stdout(Stdio::null());
                    cmd.stderr(Stdio::null());
                }
                let spawned_identity = image_identity
                    .as_ref()
                    .map(|_| ImageIdentity::read(image.clone()))
                    .transpose();
                let spawned = match spawned_identity {
                    Ok(identity) => {
                        spawn_process(
                            &state_updater,
                            cmd,
                            &pid_file,
                            identity.as_ref(),
                            !command_wrapper.is_empty(),
                            Arc::clone(&terminate_on_drop),
                        )
                        .await
                    }
                    Err(error) => Err(error),
                };
                match spawned {
                    Ok(status) => {
                        assert!(
                            !(propagate_crashes && did_process_crash(status)),
                            "{full_id}-{i} crashed; aborting because propagate_crashes is enabled"
                        );
                        error!("{full_id}-{i} exited: {:?}; relaunching in 5s", status);
                    }
                    Err(e) => {
                        error!("{full_id}-{i} failed to spawn: {}; relaunching in 5s", e);
                    }
                };
                state_updater.update_state(ProcessStatus::NotReady);
                time::sleep(Duration::from_secs(5)).await;
            }
        }
    }

    async fn maybe_write_prometheus_service_discovery_file(&self) {
        #[derive(Serialize)]
        struct StaticConfig {
            labels: BTreeMap<String, String>,
            targets: Vec<String>,
        }

        let Some(tcp_proxy) = &self.config.tcp_proxy else {
            return;
        };
        let Some(dir) = &tcp_proxy.prometheus_service_discovery_dir else {
            return;
        };

        let mut static_configs = vec![];
        {
            let services = self.services.lock().expect("lock poisoned");
            for (id, states) in &*services {
                for (i, state) in states.iter().enumerate() {
                    for (name, addr) in &state.tcp_proxy_addrs {
                        let mut labels = btreemap! {
                            "mz_orchestrator_namespace".into() => self.config.namespace.clone(),
                            "mz_orchestrator_service_id".into() => id.clone(),
                            "mz_orchestrator_port".into() => name.clone(),
                            "mz_orchestrator_ordinal".into() => i.to_string(),
                        };
                        for (k, v) in &state.labels {
                            let k = format!("mz_orchestrator_{}", k.replace('-', "_"));
                            labels.insert(k, v.clone());
                        }
                        static_configs.push(StaticConfig {
                            labels,
                            targets: vec![addr.to_string()],
                        })
                    }
                }
            }
        }

        let path = dir.join(Path::new(&self.config.namespace).with_extension("json"));
        let contents = serde_json::to_vec_pretty(&static_configs).expect("valid json");
        if let Err(e) = fs::write(&path, &contents).await {
            warn!(
                "{}: failed to write prometheus service discovery file: {}",
                self.config.namespace,
                e.display_with_causes()
            );
        }
    }
}

// File identity detects in-place build replacement without hashing large binaries
// on every reconciliation. This local record contains no launch arguments.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ImageIdentity {
    path: PathBuf,
    device: u64,
    inode: u64,
    size: u64,
    modified: i64,
    modified_nsec: i64,
}

impl ImageIdentity {
    fn read(path: PathBuf) -> Result<Self, anyhow::Error> {
        let metadata = std::fs::metadata(&path).context("reading service executable identity")?;
        Ok(Self {
            path,
            device: metadata.dev(),
            inode: metadata.ino(),
            size: metadata.size(),
            modified: metadata.mtime(),
            modified_nsec: metadata.mtime_nsec(),
        })
    }
}

struct ServiceProcessConfig {
    image_identity: Option<ImageIdentity>,
    id: String,
    run_dir: PathBuf,
    i: usize,
    image: String,
    args: Vec<String>,
    command_wrapper: Vec<String>,
    ports: Vec<ServiceProcessPort>,
    memory_limit: Option<MemoryLimit>,
    cpu_limit: Option<CpuLimit>,
    launch_spec: LaunchSpec,
    terminate_on_drop: Arc<AtomicBool>,
}

struct ServiceProcessPort {
    name: String,
    listen_addr: String,
    tcp_proxy_listener: Option<AddressedTcpListener>,
}

/// Supervises an existing process, if it exists.
async fn supervise_existing_process(
    state_updater: &ProcessStateUpdater,
    pid_file: &Path,
    terminate_on_drop: &AtomicBool,
    image_identity: Option<&ImageIdentity>,
    send_sigterm: bool,
) {
    let name = format!(
        "{}-{}-{}",
        state_updater.namespace, state_updater.id, state_updater.i
    );

    let mut system = System::new();
    let Some(process) = find_process_from_pid_file(&mut system, pid_file).await else {
        return;
    };
    let pid = process.pid();
    let start_time = process.start_time();

    info!(%pid, "discovered existing process for {name}");
    let actual_image = image_identity.and_then(|_| {
        std::fs::read(pid_file.with_extension("image"))
            .ok()
            .and_then(|bytes| serde_json::from_slice::<(u32, u64, ImageIdentity)>(&bytes).ok())
            .filter(|(recorded_pid, recorded_start, _)| {
                *recorded_pid == pid.as_u32() && *recorded_start == start_time
            })
            .map(|(_, _, actual)| actual)
    });
    let replace = match (image_identity, actual_image.as_ref()) {
        (Some(desired), Some(actual)) => desired != actual,
        (Some(_), None) => {
            // Unknown legacy state is not authority to replace an adopted process.
            warn!(%pid, "adopting {name} without a verified executable identity");
            false
        }
        _ => false,
    };
    // A replacement is pending, or an adoption's actual executable is unknown.
    // Do not publish the desired executable as if it had already been launched.
    state_updater.update_image_identity(if replace { None } else { actual_image });
    if replace {
        info!(%pid, "replacing {name} after executable change");
    } else {
        state_updater.update_state(ProcessStatus::Ready { pid });
    }

    // Stopping observation is distinct from an explicit service deletion.
    let need_kill = AtomicBool::new(true);
    defer! {
        state_updater.update_state(ProcessStatus::NotReady);
        if need_kill.load(Ordering::SeqCst) && terminate_on_drop.load(Ordering::SeqCst) {
            info!(%pid, "terminating existing process for {name}");
            if image_identity.is_some() && send_sigterm {
                process.kill_with(sysinfo::Signal::Term);
                tokio::task::block_in_place(|| std::thread::sleep(Duration::from_millis(500)));
            }
            // A forwarding wrapper may have exited during the grace period.
            // Recheck incarnation before escalating rather than signaling a reused PID.
            let mut current = System::new();
            current.refresh_processes_specifics(
                ProcessesToUpdate::Some(&[pid]), true, ProcessRefreshKind::nothing(),
            );
            if let Some(current) = current.process(pid).filter(|p| p.start_time() == start_time) {
                current.kill();
            }
        }
    }

    if replace && send_sigterm {
        // Match owned-child replacement: let forwarding wrappers stop their
        // children before escalating. The guard above also covers cancellation.
        process.kill_with(sysinfo::Signal::Term);
        time::sleep(Duration::from_millis(500)).await;
    }

    // Periodically check if the process has terminated. Verify start_time
    // on each iteration to detect PID reuse.
    let mut system = System::new();
    loop {
        let refreshed = system.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[pid]),
            true,
            ProcessRefreshKind::nothing(),
        );
        if refreshed == 0 {
            break;
        }
        match system.process(pid) {
            Some(p)
                if p.start_time() == start_time
                    && !matches!(
                        p.status(),
                        sysinfo::ProcessStatus::Zombie | sysinfo::ProcessStatus::Dead
                    ) =>
            {
                if replace {
                    p.kill();
                }
            }
            _ => break,
        }
        time::sleep(Duration::from_secs(5)).await;
    }

    // The process has crashed. Exit the function without attempting to
    // kill it.
    warn!(%pid, "process for {name} has crashed; will reboot");
    need_kill.store(false, Ordering::SeqCst)
}

fn interpolate_command(
    command_part: &str,
    full_id: &str,
    ports: &BTreeMap<String, String>,
) -> String {
    let mut command_part = command_part.replace("%N", full_id);
    for (endpoint, port) in ports {
        command_part = command_part.replace(&format!("%P:{endpoint}"), port);
    }
    command_part
}

async fn spawn_process(
    state_updater: &ProcessStateUpdater,
    mut cmd: Command,
    pid_file: &Path,
    image_identity: Option<&ImageIdentity>,
    send_sigterm: bool,
    terminate_on_drop: Arc<AtomicBool>,
) -> Result<ExitStatus, anyhow::Error> {
    struct KillOnDropChild {
        child: Child,
        send_sigterm: bool,
        terminate_on_drop: Arc<AtomicBool>,
        pid_published: bool,
    }

    impl Drop for KillOnDropChild {
        fn drop(&mut self) {
            // A child without recoverable metadata must not survive an error or
            // cancellation while publishing its PID, even in retained mode.
            if self.pid_published && !self.terminate_on_drop.load(Ordering::SeqCst) {
                return;
            }
            if let (Some(pid), true) = (
                self.child.id().and_then(|id| i32::try_from(id).ok()),
                self.send_sigterm,
            ) {
                let _ = nix::sys::signal::kill(
                    nix::unistd::Pid::from_raw(pid),
                    nix::sys::signal::Signal::SIGTERM,
                );
                // Give the process a bit of time to react to the signal
                tokio::task::block_in_place(|| std::thread::sleep(Duration::from_millis(500)));
            }
            let _ = self.child.start_kill();
        }
    }

    let mut child = KillOnDropChild {
        child: cmd.spawn()?,
        send_sigterm,
        terminate_on_drop,
        pid_published: false,
    };

    // Immediately write out a file containing the PID of the child process and
    // its start time. We'll use this state to rediscover our children if we
    // crash and restart. There's a very small window where we can crash after
    // having spawned the child but before writing this file, in which case we
    // might orphan the process. We accept this risk, though. It's hard to do
    // anything more robust given the Unix APIs available to us, and the
    // solution here is good enough given that the process orchestrator is only
    // used in development/testing.
    let pid = Pid::from_u32(child.child.id().unwrap());
    write_pid_file(pid_file, pid, image_identity)?;
    child.pid_published = true;
    state_updater.update_image_identity(image_identity.cloned());
    state_updater.update_state(ProcessStatus::Ready { pid });
    Ok(child.child.wait().await?)
}

fn did_process_crash(status: ExitStatus) -> bool {
    // Likely not exhaustive. Feel free to add additional tests for other
    // indications of a crashed child process, as those conditions are
    // discovered.
    status.signal().is_some_and(|s| {
        matches!(
            Signal::try_from(s),
            Ok(Signal::SIGABRT
                | Signal::SIGBUS
                | Signal::SIGSEGV
                | Signal::SIGTRAP
                | Signal::SIGILL)
        )
    })
}

fn write_pid_file(
    pid_file: &Path,
    pid: Pid,
    image_identity: Option<&ImageIdentity>,
) -> Result<(), anyhow::Error> {
    let mut system = System::new();
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        true,
        ProcessRefreshKind::nothing(),
    );
    let start_time = system.process(pid).map_or(0, |p| p.start_time());
    // Publish this small local record synchronously. A cancelled Tokio file
    // write can continue on its blocking worker after supervisor termination,
    // overwriting a replacement service's PID file after deletion/recreation.
    if let Some(identity) = image_identity {
        // Describe the actual spawn, not desired state before replacement. The
        // PID/start qualification makes a sidecar from another incarnation inert.
        // Publish it first, preserving the existing two-line PID format.
        std::fs::write(
            pid_file.with_extension("image"),
            serde_json::to_vec(&(pid.as_u32(), start_time, identity))?,
        )?;
    }
    std::fs::write(pid_file, format!("{pid}\n{start_time}\n"))?;
    Ok(())
}

async fn find_process_from_pid_file<'a>(
    system: &'a mut System,
    pid_file: &Path,
) -> Option<&'a Process> {
    let Ok(contents) = fs::read_to_string(pid_file).await else {
        return None;
    };
    let lines = contents.trim().split('\n').collect::<Vec<_>>();
    let [pid, start_time] = lines.as_slice() else {
        return None;
    };
    let Ok(pid) = Pid::from_str(pid) else {
        return None;
    };
    let Ok(start_time) = u64::from_str(start_time) else {
        return None;
    };
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        true,
        ProcessRefreshKind::nothing(),
    );
    let process = system.process(pid)?;
    // Checking the start time protects against killing an unrelated process due
    // to PID reuse.
    if process.start_time() != start_time {
        return None;
    }
    Some(process)
}

struct TcpProxyConfig {
    name: String,
    tcp_listener: AddressedTcpListener,
    uds_path: String,
}

async fn tcp_proxy(
    TcpProxyConfig {
        name,
        tcp_listener,
        uds_path,
    }: TcpProxyConfig,
) {
    let mut conns = FuturesUnordered::new();
    loop {
        select! {
            res = tcp_listener.listener.accept() => {
                debug!("{name}: accepting tcp proxy connection");
                let uds_path = uds_path.clone();
                conns.push(Box::pin(async move {
                    let (mut tcp_conn, _) = res.context("accepting tcp connection")?;
                    let mut uds_conn = UnixStream::connect(uds_path)
                        .await
                        .context("making uds connection")?;
                    io::copy_bidirectional(&mut tcp_conn, &mut uds_conn)
                        .await
                        .context("proxying")
                }));
            }
            Some(result) = conns.next() => if let Err(e) = result {
                warn!("{name}: tcp proxy connection failed: {}", e.display_with_causes());
            }
        }
    }
}

struct ProcessStateUpdater {
    namespace: String,
    id: String,
    i: usize,
    // A supervisor observes the service map without owning its own task handle.
    services: Weak<Mutex<BTreeMap<String, Vec<ProcessState>>>>,
    service_event_tx: broadcast::Sender<ServiceEvent>,
}

impl ProcessStateUpdater {
    fn update_image_identity(&self, identity: Option<ImageIdentity>) {
        let Some(services) = self.services.upgrade() else {
            return;
        };
        let mut services = services.lock().expect("lock poisoned");
        if let Some(state) = services
            .get_mut(&self.id)
            .and_then(|states| states.get_mut(self.i))
        {
            state.image_identity = identity;
        }
    }

    fn update_state(&self, status: ProcessStatus) {
        let Some(services) = self.services.upgrade() else {
            return;
        };
        let mut services = services.lock().expect("lock poisoned");
        let Some(process_states) = services.get_mut(&self.id) else {
            return;
        };
        let Some(process_state) = process_states.get_mut(self.i) else {
            return;
        };
        let status_time = Utc::now();
        // Count each transition to NotReady as a restart. The process
        // orchestrator always relaunches a process that exits, so a death is a
        // restart. This is monotonic for a given `ProcessState` and changes on
        // every restart, which is what the 0dt caught-up check needs. It only
        // resets to zero if the whole service is dropped and recreated.
        if matches!(status, ProcessStatus::NotReady) {
            process_state.restart_count += 1;
        }
        process_state.status = status;
        process_state.status_time = status_time;
        let _ = self.service_event_tx.send(ServiceEvent {
            service_id: self.id.to_string(),
            process_id: u64::cast_from(self.i),
            status: status.into(),
            restart_count: process_state.restart_count,
            time: status_time,
        });
    }
}

#[derive(Debug)]
struct ProcessState {
    image_identity: Option<ImageIdentity>,
    _handle: AbortOnDropHandle<()>,
    terminate_on_drop: Arc<AtomicBool>,
    status: ProcessStatus,
    status_time: DateTime<Utc>,
    /// Number of times this process has died and been relaunched. Monotonic for
    /// the lifetime of this `ProcessState`. See [`ProcessStateUpdater::update_state`].
    restart_count: u64,
    labels: BTreeMap<String, String>,
    tcp_proxy_addrs: BTreeMap<String, SocketAddr>,
}

impl ProcessState {
    fn pid(&self) -> Option<Pid> {
        match &self.status {
            ProcessStatus::NotReady => None,
            ProcessStatus::Ready { pid } => Some(*pid),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum ProcessStatus {
    NotReady,
    Ready { pid: Pid },
}

impl From<ProcessStatus> for ServiceStatus {
    fn from(status: ProcessStatus) -> ServiceStatus {
        match status {
            ProcessStatus::NotReady => ServiceStatus::Offline(None),
            ProcessStatus::Ready { .. } => ServiceStatus::Online,
        }
    }
}

// Only direct TCP assignments need storage. Unix addresses remain pure derivations.
type TcpAssignments = Vec<BTreeMap<String, SocketAddr>>;
const TCP_ASSIGNMENTS_FILE: &str = "tcp-ports.json";

fn clean_service_run_dir(run_dir: &Path, retain_assignment: bool) -> io::Result<()> {
    if !retain_assignment {
        return std::fs::remove_dir_all(run_dir);
    }
    for entry in std::fs::read_dir(run_dir)? {
        let entry = entry?;
        if entry.file_name() == TCP_ASSIGNMENTS_FILE {
            continue;
        }
        if entry.file_type()?.is_dir() {
            std::fs::remove_dir_all(entry.path())?;
        } else {
            std::fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

fn read_tcp_assignments(run_dir: &Path) -> Result<TcpAssignments, anyhow::Error> {
    match std::fs::read(run_dir.join(TCP_ASSIGNMENTS_FILE)) {
        Ok(bytes) => serde_json::from_slice(&bytes).context("reading TCP assignments"),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(e.into()),
    }
}

fn write_tcp_assignments(
    run_dir: &Path,
    assignments: &TcpAssignments,
) -> Result<(), anyhow::Error> {
    std::fs::create_dir_all(run_dir)?;
    let temporary = run_dir.join("tcp-ports.json.tmp");
    std::fs::write(&temporary, serde_json::to_vec(assignments)?)?;
    std::fs::rename(temporary, run_dir.join(TCP_ASSIGNMENTS_FILE))?;
    Ok(())
}

fn assign_tcp_ports(
    metadata_dir: &Path,
    run_dir: &Path,
    config: &ServiceConfig,
) -> Result<TcpAssignments, anyhow::Error> {
    if !config.ports.iter().any(|port| port.requires_tcp) {
        return Ok(Vec::new());
    }
    let previous = read_tcp_assignments(run_dir)?;
    let mut used = BTreeSet::new();
    // Retained services may not be listening yet. Their records still reserve
    // endpoints, including services in namespaces not adopted by this owner.
    for entry in std::fs::read_dir(metadata_dir)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            for ports in read_tcp_assignments(&entry.path())? {
                used.extend(ports.into_values());
            }
        }
    }
    // Keep old reservations until explicit deletion. A queued scale change can
    // still have children about to bind the previous assignments.
    let mut assignments = previous.clone();
    assignments.resize_with(
        assignments.len().max(usize::from(config.scale.get())),
        BTreeMap::new,
    );
    for i in 0..usize::from(config.scale.get()) {
        let ports = &mut assignments[i];
        for port in config.ports.iter().filter(|port| port.requires_tcp) {
            let address = match previous.get(i).and_then(|ports| ports.get(&port.name)) {
                // Never probe an existing assignment: a retained child can own it.
                Some(address) => *address,
                None => {
                    let hint = SocketAddr::from(([127, 0, 0, 1], port.port_hint));
                    let listener = if !used.contains(&hint) {
                        StdTcpListener::bind(hint).ok()
                    } else {
                        None
                    };
                    let listener = match listener {
                        Some(listener) if !used.contains(&listener.local_addr()?) => listener,
                        candidate => {
                            // Keep rejected ephemeral sockets bound until selection
                            // finishes so the OS cannot keep returning the same port.
                            let mut rejected: Vec<_> = candidate.into_iter().collect();
                            loop {
                                let listener = StdTcpListener::bind(("127.0.0.1", 0))?;
                                if !used.contains(&listener.local_addr()?) {
                                    drop(rejected);
                                    break listener;
                                }
                                rejected.push(listener);
                            }
                        }
                    };
                    // Reserve-release races are an accepted dev-backend limitation.
                    listener.local_addr()?
                }
            };
            used.insert(address);
            ports.insert(port.name.clone(), address);
        }
    }
    Ok(assignments)
}

fn socket_path(run_dir: &Path, port: &str, process: u16) -> String {
    let desired = run_dir
        .join(format!("{port}-{process}"))
        .to_string_lossy()
        .into_owned();
    if UnixSocketAddr::from_pathname(&desired).is_err() {
        // Unix socket addresses have a very low maximum length of around 100
        // bytes on most platforms.
        env::temp_dir()
            .join(hex::encode(Sha1::digest(desired)))
            .display()
            .to_string()
    } else {
        desired
    }
}

struct AddressedTcpListener {
    listener: TcpListener,
    local_addr: SocketAddr,
}

#[derive(Debug)]
struct ProcessService {
    run_dir: PathBuf,
    scale: NonZero<u16>,
    tcp: TcpAssignments,
}

impl ProcessService {
    fn address(&self, port: &str, process: u16) -> String {
        self.tcp
            .get(usize::from(process))
            .and_then(|a| a.get(port))
            .map(ToString::to_string)
            .unwrap_or_else(|| socket_path(&self.run_dir, port, process))
    }
}

impl Service for ProcessService {
    fn addresses(&self, port: &str) -> Vec<String> {
        (0..self.scale.get())
            .map(|i| self.address(port, i))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use itertools::Itertools;

    #[mz_ore::test(tokio::test)]
    async fn service_addresses_parity() {
        for metadata_dir in [
            PathBuf::from("/tmp/discovery"),
            PathBuf::from("/tmp").join("x".repeat(200)),
        ] {
            let (command_tx, mut command_rx) = mpsc::unbounded_channel();
            let (_, service_event_rx) = broadcast::channel(16);
            let orchestrator = NamespacedProcessOrchestrator {
                config: Arc::new(NamespacedProcessOrchestratorConfig {
                    namespace: "compute".into(),
                    image_dir: PathBuf::new(),
                    suppress_output: true,
                    metadata_dir,
                    command_wrapper: Vec::new(),
                    propagate_crashes: false,
                    retain_on_shutdown: false,
                    tcp_proxy: None,
                    scratch_directory: PathBuf::new(),
                    launch_spec: LaunchSpec::Direct,
                    tcp_assignments: Default::default(),
                }),
                services: Default::default(),
                service_event_rx,
                command_tx,
                scheduling_config: Default::default(),
                // Keep lifecycle commands observable without running any processes.
                _worker: mz_ore::task::spawn(|| "discovery-test", std::future::pending())
                    .abort_on_drop(),
            };
            for scale in [1, 3, 12] {
                let scale = NonZero::new(scale).unwrap();
                let id = "u1-replica-1";
                let port = ServicePort {
                    name: "compute".into(),
                    port_hint: 2100,
                    requires_tcp: false,
                };
                let addresses = orchestrator.service_addresses(id, scale, &port).unwrap();
                assert!(command_rx.try_recv().is_err());
                assert!(orchestrator.services.lock().unwrap().is_empty());
                let config = ServiceConfig {
                    inherit_kubernetes_owner_references: true,
                    process_restart_on_image_change: false,
                    app_name: "discovery-test".into(),
                    image: "unused".into(),
                    init_container_image: None,
                    args: Box::new(|_| Vec::new()),
                    ports: vec![port.clone()],
                    memory_limit: None,
                    memory_request: None,
                    cpu_limit: None,
                    cpu_request: None,
                    scale,
                    labels: BTreeMap::new(),
                    annotations: BTreeMap::new(),
                    availability_zones: None,
                    other_replicas_selector: Vec::new(),
                    replicas_selector: Vec::new(),
                    disk_limit: Some(DiskLimit::ZERO),
                    node_selector: BTreeMap::new(),
                };
                let service = orchestrator.ensure_service(id, config).unwrap();
                assert_eq!(addresses, service.addresses(&port.name));
                assert!(matches!(
                    command_rx.try_recv(),
                    Ok(WorkerCommand::EnsureService { .. })
                ));
                assert_eq!(addresses.len(), usize::from(scale.get()));
                for (i, address) in (0..scale.get()).zip_eq(&addresses) {
                    let desired = orchestrator
                        .config
                        .service_run_dir(id)
                        .join(format!("{}-{i}", port.name));
                    if UnixSocketAddr::from_pathname(desired.to_string_lossy()).is_ok() {
                        assert_eq!(address, &desired.display().to_string());
                    } else {
                        assert_eq!(
                            address,
                            &env::temp_dir()
                                .join(hex::encode(Sha1::digest(
                                    desired.to_string_lossy().as_bytes()
                                )))
                                .display()
                                .to_string()
                        );
                    }
                    assert!(UnixSocketAddr::from_pathname(address).is_ok());
                }
            }
        }
    }
}
