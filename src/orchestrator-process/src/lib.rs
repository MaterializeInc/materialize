// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::env;
use std::ffi::OsStr;
use std::fmt::Debug;
use std::fs::Permissions;
use std::future::Future;
use std::net::{IpAddr, SocketAddr, TcpListener as StdTcpListener};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{Context, anyhow, bail};
use async_stream::stream;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use futures::stream::{BoxStream, FuturesUnordered};
use itertools::Itertools;
use libc::{SIGABRT, SIGBUS, SIGILL, SIGSEGV, SIGTRAP};
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
use scopeguard::defer;
use serde::Serialize;
use sha1::{Digest, Sha1};
use sysinfo::{Pid, PidExt, Process, ProcessExt, ProcessRefreshKind, System, SystemExt};
use tokio::fs::remove_dir_all;
use tokio::net::{TcpListener, UnixStream};
use tokio::process::{Child, Command};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::time::{self, Duration};
use tokio::{fs, io, select};
use tracing::{debug, error, info, warn};

pub mod secrets;

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
    tcp_proxy: Option<ProcessOrchestratorTcpProxyConfig>,
    scratch_directory: PathBuf,
    launch_spec: LaunchSpec,
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
            tcp_proxy,
            scratch_directory,
            launch_spec,
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
                tcp_proxy: self.tcp_proxy.clone(),
                scratch_directory: self.scratch_directory.clone(),
                launch_spec: self.launch_spec,
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
    tcp_proxy: Option<ProcessOrchestratorTcpProxyConfig>,
    scratch_directory: PathBuf,
    launch_spec: LaunchSpec,
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
    fn ensure_service(
        &self,
        id: &str,
        config: ServiceConfig,
    ) -> Result<Box<dyn Service>, anyhow::Error> {
        let service = ProcessService {
            run_dir: self.config.service_run_dir(id),
            scale: config.scale,
        };

        // Enable disk if the size does not disable it.
        let disk = config.disk_limit != Some(DiskLimit::ZERO);

        let config = EnsureServiceConfig {
            image: config.image,
            args: config.args,
            ports: config.ports,
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
        self.send_command(WorkerCommand::DropService { id: id.to_string() });
        Ok(())
    }

    async fn list_services(&self) -> Result<Vec<String>, anyhow::Error> {
        let (result_tx, result_rx) = oneshot::channel();
        self.send_command(WorkerCommand::ListServices { result_tx });

        result_rx.await.expect("worker task not dropped")
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
    FetchServiceMetrics {
        id: String,
        result_tx: oneshot::Sender<Result<Vec<ServiceProcessMetrics>, anyhow::Error>>,
    },
}

/// Describes the desired state of a process.
struct EnsureServiceConfig {
    /// An opaque identifier for the executable or container image to run.
    ///
    /// Often names a container on Docker Hub or a path on the local machine.
    pub image: String,
    /// A function that generates the arguments for each process of the service
    /// given the assigned listen addresses for each named port.
    pub args: Box<dyn Fn(ServiceAssignments) -> Vec<String> + Send + Sync>,
    /// Ports to expose.
    pub ports: Vec<ServicePort>,
    /// An optional limit on the memory that the service can use.
    pub memory_limit: Option<MemoryLimit>,
    /// An optional limit on the CPU that the service can use.
    pub cpu_limit: Option<CpuLimit>,
    /// The number of copies of this service to run.
    pub scale: u16,
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
/// This type exists to enable making [`NamespacedProcessOrchestrator::ensure_service`] and
/// [`NamespacedProcessOrchestrator::drop_service`] non-blocking, allowing invocation of these
/// methods in latency-sensitive contexts.
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
                    self.system
                        .refresh_process_specifics(pid, ProcessRefreshKind::new().with_cpu());
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
                // Process orchestrator does not support this right now.
                disk_usage_bytes: None,
            });
        }
        Ok(metrics)
    }

    async fn ensure_service(
        &self,
        id: String,
        EnsureServiceConfig {
            image,
            args,
            ports: ports_in,
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

        // The service might already exist. If it has the same config as requested (currently we
        // check only the scale), we have nothing to do. Otherwise we need to drop and recreate it.
        let old_scale = {
            let services = self.services.lock().expect("poisoned");
            services.get(&id).map(|states| states.len())
        };
        match old_scale {
            Some(old) if old == usize::from(scale) => return Ok(()),
            Some(_) => self.drop_service(&id).await?,
            None => (),
        }

        // Create sockets for all processes in the service.
        let mut peer_addrs = Vec::new();
        for i in 0..scale.into() {
            let addresses = ports_in
                .iter()
                .map(|port| {
                    let addr = socket_path(&run_dir, &port.name, i);
                    (port.name.clone(), addr)
                })
                .collect();
            peer_addrs.push(addresses);
        }

        {
            let mut services = self.services.lock().expect("lock poisoned");

            // Create the state for new processes.
            let mut process_states = vec![];
            for i in 0..scale.into() {
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
                    let tcp_proxy_listener = match &self.config.tcp_proxy {
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

                // Launch supervisor process.
                let handle = mz_ore::task::spawn(
                    || format!("process-orchestrator:{full_id}-{i}"),
                    self.supervise_service_process(ServiceProcessConfig {
                        id: id.to_string(),
                        run_dir: run_dir.clone(),
                        i,
                        image: image.clone(),
                        args,
                        command_wrapper,
                        ports,
                        memory_limit,
                        cpu_limit,
                        launch_spec: self.config.launch_spec,
                    }),
                );

                process_states.push(ProcessState {
                    _handle: handle.abort_on_drop(),
                    status: ProcessStatus::NotReady,
                    status_time: Utc::now(),
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
        {
            let mut supervisors = self.services.lock().expect("lock poisoned");
            supervisors.remove(id);
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
                        continue;
                    };
                    let pid = process.pid();
                    info!("terminating orphaned process for {full_id} with PID {pid}");
                    process.kill();
                }
            }
        }

        // Clean up the on-disk state of the service.
        if let Err(e) = remove_dir_all(run_dir).await {
            if e.kind() != io::ErrorKind::NotFound {
                warn!(
                    "error cleaning up run directory for {full_id}: {}",
                    e.display_with_causes()
                );
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
            services: Arc::clone(&self.services),
            service_event_tx: self.service_event_tx.clone(),
        };

        async move {
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

            supervise_existing_process(&state_updater, &pid_file).await;

            loop {
                let mut cmd = launch_spec.refine_command(
                    &image,
                    &args,
                    &command_wrapper,
                    memory_limit.as_ref(),
                    cpu_limit.as_ref(),
                );
                info!(
                    "launching {full_id}-{i} via {} {}...",
                    cmd.as_std().get_program().to_string_lossy(),
                    cmd.as_std()
                        .get_args()
                        .map(|arg| arg.to_string_lossy())
                        .join(" ")
                );
                if suppress_output {
                    cmd.stdout(Stdio::null());
                    cmd.stderr(Stdio::null());
                }
                match spawn_process(&state_updater, cmd, &pid_file, !command_wrapper.is_empty())
                    .await
                {
                    Ok(status) => {
                        if propagate_crashes && did_process_crash(status) {
                            panic!(
                                "{full_id}-{i} crashed; aborting because propagate_crashes is enabled"
                            );
                        }
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

struct ServiceProcessConfig {
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
}

struct ServiceProcessPort {
    name: String,
    listen_addr: String,
    tcp_proxy_listener: Option<AddressedTcpListener>,
}

/// Supervises an existing process, if it exists.
async fn supervise_existing_process(state_updater: &ProcessStateUpdater, pid_file: &Path) {
    let name = format!(
        "{}-{}-{}",
        state_updater.namespace, state_updater.id, state_updater.i
    );

    let mut system = System::new();
    let Some(process) = find_process_from_pid_file(&mut system, pid_file).await else {
        return;
    };
    let pid = process.pid();

    info!(%pid, "discovered existing process for {name}");
    state_updater.update_state(ProcessStatus::Ready { pid });

    // Kill the process if the future is dropped.
    let need_kill = AtomicBool::new(true);
    defer! {
        state_updater.update_state(ProcessStatus::NotReady);
        if need_kill.load(Ordering::SeqCst) {
            info!(%pid, "terminating existing process for {name}");
            process.kill();
        }
    }

    // Periodically check if the process has terminated.
    let mut system = System::new();
    while system.refresh_process_specifics(pid, ProcessRefreshKind::new()) {
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
    send_sigterm: bool,
) -> Result<ExitStatus, anyhow::Error> {
    struct KillOnDropChild(Child, bool);

    impl Drop for KillOnDropChild {
        fn drop(&mut self) {
            if let (Some(pid), true) = (self.0.id().and_then(|id| i32::try_from(id).ok()), self.1) {
                let _ = nix::sys::signal::kill(
                    nix::unistd::Pid::from_raw(pid),
                    nix::sys::signal::Signal::SIGTERM,
                );
                // Give the process a bit of time to react to the signal
                tokio::task::block_in_place(|| std::thread::sleep(Duration::from_millis(500)));
            }
            let _ = self.0.start_kill();
        }
    }

    let mut child = KillOnDropChild(cmd.spawn()?, send_sigterm);

    // Immediately write out a file containing the PID of the child process and
    // its start time. We'll use this state to rediscover our children if we
    // crash and restart. There's a very small window where we can crash after
    // having spawned the child but before writing this file, in which case we
    // might orphan the process. We accept this risk, though. It's hard to do
    // anything more robust given the Unix APIs available to us, and the
    // solution here is good enough given that the process orchestrator is only
    // used in development/testing.
    let pid = Pid::from_u32(child.0.id().unwrap());
    write_pid_file(pid_file, pid).await?;
    state_updater.update_state(ProcessStatus::Ready { pid });
    Ok(child.0.wait().await?)
}

fn did_process_crash(status: ExitStatus) -> bool {
    // Likely not exhaustive. Feel free to add additional tests for other
    // indications of a crashed child process, as those conditions are
    // discovered.
    matches!(
        status.signal(),
        Some(SIGABRT | SIGBUS | SIGSEGV | SIGTRAP | SIGILL)
    )
}

async fn write_pid_file(pid_file: &Path, pid: Pid) -> Result<(), anyhow::Error> {
    let mut system = System::new();
    system.refresh_process_specifics(pid, ProcessRefreshKind::new());
    let start_time = system.process(pid).map_or(0, |p| p.start_time());
    fs::write(pid_file, format!("{pid}\n{start_time}\n")).await?;
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
    system.refresh_process_specifics(pid, ProcessRefreshKind::new());
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
            Some(Err(e)) = conns.next() => {
                warn!("{name}: tcp proxy connection failed: {}", e.display_with_causes());
            }
        }
    }
}

struct ProcessStateUpdater {
    namespace: String,
    id: String,
    i: usize,
    services: Arc<Mutex<BTreeMap<String, Vec<ProcessState>>>>,
    service_event_tx: broadcast::Sender<ServiceEvent>,
}

impl ProcessStateUpdater {
    fn update_state(&self, status: ProcessStatus) {
        let mut services = self.services.lock().expect("lock poisoned");
        let Some(process_states) = services.get_mut(&self.id) else {
            return;
        };
        let Some(process_state) = process_states.get_mut(self.i) else {
            return;
        };
        let status_time = Utc::now();
        process_state.status = status;
        process_state.status_time = status_time;
        let _ = self.service_event_tx.send(ServiceEvent {
            service_id: self.id.to_string(),
            process_id: u64::cast_from(self.i),
            status: status.into(),
            time: status_time,
        });
    }
}

#[derive(Debug)]
struct ProcessState {
    _handle: AbortOnDropHandle<()>,
    status: ProcessStatus,
    status_time: DateTime<Utc>,
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

fn socket_path(run_dir: &Path, port: &str, process: usize) -> String {
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

#[derive(Debug, Clone)]
struct ProcessService {
    run_dir: PathBuf,
    scale: u16,
}

impl Service for ProcessService {
    fn addresses(&self, port: &str) -> Vec<String> {
        (0..self.scale)
            .map(|i| socket_path(&self.run_dir, port, i.into()))
            .collect()
    }
}
