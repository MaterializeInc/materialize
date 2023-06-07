// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// BEGIN LINT CONFIG
// DO NOT EDIT. Automatically generated by bin/gen-lints.
// Have complaints about the noise? See the note in misc/python/materialize/cli/gen-lints.py first.
#![allow(clippy::style)]
#![allow(clippy::complexity)]
#![allow(clippy::large_enum_variant)]
#![allow(clippy::mutable_key_type)]
#![allow(clippy::stable_sort_primitive)]
#![allow(clippy::map_entry)]
#![allow(clippy::box_default)]
#![warn(clippy::bool_comparison)]
#![warn(clippy::clone_on_ref_ptr)]
#![warn(clippy::no_effect)]
#![warn(clippy::unnecessary_unwrap)]
#![warn(clippy::dbg_macro)]
#![warn(clippy::todo)]
#![warn(clippy::wildcard_dependencies)]
#![warn(clippy::zero_prefixed_literal)]
#![warn(clippy::borrowed_box)]
#![warn(clippy::deref_addrof)]
#![warn(clippy::double_must_use)]
#![warn(clippy::double_parens)]
#![warn(clippy::extra_unused_lifetimes)]
#![warn(clippy::needless_borrow)]
#![warn(clippy::needless_question_mark)]
#![warn(clippy::needless_return)]
#![warn(clippy::redundant_pattern)]
#![warn(clippy::redundant_slicing)]
#![warn(clippy::redundant_static_lifetimes)]
#![warn(clippy::single_component_path_imports)]
#![warn(clippy::unnecessary_cast)]
#![warn(clippy::useless_asref)]
#![warn(clippy::useless_conversion)]
#![warn(clippy::builtin_type_shadow)]
#![warn(clippy::duplicate_underscore_argument)]
#![warn(clippy::double_neg)]
#![warn(clippy::unnecessary_mut_passed)]
#![warn(clippy::wildcard_in_or_patterns)]
#![warn(clippy::crosspointer_transmute)]
#![warn(clippy::excessive_precision)]
#![warn(clippy::overflow_check_conditional)]
#![warn(clippy::as_conversions)]
#![warn(clippy::match_overlapping_arm)]
#![warn(clippy::zero_divided_by_zero)]
#![warn(clippy::must_use_unit)]
#![warn(clippy::suspicious_assignment_formatting)]
#![warn(clippy::suspicious_else_formatting)]
#![warn(clippy::suspicious_unary_op_formatting)]
#![warn(clippy::mut_mutex_lock)]
#![warn(clippy::print_literal)]
#![warn(clippy::same_item_push)]
#![warn(clippy::useless_format)]
#![warn(clippy::write_literal)]
#![warn(clippy::redundant_closure)]
#![warn(clippy::redundant_closure_call)]
#![warn(clippy::unnecessary_lazy_evaluations)]
#![warn(clippy::partialeq_ne_impl)]
#![warn(clippy::redundant_field_names)]
#![warn(clippy::transmutes_expressible_as_ptr_casts)]
#![warn(clippy::unused_async)]
#![warn(clippy::disallowed_methods)]
#![warn(clippy::disallowed_macros)]
#![warn(clippy::disallowed_types)]
#![warn(clippy::from_over_into)]
// END LINT CONFIG

use std::collections::BTreeMap;
use std::env;
use std::fmt::Debug;
use std::fs::Permissions;
use std::future::Future;
use std::net::{IpAddr, SocketAddr, TcpListener as StdTcpListener};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::{ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{bail, Context};
use async_stream::stream;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::future;
use futures::stream::{BoxStream, FuturesUnordered, TryStreamExt};
use itertools::Itertools;
use libc::{SIGABRT, SIGBUS, SIGILL, SIGSEGV, SIGTRAP};
use maplit::btreemap;
use mz_orchestrator::{
    NamespacedOrchestrator, Orchestrator, Service, ServiceConfig, ServiceEvent,
    ServiceProcessMetrics, ServiceStatus,
};
use mz_ore::cast::{CastFrom, ReinterpretCast, TryCastFrom};
use mz_ore::error::ErrorExt;
use mz_ore::netio::UnixSocketAddr;
use mz_ore::result::ResultExt;
use mz_ore::task::{self, AbortOnDropHandle, JoinHandleExt};
use mz_pid_file::PidFile;
use scopeguard::defer;
use serde::Serialize;
use sha1::{Digest, Sha1};
use sysinfo::{Pid, PidExt, ProcessExt, ProcessRefreshKind, System, SystemExt};
use tokio::fs::remove_dir_all;
use tokio::net::{TcpListener, UnixStream};
use tokio::process::{Child, Command};
use tokio::sync::broadcast::{self, Sender};
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
    pub scratch_directory: Option<PathBuf>,
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
///
/// Processes launched by this orchestrator must support a `--pid-file-location`
/// command line flag which causes a PID file to be emitted at the specified
/// path.
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
    scratch_directory: Option<PathBuf>,
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
        })
    }
}

impl Orchestrator for ProcessOrchestrator {
    fn namespace(&self, namespace: &str) -> Arc<dyn NamespacedOrchestrator> {
        let (service_event_tx, _) = broadcast::channel(16384);
        let mut namespaces = self.namespaces.lock().expect("lock poisoned");
        Arc::clone(namespaces.entry(namespace.into()).or_insert_with(|| {
            Arc::new(NamespacedProcessOrchestrator {
                namespace: namespace.into(),
                image_dir: self.image_dir.clone(),
                suppress_output: self.suppress_output,
                secrets_dir: self.secrets_dir.clone(),
                metadata_dir: self.metadata_dir.clone(),
                command_wrapper: self.command_wrapper.clone(),
                services: Arc::new(Mutex::new(BTreeMap::new())),
                service_event_tx,
                system: Mutex::new(System::new()),
                propagate_crashes: self.propagate_crashes,
                tcp_proxy: self.tcp_proxy.clone(),
                scratch_directory: self.scratch_directory.clone(),
            })
        }))
    }
}

#[derive(Debug)]
struct NamespacedProcessOrchestrator {
    namespace: String,
    image_dir: PathBuf,
    suppress_output: bool,
    secrets_dir: PathBuf,
    metadata_dir: PathBuf,
    command_wrapper: Vec<String>,
    services: Arc<Mutex<BTreeMap<String, Vec<ProcessState>>>>,
    service_event_tx: Sender<ServiceEvent>,
    system: Mutex<System>,
    propagate_crashes: bool,
    tcp_proxy: Option<ProcessOrchestratorTcpProxyConfig>,
    scratch_directory: Option<PathBuf>,
}

#[async_trait]
impl NamespacedOrchestrator for NamespacedProcessOrchestrator {
    async fn fetch_service_metrics(
        &self,
        id: &str,
    ) -> Result<Vec<ServiceProcessMetrics>, anyhow::Error> {
        let pids: Vec<_> = {
            let services = self.services.lock().expect("lock poisoned");
            let Some(service) = services.get(id) else {
                bail!("unknown service {id}")
            };
            service.iter().map(|p| p.pid()).collect()
        };

        let mut system = self.system.lock().expect("lock poisoned");
        let mut metrics = vec![];
        for pid in pids {
            let (cpu_nano_cores, memory_bytes) = match pid {
                None => (None, None),
                Some(pid) => {
                    system.refresh_process_specifics(pid, ProcessRefreshKind::new().with_cpu());
                    match system.process(pid) {
                        None => (None, None),
                        Some(process) => {
                            // Justification for `unwrap`:
                            //
                            // `u64::try_cast_from(f: f64)`
                            // will always succeed if 0 <= f <= 2^53.
                            // Since the max value of `process.cpu_usage()` is
                            // 100.0 * num_of_cores, this will be true whenever there
                            // are less than 2^53 / 10^9 logical cores, or about
                            // 9 million.
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
            });
        }
        Ok(metrics)
    }

    async fn ensure_service(
        &self,
        id: &str,
        ServiceConfig {
            image,
            init_container_image: _,
            args,
            ports: ports_in,
            memory_limit: _,
            cpu_limit: _,
            scale,
            labels,
            availability_zone: _,
            anti_affinity: _,
        }: ServiceConfig<'_>,
    ) -> Result<Box<dyn Service>, anyhow::Error> {
        let full_id = format!("{}-{}", self.namespace, id);

        let run_dir = self.metadata_dir.join(&full_id);
        fs::create_dir_all(&run_dir)
            .await
            .context("creating run directory")?;

        {
            let mut services = self.services.lock().expect("lock poisoned");
            let process_states = services.entry(id.to_string()).or_default();

            // Create the state for new processes.
            let mut new_process_states = vec![];
            for i in process_states.len()..scale.into() {
                // Allocate listeners for each TCP proxy, if requested.
                let mut ports = vec![];
                let mut tcp_proxy_addrs = BTreeMap::new();
                for port in &ports_in {
                    let tcp_proxy_listener = match &self.tcp_proxy {
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
                        tcp_proxy_listener,
                    });
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
                        ports,
                    }),
                );

                new_process_states.push(ProcessState {
                    _handle: handle.abort_on_drop(),
                    status: ProcessStatus::NotReady,
                    status_time: Utc::now(),
                    labels: labels.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
                    tcp_proxy_addrs,
                });
            }

            // Update the in-memory process state. We do this after we've created
            // all process states to avoid partially updating our in-memory state.
            process_states.truncate(scale.into());
            process_states.extend(new_process_states);
        }

        self.maybe_write_prometheus_service_discovery_file().await;

        Ok(Box::new(ProcessService { run_dir, scale }))
    }

    async fn drop_service(&self, id: &str) -> Result<(), anyhow::Error> {
        {
            let mut supervisors = self.services.lock().expect("lock poisoned");
            supervisors.remove(id);
        }
        self.maybe_write_prometheus_service_discovery_file().await;
        Ok(())
    }

    async fn list_services(&self) -> Result<Vec<String>, anyhow::Error> {
        let supervisors = self.services.lock().expect("lock poisoned");
        Ok(supervisors.keys().cloned().collect())
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
            self.service_event_tx.subscribe()
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
}

impl NamespacedProcessOrchestrator {
    fn supervise_service_process(
        &self,
        ServiceProcessConfig {
            id,
            run_dir,
            i,
            image,
            args,
            ports,
        }: ServiceProcessConfig,
    ) -> impl Future<Output = ()> {
        let suppress_output = self.suppress_output;
        let propagate_crashes = self.propagate_crashes;
        let command_wrapper = self.command_wrapper.clone();
        let image = self.image_dir.join(image);
        let pid_file = run_dir.join(format!("{i}.pid"));
        let full_id = format!("{}-{}", self.namespace, id);

        let state_updater = ProcessStateUpdater {
            namespace: self.namespace.clone(),
            id: id.clone(),
            i,
            services: Arc::clone(&self.services),
            service_event_tx: self.service_event_tx.clone(),
        };

        let listen_addrs = ports
            .iter()
            .map(|p| {
                let addr = socket_path(&run_dir, &p.name, i);
                (p.name.clone(), addr)
            })
            .collect();
        let mut args = args(&listen_addrs);
        args.push(format!("--pid-file-location={}", pid_file.display()));
        args.push("--secrets-reader=process".into());
        args.push(format!(
            "--secrets-reader-process-dir={}",
            self.secrets_dir.display()
        ));

        let scratch_directory = self.scratch_directory.as_ref().map(|dir| dir.join(id));
        if let Some(scratch_directory) = &scratch_directory {
            args.push(format!(
                "--scratch-directory={}",
                scratch_directory.display()
            ));
        }

        async move {
            let mut proxy_handles = vec![];
            for port in ports {
                if let Some(tcp_listener) = port.tcp_proxy_listener {
                    info!(
                        "{full_id}-{i}: {} tcp proxy listening on {}",
                        port.name, tcp_listener.local_addr,
                    );
                    let uds_path = &listen_addrs[&port.name];
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

            // Clean up scratch directory when the service is terminated.
            // This is best effort as a development and testing convenience.
            // Because the process orchestrator is not used in production, we
            // don't need to be perfectly robust with the cleanup.
            let _guard = scopeguard::guard((), |_| {
                if let Some(scratch) = scratch_directory {
                    info!(scratch_dir = %scratch.display(), "cleaning up scratch directory");
                    task::spawn(|| "clean_cluster_scratch_directory", async {
                        if let Err(e) = remove_dir_all(scratch).await {
                            warn!(
                                "Error cleaning up scratch directory: {}",
                                e.display_with_causes()
                            );
                        }
                    });
                }
            });

            supervise_existing_process(&state_updater, &pid_file).await;

            loop {
                let mut cmd = if command_wrapper.is_empty() {
                    let mut cmd = Command::new(&image);
                    cmd.args(&args);
                    cmd
                } else {
                    let mut cmd = Command::new(&command_wrapper[0]);
                    cmd.args(
                        command_wrapper[1..]
                            .iter()
                            .map(|part| interpolate_command(part, &full_id, &listen_addrs)),
                    );
                    cmd.arg(&image);
                    cmd.args(&args);
                    cmd
                };
                info!(
                    "launching {full_id}-{i} via {}...",
                    cmd.as_std()
                        .get_args()
                        .map(|arg| arg.to_string_lossy())
                        .join(" ")
                );
                if suppress_output {
                    cmd.stdout(Stdio::null());
                    cmd.stderr(Stdio::null());
                }
                match spawn_process(&state_updater, cmd).await {
                    Ok(status) => {
                        if propagate_crashes && did_process_crash(status) {
                            panic!("{full_id}-{i} crashed; aborting because propagate_crashes is enabled");
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

        let Some(tcp_proxy) = &self.tcp_proxy else { return };
        let Some(dir) = &tcp_proxy.prometheus_service_discovery_dir else { return };

        let mut static_configs = vec![];
        {
            let services = self.services.lock().expect("lock poisoned");
            for (id, states) in &*services {
                for (i, state) in states.iter().enumerate() {
                    for (name, addr) in &state.tcp_proxy_addrs {
                        let mut labels = btreemap! {
                            "mz_orchestrator_namespace".into() => self.namespace.clone(),
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

        let path = dir.join(Path::new(&self.namespace).with_extension("json"));
        let contents = serde_json::to_vec_pretty(&static_configs).expect("valid json");
        if let Err(e) = fs::write(&path, &contents).await {
            warn!(
                "{}: failed to write prometheus service discovery file: {}",
                self.namespace,
                e.display_with_causes()
            );
        }
    }
}

struct ServiceProcessConfig<'a> {
    id: String,
    run_dir: PathBuf,
    i: usize,
    image: String,
    args: &'a (dyn Fn(&BTreeMap<String, String>) -> Vec<String> + Send + Sync),
    ports: Vec<ServiceProcessPort>,
}

struct ServiceProcessPort {
    name: String,
    tcp_proxy_listener: Option<AddressedTcpListener>,
}

/// Supervises an existing process, if it exists.
async fn supervise_existing_process(state_updater: &ProcessStateUpdater, pid_file: &Path) {
    let name = format!(
        "{}-{}-{}",
        state_updater.namespace, state_updater.id, state_updater.i
    );

    let Ok(pid) = PidFile::read(pid_file) else {
        return;
    };

    let pid = Pid::from_u32(u32::reinterpret_cast(pid));
    let mut system = System::new();
    system.refresh_process_specifics(pid, ProcessRefreshKind::new());
    let Some(process) = system.process(pid) else {
        return;
    };

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
) -> Result<ExitStatus, anyhow::Error> {
    struct KillOnDropChild(Child);

    impl Drop for KillOnDropChild {
        fn drop(&mut self) {
            let _ = self.0.start_kill();
        }
    }

    let mut child = KillOnDropChild(cmd.spawn()?);
    state_updater.update_state(ProcessStatus::Ready {
        pid: Pid::from_u32(child.0.id().unwrap()),
    });
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
    let mut conns = FuturesUnordered::<Pin<Box<dyn Future<Output = _> + Send>>>::new();
    conns.push(Box::pin(future::pending()));
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
            res = conns.try_next() => {
                if let Err(e) = res {
                    warn!("{name}: tcp proxy connection failed: {}", e.display_with_causes());
                }
            }
        }
    }
}

struct ProcessStateUpdater {
    namespace: String,
    id: String,
    i: usize,
    services: Arc<Mutex<BTreeMap<String, Vec<ProcessState>>>>,
    service_event_tx: Sender<ServiceEvent>,
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
            ProcessStatus::NotReady => ServiceStatus::NotReady(None),
            ProcessStatus::Ready { .. } => ServiceStatus::Ready,
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
