use std::collections::BTreeMap;
use std::sync::{Arc, mpsc};

use async_trait::async_trait;
use clap::Parser;
use futures::stream::BoxStream;
use mz_cluster_client::client::TimelyConfig;
use mz_orchestrator::scheduling_config::ServiceSchedulingConfig;
use mz_orchestrator::{
    NamespacedOrchestrator, Orchestrator, Service, ServiceAssignments, ServiceConfig, ServiceEvent,
    ServiceProcessMetrics,
};
use mz_ore::netio::SocketAddr;

pub(crate) enum Command {
    CreateProcess { name: String, args: ClusterdArgs },
}

#[derive(Clone, clap::Parser)]
pub struct ClusterdArgs {
    #[clap(long)]
    pub storage_controller_listen_addr: SocketAddr,
    #[clap(long)]
    pub compute_controller_listen_addr: SocketAddr,
    #[clap(long)]
    pub storage_timely_config: TimelyConfig,
    #[clap(long)]
    pub compute_timely_config: TimelyConfig,
    #[clap(long)]
    pub process: usize,
}

#[derive(Debug)]
pub(crate) struct SimulationOrchestrator {
    command_tx: mpsc::Sender<Command>,
}

impl SimulationOrchestrator {
    pub fn new(command_tx: mpsc::Sender<Command>) -> Self {
        Self { command_tx }
    }
}

impl Orchestrator for SimulationOrchestrator {
    fn namespace(&self, _namespace: &str) -> std::sync::Arc<dyn NamespacedOrchestrator> {
        Arc::new(NamespacedSimulationOrchestrator {
            command_tx: self.command_tx.clone(),
        })
    }
}

#[derive(Debug)]
struct NamespacedSimulationOrchestrator {
    command_tx: mpsc::Sender<Command>,
}

#[async_trait]
impl NamespacedOrchestrator for NamespacedSimulationOrchestrator {
    fn ensure_service(&self, id: &str, config: ServiceConfig) -> anyhow::Result<Box<dyn Service>> {
        let proc_names: Vec<_> = (0..config.scale.get())
            .map(|i| format!("clusterd-{id}-{i}"))
            .collect();
        let ports: BTreeMap<_, _> = config
            .ports
            .into_iter()
            .map(|p| (p.name, p.port_hint))
            .collect();

        let mut listen_addrs = BTreeMap::new();
        let mut peer_addrs = vec![BTreeMap::new(); proc_names.len()];
        for (name, port) in &ports {
            listen_addrs.insert(name.clone(), format!("0.0.0.0:{port}"));
            for (i, proc_name) in proc_names.iter().enumerate() {
                peer_addrs[i].insert(name.clone(), format!("{proc_name}:{port}"));
            }
        }
        let args = (config.args)(ServiceAssignments {
            listen_addrs: &listen_addrs,
            peer_addrs: &peer_addrs,
        });

        // Clap has no option to ignore unknown arguments, so we need to filter them out instead.
        // TODO: `ignore_errors` should work, but for some reason it doesn't. Figure out why.
        let args: Vec<_> = args
            .into_iter()
            .filter(|a| {
                a.starts_with("--storage-controller-listen-addr")
                    || a.starts_with("--compute-controller-listen-addr")
                    || a.starts_with("--storage-timely-config")
                    || a.starts_with("--compute-timely-config")
            })
            .collect();

        for (i, proc_name) in proc_names.iter().enumerate() {
            let mut cli_args = vec!["clusterd".to_string()];
            cli_args.extend(args.iter().cloned());
            cli_args.push(format!("--process={i}"));
            let args = ClusterdArgs::try_parse_from(cli_args).unwrap();

            self.command_tx
                .send(Command::CreateProcess {
                    name: proc_name.clone(),
                    args,
                })
                .expect("receiver alive");
        }

        Ok(Box::new(SimulatedService { proc_names, ports }))
    }

    fn drop_service(&self, _id: &str) -> anyhow::Result<()> {
        Ok(())
    }

    async fn list_services(&self) -> anyhow::Result<Vec<String>> {
        Ok(Vec::new())
    }

    fn watch_services(&self) -> BoxStream<'static, anyhow::Result<ServiceEvent>> {
        Box::pin(futures::stream::empty())
    }

    async fn fetch_service_metrics(&self, _id: &str) -> anyhow::Result<Vec<ServiceProcessMetrics>> {
        Ok(Vec::new())
    }

    fn update_scheduling_config(&self, _config: ServiceSchedulingConfig) {}
}

#[derive(Debug)]
struct SimulatedService {
    proc_names: Vec<String>,
    ports: BTreeMap<String, u16>,
}

impl Service for SimulatedService {
    fn addresses(&self, port: &str) -> Vec<String> {
        let port = self.ports[port];
        self.proc_names
            .iter()
            .map(|host| format!("{host}:{port}"))
            .collect()
    }
}
