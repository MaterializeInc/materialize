use std::{
    any::Any,
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
};

use anyhow::bail;
use async_trait::async_trait;
use futures::stream::BoxStream;
use tokio::sync::{
    mpsc::{self, UnboundedReceiver, UnboundedSender},
    oneshot,
};
use uuid::Uuid;

use mz_adapter::{
    session::SessionConfig, Client, ExecuteResponse, PeekResponseUnary, SessionClient,
};
use mz_orchestrator::{
    scheduling_config, NamespacedOrchestrator, Orchestrator, Service, ServiceConfig, ServiceEvent,
    ServiceProcessMetrics,
};
use mz_ore::{collections::CollectionExt, task::AbortOnDropHandle};
use mz_repr::RowIterator;
use mz_sql::{parse::StatementParseResult, session::user::SYSTEM_USER};

/// Configures an [`ExternalOrchestrator`].
#[derive(Debug, Clone)]
pub struct ExternalOrchestratorConfig {
    /// Template to use to generate service hostnames given a service name
    /// and id. For instance:
    /// "{name}-{i}.{name}.mz-environment.svc.cluster.local"
    pub hostname_template: String,
}

#[derive(Debug)]
pub struct ExternalOrchestrator {
    config: ExternalOrchestratorConfig,
    namespaces: Mutex<BTreeMap<String, Arc<dyn NamespacedOrchestrator>>>,
    adapter_client: Mutex<Option<Client>>,
}

impl ExternalOrchestrator {
    pub fn new(config: ExternalOrchestratorConfig) -> Self {
        Self {
            config,
            namespaces: Mutex::new(BTreeMap::new()),
            adapter_client: Mutex::new(None),
        }
    }

    fn adapter_client(&self) -> Option<Client> {
        self.adapter_client.lock().expect("lock poisoned").clone()
    }

    pub fn set_adapter_client(&self, new_client: Client) {
        *self.adapter_client.lock().expect("lock poisoned") = Some(new_client.clone());
        for namespace in self.namespaces.lock().expect("lock poisoned").values() {
            if let Some(namespace) = Arc::clone(namespace)
                .as_any()
                .downcast_ref::<NamespacedExternalOrchestrator>()
            {
                namespace.set_adapter_client(new_client.clone());
            }
        }
    }
}

impl Orchestrator for ExternalOrchestrator {
    fn namespace(&self, namespace: &str) -> Arc<dyn mz_orchestrator::NamespacedOrchestrator> {
        let mut namespaces = self.namespaces.lock().expect("lock poisoned");
        Arc::clone(namespaces.entry(namespace.into()).or_insert_with(|| {
            let (command_tx, command_rx) = mpsc::unbounded_channel();
            let worker = OrchestratorWorker::new(command_rx)
                .spawn(format!("external-orchestrator-worker:{namespace}"));
            let namespace = NamespacedExternalOrchestrator {
                namespace: namespace.into(),
                config: self.config.clone(),
                command_tx,
                _worker: worker,
            };
            if let Some(client) = &self.adapter_client() {
                namespace.set_adapter_client(client.clone());
            }
            Arc::new(namespace)
        }))
    }

    fn as_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }
}

#[derive(Debug)]
pub struct NamespacedExternalOrchestrator {
    namespace: String,
    config: ExternalOrchestratorConfig,
    command_tx: UnboundedSender<Command>,
    _worker: AbortOnDropHandle<()>,
}

impl NamespacedExternalOrchestrator {
    fn set_adapter_client(&self, client: Client) {
        self.command_tx
            .send(Command::SetAdapterClient { client })
            .expect("worker task not dropped");
    }
}

#[async_trait]
impl NamespacedOrchestrator for NamespacedExternalOrchestrator {
    fn ensure_service(
        &self,
        id: &str,
        config: ServiceConfig,
    ) -> Result<Box<dyn Service>, anyhow::Error> {
        self.command_tx
            .send(Command::Ensure {
                id: format!("{}-{}", self.namespace, id),
            })
            .expect("worker task not dropped");
        let hosts = (0..config.scale)
            .map(|i| {
                self.config
                    .hostname_template
                    .replace("{name}", &format!("{}-{}", self.namespace, id))
                    .replace("{idx}", &i.to_string())
            })
            .collect::<Vec<_>>();
        let ports = config
            .ports
            .into_iter()
            .map(|p| (p.name, p.port_hint))
            .collect();
        Ok(Box::new(ExternalService { hosts, ports }))
    }

    fn drop_service(&self, id: &str) -> Result<(), anyhow::Error> {
        self.command_tx
            .send(Command::Drop {
                id: format!("{}-{}", self.namespace, id),
            })
            .expect("worker task not dropped");
        Ok(())
    }

    async fn list_services(&self) -> Result<Vec<String>, anyhow::Error> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(Command::List { reply: tx })
            .expect("worker task not dropped");
        Ok(rx.await?)
    }

    fn watch_services(&self) -> BoxStream<'static, Result<ServiceEvent, anyhow::Error>> {
        todo!()
    }

    async fn fetch_service_metrics(
        &self,
        _id: &str,
    ) -> Result<Vec<ServiceProcessMetrics>, anyhow::Error> {
        todo!()
    }

    fn update_scheduling_config(&self, _config: scheduling_config::ServiceSchedulingConfig) {
        todo!()
    }

    fn as_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }
}

#[derive(Debug)]
pub struct ExternalService {
    hosts: Vec<String>,
    ports: BTreeMap<String, u16>,
}

impl Service for ExternalService {
    fn addresses(&self, port: &str) -> Vec<String> {
        let port = self.ports[port];
        self.hosts
            .iter()
            .map(|host| format!("{host}:{port}"))
            .collect()
    }
}

enum Command {
    Ensure { id: String },
    Drop { id: String },
    List { reply: oneshot::Sender<Vec<String>> },
    SetAdapterClient { client: Client },
}

struct OrchestratorWorker {
    command_rx: UnboundedReceiver<Command>,
    session_client: Option<SessionClient>,
    init_queue: VecDeque<Command>,
}

impl OrchestratorWorker {
    fn new(command_rx: UnboundedReceiver<Command>) -> Self {
        Self {
            command_rx,
            session_client: None,
            init_queue: VecDeque::new(),
        }
    }
    fn spawn(self, name: String) -> AbortOnDropHandle<()> {
        mz_ore::task::spawn(|| name, self.run()).abort_on_drop()
    }

    async fn run(mut self) {
        while let Some(cmd) = self.command_rx.recv().await {
            self.wait_for_adapter_client(cmd).await.unwrap();

            if self.session_client.is_some() {
                while let Some(cmd) = self.init_queue.pop_front() {
                    self.handle_command(cmd).await.unwrap();
                }
                break;
            }
        }
        while let Some(cmd) = self.command_rx.recv().await {
            self.handle_command(cmd).await.unwrap();
        }
    }

    async fn wait_for_adapter_client(&mut self, command: Command) -> Result<(), anyhow::Error> {
        match command {
            Command::SetAdapterClient { client } => {
                self.session_client = Some(session_client(client).await?);
            }
            _ => {
                self.init_queue.push_back(command);
            }
        }
        Ok(())
    }

    async fn handle_command(&mut self, command: Command) -> Result<(), anyhow::Error> {
        match command {
            Command::Ensure { id } => {
                // TODO: use placeholders here?
                self.execute_sql(
                    &format!(
                        "INSERT INTO mz_internal.mz_external_orchestrator_services VALUES ('{id}', 'Pending')"
                    ),
                )
                .await?;
            }
            Command::Drop { id } => {
                // TODO: use placeholders here?
                self.execute_sql(&format!(
                    "DELETE FROM mz_internal.mz_external_orchestrator_services WHERE id = '{id}'"
                ))
                .await?;
            }
            Command::List { reply } => {
                let rows = self
                    .execute_sql("SELECT id FROM mz_internal.mz_external_orchestrator_services")
                    .await?;
                reply
                    .send(
                        rows.map(|row| row.into_iter().next().unwrap().to_string())
                            .collect(),
                    )
                    .expect("channel closed");
            }
            Command::SetAdapterClient { .. } => {
                // this can't currently happen
                panic!("adapter client already set")
            }
        }
        Ok(())
    }

    async fn execute_sql(&mut self, sql: &str) -> Result<Box<dyn RowIterator>, anyhow::Error> {
        let session_client = self
            .session_client
            .as_mut()
            .expect("adapter client not initialized");
        let stmts = mz_sql::parse::parse(sql)?;
        if stmts.len() != 1 {
            bail!("must supply exactly one query");
        }
        let StatementParseResult { ast: stmt, sql } = stmts.into_element();

        const EMPTY_PORTAL: &str = "";
        session_client.start_transaction(Some(1))?;
        session_client
            .declare(EMPTY_PORTAL.into(), stmt, sql.to_string())
            .await?;
        match session_client
            .execute(EMPTY_PORTAL.into(), futures::future::pending(), None)
            .await?
        {
            (ExecuteResponse::SendingRows { future, .. }, _) => match future.await {
                PeekResponseUnary::Rows(rows) => Ok(rows),
                PeekResponseUnary::Canceled => bail!("query canceled"),
                PeekResponseUnary::Error(e) => bail!(e),
            },
            r => bail!("unsupported response type: {r:?}"),
        }
    }
}

async fn session_client(client: Client) -> Result<SessionClient, anyhow::Error> {
    let conn_id = client.new_conn_id()?;
    let session = client.new_session(SessionConfig {
        conn_id,
        uuid: Uuid::new_v4(),
        user: SYSTEM_USER.name.clone(),
        client_ip: None,
        external_metadata_rx: None,
        helm_chart_version: None,
    });
    Ok(client.startup(session).await?)
}
