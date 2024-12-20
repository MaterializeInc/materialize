use std::{
    any::Any,
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use futures::stream::BoxStream;

use mz_orchestrator::{
    scheduling_config, NamespacedOrchestrator, Orchestrator, Service, ServiceConfig, ServiceEvent,
    ServiceProcessMetrics,
};

/// Configures an [`ExternalOrchestrator`].
#[derive(Debug, Clone)]
pub struct ExternalOrchestratorConfig {}

#[derive(Debug)]
pub struct ExternalOrchestrator {
    config: ExternalOrchestratorConfig,
    namespaces: Mutex<BTreeMap<String, Arc<dyn NamespacedOrchestrator>>>,
}

impl ExternalOrchestrator {
    pub fn new(config: ExternalOrchestratorConfig) -> Self {
        Self {
            config,
            namespaces: Mutex::new(BTreeMap::new()),
        }
    }
}

impl Orchestrator for ExternalOrchestrator {
    fn namespace(&self, namespace: &str) -> Arc<dyn mz_orchestrator::NamespacedOrchestrator> {
        let mut namespaces = self.namespaces.lock().expect("lock poisoned");
        Arc::clone(namespaces.entry(namespace.into()).or_insert_with(|| {
            Arc::new(NamespacedExternalOrchestrator {
                namespace: namespace.into(),
                config: self.config.clone(),
            })
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
}

#[async_trait]
impl NamespacedOrchestrator for NamespacedExternalOrchestrator {
    fn ensure_service(
        &self,
        id: &str,
        config: ServiceConfig,
    ) -> Result<Box<dyn Service>, anyhow::Error> {
        todo!()
    }

    fn drop_service(&self, id: &str) -> Result<(), anyhow::Error> {
        todo!()
    }

    async fn list_services(&self) -> Result<Vec<String>, anyhow::Error> {
        todo!()
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
