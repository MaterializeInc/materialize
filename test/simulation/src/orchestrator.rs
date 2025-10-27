use std::num::NonZero;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;
use mz_orchestrator::scheduling_config::ServiceSchedulingConfig;
use mz_orchestrator::{
    NamespacedOrchestrator, Orchestrator, Service, ServiceConfig, ServiceEvent,
    ServiceProcessMetrics,
};

#[derive(Debug)]
pub(crate) struct SimulationOrchestrator {}

impl SimulationOrchestrator {
    pub fn new() -> Self {
        Self {}
    }
}

impl Orchestrator for SimulationOrchestrator {
    fn namespace(&self, _namespace: &str) -> std::sync::Arc<dyn NamespacedOrchestrator> {
        Arc::new(NamespacedSimulationOrchestrator {})
    }
}

#[derive(Debug)]
struct NamespacedSimulationOrchestrator {}

#[async_trait]
impl NamespacedOrchestrator for NamespacedSimulationOrchestrator {
    fn ensure_service(&self, _id: &str, config: ServiceConfig) -> anyhow::Result<Box<dyn Service>> {
        Ok(Box::new(SimulatedService {
            processes: config.scale,
        }))
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
    processes: NonZero<u16>,
}

impl Service for SimulatedService {
    fn addresses(&self, _port: &str) -> Vec<String> {
        (0..self.processes.get())
            .map(|i| format!("clusterd:{}", 6000 + i))
            .collect()
    }
}
