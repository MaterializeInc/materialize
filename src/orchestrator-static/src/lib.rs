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
#![allow(unknown_lints)]
#![allow(clippy::style)]
#![allow(clippy::complexity)]
#![allow(clippy::large_enum_variant)]
#![allow(clippy::mutable_key_type)]
#![allow(clippy::stable_sort_primitive)]
#![allow(clippy::map_entry)]
#![allow(clippy::box_default)]
#![allow(clippy::drain_collect)]
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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use anyhow::Error;
use async_trait::async_trait;
use futures::stream::BoxStream;
use mz_adapter::catalog::ClusterReplicaSizeMap;
use mz_controller::clusters::ReplicaAllocation;
use mz_orchestrator::scheduling_config::ServiceSchedulingConfig;
use mz_orchestrator::{
    NamespacedOrchestrator, Orchestrator, Service, ServiceConfig, ServiceEvent,
    ServiceProcessMetrics,
};
use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct StaticOrchestratorConfig {
    static_replicas: BTreeMap<String, StaticReplica>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StaticReplica {
    allocation: ReplicaAllocation,
    ports: BTreeMap<String, Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct StaticOrchestrator {
    config: StaticOrchestratorConfig,
    inner: Arc<dyn Orchestrator>,
    services: Arc<Mutex<BTreeSet<String>>>,
}

#[derive(Debug, Clone)]
struct NamespacedStaticOrchestrator {
    config: StaticOrchestratorConfig,
    inner: Arc<dyn NamespacedOrchestrator>,
    services: Arc<Mutex<BTreeSet<String>>>,
}

#[derive(Debug, Clone)]
struct StaticService {
    config: BTreeMap<String, Vec<String>>,
}

impl StaticOrchestrator {
    pub fn new(
        inner: Arc<dyn Orchestrator>,
        static_replicas: BTreeMap<String, StaticReplica>,
    ) -> Self {
        Self {
            config: StaticOrchestratorConfig { static_replicas },
            inner,
            services: Arc::new(Mutex::new(Default::default())),
        }
    }

    pub fn update_replica_size_map(&self, replica_size_map: &mut ClusterReplicaSizeMap) {
        for (name, replica) in &self.config.static_replicas {
            let old = replica_size_map
                .0
                .insert(name.clone(), replica.allocation.clone());
            assert!(
                old.is_none(),
                "Overriding existing size not supported, size {name}"
            );
        }
    }
}

impl Orchestrator for StaticOrchestrator {
    fn namespace(&self, namespace: &str) -> Arc<dyn NamespacedOrchestrator> {
        Arc::new(NamespacedStaticOrchestrator {
            inner: self.inner.namespace(namespace),
            config: self.config.clone(),
            services: Arc::clone(&self.services),
        })
    }
}

#[async_trait]
impl NamespacedOrchestrator for NamespacedStaticOrchestrator {
    async fn ensure_service(
        &self,
        id: &str,
        config: ServiceConfig<'_>,
    ) -> Result<Box<dyn Service>, Error> {
        if let Some(config) = config
            .labels
            .get("size")
            .and_then(|size| self.config.static_replicas.get(&*size))
            .cloned()
        {
            Ok(Box::new(StaticService {
                config: config.ports,
            }))
        } else {
            self.inner.ensure_service(id, config).await
        }
    }

    async fn drop_service(&self, id: &str) -> Result<(), Error> {
        if !self.services.lock().unwrap().remove(id) {
            self.inner.drop_service(id).await
        } else {
            Ok(())
        }
    }

    async fn list_services(&self) -> Result<Vec<String>, Error> {
        let mut services = self.inner.list_services().await?;
        services.extend(self.services.lock().unwrap().iter().cloned());
        Ok(services)
    }

    fn watch_services(&self) -> BoxStream<'static, Result<ServiceEvent, Error>> {
        self.inner.watch_services()
    }

    async fn fetch_service_metrics(&self, id: &str) -> Result<Vec<ServiceProcessMetrics>, Error> {
        if self.services.lock().unwrap().contains(id) {
            Ok(vec![])
        } else {
            self.inner.fetch_service_metrics(id).await
        }
    }

    fn update_scheduling_config(&self, config: ServiceSchedulingConfig) {
        self.inner.update_scheduling_config(config)
    }
}

impl Service for StaticService {
    fn addresses(&self, port: &str) -> Vec<String> {
        self.config
            .get(port)
            .unwrap_or_else(|| {
                panic!(
                    "No address found for port {port}, available: {:?}",
                    self.config
                )
            })
            .clone()
    }
}
