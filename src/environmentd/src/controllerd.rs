// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Deployment-scoped process management, separate from SQL coordination.

pub mod provisioning;

/// Completes fresh deployment initialization before provisioning its children.
///
/// The launcher supplies the writable initializer admission. Existing environments
/// are rejected here rather than replaying positive snapshots on restart. This
/// path starts neither a SQL coordinator nor shared maintenance.
pub async fn initialize_fresh(
    config: mz_catalog::config::Config<'_>,
    oracle: &(dyn mz_timestamp_oracle::TimestampOracle<mz_repr::Timestamp> + Send + Sync),
) -> Result<mz_catalog::catalog::Catalog, mz_catalog::catalog::CatalogError> {
    if !config.storage.initialized_catalog() {
        return Err(
            anyhow::anyhow!("fresh controller initialization requires a new catalog").into(),
        );
    }
    let persist = config.state.persist_client.clone();
    let metrics = config.metrics_registry.clone();
    mz_catalog::catalog::Catalog::open(config)
        .await?
        .complete_fresh_deployment_initialization(persist, &metrics, oracle)
        .await
}
