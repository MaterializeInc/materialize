// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{borrow::Borrow, time::Duration};

use derivative::Derivative;
use launchdarkly_server_sdk as ld;
use tokio::time;

use super::params::SynchronizedParameter;
use super::SynchronizedParameters;
use crate::session::vars::Value;

/// A frontend client for pulling [SynchronizedParameters].
#[derive(Derivative)]
#[derivative(Debug)]
pub struct SystemParameterFrontend {
    #[derivative(Debug = "ignore")]
    ld_client: ld::Client,
    ld_user: ld::User,
}

impl SystemParameterFrontend {
    /// Construct a new [SystemParameterFrontend] instance.
    pub fn new(ld_sdk_key: &str, ld_user_key: &str) -> Result<Self, anyhow::Error> {
        let ld_config = ld::ConfigBuilder::new(ld_sdk_key).build();
        let ld_client = ld::Client::build(ld_config).map_err(|e| anyhow::format_err!(e))?;
        let ld_user = ld::User::with_key(ld_user_key).build();

        Ok(Self { ld_client, ld_user })
    }

    /// Try to initialize the backing [ld::Client] once.
    ///
    /// Return `true` if the initialization was successful.
    pub async fn start_and_initialize(&self) -> bool {
        // Start and initialize LD client for the frontend.
        self.ld_client.start_with_default_executor();
        self.ld_client.initialized_async().await
    }

    /// Ensure the backing [ld::Client] is initialized.
    ///
    /// The [ld::Client::initialized_async] call will be attempted in a loop
    /// with an exponential backoff with power `2s` and max duration `60s`.
    pub async fn ensure_initialized(&self) {
        tracing::info!("waiting for SystemParameterFrontend to initialize");

        // Start and initialize LD client for the frontend.
        self.ld_client.start_with_default_executor();

        let max_backoff = Duration::from_secs(60);
        let mut backoff = Duration::from_secs(5);
        while !self.ld_client.initialized_async().await {
            tracing::error!("SystemParameterFrontend failed to initialize");
            time::sleep(backoff).await;
            backoff = (backoff * 2).min(max_backoff);
        }

        tracing::info!("successfully initialized SystemParameterFrontend");
    }

    /// Pull the current values for all [SynchronizedParameters] from the
    /// [SystemParameterFrontend] and return `true` iff at least one parameter
    /// value was modified.
    pub fn pull(&self, params: &mut SynchronizedParameters) -> bool {
        let mut changed = false;

        changed |= self.pull_parameter(&mut params.window_functions);
        changed |= self.pull_parameter(&mut params.allowed_cluster_replica_sizes);
        changed |= self.pull_parameter(&mut params.max_result_size);

        changed
    }

    /// Pull the current value for the given [SynchronizedParameter] from
    /// the [SystemParameterFrontend] and return `true` iff the parameter has been
    /// modified.
    fn pull_parameter<V: Value>(&self, param: &mut SynchronizedParameter<V>) -> bool
    where
        V::Owned: PartialEq + Eq,
    {
        let flag_var = self.ld_client.variation(
            &self.ld_user,
            param.name,
            param.default_value.borrow().format(),
        );

        let flag_str = match flag_var {
            ld::FlagValue::Bool(v) => v.to_string(),
            ld::FlagValue::Str(v) => v,
            ld::FlagValue::Number(v) => v.to_string(),
            ld::FlagValue::Json(v) => v.to_string(),
        };

        param.modify(&flag_str);

        param.modified_flag
    }
}
