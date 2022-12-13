// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{collections::BTreeMap, time::Duration};

use derivative::Derivative;
use launchdarkly_server_sdk as ld;
use tokio::time;

use super::SynchronizedParameters;

/// A frontend client for pulling [SynchronizedParameters] from LaunchDarkly.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct SystemParameterFrontend {
    /// An SDK client to mediate interactions with the LaunchDarkly client.
    #[derivative(Debug = "ignore")]
    ld_client: ld::Client,
    /// The user to use when quering LaunchDarkly using the SDK.
    ld_user: ld::User,
    /// A map from parameter names to LaunchDarkly feature keys
    /// to use when populating the the [SynchronizedParameters]
    /// instance in [SystemParameterFrontend::pull].
    ld_key_map: BTreeMap<String, String>,
}

impl SystemParameterFrontend {
    /// Construct a new [SystemParameterFrontend] instance.
    pub fn new(
        ld_sdk_key: &str,
        ld_user_key: &str,
        ld_key_map: BTreeMap<String, String>,
    ) -> Result<Self, anyhow::Error> {
        let ld_config = ld::ConfigBuilder::new(ld_sdk_key).build();
        let ld_client = ld::Client::build(ld_config)?;
        let ld_user = ld::User::with_key(ld_user_key).build();

        Ok(Self {
            ld_client,
            ld_user,
            ld_key_map,
        })
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
            tracing::warn!("SystemParameterFrontend failed to initialize");
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

        for param_name in params.synchronized().into_iter() {
            let flag_name = self
                .ld_key_map
                .get(param_name)
                .map(|flag_name| flag_name.as_str())
                .unwrap_or(param_name);

            let flag_var =
                self.ld_client
                    .variation(&self.ld_user, flag_name, params.get(param_name));

            let flag_str = match flag_var {
                ld::FlagValue::Bool(v) => v.to_string(),
                ld::FlagValue::Str(v) => v,
                ld::FlagValue::Number(v) => v.to_string(),
                ld::FlagValue::Json(v) => v.to_string(),
            };

            changed |= params.modify(param_name, flag_str.as_str());
        }

        changed
    }
}
