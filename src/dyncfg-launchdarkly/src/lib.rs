// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A dyncfg::ConfigSet backed by LaunchDarkly.

use std::time::Duration;

use launchdarkly_server_sdk as ld;
use mz_build_info::BuildInfo;
use mz_dyncfg::{ConfigSet, ConfigUpdates, ConfigVal};
use mz_ore::cast::CastLossy;
use mz_ore::task;
use tokio::time;

/// Start a background task that syncs to a ConfigSet from LaunchDarkly. `ctx_builder` can be used
/// to add additional LD contexts. A `build` context is added automatically. Returns `Ok` after the
/// LD client has been initialized and an initial sync completed. If the initialization takes longer
/// than `config_sync_timeout`, an error is returned.
///
/// A successful initialization can take up to `config_sync_timeout`, preventing the calling service
/// from starting for possibly up to that duration. Its value should be chosen based on the needs of
/// the service in the case that LaunchDarkly is down.
///
/// If the caller chooses to continue if this function returns an error, the ConfigSet will retain
/// its default values. Those should be chosen with this risk in mind.
pub async fn sync_launchdarkly_to_configset<F>(
    set: ConfigSet,
    build_info: &'static BuildInfo,
    ctx_builder: F,
    // Use an option so that local dev where this is disabled still uses the same validation logic
    // for the ConfigSet.
    launchdarkly_sdk_key: Option<&str>,
    config_sync_timeout: Duration,
    config_sync_loop_interval: Option<Duration>,
    on_update: impl Fn(&ConfigUpdates, &ConfigSet) + Send + 'static,
) -> Result<(), anyhow::Error>
where
    F: FnOnce(&mut ld::MultiContextBuilder) -> Result<(), anyhow::Error>,
{
    // Ensure that all the ConfigVals in the set support FlagValue conversion, even if LD is
    // disabled (preventing error skew in local vs prod settings).
    for entry in set.entries() {
        let _ = dyn_into_flag(entry.val())?;
    }
    let ld_client = if let Some(key) = launchdarkly_sdk_key {
        let client = ld::Client::build(ld::ConfigBuilder::new(key).build())?;
        client.start_with_default_executor();
        let init = async {
            let max_backoff = Duration::from_secs(60);
            let mut backoff = Duration::from_secs(5);
            while !client.initialized_async().await {
                tracing::warn!("SyncedConfigSet failed to initialize");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(max_backoff);
            }
        };
        if tokio::time::timeout(config_sync_timeout, init)
            .await
            .is_err()
        {
            tracing::info!("SyncedConfigSet initialize on boot: initialize has timed out");
        }
        Some(client)
    } else {
        None
    };

    let synced = SyncedConfigSet {
        set,
        ld_client,
        ld_ctx: ld_ctx(build_info, ctx_builder)?,
        on_update,
    };
    synced.sync()?;
    task::spawn(
        || "SyncedConfigSet sync_loop",
        synced.sync_loop(config_sync_loop_interval),
    );
    Ok(())
}

fn ld_ctx<F>(build_info: &'static BuildInfo, ctx_builder: F) -> Result<ld::Context, anyhow::Error>
where
    F: FnOnce(&mut ld::MultiContextBuilder) -> Result<(), anyhow::Error>,
{
    // Register multiple contexts for this client.
    //
    // Unfortunately, it seems that the order in which conflicting targeting
    // rules are applied depends on the definition order of feature flag
    // variations rather than on the order in which context are registered with
    // the multi-context builder.
    let mut builder = ld::MultiContextBuilder::new();

    builder.add_context(
        ld::ContextBuilder::new(build_info.sha)
            .kind("build")
            .set_string("semver_version", build_info.semver_version().to_string())
            .build()
            .map_err(|e| anyhow::anyhow!(e))?,
    );

    ctx_builder(&mut builder)?;

    builder.build().map_err(|e| anyhow::anyhow!(e))
}

struct SyncedConfigSet<F>
where
    F: Fn(&ConfigUpdates, &ConfigSet) + Send,
{
    set: ConfigSet,
    ld_client: Option<ld::Client>,
    ld_ctx: ld::Context,
    on_update: F,
}

impl<F: Fn(&ConfigUpdates, &ConfigSet) + Send> SyncedConfigSet<F> {
    /// Returns a future that periodically polls LaunchDarkly and updates the ConfigSet.
    async fn sync_loop(self, tick_interval: Option<Duration>) {
        let Some(tick_interval) = tick_interval else {
            tracing::info!("skipping SyncedConfigSet sync as tick_interval = None");
            return;
        };

        let mut interval = time::interval(tick_interval);
        interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

        tracing::info!(
            "synchronizing SyncedConfigSet values every {} seconds",
            tick_interval.as_secs()
        );

        loop {
            interval.tick().await;

            if let Err(err) = self.sync() {
                tracing::info!("SyncedConfigSet: {err}");
            }
        }
    }

    /// Reads current values from LaunchDarkly and updates the ConfigSet.
    fn sync(&self) -> Result<(), anyhow::Error> {
        let mut updates = ConfigUpdates::default();
        let Some(ld_client) = &self.ld_client else {
            (self.on_update)(&updates, &self.set);
            return Ok(());
        };
        for entry in self.set.entries() {
            let val = dyn_into_flag(entry.val()).expect("new() verifies all configs can convert");
            let flag_var = ld_client.variation(&self.ld_ctx, entry.name(), val);
            let update = match (entry.val(), flag_var) {
                (ConfigVal::Bool(_), ld::FlagValue::Bool(flag)) => ConfigVal::Bool(flag),
                (ConfigVal::U32(_), ld::FlagValue::Number(flag)) => {
                    ConfigVal::U32(u32::cast_lossy(flag))
                }
                (ConfigVal::Usize(_), ld::FlagValue::Number(flag)) => {
                    ConfigVal::Usize(usize::cast_lossy(flag))
                }
                (ConfigVal::F64(_), ld::FlagValue::Number(flag)) => ConfigVal::F64(flag),
                (ConfigVal::String(_), ld::FlagValue::Str(flag)) => ConfigVal::String(flag),
                (ConfigVal::Duration(_), ld::FlagValue::Str(flag)) => {
                    ConfigVal::Duration(humantime::parse_duration(&flag)?)
                }
                (ConfigVal::Json(_), ld::FlagValue::Json(flag)) => ConfigVal::Json(flag),

                // Hardcode all others so that if ConfigVal gets new types this match block will
                // compile error.
                (ConfigVal::Bool(_), _)
                | (ConfigVal::U32(_), _)
                | (ConfigVal::Usize(_), _)
                | (ConfigVal::F64(_), _)
                | (ConfigVal::Duration(_), _)
                | (ConfigVal::Json(_), _)
                | (ConfigVal::OptUsize(_), _)
                | (ConfigVal::String(_), _) => anyhow::bail!(
                    "LD flag cannot be cast to the ConfigVal for {}",
                    entry.name()
                ),
            };
            updates.add_dynamic(entry.name(), update);
        }
        updates.apply(&self.set);
        (self.on_update)(&updates, &self.set);
        Ok(())
    }
}

/// Converts a dyncfg ConfigVal into a LaunchDarkly FlagValue. Returns an error if the ConfigVal
/// type isn't supported by the FlagValue format.
fn dyn_into_flag(val: ConfigVal) -> Result<ld::FlagValue, anyhow::Error> {
    // Note that errors must only (and always) occur when the ConfigVal type isn't fully supported.
    // That is, don't error only if the current value isn't supported (like None in an Opt type):
    // error always for an Opt value because it might be None.
    Ok(match val {
        ConfigVal::Bool(v) => ld::FlagValue::Bool(v),
        ConfigVal::U32(v) => ld::FlagValue::Number(v.into()),
        ConfigVal::Usize(v) => ld::FlagValue::Number(f64::cast_lossy(v)),
        ConfigVal::OptUsize(_) => anyhow::bail!("OptUsize None cannot be converted to a FlagValue"),
        ConfigVal::F64(v) => ld::FlagValue::Number(v),
        ConfigVal::String(v) => ld::FlagValue::Str(v),
        ConfigVal::Duration(v) => ld::FlagValue::Str(humantime::format_duration(v).to_string()),
        ConfigVal::Json(v) => ld::FlagValue::Json(v),
    })
}
