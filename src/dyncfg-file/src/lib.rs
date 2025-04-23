// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A dyncfg::ConfigSet backed by a local JSON file.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Context;
use mz_dyncfg::{ConfigSet, ConfigUpdates, ConfigVal};
use mz_ore::task;
use serde_json::Value as JsonValue;
use tokio::time;

/// Start a background task that syncs a ConfigSet with a local JSON file.
/// Returns `Ok` after the initial sync is completed. If the initialization takes longer
/// than `config_sync_timeout`, an error is returned.
///
/// The file format is a simple JSON object mapping config names to their values.
pub async fn sync_file_to_configset(
    set: ConfigSet,
    config_file: impl AsRef<Path>,
    config_sync_timeout: Duration,
    config_sync_loop_interval: Option<Duration>,
    on_update: impl Fn(&ConfigUpdates, &ConfigSet) + Send + 'static,
) -> Result<(), anyhow::Error> {
    let config_file = config_file.as_ref().to_owned();

    // Create initial file if it doesn't exist
    if !config_file.exists() {
        tracing::warn!("sync config file {:?} does not exist", config_file);
        return Ok(());
    }

    let synced = SyncedConfigSet {
        set,
        config_file,
        on_update,
    };

    // Do initial sync
    if let Err(err) = tokio::time::timeout(config_sync_timeout, async {
        synced.sync()?;
        Ok::<_, anyhow::Error>(())
    })
    .await
    {
        tracing::warn!("error while initializing file-backed config set: {}", err);
        return Err(err.into());
    }

    // Start background sync task if interval is specified
    task::spawn(
        || "SyncedConfigSet sync_loop",
        synced.sync_loop(config_sync_loop_interval),
    );

    Ok(())
}

struct SyncedConfigSet<F>
where
    F: Fn(&ConfigUpdates, &ConfigSet) + Send,
{
    set: ConfigSet,
    config_file: PathBuf,
    on_update: F,
}

impl<F: Fn(&ConfigUpdates, &ConfigSet) + Send> SyncedConfigSet<F> {
    /// Returns a future that periodically reads the config file and updates the ConfigSet.
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
                tracing::warn!("SyncedConfigSet sync error: {}", err);
            }
        }
    }

    /// Reads current values from the config file and updates the ConfigSet.
    pub fn sync(&self) -> Result<(), anyhow::Error> {
        let file_contents = fs::read_to_string(&self.config_file)
            .with_context(|| format!("failed to read config file: {:?}", self.config_file))?;

        let values: BTreeMap<String, JsonValue> = serde_json::from_str(&file_contents)
            .with_context(|| format!("failed to parse config file: {:?}", self.config_file))?;

        let mut updates = ConfigUpdates::default();
        for entry in self.set.entries() {
            if let Some(val) = values.get(entry.name()) {
                match json_to_config_val(val, &entry.val()) {
                    Ok(new_val) => {
                        // Only update if the value is different
                        if new_val != entry.val() {
                            tracing::debug!(
                                "updating config value {} from {:?} to {:?}",
                                &entry.name(),
                                &entry.val(),
                                new_val
                            );
                            updates.add_dynamic(entry.name(), new_val);
                        }
                    }
                    Err(err) => {
                        tracing::warn!(
                            "failed to convert JSON value for {}: {}",
                            entry.name(),
                            err
                        );
                    }
                }
            }
        }
        updates.apply(&self.set);
        (self.on_update)(&updates, &self.set);
        Ok(())
    }
}

/// Convert a JSON value to a ConfigVal, using the existing value as a template for the type
fn json_to_config_val(json: &JsonValue, template: &ConfigVal) -> Result<ConfigVal, anyhow::Error> {
    match (template, json) {
        (ConfigVal::Bool(_), JsonValue::Bool(v)) => Ok(ConfigVal::Bool(*v)),
        (ConfigVal::U32(_), JsonValue::Number(v)) => Ok(ConfigVal::U32(
            v.as_u64()
                .and_then(|v| v.try_into().ok())
                .ok_or_else(|| anyhow::anyhow!("not a u32"))?,
        )),
        (ConfigVal::Usize(_), JsonValue::Number(v)) => Ok(ConfigVal::Usize(
            v.as_u64()
                .and_then(|v| v.try_into().ok())
                .ok_or_else(|| anyhow::anyhow!("not a usize"))?,
        )),
        (ConfigVal::OptUsize(_), JsonValue::Null) => Ok(ConfigVal::OptUsize(None)),
        (ConfigVal::OptUsize(_), JsonValue::Number(v)) => Ok(ConfigVal::OptUsize(Some(
            v.as_u64()
                .and_then(|v| v.try_into().ok())
                .ok_or_else(|| anyhow::anyhow!("not a usize"))?,
        ))),
        (ConfigVal::F64(_), JsonValue::Number(v)) => Ok(ConfigVal::F64(
            v.as_f64().ok_or_else(|| anyhow::anyhow!("not an f64"))?,
        )),
        (ConfigVal::String(_), JsonValue::String(v)) => Ok(ConfigVal::String(v.clone())),
        (ConfigVal::Duration(_), JsonValue::String(v)) => {
            Ok(ConfigVal::Duration(humantime::parse_duration(v)?))
        }
        (ConfigVal::Json(_), v) => Ok(ConfigVal::Json(v.clone())),
        _ => Err(anyhow::anyhow!("type mismatch")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_dyncfg::Config;
    use std::io::Write;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    #[mz_ore::test(tokio::test)]
    async fn test_file_sync() {
        let mut config_file = tempfile::NamedTempFile::new().unwrap();
        const BOOL_CONFIG: Config<bool> = Config::new("test_bool", true, "A test boolean config");
        const STRING_CONFIG: Config<&str> =
            Config::new("test_string", "default", "A test string config");
        let set = ConfigSet::default().add(&BOOL_CONFIG).add(&STRING_CONFIG);

        // Start sync with empty file (should create it)
        sync_file_to_configset(
            set.clone(),
            &config_file.path(),
            Duration::from_secs(1),
            None,
            |_, _| {},
        )
        .await
        .unwrap();
        assert_eq!(BOOL_CONFIG.get(&set), true);
        assert_eq!(STRING_CONFIG.get(&set), "default");

        // update the data
        config_file
            .write_all(
                String::from("{\"test_bool\": false, \"test_string\": \"modified\"}").as_bytes(),
            )
            .unwrap();

        // Start new sync to read modified values
        let updates_received = Arc::new(AtomicBool::new(false));
        let updates_received_clone = Arc::clone(&updates_received);
        sync_file_to_configset(
            set.clone(),
            &config_file,
            Duration::from_secs(1),
            None,
            move |updates, _| {
                assert_eq!(updates.updates.len(), 2);
                updates_received_clone.store(true, std::sync::atomic::Ordering::SeqCst);
            },
        )
        .await
        .unwrap();

        assert!(updates_received.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(BOOL_CONFIG.get(&set), false);
        assert_eq!(STRING_CONFIG.get(&set), "modified");
    }
}
