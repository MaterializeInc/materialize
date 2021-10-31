// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::result::Result as StdResult;
use std::time::Duration;

use serde::{Deserialize, Deserializer};

use crate::Error;

static DEFAULT_CONFIG: &str = include_str!("chbench-config.toml");

/// Verifies CH-benCHmark correctness.
#[derive(Debug, clap::Parser)]
pub struct Args {
    /// URL of the materialized instance to collect metrics from.
    #[clap(
        long,
        default_value = "postgres://materialize@materialized:6875/materialize"
    )]
    pub materialized_url: String,
    /// If set, initialize sources.
    #[clap(long = "mz-sources")]
    pub initialize_sources: bool,
    /// Config file to use.
    ///
    /// If unspecified, uses the default, built-in checks.
    #[clap(short = 'c', long, value_name = "FILE")]
    pub config_file: Option<String>,
    /// Limit to these correctness checks from config file.
    #[clap(short = 'i', long, value_name = "CHECKS")]
    pub checks: Option<String>,
    /// Print the names of the available checks in the config file.
    #[clap(long)]
    pub help_config: bool,
    /// Duration for which to run checker.
    #[clap(long, parse(try_from_str = repr::util::parse_duration), default_value = "10m")]
    pub duration: Duration,
}

pub fn load_config(config_path: Option<&str>, cli_checks: Option<&str>) -> Result<Config, Error> {
    // load and parse the toml file
    let config_file = config_path
        .map(std::fs::read_to_string)
        .unwrap_or_else(|| Ok(DEFAULT_CONFIG.to_string()));
    let mut config = match &config_file {
        Ok(contents) => toml::from_str::<Config>(&contents).map_err(|e| {
            format!(
                "Unable to parse config file {}: {}",
                config_path.as_deref().unwrap_or("DEFAULT"),
                e
            )
        })?,
        Err(e) => {
            eprintln!(
                "unable to read config file {:?}: {}",
                config_path.as_deref().unwrap_or("DEFAULT"),
                e
            );
            std::process::exit(1);
        }
    };

    // filter to only things enabled in the command line OR the config file
    if let Some(checks) = cli_checks {
        let enabled_cs: Vec<_> = checks.split(',').collect();
        if !enabled_cs.is_empty() {
            config
                .checks
                .retain(|c| enabled_cs.contains(&c.name.as_str()));
        }
    } else {
        config.checks.retain(|c| c.enabled);
    }

    println!("Number of checks is : {}", config.checks.len());
    // Sort checks by specificity (number of columns involved in the check)
    for c in &mut config.checks {
        c.rows.sort_by(|a, b| {
            let a_length = a.columns.len();
            let b_length = b.columns.len();
            a_length.cmp(&b_length)
        });
    }

    Ok(config)
}

pub fn print_config_supplied(config: Config) {
    println!("named checks:");
    for c in config.checks {
        println!(
            "    thread_count={} sleep={:?} query={}",
            c.thread_count.unwrap_or(config.default_check.thread_count),
            c.sleep_ms.unwrap_or(config.default_check.sleep_ms),
            c.name
        );
    }
}

/// Config File
#[derive(Debug, Deserialize)]
pub struct Config {
    /// Default to be filled in for other queries
    pub default_check: DefaultCheck,
    /// Checks are instantiated as views which are polled continuously
    pub checks: Vec<Check>,
    // Sources
    pub sources: Vec<Source>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct DefaultCheck {
    #[serde(deserialize_with = "deser_duration_ms")]
    sleep_ms: Duration,
    thread_count: u32,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Check {
    pub name: String,
    pub query: String,
    pub rows: Vec<Rows>,
    #[serde(default = "btrue")]
    pub enabled: bool,
    #[serde(default, deserialize_with = "deser_duration_ms_opt")]
    pub sleep_ms: Option<Duration>,
    #[serde(default)]
    pub thread_count: Option<u32>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Rows {
    pub columns: Vec<Columns>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Columns {
    pub column: String,
    pub value: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Source {
    pub schema_registry: String,
    pub kafka_broker: String,
    pub topic_namespace: String,
    pub names: Vec<String>,
    //TODO(ncrooks): update sources to take individual consistency topics
    pub consistency_topic: String,
    /// If not none, 'create source with (consistency= consistency_topic)'
    #[serde(default)]
    pub is_byo: bool,
    /// If true, `create MATERIALIZED source`
    #[serde(default)]
    pub materialized: bool,
}

/// helper for serde default
fn btrue() -> bool {
    true
}

fn deser_duration_ms<'de, D>(deser: D) -> StdResult<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let d = Duration::from_millis(Deserialize::deserialize(deser)?);
    Ok(d)
}

fn deser_duration_ms_opt<'de, D>(deser: D) -> StdResult<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    let d = Duration::from_millis(Deserialize::deserialize(deser)?);
    Ok(Some(d))
}
