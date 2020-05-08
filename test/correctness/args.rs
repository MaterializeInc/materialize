// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! [`Args::from_cli`] parses the command line arguments from the cli and the config file

use serde::{Deserialize, Deserializer};
use std::result::Result as StdResult;
use std::time::Duration;

use crate::Error;

static DEFAULT_CONFIG: &str = include_str!("chbench-config.toml");

#[derive(Debug)]
pub struct Args {
    pub mz_url: String,
    /// If true, don't run correctness peeks, just create sources and views
    pub only_initialize: bool,
    pub config: Config,
    /// Duration for which to run checker
    pub duration: Duration,
}

impl Args {
    /// Load the arguments provided on the cli, and parse the required config file
    pub fn from_cli() -> Result<Args, Error> {
        let args: Vec<_> = std::env::args().collect();

        let mut opts = getopts::Options::new();
        opts.optflag("h", "help", "show this usage information");
        opts.optopt(
            "c",
            "config-file",
            "The config file to use. Unspecified will use the default config with all correctness checks",
            "FNAME",
        );
        opts.optflag(
            "",
            "help-config",
            "print the names of the available correctness checks in the config file",
        );
        opts.optopt(
            "i",
            "checks",
            "limit to these correctness checks from config file",
            "CHECKS",
        );
        opts.optopt(
            "",
            "materialized-url",
            "url of the materialized instance to collect metrics from",
            "URL",
        );
        opts.optflag(
            "",
            "only-initialize",
            "run the initalization of sources and views, but don't start peeking",
        );
        opts.optflag("", "duration", "duration for which to run correctness test");
        let popts = match opts.parse(&args[1..]) {
            Ok(popts) => {
                if popts.opt_present("h") {
                    print!("{}", opts.usage("usage: materialized [options]"));
                    std::process::exit(0);
                } else {
                    popts
                }
            }
            Err(e) => {
                println!("error parsing arguments: {}", e);
                std::process::exit(0);
            }
        };

        let config = load_config(popts.opt_str("config-file"), popts.opt_str("checks"))?;

        if popts.opt_present("help-config") {
            print_config_supplied(config);
            std::process::exit(0);
        }

        let duration = popts.opt_get_default("duration", "600".to_owned())?;
        let duration_t = duration.parse::<u64>()?;

        Ok(Args {
            mz_url: popts.opt_get_default(
                "materialized-url",
                "postgres://ignoreuser@materialized:6875/materialize".to_owned(),
            )?,
            only_initialize: popts.opt_present("only-initialize"),
            duration: Duration::from_secs(duration_t),
            config,
        })
    }
}

fn load_config(config_path: Option<String>, cli_checks: Option<String>) -> Result<Config, Error> {
    // load and parse the toml file
    let config_file = config_path
        .as_ref()
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

fn print_config_supplied(config: Config) {
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
