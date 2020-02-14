// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! [`Args::from_cli`] parses the command line arguments from the cli and the config file

use std::time::Duration;

use serde::{Deserialize, Deserializer};

static DEFAULT_CONFIG: &str = include_str!("config.toml");

#[derive(Debug)]
pub struct Args {
    pub materialized_url: String,
    pub config: Config,
}

impl Args {
    /// Load the arguments provided on the cli, and parse the required config file
    pub fn from_cli() -> Result<Args, Box<dyn std::error::Error>> {
        let args: Vec<_> = std::env::args().collect();

        let mut opts = getopts::Options::new();
        opts.optflag("h", "help", "show this usage information");
        opts.optopt(
            "c",
            "config-file",
            "The config file to use. Unspecified will use the default config with many tpcch queries",
            "FNAME",
        );
        opts.optflag(
            "",
            "help-config",
            "print the names of the available queries in the config file",
        );
        opts.optopt(
            "q",
            "queries",
            "limit to these query names from config file",
            "QUERIES",
        );
        opts.optopt(
            "",
            "materialized-url",
            "url of the materialized instance to collect metrics from",
            "URL",
        );
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

        let config_file = popts
            .opt_str("config-file")
            .map(|config_file| std::fs::read_to_string(config_file))
            .unwrap_or_else(|| Ok(DEFAULT_CONFIG.to_string()));
        let mut conf;
        match &config_file {
            Ok(contents) => {
                conf = toml::from_str::<RawConfig>(&contents)?;
                if let Some(queries) = popts.opt_str("queries") {
                    let qs: Vec<_> = queries.split(',').collect();
                    if !qs.is_empty() {
                        conf.queries = conf
                            .queries
                            .into_iter()
                            .filter(|q| qs.contains(&&*q.name))
                            .collect()
                    }
                }
                if conf.queries.is_empty() {
                    eprintln!("at least one query in {:?} is required", config_file);
                    std::process::exit(1);
                }
            }
            Err(e) => {
                eprintln!("unable to read config file {:?}: {}", config_file, e);
                std::process::exit(1);
            }
        }

        let config = Config::from(conf);
        if popts.opt_present("help-config") {
            println!("named queries:");
            for q in config.queries {
                println!(
                    "    {} thread_count={} sleep={:?}",
                    q.name, q.thread_count, q.sleep
                );
            }
            std::process::exit(0);
        }

        Ok(Args {
            materialized_url: popts.opt_get_default(
                "materialized-url",
                "postgres://ignoreuser@materialized:6875/materialize".to_owned(),
            )?,
            config,
        })
    }
}

/// A query configuration
///
/// This is a normalized version of [`RawConfig`], which is what is actually parsed
/// from the toml config file.
#[derive(Debug)]
pub struct Config {
    /// Queries are instanciated as views which are polled continuously
    pub queries: Vec<Query>,
    /// Sources are created once at startup
    pub sources: Vec<Source>,
}

impl From<RawConfig> for Config {
    fn from(conf: RawConfig) -> Config {
        let default = conf.default_query;
        Config {
            queries: conf
                .queries
                .into_iter()
                .map(|q| Query {
                    name: q.name,
                    query: q.query,
                    enabled: q.enabled,
                    sleep: q.sleep_ms.unwrap_or(default.sleep_ms),
                    thread_count: q.thread_count.unwrap_or(default.thread_count),
                })
                .collect(),
            sources: conf.sources,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct Source {
    pub schema_registry: String,
    pub kafka_broker: String,
    pub topic_namespace: String,
    pub names: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct Query {
    pub name: String,
    pub query: String,
    pub enabled: bool,
    pub sleep: Duration,
    pub thread_count: u32,
}

// inner parsing helpers

/// The raw config file, it is parsed and then defaults are supplied, resulting in [`Config`]
#[derive(Debug, Deserialize)]
struct RawConfig {
    /// Default to be filled in for other queries
    default_query: DefaultQuery,
    /// Queries are instanciated as views which are polled continuously
    queries: Vec<RawQuery>,
    /// Sources are created once at startup
    sources: Vec<Source>,
}

#[derive(Clone, Debug, Deserialize)]
struct DefaultQuery {
    #[serde(deserialize_with = "deser_duration_ms")]
    sleep_ms: Duration,
    thread_count: u32,
}

#[derive(Clone, Debug, Deserialize)]
struct RawQuery {
    name: String,
    query: String,
    #[serde(default = "btrue")]
    enabled: bool,
    #[serde(default, deserialize_with = "deser_duration_ms_opt")]
    sleep_ms: Option<Duration>,
    #[serde(default)]
    thread_count: Option<u32>,
}

/// helper for serde default
fn btrue() -> bool {
    true
}

fn deser_duration_ms<'de, D>(deser: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let d = Duration::from_millis(Deserialize::deserialize(deser)?);
    Ok(d)
}

fn deser_duration_ms_opt<'de, D>(deser: D) -> Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    let d = Duration::from_millis(Deserialize::deserialize(deser)?);
    Ok(Some(d))
}
