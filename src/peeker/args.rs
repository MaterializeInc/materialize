// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! [`Args::from_cli`] parses the command line arguments from the cli and the config file

use std::collections::HashMap;
use std::convert::TryFrom;
use std::result::Result as StdResult;
use std::time::Duration;

use serde::{Deserialize, Deserializer};

use crate::{Error, Result};

static DEFAULT_CONFIG: &str = include_str!("config.toml");

#[derive(Debug)]
pub struct Args {
    pub materialized_url: String,
    pub config: Config,
}

impl Args {
    /// Load the arguments provided on the cli, and parse the required config file
    pub fn from_cli() -> Result<Args> {
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

        let config = load_config(popts.opt_str("config-file"), popts.opt_str("queries"))?;

        if popts.opt_present("help-config") {
            print_config_supplied(config);
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

fn load_config(config_path: Option<String>, cli_queries: Option<String>) -> Result<Config> {
    // load and parse th toml
    let config_file = config_path
        .as_ref()
        .map(|config_file| std::fs::read_to_string(config_file))
        .unwrap_or_else(|| Ok(DEFAULT_CONFIG.to_string()));
    let conf = match &config_file {
        Ok(contents) => toml::from_str::<RawConfig>(&contents).map_err(|e| {
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

    // Get everything into the normalized QueryGroup representation
    let mut config = Config::try_from(conf)?;

    // filter to only things enabled in the command line OR the config file
    //
    // TODO: consider if this would be better to just flip the enabled flag to true/false and retail everthing
    if let Some(queries) = cli_queries {
        let enabled_qs: Vec<_> = queries.split(',').collect();
        if !enabled_qs.is_empty() {
            config.groups = config
                .groups
                .into_iter()
                .filter(|qg| enabled_qs.contains(&&*qg.name))
                .collect();
        }
    } else {
        config.groups.retain(|q| q.enabled);
    }
    if config.groups.is_empty() {
        eprintln!(
            "at least one enabled query or group in {:?} is required",
            config_path.as_deref().unwrap_or("the default config")
        );
        std::process::exit(1);
    }
    Ok(config)
}

fn print_config_supplied(config: Config) {
    println!("named queries:");
    let mut groups = config.groups.iter().collect::<Vec<_>>();
    groups.sort_by_key(|g| g.queries.len());
    for g in groups {
        if g.queries.len() == 1 {
            println!(
                "    thread_count={} sleep={:?} query={}",
                g.thread_count, g.sleep, g.queries[0].name
            );
        } else {
            println!(
                "    group={} thread_count={} sleep={:?}",
                g.name, g.thread_count, g.sleep
            );
            for q in &g.queries {
                println!("        query={}", q.name);
            }
        }
    }
}

/// A query configuration
///
/// This is a normalized version of [`RawConfig`], which is what is actually parsed
/// from the toml config file.
#[derive(Debug)]
pub struct Config {
    /// Queries are instanciated as views which are polled continuously
    pub groups: Vec<QueryGroup>,
    queries: Vec<QueryGroup>,
    /// Sources are created once at startup
    pub sources: Vec<Source>,
}

impl Config {
    /// How many total queries there are in this config
    pub fn query_count(&self) -> usize {
        self.groups.iter().map(|g| g.queries.iter().count()).sum()
    }

    pub fn queries_in_declaration_order(&self) -> impl Iterator<Item = &QueryGroup> {
        self.queries.iter()
    }
}

impl TryFrom<RawConfig> for Config {
    type Error = Error;

    /// Convert the toml into the nicer representation
    ///
    /// This performs the grouping and normalization from [`Query`]s and [`GroupConfig`]s
    /// into [`QueryGroup`]s
    fn try_from(conf: RawConfig) -> Result<Config> {
        let default = conf.default_query;
        let mut queries_by_name = HashMap::new();
        let queries: Vec<_> = conf
            .queries
            .into_iter()
            .map(|rq| {
                let q = Query {
                    name: rq.name.clone(),
                    query: rq.query.clone(),
                };
                queries_by_name.insert(q.name.clone(), q);
                QueryGroup::from_raw_query(rq, &default)
            })
            .collect();

        let groups = conf
            .groups
            .into_iter()
            .map(|g| QueryGroup::from_group_config(g, &queries_by_name))
            .chain(queries.iter().cloned().map(|q| Ok(q)))
            .collect::<Result<Vec<_>>>()?;

        Ok(Config {
            groups,
            queries,
            sources: conf.sources,
        })
    }
}

#[derive(Clone, Debug)]
pub struct QueryGroup {
    /// The name of the group
    ///
    /// possibly the name of the query if it was default created
    pub name: String,
    pub thread_count: u32,
    pub sleep: Duration,
    pub queries: Vec<Query>,
    pub enabled: bool,
}

impl QueryGroup {
    /// Create a default QueryGroup using fields from the query
    fn from_raw_query(q: RawQuery, default: &DefaultQuery) -> QueryGroup {
        QueryGroup {
            name: q.name.clone(),
            sleep: q.sleep_ms.unwrap_or(default.sleep_ms),
            thread_count: q.thread_count.unwrap_or(default.thread_count),
            queries: vec![Query {
                name: q.name,
                query: q.query,
            }],
            enabled: q.enabled,
        }
    }

    fn from_group_config(g: GroupConfig, queries: &HashMap<String, Query>) -> Result<QueryGroup> {
        let g_name = g.name.clone();
        Ok(QueryGroup {
            name: g_name.clone(),
            sleep: g.sleep_ms,
            thread_count: g.thread_count,
            queries: g
                .queries
                .into_iter()
                .map(|q_name| {
                    let q = queries.get(&*q_name).cloned();
                    q.ok_or_else(|| {
                        format!("Unable to get query for group {}: {}", g_name, q_name).into()
                    })
                })
                .collect::<Result<_>>()?,
            enabled: g.enabled,
        })
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
}

// inner parsing helpers

/// The raw config file, it is parsed and then defaults are supplied, resulting in [`Config`]
#[derive(Debug, Deserialize)]
struct RawConfig {
    /// Default to be filled in for other queries
    default_query: DefaultQuery,
    /// Queries are instanciated as views which are polled continuously
    queries: Vec<RawQuery>,
    /// Defined query groups
    groups: Vec<GroupConfig>,
    /// Sources are created once at startup
    sources: Vec<Source>,
}

#[derive(Clone, Debug, Deserialize)]
struct DefaultQuery {
    #[serde(deserialize_with = "deser_duration_ms")]
    sleep_ms: Duration,
    thread_count: u32,
    /// Groups share their connection and only one query happens at a time
    #[serde(default)]
    group: Option<String>,
}

/// An explicitly created, named group
#[derive(Clone, Debug, Deserialize)]
struct GroupConfig {
    name: String,
    /// The names of the queries that belong in this group, must be specified separately
    /// in the config file
    queries: Vec<String>,
    #[serde(default = "one")]
    thread_count: u32,
    #[serde(default, deserialize_with = "deser_duration_ms")]
    sleep_ms: Duration,
    /// Whether to enabled this group. Overrides enabled in queries
    #[serde(default = "btrue")]
    enabled: bool,
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

fn one() -> u32 {
    1
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
