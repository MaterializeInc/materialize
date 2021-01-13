// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! CLI argument and config file parsing.

use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::env;
use std::result::Result as StdResult;
use std::time::Duration;

use lazy_static::lazy_static;
use log::{debug, info};
use regex::Regex;
use serde::{Deserialize, Deserializer};
use structopt::StructOpt;

use crate::{Error, Result};

static DEFAULT_CONFIG: &str = include_str!("../config.toml");

/// Measures Materialize's query latency.
#[derive(Debug, StructOpt)]
pub struct Args {
    /// Config file to use.
    ///
    /// If unspecified, uses the default, built-in config that includes many
    /// CH-benCHmark queries.
    #[structopt(short = "c", long, value_name = "FILE")]
    pub config_file: Option<String>,
    /// Limit to these query names from config file.
    #[structopt(short = "q", long, value_name = "QUERIES")]
    pub queries: Option<String>,
    /// URL of materialized instance to collect metrics from.
    #[structopt(
        long,
        default_value = "postgres://ignoreuser@materialized:6875/materialize",
        value_name = "URL"
    )]
    pub materialized_url: String,
    /// Run the initalization of sources and views, but don't start peeking.
    #[structopt(long)]
    pub only_initialize: bool,
    /// How long to spend trying to initialize.
    #[structopt(long, parse(try_from_str = parse_duration::parse), default_value = "60s")]
    pub init_timeout: Duration,
    /// Print the names of the available queries in the config file.
    #[structopt(long)]
    pub help_config: bool,
    /// How long to wait before connecting to materialized.
    #[structopt(long, default_value = "0")]
    pub warmup_seconds: u32,
    /// How long to run before shutting down.
    ///
    /// A value of 0 never shuts down.
    #[structopt(long, default_value = "0")]
    pub run_seconds: u32,
}

pub fn load_config(config_path: Option<&str>, cli_queries: Option<&str>) -> Result<Config> {
    // load and parse th toml
    let config_file = config_path
        .map(std::fs::read_to_string)
        .unwrap_or_else(|| Ok(DEFAULT_CONFIG.to_string()));
    let conf = match &config_file {
        Ok(contents) => {
            let contents = substitute_config_env_vars(contents);
            toml::from_str::<RawConfig>(&contents).map_err(|e| {
                format!(
                    "Unable to parse config file {}: {}",
                    config_path.as_deref().unwrap_or("DEFAULT"),
                    e
                )
            })?
        }
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
            debug!("filtering to queries: {:?}", enabled_qs);
            config.groups = config
                .groups
                .into_iter()
                .filter(|qg| enabled_qs.contains(&&*qg.name))
                .collect();
        }
    } else {
        debug!("using all config-enabled queries");
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

pub fn print_config_supplied(config: Config) {
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
    ///
    /// This contains the list of only *enabled* query groups
    pub groups: Vec<QueryGroup>,
    /// The raw list of all queries and groups, in declaration order
    queries: Vec<QueryGroup>,
    /// Sources are created once at startup
    pub sources: Vec<Source>,
}

impl Config {
    /// How many total queries there are in this config
    pub fn query_count(&self) -> usize {
        self.groups.iter().map(|g| g.queries.iter().count()).sum()
    }

    /// A list of queries that may need to be initialized
    pub fn queries_in_declaration_order(&self) -> Vec<&QueryGroup> {
        let enabled_queries: HashSet<_> = self
            .groups
            .iter()
            .flat_map(|qg| qg.queries.iter().map(|q| &q.name))
            .collect();
        self.queries
            .iter()
            .filter(|qg| enabled_queries.contains(&qg.name))
            .collect::<Vec<_>>()
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
            .chain(queries.iter().cloned().map(Ok))
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
    pub queries: Vec<Query>,
    // configuration parameters
    pub thread_count: u32,
    pub sleep: Duration,
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

/// The non-configuration parts of a [`QueryGroup`]
#[derive(Clone, Debug)]
pub struct Query {
    pub name: String,
    pub query: String,
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

lazy_static! {
    static ref BASHLIKE_ENV_VAR_PATTERN: Regex =
        Regex::new(r"(?P<declaration>\$\{(?P<var>[^:}]+)(:-(?P<default>[^}]+))?\})").unwrap();
}

/// replace instances of `${VAR:-default}` with a local environment variable if
/// present or the default value.
///
/// # Panics
/// - If environment variable does not exist and no default value is provided.
fn substitute_config_env_vars(contents: &str) -> String {
    let mut parsed_contents = contents.to_string();
    for cap in BASHLIKE_ENV_VAR_PATTERN.captures_iter(contents) {
        let var = cap.name("var").unwrap().as_str();
        let (val, is_env) = match env::var(var) {
            Ok(val) => (val, true),
            Err(_) => (
                cap.name("default")
                    .unwrap_or_else(|| panic!("Env Var is not set and has no default: {}", var))
                    .as_str()
                    .to_string(),
                false,
            ),
        };
        info!(
            "substituting config var {} w/ {} {}",
            var,
            if is_env { "env var" } else { "default val" },
            val
        );
        parsed_contents = parsed_contents.replace(cap.name("declaration").unwrap().as_str(), &val);
    }
    parsed_contents
}

#[test]
fn test_substitute_config_env_vars() {
    fn remove_test_vars() {
        env::remove_var("MZ_TEST_ENV_VAR_NAME");
        env::remove_var("MZ_TEST_ENV_VAR_DAY");
    }

    remove_test_vars();
    let contents =
        "Hello, ${MZ_TEST_ENV_VAR_NAME:-Sean}; how is your ${MZ_TEST_ENV_VAR_DAY:-Friday}?";
    let parsed_contents = substitute_config_env_vars(contents);
    assert_eq!(parsed_contents, "Hello, Sean; how is your Friday?");

    env::set_var("MZ_TEST_ENV_VAR_NAME", "friend");
    env::set_var("MZ_TEST_ENV_VAR_DAY", "weekend");
    let parsed_contents = substitute_config_env_vars(contents);
    let valid_env_var_sub = "Hello, friend; how is your weekend?";
    if parsed_contents != valid_env_var_sub {
        remove_test_vars();
        assert_eq!(parsed_contents, valid_env_var_sub);
    }
    remove_test_vars();
}
