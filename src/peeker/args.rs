// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! [`Args::from_cli`] parses the command line arguments from the cli and the config file

use serde::Deserialize;

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
        opts.reqopt("c", "config-file", "The config file to use", "FNAME");
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
            .expect("config-file is a reqopt");
        let mut conf;
        match std::fs::read_to_string(&config_file) {
            Ok(contents) => {
                conf = toml::from_str::<Config>(&contents)?;
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

        Ok(Args {
            materialized_url: popts.opt_get_default(
                "materialized-url",
                "postgres://ignoreuser@materialized:6875/materialize".to_owned(),
            )?,
            config: conf,
        })
    }
}

/// The config file
#[derive(Debug, Deserialize)]
pub struct Config {
    /// Queries are instanciated as views which are polled continuously
    pub queries: Vec<Query>,
    /// Sources are created once at startup
    pub sources: Vec<Source>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Query {
    pub name: String,
    pub query: String,
    #[serde(default = "btrue")]
    pub enabled: bool,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Source {
    pub schema_registry: String,
    pub kafka_broker: String,
    pub topic_namespace: String,
    pub names: Vec<String>,
}

/// helper for serde default
fn btrue() -> bool {
    true
}
