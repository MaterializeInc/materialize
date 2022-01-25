// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashSet};
use std::error::Error;
use std::path::{Path, PathBuf};
use std::process;
use std::time::Duration;

use aws_smithy_http::endpoint::Endpoint;
use aws_types::region::Region;
use aws_types::Credentials;
use globset::GlobBuilder;
use http::Uri;
use itertools::Itertools;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use url::Url;
use walkdir::WalkDir;

use mz_aws_util::config::AwsConfig;
use ore::path::PathExt;

use testdrive::Config;

macro_rules! die {
    ($($e:expr),*) => {{
        eprintln!($($e),*);
        process::exit(1);
    }}
}

/// Integration test driver for Materialize.
#[derive(clap::Parser)]
struct Args {
    // === Confluent options. ===
    /// Kafka bootstrap address.
    #[clap(
        long,
        value_name = "ENCRYPTION://HOST:PORT",
        default_value = "localhost:9092"
    )]
    kafka_addr: String,
    /// Kafka configuration option.
    #[clap(long, value_name = "KEY=VAL", parse(from_str = parse_kafka_opt))]
    kafka_option: Vec<(String, String)>,
    /// Schema registry URL.
    #[clap(long, value_name = "URL", default_value = "http://localhost:8081")]
    schema_registry_url: Url,

    // === TLS options. ===
    /// Path to TLS certificate keystore.
    ///
    /// The keystore must be in the PKCS#12 format.
    #[clap(long, value_name = "PATH")]
    cert: Option<String>,
    /// Password for the TLS certificate keystore.
    #[clap(long, value_name = "PASSWORD")]
    cert_password: Option<String>,

    // === AWS options. ===
    /// Named AWS region to target for AWS API requests.
    ///
    /// Cannot be specified is --aws-endpoint is specified.
    #[clap(long, conflicts_with = "aws-endpoint")]
    aws_region: Option<String>,
    /// Custom AWS endpoint.
    ///
    /// Defaults to http://localhost:4566 unless --aws-region is specified.
    /// Cannot be specified if --aws-region is specified.
    #[clap(long, conflicts_with = "aws-region")]
    aws_endpoint: Option<Uri>,

    // === Materialize options. ===
    /// materialized connection string.
    #[clap(long, default_value = "postgres://materialize@localhost:6875")]
    materialized_url: tokio_postgres::Config,
    /// Validate the on-disk state of the materialized catalog.
    #[clap(long)]
    validate_catalog: Option<PathBuf>,
    /// Don't reset materialized state before executing each script.
    #[clap(long)]
    no_reset: bool,

    // === Testdrive options. ===
    /// Default timeout in seconds.
    #[clap(long, parse(try_from_str = repr::util::parse_duration), default_value = "10s")]
    default_timeout: Duration,

    /// Initial backoff interval. Set to 0 to retry immediately on failure
    #[clap(long, parse(try_from_str = repr::util::parse_duration), default_value = "50ms")]
    initial_backoff: Duration,

    /// Backoff factor when retrying. Set to 1 to retry at a steady pace
    #[clap(long, default_value = "1.5")]
    backoff_factor: f64,

    /// A random number to distinguish each TestDrive run.
    #[clap(long)]
    seed: Option<u32>,

    /// Maximum number of errors before aborting
    #[clap(long, default_value = "10")]
    max_errors: usize,

    /// Max number of tests to run before terminating
    #[clap(long, default_value = "18446744073709551615")]
    max_tests: usize,

    /// Shuffle tests (using the value from --seed, if any)
    #[clap(long)]
    shuffle_tests: bool,

    /// Force the use of the specfied temporary directory rather than creating one with a random name
    #[clap(long)]
    temp_dir: Option<String>,

    /// CLI arguments that are converted to testdrive variables.
    ///
    /// Passing: `--var foo=bar` will create a variable named 'arg.foo' with the value 'bar'.
    /// Can be specified multiple times to set multiple variables.
    #[clap(long, value_name = "NAME=VALUE")]
    var: Vec<String>,

    // === Positional arguments. ===
    /// Glob patterns of testdrive scripts to run.
    globs: Vec<String>,
}

#[tokio::main]
async fn main() {
    let args: Args = ore::cli::parse_args();

    let (aws_config, aws_account) = match args.aws_region {
        Some(region) => {
            // Standard AWS region without a custom endpoint. Try to find actual
            // AWS credentials.
            let loader = aws_config::from_env().region(Region::new(region));
            let config = AwsConfig::from_loader(loader).await;
            let account = async {
                let sts_client = mz_aws_util::sts::client(&config);
                Ok::<_, Box<dyn Error>>(
                    sts_client
                        .get_caller_identity()
                        .send()
                        .await?
                        .account
                        .ok_or("account ID is missing")?,
                )
            };
            let account = account
                .await
                .unwrap_or_else(|e| die!("testdrive: failed fetching AWS account ID: {}", e));
            (config, account)
        }
        None => {
            // The user specified a a custom endpoint. We assume we're targeting
            // a stubbed-out AWS implementation that does not use regions or
            // check authentication credentials, so we use dummy credentials and
            // a dummy region.
            let endpoint = args
                .aws_endpoint
                .unwrap_or_else(|| "http://localhost:4566".parse().unwrap());
            let loader = aws_config::from_env()
                .region(Region::new("us-east-1"))
                .credentials_provider(Credentials::from_keys(
                    "dummy-access-key-id",
                    "dummy-secret-access-key",
                    None,
                ));
            let mut config = AwsConfig::from_loader(loader).await;
            config.set_endpoint(Endpoint::immutable(endpoint));
            let account = "000000000000".into();
            (config, account)
        }
    };

    println!(
        "Configuration parameters:
    Kafka address: {}
    Schema registry URL: {}
    materialized host: {:?}
    error limit: {}",
        args.kafka_addr,
        args.schema_registry_url,
        args.materialized_url.get_hosts()[0],
        args.max_errors
    );

    let mut arg_vars = BTreeMap::new();
    for arg in &args.var {
        let mut parts = arg.splitn(2, '=');
        let name = parts.next().expect("Clap ensures all --vars get a value");
        let val = match parts.next() {
            Some(val) => val,
            None => {
                println!("No =VALUE for --var {}", name);
                process::exit(1)
            }
        };
        arg_vars.insert(name.to_string(), val.to_string());
    }

    let config = Config {
        kafka_addr: args.kafka_addr,
        kafka_opts: args.kafka_option,
        schema_registry_url: args.schema_registry_url,
        cert_path: args.cert,
        cert_pass: args.cert_password,
        aws_config,
        aws_account,
        materialized_pgconfig: args.materialized_url,
        materialized_catalog_path: args.validate_catalog,
        reset: !args.no_reset,
        default_timeout: args.default_timeout,
        initial_backoff: args.initial_backoff,
        backoff_factor: args.backoff_factor,
        arg_vars,
        seed: args.seed,
        temp_dir: args.temp_dir,
    };

    // Build the list of files to test.
    //
    // The requirements here are a bit sensitive. Each argument on the command
    // line must be processed in order, but each individual glob expansion is
    // sorted.
    //
    // NOTE(benesch): it'd be nice to use `glob` or `globwalk` instead of
    // hand-rolling the directory traversal (it's pretty inefficient to list
    // every directory and only then apply the globs), but `globset` is the only
    // crate with a sensible globbing syntax.
    // See: https://github.com/rust-lang-nursery/glob/issues/59
    let mut files = vec![];
    if args.globs.is_empty() {
        files.push(PathBuf::from("-"))
    } else {
        let all_files = WalkDir::new(".")
            .sort_by_file_name()
            .into_iter()
            .map(|f| f.map(|f| f.path().clean()))
            .collect::<Result<Vec<_>, _>>()
            .unwrap_or_else(|e| die!("testdrive: failed walking directory: {}", e));
        for glob in args.globs {
            if glob == "-" {
                files.push(glob.into());
                continue;
            }
            let matcher = GlobBuilder::new(&Path::new(&glob).clean().to_string_lossy())
                .literal_separator(true)
                .build()
                .unwrap_or_else(|e| die!("testdrive: invalid glob syntax: {}: {}", glob, e))
                .compile_matcher();
            let mut found = false;
            for file in &all_files {
                if matcher.is_match(file) {
                    files.push(file.clone());
                    found = true;
                }
            }
            if !found {
                die!("testdrive: glob did not match any patterns: {}", glob)
            }
        }
    }

    if args.shuffle_tests {
        let seed = args.seed.unwrap_or_else(|| rand::thread_rng().gen());
        let mut rng = StdRng::seed_from_u64(seed.into());
        files.shuffle(&mut rng);
    }

    let mut error_count = 0;
    let mut error_files = HashSet::new();

    for file in files.into_iter().take(args.max_tests) {
        let res = if file == Path::new("-") {
            testdrive::run_stdin(&config).await
        } else {
            testdrive::run_file(&config, &file).await
        };
        if let Err(error) = res {
            let _ = error.print_stderr();
            error_count += 1;
            error_files.insert(file);
            if error_count >= args.max_errors {
                eprintln!("testdrive: maximum number of errors reached; giving up");
                break;
            }
        }
    }

    print!("+++ ");
    if error_count == 0 {
        println!("testdrive completed successfully.");
    } else {
        eprintln!("!!! Error Report");
        eprintln!("{} errors were encountered during execution", error_count);
        if !error_files.is_empty() {
            eprintln!(
                "files involved: {}",
                error_files.iter().map(|p| p.display()).join(" ")
            );
        }
        process::exit(1);
    }
}

fn parse_kafka_opt(opt: &str) -> (String, String) {
    let mut pieces = opt.splitn(2, '=');
    let key = pieces.next().unwrap_or("").to_owned();
    let val = pieces.next().unwrap_or("").to_owned();
    (key, val)
}
