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
use std::fs::File;
use std::io;
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
use time::Instant;
use tracing::info;
use tracing_subscriber::filter::EnvFilter;
use url::Url;
use walkdir::WalkDir;

use mz_ore::cli::{self, CliConfig};
use mz_ore::path::PathExt;

use mz_testdrive::Config;

macro_rules! die {
    ($($e:expr),*) => {{
        eprintln!($($e),*);
        process::exit(1);
    }}
}

/// Integration test driver for Materialize.
#[derive(clap::Parser)]
struct Args {
    // === Testdrive options. ===
    /// Variables to make available to the testdrive script.
    ///
    /// Passing `--var foo=bar` will create a variable named `arg.foo` with the
    /// value `bar`. Can be specified multiple times to set multiple variables.
    #[clap(long, value_name = "NAME=VALUE")]
    var: Vec<String>,
    /// A random number to distinguish each testdrive run.
    #[clap(long, value_name = "N")]
    seed: Option<u32>,
    /// Whether to reset Materialize state before executing each script and
    /// to clean up AWS state after each script.
    #[clap(long)]
    no_reset: bool,
    /// Force the use of the specified temporary directory.
    ///
    /// If unspecified, testdrive creates a temporary directory with a random
    /// name.
    #[clap(long, value_name = "PATH")]
    temp_dir: Option<String>,
    /// Default timeout for cancellable operations.
    #[clap(long, parse(try_from_str = mz_repr::util::parse_duration), default_value = "30s", value_name = "DURATION")]
    default_timeout: Duration,
    /// The default number of times to retry a query expecting it to converge to the desired result.
    #[clap(long, default_value = "18446744073709551615", value_name = "N")]
    default_max_tries: usize,
    /// Initial backoff interval for retry operations.
    ///
    /// Set to 0 to retry immediately on failure.
    #[clap(long, parse(try_from_str = mz_repr::util::parse_duration), default_value = "50ms", value_name = "DURATION")]
    initial_backoff: Duration,
    /// Backoff factor when retrying.
    ///
    /// Set to 1 to retry at a steady pace.
    #[clap(long, default_value = "1.5", value_name = "FACTOR")]
    backoff_factor: f64,
    /// Maximum number of errors to accumulate before aborting.
    #[clap(long, default_value = "10", value_name = "N")]
    max_errors: usize,
    /// Maximum number of tests to run before terminating.
    #[clap(long, default_value = "18446744073709551615", value_name = "N")]
    max_tests: usize,
    /// Shuffle tests.
    ///
    /// The shuffled order reflects the seed specified by --seed, if any.
    #[clap(long)]
    shuffle_tests: bool,
    /// Divide the test files into shards and run only the test files in this
    /// shard.
    #[clap(long, requires = "shard-count", value_name = "N")]
    shard: Option<usize>,
    /// Total number of shards in use.
    #[clap(long, requires = "shard", value_name = "N")]
    shard_count: Option<usize>,
    /// Generate a JUnit-compatible XML report to the specified file.
    #[clap(long, value_name = "FILE")]
    junit_report: Option<PathBuf>,
    /// Which log messages to emit.
    ///
    /// See environmentd's `--log-filter` option for details.
    #[clap(long, value_name = "FILTER", default_value = "off")]
    log_filter: EnvFilter,
    /// Glob patterns of testdrive scripts to run.
    globs: Vec<String>,

    // === Materialize options. ===
    /// materialize SQL connection string.
    #[clap(
        long,
        default_value = "postgres://materialize@localhost:6875",
        value_name = "URL"
    )]
    materialize_url: tokio_postgres::Config,
    /// materialize SQL internal connection string.
    #[clap(
        long,
        default_value = "postgres://materialize@localhost:6877",
        value_name = "INTERNAL_URL"
    )]
    materialize_url_internal: tokio_postgres::Config,
    /// The port on which Materialize is listening for untrusted HTTP connections.
    ///
    /// The hostname is taken from `materialize_url`.
    #[clap(long, default_value = "6876", value_name = "PORT")]
    materialize_http_port: u16,
    /// Arbitrary session parameters for testdrive to set after connecting to
    /// Materialize.
    #[clap(long, value_name = "KEY=VAL", parse(from_str = parse_kafka_opt))]
    materialize_param: Vec<(String, String)>,
    /// Validate the on-disk state of the specified Materialize data directory.
    #[clap(long, value_name = "PATH")]
    validate_data_dir: Option<PathBuf>,
    /// Validate the stash state of the specified postgres connection string.
    #[clap(long, value_name = "POSTGRES_URL")]
    validate_postgres_stash: Option<String>,

    // === Confluent options. ===
    /// Address of the Kafka broker that testdrive will interact with.
    #[clap(
        long,
        value_name = "ENCRYPTION://HOST:PORT",
        default_value = "localhost:9092"
    )]
    kafka_addr: String,
    /// Default number of partitions to create for topics
    #[clap(long, default_value = "1", value_name = "N")]
    kafka_default_partitions: usize,
    /// Arbitrary rdkafka options for testdrive to use when connecting to the
    /// Kafka broker.
    #[clap(long, value_name = "KEY=VAL", parse(from_str = parse_kafka_opt))]
    kafka_option: Vec<(String, String)>,
    /// URL of the schema registry that testdrive will connect to.
    #[clap(long, value_name = "URL", default_value = "http://localhost:8081")]
    schema_registry_url: Url,
    /// Path to a TLS certificate that testdrive will present when performing
    /// client authentication.
    ///
    /// The keystore must be in the PKCS#12 format.
    #[clap(long, value_name = "PATH")]
    cert: Option<String>,
    /// Password for the TLS certificate.
    #[clap(long, value_name = "PASSWORD")]
    cert_password: Option<String>,
    /// Username for basic authentication with the Confluent Schema Registry.
    #[clap(long, value_name = "USERNAME")]
    ccsr_username: Option<String>,
    /// Password for basic authentication with the Confluent Schema Registry.
    #[clap(long, value_name = "PASSWORD")]
    ccsr_password: Option<String>,

    // === AWS options. ===
    /// Named AWS region to target for AWS API requests.
    ///
    /// Cannot be specified if --aws-endpoint is specified.
    #[clap(
        long,
        conflicts_with = "aws-endpoint",
        value_name = "REGION",
        env = "AWS_REGION"
    )]
    aws_region: Option<String>,
    /// Custom AWS endpoint.
    ///
    /// Defaults to http://localhost:4566 unless --aws-region is specified.
    /// Cannot be specified if --aws-region is specified.
    #[clap(
        long,
        conflicts_with = "aws-region",
        value_name = "URL",
        env = "AWS_ENDPOINT"
    )]
    aws_endpoint: Option<Uri>,

    #[clap(
        long,
        value_name = "KEY_ID",
        default_value = "dummy-access-key-id",
        env = "AWS_ACCESS_KEY_ID"
    )]
    aws_access_key_id: String,

    #[clap(
        long,
        value_name = "KEY",
        default_value = "dummy-secret-access-key",
        env = "AWS_SECRET_ACCESS_KEY"
    )]
    aws_secret_access_key: String,
}

#[tokio::main]
async fn main() {
    let args: Args = cli::parse_args(CliConfig::default());

    tracing_subscriber::fmt()
        .with_env_filter(args.log_filter)
        .with_writer(io::stderr)
        .init();

    let (aws_config, aws_account) = match args.aws_region {
        Some(region) => {
            // Standard AWS region without a custom endpoint. Try to find actual
            // AWS credentials.
            let config = aws_config::from_env()
                .region(Region::new(region))
                .load()
                .await;
            let account = async {
                let sts_client = aws_sdk_sts::Client::new(&config);
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
            // a stubbed-out AWS implementation.
            let endpoint = args
                .aws_endpoint
                .unwrap_or_else(|| "http://localhost:4566".parse().unwrap());
            let config = aws_config::from_env()
                .region(Region::new("us-east-1"))
                .credentials_provider(Credentials::from_keys(
                    args.aws_access_key_id,
                    args.aws_secret_access_key,
                    None,
                ))
                .endpoint_resolver(Endpoint::immutable(endpoint))
                .load()
                .await;
            let account = "000000000000".into();
            (config, account)
        }
    };

    info!(
        "Configuration parameters:
    Kafka address: {}
    Schema registry URL: {}
    Materialize host: {:?}
    Error limit: {}",
        args.kafka_addr,
        args.schema_registry_url,
        args.materialize_url.get_hosts()[0],
        args.max_errors
    );
    if let (Some(shard), Some(shard_count)) = (args.shard, args.shard_count) {
        eprintln!("    Shard: {}/{}", shard + 1, shard_count);
    }

    let mut arg_vars = BTreeMap::new();
    for arg in &args.var {
        let mut parts = arg.splitn(2, '=');
        let name = parts.next().expect("Clap ensures all --vars get a value");
        let val = match parts.next() {
            Some(val) => val,
            None => {
                eprintln!("No =VALUE for --var {}", name);
                process::exit(1)
            }
        };
        arg_vars.insert(name.to_string(), val.to_string());
    }

    let config = Config {
        // === Testdrive options. ===
        arg_vars,
        seed: args.seed,
        reset: !args.no_reset,
        temp_dir: args.temp_dir,
        default_timeout: args.default_timeout,
        default_max_tries: args.default_max_tries,
        initial_backoff: args.initial_backoff,
        backoff_factor: args.backoff_factor,

        // === Materialize options. ===
        materialize_pgconfig: args.materialize_url,
        materialize_pgconfig_internal: args.materialize_url_internal,
        materialize_http_port: args.materialize_http_port,
        materialize_params: args.materialize_param,
        materialize_catalog_postgres_stash: args.validate_postgres_stash,

        // === Confluent options. ===
        kafka_addr: args.kafka_addr,
        kafka_default_partitions: args.kafka_default_partitions,
        kafka_opts: args.kafka_option,
        schema_registry_url: args.schema_registry_url,
        cert_path: args.cert,
        cert_password: args.cert_password,
        ccsr_password: args.ccsr_password,
        ccsr_username: args.ccsr_username,

        // === AWS options. ===
        aws_config,
        aws_account,
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

    if let (Some(shard), Some(shard_count)) = (args.shard, args.shard_count) {
        files = files.into_iter().skip(shard).step_by(shard_count).collect();
    }

    if args.shuffle_tests {
        let seed = args.seed.unwrap_or_else(|| rand::thread_rng().gen());
        let mut rng = StdRng::seed_from_u64(seed.into());
        files.shuffle(&mut rng);
    }

    let mut error_count = 0;
    let mut error_files = HashSet::new();
    let mut junit = match args.junit_report {
        Some(filename) => match File::create(&filename) {
            Ok(file) => Some((file, junit_report::TestSuite::new("testdrive"))),
            Err(err) => die!("creating {}: {}", filename.display(), err),
        },
        None => None,
    };

    for file in files.into_iter().take(args.max_tests) {
        let start_time = Instant::now();
        let res = if file == Path::new("-") {
            mz_testdrive::run_stdin(&config).await
        } else {
            mz_testdrive::run_file(&config, &file).await
        };
        if let Some((_, junit_suite)) = &mut junit {
            let mut test_case = match &res {
                Ok(()) => {
                    junit_report::TestCase::success(&file.to_string_lossy(), start_time.elapsed())
                }
                Err(error) => junit_report::TestCase::failure(
                    &file.to_string_lossy(),
                    start_time.elapsed(),
                    "failure",
                    &error.to_string(),
                ),
            };
            test_case.set_classname("testdrive");
            junit_suite.add_testcase(test_case);
        }
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

    if let Some((mut junit_file, junit_suite)) = junit {
        let report = junit_report::ReportBuilder::new()
            .add_testsuite(junit_suite)
            .build();
        match report.write_xml(&mut junit_file) {
            Ok(()) => (),
            Err(e) => die!("error: unable to write junit report: {}", e),
        }
    }

    eprint!("+++ ");
    if error_count == 0 {
        eprintln!("testdrive completed successfully.");
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
