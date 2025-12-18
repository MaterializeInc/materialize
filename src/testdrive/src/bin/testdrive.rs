// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::error::Error;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::{io, process};

use aws_credential_types::Credentials;
use aws_types::region::Region;
use clap::ArgAction;
use globset::GlobBuilder;
use itertools::Itertools;
use mz_build_info::{BuildInfo, build_info};
use mz_catalog::config::ClusterReplicaSizeMap;
use mz_license_keys::ValidatedLicenseKey;
use mz_ore::cli::{self, CliConfig};
use mz_ore::path::PathExt;
use mz_ore::url::SensitiveUrl;
use mz_testdrive::{CatalogConfig, Config, ConsistencyCheckLevel};
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
#[allow(deprecated)] // fails with libraries still using old time lib
use time::Instant;
use tracing::info;
use tracing_subscriber::filter::EnvFilter;
use url::Url;
use walkdir::WalkDir;

macro_rules! die {
    ($($e:expr),*) => {{
        eprintln!($($e),*);
        process::exit(1);
    }}
}

pub const BUILD_INFO: BuildInfo = build_info!();

/// Integration test driver for Materialize.
#[derive(clap::Parser)]
struct Args {
    // === Testdrive options. ===
    /// Variables to make available to the testdrive script.
    ///
    /// Passing `--var foo=bar` will create a variable named `arg.foo` with the
    /// value `bar`. Can be specified multiple times to set multiple variables.
    #[clap(long, env = "VAR", value_name = "NAME=VALUE")]
    var: Vec<String>,
    /// A random number to distinguish each testdrive run.
    #[clap(long, value_name = "N", action = ArgAction::Set)]
    seed: Option<u32>,
    /// Whether to reset Materialize state before executing each script and
    /// to clean up AWS state after each script.
    #[clap(long, action = ArgAction::SetTrue)]
    no_reset: bool,
    /// Force the use of the specified temporary directory.
    ///
    /// If unspecified, testdrive creates a temporary directory with a random
    /// name.
    #[clap(long, value_name = "PATH")]
    temp_dir: Option<String>,
    /// Source string to print out on errors.
    #[clap(long, value_name = "SOURCE")]
    source: Option<String>,
    /// Default timeout for cancellable operations.
    #[clap(long, value_parser = humantime::parse_duration, default_value = "30s", value_name = "DURATION")]
    default_timeout: Duration,
    /// The default number of times to retry a query expecting it to converge to the desired result.
    #[clap(long, default_value = "18446744073709551615", value_name = "N")]
    default_max_tries: usize,
    /// Initial backoff interval for retry operations.
    ///
    /// Set to 0 to retry immediately on failure.
    #[clap(long, value_parser = humantime::parse_duration, default_value = "50ms", value_name = "DURATION")]
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
    #[clap(long, requires = "shard_count", value_name = "N")]
    shard: Option<usize>,
    /// Total number of shards in use.
    #[clap(long, requires = "shard", value_name = "N")]
    shard_count: Option<usize>,
    /// Generate a JUnit-compatible XML report to the specified file.
    #[clap(long, value_name = "FILE")]
    junit_report: Option<PathBuf>,
    /// Whether we skip coordinator and catalog consistency checks.
    #[clap(long, default_value_t = ConsistencyCheckLevel::default(), value_enum)]
    consistency_checks: ConsistencyCheckLevel,
    /// Whether to run statement logging consistency checks (adds a few seconds at the end of every
    /// test file).
    #[clap(long, action = ArgAction::SetTrue)]
    check_statement_logging: bool,
    /// Which log messages to emit.
    ///
    /// See environmentd's `--startup-log-filter` option for details.
    #[clap(
        long,
        env = "LOG_FILTER",
        value_name = "FILTER",
        default_value = "librdkafka=off,mz_kafka_util::client=off,warn"
    )]
    log_filter: String,
    /// Glob patterns of testdrive scripts to run.
    globs: Vec<String>,
    /// Automatically rewrite the testdrive file with the correct results when they are not as
    /// expected
    #[clap(long)]
    rewrite_results: bool,

    // === Materialize options. ===
    /// materialize SQL connection string.
    #[clap(
        long,
        default_value = "postgres://materialize@localhost:6875",
        value_name = "URL",
        action = ArgAction::Set,
    )]
    materialize_url: tokio_postgres::Config,
    /// materialize internal SQL connection string.
    #[clap(
        long,
        default_value = "postgres://materialize@localhost:6877",
        value_name = "INTERNAL_URL",
        action = ArgAction::Set,
    )]
    materialize_internal_url: tokio_postgres::Config,
    #[clap(long)]
    materialize_use_https: bool,
    /// The port on which Materialize is listening for untrusted HTTP
    /// connections.
    ///
    /// The hostname is taken from `materialize_url`.
    #[clap(long, default_value = "6876", value_name = "PORT")]
    materialize_http_port: u16,
    /// The port on which Materialize is listening for trusted HTTP connections.
    ///
    /// The hostname is taken from `materialize_internal_url`.
    #[clap(long, default_value = "6878", value_name = "PORT")]
    materialize_internal_http_port: u16,
    /// The port on which Materialize is listening for password authenticated SQL connections.
    ///
    /// The hostname is taken from `materialize_url`.
    #[clap(long, default_value = "6880", value_name = "PORT")]
    materialize_password_sql_port: u16,
    /// The port on which Materialize is listening for password authenticated SQL connections.
    ///
    /// The hostname is taken from `materialize_url`.
    #[clap(long, default_value = "6881", value_name = "PORT")]
    materialize_sasl_sql_port: u16,
    /// Arbitrary session parameters for testdrive to set after connecting to
    /// Materialize.
    #[clap(long, value_name = "KEY=VAL", value_parser = parse_kafka_opt)]
    materialize_param: Vec<(String, String)>,
    /// Validate the catalog state of the specified catalog kind.
    #[clap(long)]
    validate_catalog_store: bool,

    // === Persist options. ===
    /// Handle to the persist consensus system.
    #[clap(
        long,
        value_name = "PERSIST_CONSENSUS_URL",
        required_if_eq("validate_catalog_store", "true"),
        action = ArgAction::Set,
    )]
    persist_consensus_url: Option<SensitiveUrl>,
    /// Handle to the persist blob storage.
    #[clap(
        long,
        value_name = "PERSIST_BLOB_URL",
        required_if_eq("validate_catalog_store", "true")
    )]
    persist_blob_url: Option<SensitiveUrl>,

    // === Confluent options. ===
    /// Address of the Kafka broker that testdrive will interact with.
    #[clap(
        long,
        value_name = "ENCRYPTION://HOST:PORT",
        default_value = "localhost:9092",
        action = ArgAction::Set,
    )]
    kafka_addr: String,
    /// Default number of partitions to create for topics
    #[clap(long, default_value = "1", value_name = "N")]
    kafka_default_partitions: usize,
    /// Arbitrary rdkafka options for testdrive to use when connecting to the
    /// Kafka broker.
    #[clap(long, env = "KAFKA_OPTION", use_value_delimiter=true, value_name = "KEY=VAL", value_parser = parse_kafka_opt)]
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
        conflicts_with = "aws_endpoint",
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
        conflicts_with = "aws_region",
        value_name = "URL",
        env = "AWS_ENDPOINT"
    )]
    aws_endpoint: Option<String>,

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

    // === Fivetran options. ===
    /// Address of the Fivetran Destination that testdrive will interact with.
    #[clap(
        long,
        value_name = "FIVETRAN_DESTINATION_URL",
        default_value = "http://localhost:6874"
    )]
    fivetran_destination_url: String,
    #[clap(
        long,
        value_name = "FIVETRAN_DESTINATION_FILES_PATH",
        default_value = "/tmp"
    )]
    fivetran_destination_files_path: String,
    /// A map from size name to resource allocations for cluster replicas.
    #[clap(long, env = "CLUSTER_REPLICA_SIZES")]
    cluster_replica_sizes: String,

    #[clap(long, env = "MZ_CI_LICENSE_KEY")]
    license_key: Option<String>,
}

#[tokio::main]
async fn main() {
    let args: Args = cli::parse_args(CliConfig::default());

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from(args.log_filter))
        .with_writer(io::stdout)
        .init();

    let (aws_config, aws_account) = match args.aws_region {
        Some(region) => {
            // Standard AWS region without a custom endpoint. Try to find actual
            // AWS credentials.
            let config = mz_aws_util::defaults()
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
            let config = mz_aws_util::defaults()
                .region(Region::new("us-east-1"))
                .credentials_provider(Credentials::from_keys(
                    args.aws_access_key_id,
                    args.aws_secret_access_key,
                    None,
                ))
                .endpoint_url(endpoint)
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
    Error limit: {}
    Consistency check level: {:?}",
        args.kafka_addr,
        args.schema_registry_url,
        args.materialize_url.get_hosts()[0],
        args.max_errors,
        args.consistency_checks,
    );
    if let (Some(shard), Some(shard_count)) = (args.shard, args.shard_count) {
        if shard != 0 || shard_count != 1 {
            eprintln!("    Shard: {}/{}", shard + 1, shard_count);
        }
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

    let license_key = if let Some(license_key_text) = args.license_key {
        mz_license_keys::validate(license_key_text.trim())
            .unwrap_or_else(|e| die!("testdrive: failed to validate license key: {}", e))
    } else {
        ValidatedLicenseKey::default()
    };

    let cluster_replica_sizes = ClusterReplicaSizeMap::parse_from_str(
        &args.cluster_replica_sizes,
        !license_key.allow_credit_consumption_override,
    )
    .unwrap_or_else(|e| die!("testdrive: failed to parse replica size map: {}", e));

    let materialize_catalog_config = if args.validate_catalog_store {
        Some(CatalogConfig {
            persist_consensus_url: args
                .persist_consensus_url
                .clone()
                .expect("required for persist catalog"),
            persist_blob_url: args
                .persist_blob_url
                .clone()
                .expect("required for persist catalog"),
        })
    } else {
        None
    };
    let config = Config {
        // === Testdrive options. ===
        arg_vars,
        seed: args.seed,
        reset: !args.no_reset,
        temp_dir: args.temp_dir,
        source: args.source,
        default_timeout: args.default_timeout,
        default_max_tries: args.default_max_tries,
        initial_backoff: args.initial_backoff,
        backoff_factor: args.backoff_factor,
        consistency_checks: args.consistency_checks,
        check_statement_logging: args.check_statement_logging,
        rewrite_results: args.rewrite_results,

        // === Materialize options. ===
        materialize_pgconfig: args.materialize_url,
        materialize_cluster_replica_sizes: cluster_replica_sizes,
        materialize_internal_pgconfig: args.materialize_internal_url,
        materialize_http_port: args.materialize_http_port,
        materialize_internal_http_port: args.materialize_internal_http_port,
        materialize_use_https: args.materialize_use_https,
        materialize_password_sql_port: args.materialize_password_sql_port,
        materialize_sasl_sql_port: args.materialize_sasl_sql_port,
        materialize_params: args.materialize_param,
        materialize_catalog_config,
        build_info: &BUILD_INFO,

        // === Persist options. ===
        persist_consensus_url: args.persist_consensus_url,
        persist_blob_url: args.persist_blob_url,

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

        // === Fivetran options. ===
        fivetran_destination_url: args.fivetran_destination_url,
        fivetran_destination_files_path: args.fivetran_destination_files_path,
    };

    if args.junit_report.is_some() && args.rewrite_results {
        eprintln!("--rewrite-results is not compatible with --junit-report");
        process::exit(1);
    }

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
        let seed = args.seed.unwrap_or_else(rand::random);
        let mut rng = StdRng::seed_from_u64(seed.into());
        files.shuffle(&mut rng);
    }

    let mut error_count = 0;
    let mut error_files = BTreeSet::new();
    let mut junit = match args.junit_report {
        Some(filename) => match File::create(&filename) {
            Ok(file) => Some((file, junit_report::TestSuite::new("testdrive"))),
            Err(err) => die!("creating {}: {}", filename.display(), err),
        },
        None => None,
    };

    for file in files.into_iter().take(args.max_tests) {
        #[allow(deprecated)] // fails with libraries still using old time lib
        let start_time = Instant::now();
        let res = if file == Path::new("-") {
            if args.rewrite_results {
                eprintln!("--rewrite-results is not compatible with stdin files");
                process::exit(1);
            }
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
                    // Encode newlines so they get preserved when being parsed by python-junit-xml
                    &error.to_string().replace("\n", "&#10;"),
                ),
            };
            test_case.set_classname("testdrive");
            junit_suite.add_testcase(test_case);
        }
        if let Err(error) = res {
            let _ = error.print_error();
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

    if error_count > 0 {
        eprint!("+++ ");
        eprintln!("!!! Error Report");
        eprintln!("{} errors were encountered during execution", error_count);
        if config.source.is_some() {
            eprintln!("source: {}", config.source.unwrap());
        } else if !error_files.is_empty() {
            eprintln!(
                "files involved: {}",
                error_files.iter().map(|p| p.display()).join(" ")
            );
        }
        process::exit(1);
    }
}

fn parse_kafka_opt(opt: &str) -> Result<(String, String), Infallible> {
    let mut pieces = opt.splitn(2, '=');
    let key = pieces.next().unwrap_or("").to_owned();
    let val = pieces.next().unwrap_or("").to_owned();
    Ok((key, val))
}
