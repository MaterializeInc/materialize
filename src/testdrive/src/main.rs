// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp;
use std::path::PathBuf;
use std::process;
use std::time::Duration;

use aws_util::aws;
use rand::{rngs::StdRng, seq::SliceRandom, Rng, SeedableRng};
use rusoto_credential::{AwsCredentials, ChainProvider, ProvideAwsCredentials};
use structopt::StructOpt;
use url::Url;

use testdrive::Config;

/// Integration test driver for Materialize.
#[derive(StructOpt)]
struct Args {
    // === Confluent options. ===
    /// Kafka bootstrap address.
    #[structopt(
        long,
        value_name = "ENCRYPTION://HOST:PORT",
        default_value = "localhost:9092"
    )]
    kafka_addr: String,
    /// Kafka configuration option.
    #[structopt(long, value_name = "KEY=VAL", parse(from_str = parse_kafka_opt))]
    kafka_option: Vec<(String, String)>,
    /// Schema registry URL.
    #[structopt(long, value_name = "URL", default_value = "http://localhost:8081")]
    schema_registry_url: Url,

    // === TLS options. ===
    /// Path to TLS certificate keystore.
    ///
    /// The keystore must be in the PKCS#12 format.
    #[structopt(long, value_name = "PATH")]
    cert: Option<String>,
    /// Password for the TLS certificate keystore.
    #[structopt(long, value_name = "PASSWORD")]
    cert_password: Option<String>,

    // === AWS options. ===
    /// Named AWS region to target for AWS API requests.
    #[structopt(long, default_value = "localstack")]
    aws_region: String,
    /// Custom AWS endpoint. Default: "http://localhost:4566".
    #[structopt(long)]
    aws_endpoint: Option<String>,

    // === Materialize options. ===
    /// materialized connection string.
    #[structopt(long, default_value = "postgres://materialize@localhost:6875")]
    materialized_url: tokio_postgres::Config,
    /// Validate the on-disk state of the materialized catalog.
    #[structopt(long)]
    validate_catalog: Option<PathBuf>,
    /// Don't reset materialized state before executing each script.
    #[structopt(long)]
    no_reset: bool,

    // === Testdrive options. ===
    /// Emit Buildkite-specific markup.
    #[structopt(long)]
    ci_output: bool,

    /// Default timeout in seconds.
    #[structopt(long, default_value = "10")]
    default_timeout: f64,

    /// A random number to distinguish each TestDrive run.
    #[structopt(long)]
    seed: Option<u32>,

    /// Maximum number of errors before aborting
    #[structopt(long, default_value = "10")]
    max_errors: usize,

    /// Max number of tests to run before terminating
    #[structopt(long, default_value = "18446744073709551615")]
    max_tests: usize,

    /// Shuffle tests (using the value from --seed, if any)
    #[structopt(long)]
    shuffle_tests: bool,

    /// Force the use of the specfied temporary directory rather than creating one with a random name
    #[structopt(long)]
    temp_dir: Option<String>,

    // === Positional arguments. ===
    /// Paths to testdrive scripts to run.
    files: Vec<String>,
}

#[tokio::main]
async fn main() {
    let args: Args = ore::cli::parse_args();
    let mut files = args.files;
    let default_timeout = Duration::from_secs_f64(args.default_timeout);

    let (aws_region, aws_account, aws_credentials) = match (
        args.aws_region.parse::<rusoto_core::Region>(),
        args.aws_endpoint,
    ) {
        (Ok(region), None) => {
            // Standard AWS region without a custom endpoint. Try to find actual
            // AWS credentials.
            let mut provider = ChainProvider::new();
            provider.set_timeout(default_timeout);
            let credentials = provider
                .credentials()
                .await
                .expect("retrieving AWS credentials");
            let account = aws::account(provider, region.clone(), default_timeout)
                .await
                .expect("getting AWS account details");
            (region, account, credentials)
        }
        (_, aws_endpoint) => {
            // The user specified a non-standard AWS region, a custom endpoint,
            // or both. We instruct Rusoto to use these values by constructing
            // an appropriate `Region::Custom`. We additionally assume we're
            // targeting a stubbed-out AWS implementation that does not check
            // authentication credentials, so we use dummy credentials.
            let region = rusoto_core::Region::Custom {
                name: args.aws_region,
                endpoint: aws_endpoint.unwrap_or_else(|| "http://localhost:4566".into()),
            };
            let account = "000000000000";
            let credentials =
                AwsCredentials::new("dummy-access-key-id", "dummy-secret-access-key", None, None);
            (region, account.into(), credentials)
        }
    };

    println!(
        "Configuration parameters:
    AWS region: {:?}
    Kafka Address: {}
    Schema registry URL: {}
    materialized host: {:?}
    error limit: {}",
        aws_region,
        args.kafka_addr,
        args.schema_registry_url,
        args.materialized_url.get_hosts()[0],
        args.max_errors
    );

    let config = Config {
        kafka_addr: args.kafka_addr,
        kafka_opts: args.kafka_option,
        schema_registry_url: args.schema_registry_url,
        cert_path: args.cert,
        cert_pass: args.cert_password,
        aws_region,
        aws_account,
        aws_credentials,
        materialized_pgconfig: args.materialized_url,
        materialized_catalog_path: args.validate_catalog,
        reset: !args.no_reset,
        ci_output: args.ci_output,
        default_timeout,
        seed: args.seed,
        temp_dir: args.temp_dir,
    };

    let mut errors = Vec::new();
    let mut error_files = Vec::new();

    if files.is_empty() {
        files.push("-".to_string())
    }

    if args.shuffle_tests {
        let mut rng: StdRng = SeedableRng::seed_from_u64(
            args.seed.unwrap_or_else(|| rand::thread_rng().gen()).into(),
        );
        files.shuffle(&mut rng);
    }

    for file in &files[..cmp::min(args.max_tests, files.len())] {
        if let Err(error) = match file.as_str() {
            "-" => testdrive::run_stdin(&config).await,
            _ => testdrive::run_file(&config, &file).await,
        } {
            let _ = error.print_stderr(args.ci_output);
            error_files.push(file.clone());

            errors.push(error);
            if errors.len() >= args.max_errors {
                break;
            }
        }
    }

    if config.ci_output {
        print!("+++ ")
    }
    if errors.is_empty() {
        println!("testdrive completed successfully.");
    } else {
        eprintln!("!!! Error Report");
        eprintln!("{} errors were encountered during execution", errors.len());

        if !error_files.is_empty() {
            eprintln!("files involved: {}", error_files.join(" "));
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
