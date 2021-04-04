// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;
use std::process;
use std::time::Duration;

use aws_util::aws;
use rusoto_credential::{AwsCredentials, ChainProvider, ProvideAwsCredentials};
use structopt::StructOpt;
use url::Url;

use testdrive::{Config, Error, ResultExt};

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

    // === Positional arguments. ===
    /// Paths to testdrive scripts to run.
    files: Vec<String>,
}

#[tokio::main]
async fn main() {
    let args: Args = ore::cli::parse_args();
    let ci_output = args.ci_output;
    if let Err(err) = run(args).await {
        // If printing the error message fails, there's not a whole lot we can
        // do.
        let _ = err.print_stderr(ci_output);
        process::exit(1);
    }
}

async fn run(args: Args) -> Result<(), Error> {
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
                .err_ctx("retrieving AWS credentials")?;
            let account = aws::account(provider, region.clone(), default_timeout)
                .await
                .err_ctx("getting AWS account details")?;
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
    materialized host: {:?}",
        aws_region,
        args.kafka_addr,
        args.schema_registry_url,
        args.materialized_url.get_hosts()[0],
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
        reset_materialized: !args.no_reset,
        ci_output: args.ci_output,
        default_timeout,
        seed: args.seed,
    };

    if args.files.is_empty() {
        testdrive::run_stdin(&config).await
    } else {
        for file in args.files {
            if file == "-" {
                testdrive::run_stdin(&config).await?
            } else {
                testdrive::run_file(&config, &file).await?
            }
        }
        Ok(())
    }
}

fn parse_kafka_opt(opt: &str) -> (String, String) {
    let mut pieces = opt.splitn(2, '=');
    let key = pieces.next().unwrap_or("").to_owned();
    let val = pieces.next().unwrap_or("").to_owned();
    (key, val)
}
