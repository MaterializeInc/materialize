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
use rusoto_credential::AwsCredentials;
use structopt::StructOpt;
use url::Url;

use testdrive::{Config, Error};

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
    /// Custom AWS endpoint.
    #[structopt(long)]
    aws_endpoint: Option<String>,

    // === Materialize options. ===
    /// materialized connection string.
    #[structopt(long, default_value = "postgres://localhost:6875")]
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
    let (aws_region, aws_account, aws_credentials) =
        match (args.aws_region.parse(), args.aws_endpoint) {
            (Ok(region), None) => {
                // Standard AWS region without a custom endpoint. Try to find actual AWS
                // credentials.
                let timeout = Duration::from_secs(5);
                let account = aws::account(timeout).await.map_err(|e| Error::General {
                    ctx: "getting AWS account details".into(),
                    cause: Some(e.into()),
                    hints: vec![],
                })?;
                let credentials = aws::credentials(timeout)
                    .await
                    .map_err(|e| Error::General {
                        ctx: "getting AWS account credentials".into(),
                        cause: Some(e.into()),
                        hints: vec![],
                    })?;
                (region, account, credentials)
            }
            (_, aws_endpoint) => {
                // Either a non-standard AWS region, a custom endpoint, or both. Assume
                // dummy authentication, and just use default dummy credentials in the
                // default config.
                let region = rusoto_core::Region::Custom {
                    name: args.aws_region,
                    endpoint: aws_endpoint.unwrap_or_else(|| "http://localhost:4566".into()),
                };
                let account = "000000000000";
                let credentials = AwsCredentials::new(
                    "dummy-access-key-id",
                    "dummy-secret-access-key",
                    None,
                    None,
                );
                (region, account.into(), credentials)
            }
        };

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
