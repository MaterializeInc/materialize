// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::env;
use std::process;
use std::time::Duration;

use getopts::Options;

use aws_util::aws;
use testdrive::error::{Error, ResultExt};
use testdrive::Config;

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        // If printing the error message fails, there's not a whole lot we can
        // do.
        let _ = err.print_stderr();
        process::exit(err.exit_code());
    }
}

async fn run() -> Result<(), Error> {
    let args: Vec<_> = env::args().collect();

    let mut opts = Options::new();

    // Confluent options.
    opts.optopt(
        "",
        "kafka-addr",
        "kafka bootstrap address",
        "ENCRYPTION://HOST:PORT",
    );
    opts.optmulti("", "kafka-option", "kafka configuration option", "KEY=VAL");
    opts.optopt("", "schema-registry-url", "schema registry URL", "URL");

    // TLS options.
    opts.optopt(
        "",
        "cert",
        "Path to the keystore for the client cert and key; must be der-encoded, e.g. .p12",
        "PATH",
    );
    opts.optopt("", "cert-password", "Keystore password", "PASSWORD");

    // Kerberos options.
    opts.optopt("", "krb5-keytab", "Path to the Kerberos keystab", "PATH");
    opts.optopt(
        "",
        "krb5-service-name",
        "The name of the service you want to connect to (probably 'kafka')",
        "STRING",
    );
    opts.optopt(
        "",
        "krb5-principal",
        "The client's Kerberos principal",
        "STRING",
    );

    // AWS options.
    opts.optopt(
        "",
        "aws-region",
        "a named AWS region to target for AWS API requests",
        "custom",
    );
    opts.optopt("", "aws-endpoint", "custom AWS endpoint", "URL");

    // Materialize options.
    opts.optopt(
        "",
        "materialized-url",
        "materialized connection string",
        "URL",
    );
    opts.optopt(
        "",
        "validate-catalog",
        "validate the on-disk state of the materialized catalog",
        "PATH",
    );
    opts.optflag(
        "",
        "no-reset",
        "don't reset materialized state before executing testdrive script",
    );
    opts.optflag("h", "help", "show this usage information");

    let usage_details = opts.usage("usage: testdrive [options] FILE");
    let opts = opts
        .parse(&args[1..])
        .err_ctx("parsing options".into())
        .map_err(|e| Error::Usage {
            details: format!("{}\n{}\n", usage_details, e),
            requested: false,
        })?;

    if opts.opt_present("h") {
        return Err(Error::Usage {
            details: usage_details,
            requested: true,
        });
    }

    // Confluent options.
    let mut config = Config::default();
    if let Some(addr) = opts.opt_str("kafka-addr") {
        config.kafka_addr = addr;
    }
    for opt in opts.opt_strs("kafka-option") {
        let mut pieces = opt.splitn(2, '=');
        let key = pieces.next().unwrap_or("").to_owned();
        let val = pieces.next().unwrap_or("").to_owned();
        config.kafka_opts.push((key, val));
    }
    if let Some(url) = opts.opt_str("schema-registry-url") {
        config.schema_registry_url = url.parse().map_err(|e| Error::General {
            ctx: "parsing schema registry url".into(),
            cause: Some(Box::new(e)),
            hints: vec![],
        })?;
    }

    // TLS options.
    if let Some(path) = opts.opt_str("cert") {
        config.cert_path = Some(path);
    }
    if let Some(pass) = opts.opt_str("cert-password") {
        config.cert_pass = Some(pass);
    }

    // AWS options.
    if let (Ok(Some(region)), None) = (opts.opt_get("aws-region"), opts.opt_str("aws-endpoint")) {
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

        config.aws_region = region;
        config.aws_account = account;
        config.aws_credentials = credentials;
    } else {
        // Either a non-standard AWS region, a custom endpoint, or both. Assume
        // dummy authentication, and just use the default dummy credentials in
        // the default config.
        config.aws_region = rusoto_core::Region::Custom {
            name: opts
                .opt_str("aws-region")
                .unwrap_or_else(|| "localstack".into()),
            endpoint: opts
                .opt_str("aws-endpoint")
                .unwrap_or_else(|| "http://localhost:4566".into()),
        };
    }

    // Materialize options.
    if let Some(url) = opts.opt_str("materialized-url") {
        config.materialized_pgconfig = url.parse().map_err(|e| Error::General {
            ctx: "parsing materialized url".into(),
            cause: Some(Box::new(e)),
            hints: vec![],
        })?;
    }
    if let Some(path) = opts.opt_str("validate-catalog") {
        config.materialized_catalog_path = Some(path.into());
    }
    if opts.opt_present("no-reset") {
        config.reset_materialized = false;
    }

    if opts.free.is_empty() {
        testdrive::run_stdin(&config).await
    } else {
        for arg in opts.free {
            if arg == "-" {
                testdrive::run_stdin(&config).await?
            } else {
                testdrive::run_file(&config, &arg).await?
            }
        }
        Ok(())
    }
}
