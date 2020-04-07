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

use getopts::Options;

use testdrive::error::{Error, ResultExt};
use testdrive::Config;

fn main() {
    if let Err(err) = run() {
        // If printing the error message fails, there's not a whole lot we can
        // do.
        let _ = err.print_stderr();
        process::exit(err.exit_code());
    }
}

fn run() -> Result<(), Error> {
    let args: Vec<_> = env::args().collect();

    let mut opts = Options::new();
    opts.optopt("", "kafka-addr", "kafka bootstrap address", "HOST:PORT");
    opts.optopt("", "schema-registry-url", "schema registry URL", "URL");
    opts.optopt(
        "",
        "kinesis-region",
        "optional custom kinesis region",
        "custom",
    );
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

    let mut config = Config::default();
    if let Some(addr) = opts.opt_str("kafka-addr") {
        config.kafka_addr = addr;
    }
    if let Some(url) = opts.opt_str("schema-registry-url") {
        config.schema_registry_url = url.parse().map_err(|e| Error::General {
            ctx: "parsing schema registry url".into(),
            cause: Some(Box::new(e)),
            hints: vec![],
        })?;
    }
    if let Some(region) = opts.opt_str("kinesis-region") {
        config.kinesis_region = region;
    }
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

    if opts.free.is_empty() {
        testdrive::run_stdin(&config)
    } else {
        for arg in opts.free {
            if arg == "-" {
                testdrive::run_stdin(&config)?
            } else {
                testdrive::run_file(&config, &arg)?
            }
        }
        Ok(())
    }
}
