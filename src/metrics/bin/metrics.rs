// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc..

use std::cmp::min;
use std::collections::HashMap;
use std::thread;
use std::time::Duration;

use chrono::Utc;
use env_logger::{Builder as LogBuilder, Env, Target};
use log::{error, info};
use postgres::Connection;
use prometheus::Histogram;

static MAX_BACKOFF: Duration = Duration::from_secs(60);

#[derive(Debug)]
struct Config {
    pushgateway_url: String,
    materialized_url: String,
}

fn main() -> Result<(), failure::Error> {
    LogBuilder::from_env(Env::new().filter_or("MZ_LOG", "info"))
        .target(Target::Stdout)
        .init();
    info!("startup {}", Utc::now());

    let args: Vec<_> = std::env::args().collect();

    let mut opts = getopts::Options::new();
    opts.optflag("h", "help", "show this usage information");
    opts.optopt(
        "",
        "pushgateway-url",
        "url of the prometheus pushgateway to send metrics to",
        "URL",
    );
    opts.optopt(
        "",
        "materialized-url",
        "url of the materialized instance to collect metrics from",
        "URL",
    );
    let popts = opts.parse(&args[1..])?;
    if popts.opt_present("h") {
        print!("{}", opts.usage("usage: materialized [options]"));
        return Ok(());
    }
    let config = Config {
        pushgateway_url: popts
            .opt_get_default("pushgateway-url", "http://pushgateway:9091".to_owned())?,
        materialized_url: popts.opt_get_default(
            "materialized-url",
            "postgres://ignoreuser@materialized:6875/tpcch".to_owned(),
        )?,
    };

    measure_peek_times(&config);
}

fn measure_peek_times(config: &Config) -> ! {
    let postgres_connection = create_postgres_connection(config);
    try_initialize(&postgres_connection);

    thread::spawn(move || {
        let query = "SELECT * FROM q01;";
        let histogram = create_histogram(query);
        let mut backoff = get_baseline_backoff();
        let mut last_was_failure = false;
        loop {
            let query_result = {
                // Drop is observe for prometheus::Histogram
                let _timer = prometheus::Histogram::start_timer(&histogram);
                postgres_connection.query(query, &[])
            };

            if let Err(err) = query_result {
                last_was_failure = true;
                print_error_and_backoff(&mut backoff, err.to_string());
                try_initialize(&postgres_connection);
            }
            if !last_was_failure {
                backoff = get_baseline_backoff();
            }
        }
    });

    let mut count = 0;
    loop {
        if let Err(err) = prometheus::push_metrics(
            "mz_client_peek",
            HashMap::new(),
            &config.pushgateway_url,
            prometheus::gather(),
            None,
        ) {
            error!("Error pushing metrics: {}", err.to_string())
        } else {
            count += 1;
        }
        if count % 60 == 0 {
            info!("pushed metrics {} times", count);
        }
        thread::sleep(Duration::from_secs(1));
    }
}

fn get_baseline_backoff() -> Duration {
    Duration::from_secs(1)
}

fn create_postgres_connection(config: &Config) -> Connection {
    let mut backoff = get_baseline_backoff();
    loop {
        match postgres::Connection::connect(&*config.materialized_url, postgres::TlsMode::None) {
            Ok(connection) => return connection,
            Err(err) => print_error_and_backoff(&mut backoff, err.to_string()),
        }
    }
}

fn print_error_and_backoff(backoff: &mut Duration, error_message: String) {
    let current_backoff = min(*backoff, MAX_BACKOFF);
    println!(
        "{}. Sleeping for {:#?} seconds.\n",
        error_message, current_backoff
    );
    thread::sleep(current_backoff);
    *backoff = Duration::from_secs(backoff.as_secs() * 2);
}

fn create_histogram(query: &str) -> Histogram {
    let hist_vec = prometheus::register_histogram_vec!(
        "mz_client_peek_seconds",
        "how long peeks took",
        &["query"],
        vec![
            0.000_250, 0.000_500, 0.001, 0.002, 0.004, 0.008, 0.016, 0.034, 0.067, 0.120, 0.250,
            0.500, 1.0
        ]
    )
    .expect("can create histogram");
    hist_vec.with_label_values(&[query])
}

/// Try to build the views and sources that are needed for this script
///
/// This ignores errors (just logging them), and can just be run multiple times.
fn try_initialize(postgres_connection: &Connection) {
    match postgres_connection.execute(
        "CREATE SOURCES LIKE 'mysql.tpcch.%' \
         FROM 'kafka://kafka:9092' \
         USING SCHEMA REGISTRY 'http://schema-registry:8081'",
        &[],
    ) {
        Ok(_) => info!("Created sources"),
        Err(err) => error!("IGNORING CREATE VIEW error: {}", err),
    }
    match postgres_connection.execute(
        "CREATE VIEW q01 as SELECT
                 ol_number,
                 sum(ol_quantity) as sum_qty,
                 sum(ol_amount) as sum_amount,
                 avg(ol_quantity) as avg_qty,
                 avg(ol_amount) as avg_amount,
                 count(*) as count_order
         FROM
                 mysql_tpcch_orderline
         WHERE
                 ol_delivery_d > date '1998-12-01'
         GROUP BY
                 ol_number;",
        &[],
    ) {
        Ok(_) => info!("created view q01"),
        Err(err) => error!("IGNORING CREATE VIEW error: {}", err),
    }
}
