// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc..

// the prometheus macros (e.g. `register*`) all depend on each other, including on
// internal `__register*` macros, instead of doing the right thing and I assume using
// something like `$crate::__register_*`. That means that without using a macro_use here,
// we would end up needing to import several internal macros everywhere we want to use
// any of the prometheus macros.
#[macro_use]
extern crate prometheus;

use std::collections::HashMap;
use std::thread;

use chrono::Utc;
use postgres::Connection;
use prometheus::Histogram;
use std::cmp::min;
use std::time::Duration;

static MAX_BACKOFF: Duration = Duration::from_secs(60);

#[derive(Debug)]
struct Config {
    pushgateway_url: String,
    materialized_url: String,
}

fn main() -> Result<(), failure::Error> {
    println!("startup {}", Utc::now());

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
    create_view_ignore_errors(&postgres_connection);

    thread::spawn(move || {
        let query = "SELECT * FROM q01;";
        let histogram = create_histogram(query);
        let mut backoff = get_baseline_backoff();
        loop {
            let query_result = {
                // Drop is observe for prometheus::Histogram
                let _timer = prometheus::Histogram::start_timer(&histogram);
                postgres_connection.query(query, &[])
            };

            if let Err(err) = query_result {
                print_error_and_backoff(&mut backoff, err.to_string());
                create_view_ignore_errors(&postgres_connection);
            }
        }
    });

    loop {
        if let Err(err) = prometheus::push_metrics(
            "mz_client_peek",
            HashMap::new(),
            &config.pushgateway_url,
            prometheus::gather(),
            None,
        ) {
            println!("Error pushing metrics: {}", err.to_string())
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
    let hist_vec = register_histogram_vec!(
        "mz_client_peek_seconds",
        "how long peeks took",
        &["query"],
        vec![
            0.001, 0.003, 0.005, 0.01, 0.015, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.1,
            0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.5, 2.0, 2.5,
            3.0, 3.5, 4.0, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0,
            45.0, 50.0, 55.0, 60.0, 70.0, 80.0, 90.0, 120.0, 150.0, 180.0, 210.0, 240.0, 270.0,
            300.0
        ]
    )
    .expect("can create histogram");
    hist_vec.with_label_values(&[query])
}

fn create_view_ignore_errors(postgres_connection: &Connection) {
    if let Err(err) = postgres_connection.execute(
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
        println!("IGNORING CREATE VIEW error: {}", err)
    }
}
