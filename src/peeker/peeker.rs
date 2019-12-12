// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc..

use std::cmp::min;
use std::convert::Infallible;
use std::thread;
use std::time::Duration;

use chrono::Utc;
use env_logger::{Builder as LogBuilder, Env, Target};
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Response, Server};
use lazy_static::lazy_static;
use log::{error, info, warn};
use postgres::Connection;
use prometheus::{register_counter_vec, register_histogram_vec, CounterVec, Encoder, HistogramVec};

static MAX_BACKOFF: Duration = Duration::from_secs(60);
static METRICS_PORT: u16 = 16875;

lazy_static! {
    static ref HISTOGRAM_UNLABELED: HistogramVec = register_histogram_vec!(
        "mz_client_peek_seconds",
        "how long peeks took",
        &["query"],
        vec![
            0.000_250, 0.000_500, 0.001, 0.002, 0.004, 0.008, 0.016, 0.034, 0.067, 0.120, 0.250,
            0.500, 1.0
        ]
    )
    .expect("can create histogram");
    static ref ERRORS_UNLABELED: CounterVec = register_counter_vec!(
        "mz_client_error_count",
        "number of errors encountered",
        &["query"]
    )
    .expect("can create histogram");
}

#[derive(Debug)]
struct Config {
    materialized_url: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    LogBuilder::from_env(Env::new().filter_or("MZ_LOG", "info"))
        .target(Target::Stdout)
        .init();
    info!("startup {}", Utc::now());

    let args: Vec<_> = std::env::args().collect();

    let mut opts = getopts::Options::new();
    opts.optflag("h", "help", "show this usage information");
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
        materialized_url: popts.opt_get_default(
            "materialized-url",
            "postgres://ignoreuser@materialized:6875/tpcch".to_owned(),
        )?,
    };

    // these appear to need to be in this order for both the metrics server and the peek
    // client to start up
    let server = thread::spawn(|| serve_metrics().unwrap());
    let peek_fast = measure_peek_times(&config);
    peek_fast.join().unwrap();
    server.join().unwrap();
    Ok(())
}

fn measure_peek_times(config: &Config) -> thread::JoinHandle<()> {
    let postgres_connection = create_postgres_connection(config);
    try_initialize(&postgres_connection);

    thread::spawn(move || {
        let query = "SELECT * FROM q01;";
        let histogram = HISTOGRAM_UNLABELED.with_label_values(&[query]);
        let error_count = ERRORS_UNLABELED.with_label_values(&[query]);
        let mut backoff = get_baseline_backoff();
        let mut last_was_failure = false;
        loop {
            let timer = prometheus::Histogram::start_timer(&histogram);
            let query_result = postgres_connection.query(query, &[]);

            match query_result {
                Ok(_) => drop(timer),
                Err(err) => {
                    timer.stop_and_discard();
                    error_count.inc();
                    last_was_failure = true;
                    print_error_and_backoff(&mut backoff, err.to_string());
                    try_initialize(&postgres_connection);
                }
            }
            if !last_was_failure {
                backoff = get_baseline_backoff();
            }
        }
    })
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
    warn!(
        "{}. Sleeping for {:#?} seconds",
        error_message, current_backoff
    );
    thread::sleep(current_backoff);
    *backoff = Duration::from_secs(backoff.as_secs() * 2);
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
        Err(err) => warn!("trying to create sources: {}", err),
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
        Err(err) => warn!("trying to create view: {}", err),
    }
}

fn serve_metrics() -> Result<(), failure::Error> {
    info!("serving prometheus metrics on port {}", METRICS_PORT);
    let addr = ([0, 0, 0, 0], METRICS_PORT).into();

    let make_service = make_service_fn(|_conn| {
        async {
            Ok::<_, Infallible>(service_fn(|_req| {
                async {
                    let metrics = prometheus::gather();
                    let encoder = prometheus::TextEncoder::new();
                    let mut buffer = Vec::new();

                    encoder
                        .encode(&metrics, &mut buffer)
                        .unwrap_or_else(|e| error!("error gathering metrics: {}", e));
                    Ok::<_, Infallible>(Response::new(Body::from(buffer)))
                }
            }))
        }
    });
    let mut rt = tokio::runtime::Runtime::new()?;
    rt.block_on(Server::bind(&addr).serve(make_service))?;
    Ok(())
}
