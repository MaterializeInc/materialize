// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0..

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
use postgres::Client;
use prometheus::{register_counter_vec, register_histogram_vec, CounterVec, Encoder, HistogramVec};

static MAX_BACKOFF: Duration = Duration::from_secs(2);
static METRICS_PORT: u16 = 16875;

lazy_static! {
    static ref HISTOGRAM_UNLABELED: HistogramVec = register_histogram_vec!(
        "mz_client_peek_seconds",
        "how long peeks took",
        &["query"],
        vec![
            0.000_250, 0.000_500, 0.001, 0.002, 0.004, 0.008, 0.016, 0.034, 0.067, 0.120, 0.250,
            0.500, 1.0
        ],
        expose_decumulated => true
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
    ore::panic::set_abort_on_panic();

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
            "postgres://ignoreuser@materialized:6875/materialize".to_owned(),
        )?,
    };

    // Launch metrics server.
    let runtime = tokio::runtime::Runtime::new().expect("creating tokio runtime failed");
    runtime.spawn(serve_metrics());

    // Start peek timing loop. This should never return.
    measure_peek_times(&config);
    unreachable!()
}

fn measure_peek_times(config: &Config) {
    let mut postgres_client = create_postgres_client(&config);
    try_initialize(&mut postgres_client);

    let query = "SELECT * FROM q01;";
    let histogram = HISTOGRAM_UNLABELED.with_label_values(&[query]);
    let error_count = ERRORS_UNLABELED.with_label_values(&[query]);
    let mut backoff = get_baseline_backoff();
    let mut last_was_failure = false;
    loop {
        let timer = prometheus::Histogram::start_timer(&histogram);
        let query_result = postgres_client.query(query, &[]);

        match query_result {
            Ok(_) => drop(timer),
            Err(err) => {
                timer.stop_and_discard();
                error_count.inc();
                last_was_failure = true;
                print_error_and_backoff(&mut backoff, err.to_string());
                try_initialize(&mut postgres_client);
            }
        }
        if !last_was_failure {
            backoff = get_baseline_backoff();
        }
    }
}

fn get_baseline_backoff() -> Duration {
    Duration::from_millis(250)
}

fn create_postgres_client(config: &Config) -> Client {
    let mut backoff = get_baseline_backoff();
    loop {
        match Client::connect(&*config.materialized_url, postgres::NoTls) {
            Ok(client) => return client,
            Err(err) => print_error_and_backoff(&mut backoff, err.to_string()),
        }
    }
}

fn print_error_and_backoff(backoff: &mut Duration, error_message: String) {
    warn!("{}. Sleeping for {:#?} seconds", error_message, *backoff);
    thread::sleep(*backoff);
    *backoff = min(*backoff * 2, MAX_BACKOFF);
}

/// Try to build the views and sources that are needed for this script
///
/// This ignores errors (just logging them), and can just be run multiple times.
fn try_initialize(client: &mut Client) {
    match client.batch_execute(
        "CREATE SOURCE IF NOT EXISTS mysql_tpcch_orderline \
        FROM KAFKA BROKER 'kafka:9092' TOPIC 'mysql.tpcch.orderline' \
        FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://schema-registry:8081'
        ENVELOPE DEBEZIUM",
    ) {
        Ok(_) => info!("source is installed"),
        Err(err) => warn!("error trying to create sources: {}", err),
    }
    match client.batch_execute(
        "CREATE MATERIALIZED VIEW q01 AS
         SELECT
            ol_number,
            sum(ol_quantity) as sum_qty,
            sum(ol_amount) as sum_amount,
            avg(ol_quantity) as avg_qty,
            avg(ol_amount) as avg_amount,
            count(*) as count_order
         FROM orderline
         WHERE ol_delivery_d > date '1998-12-01'
         GROUP BY ol_number",
    ) {
        Ok(_) => info!("view q01 is installed"),
        Err(err) => warn!("error trying to create view: {}", err),
    }
}

async fn serve_metrics() -> Result<(), failure::Error> {
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
    Server::bind(&addr).serve(make_service).await?;
    Ok(())
}
