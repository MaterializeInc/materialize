// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Peek materialized views as fast as possible

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

use crate::args::{Args, Query, Source};

mod args;

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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    ore::panic::set_abort_on_panic();

    LogBuilder::from_env(Env::new().filter_or("MZ_LOG", "info"))
        .target(Target::Stdout)
        .init();
    info!("startup {}", Utc::now());

    let config = Args::from_cli()?;

    // Launch metrics server.
    let runtime = tokio::runtime::Runtime::new().expect("creating tokio runtime failed");
    runtime.spawn(serve_metrics());

    // Start peek timing loop. This should never return.
    measure_peek_times(&config);
    unreachable!()
}

fn measure_peek_times(config: &Args) {
    let mut postgres_client = create_postgres_client(&config.materialized_url);
    initialize_sources(&mut postgres_client, &config.config.sources)
        .expect("need to have sources for anything else to work");
    let mut peek_threads = vec![];
    for query in &config.config.queries {
        try_initialize(&mut postgres_client, query);
        peek_threads.push(spawn_query_thread(
            config.materialized_url.clone(),
            query.clone(),
        ));
    }

    info!("started {} peek threads", peek_threads.len());
    for pthread in peek_threads {
        pthread.join().unwrap();
    }
}

fn spawn_query_thread(mz_url: String, query: Query) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let query_name = &query.name;
        let mut postgres_client = create_postgres_client(&mz_url);
        let select = format!("SELECT * FROM {};", query_name);
        let histogram = HISTOGRAM_UNLABELED.with_label_values(&[&query_name]);
        let error_count = ERRORS_UNLABELED.with_label_values(&[&query_name]);
        let mut backoff = get_baseline_backoff();
        let mut last_was_failure = false;
        let mut counter = 0u64;
        let mut last_log = std::time::Instant::now();
        loop {
            let timer = prometheus::Histogram::start_timer(&histogram);
            let query_result = postgres_client.query(&*select, &[]);

            match query_result {
                Ok(_) => drop(timer),
                Err(err) => {
                    timer.stop_and_discard();
                    error_count.inc();
                    last_was_failure = true;
                    print_error_and_backoff(&mut backoff, &query.name, err.to_string());
                    try_initialize(&mut postgres_client, &query);
                }
            }
            counter += 1;
            if counter % 10 == 0 {
                let now = std::time::Instant::now();
                // log at most once per minute per thread
                if now.duration_since(last_log) > Duration::from_secs(60) {
                    info!("peeked {} {} times", query_name, counter);
                    last_log = now;
                }
            }
            if !last_was_failure {
                backoff = get_baseline_backoff();
            }
        }
    })
}

fn get_baseline_backoff() -> Duration {
    Duration::from_millis(250)
}

fn create_postgres_client(mz_url: &str) -> Client {
    let mut backoff = get_baseline_backoff();
    loop {
        match Client::connect(&mz_url, postgres::NoTls) {
            Ok(client) => return client,
            Err(err) => print_error_and_backoff(&mut backoff, "client creation", err.to_string()),
        }
    }
}

fn print_error_and_backoff(backoff: &mut Duration, context: &str, error_message: String) {
    warn!(
        "for {}: {}. Sleeping for {:#?} seconds",
        context, error_message, *backoff
    );
    thread::sleep(*backoff);
    *backoff = min(*backoff * 2, MAX_BACKOFF);
}

fn initialize_sources(
    client: &mut Client,
    sources: &[Source],
) -> Result<(), Box<dyn std::error::Error>> {
    let mut failed = false;
    for source in sources {
        let mut still_to_try = source.names.clone();
        for _ in 0..10 {
            let this_time = still_to_try.clone();
            still_to_try.clear();
            for name in this_time {
                let create_source = format!(
                    r#"CREATE SOURCE IF NOT EXISTS "{name}"
                     FROM KAFKA BROKER '{broker}' TOPIC '{prefix}{name}'
                     FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '{registry}'
                     ENVELOPE DEBEZIUM"#,
                    name = name,
                    broker = source.kafka_broker,
                    prefix = source.topic_namespace,
                    registry = source.schema_registry
                );
                match client.batch_execute(&create_source) {
                    Ok(_) => info!(
                        "installed source {} for topic {}{}",
                        name, source.topic_namespace, name
                    ),
                    Err(err) => {
                        warn!("error trying to create source {}: {}", name, err);
                        still_to_try.push(name)
                    }
                }
            }
            if still_to_try.is_empty() {
                return Ok(());
            } else {
                std::thread::sleep(Duration::from_secs(3));
            }
        }
        if !still_to_try.is_empty() {
            warn!(
                "Some sources were not successfully created! {:?}",
                still_to_try
            );
            failed = true;
        }
    }
    if failed {
        Err("Some sources were not created".into())
    } else {
        Ok(())
    }
}

/// Try to build the views and sources that are needed for this script
///
/// This ignores errors (just logging them), and can just be run multiple times.
fn try_initialize(client: &mut Client, query: &Query) {
    if !query.enabled {
        info!("skipping disabled query {}", query.name);
        return;
    }
    match client.batch_execute(&format!(
        "CREATE MATERIALIZED VIEW {} AS {}",
        query.name, query.query
    )) {
        Ok(_) => info!("view {} is installed", query.name),
        Err(err) => warn!("error trying to create view {}: {}", query.name, err),
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
