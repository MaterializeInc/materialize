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
use std::process;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use chrono::Utc;
use env_logger::{Builder as LogBuilder, Env, Target};
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Response, Server};
use lazy_static::lazy_static;
use log::{debug, error, info, warn};
use postgres::Client;
use prometheus::{register_histogram_vec, register_int_counter_vec};
use prometheus::{Encoder, HistogramVec, IntCounterVec};

use crate::args::{Args, Config, QueryGroup, Source};

mod args;

static MAX_BACKOFF: Duration = Duration::from_secs(2);
static METRICS_PORT: u16 = 16875;

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;

fn main() -> Result<()> {
    ore::panic::set_abort_on_panic();

    LogBuilder::from_env(Env::new().filter_or("MZ_LOG", "info"))
        .target(Target::Stdout)
        .init();

    mz_process_collector::register_default_process_collector()?;

    let args: Args = ore::cli::parse_args();

    if let Some(write_config) = args.write_config {
        args::write_config_supplied(args.config_file.as_deref(), &write_config);
        return Ok(());
    }

    let config = args::load_config(args.config_file.as_deref(), args.queries.as_deref())?;

    if args.help_config {
        args::print_config_supplied(config);
        return Ok(());
    }

    info!("startup {}", Utc::now());

    // Launch metrics server.
    let runtime = tokio::runtime::Runtime::new().expect("creating tokio runtime failed");
    runtime.spawn(serve_metrics());

    info!(
        "Allowing chbench to warm up for {} seconds at {}",
        args.warmup_seconds,
        Utc::now()
    );
    let warmup_dur = std::time::Duration::from_secs(args.warmup_seconds.into());
    std::thread::sleep(warmup_dur);
    info!("Done warming up at {}", Utc::now());

    let init_result = initialize(&args, &config);
    // Start peek timing loop.
    if args.only_initialize {
        match init_result {
            Ok(()) => Ok(()),
            Err(e) => {
                error!("{}", e);
                process::exit(1);
            }
        }
    } else {
        let _ = init_result;
        measure_peek_times(&args, &config);
        Ok(())
    }
}

fn measure_peek_times(args: &Args, config: &Config) {
    let mut peek_threads = vec![];
    let run_dur = chrono::Duration::seconds(args.run_seconds as i64);
    for (group_id, group) in config.groups.iter().enumerate() {
        peek_threads.extend(spawn_query_thread(
            args.materialized_url.clone(),
            group.clone(),
            group_id,
            run_dur.clone(),
        ));
    }

    info!(
        "started {} peek threads for {} queries",
        peek_threads.len(),
        config.query_count()
    );
    for pthread in peek_threads {
        pthread.join().unwrap();
    }
}

fn spawn_query_thread(
    mz_url: String,
    query_group: QueryGroup,
    group_id: usize,
    run_dur: chrono::Duration,
) -> Vec<JoinHandle<()>> {
    let qg = query_group;
    let mut qs = vec![];
    for thread_id in 0..qg.thread_count {
        let group = qg.clone();
        let mz_url = mz_url.clone();
        qs.push(thread::spawn(move || {
            let mut postgres_client = create_postgres_client(&mz_url);
            let selects = group
                .queries
                .iter()
                .map(|q| {
                    let stmt = postgres_client
                        .prepare(&format!("SELECT * FROM {}", q.name))
                        .expect("should be able to prepare a query");
                    let hist = HISTOGRAM_UNLABELED.with_label_values(&[&q.name]);
                    let rows_counter = ROWS_UNLABELED.with_label_values(&[&q.name]);
                    (stmt, q.name.clone(), hist, rows_counter)
                })
                .collect::<Vec<_>>();
            let mut backoff = get_baseline_backoff();
            let mut last_was_failure = false;
            let mut counter = 0u64;
            let mut err_count = 0u64;
            let mut last_log = Instant::now();
            let end_at = if run_dur > chrono::Duration::seconds(0) {
                Utc::now() + run_dur
            } else {
                chrono::MAX_DATETIME
            };
            info!(
                "Will terminate thread {}.{} at {:?}",
                group_id, thread_id, end_at
            );
            'main: loop {
                for (select, q_name, hist, rows_counter) in &selects {
                    let timer = hist.start_timer();
                    let query_result = postgres_client.query(select, &[]);

                    match query_result {
                        Ok(r) => {
                            drop(timer);
                            counter += 1;
                            rows_counter.inc_by(r.len() as i64)
                        }
                        Err(err) => {
                            timer.stop_and_discard();
                            err_count += 1;
                            prom_error(&q_name);
                            last_was_failure = true;
                            print_error_and_backoff(&mut backoff, &q_name, err.to_string());
                            try_initialize(&mut postgres_client, &group);
                        }
                    }
                    if (err_count + counter) % 10 == 0 {
                        let now = Instant::now();
                        // log at most once per minute per thread
                        if now.duration_since(last_log) > Duration::from_secs(60) {
                            info!(
                                "peeked {} {} times with {} errors",
                                group.name, counter, err_count
                            );
                            last_log = now;
                        }
                    }
                    if Utc::now() >= end_at {
                        info!("Terminating thread {}.{}", group_id, thread_id);
                        break 'main;
                    }
                    if !last_was_failure {
                        backoff = get_baseline_backoff();
                    }
                    if group.sleep != Duration::from_millis(0) {
                        thread::sleep(group.sleep);
                    }
                }
            }
        }))
    }
    qs
}

// Initialization

fn create_postgres_client(mz_url: &str) -> Client {
    let mut backoff = get_baseline_backoff();
    loop {
        match Client::connect(&mz_url, postgres::NoTls) {
            Ok(client) => return client,
            Err(err) => print_error_and_backoff(&mut backoff, "client creation", err.to_string()),
        }
    }
}

fn initialize(args: &Args, config: &Config) -> Result<()> {
    let start = std::time::Instant::now();
    let mut init_result = init_inner(args, config);
    while let Err(e) = init_result {
        if start.elapsed() > args.init_timeout {
            return Err(format!("unable to initialize: {}", e).into());
        }
        warn!(
            "init error, retry in 10 seconds ({:?} remaining)",
            args.init_timeout - start.elapsed()
        );
        thread::sleep(Duration::from_secs(10));
        init_result = init_inner(args, config);
    }
    Ok(())
}

fn init_inner(args: &Args, config: &Config) -> Result<()> {
    let mut postgres_client = create_postgres_client(&args.materialized_url);
    initialize_sources(&mut postgres_client, &config.sources)
        .map_err(|e| format!("need to have sources for anything else to work: {}", e))?;
    let mut errors = 0;
    for group in config.queries_in_declaration_order() {
        if !try_initialize(&mut postgres_client, group) {
            errors += 1;
        }
    }
    if errors == 0 {
        Ok(())
    } else {
        Err(format!("There were {} errors initializing query groups", errors).into())
    }
}

fn initialize_sources(client: &mut Client, sources: &[Source]) -> Result<()> {
    let mut failed = false;
    for source in sources {
        let mut still_to_try = source.names.clone();
        let mut succeeded = Vec::new();
        let materialized = if source.materialized {
            "MATERIALIZED "
        } else {
            ""
        };
        let this_time = still_to_try.clone();
        still_to_try.clear();
        for name in this_time {
            let delete_source = format!(r#" DROP SOURCE {name} CASCADE"#, name = name);
            match client.batch_execute(&delete_source) {
                Ok(_) => info!("Deleted source in preparation for creation {}", name),
                Err(err) => {
                    debug!("error trying to delete source {}: {}", name, err);
                }
            }
            let create_source = format!(
                r#"CREATE {materialized} SOURCE IF NOT EXISTS "{name}"
                     FROM KAFKA BROKER '{broker}' TOPIC '{prefix}{name}'
                     FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '{registry}'
                     ENVELOPE DEBEZIUM"#,
                name = name,
                broker = source.kafka_broker,
                prefix = source.topic_namespace,
                registry = source.schema_registry,
                materialized = materialized,
            );
            match client.batch_execute(&create_source) {
                Ok(_) => {
                    info!(
                        "installed source {} for topic {}{}",
                        name, source.topic_namespace, name
                    );
                    succeeded.push(name)
                }
                Err(err) => {
                    warn!("error trying to create source {}: {}", name, err);
                    debug!("For query:\n                     {}", create_source);
                    still_to_try.push(name)
                }
            }
        }
        if still_to_try.is_empty() {
            return Ok(());
        } else {
            thread::sleep(Duration::from_secs(3));
        }
        if !still_to_try.is_empty() {
            warn!(
                "Some sources were not successfully created! created={:?} failed={:?}",
                succeeded, still_to_try
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
///
/// # Returns
///
/// Success: `true` if everything succeed
fn try_initialize(client: &mut Client, query_group: &QueryGroup) -> bool {
    let mut success = true;
    if !query_group.enabled {
        info!("skipping disabled query group {}", query_group.name);
        return success;
    }
    for query in &query_group.queries {
        let mz_result = client.batch_execute(&format!(
            "CREATE MATERIALIZED VIEW {} AS {}",
            query.name, query.query
        ));
        match mz_result {
            Ok(_) => info!("installed view {}", query.name),
            Err(err) => {
                let errmsg = err.to_string();
                if !errmsg.ends_with("already exists") {
                    success = false;
                    warn!("error trying to create view {}: {}", query.name, err);
                } else {
                    // this only matters for timeline debugging, in general it is fine
                    debug!("view previously installed: {} err={}", query.name, err);
                }
            }
        }
    }
    success
}

// prometheus items

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
    static ref ERRORS_UNLABELED: IntCounterVec = register_int_counter_vec!(
        "mz_client_error_count",
        "number of errors encountered",
        &["query"]
    )
    .expect("can create histogram");
    static ref ROWS_UNLABELED: IntCounterVec = register_int_counter_vec!(
        "mz_client_pg_rows_total",
        "number of rows received",
        &["query"]
    )
    .unwrap();
}

async fn serve_metrics() -> Result<()> {
    info!("serving prometheus metrics on port {}", METRICS_PORT);
    let addr = ([0, 0, 0, 0], METRICS_PORT).into();

    let make_service = make_service_fn(|_conn| async {
        Ok::<_, Infallible>(service_fn(|_req| async {
            let metrics = prometheus::gather();
            let encoder = prometheus::TextEncoder::new();
            let mut buffer = Vec::new();

            encoder
                .encode(&metrics, &mut buffer)
                .unwrap_or_else(|e| error!("error gathering metrics: {}", e));
            Ok::<_, Infallible>(Response::new(Body::from(buffer)))
        }))
    });
    Server::bind(&addr).serve(make_service).await?;
    Ok(())
}

// error helpers

fn prom_error(query_name: &str) {
    ERRORS_UNLABELED.with_label_values(&[&query_name]).inc()
}

fn get_baseline_backoff() -> Duration {
    Duration::from_millis(250)
}

fn print_error_and_backoff(backoff: &mut Duration, context: &str, error_message: String) {
    warn!(
        "for {}: {}. Sleeping for {:#?}",
        context, error_message, *backoff
    );
    if !error_message.contains("Evaluation error") {
        thread::sleep(*backoff);
        *backoff = min(*backoff * 2, MAX_BACKOFF);
    }
}
