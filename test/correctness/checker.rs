// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// Checker runs a set of consistency checks as materialized views. It spawns a separate
/// thread per check. It will record how many times a check failed vs succeeded, along with
/// the incorrect value.
use std::ascii;
use std::cmp::min;
use std::collections::HashMap;
use std::str;
use std::time::{Duration, Instant};

use chrono::Utc;
use env_logger::{Builder as LogBuilder, Env, Target};
use log::{debug, error, info, warn};
use pgrepr::{Interval, Numeric};
use postgres::{Client, Row};
use postgres_types::Type;
use std::thread::{self, JoinHandle};

use crate::args::{Args, Check, Source};

mod args;

type Error = Box<dyn std::error::Error + Send + Sync>;

static MAX_BACKOFF: Duration = Duration::from_secs(2);

fn main() -> Result<(), Error> {
    LogBuilder::from_env(Env::new().filter_or("MZ_LOG", "info"))
        .target(Target::Stdout)
        .init();

    let config = Args::from_cli()?;
    info!("startup {}", Utc::now());

    match initialize(&config) {
        Ok(()) => {
            run_checks(&config);
            info!("Finished running checker. Exiting ...");
            Ok(())
        }
        Err(e) => {
            error!(
                "Could not initialize correctness checker. Exiting with error: {}",
                e
            );
            Err(e)
        }
    }
}

fn run_checks(config: &Args) {
    let mut peek_threads = vec![];
    for c in &config.config.checks {
        peek_threads.extend(spawn_query_thread(
            config.mz_url.clone(),
            c.clone(),
            config.duration,
        ))
    }
    info!(
        "started {} peek threads for {} checks",
        peek_threads.len(),
        &config.config.checks.len()
    );
    for pthread in peek_threads {
        pthread.join().unwrap();
    }
}

/// This method iterates over all checks and removes the first row that matches
/// the check, as well as the check.
/// The result is considered valid when
/// all checks have identified a row which matches the outlined equality
/// Given that we have no guarantee on the order in which rows are returned in the database,
/// checks are considered valid if one row matches the check. That row is then removed. This function
/// assumes that checks are already sorted by specificity
/// (number of columns that the check applies to). Once a row is found that satisfies the check, we remove the row from
/// the result vector. Once all row-checks within a check have passed, and there are no more rows in the result set
/// return true, otherwise, return false
/// TODO(natacha): this requires knowing the number of rows returned a priori. We should expand the correctness
/// to support unknown number of rows
fn validate_result(check: &Check, rows: &[Row]) -> bool {
    let mut decoded_rows: Vec<Result<HashMap<String, String>, String>> =
        rows.iter().map(decode).collect();
    for row_check in &check.rows {
        // Identify a row that matches the check. Iterate over
        // all decoded rows and return the index of the first matching row
        let matching = decoded_rows.iter().position(|row| {
            match row {
                Ok(row) => {
                    // Check that all columns specified in the check
                    // have the expected value in the decoded row
                    for col in &row_check.columns {
                        let result = row.get(&col.column).unwrap();
                        if *result != col.value {
                            return false;
                        }
                    }
                    // If all match, then return row index
                    true
                }
                Err(e) => {
                    error!("{}", e);
                    false
                }
            }
        });

        // Remove item that we found (if any)
        if let Some(matched) = matching {
            decoded_rows.remove(matched).expect("Row must exist");
        } else {
            // No row matched our check, the validation should fail
            return false;
        }
    }

    // If there are any rows left in checked, the validation should fail
    decoded_rows.len() == 0
}

fn decode(row: &Row) -> Result<HashMap<String, String>, String> {
    let mut out = HashMap::new();
    for (i, col) in row.columns().iter().enumerate() {
        let ty = col.type_();
        let conversion = match *ty {
            Type::BOOL => row.get::<_, Option<bool>>(i).map(|x| x.to_string()),
            Type::CHAR | Type::TEXT => row.get::<_, Option<String>>(i),
            Type::BYTEA => row.get::<_, Option<Vec<u8>>>(i).map(|x| {
                let s = x.into_iter().map(ascii::escape_default).flatten().collect();
                String::from_utf8(s).unwrap()
            }),
            Type::INT4 => row.get::<_, Option<i32>>(i).map(|x| x.to_string()),
            Type::INT8 => row.get::<_, Option<i64>>(i).map(|x| x.to_string()),
            Type::NUMERIC => row.get::<_, Option<Numeric>>(i).map(|x| x.to_string()),
            Type::TIMESTAMP => row
                .get::<_, Option<chrono::NaiveDateTime>>(i)
                .map(|x| x.to_string()),
            Type::TIMESTAMPTZ => row
                .get::<_, Option<chrono::DateTime<chrono::Utc>>>(i)
                .map(|x| x.to_string()),
            Type::DATE => row
                .get::<_, Option<chrono::NaiveDate>>(i)
                .map(|x| x.to_string()),
            Type::INTERVAL => row.get::<_, Option<Interval>>(i).map(|x| x.to_string()),
            _ => return Err(format!("unable to handle SQL type: {:?}", ty)),
        }
        .unwrap_or_else(|| "<null>".into());
        out.insert(String::from(col.name()), conversion);
    }
    Ok(out)
}

fn spawn_query_thread(mz_url: String, check: Check, run_duration: Duration) -> Vec<JoinHandle<()>> {
    let mut cs = vec![];
    for _ in 0..check.thread_count.unwrap_or(1) {
        let check = check.clone();
        let mut backoff = get_baseline_backoff();
        let mz_url = mz_url.clone();
        cs.push(thread::spawn(  move ||  {
            let mut postgres_client =
            create_postgres_client(&mz_url);
            let mut last_was_failure = false;
            let mut total_count = 0u64;
            let mut err_count = 0u64;
            let mut mismatch_count = 0u64;
            let mut correct_count = 0u64;
            let mut last_log = Instant::now();
            let start_thread_time = Instant::now();
            loop {
                let query = postgres_client.prepare(&format!("SELECT * FROM {}", check.name)).expect("should be able to prepare a query");
                let query_result = postgres_client.query(&query, &[]);

                total_count += 1;
                match query_result {
                    Ok(rows) => {
                        if !validate_result(&check, &rows) {
                            mismatch_count += 1;
                            info!("Failed to validate check: {}", check.name);
                            let decoded_rows: Vec<Result<HashMap<String, String>, String>> =
                                rows.iter().map(decode).collect();
                            info!("Output");
                            for row in decoded_rows {
                                let res = row.unwrap();
                                for (key,value) in res {
                                    info!("{}:{}", key, value);
                                }
                                info!("----");
                            }
                        } else {
                            correct_count +=1 ;
                        }
                    }
                    Err(err) => {
                        err_count += 1;
                        last_was_failure = true;
                        print_error_and_backoff(&mut backoff, &check.name, err.to_string());
                        try_initialize(&mut postgres_client, &check);
                    }
                }
                if !last_was_failure {
                    backoff = get_baseline_backoff();
                }
                    let now = Instant::now();
                    // log at most once per minute per thread
                    if now.duration_since(last_log) > Duration::from_secs(1) {
                        info!(
                            "peeked {} {} times with {} errors, {} correctness violations, {} successes",
                            check.name, total_count, err_count, mismatch_count, correct_count
                        );
                        last_log = now;
                    }

                    if now.duration_since(start_thread_time) > run_duration {
                    info!(
                        "peeked {} {} times with {} errors, {} correctness violations, {} successes",
                        check.name, total_count, err_count, mismatch_count, correct_count
                    );
                        break;
                    }

                }
        }))
    }
    cs
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

fn get_baseline_backoff() -> Duration {
    Duration::from_millis(250)
}

fn print_error_and_backoff(backoff: &mut Duration, context: &str, error_message: String) {
    warn!(
        "for {}: {}. Sleeping for {:#?}",
        context, error_message, *backoff
    );
    std::thread::sleep(*backoff);
    *backoff = min(*backoff * 2, MAX_BACKOFF);
}

fn initialize(config: &Args) -> Result<(), Error> {
    let mut counter = 6;
    let mut init_result = init_inner(&config);
    while let Err(e) = init_result {
        counter -= 1;
        if counter <= 0 {
            return Err(format!("unable to initialize: {}", e).into());
        }
        warn!("init error, retry in 10 seconds ({} remaining)", counter);
        std::thread::sleep(Duration::from_secs(10));
        init_result = init_inner(&config);
    }
    Ok(())
}

fn init_inner(config: &Args) -> Result<(), Error> {
    let mut postgres_client = create_postgres_client(&config.mz_url);
    if config.initialize_sources {
        initialize_sources(&mut postgres_client, &config.config.sources)
            .map_err(|e| format!("need to have sources for anything else to work: {}", e))?;
    }
    let mut errors = 0;
    for check in &config.config.checks {
        println!("Initialize Check: {}", check.name);
        if !try_initialize(&mut postgres_client, &check) {
            errors += 1;
        }
    }
    if errors == 0 {
        Ok(())
    } else {
        Err(format!("There were {} errors initializing checks", errors).into())
    }
}

fn initialize_sources(client: &mut Client, sources: &[Source]) -> Result<(), Error> {
    let mut failed = false;
    for source in sources {
        let mut still_to_try = source.names.clone();
        let materialized = if source.materialized {
            "MATERIALIZED "
        } else {
            ""
        };
        let consistency = if source.is_byo {
            " with (consistency = '".to_owned() + &source.consistency_topic.clone() + "') "
        } else {
            " ".to_owned()
        };
        for _ in 0..10 {
            let this_time = still_to_try.clone();
            still_to_try.clear();
            for name in this_time {
                let create_source = format!(
                    r#"CREATE {materialized} SOURCE IF NOT EXISTS "{name}"
                     FROM KAFKA BROKER '{broker}' TOPIC '{prefix}{name}'
                     {consistency}
                     FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '{registry}'
                     ENVELOPE DEBEZIUM"#,
                    name = name,
                    broker = source.kafka_broker,
                    prefix = source.topic_namespace,
                    registry = source.schema_registry,
                    materialized = materialized,
                    consistency = consistency,
                );
                match client.execute(&create_source[..], &[]) {
                    Ok(_) => info!(
                        "installed source {} for topic {}{}",
                        name, source.topic_namespace, name
                    ),
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
/// This ignores errors (just logging them), and can just be run multiple times.
///
/// # Returns Success: `true` if everything succeed
fn try_initialize(client: &mut Client, check: &Check) -> bool {
    let mut success = true;
    if !check.enabled {
        info!("skipping disabled query group {}", check.name);
        return success;
    }
    let mz_result = client.batch_execute(&format!(
        "CREATE MATERIALIZED VIEW {} AS {}",
        check.name, check.query
    ));
    match mz_result {
        Ok(_) => info!("installed view {}", check.name),
        Err(err) => {
            let errmsg = err.to_string();
            if !errmsg.ends_with("already exists") {
                success = false;
                warn!("error trying to create view {}: {}", check.name, err);
            } else {
                // this only matters for timeline debugging, in general it is fine
                debug!("view previously installed: {} err={}", check.name, err);
            }
        }
    }
    success
}
