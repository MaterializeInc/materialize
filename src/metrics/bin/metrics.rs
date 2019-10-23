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
use std::time::Duration;

static MAX_BACKOFF: Duration = Duration::from_secs(60);

fn main() {
    println!("startup {}", Utc::now());
    measure_peek_times();
}

fn measure_peek_times() -> ! {
    let postgres_connection = create_postgres_connection();
    init_ignore_errors(&postgres_connection);

    thread::spawn(move || {
        let query = "SELECT COUNT(*) FROM q01;";
        let histogram = create_histogram(query);
        let mut backoff = get_baseline_backoff();
        loop {
            let query_result = {
                // Drop is observe for prometheus::Histogram
                let _timer = prometheus::Histogram::start_timer(&histogram);
                postgres_connection.query(query, &[])
            };

            if let Err(err) = query_result {
                backoff_or_panic(
                    &mut backoff,
                    err.to_string(),
                    format!(
                        "Hit error running metrics query on multiple attempts: {}",
                        err.to_string()
                    ),
                );
                init_ignore_errors(&postgres_connection);
            }
        }
    });

    let address = "http://pushgateway:9091";
    let mut count = 0;
    loop {
        count += 1;
        if count % 10 == 0 {
            if let Err(err) = prometheus::push_metrics(
                "mz_client_peek",
                HashMap::new(),
                &address,
                prometheus::gather(),
                None,
            ) {
                println!("Error pushing metrics: {}", err.to_string())
            }
        }
        thread::sleep(Duration::from_secs(1));
    }
}

fn get_baseline_backoff() -> Duration {
    Duration::from_secs(1)
}

fn create_postgres_connection() -> Connection {
    let mut backoff = Duration::from_secs(1);
    loop {
        match postgres::Connection::connect(
            "postgres://ignoreuser@materialized:6875/tpcch",
            postgres::TlsMode::None,
        ) {
            Ok(connection) => return connection,
            Err(err) => backoff_or_panic(
                &mut backoff,
                err.to_string(),
                format!(
                    "Unable to create postgres client after multiple attempts: {}",
                    err.to_string()
                ),
            ),
        }
    }
}

fn backoff_or_panic(backoff: &mut Duration, error_message: String, panic_message: String) {
    if *backoff < MAX_BACKOFF {
        println!("{}. Sleeping for {:#?} seconds.", error_message, *backoff);
        thread::sleep(*backoff);
        *backoff = Duration::from_secs(backoff.as_secs() * 2);
    } else {
        panic!("Exceeded MAX_BACKOFF. {}", panic_message);
    }
}

fn create_histogram(query: &str) -> Histogram {
    let hist_vec = register_histogram_vec!(
        "mz_client_peek_millis",
        "how long peeks took",
        &["query"],
        vec![
            1.0, 3.0, 5.0, 10.0, 15.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0,
            150.0, 200.0, 250.0, 300.0, 350.0, 400.0, 450.0, 500.0, 600.0, 700.0, 800.0, 900.0,
            1000.0, 1500.0, 2000.0, 2500.0, 3000.0, 3500.0, 4000.0, 4500.0, 5000.0, 6000.0, 7000.0,
            8000.0, 9000.0, 10_000.0, 15_000.0, 20_000.0, 25_000.0, 30_000.0, 35_000.0, 40_000.0,
            45_000.0, 50_000.0, 55_000.0, 60_000.0, 70_000.0, 80_000.0, 90_000.0, 120_000.0,
            150_000.0, 180_000.0, 210_000.0, 240_000.0, 270_000.0, 300_000.0
        ]
    )
    .expect("can create histogram");
    hist_vec.with_label_values(&[query])
}

fn init_ignore_errors(postgres_connection: &Connection) {
    if let Err(err) = postgres_connection.execute(
        "CREATE SOURCES LIKE 'mysql.tpcch.%' FROM 'kafka://kafka:9092' USING SCHEMA REGISTRY 'http://schema-registry:8081';",
        &[],
    ) {
        println!(
            "IGNORING CREATE SOURCES error: {}",
            err.to_string()
        )
    }

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
        println!("IGNORING CREATE VIEW error: {}", err.to_string())
    }
}
