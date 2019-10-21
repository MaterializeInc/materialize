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
use std::process::Command;
use std::sync::mpsc::{self, Receiver};
use std::thread;

use chrono::Utc;
use postgres::{Client, NoTls};
use prometheus::Histogram;
use std::time::{Duration, Instant};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + 'static>>;

fn main() {
    println!("startup {}", Utc::now());
    measure_peek_times();
}

fn measure_peek_times() -> ! {
    init();
    let (sender, receiver) = mpsc::channel();
    let mut postgres_client = create_postgres_client();

    let query = "SELECT COUNT(*) FROM q01;";
    thread::spawn(move || {
        let mut sleep_duration = Duration::from_secs(1);
        loop {
            let start = Instant::now();
            let query_result = postgres_client.simple_query(query);
            let query_duration = start.elapsed().as_millis();
            match query_result {
                Ok(_rows) => sender.send(query_duration).unwrap(),
                Err(err) => {
                    println!("Hit error trying to run metrics query. Sleeping for {:#?} seconds and trying again. Error: {:#?}", sleep_duration, err);
                    thread::sleep(sleep_duration);
                    sleep_duration *= 2;
                    init();
                }
            }
        }
    });
    listen_and_push_metrics(receiver, query);
}

fn create_postgres_client() -> Client {
    let mut sleep_duration = Duration::from_secs(1);
    loop {
        match postgres::config::Config::new()
            .host("materialized")
            .port(6875)
            .dbname("tpcch")
            .connect(NoTls)
        {
            Ok(client) => return client,
            Err(err) => {
                println!("Hit error creating postgres client. Sleeping for {:#?} seconds and trying again. Error: {:#?}", sleep_duration, err);
                thread::sleep(sleep_duration);
                sleep_duration *= 2;
                init();
            }
        }
    }
}

fn listen_and_push_metrics(receiver: Receiver<u128>, query: &str) -> ! {
    let address = String::from("http://pushgateway:9091");
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
    let hist = hist_vec.with_label_values(&[query]);

    let mut count = 0;
    loop {
        if let Err(e) = push_metrics(&receiver, &hist, &mut count, &address) {
            println!("error pushing metrics: {}", e);
        }
    }
}

fn push_metrics(
    receiver: &Receiver<u128>,
    hist: &Histogram,
    count: &mut usize,
    address: &str,
) -> Result<()> {
    let psql_output = receiver.recv()?;
    hist.observe(psql_output as f64); // why?
    *count += 1;
    if *count % 10 == 0 {
        //
        match prometheus::push_metrics(
            "mz_client_peek",
            HashMap::new(),
            &address,
            prometheus::gather(),
            None,
        ) {
            Ok(_ok) => {
                // do nothing.
            }
            Err(_err) => {
                // todo: merge change and report actual errors!
                // Ignore noisy errors from: https://github.com/pingcap/rust-prometheus/issues/287
            }
        }
    }
    Ok(())
}

fn init() {
    run_psql_command_ignore_errors(
        "CREATE SOURCES LIKE 'mysql.tpcch.%'
         FROM 'kafka://kafka:9092'
         USING SCHEMA REGISTRY 'http://schema-registry:8081';",
    );
    run_psql_command_ignore_errors(
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
    );
}

fn run_psql_command_ignore_errors(cmd: &str) {
    let result = Command::new("psql")
        .arg("-q")
        .arg("-h")
        .arg("materialized")
        .arg("-p")
        .arg("6875")
        .arg("sslmode=disable")
        .arg("-c")
        .arg("\\timing")
        .arg("-c")
        .arg(cmd)
        .output();
    match result {
        Ok(out) => println!(
            "mz> {}\n\
             out| {}\n\
             err| {}",
            cmd,
            String::from_utf8(out.stdout).unwrap(),
            String::from_utf8(out.stderr).unwrap()
        ),
        Err(e) => println!("ERROR mz> {}\n| {}", cmd, e),
    }
}
