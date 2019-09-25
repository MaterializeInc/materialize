// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc..
use std::process::Command;

fn main() {
    measure_peek_times();
}

fn measure_peek_times() {
    let mut latencies = Vec::new();
    let mut running = true;
    while running {
        let output = Command::new("psql") // use postgres rust package, look at sql logic test
            .arg("-q")
            .arg("-h")
            .arg("materialized")
            .arg("-p")
            .arg("6875")
            .arg("sslmode=disable")
            .arg("-c")
            .arg("\\timing")
            .arg("-c")
            .arg("\"PEEK q01\"") // parameterize this later
            .output();

        match output {
            Ok(latency) => latencies.push(latency),
            Err(error) => {
                println!("Hit error: {:#?}", error);
                running = false;
            }
        }
    }
    println!("Latencies are: {:#?}", latencies);
}
