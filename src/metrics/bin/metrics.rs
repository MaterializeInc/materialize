// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc..
#[macro_use]
extern crate prometheus;

use regex::Regex;
use std::process::Command;

fn main() {
    measure_peek_times();
}

fn measure_peek_times() {
    let re = Regex::new(r"Time: (\d*).(\d*) ms").unwrap();
    let address = String::from("http://pushgateway:9091");
    loop {
        // TODO@jldlaughlin: use rust postgres package instead, look at sql logic test
        let output = Command::new("psql")
            .arg("-q")
            .arg("-h")
            .arg("materialized")
            .arg("-p")
            .arg("6875")
            .arg("sslmode=disable")
            .arg("-c")
            .arg("\\timing")
            .arg("-c")
            .arg("PEEK q01") // TODO@jldlaughlin: parameterize the SQL command?
            .output();

        match output {
            Ok(psql_output) => {
                if let Some(matched) = re.captures(String::from_utf8(psql_output.stdout).unwrap().as_ref()) {
                    let metric_families = prometheus::gather();
                    prometheus::push_metrics(
                        "peek_q01",
                        labels! {"timing".to_owned() => format!("{}.{}", &matched[1], &matched[2]).to_owned(),},
                        &address,
                        metric_families,
                        None
                    ).unwrap_or_else(|err| println!("Hit error trying to send to pushgateway: {:#?}", err));
                }
            }
            Err(error) => {
                println!("Hit error: {:#?}", error);
            }
        }
    }
}
