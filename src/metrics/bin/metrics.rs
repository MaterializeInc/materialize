// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc..
use regex::Regex;
use std::process::Command;

fn main() {
    measure_peek_times();
}

fn measure_peek_times() {
    let re = Regex::new(r"Time: (\d*).(\d*) ms").unwrap();
    loop {
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
            .arg("PEEK q01") // TODO@jldlaughlin: parameterize the SQL command
            .output();

        match output {
            Ok(psql_output) => {
                match re.captures(String::from_utf8(psql_output.stdout).unwrap().as_ref()) {
                    // TODO@jldlaughlin: send times to Prometheus.
                    Some(matched) => println!("Found time: {}.{}", &matched[1], &matched[2]),
                    None => println!("Didn't find a time!"),
                }
            }
            Err(error) => {
                println!("Hit error: {:#?}", error);
            }
        }
    }
}
