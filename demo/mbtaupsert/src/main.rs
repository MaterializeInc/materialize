// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use getopts::Options;
use parse_duration::parse;

use std::collections::hash_map::DefaultHasher;
use std::env;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::prelude::*;
use std::io::BufReader;
use std::io::SeekFrom;
use std::{thread, time};

mod kafka_interaction;

extern crate futures;
extern crate json;
extern crate rdkafka;
extern crate reqwest;

fn parse_entry(parsed_object: &mut json::JsonValue) -> (String, Option<String>) {
    assert!(parsed_object.is_object());
    parsed_object.remove("type");
    let key = parsed_object["id"].take_string().unwrap();
    parsed_object.remove("id");
    let value = if parsed_object.len() == 0 {
        None
    } else {
        Some(parsed_object.to_string())
    };
    (key, value)
}

fn parse_line(line: &str) -> Result<Vec<(String, Option<String>)>, String> {
    let mut results = Vec::new();
    if line.starts_with("data: ") {
        let mut parsed_json = json::parse(&line[6..]);
        if let Ok(parsed_json) = &mut parsed_json {
            if parsed_json.is_array() {
                for member in parsed_json.members_mut() {
                    results.push(parse_entry(member))
                }
            } else {
                results.push(parse_entry(parsed_json))
            }
        } else {
            return Err(format!("broken line: {}", line));
        }
    }
    Ok(results)
}

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

fn run_stream() -> Result<(), String> {
    let args: Vec<_> = env::args().collect();

    let mut opts = Options::new();
    opts.optopt("", "kafka-addr", "kafka bootstrap address", "HOST:PORT");
    opts.reqopt(
        "f",
        "file-name",
        "path to file where information is being logged",
        "ABSOLUTE_PATH",
    );
    opts.optflag("h", "help", "show this usage information");
    opts.reqopt("t", "topic-name", "name of topic to write to", "STRING");
    opts.optopt(
        "",
        "heartbeat",
        "wait time before checking logs for updates (default 250ms)",
        "DURATION",
    );
    opts.optopt(
        "p",
        "partitions",
        "number of partitions to write to",
        "POSITIVE_INTEGER",
    );
    opts.optopt(
        "r",
        "replication",
        "replication factor of topic",
        "POSITIVE_INTEGER",
    );
    opts.optmulti(
        "",
        "topic-property",
        "topic-level kafka config",
        "NAME=PROPERTY",
    );
    opts.optflag(
        "d",
        "disable-topic-create",
        "add this flag to disable topic creation",
    );
    opts.optflag("", "byo", "enable byo consistency");
    opts.optopt(
        "c",
        "consistency-name",
        "name of consistency topic to write to (default TOPIC_NAME-consistency topic)",
        "STRING",
    );
    opts.optflag(
        "e",
        "exit-at-end",
        "automatically exit when the end of the file is reached",
    );
    let usage_details = opts.usage("usage: mbtaupsert [options] FILE");
    let opts = opts
        .parse(&args[1..])
        .map_err(|e| format!("{}\n{}\n", usage_details, e))?;

    if opts.opt_present("h") {
        return Err(usage_details);
    }

    let config = kafka_interaction::Config {
        kafka_addr: opts.opt_str("kafka-addr"),
    };

    let heartbeat_spec = opts
        .opt_str("heartbeat")
        .unwrap_or_else(|| "250ms".to_string());
    let heartbeat = match parse(&heartbeat_spec) {
        Ok(x) => x,
        Err(err) => {
            return Err(err.to_string());
        }
    };

    let partitions = opts
        .opt_str("partitions")
        .unwrap_or_else(|| "1".to_string());
    let partitions = match partitions.parse::<i32>() {
        Ok(x) => x,
        Err(err) => {
            return Err(err.to_string());
        }
    };
    if partitions < 1 {
        return Err("partitions must be positive".to_string());
    }
    let replication = opts
        .opt_str("replication")
        .unwrap_or_else(|| "1".to_string());
    let replication = match replication.parse::<i32>() {
        Ok(x) => x,
        Err(err) => {
            return Err(err.to_string());
        }
    };
    if replication < 1 {
        return Err("replication must be positive".to_string());
    }

    let byo = opts.opt_present("byo");

    let mut state = kafka_interaction::State::new(config, byo)?;

    let topic_name = opts.opt_str("t").unwrap();
    let topic_configs: Result<Vec<_>, _> = opts
        .opt_strs("topic-property")
        .into_iter()
        .map(|property| {
            let mut split_iter = property.split('=');
            let property_name = split_iter.next();
            let property_value = split_iter.next();
            match property_value {
                Some(property_value) => {
                    Ok((property_name.unwrap().to_owned(), property_value.to_owned()))
                }
                None => Err(format!(
                    "Property {} has no argument",
                    property_name.unwrap()
                )),
            }
        })
        .collect();
    let topic_configs = topic_configs?;
    if !opts.opt_present("disable-topic-create") {
        state.create_topic(&topic_name, partitions, replication, topic_configs)?;
    }
    let consistency_name = opts
        .opt_str("c")
        .unwrap_or(format!("{}-data-consistency", topic_name));
    if byo && !opts.opt_present("disable-topic-create") {
        state.create_consistency_topic(&consistency_name, replication)?;
    }

    //read off of the stream file
    let filename = opts.opt_str("f").unwrap();
    let f = match File::open(filename) {
        Ok(x) => x,
        Err(err) => {
            return Err(err.to_string());
        }
    };

    // tracks position in the logs
    let mut pos = 0;
    let mut reader = BufReader::new(f);
    let mut timestamp = 0;
    // tracks bytes read in the previous iteration of the loop.
    // this is only used when there is an error parsing the line
    // sometimes, the log parser is overly eager and reads a line before
    // it has finished writing. so we want to retry reading the whole line
    // until number of bytes read from the line is no longer increasing
    let mut old_len = 0;

    let exit_at_end = opts.opt_present("exit-at-end");

    loop {
        let mut line = String::new();
        let resp = reader.read_line(&mut line);
        match resp {
            Ok(len) => {
                if len > 0 {
                    match parse_line(&line) {
                        Ok(key_values) => {
                            for (key, value) in key_values {
                                let mut partition_key = (calculate_hash(&key) as i32) % partitions;
                                if partition_key < 0 {
                                    partition_key += partitions;
                                }
                                state.ingest(&topic_name, partition_key, Some(key), value)?;
                                if byo {
                                    state.ingest_consistency(
                                        &consistency_name,
                                        &topic_name,
                                        timestamp,
                                        timestamp + 1,
                                    )?;
                                }
                                timestamp += 1;
                            }
                            old_len = 0;
                            pos += len as u64;
                            reader.seek(SeekFrom::Start(pos)).unwrap();
                        }
                        Err(msg) => {
                            if len <= old_len {
                                // emit an error and advance the reader.
                                println!("{}", msg);
                                pos += len as u64;
                                reader.seek(SeekFrom::Start(pos)).unwrap();
                            } else {
                                //retry after waiting for the line to update.
                                old_len = len;
                                thread::sleep(time::Duration::from_millis(250));
                            }
                        }
                    }
                    line.clear();
                } else {
                    if exit_at_end {
                        return Ok(());
                    }
                    thread::sleep(heartbeat);
                }
            }
            Err(err) => {
                println!("{}", err);
            }
        }
    }
}

fn main() {
    match run_stream() {
        Ok(()) => {}
        Err(err) => println!("{}", err),
    }
}
