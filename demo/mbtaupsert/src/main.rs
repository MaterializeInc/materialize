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

//use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, VecDeque};
use std::env;
use std::fs::File;
//use std::hash::{Hash, Hasher};
use std::io::prelude::*;
use std::io::BufReader;
use std::io::SeekFrom;
use std::{thread, time};

use test_util::kafka;

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
/*
fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}*/

async fn run_stream() -> Result<(), anyhow::Error> {
    let args: Vec<_> = env::args().collect();

    let mut opts = Options::new();
    opts.optopt("", "kafka-addr", "kafka bootstrap address", "HOST:PORT");
    opts.optopt(
        "c",
        "config-file",
        "path to file listing all logs to tail",
        "PATH",
    );
    opts.optopt(
        "f",
        "file-name",
        "path to file where information is being logged",
        "PATH",
    );
    opts.optflag("h", "help", "show this usage information");
    opts.optopt("t", "topic-name", "name of topic to write to", "STRING");
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
    /*opts.optflag("", "byo", "enable byo consistency");
    opts.optopt(
        "",
        "consistency-name",
        "name of consistency topic to write to (default TOPIC_NAME-consistency topic)",
        "STRING",
    );*/
    opts.optflag(
        "e",
        "exit-at-end",
        "automatically exit when the end of the file is reached",
    );
    let usage_details = opts.usage("usage: mbtaupsert [options] FILE");
    let opts = opts.parse(&args[1..])?;

    if opts.opt_present("h") {
        print!("{}", usage_details);
        std::process::exit(0);
    }

    let (stream_configs, heartbeat) = if let Some(config_file) = opts.opt_str("config-file") {
        // read the config file line by line, skipping comments
        let mut configs = Vec::new();
        let mut min_heartbeat = time::Duration::new(0, 0);
        let f = File::open(config_file)?;
        let reader = BufReader::new(f);
        for line in reader.lines().map(|l| l.unwrap()) {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let record: Vec<_> = line.split(',').collect();
            if record.len() != 6 {
                return Err(anyhow::anyhow!(
                    "Config file requires six fields per row".to_owned()
                ));
            }
            let filename = if record[2].is_empty() {
                format!("workspace/mbta-{}.log", &record[1])
            } else {
                format!(
                    "workspace/mbta-{}-{}-{}.log",
                    &record[1], &record[2], &record[3]
                )
            };
            let topic_name = if record[0].is_empty() {
                if record[2].is_empty() {
                    format!("mbta-{}", &record[1])
                } else {
                    format!("mbta-{}-{}-{}", &record[1], &record[2], &record[3])
                }
            } else {
                record[0].to_string()
            };
            let partitions = if record[4].is_empty() {
                1
            } else {
                record[4].parse::<i32>()?
            };
            configs.push((filename, topic_name, partitions));
            let heartbeat = if record[5].is_empty() {
                "250ms".to_string()
            } else {
                record[5].to_string()
            };
            let heartbeat = parse(&heartbeat)?;
            if min_heartbeat > heartbeat
                || (min_heartbeat.as_secs() == 0 && min_heartbeat.subsec_nanos() == 0)
            {
                min_heartbeat = heartbeat;
            }
        }
        if configs.is_empty() {
            return Err(anyhow::anyhow!("Config file is empty".to_owned()));
        }
        (configs, min_heartbeat)
    } else {
        // we assume that the user is specifying the translation of a single
        // topic using command line argument
        if let Some(filename) = opts.opt_str("f") {
            if let Some(topic_name) = opts.opt_str("t") {
                let heartbeat_spec = opts
                    .opt_str("heartbeat")
                    .unwrap_or_else(|| "250ms".to_string());
                let heartbeat = parse(&heartbeat_spec)?;

                let partitions = opts
                    .opt_str("partitions")
                    .unwrap_or_else(|| "1".to_string());
                let partitions = partitions.parse::<i32>()?;

                (vec![(filename, topic_name, partitions)], heartbeat)
            } else {
                return Err(anyhow::anyhow!("Must specify target topic".to_owned()));
            }
        } else {
            return Err(anyhow::anyhow!(
                "Must specify a config file with -c or a file to tail with -f".to_owned()
            ));
        }
    };

    //let byo = opts.opt_present("byo");

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
                None => Err(anyhow::anyhow!(format!(
                    "Property {} has no argument",
                    property_name.unwrap()
                ))),
            }
        })
        .collect();
    let topic_configs = topic_configs?;
    let topic_configs_refs: Vec<_> = topic_configs
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect();

    let replication = opts
        .opt_str("replication")
        .unwrap_or_else(|| "1".to_string());
    let replication = replication.parse::<i32>()?;

    let k_client = kafka::kafka_client::KafkaClient::new(
        &opts
            .opt_str("kafka-addr")
            .unwrap_or_else(|| "localhost:9092".to_string()),
        "materialize.mbtaupsert",
        &[],
    )?;

    if !opts.opt_present("disable-topic-create") {
        // find the unique topic names, then create them
        let mut unique_topics = HashMap::new();
        for (_, topic_name, partitions) in stream_configs.iter() {
            let old_partitions = unique_topics.insert(topic_name, *partitions);
            if let Some(old_partitions) = old_partitions {
                if old_partitions != *partitions {
                    return Err(anyhow::anyhow!(format!(
                        "Different partition numbers for topic {} were specified",
                        &topic_name
                    )));
                }
            }
        }
        for (topic_name, partitions) in unique_topics.iter() {
            k_client
                .create_topic(
                    &topic_name,
                    *partitions,
                    replication,
                    topic_configs_refs.as_slice(),
                    None,
                )
                .await?;
        }
    }

    // byo stuff commented out
    /*let consistency_name = opts
        .opt_str("c")
        .unwrap_or(format!("{}-data-consistency", topic_name));
    if byo && !opts.opt_present("disable-topic-create") {
        k_client.create_topic(&consistency_name,
            1,
            replication,
            &vec![
                ("cleanup.policy", "delete"),
                ("retention.ms", "-1"),
            ], None).await?;
    }
    let mut timestamp = 0; */

    let exit_at_end = opts.opt_present("exit-at-end");

    // we read from all the files in a round-robin fashion
    let mut file_readers = VecDeque::with_capacity(stream_configs.len());
    for (filename, topic_name, _) in stream_configs {
        let f = File::open(filename)?;
        let reader = BufReader::new(f);
        file_readers.push_back((reader, topic_name, 0, 0));
    }

    let mut consecutive_ends = 0;
    while let Some((mut reader, topic_name, mut pos, mut old_len)) = file_readers.pop_front() {
        let mut line = String::new();
        let resp = reader.read_line(&mut line);
        match resp {
            Ok(len) => {
                if len > 0 {
                    consecutive_ends = 0;
                    // try to parse the line into JSON objects
                    match parse_line(&line) {
                        Ok(key_values) => {
                            for (key, value) in key_values {
                                k_client.send_key_value(
                                    &topic_name,
                                    key.as_bytes(),
                                    value.map(|v| v.as_bytes().to_owned()),
                                )?;
                                /*if byo {
                                    state.ingest_consistency(
                                        &consistency_name,
                                        &topic_name,
                                        timestamp,
                                        timestamp + 1,
                                    )?;
                                }
                                timestamp += 1;*/
                            }
                            old_len = 0;
                            pos += len as u64;
                        }
                        Err(msg) => {
                            // sometimes, the log parser is overly eager and reads a line before
                            // it has finished writing. so we want to retry reading the whole line
                            // until number of bytes read from the line is no longer increasing
                            if len <= old_len {
                                println!("{}", msg);
                                pos += len as u64;
                                old_len = 0;
                            } else {
                                old_len = len;
                                reader.seek(SeekFrom::Start(pos)).unwrap();
                                std::thread::sleep(time::Duration::from_millis(250));
                            }
                        }
                    }
                    line.clear();
                } else {
                    // we have reached the end of the file
                    if exit_at_end {
                        // stop reading the file by not returning the reader to the queue
                        continue;
                    }
                    consecutive_ends += 1;
                    if consecutive_ends == file_readers.len() {
                        // if in this round, we found that we reached the end of
                        // every log, sleep before checking for updates again
                        thread::sleep(heartbeat);
                        consecutive_ends = 0;
                    }
                }
            }
            Err(err) => {
                println!("{}", err);
            }
        }
        // schedule another read
        file_readers.push_back((reader, topic_name, pos, old_len));
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    if let Err(e) = run_stream().await {
        eprintln!("ERROR: {:#?}", e);
        std::process::exit(1);
    }
}
