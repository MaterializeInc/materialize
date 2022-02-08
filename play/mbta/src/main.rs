// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::PathBuf;
use std::thread;
use std::time::{self, Duration};

use mz_test_util::kafka::kafka_client::KafkaClient;

/// Converts MBTA streams to Kafka streams to use in Materialize.
#[derive(clap::Parser)]
struct Args {
    /// Kafka bootstrap address.
    #[clap(long, default_value = "localhost:9092", value_name = "HOST:PORT")]
    kafka_addr: String,
    /// Path to file listing all logs to tail.
    #[clap(short = 'c', long, value_name = "PATH")]
    config_file: Option<PathBuf>,
    /// Path to file where information is being logged.
    #[clap(
        short = 'f',
        long,
        value_name = "PATH",
        required_unless_present("config-file")
    )]
    file_name: Option<PathBuf>,
    /// Name of topic to write to.
    #[clap(
        short = 't',
        long,
        value_name = "TOPIC",
        required_unless_present("config-file")
    )]
    topic_name: Option<String>,
    /// Wait time before checking logs for updates.
    #[clap(long, default_value = "250ms", parse(try_from_str = mz_repr::util::parse_duration), value_name = "DURATION")]
    heartbeat: Duration,
    /// Number of partitions to write to.
    #[clap(short = 'p', long, default_value = "1", value_name = "N")]
    partitions: i32,
    /// Replication factor of topic.
    #[clap(short = 'r', long, default_value = "1", value_name = "N")]
    replication: i32,
    /// Topic-level Kafka property.
    #[clap(long, value_name = "NAME=PROPERTY")]
    topic_property: Vec<String>,
    /// Disable topic creation.
    #[clap(short = 'd', long)]
    disable_topic_create: bool,
    /// Automatically exit when the end of the file is reached.
    #[clap(short = 'e', long)]
    exit_at_end: bool,
}

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
    if let Some(remainder) = line.strip_prefix("data: ") {
        let mut parsed_json = json::parse(remainder);
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

// if we receive a "event: reset", we need to retract all inserts that happened
// since the previous "event: reset"
fn delete_previous_entries(
    reader: &mut BufReader<File>,
    k_client: &KafkaClient,
    topic_name: &str,
    last_reset: u64,
    new_reset: u64,
) -> Result<(), anyhow::Error> {
    reader.seek(SeekFrom::Start(last_reset)).unwrap();
    let mut current_pos = last_reset;
    let mut line = String::new();
    // read from the last reset to the new reset
    while current_pos < new_reset {
        let len = reader.read_line(&mut line)?;
        if let Ok(key_values) = parse_line(&line) {
            for (key, value) in key_values {
                if value.is_some() {
                    // retract the value insertion
                    k_client.send_key_value(&topic_name, key.as_bytes(), None)?;
                }
            }
        }
        current_pos += len as u64;
        line.clear();
    }
    Ok(())
}

async fn run_stream() -> Result<(), anyhow::Error> {
    let args: Args = mz_ore::cli::parse_args();

    let (stream_configs, heartbeat) = if let Some(config_file) = args.config_file {
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
                PathBuf::from(format!("workspace/mbta-{}.log", &record[1]))
            } else {
                PathBuf::from(format!(
                    "workspace/mbta-{}-{}-{}.log",
                    &record[1], &record[2], &record[3]
                ))
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
            let heartbeat = mz_repr::util::parse_duration(&heartbeat)?;
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
        // Safe to unwrap here because clap will verify that file_name and
        // topic_name are present if config_file is not.
        (
            vec![(
                args.file_name.unwrap(),
                args.topic_name.unwrap(),
                args.partitions,
            )],
            args.heartbeat,
        )
    };

    let topic_configs: Result<Vec<_>, _> = args
        .topic_property
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

    let k_client = KafkaClient::new(&args.kafka_addr, "materialize.mbta-to-mtrlz", &[])?;

    if !args.disable_topic_create {
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
                    args.replication,
                    topic_configs_refs.as_slice(),
                    None,
                )
                .await?;
        }
    }

    // we read from all the files in a round-robin fashion
    let mut file_readers = VecDeque::with_capacity(stream_configs.len());
    for (filename, topic_name, _) in stream_configs {
        let f = File::open(filename)?;
        let reader = BufReader::new(f);
        file_readers.push_back((reader, topic_name, 0, 0, 0));
    }

    let mut consecutive_ends = 0;
    while let Some((mut reader, topic_name, mut pos, mut retries, mut last_reset)) =
        file_readers.pop_front()
    {
        let mut line = String::new();
        let len = reader.read_line(&mut line)?;

        if len > 0 {
            consecutive_ends = 0;
            if &line == "event: reset\n" {
                // acknowledge the line as read, then retract entries inserts
                // since the last reset
                pos += len as u64;
                if last_reset > 0 {
                    delete_previous_entries(&mut reader, &k_client, &topic_name, last_reset, pos)?;
                }
                last_reset = pos;
            } else {
                // try to parse the line into JSON objects
                match parse_line(&line) {
                    Ok(key_values) => {
                        for (key, value) in key_values {
                            k_client.send_key_value(
                                &topic_name,
                                key.as_bytes(),
                                value.map(|v| v.as_bytes().to_owned()),
                            )?;
                        }
                        retries = 0;
                        pos += len as u64;
                    }
                    Err(msg) => {
                        // sometimes, the log parser is overly eager and reads a line before
                        // it has finished writing.
                        if retries > 10 {
                            println!("{}", msg);
                            pos += len as u64;
                            retries = 0;
                        } else {
                            retries += 1;
                            reader.seek(SeekFrom::Start(pos)).unwrap();
                            std::thread::sleep(time::Duration::from_millis(250));
                        }
                    }
                }
            }
            line.clear();
        } else {
            // we have reached the end of the file
            if args.exit_at_end {
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

        // schedule another read
        file_readers.push_back((reader, topic_name, pos, retries, last_reset));
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
