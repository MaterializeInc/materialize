// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::io::Write;
use std::process::{Command, Stdio};

use crate::action::{Action, State};
use crate::parser::BuiltinCommand;

pub struct IngestAction {
    topic: String,
    schema: String,
    rows: Vec<String>,
}

pub fn build_ingest(mut cmd: BuiltinCommand) -> Result<IngestAction, String> {
    let topic = cmd
        .args
        .remove("topic")
        .ok_or_else(|| String::from("missing topic argument"))?;
    let schema = cmd
        .args
        .remove("schema")
        .ok_or_else(|| String::from("missing schema argument"))?;
    Ok(IngestAction {
        topic,
        schema,
        rows: cmd.input,
    })
}

impl Action for IngestAction {
    fn undo(&self, _state: &mut State) -> Result<(), String> {
        println!("Deleting Kafka topic {:?}", self.topic);
        Command::new("kafka-topics")
            .args(&[
                "--zookeeper",
                "localhost:2181",
                "--delete",
                "--if-exists",
                "--topic",
                &self.topic,
            ])
            .status()
            .expect("failed to delete Kafka topic");
        Ok(())
    }

    fn redo(&self, _state: &mut State) -> Result<(), String> {
        println!("Ingesting data into Kafka topic {:?}", self.topic);
        let mut cmd = Command::new("kafka-avro-console-producer")
            .args(&[
                "--topic",
                &self.topic,
                "--broker-list",
                "localhost:9092",
                "--property",
                &format!("value.schema={}", self.schema),
            ])
            .stdin(Stdio::piped())
            .spawn()
            .expect("failed to spawn");
        let stdin = cmd.stdin.as_mut().unwrap();
        for row in &self.rows {
            writeln!(stdin, "{}", row).map_err(|e| e.to_string())?;
        }
        let exit_code = cmd.wait().expect("failed to wait");
        assert!(exit_code.success());
        Ok(())
    }
}
