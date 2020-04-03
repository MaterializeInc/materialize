// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fs::{self, File};
use std::io::{Cursor, Write};
use std::path;

use avro::{Codec, Writer};
use retry::delay::Fibonacci;
use tokio::stream::StreamExt;

use crate::action::{Action, State};
use crate::parser::BuiltinCommand;

pub struct WriteAction {
    path: String,
    schema: String,
    records: Vec<String>,
    codec: Option<Codec>,
}

pub fn build_write(mut cmd: BuiltinCommand) -> Result<WriteAction, String> {
    let path = cmd.args.string("path")?;
    let schema = cmd.args.string("schema")?;
    let codec = cmd.args.opt_parse("codec")?;

    let records = cmd.input;
    cmd.args.done()?;
    if path.contains(path::MAIN_SEPARATOR) {
        // The goal isn't security, but preventing mistakes.
        return Err("separators in paths are forbidden".into());
    }
    Ok(WriteAction {
        path,
        schema,
        records,
        codec,
    })
}

impl Action for WriteAction {
    fn undo(&self, _state: &mut State) -> Result<(), String> {
        // Files are written to a fresh temporary directory, so no need to
        // explicitly remove the file here.
        Ok(())
    }

    fn redo(&self, state: &mut State) -> Result<(), String> {
        let path = state.temp_dir.path().join(&self.path);
        println!("Writing to {}", path.display());
        let mut file = File::create(path).map_err(|e| e.to_string())?;
        let schema = interchange::avro::parse_schema(&self.schema)
            .map_err(|e| format!("parsing avro schema: {}", e))?;
        let mut writer = Writer::with_codec_opt(schema, &mut file, self.codec);
        write_records(&mut writer, &self.records)?;
        file.sync_all()
            .map_err(|e| format!("error syncing file: {}", e))?;
        Ok(())
    }
}

pub struct AppendAction {
    path: String,
    records: Vec<String>,
}

pub fn build_append(mut cmd: BuiltinCommand) -> Result<AppendAction, String> {
    let path = cmd.args.string("path")?;
    let records = cmd.input;
    cmd.args.done()?;
    if path.contains(path::MAIN_SEPARATOR) {
        // The goal isn't security, but preventing mistakes.
        return Err("separators in paths are forbidden".into());
    }
    Ok(AppendAction { path, records })
}

impl Action for AppendAction {
    fn undo(&self, _state: &mut State) -> Result<(), String> {
        Ok(())
    }

    fn redo(&self, state: &mut State) -> Result<(), String> {
        let path = state.temp_dir.path().join(&self.path);
        println!("Appending to {}", path.display());
        let mut buf = fs::read(&path).map_err(|e| e.to_string())?;
        // TODO(benesch): we'll be able to open the writer on the file directly
        // once the Avro reader is no longer asynchronous.
        let mut writer = state
            .tokio_runtime
            .block_on(Writer::append_to(Cursor::new(&mut buf)))
            .map_err(|e| e.to_string())?;
        write_records(&mut writer, &self.records)?;
        fs::write(path, buf).map_err(|e| format!("error syncing file: {}", e))?;
        Ok(())
    }
}

fn write_records<W>(writer: &mut Writer<W>, records: &[String]) -> Result<(), String>
where
    W: Write,
{
    let schema = writer.schema().clone();
    for record in records {
        let record = crate::format::avro::json_to_avro(
            &serde_json::from_str(record).map_err(|e| format!("parsing avro datum: {}", e))?,
            schema.top_node(),
        )?;
        writer
            .append(record)
            .map_err(|e| format!("writing avro record: {}", e))?;
    }
    writer
        .flush()
        .map_err(|e| format!("flushing avro writer: {}", e))?;
    Ok(())
}

pub struct VerifyAction {
    sink: String,
    expected: Vec<String>,
}

pub fn build_verify(mut cmd: BuiltinCommand) -> Result<VerifyAction, String> {
    let sink = cmd.args.string("sink")?;
    let expected = cmd.input;
    cmd.args.done()?;
    if sink.contains(path::MAIN_SEPARATOR) {
        // The goal isn't security, but preventing mistakes.
        return Err("separators in file sink names are forbidden".into());
    }
    Ok(VerifyAction { sink, expected })
}

impl Action for VerifyAction {
    fn undo(&self, _state: &mut State) -> Result<(), String> {
        Ok(())
    }

    fn redo(&self, state: &mut State) -> Result<(), String> {
        let path: String = retry::retry(Fibonacci::from_millis(100).take(5), || {
            let row = state.pgclient.query_one(
                "SELECT path FROM mz_catalog_names NATURAL JOIN mz_avro_ocf_sinks \
                 WHERE name = $1",
                &[&self.sink],
            )?;
            Ok::<_, postgres::Error>(row.get("path"))
        })
        .map_err(|e| format!("retrieving path: {:?}", e))?;

        println!("Verifying results in file {}", path);

        // Get the rows from this file
        let sink_file = state
            .tokio_runtime
            .block_on(tokio::fs::File::open(&path))
            .map_err(|e| format!("reading sink file {}: {}", path, e))?;
        let reader = state
            .tokio_runtime
            .block_on(avro::Reader::new(sink_file))
            .map_err(|e| format!("parsing avro values from file: {}", e))?;
        let schema = reader.writer_schema().clone();
        let actual_messages: Vec<_> = state
            .tokio_runtime
            .block_on(reader.into_stream().collect::<Vec<_>>())
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("converting to rust objects: {}", e))?;

        let mut converted_expected_messages = Vec::new();
        for expected in &self.expected {
            converted_expected_messages.push(
                crate::format::avro::json_to_avro(
                    &serde_json::from_str(expected)
                        .map_err(|e| format!("parsing avro datum: {}", e.to_string()))?,
                    schema.top_node(),
                )
                .unwrap(),
            );
        }

        let missing_values = crate::action::get_values_in_first_list_not_in_second(
            &converted_expected_messages,
            &actual_messages,
        );
        let additional_values = crate::action::get_values_in_first_list_not_in_second(
            &actual_messages,
            &converted_expected_messages,
        );

        if !missing_values.is_empty() || !additional_values.is_empty() {
            return Err(format!(
                "Mismatched Kafka sink rows. Missing: {:#?}, Unexpected: {:#?}",
                missing_values, additional_values
            ));
        }

        Ok(())
    }
}
