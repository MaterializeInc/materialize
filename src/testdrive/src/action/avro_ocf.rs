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
