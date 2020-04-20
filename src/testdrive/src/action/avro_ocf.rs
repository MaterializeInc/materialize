// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::ffi::OsStringExt;
use std::path::{self, PathBuf};
use std::time::Duration;

use async_trait::async_trait;

use crate::action::{Action, State, SyncAction};
use crate::format::avro::{self, Codec, Reader, Writer};
use crate::parser::BuiltinCommand;
use crate::util::retry;

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

impl SyncAction for WriteAction {
    fn undo(&self, _state: &mut State) -> Result<(), String> {
        // Files are written to a fresh temporary directory, so no need to
        // explicitly remove the file here.
        Ok(())
    }

    fn redo(&self, state: &mut State) -> Result<(), String> {
        let path = state.temp_dir.path().join(&self.path);
        println!("Writing to {}", path.display());
        let mut file = File::create(path).map_err(|e| e.to_string())?;
        let schema =
            avro::parse_schema(&self.schema).map_err(|e| format!("parsing avro schema: {}", e))?;
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

impl SyncAction for AppendAction {
    fn undo(&self, _state: &mut State) -> Result<(), String> {
        Ok(())
    }

    fn redo(&self, state: &mut State) -> Result<(), String> {
        let path = state.temp_dir.path().join(&self.path);
        println!("Appending to {}", path.display());
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| e.to_string())?;
        let mut writer = Writer::append_to(file).map_err(|e| e.to_string())?;
        write_records(&mut writer, &self.records)?;
        Ok(())
    }
}

fn write_records<W>(writer: &mut Writer<W>, records: &[String]) -> Result<(), String>
where
    W: Write,
{
    let schema = writer.schema().clone();
    for record in records {
        let record = avro::from_json(
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

#[async_trait]
impl Action for VerifyAction {
    async fn undo(&self, _state: &mut State) -> Result<(), String> {
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<(), String> {
        let path = retry::retry_for(Duration::from_secs(8), |_| async {
            let row = state
                .pgclient
                .query_one(
                    "SELECT path FROM mz_catalog_names NATURAL JOIN mz_avro_ocf_sinks \
                     WHERE name = $1",
                    &[&self.sink],
                )
                .await
                .map_err(|e| format!("querying materialize: {}", e.to_string()))?;
            let bytes: Vec<u8> = row.get("path");
            Ok(PathBuf::from(OsString::from_vec(bytes)))
        })
        .await
        .map_err(|e| format!("retrieving path: {:?}", e))?;

        println!("Verifying results in file {}", path.display());

        // Get the rows from this file. There is no async `avro::Reader`, so
        // we drop into synchronous code here.
        tokio::task::block_in_place(|| {
            let file = File::open(&path)
                .map_err(|e| format!("reading sink file {}: {}", path.display(), e))?;
            let reader = Reader::new(file).map_err(|e| format!("creating avro reader: {}", e))?;
            let schema = reader.writer_schema().clone();
            let actual = reader
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| format!("reading avro values from file: {}", e))?;
            avro::validate_sink(&schema, &self.expected, &actual)
        })
    }
}
