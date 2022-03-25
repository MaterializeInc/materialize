// Copyright Materialize, Inc. and contributors. All rights reserved.
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

use anyhow::{bail, Context};
use async_trait::async_trait;

use mz_ore::retry::Retry;

use crate::action::{Action, ControlFlow, State, SyncAction};
use crate::format::avro::{self, Codec, Reader, Writer};
use crate::parser::BuiltinCommand;

pub struct WriteAction {
    path: String,
    schema: String,
    records: Vec<String>,
    codec: Option<Codec>,
    repeat: usize,
}

pub fn build_write(mut cmd: BuiltinCommand) -> Result<WriteAction, anyhow::Error> {
    let path = cmd.args.string("path")?;
    let schema = cmd.args.string("schema")?;
    let codec = cmd.args.opt_parse("codec")?;
    let repeat = cmd.args.opt_parse("repeat")?.unwrap_or(1);

    let records = cmd.input;
    cmd.args.done()?;
    if path.contains(path::MAIN_SEPARATOR) {
        // The goal isn't security, but preventing mistakes.
        bail!("separators in paths are forbidden");
    }
    Ok(WriteAction {
        path,
        schema,
        records,
        codec,
        repeat,
    })
}

impl SyncAction for WriteAction {
    fn undo(&self, _state: &mut State) -> Result<(), anyhow::Error> {
        // Files are written to a fresh temporary directory, so no need to
        // explicitly remove the file here.
        Ok(())
    }

    fn redo(&self, state: &mut State) -> Result<ControlFlow, anyhow::Error> {
        let path = state.temp_path.join(&self.path);
        println!("Writing to {}", path.display());
        let mut file = File::create(&path)
            .with_context(|| format!("creating Avro OCF file: {}", path.display()))?;
        let schema = avro::parse_schema(&self.schema).context("parsing avro schema")?;
        let mut writer = Writer::with_codec_opt(schema, &mut file, self.codec);
        for _ in 0..self.repeat {
            write_records(&mut writer, &self.records)?;
        }
        writer.flush().context("flushing avro writer")?;
        file.sync_all()
            .with_context(|| format!("syncing Avro OCF file: {}", path.display()))?;
        Ok(ControlFlow::Continue)
    }
}

pub struct AppendAction {
    path: String,
    records: Vec<String>,
    repeat: usize,
}

pub fn build_append(mut cmd: BuiltinCommand) -> Result<AppendAction, anyhow::Error> {
    let path = cmd.args.string("path")?;
    let repeat = cmd.args.opt_parse("repeat")?.unwrap_or(1);
    let records = cmd.input;
    cmd.args.done()?;
    if path.contains(path::MAIN_SEPARATOR) {
        // The goal isn't security, but preventing mistakes.
        bail!("separators in paths are forbidden");
    }
    Ok(AppendAction {
        path,
        records,
        repeat,
    })
}

impl SyncAction for AppendAction {
    fn undo(&self, _state: &mut State) -> Result<(), anyhow::Error> {
        Ok(())
    }

    fn redo(&self, state: &mut State) -> Result<ControlFlow, anyhow::Error> {
        let path = state.temp_path.join(&self.path);
        println!("Appending to {}", path.display());
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let mut writer = Writer::append_to(file)?;
        for _ in 0..self.repeat {
            write_records(&mut writer, &self.records)?;
        }
        writer.flush().context("flushing avro writer")?;
        Ok(ControlFlow::Continue)
    }
}

fn write_records<W>(writer: &mut Writer<W>, records: &[String]) -> Result<(), anyhow::Error>
where
    W: Write,
{
    let schema = writer.schema().clone();
    for record in records {
        let record = avro::from_json(
            &serde_json::from_str(record).context("parsing avro datum: {:#}")?,
            schema.top_node(),
        )?;
        writer.append(record).context("writing avro record")?;
    }
    Ok(())
}

pub struct VerifyAction {
    sink: String,
    expected: Vec<String>,
}

pub fn build_verify(mut cmd: BuiltinCommand) -> Result<VerifyAction, anyhow::Error> {
    let sink = cmd.args.string("sink")?;
    let expected = cmd.input;
    cmd.args.done()?;
    if sink.contains(path::MAIN_SEPARATOR) {
        // The goal isn't security, but preventing mistakes.
        bail!("separators in file sink names are forbidden");
    }
    Ok(VerifyAction { sink, expected })
}

#[async_trait]
impl Action for VerifyAction {
    async fn undo(&self, _state: &mut State) -> Result<(), anyhow::Error> {
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<ControlFlow, anyhow::Error> {
        let path = Retry::default()
            .max_duration(state.default_timeout)
            .retry_async(|_| async {
                let row = state
                    .pgclient
                    .query_one(
                        "SELECT path FROM mz_catalog_names
                     JOIN mz_avro_ocf_sinks ON global_id = sink_id
                     WHERE name = $1",
                        &[&self.sink],
                    )
                    .await
                    .context("querying materialize")?;
                let bytes: Vec<u8> = row.get("path");
                Ok::<_, anyhow::Error>(PathBuf::from(OsString::from_vec(bytes)))
            })
            .await
            .context("retrieving path")?;

        println!("Verifying results in file {}", path.display());

        // Get the rows from this file. There is no async `mz_avro::Reader`, so
        // we drop into synchronous code here.
        tokio::task::block_in_place(|| {
            let file = File::open(&path)
                .with_context(|| format!("reading sink file {}", path.display()))?;
            let reader = Reader::new(file).context("creating avro reader")?;
            let schema = reader.writer_schema().clone();
            let actual = reader
                .map(|res| res.map(|val| (None, Some(val))))
                .collect::<Result<Vec<_>, _>>()
                .context("reading avro values from file")?;
            avro::validate_sink(
                None,
                &schema,
                &self.expected,
                &actual,
                &state.regex,
                &state.regex_replacement,
            )
        })?;

        Ok(ControlFlow::Continue)
    }
}
