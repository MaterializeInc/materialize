// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path;
use std::str::FromStr;

use anyhow::bail;
use async_compression::tokio::write::GzipEncoder;
use async_trait::async_trait;
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::action::{Action, ControlFlow, State};
use crate::format::bytes;
use crate::parser::BuiltinCommand;

pub struct AppendAction {
    path: String,
    contents: Vec<u8>,
    compression: Compression,
    repeat: usize,
}

pub enum Compression {
    Gzip,
    None,
}

impl FromStr for Compression {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, anyhow::Error> {
        match s {
            "gzip" => Ok(Compression::Gzip),
            "none" => Ok(Compression::None),
            f => bail!("unknown compression format: {}", f),
        }
    }
}

pub fn build_compression(cmd: &mut BuiltinCommand) -> Result<Compression, anyhow::Error> {
    match cmd.args.opt_string("compression") {
        Some(s) => s.parse(),
        None => Ok(Compression::None),
    }
}

fn build_path(cmd: &mut BuiltinCommand) -> Result<String, anyhow::Error> {
    let path = cmd.args.string("path")?;
    if path.contains(path::MAIN_SEPARATOR) {
        // The goal isn't security, but preventing mistakes.
        bail!("separators in paths are forbidden")
    } else {
        Ok(path)
    }
}

pub fn build_append(mut cmd: BuiltinCommand) -> Result<AppendAction, anyhow::Error> {
    let path = build_path(&mut cmd)?;
    let compression = build_compression(&mut cmd)?;
    let trailing_newline = cmd.args.opt_bool("trailing-newline")?.unwrap_or(true);
    let repeat = cmd.args.opt_parse("repeat")?.unwrap_or(1);
    cmd.args.done()?;
    let mut contents = vec![];
    for line in cmd.input {
        contents.extend(bytes::unescape(line.as_bytes())?);
        contents.push(b'\n');
    }
    if !trailing_newline {
        contents.pop();
    }
    Ok(AppendAction {
        path,
        contents,
        compression,
        repeat,
    })
}

#[async_trait]
impl Action for AppendAction {
    async fn undo(&self, _: &mut State) -> Result<(), anyhow::Error> {
        // Files are written to a fresh temporary directory, so no need to
        // explicitly remove the file here.
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<ControlFlow, anyhow::Error> {
        let path = state.temp_path.join(&self.path);
        println!("Appending to file {}", path.display());
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;

        let mut file: Box<dyn AsyncWrite + Unpin + Send> = match self.compression {
            Compression::Gzip => Box::new(GzipEncoder::new(file)),
            Compression::None => Box::new(file),
        };

        for _ in 0..self.repeat {
            file.write_all(&self.contents).await?;
        }
        file.shutdown().await?;

        Ok(ControlFlow::Continue)
    }
}

pub struct DeleteAction {
    path: String,
}

pub fn build_delete(mut cmd: BuiltinCommand) -> Result<DeleteAction, anyhow::Error> {
    let path = build_path(&mut cmd)?;
    cmd.args.done()?;
    Ok(DeleteAction { path })
}

#[async_trait]
impl Action for DeleteAction {
    async fn undo(&self, _: &mut State) -> Result<(), anyhow::Error> {
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<ControlFlow, anyhow::Error> {
        let path = state.temp_path.join(&self.path);
        println!("Deleting file {}", path.display());
        fs::remove_file(&path).await?;
        Ok(ControlFlow::Continue)
    }
}
