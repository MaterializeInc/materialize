// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path;
use std::str::FromStr;

use async_compression::tokio::write::GzipEncoder;
use async_trait::async_trait;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::action::{Action, State};
use crate::format::bytes;
use crate::parser::BuiltinCommand;

pub struct AppendAction {
    path: String,
    contents: Vec<u8>,
    compression: Compression,
}

pub enum Compression {
    Gzip,
    None,
}

impl FromStr for Compression {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "gzip" => Ok(Compression::Gzip),
            "none" => Ok(Compression::None),
            f => Err(format!("unknown compression format: {}", f)),
        }
    }
}

pub fn build_compression(cmd: &mut BuiltinCommand) -> Result<Compression, String> {
    match cmd.args.opt_string("compression") {
        Some(s) => s.parse(),
        None => Ok(Compression::None),
    }
}

fn build_path(cmd: &mut BuiltinCommand) -> Result<String, String> {
    let path = cmd.args.string("path")?;
    if path.contains(path::MAIN_SEPARATOR) {
        // The goal isn't security, but preventing mistakes.
        Err("separators in paths are forbidden".into())
    } else {
        Ok(path)
    }
}

pub fn build_append(mut cmd: BuiltinCommand) -> Result<AppendAction, String> {
    let path = build_path(&mut cmd)?;
    let compression = build_compression(&mut cmd)?;
    cmd.args.done()?;
    let mut contents = vec![];
    for line in cmd.input {
        contents.extend(bytes::unescape(line.as_bytes())?);
        contents.push(b'\n');
    }
    Ok(AppendAction {
        path,
        contents,
        compression,
    })
}

#[async_trait]
impl Action for AppendAction {
    async fn undo(&self, _: &mut State) -> Result<(), String> {
        // Files are written to a fresh temporary directory, so no need to
        // explicitly remove the file here.
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<(), String> {
        let path = state.temp_dir.path().join(&self.path);
        println!("Appending to file {}", path.display());
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .map_err(|e| e.to_string())?;

        let mut file: Box<dyn AsyncWrite + Unpin + Send> = match self.compression {
            Compression::Gzip => Box::new(GzipEncoder::new(file)),
            Compression::None => Box::new(file),
        };

        file.write_all(&self.contents)
            .await
            .map_err(|e| e.to_string())?;

        file.shutdown().await.map_err(|e| e.to_string())?;
        Ok(())
    }
}

pub struct DeleteAction {
    path: String,
}

pub fn build_delete(mut cmd: BuiltinCommand) -> Result<DeleteAction, String> {
    let path = build_path(&mut cmd)?;
    cmd.args.done()?;
    Ok(DeleteAction { path })
}

#[async_trait]
impl Action for DeleteAction {
    async fn undo(&self, _: &mut State) -> Result<(), String> {
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<(), String> {
        let path = state.temp_dir.path().join(&self.path);
        println!("Deleting file {}", path.display());
        tokio::fs::remove_file(&path)
            .await
            .map_err(|e| e.to_string())
    }
}
