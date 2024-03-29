// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{self, PathBuf};
use std::str::FromStr;

use anyhow::bail;
use async_compression::tokio::write::GzipEncoder;
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::action::{ControlFlow, State};
use crate::format::bytes;
use crate::parser::BuiltinCommand;

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

fn build_path(state: &State, cmd: &mut BuiltinCommand) -> Result<PathBuf, anyhow::Error> {
    let path = cmd.args.string("path")?;
    let container = cmd.args.opt_string("container");

    if path.contains(path::MAIN_SEPARATOR) {
        // The goal isn't security, but preventing mistakes.
        bail!("separators in paths are forbidden")
    }

    match container.as_deref() {
        None => Ok(state.temp_path.join(path)),
        Some("fivetran") => Ok(PathBuf::from(&state.fivetran_destination_files_path).join(path)),
        Some(x) => bail!("Unrecognized container '{x}'"),
    }
}

pub async fn run_append(
    mut cmd: BuiltinCommand,
    state: &State,
) -> Result<ControlFlow, anyhow::Error> {
    let path = build_path(state, &mut cmd)?;
    let compression = build_compression(&mut cmd)?;
    let header = cmd.args.opt_string("header");
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
    println!("Appending to file {}", path.display());
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .await?;

    let mut file: Box<dyn AsyncWrite + Unpin + Send> = match compression {
        Compression::Gzip => Box::new(GzipEncoder::new(file)),
        Compression::None => Box::new(file),
    };

    if let Some(header) = header {
        file.write_all(header.as_bytes()).await?;
        file.write_all("\n".as_bytes()).await?;
    }
    for _ in 0..repeat {
        file.write_all(&contents).await?;
    }
    file.shutdown().await?;

    Ok(ControlFlow::Continue)
}

pub async fn run_delete(
    mut cmd: BuiltinCommand,
    state: &State,
) -> Result<ControlFlow, anyhow::Error> {
    let path = build_path(state, &mut cmd)?;
    cmd.args.done()?;
    println!("Deleting file {}", path.display());
    fs::remove_file(&path).await?;
    Ok(ControlFlow::Continue)
}
