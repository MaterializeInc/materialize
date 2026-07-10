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
use async_compression::tokio::write::{BzEncoder, GzipEncoder, XzEncoder, ZstdEncoder};
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::action::{ControlFlow, State};
use crate::format::bytes;
use crate::parser::BuiltinCommand;

pub enum Compression {
    Bzip2,
    Gzip,
    Xz,
    Zstd,
    None,
}

impl FromStr for Compression {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, anyhow::Error> {
        match s {
            "bzip2" => Ok(Compression::Bzip2),
            "gzip" => Ok(Compression::Gzip),
            "xz" => Ok(Compression::Xz),
            "zstd" => Ok(Compression::Zstd),
            "none" => Ok(Compression::None),
            f => bail!("unknown compression format: {}", f),
        }
    }
}

pub(crate) fn build_compression(cmd: &mut BuiltinCommand) -> Result<Compression, anyhow::Error> {
    match cmd.args.opt_string("compression") {
        Some(s) => s.parse(),
        None => Ok(Compression::None),
    }
}

/// Returns the full contents of a file: an optional header line, then the
/// input lines repeated `repeat` times, each line followed by a newline. With
/// `trailing-newline=false` the final newline is omitted.
pub(crate) fn build_contents(cmd: &mut BuiltinCommand) -> Result<Vec<u8>, anyhow::Error> {
    let header = cmd.args.opt_string("header");
    let trailing_newline = cmd.args.opt_bool("trailing-newline")?.unwrap_or(true);
    let repeat: usize = cmd.args.opt_parse("repeat")?.unwrap_or(1);

    let mut lines = vec![];
    for line in &cmd.input {
        lines.push(bytes::unescape(line.as_bytes())?);
    }

    let mut contents = vec![];
    if let Some(header) = header {
        contents.extend_from_slice(header.as_bytes());
        contents.push(b'\n');
    }
    for _ in 0..repeat {
        for line in &lines {
            contents.extend_from_slice(line);
            contents.push(b'\n');
        }
    }
    if !trailing_newline {
        contents.pop();
    }

    Ok(contents)
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
    let contents = build_contents(&mut cmd)?;
    cmd.args.done()?;

    println!("Appending to file {}", path.display());
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .await?;

    let mut file: Box<dyn AsyncWrite + Unpin + Send> = match compression {
        Compression::Gzip => Box::new(GzipEncoder::new(file)),
        Compression::Bzip2 => Box::new(BzEncoder::new(file)),
        Compression::Xz => Box::new(XzEncoder::new(file)),
        Compression::Zstd => Box::new(ZstdEncoder::new(file)),
        Compression::None => Box::new(file),
    };

    file.write_all(&contents).await?;
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
