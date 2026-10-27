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
use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};

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

/// The parsed contents of a file to be written: an optional header line
/// followed by the input lines repeated `repeat` times. Every line is
/// terminated by a newline, except that with `trailing_newline == false` the
/// final newline is omitted.
///
/// Lines are streamed on write (see [`Contents::write_to`]) rather than
/// materialized, so a large `repeat` generates a large file without holding
/// the whole file in memory.
pub(crate) struct Contents {
    header: Option<String>,
    /// Input lines with their escape sequences already resolved.
    lines: Vec<Vec<u8>>,
    repeat: usize,
    trailing_newline: bool,
}

impl Contents {
    pub(crate) fn parse(cmd: &mut BuiltinCommand) -> Result<Contents, anyhow::Error> {
        let header = cmd.args.opt_string("header");
        let trailing_newline = cmd.args.opt_bool("trailing-newline")?.unwrap_or(true);
        let repeat: usize = cmd.args.opt_parse("repeat")?.unwrap_or(1);

        let mut lines = vec![];
        for line in &cmd.input {
            lines.push(bytes::unescape(line.as_bytes())?);
        }

        Ok(Contents {
            header,
            lines,
            repeat,
            trailing_newline,
        })
    }

    /// The output lines in order: the header, if any, then the input lines
    /// repeated `repeat` times. Newlines are not included.
    fn output_lines(&self) -> impl Iterator<Item = &[u8]> {
        let header = self.header.as_deref().map(str::as_bytes).into_iter();
        let body = (0..self.repeat).flat_map(move |_| self.lines.iter().map(Vec::as_slice));
        header.chain(body)
    }

    /// Streams the contents to `writer`, terminating each line with a newline
    /// and suppressing the final newline when `trailing_newline` is false.
    pub(crate) async fn write_to<W>(&self, writer: &mut W) -> Result<(), anyhow::Error>
    where
        W: AsyncWrite + Unpin,
    {
        let mut wrote_line = false;
        for line in self.output_lines() {
            // Emit the newline that terminates the previous line only once we
            // know another line follows, so the final newline can be dropped.
            if wrote_line {
                writer.write_all(b"\n").await?;
            }
            writer.write_all(line).await?;
            wrote_line = true;
        }
        if wrote_line && self.trailing_newline {
            writer.write_all(b"\n").await?;
        }
        Ok(())
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
    let contents = Contents::parse(&mut cmd)?;
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
        // The compression encoders buffer their writes, but a bare
        // `tokio::fs::File` turns every per-line `write_all` into a separate
        // blocking filesystem job. Buffer it so a large `repeat` issues writes
        // in bounded chunks rather than two jobs per line.
        Compression::None => Box::new(BufWriter::new(file)),
    };

    contents.write_to(&mut file).await?;
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
