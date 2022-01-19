// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![warn(missing_docs)]

//! A pure Rust protobuf compiler.
//!
//! This crates provides a compiler for [Protocol Buffers] ("protobuf"),
//! Google's data interchange format. It generates Rust code to serialize and
//! deserialize protobufs given message schemas defined in a `.proto` file.
//!
//! `protoc` is written entirely in Rust. In other words, it does *not* depend
//! on libprotobuf and does *not* require Google's `protoc` binary to be
//! installed on the system.
//!
//! This crate delegates all of the hard work to the [`protobuf_codegen`]
//! crate. The primary motivation is to provide a more stable and ergonomic API
//! that is geared towards usage in build scripts. This insulates downstream
//! crates from changes in `protobuf_codegen`'s API and avoids duplicative
//! boilerplate in their build scripts. This crate also works around several
//! bugs in `protobuf_codegen` by patching the generated code, but the hope
//! is to upstream these bugfixes over time.

use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process;

use anyhow::{bail, Context};

use protobuf::descriptor::FileDescriptorSet;

/// A builder for a protobuf compilation.
#[derive(Default, Debug)]
pub struct Protoc {
    includes: Vec<PathBuf>,
    inputs: Vec<PathBuf>,
}

impl Protoc {
    /// Starts a new compilation.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds an include path to the compilation.
    pub fn include<S>(&mut self, include: S) -> &mut Self
    where
        S: Into<PathBuf>,
    {
        self.includes.push(include.into());
        self
    }

    /// Adds an input file to the compilation.
    pub fn input<S>(&mut self, input: S) -> &mut Self
    where
        S: Into<PathBuf>,
    {
        self.inputs.push(input.into());
        self
    }

    /// Parses the inputs into a file descriptor set.
    ///
    /// A [`FileDescriptorSet`] is protobuf's internal representation of a
    /// collection .proto file. It is similar in spirit to an AST, and is useful
    /// for callers who will analyze the protobuf messages to provide bespoke
    /// deserialization and serialization behavior, rather than relying on the
    /// stock code generation.
    ///
    /// Most users will want to call [`Protoc::compile_into`] or
    /// [`Protoc::build_script_exec`] instead.
    // TODO(benesch): switch this to protobuf_native to avoid the dependency
    // on rust-protobuf.
    pub fn parse(&self) -> Result<FileDescriptorSet, anyhow::Error> {
        for input in &self.inputs {
            if !input.exists() {
                bail!("input protobuf file does not exist: {}", input.display());
            }
        }
        let parsed = protobuf_parse::pure::parse_and_typecheck(&self.includes, &self.inputs)?;
        let mut fds = FileDescriptorSet::new();
        fds.file = parsed.file_descriptors.into_iter().collect();
        Ok(fds)
    }

    /// Executes the compilation.
    ///
    /// The generated files are placed into `out_dir` according to the
    /// conventions of the [`protobuf_codegen`] crate. Roughly speaking, for
    /// each input file `path/to/file.proto`, this method generates the Rust
    /// file `OUT_DIR/path/to/file.rs`. The details involve some special rules
    /// for escaping Rust keywords and special characters, but you will have to
    /// consult the `protobuf_codegen` source code for details.
    pub fn compile_into(&mut self, out_dir: &Path) -> Result<(), anyhow::Error> {
        if !out_dir.exists() {
            bail!(
                "out directory for protobuf generation does not exist: {}",
                out_dir.display()
            );
        }

        prost_build::Config::new()
            .out_dir(out_dir)
            .compile_protos(&self.inputs, &self.includes)?;

        Ok(())
    }

    /// Executes the compilation, following build script conventions for input
    /// and output.
    ///
    /// This is roughly equivalent to calling
    /// [`compile_into`](Self::compile_into), with the following adjustments.
    ///
    ///   * The directory to generate into is read from the environment variable
    ///     `OUT_DIR`.
    ///
    ///   * If an error occurs, instead of returning the error, the error is
    ///     printed to stderr and the process is aborted.
    ///
    ///   * Various diagnostic information is printed to stdout for users
    ///     following along with e.g. `cargo build -vv`.
    ///
    pub fn build_script_exec(&mut self) -> ! {
        let res = (|| {
            // Generate files into the output directory Cargo has created for
            // the crate.
            let out_dir = PathBuf::from(env::var_os("OUT_DIR").unwrap()).join("protobuf");

            // Output diagnostic information. Cargo will only display this if
            // an error occurs, or if run in very verbose mode (`-vv`).
            println!("protoc build script invoked");
            println!("output directory: {}", out_dir.display());
            println!("include directories:");
            for include in &self.includes {
                println!("    {}", include.display());
            }
            println!("inputs:");
            for input in &self.inputs {
                println!("    {}", input.display());
            }

            // Ask Cargo to re-run the build script if any of the input .proto
            // files change.
            for input in &self.inputs {
                println!("cargo:rerun-if-changed={}", input.display());
            }

            // Destroy and recreate the build directory, in case any inputs have
            // been deleted since the last invocation.
            match fs::remove_dir_all(&out_dir) {
                Ok(()) => (),
                Err(e) if e.kind() == io::ErrorKind::NotFound => (),
                Err(e) => {
                    return Err(anyhow::Error::new(e)).with_context(|| {
                        format!("removing existing out directory {}", out_dir.display())
                    })
                }
            }
            fs::create_dir(&out_dir)
                .with_context(|| format!("creating out directory {}", out_dir.display()))?;

            // Compile.
            self.compile_into(&out_dir)
        })();
        match res {
            Ok(()) => process::exit(0),
            Err(e) => {
                eprintln!("{}", e);
                process::exit(1);
            }
        }
    }
}
