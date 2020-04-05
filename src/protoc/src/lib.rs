// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![forbid(missing_docs)]

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
//! This crate delegates all of the hard work to the [`protobuf_codegen_pure`]
//! crate. The primary motivation is to provide a more stable and ergonomic API
//! that is geared towards usage in build scripts. This insulates downstream
//! crates from changes in `protobuf_codegen_pure`'s API and avoids duplicative
//! boilerplate in their build scripts. This crate also works around several
//! bugs in `protobuf_codegen_pure` by patching the generated code, but the hope
//! is to upstream these bugfixes over time.

use std::env;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process;

use anyhow::{anyhow, bail, Context};
use protobuf::descriptor::FileDescriptorSet;
use protobuf::Message;
use protobuf_codegen_pure::Customize;

/// A builder for a protobuf compilation.
#[derive(Default, Debug)]
pub struct Protoc {
    includes: Vec<PathBuf>,
    inputs: Vec<PathBuf>,
    customize: Customize,
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

    /// Enables or disables `serde` support.
    pub fn serde(&mut self, set: bool) -> &mut Self {
        self.customize.serde_derive = Some(set);
        self
    }

    /// Executes the compilation.
    ///
    /// The generated files are placed into `out_dir` according to the
    /// conventions of the [`protoc_codegen_pure`]. Rougly speaking, for each
    /// input file `path/to/file.proto`, this method generates the Rust file
    /// `OUT_DIR/path/to/file.rs`. The details involve some special rules for
    /// escaping Rust keywords and special characters, but you will have to
    /// consult the `protoc_codegen_pure` source code for details.
    pub fn compile_into(&mut self, out_dir: &Path) -> Result<(), anyhow::Error> {
        if !out_dir.exists() {
            bail!(
                "out directory for protobuf generation does not exist: {}",
                out_dir.display()
            );
        }

        for input in &self.inputs {
            if !input.exists() {
                bail!("input protobuf file does not exist: {}", input.display());
            }
        }

        let includes: Vec<_> = self.includes.iter().map(|p| p.as_path()).collect();
        let inputs: Vec<_> = self.inputs.iter().map(|p| p.as_path()).collect();
        let parsed =
            protobuf_codegen_pure::parse_and_typecheck(&includes, &inputs).map_err(|e| {
                // The `fmt::Display` implementation for `e` is hopelessly broken
                // and displays no useful information. Use the debug implementation
                // instead.
                anyhow!("{:#?}", e)
            })?;

        protobuf_codegen::gen_and_write(
            &parsed.file_descriptors,
            &parsed.relative_paths,
            &out_dir,
            &self.customize,
        )?;

        let mut mods = vec![];
        for entry in fs::read_dir(out_dir)? {
            let entry = entry?;

            match entry.path().file_stem() {
                None => bail!(
                    "unexpected file in protobuf out directory: {}",
                    entry.path().display()
                ),
                Some(m) => mods.push(m.to_string_lossy().into_owned()),
            }

            // rust-protobuf assumes 2015-style `#[macro_use] extern crate` and
            // so doesn't `use` the serde macros it requires. Add them in.
            // This is fixed in the unreleased v3.0 of rust-protobuf.
            if self.customize.serde_derive == Some(true) {
                fs::OpenOptions::new()
                    .append(true)
                    .open(entry.path())?
                    .write_all(b"use serde::{Deserialize, Serialize};\n")?;
            }
        }

        // Generate a module index that includes all generated modules.
        // Upstream issue: https://github.com/stepancheg/rust-protobuf/issues/438
        #[rustfmt::skip]
        {
            let mut f = File::create(out_dir.join("mod.rs"))?;
            writeln!(f, "// Generated by Materialize's protoc crate. Do not edit!")?;
            writeln!(f, "// @generated")?;
            writeln!(f)?;
            writeln!(f)?;
            for m in &mods {
                writeln!(f, "pub mod {};", m)?;
            }
            writeln!(f)?;
            writeln!(f, "pub const FILE_DESCRIPTOR_SET_DATA: &[u8] = include_bytes!(\"file_descriptor_set.pb\");")?;
            writeln!(f)?;
            writeln!(f, "#[allow(dead_code)]")?;
            writeln!(f, "pub fn file_descriptor_set() -> &'static protobuf::descriptor::FileDescriptorSet {{")?;
            writeln!(f, "    static LAZY: protobuf::rt::LazyV2<protobuf::descriptor::FileDescriptorSet> = protobuf::rt::LazyV2::INIT;")?;
            writeln!(f, "    LAZY.get(|| protobuf::parse_from_bytes(FILE_DESCRIPTOR_SET_DATA).unwrap())")?;
            writeln!(f, "}}")?;

            let mut f = File::create(out_dir.join("file_descriptor_set.pb"))
                .context("creating file_descriptor_set.pb")?;
            let mut fds = FileDescriptorSet::new();
            fds.file = parsed.file_descriptors.into_iter().collect();
            fds.write_to_writer(&mut f)?;
        };

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
    ///   * If the compilation is Serde-enabled, Cargo will be instructed to
    ///     enable the `with-serde` feature to work around a bug in
    ///     [`protobuf_codegen_pure`].
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

            // rust-protobuf v2.12 inexplicably puts the derive attributes
            // behind the `with-serde` feature. This is fixed on master. For
            // now, just force that feature to be true in the crate we're
            // compiling.
            if self.customize.serde_derive == Some(true) {
                println!("cargo:rustc-cfg=feature=\"with-serde\"");
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
