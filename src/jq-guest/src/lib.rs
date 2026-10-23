// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! jq programs on jaq, built as a WebAssembly module that `mz-jq` runs.
//!
//! The available filters are jaq's standard library minus those whose result
//! depends on the machine or the moment (`now`, `env`, `localtime`, `input`,
//! ...), so that every replica computes the same answer. Module imports are
//! not supported.
//!
//! On `wasm32`, the module exports this ABI. Arguments are UTF-8 byte strings
//! the host writes into a buffer from `alloc(len)`, and the call that receives
//! a buffer frees it.
//!
//! * `compile(ptr, len) -> status` compiles a program, replacing any earlier
//!   one. It returns [`status::NONE`] or [`status::ERROR`].
//! * `eval(ptr, len) -> status` applies the compiled program to a JSON value.
//! * `output_ptr()` and `output_len()` locate the text of the last value or
//!   error, valid until the next call.

use jaq_core::compile::Undefined;
use jaq_core::load::{self, Arena, File, Loader};
use jaq_core::native::{Fun, run, v};
use jaq_core::{Compiler, Ctx, Exn, Vars, data};
use jaq_json::Val;

type Data = data::JustLut<Val>;

/// A compiled jq program.
pub type Filter = jaq_core::Filter<Data>;

/// Return codes of the module's `compile` and `eval` exports.
pub mod status {
    /// Success without a value.
    pub const NONE: u32 = 0;
    /// Success, and the output is the JSON text of a value.
    pub const VALUE: u32 = 1;
    /// Failure, and the output is an error message.
    pub const ERROR: u32 = 2;
}

/// Native filters withheld from programs, because their results depend on the
/// environment, they read input other than the argument, or they write to the
/// process's log or stderr.
pub const WITHHELD: &[&str] = &[
    "env",
    "now",
    "localtime",
    "strflocaltime",
    "input",
    "inputs",
    // Resolves time zone names against the host's time zone database.
    "strptime",
    "debug_empty",
    "stderr_empty",
];

/// Compiles `program`, or describes why it does not compile.
pub fn compile(program: &str) -> Result<Filter, String> {
    let defs = jaq_core::defs()
        .chain(jaq_std::defs())
        .chain(jaq_json::defs());
    // The standard library defines `debug` and `stderr` in terms of these,
    // so they stay defined, as no-ops.
    let silenced: [Fun<Data>; 2] = [
        run(("debug_empty", v(0), |_| Box::new(std::iter::empty()))),
        run(("stderr_empty", v(0), |_| Box::new(std::iter::empty()))),
    ];
    let funs = jaq_core::funs()
        .chain(jaq_std::funs())
        .chain(jaq_json::funs())
        .filter(|(name, _, _)| !WITHHELD.contains(name))
        .chain(silenced);

    let arena = Arena::default();
    let modules = Loader::new(defs)
        .load(
            &arena,
            File {
                code: program,
                path: (),
            },
        )
        .map_err(|errs| {
            let msgs = errs.into_iter().flat_map(|(_file, err)| match err {
                load::Error::Io(errs) => errs
                    .into_iter()
                    .map(|(path, msg)| format!("cannot import {path}: {msg}"))
                    .collect(),
                load::Error::Lex(errs) => errs
                    .into_iter()
                    .map(|(expect, at)| syntax_error(program, expect.as_str(), at))
                    .collect(),
                load::Error::Parse(errs) => errs
                    .into_iter()
                    .map(|(expect, at)| syntax_error(program, expect.as_str(), at))
                    .collect::<Vec<_>>(),
            });
            msgs.collect::<Vec<_>>().join("; ")
        })?;
    Compiler::default()
        .with_funs(funs)
        .compile(modules)
        .map_err(|errs| {
            let msgs = errs.into_iter().flat_map(|(_file, errs)| errs);
            let msgs = msgs.map(|(name, undefined)| match undefined {
                Undefined::Filter(arity) => format!("undefined filter {name}/{arity}"),
                other => format!("undefined {} {name}", other.as_str()),
            });
            msgs.collect::<Vec<_>>().join("; ")
        })
}

/// Applies `filter` to the JSON text `input`, returning the text of its only
/// output, if any.
///
/// A program that produces more than one value is an error, as jq's `[...]`
/// collects a stream into a single array. The output is jq's rendering of the
/// value, which is not JSON for values like NaN or byte strings.
pub fn eval(filter: &Filter, input: &[u8]) -> Result<Option<String>, String> {
    let input = jaq_json::read::parse_single(input).map_err(|e| format!("input: {e}"))?;
    let ctx = Ctx::<Data>::new(&filter.lut, Vars::new([]));
    let mut outputs = filter.id.run((ctx, input));
    let output = match outputs.next() {
        None => return Ok(None),
        Some(output) => output.map_err(exception)?,
    };
    if let Some(next) = outputs.next() {
        next.map_err(exception)?;
        return Err("program produced more than one value".into());
    }
    Ok(Some(output.to_string()))
}

/// Describes a syntax error at `at`, a suffix of `program`.
fn syntax_error(program: &str, expected: &str, at: &str) -> String {
    if at.is_empty() {
        format!("expected {expected} at end of program")
    } else {
        let offset = load::span(program, at).start;
        format!("expected {expected} at offset {offset}")
    }
}

fn exception(exn: Exn<Val>) -> String {
    match exn.get_err() {
        Ok(err) => err.to_string(),
        Err(exn) => match exn.get_halt() {
            Ok(code) => format!("program halted with exit code {code}"),
            Err(_) => "program raised an unexpected exception".into(),
        },
    }
}

#[cfg(target_arch = "wasm32")]
mod abi {
    use std::cell::RefCell;

    use super::{Filter, status};

    thread_local! {
        static FILTER: RefCell<Option<Filter>> = const { RefCell::new(None) };
        static OUTPUT: RefCell<String> = const { RefCell::new(String::new()) };
    }

    #[unsafe(no_mangle)]
    extern "C" fn alloc(len: usize) -> *mut u8 {
        Box::leak(vec![0u8; len].into_boxed_slice()).as_mut_ptr()
    }

    /// Takes back a buffer from [`alloc`].
    ///
    /// # Safety
    ///
    /// `ptr` and `len` must be from one call to [`alloc`], not yet taken.
    unsafe fn take(ptr: *mut u8, len: usize) -> Box<[u8]> {
        // SAFETY: the caller passes a live allocation of `len` bytes.
        unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, len)) }
    }

    fn output(status: u32, text: String) -> u32 {
        OUTPUT.with(|o| *o.borrow_mut() = text);
        status
    }

    #[unsafe(no_mangle)]
    unsafe extern "C" fn compile(ptr: *mut u8, len: usize) -> u32 {
        // SAFETY: the ABI requires a buffer from `alloc`.
        let program = unsafe { take(ptr, len) };
        let compiled = std::str::from_utf8(&program)
            .map_err(|e| e.to_string())
            .and_then(super::compile);
        match compiled {
            Ok(filter) => {
                FILTER.with(|f| *f.borrow_mut() = Some(filter));
                output(status::NONE, String::new())
            }
            Err(e) => output(status::ERROR, e),
        }
    }

    #[unsafe(no_mangle)]
    unsafe extern "C" fn eval(ptr: *mut u8, len: usize) -> u32 {
        // SAFETY: the ABI requires a buffer from `alloc`.
        let input = unsafe { take(ptr, len) };
        let result = FILTER.with(|f| match &*f.borrow() {
            Some(filter) => super::eval(filter, &input),
            None => Err("no program compiled".into()),
        });
        match result {
            Ok(None) => output(status::NONE, String::new()),
            Ok(Some(value)) => output(status::VALUE, value),
            Err(e) => output(status::ERROR, e),
        }
    }

    #[unsafe(no_mangle)]
    extern "C" fn output_ptr() -> *const u8 {
        OUTPUT.with(|o| o.borrow().as_ptr())
    }

    #[unsafe(no_mangle)]
    extern "C" fn output_len() -> usize {
        OUTPUT.with(|o| o.borrow().len())
    }
}

#[cfg(test)]
mod tests;
