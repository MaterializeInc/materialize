// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The `jq` function, which applies a jq program to a `jsonb` value.
//!
//! Programs run on jaq, a Rust implementation of jq. The available filters
//! are jaq's standard library minus those whose result depends on the
//! machine or the moment (`now`, `env`, `localtime`, `input`, ...), so that
//! every replica computes the same answer. Module imports are not supported.
//!
//! NOTE: jaq has no evaluation budget. A program that loops forever stalls
//! the worker evaluating it, and a program that recurses without bound
//! overflows that worker's stack.

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::sync::Arc;

use jaq_core::compile::Undefined;
use jaq_core::load::{self, Arena, File, Loader};
use jaq_core::native::{Fun, run, v};
use jaq_core::{Compiler, Ctx, Exn, Vars, data};
use jaq_json::Val;
use mz_expr_derive::sqlfunc;
use mz_ore::str::StrExt;
use mz_repr::adt::jsonb::{Jsonb, JsonbRef};
use serde::{Deserialize, Serialize};

use crate::EvalError;

type Data = data::JustLut<Val>;
type Filter = jaq_core::Filter<Data>;

/// Native filters withheld from programs, because their results depend on the
/// environment, they read input other than the argument, or they write to the
/// process's log or stderr.
const WITHHELD: &[&str] = &[
    "env",
    "now",
    "localtime",
    "strflocaltime",
    "input",
    "inputs",
    "debug_empty",
    "stderr_empty",
];

/// A compiled jq program.
///
/// Compares, orders, hashes, and serializes as its source text, and compiles
/// again on deserialization.
#[derive(Clone, Serialize, Deserialize)]
#[serde(into = "String", try_from = "String")]
pub struct JqProgram {
    source: String,
    filter: Arc<Filter>,
}

impl JqProgram {
    /// Compiles `source`, or describes why it does not compile.
    pub fn new(source: &str) -> Result<Self, String> {
        Ok(JqProgram {
            source: source.to_owned(),
            filter: Arc::new(compile(source)?),
        })
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    /// Applies the program to `input`.
    ///
    /// A program that produces no value yields `None`. A program that
    /// produces more than one value is an error, as jq's `[...]` collects a
    /// stream into a single array.
    pub fn eval(&self, input: JsonbRef<'_>) -> Result<Option<Jsonb>, EvalError> {
        // Values cross between Materialize and jaq as JSON text.
        let input = jaq_json::read::parse_single(input.to_string().as_bytes())
            .map_err(|e| error(format!("cannot read input: {e:?}")))?;
        let ctx = Ctx::<Data>::new(&self.filter.lut, Vars::new([]));
        let mut outputs = self.filter.id.run((ctx, input));
        let output = match outputs.next() {
            None => return Ok(None),
            Some(output) => output.map_err(exception)?,
        };
        if let Some(next) = outputs.next() {
            next.map_err(exception)?;
            return Err(error(
                "program produced more than one value; collect them with [...]",
            ));
        }
        let output = output.to_string();
        let output = Jsonb::from_str(&output)
            .map_err(|e| error(format!("output {output} is not JSON: {e}")))?;
        Ok(Some(output))
    }
}

impl From<JqProgram> for String {
    fn from(program: JqProgram) -> String {
        program.source
    }
}

impl TryFrom<String> for JqProgram {
    type Error = String;
    fn try_from(source: String) -> Result<Self, String> {
        JqProgram::new(&source)
    }
}

impl PartialEq for JqProgram {
    fn eq(&self, other: &Self) -> bool {
        self.source == other.source
    }
}

impl Eq for JqProgram {}

impl PartialOrd for JqProgram {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for JqProgram {
    fn cmp(&self, other: &Self) -> Ordering {
        self.source.cmp(&other.source)
    }
}

impl Hash for JqProgram {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.source.hash(state);
    }
}

impl fmt::Debug for JqProgram {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_tuple("JqProgram").field(&self.source).finish()
    }
}

/// Applies a literal jq program to a `jsonb` value. See [`JqProgram::eval`].
#[derive(
    Ord,
    PartialOrd,
    Clone,
    Debug,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    Hash
)]
pub struct Jq(pub JqProgram);

#[sqlfunc(Jq, skip_display = true)]
fn jq<'a>(&self, input: JsonbRef<'a>) -> Result<Option<Jsonb>, EvalError> {
    self.0.eval(input)
}

impl fmt::Display for Jq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "jq[{}]", self.0.source().escaped())
    }
}

fn compile(program: &str) -> Result<Filter, String> {
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

/// Describes a syntax error at `at`, a suffix of `program`.
fn syntax_error(program: &str, expected: &str, at: &str) -> String {
    let offset = load::span(program, at).start;
    if at.is_empty() {
        format!("expected {expected} at end of program")
    } else {
        format!("expected {expected} at offset {offset}")
    }
}

fn exception(exn: Exn<Val>) -> EvalError {
    match exn.get_err() {
        Ok(err) => error(err),
        Err(exn) => match exn.get_halt() {
            Ok(code) => error(format!("program halted with exit code {code}")),
            Err(_) => error("program raised an unexpected exception"),
        },
    }
}

fn error(msg: impl std::fmt::Display) -> EvalError {
    EvalError::InvalidParameterValue(format!("jq: {msg}").into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(input: &str, program: &str) -> Result<Option<String>, String> {
        let input: Jsonb = input.parse().unwrap();
        let program = JqProgram::new(program).map_err(|e| format!("jq: {e}"))?;
        program
            .eval(input.as_ref())
            .map(|out| out.map(|out| out.to_string()))
            .map_err(|e| e.to_string())
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn jq_eval() {
        assert_eq!(
            run(r#"{"a": [1, 2.5, 3]}"#, ".a | map(. * 2)"),
            Ok(Some("[2,5,6]".into()))
        );
        assert_eq!(
            run(
                r#"{"b": 2, "a": 1}"#,
                r#"to_entries | map(.key) | join(",")"#
            ),
            Ok(Some(r#""a,b""#.into()))
        );
        assert_eq!(
            run(r#"{"x": 123456789012345678901234567890.123}"#, ".x"),
            Ok(Some("123456789012345678901234567890.123".into()))
        );
        assert_eq!(run("[1, 2, 3]", ".[] | select(. > 5)"), Ok(None));
        assert_eq!(
            run("[1, 2, 3]", "[.[] | select(. > 1)]"),
            Ok(Some("[2,3]".into()))
        );
        // Logging filters are kept, as no-ops.
        assert_eq!(run("[1, 2]", "debug"), Ok(Some("[1,2]".into())));
        for (program, error) in [
            (
                ".[]",
                "jq: program produced more than one value; collect them with [...]",
            ),
            (". +", "jq: expected term at end of program"),
            ("now", "jq: undefined filter now/0"),
            ("env", "jq: undefined filter env/0"),
            (
                r#"import "x" as x; ."#,
                "jq: cannot import x: module loading not supported",
            ),
            (r#"error("boom")"#, r#"jq: "boom""#),
            ("halt_error(3)", "jq: program halted with exit code 3"),
        ] {
            assert_eq!(run("[1, 2]", program), Err(error.into()), "{program}");
        }
    }
}
