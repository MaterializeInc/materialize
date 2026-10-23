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
//! Programs compile and run in `mz-jq`'s WebAssembly sandbox, which bounds
//! each evaluation's instructions, stack, and memory.

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::sync::Arc;

use mz_expr_derive::sqlfunc;
use mz_ore::str::StrExt;
use mz_repr::adt::jsonb::{Jsonb, JsonbRef};
use serde::{Deserialize, Serialize};

use crate::EvalError;

/// A compiled jq program.
///
/// Compares, orders, hashes, and serializes as its source text, and compiles
/// again on deserialization.
#[derive(Clone, Serialize, Deserialize)]
#[serde(into = "String", try_from = "String")]
pub struct JqProgram {
    source: String,
    program: Arc<mz_jq::Program>,
}

impl JqProgram {
    /// Compiles `source`, or describes why it does not compile.
    pub fn new(source: &str) -> Result<Self, String> {
        if source.len() > MAX_PROGRAM_LEN {
            return Err(format!(
                "program is {} bytes, more than the maximum of {MAX_PROGRAM_LEN}",
                source.len()
            ));
        }
        Ok(JqProgram {
            source: source.to_owned(),
            program: Arc::new(mz_jq::Program::compile(source)?),
        })
    }

    /// The program's source text.
    pub fn source(&self) -> &str {
        &self.source
    }

    /// Applies the program to `input`. See [`mz_jq::Program::eval`].
    pub fn eval(&self, input: JsonbRef<'_>) -> Result<Option<Jsonb>, EvalError> {
        let Some(output) = self.program.eval(&input.to_string()).map_err(error)? else {
            return Ok(None);
        };
        // jq values are a superset of JSON, so output goes through the
        // canonical `jsonb` text parser, which rejects anything that is not.
        let output =
            Jsonb::from_str(&output).map_err(|e| error(format!("output is not JSON: {e}")))?;
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

/// The longest program, in bytes, that [`JqProgram::new`] accepts.
pub const MAX_PROGRAM_LEN: usize = 8 << 10;

fn error(msg: impl std::fmt::Display) -> EvalError {
    EvalError::InvalidParameterValue(format!("jq: {msg}").into())
}

#[cfg(test)]
mod tests;
