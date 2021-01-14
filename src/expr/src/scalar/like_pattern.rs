// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use regex::{Regex, RegexBuilder};

use crate::scalar::EvalError;

/// Builds a regular expression that matches the same strings as a SQL
/// LIKE pattern.
pub fn build_regex(pattern: &str, flags: &str) -> Result<Regex, EvalError> {
    // LIKE patterns always cover the whole string, so we anchor the regex on
    // both sides. An underscore (`_`) in a LIKE pattern matches any single
    // character and a percent sign (`%`) matches any sequence of zero or more
    // characters, so we translate those operators to their equivalent regex
    // operators, `.` and `.*`, respectively. Other characters match themselves
    // and are copied directly, unless they have special meaning in a regex, in
    // which case they are escaped in the regex with a backslash (`\`).
    //
    // Note that characters in LIKE patterns may also be escaped by preceding
    // them with a backslash. This has no effect on most characters, but it
    // removes the special meaning from the underscore and percent sign
    // operators, and means that matching a literal backslash requires doubling
    // the backslash.
    //
    // TODO(benesch): SQL permits selecting a different escape character than
    // the backslash via LIKE '...' ESCAPE '...'. We will need to support this
    // syntax eventually.
    let mut regex = String::from("^");
    let mut escape = false;
    for c in pattern.chars() {
        match c {
            '\\' if !escape => escape = true,
            '_' if !escape => regex.push('.'),
            '%' if !escape => regex.push_str(".*"),
            c => {
                if regex_syntax::is_meta_character(c) {
                    regex.push('\\');
                }
                regex.push(c);
                escape = false;
            }
        }
    }
    regex.push('$');
    if escape {
        return Err(EvalError::UnterminatedLikeEscapeSequence);
    }

    let mut regex = RegexBuilder::new(&regex);
    for f in flags.chars() {
        match f {
            'i' => {
                regex.case_insensitive(true);
            }
            'c' => {
                regex.case_insensitive(false);
            }
            _ => return Err(EvalError::InvalidRegexFlag(f)),
        }
    }
    Ok(regex.build().expect("regex constructed to be valid"))
}
