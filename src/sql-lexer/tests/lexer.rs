// Copyright 2018 sqlparser-rs contributors. All rights reserved.
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// This file is derived from the sqlparser-rs project, available at
// https://github.com/andygrove/sqlparser-rs. It was incorporated
// directly into Materialize on December 21, 2019.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use datadriven::walk;
use mz_sql_lexer::lexer::lex;

/// Raw NUL characters in string literals must be rejected. They cannot appear
/// in datadriven testdata files, which hold text, so they are exercised here.
#[mz_ore::test]
fn test_nul_characters_rejected() {
    for query in [
        "SELECT 'foo\0bar'",
        "SELECT E'foo\0bar'",
        "SELECT E'foo\\\0bar'",
        "SELECT $$foo\0bar$$",
        "SELECT $tag$foo\0bar$tag$",
        "SELECT x'\0'",
        "SELECT E'\\u0000'",
        "SELECT E'\\U00000000'",
    ] {
        let err = match lex(query) {
            Ok(_) => panic!("lexing {query:?} must fail"),
            Err(err) => err,
        };
        assert_eq!(
            err.to_string(),
            "null character in string literal",
            "query: {query:?}"
        );
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
fn test_datadriven() {
    walk("tests/testdata", |f| {
        f.run(|tc| -> String {
            match tc.directive.as_str() {
                "lex" => match lex(&tc.input) {
                    Ok(tokens) => tokens
                        .into_iter()
                        .map(|t| format!("{}: {:?}\n", t.offset, t.kind))
                        .collect::<Vec<_>>()
                        .join(""),
                    Err(err) => format!("{}\n", err),
                },
                _ => unreachable!("unknown directive"),
            }
        })
    });
}
