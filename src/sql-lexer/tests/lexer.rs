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
                    Err(err) => err.to_string(),
                },
                _ => unreachable!("unknown directive"),
            }
        })
    });
}
