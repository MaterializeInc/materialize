// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Handlers for testdriven test commands.

use mz_repr::explain::ExplainConfig;

use crate::{try_parse_def, try_parse_mir, Def, TestCatalog};

pub fn handle_define(catalog: &mut TestCatalog, input: &str) -> String {
    // Parse the relation, returning early on parse error.
    let result = match try_parse_def(catalog, input) {
        Ok(def) => match def {
            Def::Source { name, typ } => match catalog.insert(&name, typ, true) {
                Ok(id) => format!("Source defined as {id}"),
                Err(err) => err,
            },
        },
        Err(err) => err,
    };
    result + "\n"
}

pub fn handle_roundtrip(catalog: &TestCatalog, input: &str) -> String {
    let output = match try_parse_mir(catalog, input) {
        Ok(expr) => expr.explain(&ExplainConfig::default(), Some(catalog)),
        Err(err) => return err,
    };

    if strip_comments(input).trim() == output.trim() {
        "roundtrip OK\n".to_string()
    } else {
        format!(
            "roundtrip produced a different output:\n~~~ expected:\n{}\n~~~ actual:\n{}\n~~~\n",
            strip_comments(input),
            output
        )
    }
}

fn strip_comments(s: &str) -> String {
    let mut r = "".to_string();
    for line in s.lines() {
        let len = line.find("// {").unwrap_or(line.len());
        r.push_str(line[0..len].trim_end());
        r.push_str("\n");
    }
    r
}
