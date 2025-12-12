// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Generate function documentation from macros.

use differential_dataflow::trace::Description;
use mz_sql_parser::ast::Expr;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::env;
use std::path::PathBuf;

fn main() {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    // 2. Define the path for the temporary collection file
    // The macro will append data to this file.
    let temp_collection_path = out_dir.join("macro_docs_temp");

    // 3. List all files in the temporary collection directory and combine into a single file
    // Group the entries by `category` and sort each group by name.

    // List all files in `temp_collection_path`
    let mut entries = vec![];
    for entry in std::fs::read_dir(&temp_collection_path).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("json") {
            entries.push(path);
        }
    }

    let mut categories: BTreeMap<String, Category> = BTreeMap::default();

    for path in entries {
        // Read contents into string
        let json_string = std::fs::read_to_string(path).expect("can read");
        let doc = serde_json::from_str::<FnDoc>(&json_string).expect("can parse");
        let function = Function {
            signature: doc.signature,
            description: doc.description,
            url: "".to_string(),
            version_added: "".to_string(),
            unmaterializable: false,
            known_time_zone_limitation_cast: false,
        };
        categories
            .entry(doc.category.clone())
            .or_insert_with(|| Category {
                r#type: doc.category,
                description: "".to_string(),
                functions: Default::default(),
            })
            .functions
            .push(function);
    }

    for category in categories.values_mut() {
        category.functions.sort();
    }

    let categories = categories.into_values().collect::<Vec<_>>();

    let json = serde_json::to_string_pretty(&categories).expect("can serialize");
    println!("{json}\n");
}

/// Struct from `mz-expr-derive-impl`, copied here to avoid dependency.
#[derive(Debug, Deserialize)]
struct FnDoc {
    unique_name: String,
    category: String,
    signature: String,
    description: String,
}

#[derive(Debug, Serialize, Ord, PartialOrd, Eq, PartialEq)]
struct Category {
    r#type: String,
    description: String,
    functions: Vec<Function>,
}

#[derive(Debug, Serialize, Ord, PartialOrd, Eq, PartialEq)]
struct Function {
    signature: String,
    description: String,
    url: String,
    version_added: String,
    unmaterializable: bool,
    known_time_zone_limitation_cast: bool,
}
