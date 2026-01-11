// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Generate function documentation from macros.

use std::collections::BTreeMap;

use itertools::Itertools;
use mz_expr::func::{BinaryFuncKind, FuncDoc, UnaryFuncKind};
use serde::Serialize;

fn main() {
    let mut categories: BTreeMap<String, Category<_>> = BTreeMap::default();

    for function in BinaryFuncKind::kinds()
        .into_iter()
        .filter_map(|f| f.func_doc())
        .chain(UnaryFuncKind::kinds().into_iter().map(|f| f.func_doc()))
    {
        lint_function(&function);
        categories
            .entry(function.category.to_string())
            .or_insert_with(|| Category {
                r#type: function.category.to_string(),
                functions: Default::default(),
            })
            .functions
            .push(function);
    }

    for category in categories.values_mut() {
        category.functions.sort();
        lint_category(category);
    }

    let categories = categories.into_values().collect::<Vec<_>>();

    let json = serde_json::to_string_pretty(&categories).expect("can serialize");
    println!("{json}\n");
}

fn lint_category(category: &Category<FuncDoc>) {
    for (a, b) in category.functions.iter().tuple_windows() {
        assert_ne!(a.unique_name, b.unique_name, "Duplicate function name");
    }
}

fn lint_function(function: &FuncDoc) {
    if function.category.is_empty() {
        panic!("Uncategorized function: {:?}", function.unique_name);
    }
    if function.category == "Uncategorized" {
        eprintln!(
            "Function categorized as Uncategorized: {:?}",
            function.unique_name
        );
    }
    if function.description.is_empty() {
        eprintln!("Function missing description: {:?}", function.unique_name);
    }
}

#[derive(Debug, Serialize, Ord, PartialOrd, Eq, PartialEq)]
struct Category<T> {
    r#type: String,
    functions: Vec<T>,
}
