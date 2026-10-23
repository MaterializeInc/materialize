// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.


use super::*;

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn jq_withheld_filters_are_undefined() {
    let calls = [
        ("env", "env/0"),
        ("now", "now/0"),
        ("localtime", "localtime/0"),
        (r#"strflocaltime("%H")"#, "strflocaltime/1"),
        ("input", "input/0"),
        ("inputs", "inputs/0"),
        (r#"strptime("%H")"#, "strptime/1"),
    ];
    // The logging filters are withheld and then defined again as no-ops.
    let silenced = ["debug_empty", "stderr_empty"];
    let mut covered: Vec<_> = calls
        .iter()
        .map(|(_, name)| name.split('/').next().unwrap())
        .chain(silenced)
        .collect();
    covered.sort();
    let mut withheld = WITHHELD.to_vec();
    withheld.sort();
    assert_eq!(covered, withheld);

    for (program, name) in calls {
        assert_eq!(
            compile(program).err(),
            Some(format!("undefined filter {name}")),
            "{program}"
        );
    }
    assert!(compile("$ENV").is_err());
}

