// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub fn indent(s: &str, n: usize) -> String {
    let space = " ".repeat(n);
    let s = s.replace('\n', &format!("\n{}", space));
    space + &s
}
