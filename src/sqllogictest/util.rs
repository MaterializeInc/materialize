// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

pub fn indent(s: &str, n: usize) -> String {
    let space = " ".repeat(n);
    let s = s.replace("\n", &format!("\n{}", space));
    space + &s
}
