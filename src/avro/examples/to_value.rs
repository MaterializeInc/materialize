// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use avro::to_value;
use failure::Error;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
struct Test {
    a: i64,
    b: String,
}

fn main() -> Result<(), Error> {
    let test = Test {
        a: 27,
        b: "foo".to_owned(),
    };
    println!("{:?}", to_value(test)?);

    Ok(())
}
