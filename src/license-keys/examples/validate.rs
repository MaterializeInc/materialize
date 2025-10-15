// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use clap::Parser;

use mz_license_keys::validate;

#[derive(clap::Parser)]
struct Opt {
    #[clap(long)]
    license_key_file: String,
}

fn main() {
    let opt = Opt::parse();

    let license_key = std::fs::read_to_string(&opt.license_key_file).unwrap();

    let validated_key = validate(license_key.trim()).unwrap();

    println!("valid license key");
    println!("{validated_key:?}");
}
