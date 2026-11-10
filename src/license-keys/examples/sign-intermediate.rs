// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use aws_sdk_kms::config::Region;
use clap::Parser;

use mz_license_keys::make_intermediate_key;

#[derive(clap::Parser)]
struct Opt {
    #[clap(long)]
    region: String,
    #[clap(long)]
    profile: String,
    /// The root key to sign the intermediate certificate with.
    #[clap(long)]
    key_id: String,
    /// Name recorded in the certificate, e.g. the stack it is issued for.
    #[clap(long)]
    name: String,
    /// PEM file containing the intermediate public key.
    #[clap(long)]
    public_key_file: String,
    #[clap(long, default_value_t = 365 * 24 * 60 * 60)]
    validity_secs: u64,
}

#[tokio::main]
async fn main() {
    let opt = Opt::parse();

    let public_key_pem = std::fs::read_to_string(&opt.public_key_file).unwrap();

    let config = mz_aws_util::defaults()
        .region(Region::new(opt.region))
        .profile_name(opt.profile)
        .load()
        .await;
    let client = aws_sdk_kms::Client::new(&config);

    let cert = make_intermediate_key(
        &client,
        &opt.key_id,
        Duration::from_secs(opt.validity_secs),
        opt.name,
        public_key_pem,
    )
    .await
    .unwrap();

    println!("{cert}");
}
