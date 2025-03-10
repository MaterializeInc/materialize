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
use uuid::Uuid;

use mz_license_keys::{make_license_key, ExpirationBehavior};

#[derive(clap::Parser)]
struct Opt {
    #[clap(long)]
    region: String,
    #[clap(long)]
    profile: String,
    #[clap(long)]
    key_id: String,
    #[clap(long)]
    organization_id: String,
    #[clap(long, default_value_t = Uuid::new_v4().to_string())]
    environment_id: String,
    #[clap(long)]
    max_credit_consumption_rate: f64,
    #[clap(long, default_value_t = 365 * 24 * 60 * 60)]
    validity_secs: u64,
}

#[tokio::main]
async fn main() {
    let opt = Opt::parse();

    let config = mz_aws_util::defaults()
        .region(Region::new(opt.region))
        .profile_name(opt.profile)
        .load()
        .await;
    let client = aws_sdk_kms::Client::new(&config);

    let license_key = make_license_key(
        &client,
        &opt.key_id,
        Duration::from_secs(opt.validity_secs),
        opt.organization_id,
        opt.environment_id,
        opt.max_credit_consumption_rate,
        false,
        ExpirationBehavior::Warn,
    )
    .await
    .unwrap();

    println!("{license_key}");
}
