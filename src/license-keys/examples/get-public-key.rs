// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use aws_sdk_kms::config::Region;
use clap::Parser;

use mz_license_keys::get_pubkey_pem;

#[derive(clap::Parser)]
struct Opt {
    #[clap(long)]
    region: String,
    #[clap(long)]
    profile: String,
    #[clap(long)]
    key_id: String,
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

    let pubkey = get_pubkey_pem(&client, &opt.key_id).await.unwrap();

    print!("{pubkey}");
}
