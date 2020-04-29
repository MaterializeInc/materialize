// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use structopt::StructOpt;
use url::Url;

#[derive(Clone, Debug, StructOpt)]
pub struct Args {
    /// The materialized host
    #[structopt(long, default_value = "localhost")]
    pub materialized_host: String,

    /// The materialized port
    #[structopt(long, default_value = "6875")]
    pub materialized_port: u16,

    /// The total number of queries to create
    #[structopt(long, default_value = "50")]
    pub query_count: usize,

    /// Whether or not to delete views before starting
    #[structopt(long)]
    pub preserve_views: bool,

    /// The fuzzer URL
    #[structopt(long, default_value = "https://api.jibson.dev/v0/smith")]
    pub fuzzer_url: String,
}

impl Args {
    pub fn mz_config(&self) -> MzConfig {
        MzConfig {
            host: self.materialized_host.clone(),
            port: self.materialized_port,
            preserve_views: self.preserve_views,
        }
    }

    pub fn fuzzer_config(&self) -> FuzzerConfig {
        FuzzerConfig {
            mz_config: self.mz_config(),
            fuzzer_url: Url::parse(&self.fuzzer_url).expect("fuzzer url expected to be valid URL"),
            query_count: self.query_count,
        }
    }
}

#[derive(Clone, Debug)]
pub struct MzConfig {
    pub host: String,
    pub port: u16,
    pub preserve_views: bool,
}

#[derive(Clone, Debug)]
pub struct FuzzerConfig {
    pub mz_config: MzConfig,
    pub fuzzer_url: Url,
    pub query_count: usize,
}
