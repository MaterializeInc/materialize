// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::process::exit;

use indicatif::{ProgressBar, ProgressStyle};

use crate::ExitMessage;

use clap::ArgEnum;

/// Cloud providers and regions available.
#[derive(Debug, Clone, ArgEnum)]
pub(crate) enum CloudProviderRegion {
    AwsUsEast1,
    AwsEuWest1,
}

/// Implementation to name the possible values and parse every option.
impl CloudProviderRegion {
    pub fn variants() -> [&'static str; 2] {
        ["aws/us-east-1", "aws/eu-west-1"]
    }
    pub fn parse(region: String) -> CloudProviderRegion {
        match region.as_str() {
            "aws/us-east-1" => CloudProviderRegion::AwsUsEast1,
            "aws/eu-west-1" => CloudProviderRegion::AwsEuWest1,
            _ => panic!("Unknown region."),
        }
    }
    pub fn parse_region(region: &str) -> &'static str {
        match region {
            "aws/us-east-1" => "us-east-1",
            "aws/eu-west-1" => "eu-west-1",
            _ => panic!("Unknown region."),
        }
    }
    pub fn parse_enum_region(region: CloudProviderRegion) -> &'static str {
        match region {
            CloudProviderRegion::AwsUsEast1 => "us-east-1",
            CloudProviderRegion::AwsEuWest1 => "eu-west-1",
        }
    }
    //-----------------
    // Unused parsers
    //-----------------
    //
    // pub fn parse_provider(region: &str) -> &'static str {
    //     match region {
    //         "aws/us-east-1" => "aws",
    //         "aws/eu-west-1" => "aws",
    //         _ => panic!("Unknown provider.")
    //     }
    // }
    // pub fn parse_enum(region: CloudProviderRegion) -> &'static str {
    //     match region {
    //         CloudProviderRegion::AwsUsEast1 => "aws/us-east-1",
    //         CloudProviderRegion::AwsEuWest1 => "aws/eu-west-1",
    //     }
    // }
    // pub fn parse_enum_provider(region: CloudProviderRegion) -> &'static str {
    //     match region {
    //         CloudProviderRegion::AwsUsEast1 => "aws",
    //         CloudProviderRegion::AwsEuWest1 => "aws",
    //     }
    // }
}

/// Trim lines. Useful when reading input data.
pub(crate) fn trim_newline(s: &mut String) {
    if s.ends_with('\n') {
        s.pop();
        if s.ends_with('\r') {
            s.pop();
        }
    }
}

/// Print a loading spinner with a particular message til finished.
pub(crate) fn run_loading_spinner(message: String) -> ProgressBar {
    let progress_bar = ProgressBar::new_spinner();
    progress_bar.enable_steady_tick(120);
    progress_bar.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner} {msg}")
            // For more spinners check out the cli-spinners project:
            // https://github.com/sindresorhus/cli-spinners/blob/master/spinners.json
            .tick_strings(&["⣾", "⣽", "⣻", "⢿", "⡿", "⣟", "⣯", "⣷", ""]),
    );

    progress_bar.set_message(message);

    progress_bar
}

/// Standard function to exit the process when there is any type of error
pub(crate) fn exit_with_fail_message(message: ExitMessage) -> ! {
    match message {
        ExitMessage::String(string_message) => println!("{}", string_message),
        ExitMessage::Str(str_message) => println!("{}", str_message),
    }
    exit(1);
}
