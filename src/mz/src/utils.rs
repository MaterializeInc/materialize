// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::process::exit;
use std::str::FromStr;
use std::time::Duration;

use indicatif::{ProgressBar, ProgressStyle};
use uuid::Uuid;

use crate::{ExitMessage, FronteggAPIToken};

/// Cloud providers and regions available.
#[derive(Debug, Clone, Copy)]
pub(crate) enum CloudProviderRegion {
    AwsUsEast1,
    AwsEuWest1,
}

/// Implementation to name the possible values and parse every option.
impl CloudProviderRegion {
    pub fn variants() -> [&'static str; 2] {
        ["aws/us-east-1", "aws/eu-west-1"]
    }

    /// Return the region name inside a cloud provider.
    pub fn region_name(self) -> &'static str {
        match self {
            CloudProviderRegion::AwsUsEast1 => "us-east-1",
            CloudProviderRegion::AwsEuWest1 => "eu-west-1",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ParseError;

impl FromStr for CloudProviderRegion {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "aws/us-east-1" => Ok(CloudProviderRegion::AwsUsEast1),
            "aws/eu-west-1" => Ok(CloudProviderRegion::AwsEuWest1),
            _ => Err(ParseError),
        }
    }
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
    progress_bar.enable_steady_tick(Duration::from_millis(120));
    progress_bar.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner} {msg}")
            .expect("template known to be valid")
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

pub(crate) type AppPassword = String;

pub(crate) trait FromTos {
    /// Turn a password into a Materialize app-password
    fn from_password(password: String) -> Self;

    /// Turn a profile into a Materialize app-password
    fn from_api_token(api_token: FronteggAPIToken) -> Self;

    /// Turns the app-password into an API token
    fn to_api_token(&self) -> Option<FronteggAPIToken>;
}

impl FromTos for AppPassword {
    fn from_password(password: String) -> Self {
        password
    }

    fn from_api_token(api_token: FronteggAPIToken) -> Self {
        ("mzp_".to_owned() + &api_token.client_id + &api_token.secret).replace('-', "")
    }

    fn to_api_token(&self) -> Option<FronteggAPIToken> {
        if self.len() != 68 || !self.starts_with("mzp_") {
            None
        } else {
            let client_id_uuid = Uuid::parse_str(&self[4..36]).ok();
            let secret_uuid = Uuid::parse_str(&self[36..68]).ok();

            match client_id_uuid {
                Some(client_id) => secret_uuid.map(|secret| FronteggAPIToken {
                    client_id: client_id.to_string(),
                    secret: secret.to_string(),
                }),
                None => None,
            }
        }
    }
}
