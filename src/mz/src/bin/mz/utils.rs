// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::{Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE, USER_AGENT};
use reqwest::{Client, ClientBuilder};
use std::time::Duration;

/// Create a standard client with common
/// header values for all requests.
pub fn new_client() -> Result<Client> {
    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, HeaderValue::from_static("reqwest"));
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

    ClientBuilder::new()
        .default_headers(headers)
        .build()
        .context("failed to create client")
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

// Validator function that accepts only ASCII letters, digits, underscores, and dashes
pub(crate) fn ascii_validator(val: &str) -> Result<(), String> {
    // Iterate over the arg value
    for c in val.chars() {
        // Check if the character is an ASCII letter or digit
        if !c.is_ascii_alphanumeric() {
            // If not, check if it is an underscore or dash
            if c != '_' && c != '-' {
                // If not, return Err with a message
                return Err(String::from(
                    "The value must contain only ASCII letters, digits, underscores, and dashes",
                ));
            }
        }
    }
    // If all characters are valid, return Ok
    Ok(())
}
