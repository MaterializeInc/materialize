// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Integration tests for Materialize server.

use std::error::Error;
use std::fs::{self};
use std::path::PathBuf;

pub mod util;

// Ensures that once a node is started with `--experimental`, it requires it on each restart.
#[test]
fn test_experimental_mode() -> Result<(), Box<dyn Error>> {
    fn run_test() -> Result<(), Box<dyn Error>> {
        // Create initial server.
        let _server = match util::start_server(
            util::Config::default()
                .data_directory("experimental_mode_test")
                .enable_experimental(),
        ) {
            Ok((s, _)) => s,
            Err(e) => {
                return Err(e);
            }
        };

        // Ensure restarting server fails without without experimental.
        match util::start_server(util::Config::default().data_directory("experimental_mode_test")) {
            Err(e) => {
                if !e
                    .to_string()
                    .contains("Materialize previously started with --experimental")
                {
                    return Err(e);
                }
            }
            Ok((_server, _)) => {
                return Err(
                    "Server should have required experimental mode, but started without it".into(),
                );
            }
        }

        // Ensure restarting server succeeds with experimental.
        let _server = match util::start_server(
            util::Config::default()
                .data_directory("experimental_mode_test")
                .enable_experimental(),
        ) {
            Ok((s, _)) => s,
            Err(e) => {
                return Err(e);
            }
        };
        Ok(())
    }

    fs::create_dir_all(&PathBuf::from("experimental_mode_test"))?;
    let res = run_test();
    fs::remove_dir_all(&PathBuf::from("experimental_mode_test"))?;
    res
}
