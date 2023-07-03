// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// BEGIN LINT CONFIG
// DO NOT EDIT. Automatically generated by bin/gen-lints.
// Have complaints about the noise? See the note in misc/python/materialize/cli/gen-lints.py first.
#![allow(clippy::style)]
#![allow(clippy::complexity)]
#![allow(clippy::large_enum_variant)]
#![allow(clippy::mutable_key_type)]
#![allow(clippy::stable_sort_primitive)]
#![allow(clippy::map_entry)]
#![allow(clippy::box_default)]
#![warn(clippy::bool_comparison)]
#![warn(clippy::clone_on_ref_ptr)]
#![warn(clippy::no_effect)]
#![warn(clippy::unnecessary_unwrap)]
#![warn(clippy::dbg_macro)]
#![warn(clippy::todo)]
#![warn(clippy::wildcard_dependencies)]
#![warn(clippy::zero_prefixed_literal)]
#![warn(clippy::borrowed_box)]
#![warn(clippy::deref_addrof)]
#![warn(clippy::double_must_use)]
#![warn(clippy::double_parens)]
#![warn(clippy::extra_unused_lifetimes)]
#![warn(clippy::needless_borrow)]
#![warn(clippy::needless_question_mark)]
#![warn(clippy::needless_return)]
#![warn(clippy::redundant_pattern)]
#![warn(clippy::redundant_slicing)]
#![warn(clippy::redundant_static_lifetimes)]
#![warn(clippy::single_component_path_imports)]
#![warn(clippy::unnecessary_cast)]
#![warn(clippy::useless_asref)]
#![warn(clippy::useless_conversion)]
#![warn(clippy::builtin_type_shadow)]
#![warn(clippy::duplicate_underscore_argument)]
#![warn(clippy::double_neg)]
#![warn(clippy::unnecessary_mut_passed)]
#![warn(clippy::wildcard_in_or_patterns)]
#![warn(clippy::crosspointer_transmute)]
#![warn(clippy::excessive_precision)]
#![warn(clippy::overflow_check_conditional)]
#![warn(clippy::as_conversions)]
#![warn(clippy::match_overlapping_arm)]
#![warn(clippy::zero_divided_by_zero)]
#![warn(clippy::must_use_unit)]
#![warn(clippy::suspicious_assignment_formatting)]
#![warn(clippy::suspicious_else_formatting)]
#![warn(clippy::suspicious_unary_op_formatting)]
#![warn(clippy::mut_mutex_lock)]
#![warn(clippy::print_literal)]
#![warn(clippy::same_item_push)]
#![warn(clippy::useless_format)]
#![warn(clippy::write_literal)]
#![warn(clippy::redundant_closure)]
#![warn(clippy::redundant_closure_call)]
#![warn(clippy::unnecessary_lazy_evaluations)]
#![warn(clippy::partialeq_ne_impl)]
#![warn(clippy::redundant_field_names)]
#![warn(clippy::transmutes_expressible_as_ptr_casts)]
#![warn(clippy::unused_async)]
#![warn(clippy::disallowed_methods)]
#![warn(clippy::disallowed_macros)]
#![warn(clippy::disallowed_types)]
#![warn(clippy::from_over_into)]
// END LINT CONFIG

#[cfg(test)]
mod tests {
    use std::{fs, time::Duration};

    use assert_cmd::{assert::Assert, Command};
    use mz::{config_file::ConfigFile, ui::OptionalStr};
    use mz_frontegg_auth::AppPassword;
    use serde::{Deserialize, Serialize};
    use tabled::{Style, Table, Tabled};

    /// Returns the password to use in the tests.
    /// The password must start with the `mzp_` prefix.
    fn get_password(mock: bool) -> String {
        // TODO: Remove mock after understanding how to get an CI app-password
        if mock {
            return AppPassword {
                client_id: uuid::Uuid::new_v4(),
                secret_key: uuid::Uuid::new_v4(),
            }
            .to_string();
        }
        std::env::var("CI_PASSWORD").unwrap()
    }

    /// Returns the admin endpoint for the testing environment.
    fn get_admin_endpoint() -> String {
        std::env::var("CI_ADMIN_ENDPOINT").unwrap()
    }

    /// Returns the cloud endpoint for the testing environment.
    fn get_cloud_endpoint() -> String {
        std::env::var("CI_CLOUD_ENDPOINT").unwrap()
    }

    /// Returns a command to execute mz.
    fn cmd() -> Command {
        let mut cmd = Command::cargo_bin("mz").unwrap();
        cmd.timeout(Duration::from_secs(10));
        cmd
    }

    /// Returns the assert's output as a string.
    fn output_to_string(assert: Assert) -> String {
        let output = assert.get_output();
        let stdout = String::from_utf8_lossy(&output.stdout).to_string();

        stdout
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `pipe2` on OS `linux`
    fn test_version() {
        // We don't make assertions about the build SHA because caching in CI can
        // cause the test binary and `mz` to have different embedded SHAs.
        let expected_version = mz::BUILD_INFO.version;
        assert!(!expected_version.is_empty());
        cmd()
            .arg("-V")
            .assert()
            .success()
            .stdout(format!("mz {}\n", expected_version));
    }

    /// Writes a valid config file at the [ConfigFile] default path.
    fn init_config_file(mock: bool) {
        let main_config_file = format!(
            r#"
            "profile" = "default"

            [profiles.default]
            app-password = "{}"
            region = "aws/us-east-1"
        "#,
            get_password(mock)
        );

        let config_file_path = ConfigFile::default_path().unwrap();
        let config_dir = config_file_path.parent().expect("Failed to get parent directory");

        if !config_dir.exists() {
            fs::create_dir_all(config_dir).expect("Failed to create directory");
        }

        fs::write(config_file_path, main_config_file).unwrap();
    }

    /// Tests local commands that do not requires interacting with any API.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `pipe2` on OS `linux`
    fn test_local() {
        init_config_file(true);

        // Test - `mz config`
        //
        // Assert `mz config get profile`
        let assert = cmd()
            .arg("config")
            .arg("get")
            .arg("profile")
            .assert()
            .success();

        let output = output_to_string(assert);
        assert!(output.trim() == "default");

        // Assert `mz config get list` output:
        //
        //  Name    | Value
        // ---------+---------
        //  profile | default
        //  vault   | <unset>
        //
        #[derive(Deserialize, Serialize, Tabled)]
        pub struct ConfigParam<'a> {
            #[tabled(rename = "Name")]
            name: &'a str,
            #[tabled(rename = "Value")]
            value: OptionalStr<'a>,
        }

        let vec = vec![
            ConfigParam {
                name: "profile",
                value: mz::ui::OptionalStr(Some("default")),
            },
            ConfigParam {
                name: "vault",
                value: mz::ui::OptionalStr(Some("<unset>")),
            },
        ];
        let expected_command_output = Table::new(vec).with(Style::psql()).to_string();
        let assert = cmd().arg("config").arg("list").assert().success();

        let output = output_to_string(assert);
        assert!(output.trim() == expected_command_output.trim());

        // Assert `mz config set profile` + `mz config get profile output:
        cmd()
            .arg("config")
            .arg("set")
            .arg("profile")
            .arg("random")
            .assert()
            .success();

        let assert = cmd()
            .arg("config")
            .arg("get")
            .arg("profile")
            .assert()
            .success();

        let output = output_to_string(assert);
        assert!(output.trim() == "random");

        // Assert `mz config remove profile`
        cmd()
            .arg("config")
            .arg("remove")
            .arg("profile")
            .assert()
            .success();

        let assert = cmd()
            .arg("config")
            .arg("get")
            .arg("profile")
            .assert()
            .success();

        let output = output_to_string(assert);
        assert!(output.trim() == "default");

        // Test - `mz profile`
        //
        // Assert `mz profile list`
        let binding = output_to_string(
            cmd()
                .arg("profile")
                .arg("config")
                .arg("get")
                .arg("app-password")
                .assert()
                .success(),
        );
        let app_password = binding.trim();

        #[derive(Deserialize, Serialize, Tabled)]
        pub struct ProfileConfigParam<'a> {
            #[tabled(rename = "Name")]
            name: &'a str,
            #[tabled(rename = "Value")]
            value: &'a str,
        }

        let vec = vec![
            ProfileConfigParam {
                name: "admin-endpoint",
                value: "<unset>",
            },
            ProfileConfigParam {
                name: "app-password",
                value: app_password,
            },
            ProfileConfigParam {
                name: "cloud-endpoint",
                value: "<unset>",
            },
            ProfileConfigParam {
                name: "region",
                value: "aws/us-east-1",
            },
            ProfileConfigParam {
                name: "vault",
                value: "<unset>",
            },
        ];
        let expected_command_output = Table::new(vec).with(Style::psql()).to_string();
        let binding = cmd()
            .arg("profile")
            .arg("config")
            .arg("list")
            .assert()
            .success();

        let output = output_to_string(binding);
        assert!(output.trim() == expected_command_output.trim());
    }

    /// TODO: Re-enable after understanding how to get an CI app-password, admin endpoint and cloud endpoint.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `pipe2` on OS `linux`
    #[ignore]
    fn test_e2e() {
        init_config_file(false);
        // Set the admin-endpoint and cloud-endpoint
        cmd()
            .arg("profile")
            .arg("config")
            .arg("set")
            .arg("admin-endpoint")
            .arg(get_admin_endpoint())
            .assert()
            .success();

        cmd()
            .arg("profile")
            .arg("config")
            .arg("set")
            .arg("cloud-endpoint")
            .arg(get_cloud_endpoint())
            .assert()
            .success();

        // Assert `mz profile config get region`
        let binding = cmd()
            .arg("profile")
            .arg("config")
            .arg("get")
            .arg("region")
            .assert()
            .success();

        let output = output_to_string(binding);

        assert!(output.trim() == "aws/us-east-1");

        // Assert `mz profile config set region`
        cmd()
            .arg("profile")
            .arg("config")
            .arg("set")
            .arg("region")
            .arg("aws/eu-west-1")
            .assert()
            .success();

        let binding = cmd()
            .arg("profile")
            .arg("config")
            .arg("get")
            .arg("region")
            .assert()
            .success();

        let output = output_to_string(binding);

        assert!(output.trim() == "aws/eu-west-1");

        // Test - `mz app-password`
        //
        // Assert `mz app-password create`
        let description = uuid::Uuid::new_v4();

        let binding = cmd()
            .arg("app-password")
            .arg("create")
            .arg(&description.to_string())
            .assert()
            .success();
        let output = output_to_string(binding);

        assert!(output.starts_with("mzp_"));

        // Assert `mz app-password list`

        let binding = cmd().arg("app-password").arg("list").assert().success();
        let output = output_to_string(binding);

        assert!(output.contains(&description.to_string()));

        // Test - `mz secrets`
        //
        // Assert `mz secret create`
        let description = format!("SAFE_TO_DELETE_{}", uuid::Uuid::new_v4());

        // Secrets
        cmd()
            .arg("secret")
            .arg("create")
            .arg(description.clone())
            .write_stdin("decode('c2VjcmV0Cg==', 'base64')")
            .assert()
            .success();

        // Assert `mz secret create -f`
        cmd()
            .arg("secret")
            .arg("create")
            .arg(description)
            .arg("force")
            .write_stdin("decode('c2VjcmV0Cg==', 'base64')")
            .assert()
            .success();

        // Test - `mz user`
        //
        // Assert `mz user create` + `mz user list`
        let name = format!("SAFE_TO_DELETE_+{}", uuid::Uuid::new_v4());
        let email = format!("{}@materialize.com", name);

        cmd()
            .arg("user")
            .arg("create")
            .arg(email.clone())
            .arg(name)
            .assert()
            .success();

        let binding = cmd()
            .arg("user")
            .arg("list")
            .args(vec!["--format", "json"])
            .assert()
            .success();
        let output = output_to_string(binding);

        assert!(output.contains(&email));

        // Assert `mz user remove` + `mz user list`
        cmd()
            .arg("user")
            .arg("remove")
            .arg(email.clone())
            .assert()
            .success();

        let binding = cmd()
            .arg("user")
            .arg("list")
            .args(vec!["--format", "json"])
            .assert()
            .success();

        let output = output_to_string(binding);

        assert!(!output.contains(&email));

        // Test - `mz region`
        //
        // Assert `mz region list`
        #[derive(Clone, Copy, Deserialize, Serialize, Tabled)]
        pub struct Region<'a> {
            #[tabled(rename = "Region")]
            region: &'a str,
            #[tabled(rename = "Status")]
            status: &'a str,
        }

        let vec = vec![
            Region {
                region: "aws/eu-west-1",
                status: "enabled",
            },
            Region {
                region: "aws/us-east-1",
                status: "enabled",
            },
        ];
        let expected_command_output = Table::new(vec.clone()).with(Style::psql()).to_string();

        let binding = cmd().arg("region").arg("list").assert().success();
        let output = output_to_string(binding);

        assert!(output.trim() == expected_command_output.trim());

        // Assert `mz region list` using JSON
        let binding = cmd()
            .args(vec!["--format", "json"])
            .arg("region")
            .arg("list")
            .assert()
            .success();
        let output = output_to_string(binding);
        let expected_command_output = serde_json::to_string(&vec).unwrap();

        assert!(output.trim() == expected_command_output.trim());

        // TODO:
        // Assert `mz region list` using CSV
        // let binding = cmd().args(vec!["--format", "CSV"]).arg("region").arg("list").assert().success();
        // let output = output_to_string(binding);

        // Assert `mz region show`
        // The path does not always contains the pg_isready binary.
        // let binding = cmd().arg("region").arg("show").env("PATH", "/opt/homebrew/bin/").assert().success();
        // cmd()
        //     .arg("region")
        //     .arg("enable")
        //     // .env("PATH", "/opt/homebrew/bin/")
        //     .assert()
        //     .success();

        let binding = cmd()
            .arg("region")
            .arg("show")
            // .env("PATH", "/opt/homebrew/bin/")
            .assert()
            .success();
        let output = output_to_string(binding);

        assert!(output.trim().starts_with("Healthy: \tyes"));

        // Test - `mz sql`
        //
        // Assert `mz sql -- -q -c "SELECT 1"
        cmd()
            .arg("sql")
            .arg("--")
            .arg("-q")
            .arg("-c")
            .arg("\"SELECT 1\"")
            .assert()
            .success();

        // TODO: Remove an app-password. Breaks the CLI config. The same if the app-password is invalid.
        // TODO: Add more tests for config_set and config_remove when you implement the actual commands.
        // TODO: Profile init + Profile remove
    }
}
