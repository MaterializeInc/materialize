// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf, time::Duration};

    use assert_cmd::{Command, assert::Assert, cargo_bin};
    use mz::ui::OptionalStr;
    use mz_frontegg_auth::AppPassword;
    use serde::{Deserialize, Serialize};
    use tabled::settings::Style;
    use tabled::{Table, Tabled};
    use uuid::Uuid;

    fn get_config_path() -> PathBuf {
        let config_file_path = dirs::cache_dir().unwrap();
        let mut config_path_buf = config_file_path.to_path_buf();
        config_path_buf.push("materialize");

        if !config_path_buf.exists() {
            fs::create_dir_all(config_path_buf.clone()).expect("Failed to create directory");
        }

        config_path_buf.push("mz_test_config.toml");
        config_path_buf
    }

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

    /// Returns a command to execute mz.
    fn cmd() -> Command {
        let mut cmd = Command::new(cargo_bin!("mz"));
        cmd.timeout(Duration::from_secs(30));
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
    fn init_config_file(file_name: &str, mock: bool) {
        let mut config_path = get_config_path();
        config_path.set_file_name(file_name);
        let main_config_file = format!(
            r#"
            "profile" = "default"
            "vault" = "inline"

            [profiles.default]
            app-password = "{}"
            region = "aws/us-east-1"
            vault = "keychain"

            [profiles.alternative]
            app-password = "{}"
            region = "aws/eu-west-1"
            vault = "inline"
        "#,
            get_password(mock),
            get_password(mock)
        );

        fs::write(&config_path, main_config_file).expect("Failed to write config file");
    }

    /// Tests local commands that do not requires interacting with any API.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `pipe2` on OS `linux`
    fn test_local() {
        let unique_id = Uuid::new_v4().to_string();
        let config_file_name = format!("test_local_config_{}.toml", unique_id);
        init_config_file(&config_file_name, true);
        let mut config_path_buf = get_config_path();
        config_path_buf.set_file_name(&config_file_name);
        let config_path = config_path_buf.to_str().unwrap();
        eprintln!("Config path: {}", config_path);

        // Test - `mz config`
        //
        // Assert `mz config get profile`
        let assert = cmd()
            .arg("config")
            .arg("get")
            .arg("profile")
            .arg("--config")
            .arg(config_path)
            .assert()
            .success();

        let output = output_to_string(assert);
        assert!(output.trim() == "default");

        // Asert `mz profile init` fails.
        let assert = cmd()
            .arg("profile")
            .arg("init")
            .arg("--config")
            .arg(config_path)
            .assert()
            .failure();

        let output = output_to_string(assert);
        assert!(
            output.trim()
                == "The profile name 'default' already exists. You can either use 'mz profile init -f' to replace it or 'mz profile init --profile <PROFILE>' to choose another name."
        );

        let assert = cmd()
            .arg("profile")
            .arg("init")
            .arg("--profile")
            .arg("alternative")
            .arg("--config")
            .arg(config_path)
            .assert()
            .failure();

        let output = output_to_string(assert);
        assert!(
            output.trim()
                == "The profile name 'alternative' already exists. You can either use 'mz profile init -f' to replace it or 'mz profile init --profile <PROFILE>' to choose another name."
        );

        // Asert `mz profile config get region --profile alternative`
        let binding = cmd()
            .arg("profile")
            .arg("config")
            .arg("get")
            .arg("region")
            .arg("--profile")
            .arg("alternative")
            .arg("--config")
            .arg(config_path)
            .assert()
            .success();

        let output = output_to_string(binding);
        assert!(output.trim() == "aws/eu-west-1");

        // Asert `mz profile config set region random --profile alternative`
        cmd()
            .arg("profile")
            .arg("config")
            .arg("set")
            .arg("admin-endpoint")
            .arg("wrongUrl")
            .arg("--profile")
            .arg("alternative")
            .arg("--config")
            .arg(config_path)
            .assert()
            .success();

        let binding = cmd()
            .arg("profile")
            .arg("config")
            .arg("get")
            .arg("admin-endpoint")
            .arg("--profile")
            .arg("alternative")
            .arg("--config")
            .arg(config_path)
            .assert()
            .success();
        let output = output_to_string(binding);
        assert!(output.trim() == "wrongUrl");

        cmd()
            .arg("profile")
            .arg("config")
            .arg("remove")
            .arg("admin-endpoint")
            .arg("--profile")
            .arg("alternative")
            .arg("--config")
            .arg(config_path)
            .assert()
            .success();

        let binding = cmd()
            .arg("profile")
            .arg("config")
            .arg("get")
            .arg("admin-endpoint")
            .arg("--profile")
            .arg("alternative")
            .arg("--config")
            .arg(config_path)
            .assert()
            .success();

        let output = output_to_string(binding);
        assert!(output.trim() == "<unset>");

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
                value: mz::ui::OptionalStr(Some("inline")),
            },
        ];
        let expected_command_output = Table::new(vec).with(Style::psql()).to_string();
        let assert = cmd()
            .arg("config")
            .arg("list")
            .arg("--config")
            .arg(config_path)
            .assert()
            .success();

        let output = output_to_string(assert);
        assert!(output.trim() == expected_command_output.trim());

        // Assert `mz config set profile` + `mz config get profile` output:
        cmd()
            .arg("config")
            .arg("set")
            .arg("profile")
            .arg("random")
            .arg("--config")
            .arg(config_path)
            .assert()
            .success();

        let assert = cmd()
            .arg("config")
            .arg("get")
            .arg("profile")
            .arg("--config")
            .arg(config_path)
            .assert()
            .success();

        let output = output_to_string(assert);
        assert!(output.trim() == "random");

        // Assert `mz config remove profile`
        cmd()
            .arg("config")
            .arg("remove")
            .arg("profile")
            .arg("--config")
            .arg(config_path)
            .assert()
            .success();

        let assert = cmd()
            .arg("config")
            .arg("get")
            .arg("profile")
            .arg("--config")
            .arg(config_path)
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
                .arg("--config")
                .arg(config_path)
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
                value: "keychain",
            },
        ];
        let expected_command_output = Table::new(vec).with(Style::psql()).to_string();
        let binding = cmd()
            .arg("profile")
            .arg("config")
            .arg("list")
            .arg("--config")
            .arg(config_path)
            .assert()
            .success();

        let output = output_to_string(binding);
        assert!(output.trim() == expected_command_output.trim());

        let valid_test_names = ["ValidName", "also_valid", "1234"];

        valid_test_names.iter().for_each(|name| {
            // The test will fail because the profile does not exist.
            // What we want to corroborate here is that the name is valid.
            // If `mz` says the profile does not exist, it means that
            // the validation went okay.
            let binding = cmd()
                .arg("profile")
                .arg("--profile")
                .arg(name)
                .arg("config")
                .arg("list")
                .arg("--config")
                .arg(config_path)
                .assert()
                .failure();

            let output = output_to_string(binding);
            assert!(output.contains(&format!(
                "Error: The profile '{}' is missing in the configuration file.",
                name
            )));
        });

        let invalid_test_names = ["-Invalid", "also invalid", "Valid-Name_123"];

        invalid_test_names.iter().for_each(|name| {
            let binding = cmd()
            .arg("profile")
            .arg(&format!("--profile=\"{}\"", name))
            .arg("config")
            .arg("list")
            .arg("--config")
            .arg(config_path)
            .assert()
            .failure();

            let output = String::from_utf8_lossy(&binding.get_output().stderr).to_string();
            assert!(output.contains("The profile name must consist of only ASCII letters, ASCII digits, underscores, and dashes."));
        });
    }
}
