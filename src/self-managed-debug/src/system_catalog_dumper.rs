// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Dumps catalog information to files.

use std::time::Duration;

use mz_ore::retry::{self, RetryResult};
use mz_ore::task::{self, JoinHandle};

pub fn spawn_internal_sql_port_forwarding_process(
    namespace: String,
    service_name: String,
    target_port: u16,
    local_port: u16,
    k8s_context: Option<String>,
) -> JoinHandle<()> {
    task::spawn(|| "port-forwarding", async move {
        // TODO: Make this message better
        println!("spawning port forwarding process");
        if let Err(err) = retry::Retry::default()
            .max_duration(Duration::from_secs(60))
            .retry_async(|retry_state| {
                let k8s_context = k8s_context.clone();
                let namespace = namespace.clone();
                let service_name = service_name.clone();
                let local_port = local_port.clone();
                let target_port = target_port.clone();

                async move {
                    let port_arg_str = format!("{}:{}", &local_port, &target_port);
                    let service_name_arg_str = format!("services/{}", &service_name);
                    let mut args = vec![
                        "port-forward",
                        &service_name_arg_str,
                        &port_arg_str,
                        "-n",
                        &namespace,
                    ];

                    if let Some(k8s_context) = &k8s_context {
                        args.extend(["--context", k8s_context]);
                    }

                    match tokio::process::Command::new("kubectl")
                        .args(args)
                        // Silence stdout/stderr
                        .stdout(std::process::Stdio::null())
                        .stderr(std::process::Stdio::null())
                        .output()
                        .await
                    {
                        Ok(output) => {
                            if !output.status.success() {
                                let retry_err_msg = format!(
                                    "Failed to port-forward{}: {}",
                                    retry_state.next_backoff.map_or_else(
                                        || "".to_string(),
                                        |d| format!(", retrying in {:?}", d)
                                    ),
                                    String::from_utf8_lossy(&output.stderr)
                                );
                                eprintln!("{}", retry_err_msg);

                                return RetryResult::RetryableErr(anyhow::anyhow!(retry_err_msg));
                            }
                        }
                        Err(err) => {
                            return RetryResult::RetryableErr(anyhow::anyhow!(
                                "Failed to port-forward: {}",
                                err
                            ));
                        }
                    }

                    RetryResult::Ok(())
                }
            })
            .await
        {
            eprintln!("{}", err);
        }
    })
}
