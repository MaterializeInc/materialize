// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Exercise the kept incremental index scenario through native runtimes in both
//! Timely topologies. Each child owns its workers until process exit.

use std::process::Stdio;
use std::time::Duration;

use mz_persist_types::PersistLocation;
use tokio::io::AsyncReadExt;
use tokio::process::Command;

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn native_renderer_scenarios() {
    for (unified, script) in [false, true]
        .into_iter()
        .flat_map(|unified| ["index.spec", "subscribe.spec"].map(|script| (unified, script)))
    {
        let location = PersistLocation::new_in_mem();
        let mut child = Command::new(env!("CARGO_BIN_EXE_headless-driver"))
            .env("PERSIST_BLOB_URL", location.blob_uri.as_str())
            .env("PERSIST_CONSENSUS_URL", location.consensus_uri.as_str())
            .env("DRIVER_PUBSUB_BIND", "127.0.0.1:0")
            .env("DRIVER_WORKERS", "2")
            .env("DRIVER_UNIFIED", unified.to_string())
            .env(
                "DRIVER_SCRIPT",
                std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../../test/clusterd-test-driver/scripts")
                    .join(script),
            )
            .env_remove("REWRITE")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .expect("start native driver");
        let mut stdout = child.stdout.take().unwrap();
        let mut stderr = child.stderr.take().unwrap();
        let (mut out, mut err) = (Vec::new(), Vec::new());
        let (status, read_out, read_err) = tokio::join!(
            async {
                match tokio::time::timeout(Duration::from_secs(120), child.wait()).await {
                    Ok(status) => status.map_err(|e| e.to_string()),
                    Err(_) => {
                        child.kill().await.unwrap();
                        Err("native driver timed out".into())
                    }
                }
            },
            stdout.read_to_end(&mut out),
            stderr.read_to_end(&mut err),
        );
        read_out.unwrap();
        read_err.unwrap();
        assert!(
            matches!(&status, Ok(status) if status.success()),
            "unified={unified}, script={script}: {status:?}\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&out),
            String::from_utf8_lossy(&err)
        );
    }
}
