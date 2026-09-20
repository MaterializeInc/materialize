// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::process::Stdio;

use mz_compute_client::logging::{LogVariant, LoggingConfig, TimelyLog};
use mz_compute_client::protocol::command::InstanceConfig;
use mz_persist_client::PersistLocation;
use mz_repr::{GlobalId, Timestamp};
use mz_service::client::GenericClient;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::time::timeout;

use super::*;

#[mz_ore::test(tokio::test)]
async fn replica_progress_outlives_query_factory() {
    const CHILD: &str = "MZ_COMPUTE_REPLICA_PROGRESS_TEST_CHILD";
    const TEST: &str = "server::replica_tests::replica_progress_outlives_query_factory";

    if std::env::var_os(CHILD).is_none() {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", TEST, "--nocapture"])
            .env(CHILD, "1")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut stdout = child.stdout.take().unwrap();
        let mut stderr = child.stderr.take().unwrap();
        let (mut out, mut err) = (Vec::new(), Vec::new());
        let (status, read_out, read_err) = tokio::join!(
            async {
                match timeout(Duration::from_secs(60), child.wait()).await {
                    Ok(status) => status.map_err(|error| error.to_string()),
                    Err(_) => {
                        child.kill().await.unwrap();
                        Err("child timed out after 60 seconds".to_string())
                    }
                }
            },
            stdout.read_to_end(&mut out),
            stderr.read_to_end(&mut err),
        );
        assert!(
            matches!(&status, Ok(status) if status.success()),
            "{status:?}\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&out),
            String::from_utf8_lossy(&err),
        );
        read_out.unwrap();
        read_err.unwrap();
        return;
    }

    // Timely workers run for the process lifetime. Neither successful assertions nor
    // panics may unwind into a runtime destructor that joins those infinite workers.
    let panic_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        panic_hook(info);
        std::process::exit(1);
    }));

    let registry = MetricsRegistry::new();
    let mut server = serve(
        TimelyConfig {
            workers: 2,
            addresses: vec!["127.0.0.1:0".to_string()],
            ..Default::default()
        },
        ComputeRuntimeRole::Solo,
        true,
        &registry,
        Arc::new(PersistClientCache::new_no_metrics()),
        TxnsContext::default(),
        Arc::new(TracingHandle::disabled()),
        ComputeInstanceContext {
            scratch_directory: None,
            worker_core_affinity: false,
            connection_context: ConnectionContext::for_tests(Arc::new(
                mz_secrets::InMemorySecretsController::new(),
            )),
        },
        Vec::new(),
    )
    .await
    .unwrap();
    let mut replica = server.take_replica().expect("process zero owns control");
    assert!(server.take_replica().is_none());
    let factory = server.client_builder();
    drop(server);

    let mut rejected = factory();
    assert!(
        rejected
            .send(ComputeCommand::Hello {
                nonce: Uuid::new_v4(),
            })
            .await
            .is_err()
    );
    drop(rejected);

    let mut query = factory();
    query
        .send(ComputeCommand::HelloQuery {
            nonce: Uuid::new_v4(),
        })
        .await
        .unwrap();
    assert!(
        timeout(Duration::from_millis(100), query.recv())
            .await
            .is_err(),
        "query must wait for native instance initialization"
    );

    let log_id = GlobalId::System(1);
    replica.send(ComputeCommand::CreateInstance(Box::new(InstanceConfig {
        logging: LoggingConfig {
            interval: Duration::from_millis(10),
            enable_logging: true,
            log_logging: false,
            index_logs: [(LogVariant::Timely(TimelyLog::Operates), log_id)].into(),
        },
        expiration_offset: None,
        peek_stash_persist_location: PersistLocation::new_in_mem(),
        arrangement_dictionary_compression: false,
        initial_config: Default::default(),
    })));
    replica.send(ComputeCommand::InitializationComplete);
    assert!(matches!(
        query.recv().await.unwrap(),
        Some(ComputeResponse::QueryReady)
    ));
    let before = logging_progress_after(&mut replica, log_id, Timestamp::MIN).await;
    drop(query);
    drop(factory);

    // Require a response to a command sent after connection teardown, not just
    // output already queued before it. Logging then continues beyond that point.
    let after_drop = before.step_forward();
    replica.send(ComputeCommand::AllowCompaction {
        id: log_id,
        frontier: Antichain::from_elem(after_drop),
    });
    loop {
        let response = replica
            .recv()
            .await
            .unwrap()
            .expect("native endpoint closed");
        if let ComputeResponse::Frontiers(id, frontiers) = response {
            assert_eq!(id, log_id);
            if frontiers
                .read_frontier
                .is_some_and(|frontier| frontier.elements() == [after_drop])
            {
                break;
            }
        }
    }
    let after = logging_progress_after(&mut replica, log_id, after_drop).await;
    assert!(after > before);

    // The native endpoint is the remaining runtime owner. Exiting, rather than
    // dropping it, pins the production process-lifetime API without leaking in
    // the parent test process.
    std::process::exit(0);
}

async fn logging_progress_after(
    replica: &mut ReplicaCompute,
    log_id: GlobalId,
    after: Timestamp,
) -> Timestamp {
    // Exercise ReplicaCompute::recv's production PartitionedComputeState with two
    // workers. This does not establish the timing of individual worker responses.
    loop {
        let response = replica
            .recv()
            .await
            .unwrap()
            .expect("native endpoint closed");
        assert!(
            matches!(
                response,
                ComputeResponse::Frontiers(..) | ComputeResponse::Status(_)
            ),
            "query response leaked into maintained progress: {response:?}",
        );
        if let ComputeResponse::Frontiers(id, frontiers) = response {
            assert_eq!(id, log_id);
            if let Some(frontier) = frontiers.write_frontier {
                let [time] = frontier.elements() else {
                    panic!("logging collection unexpectedly completed: {frontier:?}");
                };
                if *time > after {
                    return *time;
                }
            }
        }
    }
}
