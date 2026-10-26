// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cell::RefCell;
use std::sync::atomic::AtomicU64;

use super::*;

const TIMEOUT: Duration = Duration::from_secs(20);
const NAMESPACE: &str = "lifetime";
const SERVICE: &str = "sleep";
type Events = BoxStream<'static, Result<ServiceEvent, anyhow::Error>>;

struct Fixture {
    config: ProcessOrchestratorConfig,
    root: PathBuf,
    processes: RefCell<Vec<(Pid, u64)>>,
}

impl Fixture {
    fn new(retain_on_shutdown: bool) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        // Reserve the actual metadata directory atomically, including across
        // concurrent test binaries and stale directories from interrupted runs.
        let (environment_id, root) = loop {
            let id = format!(
                "lifetime-test-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            );
            let root = env::temp_dir().join(format!("environmentd-{id}"));
            match std::fs::create_dir(&root) {
                Ok(()) => break (id, root),
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(e) => panic!("creating test directory: {e}"),
            }
        };
        Self {
            config: ProcessOrchestratorConfig {
                image_dir: "/bin".into(),
                suppress_output: true,
                environment_id,
                secrets_dir: root.join("secrets"),
                command_wrapper: Vec::new(),
                propagate_crashes: false,
                retain_on_shutdown,
                tcp_proxy: None,
                scratch_directory: root.join("scratch"),
            },
            root,
            processes: RefCell::new(Vec::new()),
        }
    }

    async fn owner(&self) -> ProcessOrchestrator {
        let mut owner = time::timeout(TIMEOUT, ProcessOrchestrator::new(self.config.clone()))
            .await
            .expect("creating orchestrator timed out")
            .unwrap();
        owner.launch_spec = LaunchSpec::Direct;
        owner
    }

    fn run_dir(&self) -> PathBuf {
        self.root.join(format!("{NAMESPACE}-{SERVICE}"))
    }

    fn identity(&self) -> (Pid, u64) {
        let identity = read_identity(&self.run_dir().join("0.pid"))
            .expect("Online service must have a valid PID file");
        self.processes.borrow_mut().push(identity);
        identity
    }

    async fn ensure_online(&self, namespace: &dyn NamespacedOrchestrator) -> (Events, (Pid, u64)) {
        let mut events = namespace.watch_services();
        namespace
            .ensure_service(
                SERVICE,
                ServiceConfig {
                    app_name: "lifetime-test".into(),
                    image: "sh".into(),
                    init_container_image: None,
                    args: Box::new(|_| vec!["-c".into(), "exec sleep 300".into()]),
                    ports: Vec::new(),
                    memory_limit: None,
                    memory_request: None,
                    cpu_limit: None,
                    cpu_request: None,
                    scale: NonZero::new(1).unwrap(),
                    labels: BTreeMap::new(),
                    annotations: BTreeMap::new(),
                    availability_zones: None,
                    other_replicas_selector: Vec::new(),
                    replicas_selector: Vec::new(),
                    disk_limit: Some(DiskLimit::ZERO),
                    node_selector: BTreeMap::new(),
                },
            )
            .unwrap();
        time::timeout(TIMEOUT, async {
            loop {
                let event = events.next().await.expect("watch ended").unwrap();
                if event.service_id == SERVICE
                    && event.process_id == 0
                    && event.status == ServiceStatus::Online
                {
                    break;
                }
            }
        })
        .await
        .expect("service did not become Online");
        (events, self.identity())
    }

    async fn drop_service(&self, namespace: &dyn NamespacedOrchestrator, identity: (Pid, u64)) {
        namespace.drop_service(SERVICE).unwrap();
        time::timeout(TIMEOUT, namespace.flush())
            .await
            .expect("drop_service did not finish")
            .unwrap();
        assert!(
            !self.run_dir().exists(),
            "drop must remove service metadata"
        );
        wait_stopped(identity).await;
        assert!(
            time::timeout(TIMEOUT, namespace.list_services())
                .await
                .unwrap()
                .unwrap()
                .is_empty()
        );
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // Also inspect the PID file when an assertion or Online timeout prevented
        // recording the child. Start times protect against killing reused PIDs.
        let mut identities = self.processes.borrow().clone();
        identities.extend(read_identity(&self.run_dir().join("0.pid")));
        for identity in identities {
            let mut system = System::new();
            if let Some(process) = live_process(&mut system, identity) {
                process.kill();
            }
        }
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn read_identity(path: &Path) -> Option<(Pid, u64)> {
    let contents = std::fs::read_to_string(path).ok()?;
    let mut lines = contents.lines();
    Some((lines.next()?.parse().ok()?, lines.next()?.parse().ok()?))
}

fn live_process(system: &mut System, (pid, start_time): (Pid, u64)) -> Option<&Process> {
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        true,
        ProcessRefreshKind::nothing(),
    );
    system.process(pid).filter(|process| {
        process.start_time() == start_time
            && process.status() != sysinfo::ProcessStatus::Zombie
            && process.status() != sysinfo::ProcessStatus::Dead
    })
}

fn assert_alive(identity: (Pid, u64)) {
    assert!(
        live_process(&mut System::new(), identity).is_some(),
        "retained process {} must still be alive",
        identity.0
    );
}

async fn wait_stopped(identity: (Pid, u64)) {
    time::timeout(TIMEOUT, async {
        let mut system = System::new();
        while live_process(&mut system, identity).is_some() {
            time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("process did not stop");
}

async fn shutdown(
    owner: ProcessOrchestrator,
    namespace: Arc<dyn NamespacedOrchestrator>,
    mut events: Events,
) {
    drop(namespace);
    drop(owner);
    // The worker and supervisors own the senders. Closure, not elapsed time,
    // proves that their futures have been disposed before checking the child.
    time::timeout(TIMEOUT, async {
        loop {
            match events.next().await.expect("watch ended without closure") {
                Ok(_) => continue,
                Err(error) => {
                    assert!(matches!(
                        error.downcast_ref::<broadcast::error::RecvError>(),
                        Some(broadcast::error::RecvError::Closed)
                    ));
                    break;
                }
            }
        }
    })
    .await
    .expect("orchestrator shutdown did not close the service watch");
}

#[mz_ore::test(tokio::test)]
async fn retained_children_survive_spawned_and_adopted_supervisors() {
    let fixture = Fixture::new(true);
    let owner = fixture.owner().await;
    let namespace = owner.namespace(NAMESPACE);
    let (events, original) = fixture.ensure_online(namespace.as_ref()).await;
    shutdown(owner, namespace, events).await;
    assert_alive(original);

    let owner = fixture.owner().await;
    let namespace = owner.namespace(NAMESPACE);
    let (events, adopted) = fixture.ensure_online(namespace.as_ref()).await;
    assert_eq!(
        adopted, original,
        "must adopt rather than replace the child"
    );
    shutdown(owner, namespace, events).await;
    assert_alive(original);

    let owner = fixture.owner().await;
    let namespace = owner.namespace(NAMESPACE);
    let (events, adopted) = fixture.ensure_online(namespace.as_ref()).await;
    assert_eq!(adopted, original);
    fixture.drop_service(namespace.as_ref(), original).await;
    shutdown(owner, namespace, events).await;
}

#[mz_ore::test(tokio::test)]
async fn non_retained_child_is_killed_on_shutdown() {
    let fixture = Fixture::new(false);
    let owner = fixture.owner().await;
    let namespace = owner.namespace(NAMESPACE);
    let (events, identity) = fixture.ensure_online(namespace.as_ref()).await;
    shutdown(owner, namespace, events).await;
    wait_stopped(identity).await;
}

#[mz_ore::test(tokio::test)]
async fn explicit_drop_kills_orphan_without_a_supervisor() {
    let fixture = Fixture::new(true);
    let owner = fixture.owner().await;
    let namespace = owner.namespace(NAMESPACE);
    let (events, identity) = fixture.ensure_online(namespace.as_ref()).await;
    shutdown(owner, namespace, events).await;
    assert_alive(identity);

    let owner = fixture.owner().await;
    let namespace = owner.namespace(NAMESPACE);
    let events = namespace.watch_services();
    // Do not ensure the service: only its persisted metadata is available.
    fixture.drop_service(namespace.as_ref(), identity).await;
    shutdown(owner, namespace, events).await;
}
