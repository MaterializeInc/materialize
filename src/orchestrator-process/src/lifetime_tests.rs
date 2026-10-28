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
        namespace.ensure_service(SERVICE, service_config()).unwrap();
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

fn service_config() -> ServiceConfig {
    ServiceConfig {
        inherit_kubernetes_owner_references: true,
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
    }
}

fn tcp_port(hint: u16) -> ServicePort {
    ServicePort {
        name: "client".into(),
        port_hint: hint,
        requires_tcp: true,
    }
}

fn tcp_config(port: &ServicePort) -> ServiceConfig {
    let mut config = service_config();
    config.ports = vec![port.clone()];
    config
}

#[mz_ore::test(tokio::test)]
async fn tcp_assignment_discovery_and_queued_lifecycle() {
    let fixture = Fixture::new(true);
    let owner = fixture.owner().await;
    let namespace = owner.namespace(NAMESPACE);
    let scale = NonZero::new(1).unwrap();
    let occupied = StdTcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = tcp_port(occupied.local_addr().unwrap().port());
    assert!(namespace.service_addresses(SERVICE, scale, &port).is_err());
    assert!(
        !fixture.run_dir().exists(),
        "discovery must not create metadata"
    );
    let first = namespace
        .ensure_service(SERVICE, tcp_config(&port))
        .unwrap();
    let address = first.addresses(&port.name)[0].clone();
    assert_ne!(address, occupied.local_addr().unwrap().to_string());
    assert_eq!(
        namespace.service_addresses(SERVICE, scale, &port).unwrap(),
        first.addresses(&port.name)
    );
    assert_eq!(
        address.parse::<SocketAddr>().unwrap().ip(),
        IpAddr::from([127, 0, 0, 1])
    );

    // The first child has not started, but its assigned port is already reserved.
    let reserved = tcp_port(address.parse::<SocketAddr>().unwrap().port());
    let mut other_config = tcp_config(&reserved);
    other_config.scale = NonZero::new(2).unwrap();
    other_config.ports.push(ServicePort {
        name: "peer".into(),
        port_hint: 0,
        requires_tcp: false,
    });
    let other = namespace.ensure_service("other", other_config).unwrap();
    let other_addresses = other.addresses(&port.name);
    assert_eq!(other_addresses.len(), 2);
    assert_ne!(other_addresses[0], other_addresses[1]);
    assert!(other_addresses.iter().all(|a| a != &address));
    assert_eq!(
        namespace
            .service_addresses("other", NonZero::new(2).unwrap(), &reserved)
            .unwrap(),
        other_addresses,
    );
    assert_eq!(
        other.addresses("peer"),
        (0..2)
            .map(|i| socket_path(&fixture.root.join(format!("{NAMESPACE}-other")), "peer", i))
            .collect::<Vec<_>>()
    );
    namespace.drop_service("other").unwrap();
    namespace.drop_service(SERVICE).unwrap();
    let last = namespace
        .ensure_service(SERVICE, tcp_config(&port))
        .unwrap();
    // Discovery also runs outside the lifecycle owner's mutex, including from
    // other processes. Once ensure returns, queued cleanup cannot hide its port.
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let stop = Arc::new(AtomicBool::new(false));
    let discoverer = {
        let namespace = Arc::clone(&namespace);
        let port = port.clone();
        let expected = last.addresses(&port.name);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            assert_eq!(
                namespace.service_addresses(SERVICE, scale, &port).unwrap(),
                expected
            );
            ready_tx.send(()).expect("test waits for discoverer");
            let deadline = std::time::Instant::now() + TIMEOUT;
            while !stop.load(Ordering::SeqCst) && std::time::Instant::now() < deadline {
                assert_eq!(
                    namespace.service_addresses(SERVICE, scale, &port).unwrap(),
                    expected
                );
                std::thread::yield_now();
            }
        })
    };
    ready_rx.recv_timeout(TIMEOUT).expect("discoverer started");
    time::timeout(TIMEOUT, namespace.flush())
        .await
        .unwrap()
        .unwrap();
    stop.store(true, Ordering::SeqCst);
    discoverer
        .join()
        .expect("published endpoint remains discoverable");
    assert_eq!(
        namespace.service_addresses(SERVICE, scale, &port).unwrap(),
        last.addresses(&port.name)
    );
    let recorded = std::fs::read(fixture.run_dir().join(TCP_ASSIGNMENTS_FILE)).unwrap();
    assert!(
        namespace
            .service_addresses(SERVICE, NonZero::new(2).unwrap(), &port)
            .is_err()
    );
    assert_eq!(
        recorded,
        std::fs::read(fixture.run_dir().join(TCP_ASSIGNMENTS_FILE)).unwrap()
    );
    namespace.drop_service(SERVICE).unwrap();
    time::timeout(TIMEOUT, namespace.flush())
        .await
        .unwrap()
        .unwrap();
    assert!(!fixture.run_dir().exists());
    assert!(namespace.service_addresses(SERVICE, scale, &port).is_err());
}

#[mz_ore::test(tokio::test)]
async fn tcp_hint_is_preferred() {
    let fixture = Fixture::new(true);
    let owner = fixture.owner().await;
    let namespace = owner.namespace(NAMESPACE);
    let listener = StdTcpListener::bind(("127.0.0.1", 0)).unwrap();
    let hint = listener.local_addr().unwrap();
    drop(listener);
    let port = tcp_port(hint.port());
    let service = namespace
        .ensure_service(SERVICE, tcp_config(&port))
        .unwrap();
    assert_eq!(service.addresses(&port.name), vec![hint.to_string()]);
    namespace.drop_service(SERVICE).unwrap();
    time::timeout(TIMEOUT, namespace.flush())
        .await
        .unwrap()
        .unwrap();
}

fn echo_config(port: &ServicePort) -> ServiceConfig {
    let mut config = tcp_config(port);
    config.image = "/usr/bin/python3".into();
    let name = port.name.clone();
    config.args = Box::new(move |assignments| {
        let address = assignments.listen_addrs[&name].clone();
        assert_eq!(assignments.peer_addresses(&name), vec![address.clone()]);
        vec![
            "-c".into(),
            r#"
import socket, sys
host, port = sys.argv[1].split(':')
s = socket.socket()
s.bind((host, int(port)))
s.listen()
c, _ = s.accept()
while True:
    data = c.recv(4096)
    if not data:
        break
    c.sendall(data)
"#
            .into(),
            address,
        ]
    });
    config
}

#[mz_ore::test(tokio::test)]
async fn direct_tcp_connection_survives_owner_and_adoption() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let fixture = Fixture::new(true);
    let owner = fixture.owner().await;
    let namespace = owner.namespace(NAMESPACE);
    let events = namespace.watch_services();
    let port = tcp_port(0);
    let service = namespace
        .ensure_service(SERVICE, echo_config(&port))
        .unwrap();
    let address = service.addresses(&port.name)[0].clone();
    let mut connection = time::timeout(TIMEOUT, async {
        loop {
            match tokio::net::TcpStream::connect(&address).await {
                Ok(stream) => break stream,
                Err(_) => time::sleep(Duration::from_millis(10)).await,
            }
        }
    })
    .await
    .expect("TCP child did not listen");
    // Connecting can precede the supervisor's PID-file write.
    time::timeout(TIMEOUT, async {
        while read_identity(&fixture.run_dir().join("0.pid")).is_none() {
            time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let original = fixture.identity();
    shutdown(owner, namespace, events).await;
    assert_alive(original);
    time::timeout(TIMEOUT, async {
        connection.write_all(b"before").await.unwrap();
        let mut reply = [0; 6];
        connection.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"before");
    })
    .await
    .unwrap();

    let owner = fixture.owner().await;
    let namespace = owner.namespace(NAMESPACE);
    assert_eq!(
        namespace
            .service_addresses(SERVICE, NonZero::new(1).unwrap(), &port)
            .unwrap(),
        vec![address.clone()]
    );
    let mut events = namespace.watch_services();
    let adopted = namespace
        .ensure_service(SERVICE, echo_config(&port))
        .unwrap();
    assert_eq!(adopted.addresses(&port.name), vec![address]);
    time::timeout(TIMEOUT, async {
        while events.next().await.unwrap().unwrap().status != ServiceStatus::Online {}
    })
    .await
    .unwrap();
    assert_eq!(fixture.identity(), original);
    time::timeout(TIMEOUT, async {
        connection.write_all(b"after").await.unwrap();
        let mut reply = [0; 5];
        connection.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"after");
    })
    .await
    .unwrap();
    fixture.drop_service(namespace.as_ref(), original).await;
    assert!(
        namespace
            .service_addresses(SERVICE, NonZero::new(1).unwrap(), &port)
            .is_err()
    );
}

#[mz_ore::test(tokio::test)]
async fn launch_logging_omits_credential_arguments() {
    struct Capture {
        text: Arc<Mutex<String>>,
        next_span: AtomicU64,
    }
    impl tracing::Subscriber for Capture {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(self.next_span.fetch_add(1, Ordering::Relaxed))
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            struct Fields<'a>(&'a mut String);
            impl tracing::field::Visit for Fields<'_> {
                fn record_debug(&mut self, _: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                    use std::fmt::Write;
                    write!(self.0, "{value:?}\n").expect("write to string");
                }
            }
            event.record(&mut Fields(&mut self.text.lock().expect("capture lock")));
        }
    }
    let captured = Arc::new(Mutex::new(String::new()));
    let _subscriber = tracing::subscriber::set_default(Capture {
        text: Arc::clone(&captured),
        next_span: AtomicU64::new(1),
    });
    let fixture = Fixture::new(true);
    let owner = fixture.owner().await;
    let namespace = owner.namespace(NAMESPACE);
    let mut events = namespace.watch_services();
    let mut config = service_config();
    config.image = "/usr/bin/python3".into();
    const SECRET: &str = "launch-secret-sentinel-do-not-log";
    config.args = Box::new(|_| {
        vec![
            "-c".into(),
            "import time; time.sleep(10000)".into(),
            format!("--persist-consensus-url=postgres://user:{SECRET}@localhost/db"),
        ]
    });
    namespace
        .ensure_service(SERVICE, config)
        .expect("launch fixture");
    time::timeout(TIMEOUT, async {
        while events.next().await.expect("events").expect("event").status != ServiceStatus::Online {
        }
    })
    .await
    .expect("child became online");
    let identity = fixture.identity();
    let text = captured.lock().expect("capture lock").clone();
    assert!(
        text.contains("launching"),
        "must capture the actual launch event"
    );
    assert!(
        !text.contains(SECRET),
        "launch arguments must not reach tracing"
    );
    fixture.drop_service(namespace.as_ref(), identity).await;
}
