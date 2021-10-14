// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! coordtest attempts to be a deterministic Coordinator tester using datadriven
//! test files. Many parts of dataflow run in their own tokio task and we aren't
//! able to observe their output until we bump a timestamp and see if new data
//! are available. For example, the FILE source on mac polls every 100ms, making
//! writing a fully deterministic test difficult. Instead coordtest attempts to
//! provide enough knobs that it can test interesting cases without race
//! conditions.
//!
//! The coordinator is started with logical compaction disabled. If you want to
//! add it to an index (to allow sinces to advance past 0), use `ALTER INDEX`.
//!
//! The following datadriven directives are supported:
//! - `sql`: Executes the SQL using transaction rules similar to the
//!   simple pgwire protocol in a new session (that is, multiple `sql`
//!   directives are not in the same session). Output is formatted
//!   [`ExecuteResponse`](coord::ExecuteResponse). The input can contain the
//!   string `<TEMP>` which will be replaced with a temporary directory.
//! - `wait-sql`: Executes all SQL in a retry loop (with 5s timeout which
//!   will panic) until all datums returned (all columns in all rows in all
//!   statements) are `true`. Prior to each attempt, all pending feedback
//!   messages from the dataflow server are sent to the Coordinator. After each
//!   failed attempt, the timestamp is incremented by 1 to give any new data an
//!   opportunity to be observed.
//! - `set-upper-filter`: One per line, sets uppers to be filtered, which will
//!   prevent them from being sent during `sql` or `wait-sql` directives. No
//!   output.
//! - `async-sql`: Requires a `session=name` argument. Creates a named session,
//!   and executes the provided statements similarly to `sql`, except that the
//!   results are not immediately returned. Instead, await the results using the
//!   `await-sql` directive naming this session. The input can contain the
//!   string `<TEMP>` which will be replaced with a temporary directory when
//!   returned with `await-sql`. No output.
//! - `async-cancel`: Requires a `session=name` argument. Cancels the named
//!   session. No output.
//! - `await-sql`: Requires a `session=name` argument. Awaits the results of the
//!   named session. Output is formatted
//!   [`ExecuteResponse`](coord::ExecuteResponse). If the input to the awaited
//!   session contained the string `<TEMP>`, it will be replaced with a
//!   temporary directory.
//! - `update-upper`: Sends a batch of
//!   [`FrontierUppers`](dataflow::WorkerFeedback::FrontierUppers) to the
//!   Coordinator. Input is one update per line of the format
//!   `database.schema.item N` where N is some numeric timestamp. No output.
//! - `inc-timestamp`: Increments the timestamp by number in the input. No
//!   output.
//! - `create-file`: Requires a `name=filename` argument. Creates and truncates
//!   a file with the specified name in the TEMP directory with contents as the
//!   input. No output.
//! - `append-file`: Same as `create-file`, but appends.
//! - `print-catalog`: Outputs the catalog. Generally for debugging.
//!
//! The `sql` and `wait-sql` directives wait in a loop for the corresponding
//! PeekResponse to complete. After each iteration the global timestamp is
//! incremented and the feedback queue drained (except for messages from the
//! previous `set-upper-filter`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::future::Future;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::anyhow;
use futures::future::FutureExt;
use ore::metrics::MetricsRegistry;
use tempfile::{NamedTempFile, TempDir};
use tokio::sync::mpsc;

use coord::{
    session::{EndTransactionAction, Session},
    Client, ExecuteResponse, SessionClient, StartupResponse,
};
use dataflow::WorkerFeedback;
use dataflow_types::PeekResponse;
use expr::GlobalId;
use ore::thread::JoinOnDropHandle;
use repr::{Datum, Timestamp};
use timely::progress::change_batch::ChangeBatch;

/// CoordTest works by creating a Coordinator with mechanisms to control
/// when it receives messages. The dataflow server is started with a
/// single worker, but it's feedback channel into the Coordinator is
/// buffered (`dataflow_feedback_rx`), and only sent to the Coordinator
/// (`coord_feedback_tx`) when specified, allowing us to control when uppers
/// and sinces advance.
//
// The field order matters a lot here so the various threads/tasks are shut
// down without ever panicing.
pub struct CoordTest {
    coord_feedback_tx: mpsc::UnboundedSender<dataflow::Response>,
    client: Option<Client>,
    _handle: JoinOnDropHandle<()>,
    dataflow_feedback_rx: mpsc::UnboundedReceiver<dataflow::Response>,
    // Keep a queue of messages in the order received from dataflow_feedback_rx so
    // we can safely modify or inject things and maintain original message order.
    queued_feedback: Vec<dataflow::Response>,
    _catalog_file: NamedTempFile,
    temp_dir: TempDir,
    uppers: HashMap<GlobalId, Timestamp>,
    timestamp: Arc<Mutex<u64>>,
    exclude_uppers: HashSet<GlobalId>,
    _verbose: bool,
    _metrics_registry: MetricsRegistry,
    persisted_sessions: HashMap<String, (SessionClient, StartupResponse)>,
    deferred_results: HashMap<String, Vec<ExecuteResponse>>,
}

impl CoordTest {
    pub async fn new() -> anyhow::Result<Self> {
        let catalog_file = NamedTempFile::new()?;
        let metrics_registry = MetricsRegistry::new();
        let (handle, client, coord_feedback_tx, dataflow_feedback_rx, timestamp) =
            coord::serve_debug(catalog_file.path(), metrics_registry.clone());
        let coordtest = CoordTest {
            _handle: handle,
            client: Some(client),
            coord_feedback_tx,
            dataflow_feedback_rx,
            _catalog_file: catalog_file,
            temp_dir: tempfile::tempdir().unwrap(),
            uppers: HashMap::new(),
            _verbose: std::env::var_os("COORDTEST_VERBOSE").is_some(),
            timestamp,
            exclude_uppers: HashSet::new(),
            _metrics_registry: metrics_registry,
            queued_feedback: Vec::new(),
            persisted_sessions: HashMap::new(),
            deferred_results: HashMap::new(),
        };
        Ok(coordtest)
    }

    async fn connect(&self) -> anyhow::Result<(SessionClient, StartupResponse)> {
        let conn_client = self.client.as_ref().unwrap().new_conn()?;
        let session = Session::new(conn_client.conn_id(), "materialize".into());
        Ok(conn_client.startup(session).await?)
    }

    fn rewrite_query(&self, query: &str) -> String {
        let path = self.temp_dir.path().to_str().unwrap();
        let query = query.replace("<TEMP>", path);
        query
    }

    // This very odd signature is some of us figured out as a way to achieve
    // this. Is there a better way to do this?
    async fn with_sc_inner<F, T>(&mut self, session_name: Option<String>, f: F) -> T
    where
        F: for<'a> FnOnce(&'a mut SessionClient) -> std::pin::Pin<Box<dyn Future<Output = T> + 'a>>,
    {
        let (mut sc, sr) = self.connect().await.unwrap();
        let r = f(&mut sc).await;
        match session_name {
            Some(session_name) => {
                let r = self.persisted_sessions.insert(session_name, (sc, sr));
                assert!(r.is_none(), "duplicate named sessions started");
            }
            None => sc.terminate().await,
        }
        r
    }

    /// Provide a [`SessionClient`] to `f`, but terminate the client after
    /// executing `f`.
    async fn with_sc<F, T>(&mut self, f: F) -> T
    where
        F: for<'a> FnOnce(&'a mut SessionClient) -> std::pin::Pin<Box<dyn Future<Output = T> + 'a>>,
    {
        self.with_sc_inner(None, f).await
    }

    /// Rather than terminating the [`SessionClient`] after executing `f`, store
    /// it on `self` using some name.
    async fn with_persisted_sc<F, T>(&mut self, session_name: String, f: F) -> T
    where
        F: for<'a> FnOnce(&'a mut SessionClient) -> std::pin::Pin<Box<dyn Future<Output = T> + 'a>>,
    {
        self.with_sc_inner(Some(session_name), f).await
    }

    fn enqueue_feedback(&mut self) {
        loop {
            if let Some(Some(msg)) = self.dataflow_feedback_rx.recv().now_or_never() {
                self.queued_feedback.push(msg);
            } else {
                return;
            }
        }
    }

    // Drains messages from the queue into coord, extracting and requeueing
    // excluded uppers.
    fn drain(&mut self) {
        self.enqueue_feedback();
        let mut to_send = vec![];
        let mut to_queue = vec![];
        let msgs: Vec<_> = self.queued_feedback.drain(..).collect();
        for mut msg in msgs {
            // Filter out requested ids.
            if let WorkerFeedback::FrontierUppers(uppers) = &mut msg.message {
                // Requeue excluded uppers so future wait-sql directives don't always have to
                // specify the same exclude list forever.
                let mut requeue = uppers.clone();
                requeue.retain(|(id, _data)| self.exclude_uppers.contains(id));
                if !requeue.is_empty() {
                    to_queue.push(dataflow::Response {
                        worker_id: msg.worker_id,
                        message: WorkerFeedback::FrontierUppers(requeue),
                    });
                }
                uppers.retain(|(id, _data)| !self.exclude_uppers.contains(id));
            }
            to_send.push(msg);
        }
        for msg in to_send {
            self.coord_feedback_tx.send(msg).unwrap();
        }
        self.queued_feedback = to_queue;
    }

    // Processes PeekResponse messages until rows has a response.
    async fn wait_for_peek(
        &mut self,
        mut rows: std::pin::Pin<Box<dyn Future<Output = PeekResponse>>>,
    ) -> PeekResponse {
        loop {
            if let futures::task::Poll::Ready(rows) = futures::poll!(rows.as_mut()) {
                return rows;
            }
            // Bump the timestamp. This is necessary because sources ingest at varying
            // rates and we need to allow sinces to move forward so we can see new data.
            self.inc_timestamp(1);
            self.drain();
            // Wait a bit for dataflow to have some time to work.
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    async fn make_catalog(&mut self) -> Catalog {
        let catalog = self
            .with_sc(|sc| Box::pin(async move { sc.dump_catalog().await.unwrap() }))
            .await;
        let catalog: BTreeMap<String, coord::catalog::Database> =
            serde_json::from_str(&catalog).unwrap();
        Catalog(catalog)
    }

    fn inc_timestamp(&mut self, v: u64) {
        let mut ts = self.timestamp.lock().unwrap();
        *ts += v;
    }
}

#[derive(Debug)]
struct Catalog(BTreeMap<String, coord::catalog::Database>);

impl Catalog {
    fn get(&self, name: &str) -> GlobalId {
        let mut name = name.split('.');
        let database = self.0.get(name.next().unwrap()).unwrap();
        let schema = database.schemas.get(name.next().unwrap()).unwrap();
        let item_name = name.next().unwrap();
        let id: GlobalId = schema
            .items
            .get(item_name)
            .unwrap_or_else(|| panic!("{} not found, have: {:?}", item_name, schema.items.keys()))
            .clone();
        assert!(name.next().is_none());
        id
    }
}

async fn sql(sc: &mut SessionClient, query: String) -> anyhow::Result<Vec<ExecuteResponse>> {
    let stmts = sql_parser::parser::parse_statements(&query)?;
    let num_stmts = stmts.len();
    let mut results = vec![];
    for stmt in stmts {
        let _ = sc.start_transaction(Some(num_stmts)).await;
        // Bind this statement to the empty portal with no params.
        sc.declare("".into(), stmt, vec![]).await?;
        // Execute the empty portal.
        results.push(sc.execute("".into()).await?);
    }
    sc.end_transaction(EndTransactionAction::Commit).await?;
    Ok(results)
}

pub async fn walk(dir: &str) {
    datadriven::walk_async(dir, |tf| run_test(tf)).await;
}

pub async fn run_test(mut tf: datadriven::TestFile) -> datadriven::TestFile {
    let ct = std::rc::Rc::new(std::cell::RefCell::new(CoordTest::new().await.unwrap()));
    tf.run_async(|tc| {
        let mut ct = ct.borrow_mut();
        if ct._verbose {
            println!("{} {:?}: {}", tc.directive, tc.args, tc.input);
        }
        async move {
            let res: String = match tc.directive.as_str() {
                "sql" => {
                    let query = ct.rewrite_query(&tc.input);
                    let results = ct
                        .with_sc(|sc| Box::pin(async move { sql(sc, query).await }))
                        .await
                        .unwrap();
                    let mut strs = vec![];
                    for r in results {
                        strs.push(match r {
                            ExecuteResponse::SendingRows(rows) => {
                                format!("{:#?}", ct.wait_for_peek(rows).await)
                            }
                            r => format!("{:#?}", r),
                        });
                    }
                    strs.push("".to_string());
                    strs.join("\n")
                }
                "wait-sql" => {
                    let start = Instant::now();
                    loop {
                        ct.drain();
                        let query = ct.rewrite_query(&tc.input);
                        let results = ct
                            .with_sc(|sc| Box::pin(async move { sql(sc, query).await }))
                            .await;
                        let mut failed = Ok(());
                        match results {
                            Ok(result) => {
                                for r in result {
                                    match r {
                                        ExecuteResponse::SendingRows(rows) => {
                                            match ct.wait_for_peek(rows).await {
                                                PeekResponse::Rows(rows) => {
                                                    for row in rows {
                                                        for col in row.iter() {
                                                            if col != Datum::True {
                                                                failed =
                                                                    Err(anyhow!("datum != true"));
                                                            }
                                                        }
                                                    }
                                                }
                                                r => {
                                                    failed = Err(anyhow!("{:?}", r));
                                                }
                                            }
                                        }
                                        _ => panic!("expected SendingRows"),
                                    };
                                }
                            }
                            Err(err) => {
                                failed = Err(err);
                            }
                        };
                        match failed {
                            Ok(_) => {
                                break;
                            }
                            Err(err) => {
                                if start.elapsed() > Duration::from_secs(5) {
                                    panic!("timeout: {}", err);
                                }
                            }
                        }
                    }
                    "".into()
                }
                "async-sql" => {
                    let session_name = tc.args["session"][0].clone();
                    let query = ct.rewrite_query(&tc.input);
                    let results = ct
                        .with_persisted_sc(session_name.clone(), |sc| {
                            Box::pin(async move { sql(sc, query).await })
                        })
                        .await
                        .unwrap();

                    ct.deferred_results.insert(session_name, results);

                    "".into()
                }
                "async-cancel" => {
                    let session_name = tc.args["session"][0].clone();

                    assert!(tc.input.is_empty(), "async-cancel only takes an argument");

                    let (sc, sr) = ct
                        .persisted_sessions
                        .get_mut(&session_name)
                        .expect("named session persisted");

                    let conn_id = sc.session().conn_id();
                    let secret_key = sr.secret_key;

                    let _ = ct
                        .with_sc(|sc| Box::pin(sc.cancel_request(conn_id, secret_key)))
                        .await;
                    "".into()
                }
                "await-sql" => {
                    let session_name = tc.args["session"][0].clone();

                    assert!(tc.input.is_empty(), "await-sql only takes an argument");

                    let (sc, _sr) = ct
                        .persisted_sessions
                        .remove(&session_name)
                        .expect("named session persisted");

                    let results = ct
                        .deferred_results
                        .remove(&session_name)
                        .expect("named session async");

                    let mut strs = vec![];
                    for r in results {
                        strs.push(match r {
                            ExecuteResponse::SendingRows(rows) => {
                                format!("{:#?}", ct.wait_for_peek(rows).await)
                            }
                            r => format!("{:#?}", r),
                        });
                    }

                    sc.terminate().await;

                    strs.push("".to_string());
                    strs.join("\n")
                }
                "set-upper-filter" => {
                    let catalog = ct.make_catalog().await;
                    ct.exclude_uppers = tc.input.trim().lines().map(|s| catalog.get(s)).collect();
                    "".into()
                }
                "update-upper" => {
                    let catalog = ct.make_catalog().await;
                    let mut updates = vec![];
                    for line in tc.input.lines() {
                        let mut line = line.split_whitespace();
                        let id = catalog.get(line.next().unwrap());
                        let ts: Timestamp = line.next().unwrap().parse().unwrap();
                        assert!(line.next().is_none());
                        // A ts <= 1 won't advance any sinces (which use `upper-1`).
                        assert!(ts > 1);
                        let upper = ct.uppers.entry(id).or_insert(0);
                        let mut batch: ChangeBatch<Timestamp> = ChangeBatch::new_from(*upper, -1);
                        assert!(ts >= *upper);
                        *upper = ts;
                        batch.update(ts, 1);
                        updates.push((id, batch));
                    }
                    ct.coord_feedback_tx
                        .send(dataflow::Response {
                            worker_id: 0,
                            message: WorkerFeedback::FrontierUppers(updates),
                        })
                        .unwrap();
                    "".into()
                }
                "inc-timestamp" => {
                    let inc: u64 = tc.input.trim().parse().unwrap();
                    ct.inc_timestamp(inc);
                    "".into()
                }
                "create-file" => {
                    let path = ct.temp_dir.path().join(tc.args["name"][0].clone());
                    let mut f = File::create(path).unwrap();
                    if !tc.input.is_empty() {
                        f.write_all(tc.input.as_bytes()).unwrap();
                    }
                    "".into()
                }
                "append-file" => {
                    let path = ct.temp_dir.path().join(tc.args["name"][0].clone());
                    let mut f = OpenOptions::new().append(true).open(path).unwrap();
                    f.write_all(tc.input.as_bytes()).unwrap();
                    "".into()
                }
                "print-catalog" => {
                    let catalog = ct.make_catalog().await;
                    format!("{:#?}\n", catalog)
                }
                _ => panic!("unknown directive {}", tc.directive),
            };
            res
        }
    })
    .await;
    tf
}
