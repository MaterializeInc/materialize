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
//! - `sql`: Executes the SQL using transaction rules similar to the simple
//!   pgwire protocol in a new session (that is, multiple `sql` directives are
//!   not in the same session). Output is formatted
//!   [`ExecuteResponse`](mz_coord::ExecuteResponse). The input can contain the
//!   string `<TEMP>` which will be replaced with a temporary directory.
//! - `wait-sql`: Executes all SQL in a retry loop (with 5s timeout which will
//!   panic) until all datums returned (all columns in all rows in all
//!   statements) are `true`. Prior to each attempt, all pending feedback
//!   messages from the dataflow server are sent to the Coordinator. Messages
//!   for specified items can be skipped (but requeued) by specifying
//!   `exclude-uppers=database.schema.item` as an argument. After each failed
//!   attempt, the timestamp is incremented by 1 to give any new data an
//!   opportunity to be observed.
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
//!   [`ExecuteResponse`](mz_coord::ExecuteResponse). If the input to the awaited
//!   session contained the string `<TEMP>`, it will be replaced with a
//!   temporary directory.
//! - `update-upper`: Sends a batch of
//!   [`FrontierUppers`](mz_dataflow_types::client::ComputeResponse::FrontierUppers) to the
//!   Coordinator. Input is one update per line of the format
//!   `database.schema.item N` where N is some numeric timestamp. No output.
//! - `inc-timestamp`: Increments the timestamp by number in the input. No
//!   output.
//! - `create-file`: Requires a `name=filename` argument. Creates and truncates
//!   a file with the specified name in the TEMP directory with contents as the
//!   input. No output.
//! - `append-file`: Same as `create-file`, but appends.
//! - `print-catalog`: Outputs the catalog. Generally for debugging.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::future::Future;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::anyhow;
use async_trait::async_trait;
use futures::future::FutureExt;
use mz_dataflow_types::client::{ComputeResponse, Response};
use mz_dataflow_types::sources::AwsExternalId;
use tempfile::TempDir;
use tokio::sync::mpsc;
use tokio::sync::Mutex as TokioMutex;

use mz_build_info::DUMMY_BUILD_INFO;
use mz_coord::session::{EndTransactionAction, Session};
use mz_coord::{ExecuteResponse, PersistConfig, SessionClient, StartupResponse};
use mz_dataflow_types::PeekResponseUnary;
use mz_expr::GlobalId;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::NowFn;
use mz_repr::{Datum, Timestamp};
use timely::progress::change_batch::ChangeBatch;

/// CoordTest works by creating a Coordinator with mechanisms to control when it
/// receives messages. The dataflow server is started with a single worker, but
/// it's feedback channel is controlled by the InterceptingDataflowClient,
/// allowing us to control when uppers and sinces advance.
//
// The field order matters a lot here so the various threads/tasks are shut
// down without ever panicing.
pub struct CoordTest {
    dataflow_client: InterceptingDataflowClient<mz_dataflow_types::client::LocalClient>,
    coord_client: mz_coord::Client,
    _coord_handle: mz_coord::Handle,
    _dataflow_server: mz_dataflow::Server,
    // Keep a queue of messages in the order received from dataflow_feedback_rx so
    // we can safely modify or inject things and maintain original message order.
    queued_feedback: Vec<mz_dataflow_types::client::Response>,
    _data_directory: TempDir,
    temp_dir: TempDir,
    uppers: HashMap<GlobalId, Timestamp>,
    timestamp: Arc<Mutex<u64>>,
    verbose: bool,
    persisted_sessions: HashMap<String, (SessionClient, StartupResponse)>,
    deferred_results: HashMap<String, Vec<ExecuteResponse>>,
}

impl CoordTest {
    pub async fn new() -> anyhow::Result<Self> {
        let experimental_mode = false;
        let timestamp = Arc::new(Mutex::new(0));
        let now = {
            let timestamp = Arc::clone(&timestamp);
            NowFn::from(move || *timestamp.lock().unwrap())
        };
        let metrics_registry = MetricsRegistry::new();
        let (dataflow_server, dataflow_client) = mz_dataflow::serve(mz_dataflow::Config {
            workers: 1,
            timely_config: timely::Config {
                communication: timely::CommunicationConfig::Process(1),
                worker: timely::WorkerConfig::default(),
            },
            experimental_mode,
            now: now.clone(),
            metrics_registry: metrics_registry.clone(),
            persister: None,
            aws_external_id: AwsExternalId::NotProvided,
        })?;
        let dataflow_client = InterceptingDataflowClient::new(dataflow_client);

        let data_directory = tempfile::tempdir()?;
        let storage = mz_coord::catalog::storage::Connection::open(
            &data_directory.path().join("catalog"),
            Some(experimental_mode),
        )?;
        let persister = PersistConfig::disabled()
            .init(storage.cluster_id(), DUMMY_BUILD_INFO, &metrics_registry)
            .await?;
        let (coord_handle, coord_client) = mz_coord::serve(mz_coord::Config {
            dataflow_client: Box::new(dataflow_client.clone()),
            storage,
            logging: None,
            logical_compaction_window: None,
            timestamp_frequency: Duration::from_millis(1),
            experimental_mode,
            disable_user_indexes: false,
            safe_mode: false,
            build_info: &DUMMY_BUILD_INFO,
            aws_external_id: AwsExternalId::NotProvided,
            metrics_registry,
            persister,
            now,
        })
        .await?;
        let coordtest = CoordTest {
            dataflow_client,
            _coord_handle: coord_handle,
            coord_client,
            _dataflow_server: dataflow_server,
            _data_directory: data_directory,
            temp_dir: tempfile::tempdir().unwrap(),
            uppers: HashMap::new(),
            timestamp,
            verbose: std::env::var_os("COORDTEST_VERBOSE").is_some(),
            queued_feedback: Vec::new(),
            persisted_sessions: HashMap::new(),
            deferred_results: HashMap::new(),
        };
        Ok(coordtest)
    }

    async fn connect(&self) -> anyhow::Result<(SessionClient, StartupResponse)> {
        let conn_client = self.coord_client.new_conn()?;
        let session = Session::new(conn_client.conn_id(), "materialize".into());
        Ok(conn_client.startup(session, false).await?)
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

    async fn drain_feedback_msgs(&mut self) {
        loop {
            if let Some(msg) = self.dataflow_client.intercepting_recv().await {
                self.queued_feedback.push(msg);
            } else {
                return;
            }
        }
    }

    // Drains messages from the queue into coord, extracting and requeueing
    // excluded uppers.
    async fn drain_skip_uppers(&mut self, exclude_uppers: &HashSet<GlobalId>) {
        self.drain_feedback_msgs().await;
        let mut to_send = vec![];
        let mut to_queue = vec![];
        for mut msg in self.queued_feedback.drain(..) {
            // Filter out requested ids.
            if let Response::Compute(ComputeResponse::FrontierUppers(uppers), instance) = &mut msg {
                // Requeue excluded uppers so future wait-sql directives don't always have to
                // specify the same exclude list forever.
                let mut requeue = uppers.clone();
                requeue.retain(|(id, _data)| exclude_uppers.contains(id));
                if !requeue.is_empty() {
                    to_queue.push(Response::Compute(
                        ComputeResponse::FrontierUppers(requeue),
                        *instance,
                    ));
                }
                uppers.retain(|(id, _data)| !exclude_uppers.contains(id));
            }
            to_send.push(msg);
        }
        for msg in to_send {
            self.dataflow_client.forward_response(msg);
        }
        self.queued_feedback = to_queue;
    }

    // Processes PeekResponse messages until rows has a response.
    async fn wait_for_peek(
        &mut self,
        mut rows: std::pin::Pin<Box<dyn Future<Output = PeekResponseUnary>>>,
    ) -> PeekResponseUnary {
        loop {
            if let futures::task::Poll::Ready(rows) = futures::poll!(rows.as_mut()) {
                return rows;
            }
            self.drain_peek_response().await;
        }
    }

    // Drains PeekResponse messages from the queue into coord.
    async fn drain_peek_response(&mut self) {
        self.drain_feedback_msgs().await;
        let mut to_send = vec![];
        let mut to_queue = vec![];
        for msg in self.queued_feedback.drain(..) {
            if let Response::Compute(ComputeResponse::PeekResponse(..), _instance) = msg {
                to_send.push(msg);
            } else {
                to_queue.push(msg);
            }
        }
        for msg in to_send {
            self.dataflow_client.forward_response(msg);
        }
        self.queued_feedback = to_queue;
    }

    async fn make_catalog(&mut self) -> Catalog {
        let catalog = self
            .with_sc(|sc| Box::pin(async move { sc.dump_catalog().await.unwrap() }))
            .await;
        let catalog: BTreeMap<String, mz_coord::catalog::Database> =
            serde_json::from_str(&catalog).unwrap();
        Catalog(catalog)
    }
}

#[derive(Debug)]
struct Catalog(BTreeMap<String, mz_coord::catalog::Database>);

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
    let stmts = mz_sql_parser::parser::parse_statements(&query)?;
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
    datadriven::walk_async(dir, run_test).await;
}

pub async fn run_test(mut tf: datadriven::TestFile) -> datadriven::TestFile {
    let ct = std::rc::Rc::new(std::cell::RefCell::new(CoordTest::new().await.unwrap()));
    tf.run_async(|tc| {
        let mut ct = ct.borrow_mut();
        if ct.verbose {
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
                    let catalog = ct.make_catalog().await;
                    let exclude_uppers: HashSet<GlobalId> = tc
                        .args
                        .get("exclude-uppers")
                        .unwrap_or(&vec![])
                        .into_iter()
                        .map(|name| catalog.get(name))
                        .collect();

                    let start = Instant::now();
                    loop {
                        ct.drain_skip_uppers(&exclude_uppers).await;
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
                                                PeekResponseUnary::Rows(rows) => {
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
                                    panic!("{}", err);
                                }
                                // Bump the timestamp. This is necessary because sources ingest at varying
                                // rates and we need to allow sinces to move forward so we can see new data.
                                let mut ts = ct.timestamp.lock().unwrap();
                                *ts += 1;
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
                    ct.dataflow_client.forward_response(Response::Compute(
                        ComputeResponse::FrontierUppers(updates),
                        mz_dataflow_types::client::DEFAULT_COMPUTE_INSTANCE_ID,
                    ));
                    "".into()
                }
                "inc-timestamp" => {
                    let inc: u64 = tc.input.trim().parse().unwrap();
                    let mut ts = ct.timestamp.lock().unwrap();
                    *ts += inc;
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

/// A [`mz_dataflow_types::client::Client`] implementation that intercepts responses from the
/// dataflow server.
///
/// The implementation of the `send` method is unchanged. The implementation of
/// `recv`, however, only presents the responses that have been explicitly
/// forwarded via `forward_response`. To access the actual responses from
/// the underlying dataflow client, call `try_intercepting_recv`.
struct InterceptingDataflowClient<C> {
    inner: Arc<TokioMutex<C>>,
    feedback_tx: mpsc::UnboundedSender<mz_dataflow_types::client::Response>,
    feedback_rx: Arc<TokioMutex<mpsc::UnboundedReceiver<mz_dataflow_types::client::Response>>>,
}

impl<C> Clone for InterceptingDataflowClient<C> {
    fn clone(&self) -> InterceptingDataflowClient<C> {
        InterceptingDataflowClient {
            inner: Arc::clone(&self.inner),
            feedback_tx: self.feedback_tx.clone(),
            feedback_rx: Arc::clone(&self.feedback_rx),
        }
    }
}

#[async_trait(?Send)]
impl<C> mz_dataflow_types::client::Client for InterceptingDataflowClient<C>
where
    C: mz_dataflow_types::client::Client,
{
    async fn send(&mut self, cmd: mz_dataflow_types::client::Command) -> Result<(), anyhow::Error> {
        self.inner.lock().await.send(cmd).await
    }

    async fn recv(&mut self) -> Option<mz_dataflow_types::client::Response> {
        let mut feedback_rx = self.feedback_rx.lock().await;
        feedback_rx.recv().await
    }
}

impl<C> InterceptingDataflowClient<C>
where
    C: mz_dataflow_types::client::Client,
{
    /// Creates a new intercepting dataflow client that wraps the provided
    /// dataflow client.
    fn new(inner: C) -> InterceptingDataflowClient<C> {
        let (feedback_tx, feedback_rx) = mpsc::unbounded_channel();
        InterceptingDataflowClient {
            inner: Arc::new(TokioMutex::new(inner)),
            feedback_tx,
            feedback_rx: Arc::new(TokioMutex::new(feedback_rx)),
        }
    }

    /// Receives a response from the underlying dataflow client, if one is
    /// immediately available.
    async fn intercepting_recv(&mut self) -> Option<mz_dataflow_types::client::Response> {
        self.inner.lock().await.recv().now_or_never().flatten()
    }

    /// Makes the specified response available via the normal `recv` method.
    fn forward_response(&self, response: mz_dataflow_types::client::Response) {
        self.feedback_tx.send(response).unwrap()
    }
}
