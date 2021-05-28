// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! coordtest attempts to be a deterministic Coordinator tester using
//! datadriven test files. Many parts of dataflow run in their own tokio task
//! and we aren't able to observe their output until we bump a timestamp and
//! see if new data are available. For example, the FILE source on mac polls
//! every 100ms, making writing a fully deterministic test difficult. Instead
//! coordtest attempts to provide enough knobs that it can test interesting
//! cases without race conditions.
//!
//! The coordinator is started with logical compaction disabled. If you want to
//! add it to an index (to allow sinces to advance past 0), use `ALTER INDEX`.
//!
//! The following datadriven directives are supported:
//! - `sql`: Executes the SQL using transaction rules similar to the
//! simple pgwire protocol in a new session (that is, multiple `sql`
//! directives are not in the same session). Output is formatted
//! [`ExecuteResponse`](coord::ExecuteResponse). The input can contain the
//! string `<TEMP>` which will be replaced with a temporary directory.
//! - `wait-sql`: Executes all SQL in a retry loop (with 5s timeout
//! which will panic) until all datums returned (all columns in all
//! rows in all statements) are `true`. Prior to each attempt, all
//! pending feedback messages from the dataflow server are sent to the
//! Coordinator. Messages for specified items can be skipped by specifying
//! `exclude-uppers=database.schema.item` as an argument. After each failed
//! attempt, the timestamp is incremented by 1 to give any new data an
//! opportunity to be observed.
//! - `update-upper`: Sends a batch of
//! [`FrontierUppers`](dataflow::WorkerFeedback::FrontierUppers)
//! to the Coordinator. Input is one update per line of the format
//! `database.schema.item N` where N is some numeric timestamp. No output.
//! - `inc-timestamp`: Increments the timestamp by number in the input. No
//! output.
//! - `create-file`: Requires a `name=filename` argument. Creates and truncates
//! a file with the specified name in the TEMP directory with contents as the
//! input. No output.
//! - `append-file`: Same as `create-file`, but appends.
//! - `print-catalog`: Outputs the catalog. Generally for debugging.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::future::Future;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::anyhow;
use futures::future::FutureExt;
use tempfile::{NamedTempFile, TempDir};
use tokio::sync::mpsc;

use coord::{
    session::{EndTransactionAction, Session},
    Client, ExecuteResponse, SessionClient,
};
use dataflow::{WorkerFeedback, WorkerFeedbackWithMeta};
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
    coord_feedback_tx: mpsc::UnboundedSender<WorkerFeedbackWithMeta>,
    client: Option<Client>,
    _handle: JoinOnDropHandle<()>,
    dataflow_feedback_rx: mpsc::UnboundedReceiver<WorkerFeedbackWithMeta>,
    _catalog_file: NamedTempFile,
    temp_dir: TempDir,
    uppers: HashMap<GlobalId, Timestamp>,
    timestamp: Arc<Mutex<u64>>,
    _verbose: bool,
}

impl CoordTest {
    pub async fn new() -> anyhow::Result<Self> {
        let catalog_file = NamedTempFile::new()?;
        let (handle, client, coord_feedback_tx, dataflow_feedback_rx, timestamp) =
            coord::serve_debug(catalog_file.path());
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
        };
        Ok(coordtest)
    }

    async fn connect(&self) -> anyhow::Result<SessionClient> {
        let conn_client = self.client.as_ref().unwrap().new_conn()?;
        let session = Session::new(conn_client.conn_id(), "materialize".into());
        let (session_client, _startup_response) = conn_client.startup(session).await?;
        Ok(session_client)
    }

    fn rewrite_query(&self, query: &str) -> String {
        let path = self.temp_dir.path().to_str().unwrap();
        let query = query.replace("<TEMP>", path);
        query
    }

    // This very odd signature is some of us figured out as a way to achieve
    // this. Is there a better way to do this?
    async fn with_sc<F, T>(&self, f: F) -> T
    where
        F: for<'a> FnOnce(&'a mut SessionClient) -> std::pin::Pin<Box<dyn Future<Output = T> + 'a>>,
    {
        let mut sc = self.connect().await.unwrap();
        let r = f(&mut sc).await;
        sc.terminate().await;
        r
    }

    async fn drain_uppers(&mut self, exclude: &HashSet<GlobalId>) {
        loop {
            if let Some(Some(mut msg)) = self.dataflow_feedback_rx.recv().now_or_never() {
                // Filter out requested ids.
                if let WorkerFeedback::FrontierUppers(uppers) = &mut msg.message {
                    uppers.retain(|(id, _data)| !exclude.contains(id));
                }
                self.coord_feedback_tx.send(msg).unwrap();
            } else {
                return;
            }
        }
    }

    async fn make_catalog(&self) -> Catalog {
        let catalog = self
            .with_sc(|sc| Box::pin(async move { sc.dump_catalog().await.unwrap() }))
            .await;
        let catalog: BTreeMap<String, coord::catalog::Database> =
            serde_json::from_str(&catalog).unwrap();
        Catalog(catalog)
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
                                format!("{:#?}", rows.await)
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
                        ct.drain_uppers(&exclude_uppers).await;
                        let query = ct.rewrite_query(&tc.input);
                        let results = ct
                            .with_sc(|sc| Box::pin(async move { sql(sc, query).await }))
                            .await;
                        let mut failed = Ok(());
                        match results {
                            Ok(result) => {
                                for r in result {
                                    match r {
                                        ExecuteResponse::SendingRows(rows) => match rows.await {
                                            PeekResponse::Rows(rows) => {
                                                for row in rows {
                                                    for col in row.iter() {
                                                        if col != Datum::True {
                                                            failed = Err(anyhow!("datum != true"));
                                                        }
                                                    }
                                                }
                                            }
                                            r => {
                                                failed = Err(anyhow!("{:?}", r));
                                            }
                                        },
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
                        .send(WorkerFeedbackWithMeta {
                            worker_id: 0,
                            message: WorkerFeedback::FrontierUppers(updates),
                        })
                        .unwrap();
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
                    format!("{:#?}", catalog)
                }
                _ => panic!("unknown directive {}", tc.directive),
            };
            res
        }
    })
    .await;
    tf
}
