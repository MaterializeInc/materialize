// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A driver for interacting with Maelstrom
//!
//! This translates input into requests and requests into output. It also
//! handles issuing Maelstrom [service] requests. It's very roughly based off of
//! the node in Maelstrom's [ruby examples].
//!
//! [service]: https://github.com/jepsen-io/maelstrom/blob/v0.2.1/doc/services.md
//! [ruby examples]: https://github.com/jepsen-io/maelstrom/blob/v0.2.1/demo/ruby/node.rb

use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::sync::{Arc, Mutex};

use anyhow::anyhow;
use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::oneshot;
use tracing::{info, trace};

use mz_persist::location::ExternalError;
use mz_persist_client::ShardId;

use crate::maelstrom::api::{Body, ErrorCode, MaelstromError, Msg, MsgId, NodeId};
use crate::maelstrom::Args;

/// An implementor of a Maelstrom [workload].
///
/// [workload]: https://github.com/jepsen-io/maelstrom/blob/v0.2.1/doc/workload.md
#[async_trait]
pub trait Service: Sized + Send + Sync {
    /// Construct this service.
    ///
    /// Maelstrom services are available via the [Handle].
    async fn init(args: &Args, handle: &Handle) -> Result<Self, MaelstromError>;

    /// Respond to a single request.
    ///
    /// Implementations must either panic or respond by calling
    /// [Handle::send_res] exactly once.
    async fn eval(&self, handle: Handle, src: NodeId, req: Body);
}

/// Runs the RPC loop, accepting Maelstrom workload requests, issuing responses,
/// and communicating with Maelstrom services.
pub fn run<R, W, S>(args: Args, read: R, write: W) -> Result<(), anyhow::Error>
where
    R: BufRead,
    W: Write + Send + Sync + 'static,
    S: Service + 'static,
{
    let mut node = Node::<S>::new(args, write);
    for line in read.lines() {
        let line = line.map_err(|err| anyhow!("req read failed: {}", err))?;
        trace!("raw: [{}]", line);
        let req: Msg = line
            .parse()
            .map_err(|err| anyhow!("invalid req {}: {}", err, line))?;
        if req.body.in_reply_to().is_none() {
            info!("req: {}", req);
        } else {
            trace!("req: {}", req);
        }
        node.handle(req);
    }
    Ok(())
}

struct Node<S>
where
    S: Service + 'static,
{
    args: Args,
    core: Arc<Mutex<Core>>,
    node_id: Option<NodeId>,
    service: Arc<AsyncInitOnceWaitable<Arc<S>>>,
}

impl<S> Node<S>
where
    S: Service + 'static,
{
    fn new<W>(args: Args, write: W) -> Self
    where
        W: Write + Send + Sync + 'static,
    {
        let core = Core {
            write: Box::new(write),
            next_msg_id: MsgId(0),
            callbacks: HashMap::new(),
        };
        Node {
            args,
            core: Arc::new(Mutex::new(core)),
            node_id: None,
            service: Arc::new(AsyncInitOnceWaitable::new()),
        }
    }

    pub fn handle(&mut self, msg: Msg) {
        // If we've been initialized (i.e. have a NodeId), respond to the
        // message.
        if let Some(node_id) = self.node_id.as_ref() {
            // This message is not for us
            if node_id != &msg.dest {
                return;
            }

            let handle = Handle {
                node_id: node_id.clone(),
                core: Arc::clone(&self.core),
            };

            let body = match handle.maybe_handle_service_res(&msg.src, msg.body) {
                Ok(()) => return,
                Err(x) => x,
            };

            let service = Arc::clone(&self.service);
            let _ = mz_ore::task::spawn(|| format!("maelstrom::handle"), async move {
                let service = service.get().await;
                let () = service.eval(handle, msg.src, body).await;
            });
            return;
        }

        // Otherwise, if we haven't yet been initialized, then the only message
        // type we are allowed to process is ReqInit.
        match msg.body {
            Body::ReqInit {
                msg_id, node_id, ..
            } => {
                // Set the NodeId.
                self.node_id = Some(node_id.clone());
                let handle = Handle {
                    node_id,
                    core: Arc::clone(&self.core),
                };

                // Respond to the init req.
                //
                // NB: This must come _before_ service init! We want service
                // init to be able to use Maelstrom services, but Maelstrom
                // doesn't make services available to nodes that haven't yet
                // responded to init.
                let in_reply_to = msg_id;
                handle.send_res(msg.src, move |msg_id| Body::ResInit {
                    msg_id,
                    in_reply_to,
                });

                // Tricky! Run the service init in a task in case it uses
                // Maelstrom services. This is because Maelstrom services return
                // responses on stdin, which means we need to be processing the
                // run loop concurrently with this. This is also the reason for
                // the AsyncInitOnceWaitable nonsense.
                let args = self.args.clone();
                let service_init = Arc::clone(&self.service);
                let _ = mz_ore::task::spawn(|| format!("maelstrom::init"), async move {
                    let service = match S::init(&args, &handle).await {
                        Ok(x) => x,
                        Err(err) => {
                            // If service initialization fails, there's nothing
                            // to do but panic. Any retries should be pushed
                            // into the impl of `init`.
                            panic!("service initialization failed: {}", err);
                        }
                    };
                    service_init.init_once(Arc::new(service)).await;
                });
            }
            // All other reqs are a no-op. We can't even error without a NodeId.
            _ => {}
        }
    }
}

struct Core {
    write: Box<dyn Write + Send + Sync>,
    next_msg_id: MsgId,
    callbacks: HashMap<MsgId, oneshot::Sender<Body>>,
}

impl std::fmt::Debug for Core {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Destructure the struct to be defensive against new fields.
        let Core {
            write: _,
            next_msg_id,
            callbacks,
        } = self;
        f.debug_struct("Core")
            .field("next_msg_id", &next_msg_id)
            .field("callbacks", &callbacks.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl Core {
    fn alloc_msg_id(&mut self) -> MsgId {
        self.next_msg_id = self.next_msg_id.next();
        self.next_msg_id
    }
}

/// A handle to interact with Node.
#[derive(Debug, Clone)]
pub struct Handle {
    node_id: NodeId,
    core: Arc<Mutex<Core>>,
}

impl Handle {
    /// Send a response to Maelstrom.
    ///
    /// `dest` should be the `src` of the response. To make a service request,
    /// use [Self::send_service_req] instead.
    pub fn send_res<BodyFn: FnOnce(MsgId) -> Body>(&self, dest: NodeId, res_fn: BodyFn) {
        let mut core = self.core.lock().expect("mutex poisoned");
        let msg_id = core.alloc_msg_id();
        let res = Msg {
            src: self.node_id.clone(),
            dest,
            body: res_fn(msg_id),
        };
        info!("res: {}", res);
        write!(core.write.as_mut(), "{}\n", res).expect("res write failed");
    }

    /// Issue a service request to Maelstrom.
    ///
    /// `dest` should be the service name. To respond to a request, use
    /// [Self::send_res] instead.
    pub async fn send_service_req<BodyFn: FnOnce(MsgId) -> Body>(
        &self,
        dest: NodeId,
        req_fn: BodyFn,
    ) -> Body {
        let (tx, rx) = oneshot::channel();
        {
            let mut core = self.core.lock().expect("mutex poisoned");
            let msg_id = core.alloc_msg_id();
            core.callbacks.insert(msg_id, tx);
            let req = Msg {
                src: self.node_id.clone(),
                dest,
                body: req_fn(msg_id),
            };
            trace!("svc: {}", req);
            write!(core.write.as_mut(), "{}\n", req).expect("req write failed");
        }
        rx.await.expect("internal error: callback oneshot dropped")
    }

    /// Attempts to handle a msg as a service response, returning it back if it
    /// isn't one.
    pub fn maybe_handle_service_res(&self, src: &NodeId, msg: Body) -> Result<(), Body> {
        let in_reply_to = match msg.in_reply_to() {
            Some(x) => x,
            None => return Err(msg),
        };

        let mut core = self.core.lock().expect("mutex poisoned");
        let callback = match core.callbacks.remove(&in_reply_to) {
            Some(x) => x,
            None => {
                self.send_res(src.clone(), |msg_id| Body::Error {
                    msg_id: Some(msg_id),
                    in_reply_to,
                    code: ErrorCode::MalformedRequest,
                    text: format!("no callback expected for {:?}", in_reply_to),
                });
                return Ok(());
            }
        };

        if let Err(_) = callback.send(msg) {
            // The caller is no longer listening. This is safe to ignore.
            return Ok(());
        }

        Ok(())
    }

    /// Returns a [ShardId] for this Maelstrom run.
    ///
    /// Uses Maelstrom services to ensure all nodes end up with the same id.
    pub async fn maybe_init_shard_id(&self) -> Result<ShardId, MaelstromError> {
        let proposal = ShardId::new();
        let key = "SHARD";
        loop {
            let from = Value::Null;
            let to = Value::from(proposal.to_string());
            match self
                .lin_kv_compare_and_set(Value::from(key), from, to, Some(true))
                .await
            {
                Ok(()) => {
                    info!("initialized maelstrom shard to {}", proposal);
                    return Ok(proposal);
                }
                Err(MaelstromError {
                    code: ErrorCode::PreconditionFailed,
                    ..
                }) => match self.lin_kv_read(Value::from(key)).await? {
                    Some(value) => {
                        let value = value.as_str().ok_or_else(|| {
                            ExternalError::from(anyhow!("invalid SHARD {}", value))
                        })?;
                        let shard_id = value.parse::<ShardId>().map_err(|err| {
                            ExternalError::from(anyhow!("invalid SHARD {}: {}", value, err))
                        })?;
                        info!("fetched maelstrom shard id {}", shard_id);
                        return Ok(shard_id);
                    }
                    None => continue,
                },
                Err(err) => return Err(err),
            }
        }
    }

    /// Issues a Maelstrom lin-kv service read request.
    pub async fn lin_kv_read(&self, key: Value) -> Result<Option<Value>, MaelstromError> {
        let dest = NodeId("lin-kv".to_string());
        let res = self
            .send_service_req(dest, move |msg_id| Body::ReqLinKvRead { msg_id, key })
            .await;
        match res {
            Body::Error {
                code: ErrorCode::KeyDoesNotExist,
                ..
            } => Ok(None),
            Body::Error { code, text, .. } => Err(MaelstromError { code, text }),
            Body::ResLinKvRead { value, .. } => Ok(Some(value)),
            res => unimplemented!("unsupported res: {:?}", res),
        }
    }

    /// Issues a Maelstrom lin-kv service write request.
    pub async fn lin_kv_write(&self, key: Value, value: Value) -> Result<(), MaelstromError> {
        let dest = NodeId("lin-kv".to_string());
        let res = self
            .send_service_req(dest, move |msg_id| Body::ReqLinKvWrite {
                msg_id,
                key,
                value,
            })
            .await;
        match res {
            Body::Error { code, text, .. } => Err(MaelstromError { code, text }),
            Body::ResLinKvWrite { .. } => Ok(()),
            res => unimplemented!("unsupported res: {:?}", res),
        }
    }

    /// Issues a Maelstrom lin-kv service cas request.
    pub async fn lin_kv_compare_and_set(
        &self,
        key: Value,
        from: Value,
        to: Value,
        create_if_not_exists: Option<bool>,
    ) -> Result<(), MaelstromError> {
        trace!(
            "lin_kv_compare_and_set key={:?} from={:?} to={:?} create_if_not_exists={:?}",
            key,
            from,
            to,
            create_if_not_exists
        );
        let dest = NodeId("lin-kv".to_string());
        let res = self
            .send_service_req(dest, move |msg_id| Body::ReqLinKvCaS {
                msg_id,
                key,
                from,
                to,
                create_if_not_exists,
            })
            .await;
        match res {
            Body::Error { code, text, .. } => Err(MaelstromError { code, text }),
            Body::ResLinKvCaS { .. } => Ok(()),
            res => unimplemented!("unsupported res: {:?}", res),
        }
    }
}

/// A helper for a value that is initialized once, but used from many async
/// places.
///
/// This name sure is a mouthful. Anyone have a suggestion?
#[derive(Debug)]
struct AsyncInitOnceWaitable<T: Clone> {
    core: tokio::sync::Mutex<(Option<T>, Vec<oneshot::Sender<T>>)>,
}

impl<T: Clone> AsyncInitOnceWaitable<T> {
    pub fn new() -> Self {
        let core = (None, Vec::new());
        AsyncInitOnceWaitable {
            core: tokio::sync::Mutex::new(core),
        }
    }

    pub async fn init_once(&self, t: T) {
        let mut core = self.core.lock().await;
        assert!(core.0.is_none(), "init called more than once");
        core.0 = Some(t.clone());
        for tx in core.1.drain(..) {
            let _ = tx.send(t.clone());
        }
    }

    pub async fn get(&self) -> T {
        let rx = {
            let mut core = self.core.lock().await;
            if let Some(x) = core.0.as_ref() {
                return x.clone();
            }
            let (tx, rx) = tokio::sync::oneshot::channel();
            core.1.push(tx);
            rx
        };
        rx.await.expect("internal error: waiter oneshot dropped")
    }
}
