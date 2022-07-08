// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! gRPC transport for the [client](crate::client) module.

use std::fmt;
use std::marker::PhantomData;
use std::net::ToSocketAddrs;
use std::pin::Pin;
use std::sync::Arc;

use async_stream::stream;
use async_trait::async_trait;
use futures::future;
use futures::stream::{Stream, StreamExt, TryStreamExt};
use tokio::select;
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::sync::{oneshot, Mutex};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::body::BoxBody;
use tonic::transport::{Body, NamedService, Server};
use tonic::{Request, Response, Status, Streaming};
use tower::Service;
use tracing::{debug, error, info};

use mz_proto::{ProtoType, RustType};

use crate::client::{GenericClient, Partitionable, Partitioned};

pub type ResponseStream<PR> = Pin<Box<dyn Stream<Item = Result<PR, Status>> + Send>>;

/// A client to a remote dataflow server using gRPC and protobuf based
/// communication.
///
/// The client opens a connection using the proto client stubs that are
/// generated by tonic from a service definition. When the client is connected,
/// it will call automatically the only RPC defined in the service description,
/// encapsulated by the `BidiProtoClient` trait. This trait bound is not on the
/// `Client` type parameter here, but it IS on the impl blocks. Bidirectional
/// protobuf RPC sets up two streams that persist after the RPC has returned: A
/// Request (Command) stream (for us, backed by a unbounded mpsc queue) going
/// from this instance to the server and a response stream coming back
/// (represented directly as a Streaming<Response> instance). The recv and send
/// functions interact with the two mpsc channels or the streaming instance
/// respectively.
#[derive(Debug)]
pub struct GrpcClient<Client, PC, PR> {
    /// The sender for commands.
    tx: UnboundedSender<PC>,
    /// The receiver for responses.
    rx: Streaming<PR>,
    _client: PhantomData<Client>,
}

impl<Client, PC, PR> GrpcClient<Client, PC, PR>
where
    Client: BidiProtoClient<PC, PR> + Send + fmt::Debug,
{
    pub async fn connect(addr: String) -> Result<Self, anyhow::Error> {
        debug!("GrpcClient {}: Attempt to connect", addr);
        let (tx, rx) = mpsc::unbounded_channel();
        let mut client = Client::connect(format!("http://{}", addr)).await?;
        let rx = client
            .establish_bidi_stream(UnboundedReceiverStream::new(rx))
            .await?
            .into_inner();
        info!("GrpcClient {}: connected", &addr);
        Ok(GrpcClient {
            tx,
            rx,
            _client: PhantomData,
        })
    }

    pub async fn connect_partitioned<C, R>(
        addrs: Vec<String>,
    ) -> Result<Partitioned<Self, C, R>, anyhow::Error>
    where
        (C, R): Partitionable<C, R>,
    {
        let clients = future::try_join_all(addrs.into_iter().map(Self::connect)).await?;
        Ok(Partitioned::new(clients))
    }
}

#[async_trait]
impl<Client, C, R, PC, PR> GenericClient<C, R> for GrpcClient<Client, PC, PR>
where
    C: RustType<PC> + Send + Sync + 'static,
    R: RustType<PR> + Send + Sync,
    Client: BidiProtoClient<PC, PR> + Send + fmt::Debug,
    PC: Send + Sync + fmt::Debug + 'static,
    PR: Send + Sync + fmt::Debug,
{
    async fn send(&mut self, cmd: C) -> Result<(), anyhow::Error> {
        self.tx.send(cmd.into_proto())?;
        Ok(())
    }

    async fn recv(&mut self) -> Result<Option<R>, anyhow::Error> {
        match self.rx.try_next().await? {
            None => Ok(None),
            Some(response) => Ok(Some(response.into_rust()?)),
        }
    }
}

/// Encapsulates the core functionality of a tonic gRPC client for a service
/// that exposes a single bidirectional RPC stream.
///
/// See the documentation on [`GrpcClient`] for details.
//
// TODO(guswynn): if tonic ever presents the client API as a trait, use it
// instead of requiring an implementation of this trait.
#[async_trait]
pub trait BidiProtoClient<PC, PR> {
    async fn connect(addr: String) -> Result<Self, tonic::transport::Error>
    where
        Self: Sized;

    async fn establish_bidi_stream(
        &mut self,
        rx: UnboundedReceiverStream<PC>,
    ) -> Result<Response<Streaming<PR>>, Status>;
}

/// A command from a gRPC server.
pub enum GrpcServerCommand<T> {
    /// A command indicating the upstream client has reconnected.
    ///
    /// This message is delivered by the [`GrpcServer`] immediately after client
    /// reconnection.
    Reconnected,
    /// A command sent by the upstream client.
    Client(T),
}

/// A gRPC server that stitches a gRPC service with a single bidirectional
/// stream to a [`GenericClient`].
///
/// It is the counterpart of [`GrpcClient`].
///
/// To use, implement the tonic-generated `ProtoService` trait for this type.
/// The implementation of the bidirectional stream method should call
/// [`GrpcServer::forward_bidi_stream`] to stitch the bidirectional stream to
/// the client underlying this server.
pub struct GrpcServer<G> {
    state: Arc<GrpcServerState<G>>,
}

struct GrpcServerState<G> {
    cancel_tx: Mutex<oneshot::Sender<()>>,
    client: Mutex<G>,
}

impl<G> GrpcServer<G>
where
    G: Send + 'static,
{
    /// Starts the server, listening for gRPC connections on `listen_addr` and
    /// communicating with the provided `client`.
    ///
    /// The trait bounds on `f` are intimidating, but it is a function that
    /// turns a `GrpcServer<ProtoCommandType, ProtoResponseType>` into a
    /// [`Service`] that represents a gRPC server. This is always encapsulated
    /// by the tonic-generated `ProtoServer::new` method for a specific Protobuf
    /// service.
    pub async fn serve<S, F>(listen_addr: String, client: G, f: F) -> Result<(), anyhow::Error>
    where
        S: Service<
                http::Request<Body>,
                Response = http::Response<BoxBody>,
                Error = std::convert::Infallible,
            > + NamedService
            + Clone
            + Send
            + 'static,
        S::Future: Send + 'static,
        F: FnOnce(Self) -> S + Send + 'static,
    {
        let (cancel_tx, _cancel_rx) = oneshot::channel();
        let state = GrpcServerState {
            cancel_tx: Mutex::new(cancel_tx),
            client: Mutex::new(client),
        };
        let server = Self {
            state: Arc::new(state),
        };

        info!("Starting to listen on {}", listen_addr);
        Server::builder()
            .add_service(f(server))
            .serve(listen_addr.to_socket_addrs()?.next().unwrap())
            .await?;
        Ok(())
    }

    /// Handles a bidirectional stream request by forwarding commands to and
    /// responses from the server's underlying client.
    ///
    /// Call this method from the implementation of the tonic-generated
    /// `ProtoService`.
    pub async fn forward_bidi_stream<C, R, PC, PR>(
        &self,
        request: Request<Streaming<PC>>,
    ) -> Result<Response<ResponseStream<PR>>, Status>
    where
        G: GenericClient<GrpcServerCommand<C>, R> + 'static,
        C: RustType<PC> + Send + Sync + 'static + fmt::Debug,
        R: RustType<PR> + Send + Sync + 'static + fmt::Debug,
        PC: fmt::Debug + Send + Sync + 'static,
        PR: fmt::Debug + Send + Sync + 'static,
    {
        info!("GrpcServer: remote client connected");

        // Install our cancellation token. This may drop an existing
        // cancellation token. We're allowed to run until someone else drops our
        // cancellation token.
        //
        // TODO(benesch): rather than blindly dropping the existing cancellation
        // token, we should check epochs, and only drop the existing connection
        // if it is at a lower epoch.
        // See: https://github.com/MaterializeInc/materialize/issues/13377
        let (cancel_tx, mut cancel_rx) = oneshot::channel();
        *self.state.cancel_tx.lock().await = cancel_tx;

        // Forward commands and responses to `client` until canceled.
        let mut request = request.into_inner();
        let state = Arc::clone(&self.state);
        let response = stream! {
            let mut client = state.client.lock().await;
            if let Err(e) = client.send(GrpcServerCommand::Reconnected).await {
                yield Err(Status::unknown(e.to_string()));
            }
            loop {
                select! {
                    command = request.next() => {
                        let command = match command {
                            None => break,
                            Some(Ok(command)) => command,
                            Some(Err(e)) => {
                                error!("error handling client: {e}");
                                break;
                            }
                        };
                        let command = match command.into_rust() {
                            Ok(command) => GrpcServerCommand::Client(command),
                            Err(e) => {
                                error!("error converting command to protobuf: {}", e);
                                break;
                            }
                        };
                        if let Err(e) = client.send(command).await {
                            yield Err(Status::unknown(e.to_string()));
                        }
                    }
                    response = client.recv() => {
                        match response {
                            Ok(Some(response)) => yield Ok(response.into_proto()),
                            Ok(None) => break,
                            Err(e) => yield Err(Status::unknown(e.to_string())),
                        }
                    }
                    _ = &mut cancel_rx => break,
                }
            }
            info!("GrpcServer: remote client disconnected");
        };
        Ok(Response::new(Box::pin(response) as ResponseStream<PR>))
    }
}
