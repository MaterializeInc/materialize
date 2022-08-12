// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! WIP

use std::net::SocketAddr;

use crate::cache::PersistClientCache;
use crate::r#impl::service::proto_persist_server::ProtoPersistServer;
use crate::r#impl::service::PersistService;

/// WIP
#[derive(Debug)]
pub struct Server {
    persist_service: PersistService,
}

impl Server {
    /// WIP
    pub async fn new(persist_clients: &mut PersistClientCache, blob_uri: String) -> Self {
        let persist_service = PersistService::new(persist_clients, blob_uri).await;
        Server { persist_service }
    }

    /// WIP
    pub async fn serve(self, listen_addr: SocketAddr) -> Result<(), anyhow::Error> {
        tonic::transport::Server::builder()
            .add_service(ProtoPersistServer::new(self.persist_service))
            .serve(listen_addr)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
pub mod inprocess {
    use std::net::SocketAddr;

    use futures::FutureExt;
    use mz_ore::task::RuntimeExt;
    use tokio::net::TcpListener;
    use tokio::runtime::Handle;
    use tokio::sync::oneshot;
    use tokio::task::JoinHandle;
    use tracing::info;

    use crate::cache::PersistClientCache;
    use crate::r#impl::service::proto_persist_server::ProtoPersistServer;
    use crate::r#impl::service::PersistService;

    /// WIP
    #[derive(Debug)]

    pub struct InProcessServer {
        addr: SocketAddr,
        _shutdown_tx: oneshot::Sender<()>,
        _server_task: JoinHandle<Result<(), tonic::transport::Error>>,
    }

    impl Drop for InProcessServer {
        fn drop(&mut self) {
            info!("stopping RPC traffic on {:?}", self.addr);
        }
    }

    #[cfg(test)]
    impl InProcessServer {
        /// WIP
        pub async fn new(
            persist_clients: &mut PersistClientCache,
            blob_uri: String,
        ) -> Result<Self, anyhow::Error> {
            assert!(persist_clients.cfg.remote_compact_addr.is_none(), "WIP");
            let persist_service = PersistService::new(persist_clients, blob_uri).await;
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let addr = listener.local_addr()?;
            info!("serving RPC traffic on {:?}", addr);
            let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
            let server_task = Handle::current().spawn_named(|| "in-process persistd", async move {
                tonic::transport::Server::builder()
                    .add_service(ProtoPersistServer::new(persist_service))
                    .serve_with_incoming_shutdown(
                        tokio_stream::wrappers::TcpListenerStream::new(listener),
                        shutdown_rx.map(drop),
                    )
                    .await
            });
            Ok(InProcessServer {
                addr,
                _shutdown_tx: shutdown_tx,
                _server_task: server_task,
            })
        }

        pub fn addr(&self) -> &SocketAddr {
            &self.addr
        }
    }
}
