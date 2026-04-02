// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A minimal `MakeTlsConnect` implementation for tokio-postgres backed by rustls.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use rustls_pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_postgres::tls::{ChannelBinding, TlsStream};
use tokio_rustls::TlsConnector;

/// Wrapper around `tokio_rustls::client::TlsStream` that implements
/// `tokio_postgres::tls::TlsStream`.
pub struct RustlsTlsStream<S>(tokio_rustls::client::TlsStream<S>);

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for RustlsTlsStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_read(cx, buf)
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for RustlsTlsStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().0).poll_write(cx, buf)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_flush(cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_shutdown(cx)
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> TlsStream for RustlsTlsStream<S> {
    fn channel_binding(&self) -> ChannelBinding {
        ChannelBinding::none()
    }
}

/// A `MakeTlsConnect` implementation that creates rustls-backed TLS connections.
#[derive(Clone)]
pub struct MakeRustlsConnect {
    config: Arc<rustls::ClientConfig>,
}

impl MakeRustlsConnect {
    pub fn new(config: rustls::ClientConfig) -> Self {
        Self {
            config: Arc::new(config),
        }
    }
}

impl<S> tokio_postgres::tls::MakeTlsConnect<S> for MakeRustlsConnect
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Stream = RustlsTlsStream<S>;
    type TlsConnect = RustlsConnect;
    type Error = rustls_pki_types::InvalidDnsNameError;

    fn make_tls_connect(&mut self, domain: &str) -> Result<Self::TlsConnect, Self::Error> {
        let server_name = ServerName::try_from(domain.to_owned())?;
        Ok(RustlsConnect {
            connector: TlsConnector::from(self.config.clone()),
            server_name,
        })
    }
}

pub struct RustlsConnect {
    connector: TlsConnector,
    server_name: ServerName<'static>,
}

impl<S> tokio_postgres::tls::TlsConnect<S> for RustlsConnect
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Stream = RustlsTlsStream<S>;
    type Error = std::io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Stream, Self::Error>> + Send>>;

    fn connect(self, stream: S) -> Self::Future {
        Box::pin(async move {
            let tls_stream = self.connector.connect(self.server_name, stream).await?;
            Ok(RustlsTlsStream(tls_stream))
        })
    }
}
