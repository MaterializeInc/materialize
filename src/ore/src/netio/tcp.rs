// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Wrappers around the [`tokio::net::tcp`] types.
//!
//! These wrappers provide enhanced functionality:
//!
//!  * They disable Nagle's algorithm by default, by setting `TCP_NODELAY`, avoiding a common
//!    footgun in the implementation of low-latency network connections.
//!  * When running inside a turmoil simulation, they use the [`turmoil::net::tcp`] types instead,
//!    ensuring that connections go through the simulated network.
//!
//! The types in this module are intended to be usable as drop-in replacements for the
//! [`tokio::net::tcp`] types. Note that some of their methods are not supported by the turmoil
//! types, so they will panic when called from within a simulation.

use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{fmt, io};

use tokio::io::{AsyncRead, AsyncWrite, Interest, ReadBuf, Ready};

use crate::netio::turmoil_override;

#[cfg(not(feature = "turmoil"))]
/// Converts or resolves to a [`SocketAddr`].
pub trait ToSocketAddrs: tokio::net::ToSocketAddrs {}
#[cfg(not(feature = "turmoil"))]
impl<T> ToSocketAddrs for T where T: tokio::net::ToSocketAddrs {}

#[cfg(feature = "turmoil")]
/// Converts or resolves to a [`SocketAddr`].
pub trait ToSocketAddrs: tokio::net::ToSocketAddrs + turmoil::ToSocketAddrs {}
#[cfg(feature = "turmoil")]
impl<T> ToSocketAddrs for T where T: tokio::net::ToSocketAddrs + turmoil::ToSocketAddrs {}

/// A listener bound to a TCP socket.
pub enum TcpListener {
    /// A `tokio` TCP listener.
    Tokio(tokio::net::TcpListener),
    /// A `turmoil` TCP listener.
    #[cfg(feature = "turmoil")]
    Turmoil(turmoil::net::TcpListener),
}

impl TcpListener {
    /// Creates a new listener bound to the specified TCP address.
    pub async fn bind<A>(addr: A) -> io::Result<Self>
    where
        A: ToSocketAddrs,
    {
        turmoil_override! {
            tokio => {
                let listener = tokio::net::TcpListener::bind(addr).await?;
                Ok(Self::Tokio(listener))
            }
            turmoil => {
                let listener = turmoil::net::TcpListener::bind(addr).await?;
                Ok(Self::Turmoil(listener))
            }
        }
    }

    /// Accepts a new incoming connection to this listener.
    ///
    /// The returned stream has `TCP_NODELAY` set, making it suitable for low-latency communication
    /// by default.
    pub async fn accept(&self) -> io::Result<(TcpStream, SocketAddr)> {
        match self {
            Self::Tokio(listener) => {
                let (stream, addr) = listener.accept().await?;
                stream.set_nodelay(true)?;
                let stream = TcpStream::Tokio(stream);
                Ok((stream, addr))
            }
            #[cfg(feature = "turmoil")]
            Self::Turmoil(listener) => {
                let (stream, addr) = listener.accept().await?;
                let stream = TcpStream::Turmoil(stream);
                Ok((stream, addr))
            }
        }
    }

    /// Returns the local address that this listener is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        match self {
            Self::Tokio(listener) => listener.local_addr(),
            #[cfg(feature = "turmoil")]
            Self::Turmoil(listener) => listener.local_addr(),
        }
    }
}

impl fmt::Debug for TcpListener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tokio(inner) => f.debug_tuple("Tcp").field(inner).finish(),
            #[cfg(feature = "turmoil")]
            Self::Turmoil(_) => f.debug_tuple("Turmoil").finish(),
        }
    }
}

/// A stream associated with a TCP socket.
#[derive(Debug)]
pub enum TcpStream {
    /// A `tokio` TCP stream.
    Tokio(tokio::net::TcpStream),
    /// A `turmoil` TCP stream.
    #[cfg(feature = "turmoil")]
    Turmoil(turmoil::net::TcpStream),
}

impl TcpStream {
    /// Opens a connection to the specified TCP address.
    ///
    /// The returned stream has `TCP_NODELAY` set, making it suitable for low-latency communication
    /// by default.
    pub async fn connect<A>(addr: A) -> io::Result<Self>
    where
        A: ToSocketAddrs,
    {
        turmoil_override! {
            tokio => {
                let stream = tokio::net::TcpStream::connect(addr).await?;
                stream.set_nodelay(true)?;
                Ok(Self::Tokio(stream))
            }
            turmoil => {
                let stream = turmoil::net::TcpStream::connect(addr).await?;
                Ok(Self::Turmoil(stream))
            }
        }
    }

    /// Creates new [`TcpStream`] from a [`std::net::TcpStream`].
    pub fn from_std(stream: std::net::TcpStream) -> io::Result<TcpStream> {
        turmoil_override! {
            tokio => {
                let stream = tokio::net::TcpStream::from_std(stream)?;
                Ok(Self::Tokio(stream))
            }
            turmoil => {
                unimplemented!("TcpStream::from_std")
            }
        }
    }

    /// Turns a [`tokio::net::TcpStream`] into a [`std::net::TcpStream`].
    pub fn into_std(self) -> io::Result<std::net::TcpStream> {
        match self {
            Self::Tokio(stream) => stream.into_std(),
            #[cfg(feature = "turmoil")]
            Self::Turmoil(_) => unimplemented!("TcpStream::into_std"),
        }
    }

    /// Returns the local address that this stream is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        match self {
            Self::Tokio(stream) => stream.local_addr(),
            #[cfg(feature = "turmoil")]
            Self::Turmoil(stream) => stream.local_addr(),
        }
    }

    /// Returns the remote address that this stream is connected to.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        match self {
            Self::Tokio(stream) => stream.peer_addr(),
            #[cfg(feature = "turmoil")]
            Self::Turmoil(stream) => stream.peer_addr(),
        }
    }

    /// Waits for any of the requested ready states.
    pub async fn ready(&self, interest: Interest) -> io::Result<Ready> {
        match self {
            Self::Tokio(stream) => stream.ready(interest).await,
            #[cfg(feature = "turmoil")]
            Self::Turmoil(stream) => stream.ready(interest).await,
        }
    }

    /// Receives data on the socket without removing that data from the queue. On success, returns
    /// the number of bytes peeked.
    ///
    /// Successive calls return the same data.
    pub async fn peek(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Tokio(stream) => stream.peek(buf).await,
            #[cfg(feature = "turmoil")]
            Self::Turmoil(stream) => stream.peek(buf).await,
        }
    }

    /// Splits a stream into a read half and a write half, which can be used to read and write the
    /// stream concurrently.
    pub fn into_split(self) -> (OwnedReadHalf, OwnedWriteHalf) {
        match self {
            Self::Tokio(stream) => {
                let (rx, tx) = stream.into_split();
                (OwnedReadHalf::Tokio(rx), OwnedWriteHalf::Tokio(tx))
            }
            #[cfg(feature = "turmoil")]
            Self::Turmoil(stream) => {
                let (rx, tx) = stream.into_split();
                (OwnedReadHalf::Turmoil(rx), OwnedWriteHalf::Turmoil(tx))
            }
        }
    }
}

impl AsyncRead for TcpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut ReadBuf,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tokio(stream) => Pin::new(stream).poll_read(cx, buf),
            #[cfg(feature = "turmoil")]
            Self::Turmoil(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for TcpStream {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Tokio(stream) => Pin::new(stream).poll_write(cx, buf),
            #[cfg(feature = "turmoil")]
            Self::Turmoil(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tokio(stream) => Pin::new(stream).poll_flush(cx),
            #[cfg(feature = "turmoil")]
            Self::Turmoil(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tokio(stream) => Pin::new(stream).poll_shutdown(cx),
            #[cfg(feature = "turmoil")]
            Self::Turmoil(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

/// Read half of a [`TcpStream`], created by [`TcpStream::into_split`].
#[derive(Debug)]
pub enum OwnedReadHalf {
    /// A `tokio` TCP stream read half.
    Tokio(tokio::net::tcp::OwnedReadHalf),
    #[cfg(feature = "turmoil")]
    /// A `turmoil` TCP stream read half.
    Turmoil(turmoil::net::tcp::OwnedReadHalf),
}

impl AsyncRead for OwnedReadHalf {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut ReadBuf,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tokio(rx) => Pin::new(rx).poll_read(cx, buf),
            #[cfg(feature = "turmoil")]
            Self::Turmoil(rx) => Pin::new(rx).poll_read(cx, buf),
        }
    }
}

/// Write half of a [`TcpStream`], created by [`TcpStream::into_split`].
#[derive(Debug)]
pub enum OwnedWriteHalf {
    /// A `tokio` TCP stream write half.
    Tokio(tokio::net::tcp::OwnedWriteHalf),
    #[cfg(feature = "turmoil")]
    /// A `turmoil` TCP stream write half.
    Turmoil(turmoil::net::tcp::OwnedWriteHalf),
}

impl AsyncWrite for OwnedWriteHalf {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Tokio(tx) => Pin::new(tx).poll_write(cx, buf),
            #[cfg(feature = "turmoil")]
            Self::Turmoil(tx) => Pin::new(tx).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tokio(tx) => Pin::new(tx).poll_flush(cx),
            #[cfg(feature = "turmoil")]
            Self::Turmoil(tx) => Pin::new(tx).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tokio(tx) => Pin::new(tx).poll_shutdown(cx),
            #[cfg(feature = "turmoil")]
            Self::Turmoil(tx) => Pin::new(tx).poll_shutdown(cx),
        }
    }
}
