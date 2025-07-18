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

use std::error::Error;
use std::net::SocketAddr as InetSocketAddr;
use std::pin::Pin;
use std::str::FromStr;
use std::task::{Context, Poll, ready};
use std::{fmt, io};

use async_trait::async_trait;
use tokio::fs;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{self, TcpListener, TcpStream, UnixListener, UnixStream, tcp, unix};
use tonic::transport::server::{Connected, TcpConnectInfo, UdsConnectInfo};
use tracing::warn;

use crate::error::ErrorExt;

/// The type of a [`SocketAddr`].
#[derive(Debug, Clone, Copy)]
pub enum SocketAddrType {
    /// An internet socket address.
    Inet,
    /// A Unix domain socket address.
    Unix,
    /// A `turmoil` socket address.
    Turmoil,
}

impl SocketAddrType {
    /// Guesses the type of socket address specified by `s`.
    ///
    /// * Socket addresses that are absolute paths, as determined by a leading `/` character, are
    ///   determined to be Unix socket addresses.
    /// * Addresses with a "turmoil:" prefix are determined to be `turmoil` socket addresses.
    /// * All other addresses are assumed to be internet socket addresses.
    pub fn guess(s: &str) -> SocketAddrType {
        if s.starts_with('/') {
            SocketAddrType::Unix
        } else if s.starts_with("turmoil:") {
            SocketAddrType::Turmoil
        } else {
            SocketAddrType::Inet
        }
    }
}

/// An address associated with an internet or Unix domain socket.
#[derive(Debug, Clone)]
pub enum SocketAddr {
    /// An internet socket address.
    Inet(InetSocketAddr),
    /// A Unix domain socket address.
    Unix(UnixSocketAddr),
    /// A `turmoil` socket address.
    Turmoil(String),
}

impl PartialEq for SocketAddr {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (SocketAddr::Inet(addr1), SocketAddr::Inet(addr2)) => addr1 == addr2,
            (
                SocketAddr::Unix(UnixSocketAddr { path: Some(path1) }),
                SocketAddr::Unix(UnixSocketAddr { path: Some(path2) }),
            ) => path1 == path2,
            (SocketAddr::Turmoil(addr1), SocketAddr::Turmoil(addr2)) => addr1 == addr2,
            _ => false,
        }
    }
}

impl fmt::Display for SocketAddr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            SocketAddr::Inet(addr) => addr.fmt(f),
            SocketAddr::Unix(addr) => addr.fmt(f),
            SocketAddr::Turmoil(addr) => write!(f, "turmoil:{addr}"),
        }
    }
}

impl FromStr for SocketAddr {
    type Err = AddrParseError;

    /// Parses a socket address from a string.
    ///
    /// Whether a socket address is taken as an internet socket address or a
    /// Unix socket address is determined by [`SocketAddrType::guess`].
    fn from_str(s: &str) -> Result<SocketAddr, AddrParseError> {
        match SocketAddrType::guess(s) {
            SocketAddrType::Unix => {
                let addr = UnixSocketAddr::from_pathname(s).map_err(|e| AddrParseError {
                    kind: AddrParseErrorKind::Unix(e),
                })?;
                Ok(SocketAddr::Unix(addr))
            }
            SocketAddrType::Inet => {
                let addr = s.parse().map_err(|_| AddrParseError {
                    // The underlying error message is always "invalid socket
                    // address syntax", so there's no benefit to preserving it.
                    kind: AddrParseErrorKind::Inet,
                })?;
                Ok(SocketAddr::Inet(addr))
            }
            SocketAddrType::Turmoil => {
                let addr = s.strip_prefix("turmoil:").ok_or(AddrParseError {
                    kind: AddrParseErrorKind::Turmoil,
                })?;
                Ok(SocketAddr::Turmoil(addr.into()))
            }
        }
    }
}

/// An address associated with a Unix domain socket.
#[derive(Debug, Clone)]
pub struct UnixSocketAddr {
    path: Option<String>,
}

impl UnixSocketAddr {
    /// Constructs a Unix domain socket address from the provided path.
    ///
    /// Unlike the [`UnixSocketAddr::from_pathname`] method in the standard
    /// library, `path` is required to be valid UTF-8.
    ///
    /// # Errors
    ///
    /// Returns an error if the path is longer than `SUN_LEN` or if it contains
    /// null bytes.
    ///
    /// [`UnixSocketAddr::from_pathname`]: std::os::unix::net::SocketAddr::from_pathname
    pub fn from_pathname<S>(path: S) -> Result<UnixSocketAddr, io::Error>
    where
        S: Into<String>,
    {
        let path = path.into();
        let _ = std::os::unix::net::SocketAddr::from_pathname(&path)?;
        Ok(UnixSocketAddr { path: Some(path) })
    }

    /// Constructs a Unix domain socket address representing an unnamed Unix
    /// socket.
    pub fn unnamed() -> UnixSocketAddr {
        UnixSocketAddr { path: None }
    }

    /// Returns the pathname of this Unix domain socket address, if it was
    /// constructed from a pathname.
    pub fn as_pathname(&self) -> Option<&str> {
        self.path.as_deref()
    }
}

impl fmt::Display for UnixSocketAddr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.path {
            None => f.write_str("<unnamed>"),
            Some(path) => f.write_str(path),
        }
    }
}

/// The error returned when parsing a [`SocketAddr`] from a string.
#[derive(Debug)]
pub struct AddrParseError {
    kind: AddrParseErrorKind,
}

#[derive(Debug)]
pub enum AddrParseErrorKind {
    Inet,
    Unix(io::Error),
    Turmoil,
}

impl fmt::Display for AddrParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.kind {
            AddrParseErrorKind::Inet => f.write_str("invalid internet socket address syntax"),
            AddrParseErrorKind::Unix(e) => {
                f.write_str("invalid unix socket address syntax: ")?;
                e.fmt(f)
            }
            AddrParseErrorKind::Turmoil => f.write_str("missing 'turmoil:' prefix"),
        }
    }
}

impl Error for AddrParseError {}

/// Converts or resolves without blocking to one or more [`SocketAddr`]s.
#[async_trait]
pub trait ToSocketAddrs {
    /// Converts to resolved [`SocketAddr`]s.
    async fn to_socket_addrs(&self) -> Result<Vec<SocketAddr>, io::Error>;
}

#[async_trait]
impl ToSocketAddrs for SocketAddr {
    async fn to_socket_addrs(&self) -> Result<Vec<SocketAddr>, io::Error> {
        Ok(vec![self.clone()])
    }
}

#[async_trait]
impl<'a> ToSocketAddrs for &'a [SocketAddr] {
    async fn to_socket_addrs(&self) -> Result<Vec<SocketAddr>, io::Error> {
        Ok(self.to_vec())
    }
}

#[async_trait]
impl ToSocketAddrs for InetSocketAddr {
    async fn to_socket_addrs(&self) -> Result<Vec<SocketAddr>, io::Error> {
        Ok(vec![SocketAddr::Inet(*self)])
    }
}

#[async_trait]
impl ToSocketAddrs for UnixSocketAddr {
    async fn to_socket_addrs(&self) -> Result<Vec<SocketAddr>, io::Error> {
        Ok(vec![SocketAddr::Unix(self.clone())])
    }
}

#[async_trait]
impl ToSocketAddrs for str {
    async fn to_socket_addrs(&self) -> Result<Vec<SocketAddr>, io::Error> {
        match self.parse() {
            Ok(addr) => Ok(vec![addr]),
            Err(_) => {
                let addrs = net::lookup_host(self).await?;
                Ok(addrs.map(SocketAddr::Inet).collect())
            }
        }
    }
}

#[async_trait]
impl ToSocketAddrs for String {
    async fn to_socket_addrs(&self) -> Result<Vec<SocketAddr>, io::Error> {
        (**self).to_socket_addrs().await
    }
}

#[async_trait]
impl<T> ToSocketAddrs for &T
where
    T: ToSocketAddrs + Send + Sync + ?Sized,
{
    async fn to_socket_addrs(&self) -> Result<Vec<SocketAddr>, io::Error> {
        (**self).to_socket_addrs().await
    }
}

/// A listener bound to either a TCP socket or Unix domain socket.
pub enum Listener {
    /// A TCP listener.
    Tcp(TcpListener),
    /// A Unix domain socket listener.
    Unix(UnixListener),
    /// A `turmoil` socket listener.
    #[cfg(feature = "turmoil")]
    Turmoil(turmoil::net::TcpListener),
}

impl Listener {
    /// Creates a new listener bound to the specified socket address.
    ///
    /// If `addr` is a Unix domain address, this function attempts to unlink the
    /// socket at the address, if it exists, before binding.
    pub async fn bind<A>(addr: A) -> Result<Listener, io::Error>
    where
        A: ToSocketAddrs,
    {
        let mut last_err = None;
        for addr in addr.to_socket_addrs().await? {
            match Listener::bind_addr(addr).await {
                Ok(listener) => return Ok(listener),
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err.unwrap_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "could not resolve to any address",
            )
        }))
    }

    async fn bind_addr(addr: SocketAddr) -> Result<Listener, io::Error> {
        match &addr {
            SocketAddr::Inet(addr) => {
                let listener = TcpListener::bind(addr).await?;
                Ok(Listener::Tcp(listener))
            }
            SocketAddr::Unix(UnixSocketAddr { path: Some(path) }) => {
                // We would ideally unlink the file only if we could prove that
                // no process was still listening at the file, but there is no
                // foolproof API for doing so.
                // See: https://stackoverflow.com/q/7405932
                if let Err(e) = fs::remove_file(path).await {
                    if e.kind() != io::ErrorKind::NotFound {
                        warn!(
                            "unable to remove {path} while binding unix domain socket: {}",
                            e.display_with_causes(),
                        );
                    }
                }
                let listener = UnixListener::bind(path)?;
                Ok(Listener::Unix(listener))
            }
            SocketAddr::Unix(UnixSocketAddr { path: None }) => Err(io::Error::new(
                io::ErrorKind::Other,
                "cannot bind to unnamed Unix socket",
            )),
            #[cfg(feature = "turmoil")]
            SocketAddr::Turmoil(addr) => {
                let listener = turmoil::net::TcpListener::bind(addr).await?;
                Ok(Listener::Turmoil(listener))
            }
            #[cfg(not(feature = "turmoil"))]
            SocketAddr::Turmoil(_) => panic!("`turmoil` feature not enabled"),
        }
    }

    /// Accepts a new incoming connection to this listener.
    ///
    /// If the connection protocol is TCP, the returned stream has `TCP_NODELAY` set, making it
    /// suitable for low-latency communication by default.
    pub async fn accept(&self) -> Result<(Stream, SocketAddr), io::Error> {
        match self {
            Listener::Tcp(listener) => {
                let (stream, addr) = listener.accept().await?;
                stream.set_nodelay(true)?;
                let stream = Stream::Tcp(stream);
                let addr = SocketAddr::Inet(addr);
                Ok((stream, addr))
            }
            Listener::Unix(listener) => {
                let (stream, addr) = listener.accept().await?;
                let stream = Stream::Unix(stream);
                assert!(addr.is_unnamed());
                let addr = SocketAddr::Unix(UnixSocketAddr::unnamed());
                Ok((stream, addr))
            }
            #[cfg(feature = "turmoil")]
            Listener::Turmoil(listener) => {
                let (stream, addr) = listener.accept().await?;
                let stream = Stream::Turmoil(stream);
                let addr = SocketAddr::Inet(addr);
                Ok((stream, addr))
            }
        }
    }

    fn poll_accept(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Stream, io::Error>>> {
        match self.get_mut() {
            Listener::Tcp(listener) => {
                let (stream, _addr) = ready!(listener.poll_accept(cx))?;
                stream.set_nodelay(true)?;
                Poll::Ready(Some(Ok(Stream::Tcp(stream))))
            }
            Listener::Unix(listener) => {
                let (stream, _addr) = ready!(listener.poll_accept(cx))?;
                Poll::Ready(Some(Ok(Stream::Unix(stream))))
            }
            #[cfg(feature = "turmoil")]
            Listener::Turmoil(_) => {
                unimplemented!("`turmoil::net::TcpListener::poll_accept`");
            }
        }
    }
}

impl fmt::Debug for Listener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tcp(inner) => f.debug_tuple("Tcp").field(inner).finish(),
            Self::Unix(inner) => f.debug_tuple("Unix").field(inner).finish(),
            #[cfg(feature = "turmoil")]
            Self::Turmoil(_) => f.debug_tuple("Turmoil").finish(),
        }
    }
}

impl futures::stream::Stream for Listener {
    type Item = Result<Stream, io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_accept(cx)
    }
}

/// A stream associated with either a TCP socket or a Unix domain socket.
#[derive(Debug)]
pub enum Stream {
    /// A TCP stream.
    Tcp(TcpStream),
    /// A Unix domain socket stream.
    Unix(UnixStream),
    /// A `turmoil` socket stream.
    #[cfg(feature = "turmoil")]
    Turmoil(turmoil::net::TcpStream),
}

impl Stream {
    /// Opens a connection to the specified socket address.
    ///
    /// If the connection protocol is TCP, the returned stream has `TCP_NODELAY` set, making it
    /// suitable for low-latency communication by default.
    pub async fn connect<A>(addr: A) -> Result<Stream, io::Error>
    where
        A: ToSocketAddrs,
    {
        let mut last_err = None;
        for addr in addr.to_socket_addrs().await? {
            match Stream::connect_addr(addr).await {
                Ok(stream) => return Ok(stream),
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err.unwrap_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "could not resolve to any address",
            )
        }))
    }

    async fn connect_addr(addr: SocketAddr) -> Result<Stream, io::Error> {
        match addr {
            SocketAddr::Inet(addr) => {
                let stream = TcpStream::connect(addr).await?;
                stream.set_nodelay(true)?;
                Ok(Stream::Tcp(stream))
            }
            SocketAddr::Unix(UnixSocketAddr { path: Some(path) }) => {
                let stream = UnixStream::connect(path).await?;
                Ok(Stream::Unix(stream))
            }
            SocketAddr::Unix(UnixSocketAddr { path: None }) => Err(io::Error::new(
                io::ErrorKind::Other,
                "cannot connected to unnamed Unix socket",
            )),
            #[cfg(feature = "turmoil")]
            SocketAddr::Turmoil(addr) => {
                let stream = turmoil::net::TcpStream::connect(addr).await?;
                Ok(Stream::Turmoil(stream))
            }
            #[cfg(not(feature = "turmoil"))]
            SocketAddr::Turmoil(_) => panic!("`turmoil` feature not enabled"),
        }
    }

    /// Reports whether the underlying stream is a TCP stream.
    pub fn is_tcp(&self) -> bool {
        matches!(self, Stream::Tcp(_))
    }

    /// Reports whether the underlying stream is a Unix stream.
    pub fn is_unix(&self) -> bool {
        matches!(self, Stream::Unix(_))
    }

    /// Returns the underlying TCP stream.
    ///
    /// # Panics
    ///
    /// Panics if the stream is not a Unix stream.
    pub fn unwrap_tcp(self) -> TcpStream {
        match self {
            Stream::Tcp(stream) => stream,
            Stream::Unix(_) => panic!("Stream::unwrap_tcp called on a Unix stream"),
            #[cfg(feature = "turmoil")]
            Stream::Turmoil(_) => panic!("Stream::unwrap_tcp called on a `turmoil` stream"),
        }
    }

    /// Returns the underlying Unix stream.
    ///
    /// # Panics
    ///
    /// Panics if the stream is not a Unix stream.
    pub fn unwrap_unix(self) -> UnixStream {
        match self {
            Stream::Tcp(_) => panic!("Stream::unwrap_unix called on a TCP stream"),
            Stream::Unix(stream) => stream,
            #[cfg(feature = "turmoil")]
            Stream::Turmoil(_) => panic!("Stream::unwrap_unix called on a `turmoil` stream"),
        }
    }

    /// Splits a stream into a read half and a write half, which can be used to read and write the
    /// stream concurrently.
    pub fn split(self) -> (StreamReadHalf, StreamWriteHalf) {
        match self {
            Stream::Tcp(stream) => {
                let (rx, tx) = stream.into_split();
                (StreamReadHalf::Tcp(rx), StreamWriteHalf::Tcp(tx))
            }
            Stream::Unix(stream) => {
                let (rx, tx) = stream.into_split();
                (StreamReadHalf::Unix(rx), StreamWriteHalf::Unix(tx))
            }
            #[cfg(feature = "turmoil")]
            Stream::Turmoil(stream) => {
                let (rx, tx) = stream.into_split();
                (StreamReadHalf::Turmoil(rx), StreamWriteHalf::Turmoil(tx))
            }
        }
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut ReadBuf,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Tcp(stream) => Pin::new(stream).poll_read(cx, buf),
            Stream::Unix(stream) => Pin::new(stream).poll_read(cx, buf),
            #[cfg(feature = "turmoil")]
            Stream::Turmoil(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Stream::Tcp(stream) => Pin::new(stream).poll_write(cx, buf),
            Stream::Unix(stream) => Pin::new(stream).poll_write(cx, buf),
            #[cfg(feature = "turmoil")]
            Stream::Turmoil(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Tcp(stream) => Pin::new(stream).poll_flush(cx),
            Stream::Unix(stream) => Pin::new(stream).poll_flush(cx),
            #[cfg(feature = "turmoil")]
            Stream::Turmoil(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Tcp(stream) => Pin::new(stream).poll_shutdown(cx),
            Stream::Unix(stream) => Pin::new(stream).poll_shutdown(cx),
            #[cfg(feature = "turmoil")]
            Stream::Turmoil(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

impl Connected for Stream {
    type ConnectInfo = ConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        match self {
            Stream::Tcp(stream) => ConnectInfo::Tcp(stream.connect_info()),
            Stream::Unix(stream) => ConnectInfo::Unix(stream.connect_info()),
            #[cfg(feature = "turmoil")]
            Stream::Turmoil(stream) => ConnectInfo::Turmoil(TcpConnectInfo {
                local_addr: stream.local_addr().ok(),
                remote_addr: stream.peer_addr().ok(),
            }),
        }
    }
}

/// Read half of a [`Stream`], created by [`Stream::split`].
#[derive(Debug)]
pub enum StreamReadHalf {
    Tcp(tcp::OwnedReadHalf),
    Unix(unix::OwnedReadHalf),
    #[cfg(feature = "turmoil")]
    Turmoil(turmoil::net::tcp::OwnedReadHalf),
}

impl AsyncRead for StreamReadHalf {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut ReadBuf,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(rx) => Pin::new(rx).poll_read(cx, buf),
            Self::Unix(rx) => Pin::new(rx).poll_read(cx, buf),
            #[cfg(feature = "turmoil")]
            Self::Turmoil(rx) => Pin::new(rx).poll_read(cx, buf),
        }
    }
}

/// Write half of a [`Stream`], created by [`Stream::split`].
#[derive(Debug)]
pub enum StreamWriteHalf {
    Tcp(tcp::OwnedWriteHalf),
    Unix(unix::OwnedWriteHalf),
    #[cfg(feature = "turmoil")]
    Turmoil(turmoil::net::tcp::OwnedWriteHalf),
}

impl AsyncWrite for StreamWriteHalf {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Tcp(tx) => Pin::new(tx).poll_write(cx, buf),
            Self::Unix(tx) => Pin::new(tx).poll_write(cx, buf),
            #[cfg(feature = "turmoil")]
            Self::Turmoil(tx) => Pin::new(tx).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(tx) => Pin::new(tx).poll_flush(cx),
            Self::Unix(tx) => Pin::new(tx).poll_flush(cx),
            #[cfg(feature = "turmoil")]
            Self::Turmoil(tx) => Pin::new(tx).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(tx) => Pin::new(tx).poll_shutdown(cx),
            Self::Unix(tx) => Pin::new(tx).poll_shutdown(cx),
            #[cfg(feature = "turmoil")]
            Self::Turmoil(tx) => Pin::new(tx).poll_shutdown(cx),
        }
    }
}

/// Connection information for a [`Stream`].
#[derive(Debug, Clone)]
pub enum ConnectInfo {
    /// TCP connection information.
    Tcp(TcpConnectInfo),
    /// Unix domain socket connection information.
    Unix(UdsConnectInfo),
    /// `turmoil` socket conection information.
    Turmoil(TcpConnectInfo),
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddrV4};

    use super::*;

    #[crate::test]
    fn test_parse() {
        for (input, expected) in [
            (
                "/valid/path",
                Ok(SocketAddr::Unix(
                    UnixSocketAddr::from_pathname("/valid/path").unwrap(),
                )),
            ),
            (
                "/",
                Ok(SocketAddr::Unix(
                    UnixSocketAddr::from_pathname("/").unwrap(),
                )),
            ),
            (
                "/\0",
                Err(
                    "invalid unix socket address syntax: paths must not contain interior null bytes",
                ),
            ),
            (
                "1.2.3.4:5678",
                Ok(SocketAddr::Inet(InetSocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::new(1, 2, 3, 4),
                    5678,
                )))),
            ),
            ("1.2.3.4", Err("invalid internet socket address syntax")),
            ("bad", Err("invalid internet socket address syntax")),
            (
                "turmoil:1.2.3.4:5678",
                Ok(SocketAddr::Turmoil("1.2.3.4:5678".into())),
            ),
        ] {
            let actual = SocketAddr::from_str(input).map_err(|e| e.to_string());
            let expected = expected.map_err(|e| e.to_string());
            assert_eq!(actual, expected, "input: {}", input);
        }
    }
}
