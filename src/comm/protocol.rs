// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! The guts of the underlying network communication protocol.

use futures::{Future, Sink, Stream};
use num_enum::{TryFromPrimitive, IntoPrimitive};
use ore::future::{FutureExt, StreamExt};
use ore::netio::{SniffedStream, SniffingStream};
use serde::{Deserialize, Serialize};
use std::convert::TryInto;
use std::fmt;
use std::net::SocketAddr;
use tokio::codec::LengthDelimitedCodec;
use tokio::io::{self, AsyncRead, AsyncWrite};
use tokio::net::unix::UnixStream;
use tokio::net::TcpStream;
use tokio_serde_bincode::{ReadBincode, WriteBincode};
use uuid::Uuid;

/// A magic number that is sent along at the beginning of each network
/// connection. The intent is to make it easy to sniff out `comm` traffic when
/// multiple protocols are multiplexed on the same port.
pub const PROTOCOL_MAGIC: [u8; 8] = [0x5f, 0x65, 0x44, 0x90, 0xaf, 0x4b, 0x3c, 0xfc];

/// Reports whether the connection handshake is `comm` traffic by sniffing out
/// whether the first bytes of `buf` match [`PROTOCOL_MAGIC`].
///
/// See [`crate::Switchboard::handle_connection`] for a usage example.
pub fn match_handshake(buf: &[u8]) -> bool {
    if buf.len() < 8 {
        return false;
    }
    buf[..8] == PROTOCOL_MAGIC
}

/// A trait for objects that can serve as the underlying transport layer for
/// `comm` traffic.
///
/// Only [`TcpStream`] and [`SniffedStream`] support is provided at the moment,
/// but support for any owned, thread-safe type which implements [`AsyncRead`]
/// and [`AsyncWrite`] can be added trivially, i.e., by implementing this trait.
pub trait Connection: AsyncRead + AsyncWrite + Send + 'static {
    /// The type that identifies the endpoint when establishing a connection of
    /// this type.
    type Addr: fmt::Debug
        + Eq
        + PartialEq
        + Send
        + Sync
        + Clone
        + Serialize
        + for<'de> Deserialize<'de>
        + Into<Addr>;

    /// Connects to the specified `addr`.
    fn connect(addr: &Self::Addr) -> Box<dyn Future<Item = Self, Error = io::Error> + Send>;
}

impl Connection for TcpStream {
    type Addr = SocketAddr;

    fn connect(addr: &Self::Addr) -> Box<dyn Future<Item = Self, Error = io::Error> + Send> {
        Box::new(TcpStream::connect(&addr).map(|conn| {
            conn.set_nodelay(true).expect("set_nodelay call failed");
            conn
        }))
    }
}

impl<C> Connection for SniffedStream<C>
where
    C: Connection,
{
    type Addr = C::Addr;

    fn connect(addr: &Self::Addr) -> Box<dyn Future<Item = Self, Error = io::Error> + Send> {
        Box::new(C::connect(addr).map(|conn| SniffingStream::new(conn).into_sniffed()))
    }
}

impl Connection for UnixStream {
    type Addr = std::path::PathBuf;

    fn connect(addr: &Self::Addr) -> Box<dyn Future<Item = Self, Error = io::Error> + Send> {
        Box::new(UnixStream::connect(addr))
    }
}

/// All known address types for [`Connection`]s.
///
/// The existence of this type is a bit unfortunate. It exists so that
/// [`mpsc::Sender`] does not need to be generic over [`Connection`], as
/// MPSC transmitters are meant to be lightweight and easy to stash in places
/// where a generic parameter might be a hassle. Ideally we'd make an `Addr`
/// trait and store a `Box<dyn Addr>`, but Rust does not currently permit
/// serializing and deserializing trait objects.
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Debug)]
pub enum Addr {
    /// The address type for [`TcpStream`].
    Tcp(<TcpStream as Connection>::Addr),
    /// The address type for [`UnixStream`].
    Unix(<UnixStream as Connection>::Addr),
}

impl From<<TcpStream as Connection>::Addr> for Addr {
    fn from(addr: <TcpStream as Connection>::Addr) -> Addr {
        Addr::Tcp(addr)
    }
}

impl From<<UnixStream as Connection>::Addr> for Addr {
    fn from(addr: <UnixStream as Connection>::Addr) -> Addr {
        Addr::Unix(addr)
    }
}

#[repr(u8)]
#[derive(IntoPrimitive, TryFromPrimitive)]
enum TrafficType {
    Channel,
    Rendezvous,
}

pub(crate) fn send_channel_handshake<C>(
    conn: C,
    uuid: Uuid,
) -> impl Future<Item = Framed<C>, Error = io::Error>
where
    C: Connection,
{
    let mut buf = [0; 9];
    (&mut buf[..8]).copy_from_slice(&PROTOCOL_MAGIC);
    buf[8] = TrafficType::Channel.into();
    io::write_all(conn, buf).and_then(move |(conn, _buf)| {
        let conn = framed(conn);
        conn.send(uuid.as_bytes()[..].into())
    })
}

pub(crate) fn send_rendezvous_handshake<C>(
    conn: C,
    id: u64,
) -> impl Future<Item = C, Error = io::Error>
where
    C: Connection,
{
    let mut buf = [0; 17];
    (&mut buf[..8]).copy_from_slice(&PROTOCOL_MAGIC);
    buf[8] = TrafficType::Rendezvous.into();
    (&mut buf[9..]).copy_from_slice(&id.to_be_bytes());
    io::write_all(conn, buf).map(|(conn, _buf)| conn)
}

pub(crate) enum RecvHandshake<C> {
    Channel(Uuid, Framed<C>),
    Rendezvous(u64, C),
}

pub(crate) fn recv_handshake<C>(conn: C) -> impl Future<Item = RecvHandshake<C>, Error = io::Error>
where
    C: Connection,
{
    io::read_exact(conn, [0; 9]).and_then(|(conn, buf)| {
        assert_eq!(&buf[..8], PROTOCOL_MAGIC);
        match buf[8].try_into().unwrap() {
            TrafficType::Channel => {
                let conn = framed(conn);
                conn.recv()
                    .map(move |(bytes, conn)| {
                        assert_eq!(bytes.len(), 16);
                        let uuid = Uuid::from_slice(&bytes).unwrap();
                        RecvHandshake::Channel(uuid, conn)
                    })
                    .left()
            }
            TrafficType::Rendezvous => {
                let buf = [0; 8];
                io::read_exact(conn, buf)
                    .map(move |(conn, buf)| {
                        let id = u64::from_be_bytes(buf);
                        RecvHandshake::Rendezvous(id, conn)
                    })
                    .right()
            }
        }
    })
}

pub(crate) type Framed<C> = tokio::codec::Framed<C, LengthDelimitedCodec>;

/// Frames `conn` using a length-delimited codec. In other words, it transforms
/// a connection which implements [`AsyncRead`] and [`AsyncWrite`] into an
/// combination [`Sink`] and [`Stream`] which produces/emits byte chunks.
pub(crate) fn framed<C>(conn: C) -> Framed<C>
where
    C: Connection,
{
    Framed::new(conn, LengthDelimitedCodec::new())
}

/// Constructs a [`Sink`] which encodes incoming `D`s using [bincode] and sends
/// them over the connection `conn` with a length prefix. Its dual is
/// [`decoder`].
///
/// [bincode]: https://crates.io/crates/bincode
pub(crate) fn encoder<C, D>(
    framed: Framed<C>,
) -> impl Sink<SinkItem = D, SinkError = bincode::Error>
where
    C: Connection,
    D: Serialize + for<'de> Deserialize<'de> + Send,
{
    WriteBincode::new(framed.sink_from_err())
}

/// Constructs a [`Stream`] which decodes bincoded, length-prefixed `D`s from
/// the connection `conn`. Its dual is [`encoder`].
pub(crate) fn decoder<C, D>(framed: Framed<C>) -> impl Stream<Item = D, Error = bincode::Error>
where
    C: Connection,
    D: Serialize + for<'de> Deserialize<'de> + Send,
{
    ReadBincode::new(framed.from_err())
}
