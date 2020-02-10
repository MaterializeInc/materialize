// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Async-aware communication fabric that abstracts over thread and process
//! boundaries.
//!
//! This crate provides channels that look and feel much like the interthread
//! channels that ship with Rust in [`std::sync`], and their asynchronous
//! counterparts in [`futures::sync`], but are capable of sending messages
//! across process boundaries. The API is designed so that components that wish
//! to send messages to one another can be largely oblivious of the required
//! networking. Under the hood, channels open network connections as necessary,
//! with the help of a router called a [`Switchboard`].
//!
//! # Switchboards
//!
//! The central object in this crate is the [`Switchboard`]. There is typically
//! one switchboard per application. It needs to be hooked up wherever
//! networking is handled. For example:
//!
//! ```no_run
//! use comm::Switchboard;
//! use futures::stream::StreamExt;
//! use std::net::Ipv4Addr;
//! use std::time::Duration;
//! use tokio::net::TcpListener;
//!
//! let nodes = vec![
//!     (Ipv4Addr::new(192, 168, 1, 1), 1234),
//!     (Ipv4Addr::new(192, 168, 1, 2), 1234),
//! ];
//! let node_id = 0;
//! let mut runtime = tokio::runtime::Runtime::new()?;
//! let switchboard = Switchboard::new(nodes, node_id, runtime.handle().clone());
//! let mut listener = runtime.block_on(TcpListener::bind("0.0.0.0:1234"))?;
//! runtime.spawn({
//!     let switchboard = switchboard.clone();
//!     async move {
//!         let mut incoming = listener.incoming();
//!         while let Some(conn) = incoming.next().await {
//!             switchboard.handle_connection(conn.expect("accept failed"));
//!         }
//!     }
//! });
//!
//! // Wait for other nodes to become available.
//! runtime.block_on(switchboard.rendezvous(Duration::from_secs(1)))?;
//!
//! // Allocate channels and do work.
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! In this example, the switchboard is configured for a cluster of size two,
//! where one process is listening on `host1:1234` and another is listening on
//! `host2:1234`. You might notice that these addresses are specified at the
//! time of switchboard creation, and, indeed, the membership of a cluster
//! managed by a switchboard cannot be changed after it is created. This
//! restriction greatly simplifies the switchboard's implementation.
//!
//! Incoming connections are routed to the switchboard, which will, in turn,
//! route those connections to the appropriate channel endpoint, which is likely
//! to be on another thread, or at least in another
//! [`Task`][futures::task::Task].
//!
//! The call to [`Switchboard::rendezvous`] is important for ensuring that
//! future communication via channels is successful. `rendezvous` repeatedly
//! polls all nodes in the cluster until TCP connections have been successfully
//! established with all of them. Without a rendezvous, if all processes are
//! started simultaneously, a send on a channel might reach one of the other
//! processes before it has successfully booted.
//!
//! # Channels
//!
//! The crate provides two types of channels:
//!
//! * Multiple-producer, single-consumer (MPSC) channels, in [`mpsc`], allow
//!   multiple transmitters to send messages to a single receiver.
//! * Broadcast channels, in [`broadcast`], allow multiple transmitters to send
//!   the same messages stream to all nodes.

#![deny(missing_docs)]

mod error;
mod router;

pub mod broadcast;
pub mod mpsc;
pub mod protocol;
pub mod switchboard;
pub mod util;

pub use error::Error;
pub use protocol::Connection;
pub use switchboard::Switchboard;
