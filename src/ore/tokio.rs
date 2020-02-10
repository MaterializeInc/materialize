// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tokio extensions.

/// Tokio networking extensions.
pub mod net {
    use std::io;
    use std::os::unix::io::AsRawFd;
    use std::os::unix::io::FromRawFd;

    /// Extension methods for [`tokio::net::TcpStream`].
    pub trait TcpStreamExt {
        /// Converts a [`tokio::net::TcpStream`] into a [`std::net::TcpStream`].
        /// This will hopefully become part of Tokio one day. See
        /// [tokio-rs/tokio#856](https://github.com/tokio-rs/tokio/issues/856).
        fn into_std(self) -> Result<std::net::TcpStream, io::Error>;
    }

    impl TcpStreamExt for tokio::net::TcpStream {
        fn into_std(self) -> Result<std::net::TcpStream, io::Error> {
            let fd = self.as_raw_fd();
            std::mem::forget(self);
            let stream = unsafe { std::net::TcpStream::from_raw_fd(fd) };
            // Tokio will have put the stream into non-blocking mode. This is
            // guaranteed to confuse consumers, so undo it.
            stream.set_nonblocking(false)?;
            Ok(stream)
        }
    }
}
