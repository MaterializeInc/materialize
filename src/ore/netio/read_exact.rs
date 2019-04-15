// Copyright 2019 Tokio Contributors
// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.
//
// This file is derived from the ReadExact combinator in the Tokio project. The
// original source code was retrieved on March 1, 2019 from:
//
//     https://github.com/tokio-rs/tokio/blob/195c4b04963742ecfff202ee9d0b72cc923aee81/tokio-io/src/io/read_exact.rs
//
// The original source code is subject to the terms of the MIT license, a copy
// of which can be found in the LICENSE file at the root of this repository.

use futures::{try_ready, Future, Poll};
use std::mem;
use tokio::io;
use tokio::io::AsyncRead;

/// A future which reads exactly enough bytes to fill a buffer, unless EOF is
/// reached first.
///
/// Create a `ReadExactOrEof` struct by calling the [`read_exact_or_eof`]
/// function.
#[derive(Debug)]
pub struct ReadExactOrEof<A, T> {
    state: State<A, T>,
}

#[derive(Debug)]
enum State<A, T> {
    Reading { a: A, buf: T, pos: usize },
    Empty,
}

/// Creates a future which will read exactly enough bytes to fill `buf`, unless
/// EOF is reached first. If a short read should be considered an error, use
/// [`tokio::io::read_exact`] instead.
///
/// The returned future will resolve to the I/O stream, the mutated buffer, and
/// the number of bytes read.
///
/// In the case of an error the buffer and the object will be discarded, with
/// the error yielded. In the case of success the object will be destroyed and
/// the buffer will be returned, with all data read from the stream appended to
/// the buffer.
pub fn read_exact_or_eof<A, T>(a: A, buf: T) -> ReadExactOrEof<A, T>
where
    A: AsyncRead,
    T: AsMut<[u8]>,
{
    ReadExactOrEof {
        state: State::Reading { a, buf, pos: 0 },
    }
}

impl<A, T> Future for ReadExactOrEof<A, T>
where
    A: AsyncRead,
    T: AsMut<[u8]>,
{
    type Item = (A, T, usize);
    type Error = io::Error;

    fn poll(&mut self) -> Poll<(A, T, usize), io::Error> {
        match self.state {
            State::Reading {
                ref mut a,
                ref mut buf,
                ref mut pos,
            } => {
                let buf = buf.as_mut();
                while *pos < buf.len() {
                    let n = try_ready!(a.poll_read(&mut buf[*pos..]));
                    *pos += n;
                    if n == 0 {
                        break;
                    }
                }
            }
            State::Empty => panic!("polling a ReadExact after it's done"),
        }

        match mem::replace(&mut self.state, State::Empty) {
            State::Reading { a, buf, pos } => Ok((a, buf, pos).into()),
            State::Empty => panic!(),
        }
    }
}
