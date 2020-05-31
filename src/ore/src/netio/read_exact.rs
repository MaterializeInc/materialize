// Copyright 2019 Tokio Contributors
// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
//
// Portions of this file are derived from the ReadExact combinator in the Tokio
// project. The original source code was retrieved on March 1, 2019 from:
//
//     https://github.com/tokio-rs/tokio/blob/195c4b04963742ecfff202ee9d0b72cc923aee81/tokio-io/src/io/read_exact.rs
//
// The original source code is subject to the terms of the MIT license, a copy
// of which can be found in the LICENSE file at the root of this repository.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::ready;
use tokio::io::{self, AsyncRead};

/// A future which reads exactly enough bytes to fill a buffer, unless EOF is
/// reached first.
///
/// Create a `ReadExactOrEof` struct by calling the [`read_exact_or_eof`]
/// function.
#[derive(Debug)]
pub struct ReadExactOrEof<'a, A> {
    reader: &'a mut A,
    buf: &'a mut [u8],
    pos: usize,
}

/// Creates a future which will read exactly enough bytes to fill `buf`, unless
/// EOF is reached first. If a short read should be considered an error, use
/// [`tokio::io::AsyncReadExt::read_exact`] instead.
///
/// The returned future will resolve to the number of bytes read.
///
/// In the case of an error the contents of the buffer are unspecified.
pub fn read_exact_or_eof<'a, A>(reader: &'a mut A, buf: &'a mut [u8]) -> ReadExactOrEof<'a, A>
where
    A: AsyncRead,
{
    ReadExactOrEof {
        reader,
        buf,
        pos: 0,
    }
}

impl<A> Future for ReadExactOrEof<'_, A>
where
    A: AsyncRead + Unpin,
{
    type Output = io::Result<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        while self.pos < self.buf.len() {
            let me = &mut *self;
            let n = ready!(Pin::new(&mut me.reader).poll_read(cx, &mut me.buf[me.pos..]))?;
            self.pos += n;
            if n == 0 {
                break;
            }
        }
        Poll::Ready(Ok(self.pos))
    }
}
