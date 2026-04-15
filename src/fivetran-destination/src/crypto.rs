// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp;
use std::io;
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use aws_lc_rs::cipher::{AES_256, DecryptionContext, StreamingDecryptingKey, UnboundCipherKey};
use aws_lc_rs::iv::FixedLength;
use tokio::io::{AsyncRead, ReadBuf};

const BUF_SIZE: usize = 4096;
const BLOCK_SIZE: usize = 16;

pub struct AsyncAesDecrypter<R> {
    input: R,
    /// Wrapped in `Option` because `finish()` takes ownership.
    decrypter: Option<StreamingDecryptingKey>,
    buf: [u8; BUF_SIZE + BLOCK_SIZE],
    pos: usize,
    end: usize,
    done: bool,
}

impl<R> AsyncAesDecrypter<R> {
    pub fn new(
        input: R,
        key: &[u8],
        iv: &[u8],
    ) -> Result<AsyncAesDecrypter<R>, aws_lc_rs::error::Unspecified> {
        let unbound_key = UnboundCipherKey::new(&AES_256, key)?;
        let iv_fixed = FixedLength::<16>::try_from(iv)?;
        let context = DecryptionContext::Iv128(iv_fixed);
        let decrypter = StreamingDecryptingKey::cbc_pkcs7(unbound_key, context)?;

        Ok(AsyncAesDecrypter {
            input,
            decrypter: Some(decrypter),
            buf: [0; BUF_SIZE + BLOCK_SIZE],
            pos: 0,
            end: 0,
            done: false,
        })
    }
}

impl<R> AsyncRead for AsyncAesDecrypter<R>
where
    R: AsyncRead + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<(), io::Error>> {
        let me = self.get_mut();
        loop {
            // If we have remaining decrypted data, return it.
            if me.pos < me.end || me.done {
                let n = cmp::min(buf.remaining(), me.end - me.pos);
                buf.put_slice(&me.buf[me.pos..me.pos + n]);
                me.pos += n;
                return Poll::Ready(Ok(()));
            }

            // We're out of already decrypted data. Read the next chunk of
            // data from the underlying file.
            let mut read_buf = [MaybeUninit::<u8>::uninit(); BUF_SIZE];
            let mut read_buf = ReadBuf::uninit(&mut read_buf);
            ready!(Pin::new(&mut me.input).poll_read(cx, &mut read_buf))?;

            // Decrypt the chunk in full and stash it in `me.buf`.
            me.pos = 0;
            if !read_buf.filled().is_empty() {
                let decrypter = me.decrypter.as_mut().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::Other, "decrypter already finalized")
                })?;
                let update = decrypter
                    .update(read_buf.filled(), &mut me.buf)
                    .map_err(|_| io::Error::new(io::ErrorKind::Other, "AES decryption failed"))?;
                me.end = update.written().len();
            } else {
                let decrypter = me.decrypter.take().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::Other, "decrypter already finalized")
                })?;
                let update = decrypter.finish(&mut me.buf).map_err(|_| {
                    io::Error::new(io::ErrorKind::Other, "AES decryption finalize failed")
                })?;
                me.end = update.written().len();
                me.done = true;
            }

            // Go around the loop to return the decrypted data.
        }
    }
}
