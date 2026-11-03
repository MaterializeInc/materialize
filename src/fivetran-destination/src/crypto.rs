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
use aws_lc_rs::error::Unspecified;
use aws_lc_rs::iv::FixedLength;
use tokio::io::{AsyncRead, ReadBuf};

const BUF_SIZE: usize = 4096;
const BLOCK_SIZE: usize = 16;

pub struct AsyncAesDecrypter<R> {
    input: R,
    /// `None` once `finish` has consumed the key.
    decrypter: Option<StreamingDecryptingKey>,
    buf: [u8; BUF_SIZE + BLOCK_SIZE],
    pos: usize,
    end: usize,
    done: bool,
}

impl<R> AsyncAesDecrypter<R> {
    pub fn new(input: R, key: &[u8], iv: &[u8; 16]) -> Result<AsyncAesDecrypter<R>, Unspecified> {
        let key = UnboundCipherKey::new(&AES_256, key)?;
        let context = DecryptionContext::Iv128(FixedLength::from(iv));
        Ok(AsyncAesDecrypter {
            input,
            decrypter: Some(StreamingDecryptingKey::cbc_pkcs7(key, context)?),
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
            let decrypt_err =
                |_| io::Error::new(io::ErrorKind::InvalidData, "AES decryption failed");
            let decrypter = me.decrypter.as_mut().expect("not done");
            if !read_buf.filled().is_empty() {
                me.end = decrypter
                    .update(read_buf.filled(), &mut me.buf)
                    .map_err(decrypt_err)?
                    .written()
                    .len();
            } else {
                let decrypter = me.decrypter.take().expect("not done");
                me.end = decrypter
                    .finish(&mut me.buf)
                    .map_err(decrypt_err)?
                    .written()
                    .len();
                me.done = true;
            }

            // Go around the loop to return the decrypted data.
        }
    }
}

#[cfg(test)]
mod tests {
    use aws_lc_rs::cipher::{EncryptionContext, PaddedBlockEncryptingKey};
    use tokio::io::AsyncReadExt;

    use super::*;

    const KEY: [u8; 32] = [7; 32];
    const IV: [u8; 16] = [9; 16];

    fn encrypt(plaintext: &[u8]) -> Vec<u8> {
        let key =
            PaddedBlockEncryptingKey::cbc_pkcs7(UnboundCipherKey::new(&AES_256, &KEY).unwrap())
                .unwrap();
        let mut buf = plaintext.to_vec();
        let context = EncryptionContext::Iv128(FixedLength::from(IV));
        key.less_safe_encrypt(&mut buf, context).unwrap();
        buf
    }

    #[mz_ore::test(tokio::test)]
    async fn decrypts_across_read_chunks() {
        // Spans more than one BUF_SIZE read and ends mid-block.
        let plaintext: Vec<u8> = (0..BUF_SIZE * 2 + 5)
            .map(|i| u8::try_from(i % 251).unwrap())
            .collect();
        let ciphertext = encrypt(&plaintext);
        let mut decrypter = AsyncAesDecrypter::new(&ciphertext[..], &KEY, &IV).unwrap();
        let mut out = Vec::new();
        decrypter.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, plaintext);
    }

    #[mz_ore::test(tokio::test)]
    async fn rejects_truncated_ciphertext() {
        let ciphertext = encrypt(b"hello");
        let truncated = &ciphertext[..ciphertext.len() - 1];
        let mut decrypter = AsyncAesDecrypter::new(truncated, &KEY, &IV).unwrap();
        let mut out = Vec::new();
        let err = decrypter.read_to_end(&mut out).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
