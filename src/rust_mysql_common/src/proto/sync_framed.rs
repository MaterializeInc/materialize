// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use bytes::{Buf, BufMut, BytesMut};

use crate::{
    constants::DEFAULT_MAX_ALLOWED_PACKET,
    proto::codec::{error::PacketCodecError, PacketCodec},
};

use std::{
    io::{
        Error,
        ErrorKind::{Interrupted, Other},
        Read, Write,
    },
    ptr::slice_from_raw_parts_mut,
};

// stolen from futures-rs
macro_rules! with_interrupt {
    ($e:expr) => {
        loop {
            match $e {
                Ok(x) => {
                    break Ok(x);
                }
                Err(ref e) if e.kind() == Interrupted => {
                    continue;
                }
                Err(e) => {
                    break Err(e);
                }
            }
        }
    };
}

/// Synchronous framed stream for MySql protocol.
///
/// This type is a synchronous alternative to `tokio_codec::Framed`.
#[derive(Debug)]
pub struct MySyncFramed<T> {
    eof: bool,
    in_buf: BytesMut,
    out_buf: BytesMut,
    codec: PacketCodec,
    stream: T,
}

impl<T> MySyncFramed<T> {
    /// Creates new instance with the given `stream`.
    pub fn new(stream: T) -> Self {
        MySyncFramed {
            eof: false,
            in_buf: BytesMut::with_capacity(DEFAULT_MAX_ALLOWED_PACKET),
            out_buf: BytesMut::with_capacity(DEFAULT_MAX_ALLOWED_PACKET),
            codec: PacketCodec::default(),
            stream,
        }
    }

    /// Returns reference to a stream.
    pub fn get_ref(&self) -> &T {
        &self.stream
    }

    /// Returns mutable reference to a stream.
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.stream
    }

    /// Returns reference to a codec.
    pub fn codec(&self) -> &PacketCodec {
        &self.codec
    }

    /// Returns mutable reference to a codec.
    pub fn codec_mut(&mut self) -> &mut PacketCodec {
        &mut self.codec
    }

    /// Consumes self and returns wrapped buffers, codec and stream.
    pub fn destruct(self) -> (BytesMut, BytesMut, PacketCodec, T) {
        (self.in_buf, self.out_buf, self.codec, self.stream)
    }

    /// Creates new instance from given buffers, `codec` and `stream`.
    pub fn construct(in_buf: BytesMut, out_buf: BytesMut, codec: PacketCodec, stream: T) -> Self {
        Self {
            eof: false,
            in_buf,
            out_buf,
            codec,
            stream,
        }
    }
}

impl<T> MySyncFramed<T>
where
    T: Write,
{
    /// Will write packets into the stream. Stream may not be flushed.
    pub fn write<U: Buf>(&mut self, item: &mut U) -> Result<(), PacketCodecError> {
        self.codec.encode(item, &mut self.out_buf)?;
        with_interrupt!(self.stream.write_all(&self.out_buf))?;
        self.out_buf.clear();
        Ok(())
    }

    /// Will flush wrapped stream.
    pub fn flush(&mut self) -> Result<(), PacketCodecError> {
        with_interrupt!(self.stream.flush())?;
        Ok(())
    }

    /// Will send packets into the stream. Stream will be flushed.
    pub fn send<U: Buf>(&mut self, item: &mut U) -> Result<(), PacketCodecError> {
        self.write(item)?;
        self.flush()
    }
}

impl<T> MySyncFramed<T>
where
    T: Read,
{
    /// Returns `true` if `dst` contains the next packet.
    ///
    /// `false` means, that the `dst` is empty and the stream is at eof.
    pub fn next_packet<U>(&mut self, dst: &mut U) -> Result<bool, PacketCodecError>
    where
        U: AsRef<[u8]>,
        U: BufMut,
    {
        loop {
            if self.eof {
                return match self.codec.decode(&mut self.in_buf, dst)? {
                    true => Ok(true),
                    false => {
                        if self.in_buf.is_empty() {
                            Ok(false)
                        } else {
                            Err(Error::new(Other, "bytes remaining on stream").into())
                        }
                    }
                };
            } else {
                match self.codec.decode(&mut self.in_buf, dst)? {
                    true => return Ok(true),
                    false => unsafe {
                        self.in_buf.reserve(1);
                        match with_interrupt!(self.stream.read(&mut *slice_from_raw_parts_mut(
                            self.in_buf.chunk_mut().as_mut_ptr(),
                            self.in_buf.chunk_mut().len()
                        ))) {
                            Ok(0) => self.eof = true,
                            Ok(x) => self.in_buf.advance_mut(x),
                            Err(err) => return Err(From::from(err)),
                        }
                    },
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{constants::MAX_PAYLOAD_LEN, proto::sync_framed::MySyncFramed};

    #[test]
    fn iter_packets() {
        let mut buf = Vec::new();
        {
            let mut framed = MySyncFramed::new(&mut buf);
            framed.codec_mut().max_allowed_packet = MAX_PAYLOAD_LEN;
            framed.send(&mut &*vec![0_u8; 0]).unwrap();
            framed.send(&mut &*vec![0_u8; 1]).unwrap();
            framed.send(&mut &*vec![0_u8; MAX_PAYLOAD_LEN]).unwrap();
        }
        let mut buf = &buf[..];
        let mut framed = MySyncFramed::new(&mut buf);
        framed.codec_mut().max_allowed_packet = MAX_PAYLOAD_LEN;
        let mut dst = vec![];
        assert!(framed.next_packet(&mut dst).unwrap());
        assert_eq!(dst, vec![0_u8; 0]);
        dst.clear();
        assert!(framed.next_packet(&mut dst).unwrap());
        assert_eq!(dst, vec![0_u8; 1]);
        dst.clear();
        assert!(framed.next_packet(&mut dst).unwrap());
        assert_eq!(dst, vec![0_u8; MAX_PAYLOAD_LEN]);
        dst.clear();
        assert!(!framed.next_packet(&mut dst).unwrap());
    }

    #[test]
    #[should_panic(expected = "bytes remaining on stream")]
    fn incomplete_packet() {
        let buf = vec![2, 0, 0, 0];
        let mut buf = &buf[..];
        let mut dst = vec![];
        let mut framed = MySyncFramed::new(&mut buf);
        framed.next_packet(&mut dst).unwrap();
    }
}

#[cfg(feature = "nightly")]
mod bench {
    use std::io;

    use bytes::BytesMut;

    use super::MySyncFramed;
    use crate::constants::MAX_PAYLOAD_LEN;

    struct Null;

    impl io::Write for Null {
        fn write(&mut self, x: &[u8]) -> io::Result<usize> {
            Ok(x.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    struct Loop {
        buf: Vec<u8>,
        pos: usize,
    }

    impl io::Read for Loop {
        fn read(&mut self, x: &mut [u8]) -> io::Result<usize> {
            let count = std::cmp::min(x.len(), self.buf.len() - self.pos);
            x[..count].copy_from_slice(&self.buf[self.pos..(self.pos + count)]);
            self.pos = (self.pos + count) % self.buf.len();
            Ok(count)
        }
    }

    #[bench]
    fn write_small(bencher: &mut test::Bencher) {
        const SIZE: usize = 512;
        let mut framed = MySyncFramed::new(Null);
        framed.codec_mut().max_allowed_packet = 1024 * 1024 * 32;

        let buf = vec![0; SIZE];

        bencher.bytes = (SIZE + 4 + (SIZE / MAX_PAYLOAD_LEN * 4)) as u64;
        bencher.iter(|| {
            framed.send(&mut &*buf).unwrap();
        });
    }

    #[bench]
    fn write_med(bencher: &mut test::Bencher) {
        const SIZE: usize = 1024 * 1024;
        let mut framed = MySyncFramed::new(Null);
        framed.codec_mut().max_allowed_packet = 1024 * 1024 * 32;

        let buf = vec![0; SIZE];

        bencher.bytes = (SIZE + 4 + (SIZE / MAX_PAYLOAD_LEN * 4)) as u64;
        bencher.iter(|| {
            framed.send(&mut &*buf).unwrap();
        });
    }

    #[bench]
    fn write_large(bencher: &mut test::Bencher) {
        const SIZE: usize = 1024 * 1024 * 64;
        let mut framed = MySyncFramed::new(Null);
        framed.codec_mut().max_allowed_packet = 1024 * 1024 * 64;

        let buf = vec![0; SIZE];

        bencher.bytes = (SIZE + 4 + (SIZE / MAX_PAYLOAD_LEN * 4)) as u64;
        bencher.iter(|| {
            framed.send(&mut &*buf).unwrap();
        });
    }

    #[bench]
    fn read_small(bencher: &mut test::Bencher) {
        const SIZE: usize = 512;
        let mut buf = vec![];
        let mut framed = MySyncFramed::new(&mut buf);

        framed.send(&mut &*vec![0; SIZE]).unwrap();

        bencher.bytes = buf.len() as u64;
        let input = Loop { buf, pos: 0 };
        let mut framed = MySyncFramed::new(input);
        let mut buf = BytesMut::new();
        bencher.iter(|| {
            framed.codec_mut().reset_seq_id();
            assert!(framed.next_packet(&mut buf).unwrap());
            buf.clear();
        });
    }

    #[bench]
    fn read_med(bencher: &mut test::Bencher) {
        const SIZE: usize = 1024 * 1024;
        let mut buf = vec![];
        let mut framed = MySyncFramed::new(&mut buf);

        framed.send(&mut &*vec![0; SIZE]).unwrap();

        bencher.bytes = buf.len() as u64;
        let input = Loop { buf, pos: 0 };
        let mut framed = MySyncFramed::new(input);
        let mut buf = BytesMut::new();
        bencher.iter(|| {
            framed.codec_mut().reset_seq_id();
            assert!(framed.next_packet(&mut buf).unwrap());
            buf.clear();
        });
    }

    #[bench]
    fn read_large(bencher: &mut test::Bencher) {
        const SIZE: usize = 1024 * 1024 * 32;
        let mut buf = vec![];
        let mut framed = MySyncFramed::new(&mut buf);
        framed.codec_mut().max_allowed_packet = 1024 * 1024 * 32;

        framed.send(&mut &*vec![0; SIZE]).unwrap();

        bencher.bytes = buf.len() as u64;
        let input = Loop { buf, pos: 0 };
        let mut framed = MySyncFramed::new(input);
        framed.codec_mut().max_allowed_packet = 1024 * 1024 * 32;
        let mut buf = BytesMut::new();
        bencher.iter(|| {
            framed.codec_mut().reset_seq_id();
            assert!(framed.next_packet(&mut buf).unwrap());
            buf.clear();
        });
    }
}
