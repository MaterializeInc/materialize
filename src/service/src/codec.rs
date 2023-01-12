// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! gRPC stats collecting codec for the [client](crate::client) module.
//!
//! The implementation of StatCodec is based on tonic's ProstCodec.

use std::fmt::Debug;
use std::marker::PhantomData;

use prost::bytes::{Buf, BufMut};
use prost::{DecodeError, Message};
use tonic::codec::{Codec, Decoder, Encoder};
use tonic::{Code, Status};

pub trait StatsCollector<C, R>: Clone + Debug + Send + Sync {
    fn send_event(&self, item: &C, size: usize);
    fn receive_event(&self, item: &R, size: usize);
}

#[derive(Debug, Clone, Default)]
pub struct StatEncoder<C, R, S> {
    _pd: PhantomData<(C, R)>,
    stats_collector: S,
}

impl<C, R, S> StatEncoder<C, R, S>
where
    S: StatsCollector<C, R>,
{
    pub fn new(stats_collector: S) -> StatEncoder<C, R, S> {
        StatEncoder {
            _pd: Default::default(),
            stats_collector,
        }
    }
}

impl<C, R, S> Encoder for StatEncoder<C, R, S>
where
    C: Message,
    S: StatsCollector<C, R>,
{
    type Item = C;
    type Error = Status;

    fn encode(
        &mut self,
        item: Self::Item,
        buf: &mut tonic::codec::EncodeBuf<'_>,
    ) -> Result<(), Self::Error> {
        let initial_remaining = buf.remaining_mut();
        item.encode(buf)
            .expect("Message only errors if not enough space");
        let encoded_len = initial_remaining - buf.remaining_mut();
        self.stats_collector.send_event(&item, encoded_len);

        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct StatDecoder<C, R, S> {
    _pd: PhantomData<(C, R)>,
    stats_collector: S,
}

impl<C, R, S> StatDecoder<C, R, S> {
    pub fn new(stats_collector: S) -> StatDecoder<C, R, S> {
        StatDecoder {
            _pd: PhantomData,
            stats_collector,
        }
    }
}

impl<C, R, S> Decoder for StatDecoder<C, R, S>
where
    R: Default + Message,
    S: StatsCollector<C, R>,
{
    type Item = R;
    type Error = Status;

    fn decode(
        &mut self,
        buf: &mut tonic::codec::DecodeBuf<'_>,
    ) -> Result<Option<Self::Item>, Self::Error> {
        let remaining_before = buf.remaining();
        let item = Message::decode(buf).map_err(from_decode_error)?;
        self.stats_collector.receive_event(&item, remaining_before);
        Ok(Some(item))
    }
}

fn from_decode_error(error: DecodeError) -> Status {
    // Map Protobuf parse errors to an INTERNAL status code, as per
    // https://github.com/grpc/grpc/blob/master/doc/statuscodes.md
    Status::new(Code::Internal, error.to_string())
}

#[derive(Debug, Clone)]
pub struct StatCodec<C, R, S> {
    _pd: PhantomData<(C, R)>,
    stats_collector: S,
}

impl<C, R, S> StatCodec<C, R, S> {
    pub fn new(stats_collector: S) -> StatCodec<C, R, S> {
        StatCodec {
            _pd: PhantomData,
            stats_collector,
        }
    }
}

impl<C, R, S> Codec for StatCodec<C, R, S>
where
    C: Message + 'static,
    R: Default + Message + 'static,
    S: StatsCollector<C, R> + 'static,
{
    type Encode = C;
    type Decode = R;
    type Encoder = StatEncoder<C, R, S>;
    type Decoder = StatDecoder<C, R, S>;

    fn encoder(&mut self) -> Self::Encoder {
        StatEncoder::new(self.stats_collector.clone())
    }

    fn decoder(&mut self) -> Self::Decoder {
        StatDecoder::new(self.stats_collector.clone())
    }
}
