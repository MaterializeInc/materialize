// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::errors::{proto_dataflow_error, ProtoUpsertValueError, ProtoUpsertValueErrorLegacy};

#[derive(Clone, Debug, PartialEq)]
pub enum ProtoUpsertValueErrorShim {
    New(ProtoUpsertValueError),
    Legacy(Box<ProtoUpsertValueErrorLegacy>),
}

impl Default for ProtoUpsertValueErrorShim {
    fn default() -> Self {
        Self::New(ProtoUpsertValueError::default())
    }
}

impl prost::Message for ProtoUpsertValueErrorShim {
    fn encode_raw<B>(&self, buf: &mut B)
    where
        B: bytes::BufMut,
        Self: Sized,
    {
        match self {
            Self::New(new) => new.encode_raw(buf),
            Self::Legacy(legacy) => legacy.encode_raw(buf),
        }
    }

    fn merge_field<B>(
        &mut self,
        tag: u32,
        wire_type: prost::encoding::WireType,
        buf: &mut B,
        ctx: prost::encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError>
    where
        B: bytes::Buf,
        Self: Sized,
    {
        if tag == 1 {
            // This is  the problematic tag that changed types. Here we need to do some type
            // "inference" to figure out if we're decoding the new or the legacy format and update
            // the variant of self accordingly.

            // We need `merge_field` two times but we can only read the provided buffer once, so we
            // make a local copy. This would normally be tricky with only the `Buf` bound but we
            // know that we only get here as a result of a `Codec::decode` call, which has a &[u8]
            // argument that is what B will be. This means that the `B: Buf` generic type is backed
            // by a contiguous buffer and therefore calling `Buf::chunk` will give us all the data.
            let data = buf.chunk().to_vec();
            assert_eq!(data.len(), buf.remaining());

            let mut new_buf = &*data;
            let mut new_proto = ProtoUpsertValueError::default();
            let new_merge = new_proto.merge_field(tag, wire_type, &mut new_buf, ctx.clone());

            let mut legacy_buf = &*data;
            let mut legacy_proto = ProtoUpsertValueErrorLegacy::default();
            let legacy_merge =
                legacy_proto.merge_field(tag, wire_type, &mut legacy_buf, ctx.clone());

            // Let's see what happened
            let (is_legacy, ret) = match (new_merge, legacy_merge) {
                (Ok(()), Err(_)) => (false, Ok(())),
                (Err(_), Ok(_)) => (true, Ok(())),
                // If both succeeded we need to look deeper into the abyss and use heuristics
                (Ok(()), Ok(())) => {
                    let maybe_new = match &new_proto.inner {
                        Some(decode_err) => decode_err.raw.is_some(),
                        None => false,
                    };

                    let maybe_legacy = match &legacy_proto.inner {
                        Some(inner) => match &inner.kind {
                            Some(proto_dataflow_error::Kind::DecodeError(decode_err)) => {
                                decode_err.raw.is_some()
                            }
                            _ => false,
                        },
                        None => false,
                    };

                    if maybe_new == maybe_legacy {
                        panic!("could not figure out the variant: {new_proto:?} {legacy_proto:?}");
                    }

                    (maybe_legacy && !maybe_new, Ok(()))
                }
                // If both failed we will arbitrarily pick the new variant. This is not expected to
                // happen
                (Err(err), Err(_)) => (false, Err(err)),
            };
            if is_legacy {
                fail::fail_point!("reject_legacy_upsert_errors");
                *self = Self::Legacy(Box::new(legacy_proto));
                buf.advance(data.len() - legacy_buf.len());
            } else {
                *self = Self::New(new_proto);
                buf.advance(data.len() - new_buf.len());
            }
            ret
        } else {
            // For all other tags we simply continue with what we have since the determination has
            // already happened.
            match self {
                Self::New(new) => new.merge_field(tag, wire_type, buf, ctx),
                Self::Legacy(legacy) => legacy.merge_field(tag, wire_type, buf, ctx),
            }
        }
    }

    fn encoded_len(&self) -> usize {
        match self {
            Self::New(new) => new.encoded_len(),
            Self::Legacy(legacy) => legacy.encoded_len(),
        }
    }

    fn clear(&mut self) {
        match self {
            Self::New(new) => new.clear(),
            Self::Legacy(legacy) => legacy.clear(),
        }
    }
}

#[cfg(test)]
mod test {
    use mz_proto::RustType;
    use mz_repr::{Datum, Row};
    use prost::Message;

    use crate::errors::{proto_dataflow_error, ProtoDataflowError};
    use crate::errors::{DecodeError, DecodeErrorKind};

    use super::*;

    #[mz_ore::test]
    fn sanity_check() {
        // Create an assortment of different decode errors to make sure we can correctly tell apart
        // the two protobuf serializations. We are especially interested in pathological cases
        // (empty strings/empty vectors) which might trip the protobuf decoder up.
        let decode_errors = vec![
            // Empty Text kind plus zeros
            DecodeError {
                kind: DecodeErrorKind::Text("".into()),
                raw: vec![],
            },
            DecodeError {
                kind: DecodeErrorKind::Text("".into()),
                raw: vec![0; 1],
            },
            DecodeError {
                kind: DecodeErrorKind::Text("".into()),
                raw: vec![0; 2],
            },
            DecodeError {
                kind: DecodeErrorKind::Text("".into()),
                raw: vec![0; 4],
            },
            DecodeError {
                kind: DecodeErrorKind::Text("".into()),
                raw: vec![0; 16],
            },
            DecodeError {
                kind: DecodeErrorKind::Text("".into()),
                raw: vec![0; 32],
            },
            DecodeError {
                kind: DecodeErrorKind::Text("".into()),
                raw: vec![0; 64],
            },
            DecodeError {
                kind: DecodeErrorKind::Text("".into()),
                raw: vec![0; 128],
            },
            DecodeError {
                kind: DecodeErrorKind::Text("".into()),
                raw: vec![0; 256],
            },
            DecodeError {
                kind: DecodeErrorKind::Text("".into()),
                raw: vec![0; 512],
            },
            DecodeError {
                kind: DecodeErrorKind::Text("".into()),
                raw: vec![0; 1024],
            },
            // None-empty Text kind plus zeros
            DecodeError {
                kind: DecodeErrorKind::Text("foo".into()),
                raw: vec![],
            },
            DecodeError {
                kind: DecodeErrorKind::Text("foo".into()),
                raw: vec![0; 1],
            },
            DecodeError {
                kind: DecodeErrorKind::Text("foo".into()),
                raw: vec![0; 2],
            },
            DecodeError {
                kind: DecodeErrorKind::Text("foo".into()),
                raw: vec![0; 4],
            },
            DecodeError {
                kind: DecodeErrorKind::Text("foo".into()),
                raw: vec![0; 16],
            },
            DecodeError {
                kind: DecodeErrorKind::Text("foo".into()),
                raw: vec![0; 32],
            },
            DecodeError {
                kind: DecodeErrorKind::Text("foo".into()),
                raw: vec![0; 64],
            },
            DecodeError {
                kind: DecodeErrorKind::Text("foo".into()),
                raw: vec![0; 128],
            },
            DecodeError {
                kind: DecodeErrorKind::Text("foo".into()),
                raw: vec![0; 256],
            },
            DecodeError {
                kind: DecodeErrorKind::Text("foo".into()),
                raw: vec![0; 512],
            },
            DecodeError {
                kind: DecodeErrorKind::Text("foo".into()),
                raw: vec![0; 1024],
            },
            // Empty Text kind plus 0x42
            DecodeError {
                kind: DecodeErrorKind::Text("".into()),
                raw: vec![],
            },
            DecodeError {
                kind: DecodeErrorKind::Text("".into()),
                raw: vec![0x42; 1],
            },
            DecodeError {
                kind: DecodeErrorKind::Text("".into()),
                raw: vec![0x42; 2],
            },
            DecodeError {
                kind: DecodeErrorKind::Text("".into()),
                raw: vec![0x42; 4],
            },
            DecodeError {
                kind: DecodeErrorKind::Text("".into()),
                raw: vec![0x42; 16],
            },
            DecodeError {
                kind: DecodeErrorKind::Text("".into()),
                raw: vec![0x42; 32],
            },
            DecodeError {
                kind: DecodeErrorKind::Text("".into()),
                raw: vec![0x42; 64],
            },
            DecodeError {
                kind: DecodeErrorKind::Text("".into()),
                raw: vec![0x42; 128],
            },
            DecodeError {
                kind: DecodeErrorKind::Text("".into()),
                raw: vec![0x42; 256],
            },
            DecodeError {
                kind: DecodeErrorKind::Text("".into()),
                raw: vec![0x42; 512],
            },
            DecodeError {
                kind: DecodeErrorKind::Text("".into()),
                raw: vec![0x42; 1024],
            },
            // None-empty Text kind plus 0x42
            DecodeError {
                kind: DecodeErrorKind::Text("fooooo".into()),
                raw: vec![],
            },
            DecodeError {
                kind: DecodeErrorKind::Text("fooooo".into()),
                raw: vec![0x42; 1],
            },
            DecodeError {
                kind: DecodeErrorKind::Text("fooooo".into()),
                raw: vec![0x42; 2],
            },
            DecodeError {
                kind: DecodeErrorKind::Text("fooooo".into()),
                raw: vec![0x42; 4],
            },
            DecodeError {
                kind: DecodeErrorKind::Text("fooooo".into()),
                raw: vec![0x42; 16],
            },
            DecodeError {
                kind: DecodeErrorKind::Text("fooooo".into()),
                raw: vec![0x42; 32],
            },
            DecodeError {
                kind: DecodeErrorKind::Text("fooooo".into()),
                raw: vec![0x42; 64],
            },
            DecodeError {
                kind: DecodeErrorKind::Text("fooooo".into()),
                raw: vec![0x42; 128],
            },
            DecodeError {
                kind: DecodeErrorKind::Text("fooooo".into()),
                raw: vec![0x42; 256],
            },
            DecodeError {
                kind: DecodeErrorKind::Text("fooooo".into()),
                raw: vec![0x42; 512],
            },
            DecodeError {
                kind: DecodeErrorKind::Text("fooooo".into()),
                raw: vec![0x42; 1024],
            },
            // Empty Bytes kind plus zeros
            DecodeError {
                kind: DecodeErrorKind::Bytes("".into()),
                raw: vec![],
            },
            DecodeError {
                kind: DecodeErrorKind::Bytes("".into()),
                raw: vec![0; 1],
            },
            DecodeError {
                kind: DecodeErrorKind::Bytes("".into()),
                raw: vec![0; 2],
            },
            DecodeError {
                kind: DecodeErrorKind::Bytes("".into()),
                raw: vec![0; 4],
            },
            DecodeError {
                kind: DecodeErrorKind::Bytes("".into()),
                raw: vec![0; 16],
            },
            DecodeError {
                kind: DecodeErrorKind::Bytes("".into()),
                raw: vec![0; 32],
            },
            DecodeError {
                kind: DecodeErrorKind::Bytes("".into()),
                raw: vec![0; 64],
            },
            DecodeError {
                kind: DecodeErrorKind::Bytes("".into()),
                raw: vec![0; 128],
            },
            DecodeError {
                kind: DecodeErrorKind::Bytes("".into()),
                raw: vec![0; 256],
            },
            DecodeError {
                kind: DecodeErrorKind::Bytes("".into()),
                raw: vec![0; 512],
            },
            DecodeError {
                kind: DecodeErrorKind::Bytes("".into()),
                raw: vec![0; 1024],
            },
            // None-empty Bytes kind plus zeros
            DecodeError {
                kind: DecodeErrorKind::Bytes("foo".into()),
                raw: vec![],
            },
            DecodeError {
                kind: DecodeErrorKind::Bytes("foo".into()),
                raw: vec![0; 1],
            },
            DecodeError {
                kind: DecodeErrorKind::Bytes("foo".into()),
                raw: vec![0; 2],
            },
            DecodeError {
                kind: DecodeErrorKind::Bytes("foo".into()),
                raw: vec![0; 4],
            },
            DecodeError {
                kind: DecodeErrorKind::Bytes("foo".into()),
                raw: vec![0; 16],
            },
            DecodeError {
                kind: DecodeErrorKind::Bytes("foo".into()),
                raw: vec![0; 32],
            },
            DecodeError {
                kind: DecodeErrorKind::Bytes("foo".into()),
                raw: vec![0; 64],
            },
            DecodeError {
                kind: DecodeErrorKind::Bytes("foo".into()),
                raw: vec![0; 128],
            },
            DecodeError {
                kind: DecodeErrorKind::Bytes("foo".into()),
                raw: vec![0; 256],
            },
            DecodeError {
                kind: DecodeErrorKind::Bytes("foo".into()),
                raw: vec![0; 512],
            },
            DecodeError {
                kind: DecodeErrorKind::Bytes("foo".into()),
                raw: vec![0; 1024],
            },
            // Empty Bytes kind plus 0x42
            DecodeError {
                kind: DecodeErrorKind::Bytes("".into()),
                raw: vec![],
            },
            DecodeError {
                kind: DecodeErrorKind::Bytes("".into()),
                raw: vec![0x42; 1],
            },
            DecodeError {
                kind: DecodeErrorKind::Bytes("".into()),
                raw: vec![0x42; 2],
            },
            DecodeError {
                kind: DecodeErrorKind::Bytes("".into()),
                raw: vec![0x42; 4],
            },
            DecodeError {
                kind: DecodeErrorKind::Bytes("".into()),
                raw: vec![0x42; 16],
            },
            DecodeError {
                kind: DecodeErrorKind::Bytes("".into()),
                raw: vec![0x42; 32],
            },
            DecodeError {
                kind: DecodeErrorKind::Bytes("".into()),
                raw: vec![0x42; 64],
            },
            DecodeError {
                kind: DecodeErrorKind::Bytes("".into()),
                raw: vec![0x42; 128],
            },
            DecodeError {
                kind: DecodeErrorKind::Bytes("".into()),
                raw: vec![0x42; 256],
            },
            DecodeError {
                kind: DecodeErrorKind::Bytes("".into()),
                raw: vec![0x42; 512],
            },
            DecodeError {
                kind: DecodeErrorKind::Bytes("".into()),
                raw: vec![0x42; 1024],
            },
            // None-empty Bytes kind plus 0x42
            DecodeError {
                kind: DecodeErrorKind::Bytes("i love protobuf".into()),
                raw: vec![],
            },
            DecodeError {
                kind: DecodeErrorKind::Bytes("i love protobuf".into()),
                raw: vec![0x42; 1],
            },
            DecodeError {
                kind: DecodeErrorKind::Bytes("i love protobuf".into()),
                raw: vec![0x42; 2],
            },
            DecodeError {
                kind: DecodeErrorKind::Bytes("i love protobuf".into()),
                raw: vec![0x42; 4],
            },
            DecodeError {
                kind: DecodeErrorKind::Bytes("i love protobuf".into()),
                raw: vec![0x42; 16],
            },
            DecodeError {
                kind: DecodeErrorKind::Bytes("i love protobuf".into()),
                raw: vec![0x42; 32],
            },
            DecodeError {
                kind: DecodeErrorKind::Bytes("i love protobuf".into()),
                raw: vec![0x42; 64],
            },
            DecodeError {
                kind: DecodeErrorKind::Bytes("i love protobuf".into()),
                raw: vec![0x42; 128],
            },
            DecodeError {
                kind: DecodeErrorKind::Bytes("i love protobuf".into()),
                raw: vec![0x42; 256],
            },
            DecodeError {
                kind: DecodeErrorKind::Bytes("i love protobuf".into()),
                raw: vec![0x42; 512],
            },
            DecodeError {
                kind: DecodeErrorKind::Bytes("i love protobuf".into()),
                raw: vec![0x42; 1024],
            },
        ];

        // Now we go through each one of them, encode it in two different ways using the normal
        // Proto structs, which is how Materialize 0.68.1 and 0.69.1 would do it, and then attemp
        // to decode them with the shim. The shim should correctly understand if it is reading a
        // new or legacy format.

        // We don't care about the key, the whole determination happens during processing of the
        // firs tag, which is the inner error.
        let key = Row::pack([Datum::String("the key")]);
        for err in decode_errors {
            let inner = ProtoDataflowError {
                kind: Some(proto_dataflow_error::Kind::DecodeError(err.into_proto())),
            };
            let legacy_proto = ProtoUpsertValueErrorLegacy {
                inner: Some(inner),
                for_key: Some(key.into_proto()),
            };
            let legacy_bytes = legacy_proto.encode_to_vec();
            let legacy_shim = ProtoUpsertValueErrorShim::decode(&*legacy_bytes).unwrap();
            assert_eq!(
                legacy_shim,
                ProtoUpsertValueErrorShim::Legacy(Box::new(legacy_proto))
            );

            let new_proto = ProtoUpsertValueError {
                inner: Some(err.into_proto()),
                for_key: Some(key.into_proto()),
            };
            let new_bytes = new_proto.encode_to_vec();
            let new_shim = ProtoUpsertValueErrorShim::decode(&*new_bytes).unwrap();
            assert_eq!(new_shim, ProtoUpsertValueErrorShim::New(new_proto));
        }
    }
}
