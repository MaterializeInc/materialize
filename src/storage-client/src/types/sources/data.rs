// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `SourceData` and related traits, e.g. other structs that can be taken to/from `SourceData`.

use std::ops::{Deref, DerefMut};

use anyhow::{anyhow, bail};
use bytes::BufMut;
use prost::Message;

use mz_expr::PartitionId;
use mz_persist_types::Codec;
use mz_proto::{ProtoType, RustType, TryFromProtoError};
use mz_repr::adt::range::RangeBoundDesc;
use mz_repr::{Datum, Row};
use mz_timely_util::order::{Interval, Partitioned, RangeBound};

use crate::types::errors::DataflowError;

use super::MzOffset;

include!(concat!(
    env!("OUT_DIR"),
    "/mz_storage_client.types.sources.data.rs"
));

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SourceData(pub Result<Row, DataflowError>);

impl Deref for SourceData {
    type Target = Result<Row, DataflowError>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for SourceData {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl RustType<ProtoSourceData> for SourceData {
    fn into_proto(&self) -> ProtoSourceData {
        use proto_source_data::Kind;
        ProtoSourceData {
            kind: Some(match &**self {
                Ok(row) => Kind::Ok(row.into_proto()),
                Err(err) => Kind::Err(err.into_proto()),
            }),
        }
    }

    fn from_proto(proto: ProtoSourceData) -> Result<Self, TryFromProtoError> {
        use proto_source_data::Kind;
        match proto.kind {
            Some(kind) => match kind {
                Kind::Ok(row) => Ok(SourceData(Ok(row.into_rust()?))),
                Kind::Err(err) => Ok(SourceData(Err(err.into_rust()?))),
            },
            None => Result::Err(TryFromProtoError::missing_field("ProtoSourceData::kind")),
        }
    }
}

impl Codec for SourceData {
    fn codec_name() -> String {
        "protobuf[SourceData]".into()
    }

    fn encode<B: BufMut>(&self, buf: &mut B) {
        self.into_proto()
            .encode(buf)
            .expect("no required fields means no initialization errors");
    }

    fn decode(buf: &[u8]) -> Result<Self, String> {
        let proto = ProtoSourceData::decode(buf).map_err(|err| err.to_string())?;
        proto.into_rust().map_err(|err| err.to_string())
    }
}

impl From<Partitioned<PartitionId, MzOffset>> for SourceData {
    fn from(partition: Partitioned<PartitionId, MzOffset>) -> SourceData {
        use Datum::{Int32, Null};
        use PartitionId::Kafka;
        let mut row = Row::with_capacity(2);
        let mut packer = row.packer();

        match partition.interval() {
            Interval::Range(l, u) => match (l, u) {
                (RangeBound::Bottom, RangeBound::Top) => {
                    packer
                        .push_range(
                            RangeBoundDesc::new(Null, false),
                            RangeBoundDesc::new(Null, false),
                        )
                        .unwrap();
                }
                (RangeBound::Bottom, RangeBound::Elem(Kafka(pid))) => {
                    packer
                        .push_range(
                            RangeBoundDesc::new(Null, false),
                            RangeBoundDesc::new(Int32(*pid), false),
                        )
                        .unwrap();
                }
                (RangeBound::Elem(Kafka(pid)), RangeBound::Top) => {
                    packer
                        .push_range(
                            RangeBoundDesc::new(Int32(*pid), false),
                            RangeBoundDesc::new(Null, false),
                        )
                        .unwrap();
                }
                (RangeBound::Elem(Kafka(l_pid)), RangeBound::Elem(Kafka(u_pid))) => {
                    packer
                        .push_range(
                            RangeBoundDesc::new(Int32(*l_pid), false),
                            RangeBoundDesc::new(Int32(*u_pid), false),
                        )
                        .unwrap();
                }
                // PartitionId::None is not serialized to SourceData.
                (RangeBound::Elem(PartitionId::None), _)
                | (_, RangeBound::Elem(PartitionId::None)) => {}
                o => unreachable!("don't know how to handle this partition {o:?}"),
            },
            Interval::Point(Kafka(pid)) => {
                packer
                    .push_range(
                        RangeBoundDesc::new(Int32(*pid), true),
                        RangeBoundDesc::new(
                            match pid.checked_add(1) {
                                Some(next_pid) => Int32(next_pid),
                                None => Null,
                            },
                            false,
                        ),
                    )
                    .unwrap();
            }
            Interval::Point(PartitionId::None) => {}
        }

        packer.push(Datum::UInt64(partition.timestamp().offset));
        SourceData(Ok(row))
    }
}

impl TryFrom<SourceData> for Partitioned<PartitionId, MzOffset> {
    type Error = anyhow::Error;
    fn try_from(data: SourceData) -> Result<Self, Self::Error> {
        use PartitionId::Kafka;

        let row = data.0.map_err(|_| anyhow!("invalid binding"))?;
        let mut datums = row.iter();
        Ok(match (datums.next(), datums.next()) {
            (Some(Datum::Range(range)), Some(Datum::UInt64(offset))) => {
                let range = range
                    .inner
                    .ok_or_else(|| anyhow!("remap shards do not currently support empty ranges"))?;

                if range.upper.inclusive {
                    bail!("upper bounds are never inclusive");
                }

                let lower = range
                    .lower
                    .bound
                    .map(|lower| Kafka(lower.datum().unwrap_int32()));

                let upper = range
                    .upper
                    .bound
                    .map(|upper| Kafka(upper.datum().unwrap_int32()));

                if upper.is_some() && upper < lower {
                    bail!("range invariant violated: lower must be <= upper, but got lower {:?}, upper {:?}", lower, upper);
                }

                if range.lower.inclusive {
                    let lower = match (lower, upper) {
                        (Some(lower @ Kafka(l)), Some(Kafka(u))) => {
                            if u.checked_sub(l) != Some(1) {
                                bail!("point data always written as [lower, lower + 1)");
                            }
                            lower
                        }
                        (Some(lower @ Kafka(l)), None) => {
                            if l < i32::MAX {
                                bail!("point data always written as [lower, lower + 1)");
                            }
                            lower
                        }
                        _ => bail!("non-partition data packed into column in row"),
                    };

                    Partitioned::with_partition(lower, MzOffset::from(offset))
                } else {
                    if offset != 0 {
                        bail!("range type ({:?}) with non-0 offset {}", range, offset)
                    }

                    Partitioned::with_range(lower, upper, MzOffset::from(offset))
                }
            }
            (Some(Datum::UInt64(offset)), None) => {
                Partitioned::with_partition(PartitionId::None, MzOffset::from(offset))
            }
            invalid_binding => bail!("invalid binding {:?}", invalid_binding),
        })
    }
}
