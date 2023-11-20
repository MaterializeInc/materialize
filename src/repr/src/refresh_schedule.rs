// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::Serialize;
use mz_proto::{RustType, TryFromProtoError};
use mz_proto::IntoRustIfSome;
use crate::adt::interval::Interval;

include!(concat!(env!("OUT_DIR"), "/mz_repr.refresh_schedule.rs"));

#[derive(Clone, Debug, Serialize)]
pub struct RefreshSchedule {
    pub interval: Interval,
    //////// todo: time of first refresh will also come here
}

impl RustType<ProtoRefreshSchedule> for RefreshSchedule {
    fn into_proto(&self) -> ProtoRefreshSchedule {
        ProtoRefreshSchedule {
            interval: Some(self.interval.into_proto()),
        }
    }

    fn from_proto(proto: ProtoRefreshSchedule) -> Result<Self, TryFromProtoError> {
        Ok(RefreshSchedule {
            interval: proto.interval.into_rust_if_some("ProtoRefreshSchedule::interval")?,
        })
    }
}
