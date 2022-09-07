// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_proto::{RustType, TryFromProtoError};
use timely::progress::Antichain;

use crate::Timestamp;

include!(concat!(env!("OUT_DIR"), "/mz_repr.antichain.rs"));

impl RustType<ProtoU64Antichain> for Antichain<Timestamp> {
    fn into_proto(&self) -> ProtoU64Antichain {
        ProtoU64Antichain {
            elements: self.elements().iter().map(Into::into).collect(),
        }
    }

    fn from_proto(proto: ProtoU64Antichain) -> Result<Self, TryFromProtoError> {
        Ok(Antichain::from_iter(
            proto.elements.into_iter().map(Timestamp::from),
        ))
    }
}
