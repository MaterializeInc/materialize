// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Custom Protobuf types for the [`url`] crate.

use mz_proto::{RustType, TryFromProtoError};
use proptest::prelude::Strategy;
use url::Url;

include!(concat!(env!("OUT_DIR"), "/mz_repr.url.rs"));

impl RustType<ProtoUrl> for Url {
    fn into_proto(&self) -> ProtoUrl {
        ProtoUrl {
            url: self.to_string(),
        }
    }

    fn from_proto(proto: ProtoUrl) -> Result<Self, TryFromProtoError> {
        Ok(proto.url.parse()?)
    }
}

pub fn any_url() -> impl Strategy<Value = Url> {
    r"(http|https)://[a-z][a-z0-9]{0,10}/?([a-z0-9]{0,5}/?){0,3}".prop_map(|s| s.parse().unwrap())
}

#[cfg(test)]
mod tests {
    use mz_proto::protobuf_roundtrip;
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(4096))]

        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // too slow
        fn url_protobuf_roundtrip(expect in any_url() ) {
            let actual = protobuf_roundtrip::<_, ProtoUrl>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }
}
