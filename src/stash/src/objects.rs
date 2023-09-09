// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The current types that are serialized in the Stash.

use std::time::Duration;

use bytes::Bytes;
use mz_proto::IntoRustIfSome;
use timely::progress::Antichain;

use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem};
use mz_repr::role_id::RoleId;
use mz_repr::{GlobalId, Timestamp};

pub use mz_proto::{RustType, TryFromProtoError};

pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/objects.rs"));
}

impl RustType<proto::RoleId> for RoleId {
    fn into_proto(&self) -> proto::RoleId {
        let value = match self {
            RoleId::User(id) => proto::role_id::Value::User(*id),
            RoleId::System(id) => proto::role_id::Value::System(*id),
            RoleId::Public => proto::role_id::Value::Public(Default::default()),
        };

        proto::RoleId { value: Some(value) }
    }

    fn from_proto(proto: proto::RoleId) -> Result<Self, TryFromProtoError> {
        let value = proto
            .value
            .ok_or_else(|| TryFromProtoError::missing_field("RoleId::value"))?;
        let id = match value {
            proto::role_id::Value::User(id) => RoleId::User(id),
            proto::role_id::Value::System(id) => RoleId::System(id),
            proto::role_id::Value::Public(_) => RoleId::Public,
        };
        Ok(id)
    }
}

impl RustType<proto::AclMode> for AclMode {
    fn into_proto(&self) -> proto::AclMode {
        proto::AclMode {
            bitflags: self.bits(),
        }
    }

    fn from_proto(proto: proto::AclMode) -> Result<Self, TryFromProtoError> {
        AclMode::from_bits(proto.bitflags).ok_or_else(|| {
            TryFromProtoError::InvalidBitFlags(format!("Invalid AclMode from Stash {proto:?}"))
        })
    }
}

impl RustType<proto::MzAclItem> for MzAclItem {
    fn into_proto(&self) -> proto::MzAclItem {
        proto::MzAclItem {
            grantee: Some(self.grantee.into_proto()),
            grantor: Some(self.grantor.into_proto()),
            acl_mode: Some(self.acl_mode.into_proto()),
        }
    }

    fn from_proto(proto: proto::MzAclItem) -> Result<Self, TryFromProtoError> {
        Ok(MzAclItem {
            grantee: proto.grantee.into_rust_if_some("MzAclItem::grantee")?,
            grantor: proto.grantor.into_rust_if_some("MzAclItem::grantor")?,
            acl_mode: proto.acl_mode.into_rust_if_some("MzAclItem::acl_mode")?,
        })
    }
}

impl<T> RustType<proto::TimestampAntichain> for Antichain<T>
where
    T: RustType<proto::Timestamp> + Clone + timely::PartialOrder,
{
    fn into_proto(&self) -> proto::TimestampAntichain {
        proto::TimestampAntichain {
            elements: self
                .elements()
                .into_iter()
                .cloned()
                .map(|e| e.into_proto())
                .collect(),
        }
    }

    fn from_proto(proto: proto::TimestampAntichain) -> Result<Self, TryFromProtoError> {
        let elements: Vec<_> = proto
            .elements
            .into_iter()
            .map(|e| T::from_proto(e))
            .collect::<Result<_, _>>()?;

        Ok(Antichain::from_iter(elements))
    }
}

impl RustType<proto::EpochMillis> for u64 {
    fn into_proto(&self) -> proto::EpochMillis {
        proto::EpochMillis { millis: *self }
    }

    fn from_proto(proto: proto::EpochMillis) -> Result<Self, TryFromProtoError> {
        Ok(proto.millis)
    }
}

impl RustType<proto::Timestamp> for Timestamp {
    fn into_proto(&self) -> proto::Timestamp {
        proto::Timestamp {
            internal: self.into(),
        }
    }

    fn from_proto(proto: proto::Timestamp) -> Result<Self, TryFromProtoError> {
        Ok(Timestamp::new(proto.internal))
    }
}

impl RustType<proto::GlobalId> for GlobalId {
    fn into_proto(&self) -> proto::GlobalId {
        proto::GlobalId {
            value: Some(match self {
                GlobalId::System(x) => proto::global_id::Value::System(*x),
                GlobalId::User(x) => proto::global_id::Value::User(*x),
                GlobalId::Transient(x) => proto::global_id::Value::Transient(*x),
                GlobalId::Explain => proto::global_id::Value::Explain(Default::default()),
            }),
        }
    }

    fn from_proto(proto: proto::GlobalId) -> Result<Self, TryFromProtoError> {
        match proto.value {
            Some(proto::global_id::Value::System(x)) => Ok(GlobalId::System(x)),
            Some(proto::global_id::Value::User(x)) => Ok(GlobalId::User(x)),
            Some(proto::global_id::Value::Transient(x)) => Ok(GlobalId::Transient(x)),
            Some(proto::global_id::Value::Explain(_)) => Ok(GlobalId::Explain),
            None => Err(TryFromProtoError::missing_field("GlobalId::kind")),
        }
    }
}

impl RustType<proto::Duration> for Duration {
    fn into_proto(&self) -> proto::Duration {
        proto::Duration {
            secs: self.as_secs(),
            nanos: self.subsec_nanos(),
        }
    }

    fn from_proto(proto: proto::Duration) -> Result<Self, TryFromProtoError> {
        Ok(Duration::new(proto.secs, proto.nanos))
    }
}

impl proto::Duration {
    pub const fn from_secs(secs: u64) -> proto::Duration {
        proto::Duration { secs, nanos: 0 }
    }
}

impl From<String> for proto::StringWrapper {
    fn from(value: String) -> Self {
        proto::StringWrapper { inner: value }
    }
}

/// Denotes that `Self` is wire compatible with type `T`.
///
/// You should not implement this yourself, instead use the `wire_compatible!` macro.
pub unsafe trait WireCompatible<T: prost::Message>: prost::Message + Default {
    /// Converts the type `T` into `Self` by serializing `T` and deserializing as `Self`.
    fn convert(old: &T) -> Self {
        let bytes = old.encode_to_vec();
        // Note: use Bytes to enable possible re-use of the underlying buffer.
        let bytes = Bytes::from(bytes);
        Self::decode(bytes).expect("wire compatible")
    }
}

// SAFETY: A message type is trivially wire compatible with itself.
unsafe impl<T: prost::Message + Default + Clone> WireCompatible<T> for T {
    fn convert(old: &Self) -> Self {
        old.clone()
    }
}

/// Defines one protobuf type as wire compatible with another.
///
/// ```text
/// wire_compatible!(objects_v28::DatabaseKey with objects_v27::DatabaseKey);
/// ```
///
/// Internally this will implement the `WireCompatible<B> for <A>`, e.g.
/// `WireCompatible<objects_v27::DatabaseKey> for objects_v28::DatabaseKey` and generate `proptest`
/// cases that will create arbitrary objects of type `B` and assert they can be deserialized with
/// type `A`, and vice versa.
macro_rules! wire_compatible {
    ($a:ident $(:: $a_sub:ident)* with $b:ident $(:: $b_sub:ident)*) => {
        ::static_assertions::assert_impl_all!(
            $a $(::$a_sub)* : ::proptest::arbitrary::Arbitrary, ::prost::Message, Default,
        );
        ::static_assertions::assert_impl_all!(
            $b $(::$b_sub)*  : ::proptest::arbitrary::Arbitrary, ::prost::Message, Default,
        );

        // SAFETY: Below we assert that these types are wire compatible by generating arbitrary
        // structs, encoding in one, and then decoding in the other.
        unsafe impl $crate::objects::WireCompatible< $b $(::$b_sub)* > for $a $(::$a_sub)* {}
        unsafe impl $crate::objects::WireCompatible< $a $(::$a_sub)* > for $b $(::$b_sub)* {}

        ::paste::paste! {
            ::proptest::proptest! {
                #[mz_ore::test]
                #[cfg_attr(miri, ignore)] // slow
                fn [<proptest_wire_compat_ $a:snake $(_$a_sub:snake)* _to_ $b:snake $(_$b_sub:snake)* >](a: $a $(::$a_sub)* ) {
                    use ::prost::Message;
                    let a_bytes = a.encode_to_vec();
                    let b_decoded = $b $(::$b_sub)*::decode(&a_bytes[..]);
                    ::proptest::prelude::prop_assert!(b_decoded.is_ok());

                    // Maybe superfluous, but this is a method called in production.
                    let b_decoded = b_decoded.expect("asserted Ok");
                    let b_converted: $b $(::$b_sub)* = $crate::objects::WireCompatible::convert(&a);
                    assert_eq!(b_decoded, b_converted);

                    let b_bytes = b_decoded.encode_to_vec();
                    ::proptest::prelude::prop_assert_eq!(a_bytes, b_bytes, "a and b serialize differently");
                }

                #[mz_ore::test]
                #[cfg_attr(miri, ignore)] // slow
                fn [<proptest_wire_compat_ $b:snake $(_$b_sub:snake)* _to_ $a:snake $(_$a_sub:snake)* >](b: $b $(::$b_sub)* ) {
                    use ::prost::Message;
                    let b_bytes = b.encode_to_vec();
                    let a_decoded = $a $(::$a_sub)*::decode(&b_bytes[..]);
                    ::proptest::prelude::prop_assert!(a_decoded.is_ok());

                    // Maybe superfluous, but this is a method called in production.
                    let a_decoded = a_decoded.expect("asserted Ok");
                    let a_converted: $a $(::$a_sub)* = $crate::objects::WireCompatible::convert(&b);
                    assert_eq!(a_decoded, a_converted);

                    let a_bytes = a_decoded.encode_to_vec();
                    ::proptest::prelude::prop_assert_eq!(a_bytes, b_bytes, "a and b serialize differently");
                }
            }
        }
    };
}
pub(crate) use wire_compatible;

#[cfg(test)]
mod test {
    use std::collections::BTreeSet;
    use std::fs;
    use std::io::{BufRead, BufReader};

    use crate::{MIN_STASH_VERSION, STASH_VERSION};

    // Note: Feel free to update this path if the protos move.
    const PROTO_DIRECTORY: &str = "protos";

    #[mz_ore::test]
    fn test_assert_snapshots_exist() {
        // Get all of the files in the snapshot directory, with the `.proto` extension.
        let mut filenames: BTreeSet<_> = fs::read_dir(PROTO_DIRECTORY)
            .expect("failed to read protos dir")
            .map(|entry| entry.expect("failed to read dir entry").file_name())
            .map(|filename| filename.to_str().expect("utf8").to_string())
            .filter(|filename| filename.ends_with("proto"))
            .collect();

        // Assert objects.proto exists.
        assert!(filenames.remove("objects.proto"));

        // Assert snapshots exist for all of the versions we support.
        for version in MIN_STASH_VERSION..=STASH_VERSION {
            let filename = format!("objects_v{version}.proto");
            assert!(
                filenames.remove(&filename),
                "Missing snapshot for v{version}."
            );
        }

        // Common case. Check to make sure the user bumped the STASH_VERSION.
        if !filenames.is_empty()
            && filenames.remove(&format!("objects_v{}.proto", STASH_VERSION + 1))
        {
            panic!(
                "Found snapshot for v{}, please also bump `STASH_VERSION`.",
                STASH_VERSION + 1
            )
        }

        // Assert there aren't any extra snapshots.
        assert!(
            filenames.is_empty(),
            "Found snapshots for unsupported Stash versions {filenames:?}.\nIf you just increased `MIN_STASH_VERSION`, then please delete the old snapshots. If you created a new snapshot, please bump `STASH_VERSION`."
        );
    }

    #[mz_ore::test]
    fn test_assert_current_snapshot() {
        // Read the content from both files.
        let current = fs::File::open(format!("{PROTO_DIRECTORY}/objects.proto"))
            .map(BufReader::new)
            .expect("read current");
        let snapshot = fs::File::open(format!("{PROTO_DIRECTORY}/objects_v{STASH_VERSION}.proto"))
            .map(BufReader::new)
            .expect("read snapshot");

        // Read in all of the lines so we can compare the content of †he files.
        let current: Vec<_> = current
            .lines()
            .map(|r| r.expect("failed to read line from current"))
            // Filter out the package name, since we expect that to be different.
            .filter(|line| line != "package objects;")
            .collect();
        let snapshot: Vec<_> = snapshot
            .lines()
            .map(|r| r.expect("failed to read line from current"))
            // Filter out the package name, since we expect that to be different.
            .filter(|line| line != &format!("package objects_v{STASH_VERSION};"))
            .collect();

        // Note: objects.proto and objects_v<STASH_VERSION>.proto should be exactly the same. The
        // reason being, when bumping the Stash to the next version, STASH_VERSION + 1, we need a
        // snapshot to migrate _from_, which should be a snapshot of how the protos are today.
        // Hence why the two files should be exactly the same.
        similar_asserts::assert_eq!(current, snapshot);
    }
}
