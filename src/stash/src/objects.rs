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

#[cfg(test)]
mod test {
    use std::collections::BTreeSet;
    use std::fs;
    use std::io::{BufRead, BufReader};

    use crate::STASH_VERSION;

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
        //
        // TODO(parkmycar): Change `15` to be MIN_STASH_VERSION, once we delete all of the JSON
        // migration code.
        for version in 15..=STASH_VERSION {
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
