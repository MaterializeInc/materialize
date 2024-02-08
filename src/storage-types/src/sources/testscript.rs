// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types related to testscript sources

use mz_proto::{RustType, TryFromProtoError};
use mz_repr::{ColumnType, GlobalId, RelationDesc, ScalarType};
use once_cell::sync::Lazy;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use crate::sources::SourceConnection;

include!(concat!(
    env!("OUT_DIR"),
    "/mz_storage_types.sources.testscript.rs"
));

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TestScriptSourceConnection {
    pub desc_json: String,
}

pub static TEST_SCRIPT_PROGRESS_DESC: Lazy<RelationDesc> =
    Lazy::new(|| RelationDesc::empty().with_column("offset", ScalarType::UInt64.nullable(true)));

impl SourceConnection for TestScriptSourceConnection {
    fn name(&self) -> &'static str {
        "testscript"
    }

    fn upstream_name(&self) -> Option<&str> {
        None
    }

    fn key_desc(&self) -> RelationDesc {
        RelationDesc::empty().with_column("key", ScalarType::Bytes.nullable(true))
    }

    fn value_desc(&self) -> RelationDesc {
        RelationDesc::empty().with_column("value", ScalarType::Bytes.nullable(true))
    }

    fn timestamp_desc(&self) -> RelationDesc {
        TEST_SCRIPT_PROGRESS_DESC.clone()
    }

    fn connection_id(&self) -> Option<GlobalId> {
        None
    }

    fn metadata_columns(&self) -> Vec<(&str, ColumnType)> {
        vec![]
    }
}

impl crate::AlterCompatible for TestScriptSourceConnection {}

impl RustType<ProtoTestScriptSourceConnection> for TestScriptSourceConnection {
    fn into_proto(&self) -> ProtoTestScriptSourceConnection {
        ProtoTestScriptSourceConnection {
            desc_json: self.desc_json.clone(),
        }
    }

    fn from_proto(proto: ProtoTestScriptSourceConnection) -> Result<Self, TryFromProtoError> {
        Ok(TestScriptSourceConnection {
            desc_json: proto.desc_json,
        })
    }
}
