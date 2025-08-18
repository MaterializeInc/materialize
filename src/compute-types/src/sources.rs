// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types for describing dataflow sources.

use mz_repr::RelationType;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

/// A description of an instantiation of a source.
///
/// This includes a description of the source, but additionally any
/// context-dependent options like the ability to apply filtering and
/// projection to the records as they emerge.
#[derive(Arbitrary, Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceInstanceDesc<M> {
    /// Arguments for this instantiation of the source.
    pub arguments: SourceInstanceArguments,
    /// Additional metadata used by the storage client of a compute instance to read it.
    pub storage_metadata: M,
    /// The relation type of this source
    pub typ: RelationType,
}

/// Per-source construction arguments.
#[derive(Arbitrary, Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceInstanceArguments {
    /// Linear operators to be applied record-by-record.
    pub operators: Option<mz_expr::MapFilterProject>,
}
