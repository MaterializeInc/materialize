// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Identifies common relation subexpressions and places them behind `Let` bindings.
//!
//! All structurally equivalent expressions, defined recursively as having structurally
//! equivalent inputs, and identical parameters, will be placed behind `Let` bindings.
//! The resulting expressions likely have an excess of `Let` expressions, and therefore
//! we automatically run the `NormalizeLets` transformation to remove those that are not necessary.

use mz_expr::MirRelationExpr;

use crate::normalize_lets::NormalizeLets;
use crate::TransformArgs;

use super::anf::ANF;

/// Identifies common relation subexpressions and places them behind `Let` bindings.
#[derive(Debug)]
pub struct RelationCSE {
    anf: ANF,
    normalize_lets: NormalizeLets,
}

impl RelationCSE {
    /// Constructs a new [`RelationCSE`] instance.
    ///
    /// Also communicates its argument to let normalization.
    pub fn new(inline_mfp: bool) -> RelationCSE {
        RelationCSE {
            anf: ANF::default(),
            normalize_lets: NormalizeLets::new(inline_mfp),
        }
    }
}

impl crate::Transform for RelationCSE {
    #[tracing::instrument(
        target = "optimizer"
        level = "trace",
        skip_all,
        fields(path.segment = "relation_cse")
    )]
    fn transform(
        &self,
        rel: &mut MirRelationExpr,
        _args: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        // Run ANF.
        self.anf.transform_without_trace(rel)?;

        // Run NormalizeLets.
        self.normalize_lets.transform_without_trace(rel)?;

        // Record the result and return.
        mz_repr::explain::trace_plan(&*rel);
        Ok(())
    }
}
