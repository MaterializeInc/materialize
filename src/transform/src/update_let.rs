// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Re-assign type information and identifiers to each `Get` to ensure
//! uniqueness of identifiers.

use std::collections::HashMap;

use crate::TransformArgs;
use mz_expr::visit::VisitChildren;
use mz_expr::{Id, LocalId, MirRelationExpr, RECURSION_LIMIT};
use mz_ore::id_gen::IdGen;
use mz_ore::stack::{CheckedRecursion, RecursionGuard};
use mz_repr::RelationType;

/// Refreshes identifiers and types for local let bindings.
///
/// The analysis is capable of handling shadowing of identifiers, which
/// *shouldn't* happen, but if it does and we wanted to kick and scream,
/// this is one place we could do that. Instead, we'll just come up with
/// guaranteed unique names for each let binding.
#[derive(Debug)]
pub struct UpdateLet {
    recursion_guard: RecursionGuard,
}

impl Default for UpdateLet {
    fn default() -> UpdateLet {
        UpdateLet {
            recursion_guard: RecursionGuard::with_limit(RECURSION_LIMIT),
        }
    }
}

impl CheckedRecursion for UpdateLet {
    fn recursion_guard(&self) -> &RecursionGuard {
        &self.recursion_guard
    }
}

impl crate::Transform for UpdateLet {
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        args: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        *args.id_gen = IdGen::default(); // Get a fresh IdGen.
        self.action(relation, &mut HashMap::new(), args.id_gen)
    }
}

impl UpdateLet {
    /// Re-assign type information and identifier to each `Get`.
    pub fn action(
        &self,
        relation: &mut MirRelationExpr,
        remap: &mut HashMap<LocalId, (LocalId, RelationType)>,
        id_gen: &mut IdGen,
    ) -> Result<(), crate::TransformError> {
        self.checked_recur(|_| {
            match relation {
                MirRelationExpr::Let { id, value, body } => {
                    self.action(value, remap, id_gen)?;
                    // If a local id, assign a new identifier and refresh the type.
                    let new_id = LocalId::new(id_gen.allocate_id());
                    let prev = remap.insert(id.clone(), (new_id, value.typ()));
                    self.action(body, remap, id_gen)?;
                    remap.remove(id);
                    if let Some(prev_stuff) = prev {
                        remap.insert(id.clone(), prev_stuff);
                    }
                    *id = new_id;
                    Ok(())
                }
                MirRelationExpr::Get { id, typ } => {
                    if let Id::Local(local_id) = id {
                        if let Some((new_id, new_type)) = remap.get(local_id) {
                            *local_id = new_id.clone();
                            *typ = new_type.clone()
                        }
                    }
                    Ok(())
                }
                _ => relation.try_visit_mut_children(|e| self.action(e, remap, id_gen)),
            }
        })
    }
}
