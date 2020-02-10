// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use repr::RelationType;

use crate::{EvalEnv, GlobalId, Id, IdGen, LocalId, RelationExpr, ScalarExpr};

/// Refreshes identifiers and types for local let bindings.
///
/// The analysis is caapable of handling shadowing of identifiers, which
/// *shouldn't* happen, but if it does and we wanted to kick and scream,
/// this is one place we could do that. Instead, we'll just come up with
/// guaranteed unique names for each let binding.
#[derive(Debug)]
pub struct UpdateLet;

impl super::Transform for UpdateLet {
    fn transform(
        &self,
        relation: &mut RelationExpr,
        _: &HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
        _: &EvalEnv,
    ) {
        self.transform(relation)
    }
}

impl UpdateLet {
    pub fn transform(&self, relation: &mut RelationExpr) {
        let mut id_gen: IdGen = Default::default();
        self.action(relation, &mut HashMap::new(), &mut id_gen);
    }

    pub fn action(
        &self,
        relation: &mut RelationExpr,
        remap: &mut HashMap<LocalId, (LocalId, RelationType)>,
        id_gen: &mut IdGen,
    ) {
        match relation {
            RelationExpr::Let { id, value, body } => {
                self.action(value, remap, id_gen);
                // If a local id, assign a new identifier and refresh the type.
                let new_id = LocalId::new(id_gen.allocate_id());
                let prev = remap.insert(id.clone(), (new_id, value.typ()));
                self.action(body, remap, id_gen);
                remap.remove(id);
                if let Some(prev_stuff) = prev {
                    remap.insert(id.clone(), prev_stuff);
                }
                *id = new_id;
            }
            RelationExpr::Get { id, typ } => {
                if let Id::Local(local_id) = id {
                    if let Some((new_id, new_type)) = remap.get(local_id) {
                        *local_id = new_id.clone();
                        *typ = new_type.clone()
                    }
                }
            }
            _ => {
                relation.visit1_mut(&mut |e| self.action(e, remap, id_gen));
            }
        }
    }
}
