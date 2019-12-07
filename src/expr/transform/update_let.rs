// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::collections::HashMap;
use repr::RelationType;
use crate::{Id, RelationExpr};

#[derive(Debug)]
pub struct UpdateLet;

impl super::Transform for UpdateLet {
    fn transform(&self, relation: &mut RelationExpr) {
        self.transform(relation)
    }
}

impl UpdateLet {
    pub fn transform(&self, relation: &mut RelationExpr) {
        self.action(relation, &mut HashMap::new());
    }

    pub fn action(&self, relation: &mut RelationExpr, types: &mut HashMap<Id,RelationType>) {
        match relation {
            RelationExpr::Let { id, value, body } => {
                let local_id = Id::Local(id.clone());
                self.action(value, types);
                types.insert(local_id.clone(), value.typ());
                self.action(body, types);
                types.remove(&local_id);
            },
            RelationExpr::Get { id, typ } => {
                if let Some(new_type) = types.get(id) {
                    *typ = new_type.clone()
                }
            },
            _ => {
                relation.visit1_mut(&mut |e| self.action(e, types));
            }
        }
    }
}
