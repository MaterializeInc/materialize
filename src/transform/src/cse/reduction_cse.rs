// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Finds reductions with the same input and grouping key and creates a shared reduction
//! behind a `Let` containing all aggregations from the merged reductions.
//!
//! This transforms puts all reductions behind a `Let` so it relies on `InlineLet` to
//! remove those that are not necessary, and on `UpdateLet` to update the type
//! information of the `Get` operators installed.

use std::collections::HashMap;

use expr::{AggregateExpr, Id, IdGen, LocalId, MirRelationExpr, MirScalarExpr};
use repr::RelationType;

use crate::TransformArgs;

/// Fuses reductions with the same input and grouping key merging their aggregations.
#[derive(Debug)]
pub struct ReductionCSE;

impl crate::Transform for ReductionCSE {
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        args: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        *args.id_gen = IdGen::default(); // Get a fresh IdGen.
        let mut bindings = Bindings::default();
        bindings.action(relation, args.id_gen);
        bindings.leave_scope(relation, None);
        Ok(())
    }
}

/// Mantains the reductions to be installed.
#[derive(Debug, Default)]
pub struct Bindings {
    /// Reductions that will be installed behind a `Let` with the Id stored.
    bindings: HashMap<(MirRelationExpr, Vec<MirScalarExpr>), (Vec<AggregateExpr>, u64)>,
    /// Mapping from conventional local `Get` identifiers to new ones.
    rebindings: HashMap<LocalId, LocalId>,
}

impl Bindings {
    fn action(&mut self, relation: &mut MirRelationExpr, id_gen: &mut IdGen) {
        match relation {
            MirRelationExpr::Let { id, value, body } => {
                self.action(value, id_gen);
                let new_id = LocalId::new(id_gen.allocate_id());
                self.rebindings.insert(*id, new_id);
                self.action(body, id_gen);
                self.rebindings.remove(id);
                // Any reduction over a relation referencing `new_id` must be installed
                // before leave `new_id`'s scope.
                self.leave_scope(body, Some(new_id));
                *id = new_id;
            }
            MirRelationExpr::Get { id, .. } => {
                if let Id::Local(id) = id {
                    *id = self.rebindings[id];
                }
            }

            _ => {
                // All other expressions just need to apply the logic recursively.
                relation.visit1_mut(&mut |expr| {
                    self.action(expr, id_gen);
                })
            }
        };

        if let MirRelationExpr::Reduce {
            input,
            group_key,
            aggregates,
            ..
        } = relation
        {
            let key = ((**input).clone(), group_key.clone());
            if let Some((existing_aggs, id)) = self.bindings.get_mut(&key) {
                let typ = Self::build_typ(
                    input,
                    group_key,
                    existing_aggs.iter().chain(aggregates.iter()),
                );
                let get = MirRelationExpr::Get {
                    id: Id::Local(LocalId::new(*id)),
                    typ,
                };
                let first_new_agg = group_key.len() + existing_aggs.len();
                let new_agg_count = aggregates.len();
                // Merge the new aggregates into the existing reduction.
                existing_aggs.extend(aggregates.drain(..));
                // The projection of the previous `Reduce` must be preserved.
                *relation = get.project(
                    (0..group_key.len())
                        .chain(first_new_agg..first_new_agg + new_agg_count)
                        .collect(),
                );
            } else {
                let typ = Self::build_typ(input, group_key, aggregates.iter());
                let arity = typ.arity();
                let id = id_gen.allocate_id();
                let get = MirRelationExpr::Get {
                    id: Id::Local(LocalId::new(id)),
                    typ,
                };
                self.bindings.insert(key, ((*aggregates).to_owned(), id));
                // The projection of the previous `Reduce` must be preserved. Note that
                // new aggregates may be added to the reduction created here, so we
                // must install a projection to ensure the arity seen by the upstream
                // operator remains unchanged.
                *relation = get.project((0..arity).collect());
            }
        }
    }

    // TODO(asenac) this method can be removed since `UpdateLet` is required
    // afterwards anyway.
    fn build_typ<'a, I>(
        input: &MirRelationExpr,
        group_key: &Vec<MirScalarExpr>,
        aggregates: I,
    ) -> RelationType
    where
        I: Iterator<Item = &'a AggregateExpr>,
    {
        let input_typ = input.typ();
        let mut column_types = group_key
            .iter()
            .map(|e| e.typ(&input_typ))
            .collect::<Vec<_>>();
        for agg in aggregates {
            column_types.push(agg.typ(&input_typ));
        }
        RelationType::new(column_types)
    }

    /// Installs accumulated relations in a `Let` on top of the given relation
    /// that reference the given `scope`, or all of them if None is passed.
    fn leave_scope(&mut self, expression: &mut MirRelationExpr, scope: Option<LocalId>) {
        let mut bindings = self.bindings.drain().collect::<Vec<_>>();
        bindings.sort_by_key(|(_, (_, i))| *i);

        let mut keep = HashMap::new();
        for ((input, group_key), (aggregates, id)) in bindings.into_iter().rev() {
            if let Some(scope) = scope {
                if let Ok(_) = input.try_visit(&mut |e| {
                    if let MirRelationExpr::Get {
                        id: Id::Local(id), ..
                    } = e
                    {
                        if *id == scope {
                            return Err(());
                        }
                    }
                    Ok(())
                }) {
                    // This reduction doesn't reference the given scope, so it
                    // can be installed later.
                    keep.insert((input, group_key), (aggregates, id));
                    continue;
                }
            }
            // @todo keep expected group size
            let reduce = MirRelationExpr::Reduce {
                input: Box::new(input),
                group_key,
                aggregates,
                monotonic: false,
                expected_group_size: None,
            };
            let new_expression = MirRelationExpr::Let {
                id: LocalId::new(id),
                value: Box::new(reduce),
                body: Box::new(expression.take_dangerous()),
            };
            *expression = new_expression;
        }

        self.bindings = keep;
    }
}
