// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Push down MFP into various operators. TODO.

use std::collections::{HashMap, HashSet};

use expr::{MapFilterProject, RelationExpr, ScalarExpr};

use crate::TransformArgs;

/// Push down MFP TODO
#[derive(Debug)]
pub struct MFPPushdown;

impl crate::Transform for MFPPushdown {
    fn transform(
        &self,
        relation: &mut RelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        self.action(
            relation,
            MapFilterProject::new(relation.typ().column_types.len()),
        );
        Ok(())
    }
}

impl MFPPushdown {
    /// ...
    pub fn action(&self, relation: &mut RelationExpr, mfp: MapFilterProject) {
        match relation {
            RelationExpr::Map { input, scalars } => {
                let m = MapFilterProject::new(input.arity()).map(scalars.clone());
                self.action(input, m.apply(&mfp));
                *relation = input.take_dangerous();
            }
            RelationExpr::Filter { input, predicates } => {
                let f = MapFilterProject::new(input.arity()).filter(predicates.clone());
                self.action(input, f.apply(&mfp));
                *relation = input.take_dangerous();
            }
            RelationExpr::Project { input, outputs } => {
                let p = MapFilterProject::new(input.arity()).project(outputs.clone());
                self.action(input, p.apply(&mfp));
                *relation = input.take_dangerous();
            }
            RelationExpr::Union { base, inputs } => {
                for inp in inputs {
                    self.action(inp, mfp.clone());
                }
                self.action(base, mfp);
            }
            RelationExpr::FlatMap {
                input,
                func,
                exprs,
                demand: _,
            } => {
                // TODO(justin): we might want to absorb FlatMap into MFP, since it's also linear.
                let input_arity = input.arity();
                let added_cols = func.output_type().arity();
                let flatmap_arity = input_arity + added_cols;

                let (m, f, mut p) = mfp.as_map_filter_project();

                // Columns have several different names at different points in this process.
                // The final structure will look like this:
                //
                //       Input
                // 1.      v
                //    Pushed Map
                // 2.      v
                //   Pushed Filter
                //         v
                //   Pushed Project
                // 3.      v
                //      FlatMap
                // 4.      v
                //   Residual Map
                // 5.      v
                //  Residual Filter
                //         v
                //  Residual Project

                // First, figure out which Map expressions can be pushed down.

                // pushed_exprs contains all the map expressions we were able to move beneath the
                // FlatMap.
                let mut pushed_exprs = vec![];
                // residual_exprs contains all the map expressions we were not able to move beneath
                // the FlatMap.
                let mut residual_exprs = vec![];
                // pushdown_map maps the names of columns in the original expression to where to
                // find them beneath the FlatMap. This is used both for pushed down map expressions
                // and pushed down filter expressions.
                let mut pushdown_map = HashMap::new();
                // col_map maps the original names of the columns to where to find them at the
                // current stage of the stack above. First, immediately after Input, all the input
                // columns are the same place they were originally.
                let mut col_map = (0..input.arity())
                    .map(|c| (c, c))
                    .collect::<HashMap<_, _>>();
                // col_map is currently at stage 1 in the diagram above.

                for (idx, map_expr) in m.iter().enumerate() {
                    // We can push down any expressions bound by input, as well as the expressions
                    // we have already pushed down.
                    if map_expr.support().iter().all(|c| col_map.contains_key(c)) {
                        pushdown_map.insert(flatmap_arity + idx, input_arity + pushed_exprs.len());
                        // Update col_map to reflect the new position of this column.
                        col_map.insert(flatmap_arity + idx, input_arity + pushed_exprs.len());

                        // This expression might refer to previous map expressions, so we have to
                        // make sure we give those references their new names.
                        let mut remapped_expr = map_expr.clone();
                        remapped_expr.visit_mut(&mut |e| {
                            if let ScalarExpr::Column(c) = e {
                                *c = *col_map.get(c).unwrap();
                            }
                        });

                        pushed_exprs.push(remapped_expr);
                    } else {
                        // Some of these names might be wrong now, if they referenced other map
                        // expressions, but those will still undergo more transformation (the
                        // pushed projection and FlatMap) so we will wait to remap them until
                        // later.
                        residual_exprs.push(map_expr.clone());
                    }
                }

                // col_map is now at stage 2 in the diagram.

                // Now we have to figure out which filters we can push down.
                let (mut bound, mut unbound): (Vec<_>, Vec<_>) = f
                    .into_iter()
                    .partition(|expr| expr.support().iter().all(|c| col_map.contains_key(c)));

                // The filters that get pushed down need to use the pushed-down names for the map
                // expressions (but the ones from the input stay the same).
                for expr in bound.iter_mut() {
                    expr.visit_mut(&mut |e| {
                        if let ScalarExpr::Column(c) = e {
                            *c = *col_map.get(c).unwrap();
                        }
                    });
                }

                // Finally, we can push down a projection which strips away columns that are not
                // required by any of the M/F/P or the FlatMap. This will end up doing another
                // renaming of all the columns.
                let mut demanded_cols = HashSet::new();
                for c in p.iter() {
                    demanded_cols.insert(*c);
                }
                for e in unbound.iter() {
                    demanded_cols.extend(e.support());
                }
                for e in residual_exprs.iter() {
                    demanded_cols.extend(e.support());
                }
                for e in exprs.iter() {
                    demanded_cols.extend(e.support());
                }

                // We don't care about columns added by residual_exprs, nor do we care about ones
                // the FlatMap introduces.
                demanded_cols.retain(|c| col_map.contains_key(c));

                // Change the demanded cols to their new names.
                let demanded_cols = demanded_cols
                    .iter()
                    .map(|c| col_map.get(c).unwrap())
                    .collect::<HashSet<_>>();

                // We're going to prune away a bunch of columns. then we have to remap everything
                // according to that.
                let pushed_projection: Vec<_> = (0..(input_arity + pushed_exprs.len()))
                    .filter(|i| demanded_cols.contains(&i))
                    .collect();

                // Now we need a new col map, which maps via pushed_projection.
                // This col_map represents stage 3.
                let mut col_map = col_map
                    .into_iter()
                    .filter_map(|(k, v)| Some((k, pushed_projection.iter().position(|c| *c == v)?)))
                    .collect::<HashMap<_, _>>();

                // Remap all of the FlatMap expressions.
                for expr in exprs.iter_mut() {
                    expr.visit_mut(&mut |e| {
                        if let ScalarExpr::Column(c) = e {
                            *c = *col_map.get(c).unwrap();
                        }
                    });
                }

                // Then add each of the FlatMap added columns to col_map.
                for i in 0..added_cols {
                    col_map.insert(input_arity + i, pushed_projection.len() + i);
                }

                // col_map is now at stage 4.

                // Now get all the map cols.
                let new_flatmap_arity = pushed_projection.len() + added_cols;

                // Now extend col_map to include the residual map expressions.
                col_map.extend(
                    (flatmap_arity..(flatmap_arity + m.len()))
                        .filter(|c| !col_map.contains_key(&c))
                        .enumerate()
                        .map(|(idx, col)| (col, new_flatmap_arity + idx))
                        .collect::<Vec<_>>(),
                );

                // col_map is now at stage 5.

                // Remap all the residual expressions.
                for expr in residual_exprs.iter_mut() {
                    expr.visit_mut(&mut |e| {
                        if let ScalarExpr::Column(c) = e {
                            *c = *col_map.get(c).unwrap();
                        }
                    });
                }

                // Remap all the residual filters.
                for expr in unbound.iter_mut() {
                    expr.visit_mut(&mut |e| {
                        if let ScalarExpr::Column(c) = e {
                            *c = *col_map.get(c).unwrap();
                        }
                    });
                }

                // Finally, remap the projection (essentially putting another projection before
                // it).
                for c in p.iter_mut() {
                    *c = *col_map.get(c).unwrap();
                }

                self.action(
                    input,
                    MapFilterProject::new(input_arity)
                        .map(pushed_exprs)
                        .filter(bound)
                        .project(pushed_projection),
                );

                *relation = input
                    .take_dangerous()
                    .flat_map(func.clone(), exprs.iter().cloned().collect())
                    .map(residual_exprs)
                    .filter(unbound)
                    .project(p);
            }
            _ => {
                let (m, f, p) = mfp.as_map_filter_project();
                *relation = relation.take_dangerous().map(m).filter(f).project(p);
            }
        };
    }
}
