// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Remove redundant collections of distinct elements from joins.
//!
//! This analysis looks for joins in which one collection contains distinct
//! elements, and it can be determined that the join would only restrict the
//! results, and that the restriction is redundant (the other results would
//! not be reduced by the join).
//!
//! This type of optimization shows up often in subqueries, where distinct
//! collections are used in decorrelation, and afterwards often distinct
//! collections are then joined against the results.

// If statements seem a bit clearer in this case. Specialized methods
// that replace simple and common alternatives frustrate developers.
#![allow(clippy::comparison_chain, clippy::filter_next)]

use std::collections::HashMap;

use itertools::Itertools;
use mz_expr::{Id, JoinInputMapper, MirRelationExpr, MirScalarExpr, RECURSION_LIMIT};
use mz_ore::stack::{CheckedRecursion, RecursionGuard};

use crate::TransformArgs;

/// Remove redundant collections of distinct elements from joins.
#[derive(Debug)]
pub struct RedundantJoin {
    recursion_guard: RecursionGuard,
}

impl Default for RedundantJoin {
    fn default() -> RedundantJoin {
        RedundantJoin {
            recursion_guard: RecursionGuard::with_limit(RECURSION_LIMIT),
        }
    }
}

impl CheckedRecursion for RedundantJoin {
    fn recursion_guard(&self) -> &RecursionGuard {
        &self.recursion_guard
    }
}

impl crate::Transform for RedundantJoin {
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        self.action(relation, &mut HashMap::new()).map(|_| ())
    }
}

impl RedundantJoin {
    /// Remove redundant collections of distinct elements from joins.
    ///
    /// This method tracks "provenance" information for each collections,
    /// those being column-wise relationships to identified collections
    /// (either imported collections, or let-bound collections). These
    /// relationships state that when projected on to these columns, the
    /// records of the one collection are contained in the records of the
    /// identified collection.
    ///
    /// This provenance information is then used for the `MirRelationExpr::Join`
    /// variant to remove "redundant" joins, those that can be determined to
    /// neither restrict nor augment one of the input relations. Consult the
    /// `find_redundancy` method and its documentation for more detail.
    pub fn action(
        &self,
        relation: &mut MirRelationExpr,
        lets: &mut HashMap<Id, Vec<ProvInfo>>,
    ) -> Result<Vec<ProvInfo>, crate::TransformError> {
        self.checked_recur(|_| {
            match relation {
                MirRelationExpr::Let { id, value, body } => {
                    // Recursively determine provenance of the value.
                    let value_prov = self.action(value, lets)?;
                    let old = lets.insert(Id::Local(*id), value_prov);
                    let result = self.action(body, lets)?;
                    if let Some(old) = old {
                        lets.insert(Id::Local(*id), old);
                    } else {
                        lets.remove(&Id::Local(*id));
                    }
                    Ok(result)
                }
                MirRelationExpr::Get { id, typ } => {
                    // Extract the value provenance, or an empty list if unavailable.
                    let mut val_info = lets.get(id).cloned().unwrap_or_default();
                    // Add information about being exactly this let binding too.
                    val_info.push(ProvInfo::make_leaf(*id, typ.arity()));
                    Ok(val_info)
                }

                MirRelationExpr::Join {
                    inputs,
                    equivalences,
                    implementation,
                } => {
                    // This logic first applies what it has learned about its input provenance,
                    // and if it finds a redundant join input it removes it. In that case, it
                    // also fails to produce exciting provenance information, partly out of
                    // laziness and the challenge of ensuring it is correct. Instead, if it is
                    // unable to find a rendundant join it produces meaningful provenance information.

                    // Recursively apply transformation, and determine the provenance of inputs.
                    let mut input_prov = Vec::new();
                    for i in inputs.iter_mut() {
                        input_prov.push(self.action(i, lets)?);
                    }

                    // Determine useful information about the structure of the inputs.
                    let mut input_types = inputs.iter().map(|i| i.typ()).collect::<Vec<_>>();
                    let old_input_mapper = JoinInputMapper::new_from_input_types(&input_types);

                    // If we find an input that can be removed, we should do so!
                    // We only do this once per invocation to keep our sanity, but we could
                    // rewrite it to iterate. We can avoid looking for any relation that
                    // does not have keys, as it cannot be redundant in that case.
                    if let Some((remove_input_idx, mut bindings)) = (0..input_types.len())
                        .rev()
                        .filter(|i| !input_types[*i].keys.is_empty())
                        .flat_map(|i| {
                            find_redundancy(
                                i,
                                &input_types[i].keys,
                                &old_input_mapper,
                                equivalences,
                                &input_prov[..],
                            )
                            .map(|b| (i, b))
                        })
                        .next()
                    {
                        inputs.remove(remove_input_idx);
                        input_types.remove(remove_input_idx);

                        // Update the column offsets in the binding expressions to catch
                        // up with the removal of `remove_input_idx`.
                        for expr in bindings.iter_mut() {
                            expr.visit_mut_post(&mut |e| {
                                if let MirScalarExpr::Column(c) = e {
                                    let (_local_col, input_relation) =
                                        old_input_mapper.map_column_to_local(*c);
                                    if input_relation > remove_input_idx {
                                        *c -= old_input_mapper.input_arity(remove_input_idx);
                                    }
                                }
                            });
                        }

                        // Replace column references from `remove_input_idx` with the corresponding
                        // binding expression. Update the offsets of the column references
                        // from inputs after `remove_input_idx`.
                        for equivalence in equivalences.iter_mut() {
                            for expr in equivalence.iter_mut() {
                                expr.visit_mut_post(&mut |e| {
                                    if let MirScalarExpr::Column(c) = e {
                                        let (local_col, input_relation) =
                                            old_input_mapper.map_column_to_local(*c);
                                        if input_relation == remove_input_idx {
                                            *e = bindings[local_col].clone();
                                        } else if input_relation > remove_input_idx {
                                            *c -= old_input_mapper.input_arity(remove_input_idx);
                                        }
                                    }
                                });
                            }
                        }

                        mz_expr::canonicalize::canonicalize_equivalences(
                            equivalences,
                            &input_types,
                        );

                        // Build a projection that leaves the binding expressions in the same
                        // position as the columns of the removed join input they are replacing.
                        let new_input_mapper = JoinInputMapper::new_from_input_types(&input_types);
                        let mut projection = Vec::new();
                        let new_join_arity = new_input_mapper.total_columns();
                        for i in 0..old_input_mapper.total_inputs() {
                            if i != remove_input_idx {
                                projection.extend(
                                    new_input_mapper.global_columns(if i < remove_input_idx {
                                        i
                                    } else {
                                        i - 1
                                    }),
                                );
                            } else {
                                projection.extend(new_join_arity..new_join_arity + bindings.len());
                            }
                        }

                        // Unset implementation, as irrevocably hosed by this transformation.
                        *implementation = mz_expr::JoinImplementation::Unimplemented;

                        *relation = relation.take_dangerous().map(bindings).project(projection);
                        // The projection will gum up provenance reasoning anyhow, so don't work hard.
                        // We will return to this expression again with the same analysis.
                        Ok(Vec::new())
                    } else {
                        // Provenance information should be the union of input provenance information,
                        // with columns updated. Because rows may be dropped in the join, all `exact`
                        // bits should be un-set.
                        let mut results = Vec::new();
                        for (input, input_prov) in input_prov.into_iter().enumerate() {
                            for mut prov in input_prov {
                                prov.exact = false;
                                let mut projection = vec![None; old_input_mapper.total_columns()];
                                for (local_col, global_col) in
                                    old_input_mapper.global_columns(input).enumerate()
                                {
                                    projection[global_col] =
                                        prov.dereferenced_projection[local_col].clone();
                                }
                                prov.dereferenced_projection = projection;
                                results.push(prov);
                            }
                        }
                        Ok(results)
                    }
                }

                MirRelationExpr::Filter { input, .. } => {
                    // Filter may drop records, and so we unset `exact`.
                    let mut result = self.action(input, lets)?;
                    for prov in result.iter_mut() {
                        prov.exact = false;
                    }
                    Ok(result)
                }

                MirRelationExpr::Map { input, scalars } => {
                    let mut result = self.action(input, lets)?;
                    for prov in result.iter_mut() {
                        for scalar in scalars.iter() {
                            let dereferenced_scalar = prov.strict_dereference(scalar);
                            prov.dereferenced_projection.push(dereferenced_scalar);
                        }
                    }
                    Ok(result)
                }
                MirRelationExpr::DeclareKeys { input, .. } => self.action(input, lets),

                MirRelationExpr::Union { base, inputs } => {
                    let mut prov = self.action(base, lets)?;
                    for input in inputs {
                        let input_prov = self.action(input, lets)?;
                        // To merge a new list of provenances, we look at the cross
                        // produce of things we might know about each source.
                        // TODO(mcsherry): this can be optimized to use datastructures
                        // keyed by the source identifier.
                        let mut new_prov = Vec::new();
                        for l in prov {
                            new_prov.extend(input_prov.iter().flat_map(|r| l.meet(r)))
                        }
                        prov = new_prov;
                    }

                    Ok(prov)
                }

                MirRelationExpr::Constant { .. } => Ok(Vec::new()),

                MirRelationExpr::Reduce {
                    input,
                    group_key,
                    aggregates,
                    ..
                } => {
                    // Reduce yields its first few columns as a key, and produces
                    // all key tuples that were present in its input.
                    let mut result = self.action(input, lets)?;
                    for prov in result.iter_mut() {
                        let mut projection = group_key
                            .iter()
                            .map(|key| prov.strict_dereference(key))
                            .collect_vec();
                        projection.extend((0..aggregates.len()).map(|_| None));
                        prov.dereferenced_projection = projection;
                    }
                    // TODO: For min, max aggregates, we could preserve provenance
                    // if the expression references a column. We would need to un-set
                    // the `exact` bit in that case, and so we would want to keep both
                    // sets of provenance information.
                    Ok(result)
                }

                MirRelationExpr::Threshold { input } => {
                    // Threshold may drop records, and so we unset `exact`.
                    let mut result = self.action(input, lets)?;
                    for prov in result.iter_mut() {
                        prov.exact = false;
                    }
                    Ok(result)
                }

                MirRelationExpr::TopK { input, .. } => {
                    // TopK may drop records, and so we unset `exact`.
                    let mut result = self.action(input, lets)?;
                    for prov in result.iter_mut() {
                        prov.exact = false;
                    }
                    Ok(result)
                }

                MirRelationExpr::Project { input, outputs } => {
                    // Projections re-order, drop, and duplicate columns,
                    // but they neither drop rows nor invent values.
                    let mut result = self.action(input, lets)?;
                    for prov in result.iter_mut() {
                        let projection = outputs
                            .iter()
                            .map(|c| prov.dereference(&MirScalarExpr::Column(*c)))
                            .collect_vec();
                        prov.dereferenced_projection = projection;
                    }
                    Ok(result)
                }

                MirRelationExpr::FlatMap { input, func, .. } => {
                    // FlatMap may drop records, and so we unset `exact`.
                    let mut result = self.action(input, lets)?;
                    for prov in result.iter_mut() {
                        prov.exact = false;
                        prov.dereferenced_projection
                            .extend((0..func.output_type().column_types.len()).map(|_| None));
                    }
                    Ok(result)
                }

                MirRelationExpr::Negate { input } => {
                    // Negate does not guarantee that the multiplicity of
                    // each source record it at least one. This could have
                    // been a problem in `Union`, where we might report
                    // that the union of positive and negative records is
                    // "exact": cancellations would make this false.
                    let mut result = self.action(input, lets)?;
                    for prov in result.iter_mut() {
                        prov.exact = false;
                    }
                    Ok(result)
                }

                MirRelationExpr::ArrangeBy { input, .. } => self.action(input, lets),
            }
        })
    }
}

/// A relationship between a collections columns and some source columns.
///
/// An instance of this type indicates that some of the bearer's columns
/// derive from `id`. In particular, the non-`None` elements in
/// `dereferenced_projection` correspond to columns that can be derived
/// from `id`'s projection.
///
/// The guarantee is that projected on to these columns, the distinct values
/// of the bearer are contained in the set of distinct values of projected
/// columns of `id`. In the case that `exact` is set, the two sets are equal.
#[derive(Clone, Debug, Ord, Eq, PartialOrd, PartialEq)]
pub struct ProvInfo {
    /// The Id (local or global) of the source.
    id: Id,
    /// The projection of the bearer written in terms of the columns projected
    /// by the underlying Get operator. Set to `None` for columns that cannot
    /// be expressed as scalar expression referencing only columns of the
    /// underlying Get operator.
    dereferenced_projection: Vec<Option<MirScalarExpr>>,
    /// If true, all distinct projected source rows are present in the rows of
    /// the projection of the current collection. This constraint is lost as soon
    /// as a transformation may drop records.
    exact: bool,
}

impl ProvInfo {
    fn make_leaf(id: Id, arity: usize) -> Self {
        Self {
            id,
            dereferenced_projection: (0..arity)
                .map(|c| Some(MirScalarExpr::column(c)))
                .collect::<Vec<_>>(),
            exact: true,
        }
    }

    /// Rewrite `expr` so it refers to the columns of the original source instead
    /// of the columns of the projected source.
    fn dereference(&self, expr: &MirScalarExpr) -> Option<MirScalarExpr> {
        match expr {
            MirScalarExpr::Column(c) => {
                if let Some(expr) = &self.dereferenced_projection[*c] {
                    Some(expr.clone())
                } else {
                    None
                }
            }
            MirScalarExpr::CallUnary { func, expr } => self.dereference(expr).and_then(|expr| {
                Some(MirScalarExpr::CallUnary {
                    func: func.clone(),
                    expr: Box::new(expr),
                })
            }),
            MirScalarExpr::CallBinary { func, expr1, expr2 } => {
                self.dereference(expr1).and_then(|expr1| {
                    self.dereference(expr2).and_then(|expr2| {
                        Some(MirScalarExpr::CallBinary {
                            func: func.clone(),
                            expr1: Box::new(expr1),
                            expr2: Box::new(expr2),
                        })
                    })
                })
            }
            MirScalarExpr::CallVariadic { func, exprs } => {
                let new_exprs = exprs.iter().flat_map(|e| self.dereference(e)).collect_vec();
                if new_exprs.len() == exprs.len() {
                    Some(MirScalarExpr::CallVariadic {
                        func: func.clone(),
                        exprs: new_exprs,
                    })
                } else {
                    None
                }
            }
            MirScalarExpr::Literal(..) | MirScalarExpr::CallUnmaterializable(..) => {
                Some(expr.clone())
            }
            MirScalarExpr::If { cond, then, els } => self.dereference(cond).and_then(|cond| {
                self.dereference(then).and_then(|then| {
                    self.dereference(els).and_then(|els| {
                        Some(MirScalarExpr::If {
                            cond: Box::new(cond),
                            then: Box::new(then),
                            els: Box::new(els),
                        })
                    })
                })
            }),
        }
    }

    /// Like `dereference` but only returns expressions that actually depend on
    /// the original source.
    fn strict_dereference(&self, expr: &MirScalarExpr) -> Option<MirScalarExpr> {
        let derefed = self.dereference(expr);
        match derefed {
            Some(ref expr) if !expr.support().is_empty() => derefed,
            _ => None,
        }
    }

    /// Merge two constraints to find a constraint that satisfies both inputs.
    ///
    /// This method returns nothing if no columns are in common (either because
    /// difference sources are identified, or just no columns in common) and it
    /// intersects bindings and the `exact` bit.
    fn meet(&self, other: &Self) -> Option<Self> {
        if self.id == other.id {
            let resulting_projection = self
                .dereferenced_projection
                .iter()
                .zip(other.dereferenced_projection.iter())
                .map(|(e1, e2)| if e1 == e2 { e1.clone() } else { None })
                .collect_vec();
            if resulting_projection.iter().any(|e| e.is_some()) {
                Some(ProvInfo {
                    id: self.id,
                    dereferenced_projection: resulting_projection,
                    exact: self.exact && other.exact,
                })
            } else {
                None
            }
        } else {
            None
        }
    }
}

/// Attempts to find column bindings that make `input` redundant.
///
/// This method attempts to determine that `input` may be redundant by searching
/// the join structure for another relation `other` with provenance that contains some
/// provenance of `input`, and keys for `input` that are equated by the join to the
/// corresponding columns of `other` under their provenance. The `input` provenance
/// must also have its `exact` bit set.
///
/// In these circumstances, the claim is that because the key columns are equated and
/// determine non-key columns, any matches between `input` and
/// `other` will neither introduce new information to `other`, nor restrict the rows
/// of `other`, nor alter their multplicity.
fn find_redundancy(
    input: usize,
    keys: &[Vec<usize>],
    input_mapper: &JoinInputMapper,
    equivalences: &[Vec<MirScalarExpr>],
    input_prov: &[Vec<ProvInfo>],
) -> Option<Vec<MirScalarExpr>> {
    // Whether the `equivalence` contains an expression that only references
    // `input` that leads to the same as `root_expr` once dereferenced.
    let contains_equivalent_expr_from_input = |equivalence: &[MirScalarExpr],
                                               root_expr: &MirScalarExpr,
                                               input: usize,
                                               provenance: &ProvInfo|
     -> bool {
        equivalence.iter().any(|expr| {
            Some(input) == input_mapper.single_input(expr)
                && provenance
                    .dereference(&input_mapper.map_expr_to_local(expr.clone()))
                    .as_ref()
                    == Some(root_expr)
        })
    };
    for provenance in input_prov[input].iter() {
        // We can only elide if the input contains all records, and binds all columns.
        if provenance.exact
            && provenance
                .dereferenced_projection
                .iter()
                .all(|e| e.is_some())
        {
            // examine all *other* inputs that have not been removed...
            for other in (0..input_mapper.total_inputs()).filter(|other| other != &input) {
                for other_prov in input_prov[other].iter().filter(|p| p.id == provenance.id) {
                    let all_columns_equated = |cols: &Vec<usize>| {
                        cols.iter().all(|input_col| {
                            // The root expression behind the key column, ie. the expression
                            // re-written in terms of elements in the projection of the Get
                            // operator.
                            let root_expr =
                                provenance.dereference(&MirScalarExpr::column(*input_col));
                            root_expr.as_ref().map_or(false, |root_expr| {
                                // Check if there is a join equivalence that joins 'input' and
                                // 'other' on expressions that lead to the same root expression
                                // as the key column.
                                equivalences.iter().any(|equivalence| {
                                    contains_equivalent_expr_from_input(
                                        equivalence,
                                        root_expr,
                                        input,
                                        provenance,
                                    ) && contains_equivalent_expr_from_input(
                                        equivalence,
                                        root_expr,
                                        other,
                                        other_prov,
                                    )
                                })
                            })
                        })
                    };

                    if keys.iter().any(|key| all_columns_equated(key)) {
                        // Find out whether we can produce input's projection strictly with
                        // elements in other's projection.
                        let expressions = provenance
                            .dereferenced_projection
                            .iter()
                            .enumerate()
                            .flat_map(|(c, _)| {
                                // Check if the expression under input's 'c' column can be built
                                // with elements in other's projection.
                                provenance.dereferenced_projection[c].as_ref().map_or(
                                    None,
                                    |root_expr| {
                                        try_build_expression_using_other(
                                            root_expr,
                                            other,
                                            other_prov,
                                            input_mapper,
                                        )
                                    },
                                )
                            })
                            .collect_vec();
                        if expressions.len() == provenance.dereferenced_projection.len() {
                            return Some(expressions);
                        }
                    }
                }
            }
        }
    }

    None
}

/// Tries to build `root_expr` using elements from other's projection.
fn try_build_expression_using_other(
    root_expr: &MirScalarExpr,
    other: usize,
    other_prov: &ProvInfo,
    input_mapper: &JoinInputMapper,
) -> Option<MirScalarExpr> {
    if root_expr.is_literal() {
        return Some(root_expr.clone());
    }

    // Check if 'other' projects a column that lead to `root_expr`.
    for (other_col, derefed) in other_prov.dereferenced_projection.iter().enumerate() {
        if let Some(derefed) = derefed {
            if derefed == root_expr {
                return Some(MirScalarExpr::Column(
                    input_mapper.map_column_to_global(other_col, other),
                ));
            }
        }
    }

    // Otherwise, try to build root_expr's sub-expressions recursively
    // other's projection.
    match root_expr {
        MirScalarExpr::Column(_) => None,
        MirScalarExpr::CallUnary { func, expr } => {
            try_build_expression_using_other(expr, other, other_prov, input_mapper).and_then(
                |expr| {
                    Some(MirScalarExpr::CallUnary {
                        func: func.clone(),
                        expr: Box::new(expr),
                    })
                },
            )
        }
        MirScalarExpr::CallBinary { func, expr1, expr2 } => {
            try_build_expression_using_other(expr1, other, other_prov, input_mapper).and_then(
                |expr1| {
                    try_build_expression_using_other(expr2, other, other_prov, input_mapper)
                        .and_then(|expr2| {
                            Some(MirScalarExpr::CallBinary {
                                func: func.clone(),
                                expr1: Box::new(expr1),
                                expr2: Box::new(expr2),
                            })
                        })
                },
            )
        }
        MirScalarExpr::CallVariadic { func, exprs } => {
            let new_exprs = exprs
                .iter()
                .flat_map(|e| try_build_expression_using_other(e, other, other_prov, input_mapper))
                .collect_vec();
            if new_exprs.len() == exprs.len() {
                Some(MirScalarExpr::CallVariadic {
                    func: func.clone(),
                    exprs: new_exprs,
                })
            } else {
                None
            }
        }
        MirScalarExpr::Literal(..) | MirScalarExpr::CallUnmaterializable(..) => {
            Some(root_expr.clone())
        }
        MirScalarExpr::If { cond, then, els } => {
            try_build_expression_using_other(cond, other, other_prov, input_mapper).and_then(
                |cond| {
                    try_build_expression_using_other(then, other, other_prov, input_mapper)
                        .and_then(|then| {
                            try_build_expression_using_other(els, other, other_prov, input_mapper)
                                .and_then(|els| {
                                    Some(MirScalarExpr::If {
                                        cond: Box::new(cond),
                                        then: Box::new(then),
                                        els: Box::new(els),
                                    })
                                })
                        })
                },
            )
        }
    }
}
