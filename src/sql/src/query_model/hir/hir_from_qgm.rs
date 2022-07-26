// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Generates a [`HirRelationExpr`] from a [`Model`].
//!
//! The public interface consists of the [`From<Model>`]
//! implementation for [`HirRelationExpr`].

use std::collections::{HashMap, HashSet};

use crate::plan::expr::{ColumnRef, JoinKind};
use crate::plan::{HirRelationExpr, HirScalarExpr};
use crate::query_model::attribute::relation_type::RelationType as BoxRelationType;
use crate::query_model::error::*;
use crate::query_model::model::*;
use crate::query_model::{BoxId, Model, QGMError, QuantifierId};

use itertools::Itertools;

use mz_ore::id_gen::IdGen;

impl TryFrom<Model> for HirRelationExpr {
    type Error = QGMError;
    fn try_from(model: Model) -> Result<Self, Self::Error> {
        FromModel::default().generate(model)
    }
}

#[derive(Default)]
struct FromModel {
    /// Generates [`mz_expr::LocalId`] as an alias for common expressions.
    id_gen: IdGen,
    // TODO: Add the current correlation information.
    // Inspired by FromHir::context_stack. Subject to change.
    // context_stack: Vec<QuantifierId>,
    /// Stack of [HirRelationExpr] that have been created through visiting nodes
    /// of QGM in post-order.
    converted: Vec<HirRelationExpr>,
    /// Common expressions that have been given a [`mz_expr::LocalId`], from
    /// oldest to most recently identified.
    lets: Vec<(mz_expr::LocalId, HirRelationExpr, mz_repr::RelationType)>,
    /// Map of ([BoxId]s whose HIR representation has been given a
    /// [`mz_expr::LocalId`]) -> (position of its HIR representation in `lets`)
    common_subgraphs: HashMap<BoxId, usize>,
}

/// Tracks the column ranges in a [HirRelationExpr] in corresponding the
/// quantifiers in a box.
struct ColumnMap {
    quantifier_to_input: HashMap<QuantifierId, usize>,
    input_mapper: mz_expr::JoinInputMapper,
}

impl FromModel {
    fn generate(mut self, mut model: Model) -> Result<HirRelationExpr, QGMError> {
        // Derive the RelationType of each box.
        use crate::query_model::attribute::core::{Attribute, RequiredAttributes};
        let attributes = HashSet::from_iter(std::iter::once(
            Box::new(BoxRelationType) as Box<dyn Attribute>
        ));
        let root = model.top_box;
        RequiredAttributes::from(attributes).derive(&mut model, root);

        // Convert QGM to HIR.
        model.try_visit_pre_post(
            &mut |_, _| -> Result<(), QGMError> {
                // TODO: add to the context stack
                Ok(())
            },
            &mut |model, box_id| -> Result<(), QGMError> { self.convert_subgraph(model, box_id) },
        )?;

        debug_assert!(self.converted.len() == 1);
        // Retrieve the HIR and put `let`s around it.
        let mut result = self.converted.pop().unwrap();
        while let Some((id, value, _)) = self.lets.pop() {
            if !matches!(value, HirRelationExpr::Get { .. }) {
                result = HirRelationExpr::Let {
                    name: "".to_string(),
                    id,
                    value: Box::new(value),
                    body: Box::new(result),
                }
            }
        }
        Ok(result)
    }

    /// Convert subgraph rooted at `box_id` to [HirRelationExpr].
    ///
    /// If the box corresponding to `box_id` has multiple ranging quantifiers,
    /// the [HirRelationExpr] will be in `self.lets`, and it can be looked up in
    /// `self.common_subgraphs`. Otherwise, it will be pushed to `self.converted`.
    fn convert_subgraph(&mut self, model: &Model, box_id: &BoxId) -> Result<(), QGMError> {
        let r#box = model.get_box(*box_id);
        if self.common_subgraphs.get(&box_id).is_some() {
            // Subgraph has already been converted.
            return Ok(());
        }
        let hir = match &r#box.box_type {
            BoxType::Get(Get { id, unique_keys }) => {
                let typ = mz_repr::RelationType::new(
                    r#box.attributes.get::<BoxRelationType>().to_owned(),
                )
                .with_keys(unique_keys.clone());
                HirRelationExpr::Get {
                    id: mz_expr::Id::Global(*id),
                    typ,
                }
            }
            BoxType::Values(values) => HirRelationExpr::constant(
                // Map `Vec<Vec<BoxScalarExpr>` to `Vec<Vec<Datum>>`
                values
                    .rows
                    .iter()
                    .map(|row| {
                        row.iter()
                            .map(|expr| {
                                if let BoxScalarExpr::Literal(datum, _) = expr {
                                    Ok(datum.unpack_first())
                                } else {
                                    // TODO: The validator should make this branch
                                    // unreachable.
                                    Err(QGMError::from(UnsupportedBoxScalarExpr {
                                        context: "HIR conversion".to_string(),
                                        scalar: expr.clone(),
                                        explanation: Some("Only literal expressions are supported in Values boxes.".to_string())
                                    }))
                                }
                            })
                            .try_collect()
                    })
                    .try_collect()?,
                mz_repr::RelationType::new(r#box.attributes.get::<BoxRelationType>().to_owned()),
            ),
            BoxType::Select(select) => {
                let (rels, scalars) = self.convert_quantifiers(&r#box)?;
                let (join_q_ids, mut join_inputs): (Vec<_>, Vec<_>) = rels.into_iter().unzip();
                let (map_q_ids, maps): (Vec<_>, Vec<_>) = scalars.into_iter().unzip();

                let mut result = if let Some(first) = join_inputs.pop() {
                    // TODO (#11375): determine if this situation should be allowable.
                    first
                } else {
                    HirRelationExpr::constant(vec![vec![]], mz_repr::RelationType::new(vec![]))
                };

                while let Some(join_input) = join_inputs.pop() {
                    result = HirRelationExpr::Join {
                        left: Box::new(result),
                        right: Box::new(join_input),
                        kind: JoinKind::Inner,
                        on: HirScalarExpr::literal_true(),
                    }
                }
                if !maps.is_empty() {
                    result = result.map(maps);
                }
                let column_map = ColumnMap::new(
                    Iterator::chain(join_q_ids.into_iter().rev(), map_q_ids.into_iter().rev()),
                    model,
                );
                if !select.predicates.is_empty() {
                    let filter = select
                        .predicates
                        .iter()
                        .map(|pred| self.convert_scalar(pred, &column_map))
                        .filter(|pred| pred != &HirScalarExpr::literal_true())
                        .collect_vec();
                    if !filter.is_empty() {
                        result = result.filter(filter);
                    }
                }
                let mut result = self.convert_projection(&r#box, &column_map, result);
                if r#box.distinct == DistinctOperation::Enforce {
                    result = result.distinct();
                }
                if select.limit.is_some() || select.offset.is_some() || select.order_key.is_some() {
                    // TODO: put a TopK around the result
                    unimplemented!()
                }
                result
            }
            BoxType::OuterJoin(outer_join) => {
                let (mut rels, _) = self.convert_quantifiers(&r#box)?;
                let (_, left) = rels.pop().unwrap();
                let (_, right) = rels.pop().unwrap();
                let mut quantifier_iter = r#box.input_quantifiers();
                let left_preserved = matches!(
                    quantifier_iter.next().unwrap().quantifier_type,
                    QuantifierType::PreservedForeach
                );
                let right_preserved = matches!(
                    quantifier_iter.next().unwrap().quantifier_type,
                    QuantifierType::PreservedForeach
                );
                let kind = if left_preserved {
                    if right_preserved {
                        JoinKind::FullOuter
                    } else {
                        JoinKind::LeftOuter
                    }
                } else if right_preserved {
                    JoinKind::RightOuter
                } else {
                    JoinKind::Inner
                };
                let column_map = ColumnMap::new(r#box.input_quantifiers().map(|q| q.id), model);
                let converted_predicates = outer_join
                    .predicates
                    .iter()
                    .map(|pred| self.convert_scalar(pred, &column_map))
                    .collect_vec();
                let on = HirScalarExpr::variadic_and(converted_predicates);
                let result = HirRelationExpr::Join {
                    left: Box::new(left),
                    right: Box::new(right),
                    kind,
                    on,
                };
                self.convert_projection(&r#box, &column_map, result)
            }
            _ => unimplemented!(),
        };

        if r#box.ranging_quantifiers().count() > 1 {
            let id = mz_expr::LocalId::new(self.id_gen.allocate_id());
            let typ =
                mz_repr::RelationType::new(r#box.attributes.get::<BoxRelationType>().to_owned());
            self.common_subgraphs.insert(r#box.id, self.lets.len());
            self.lets.push((id, hir, typ));
        } else {
            self.converted.push(hir);
        }
        Ok(())
    }

    /// Convert the quantifiers of `box` into [HirRelationExpr] and [HirScalarExpr].
    ///
    /// Results are returned in reverse quantifier order; the last entry in the
    /// second vector corresponds to the first subquery quantifier in the box.
    fn convert_quantifiers<'a>(
        &mut self,
        r#box: &BoundRef<'a, QueryBox>,
    ) -> Result<
        (
            Vec<(QuantifierId, HirRelationExpr)>,
            Vec<(QuantifierId, HirScalarExpr)>,
        ),
        QGMError,
    > {
        let mut rels = Vec::new();
        let mut scalars = Vec::new();
        for q in r#box.input_quantifiers().rev() {
            // Retrive the HIR corresponding to the input box of the quantifier
            let inner_relation = if let Some(let_pos) = self.common_subgraphs.get(&q.input_box) {
                if matches!(self.lets[*let_pos].1, HirRelationExpr::Get { .. }) {
                    self.lets[*let_pos].1.clone()
                } else {
                    HirRelationExpr::Get {
                        id: mz_expr::Id::Local(self.lets[*let_pos].0),
                        typ: self.lets[*let_pos].2.clone(),
                    }
                }
            } else {
                self.converted.pop().unwrap()
            };

            match q.quantifier_type {
                QuantifierType::Foreach | QuantifierType::PreservedForeach => {
                    rels.push((q.id, inner_relation))
                }
                QuantifierType::Existential => {
                    scalars.push((q.id, HirScalarExpr::Exists(Box::new(inner_relation))))
                }
                QuantifierType::Scalar => {
                    scalars.push((q.id, HirScalarExpr::Select(Box::new(inner_relation))))
                }
                q @ QuantifierType::All => {
                    return Err(QGMError::from(UnsupportedQuantifierType {
                        quantifier_type: q.clone(),
                        context: "HIR conversion".to_string(),
                    }));
                }
            }
        }
        Ok((rels, scalars))
    }

    /// Convert the projection of a box into a Map + Project around the
    /// [HirRelationExpr] representing the rest of the box.
    fn convert_projection<'a>(
        &mut self,
        r#box: &BoundRef<'a, QueryBox>,
        column_map: &ColumnMap,
        mut result: HirRelationExpr,
    ) -> HirRelationExpr {
        let mut map = Vec::new();
        let mut project = Vec::new();

        for column in &r#box.columns {
            let hir_scalar = self.convert_scalar(&column.expr, column_map);
            if let HirScalarExpr::Column(ColumnRef { level: 0, column }) = hir_scalar {
                // We can skip making a copy of the column.
                project.push(column);
            } else {
                project.push(column_map.arity() + map.len());
                map.push(hir_scalar);
            }
        }

        if !map.is_empty() {
            result = result.map(map);
        }
        result.project(project)
    }

    /// Convert a [BoxScalarExpr] to [HirScalarExpr].
    fn convert_scalar(&mut self, expr: &BoxScalarExpr, column_map: &ColumnMap) -> HirScalarExpr {
        let mut results = Vec::new();
        expr.visit_post(&mut |e| match e {
            BoxScalarExpr::BaseColumn(BaseColumn { position, .. }) => {
                results.push(HirScalarExpr::Column(ColumnRef {
                    level: 0,
                    column: *position,
                }));
            }
            BoxScalarExpr::ColumnReference(ColumnReference {
                quantifier_id,
                position,
            }) => {
                let converted = if let Some(column) =
                    column_map.lookup_level_0_col(*quantifier_id, *position)
                {
                    HirScalarExpr::Column(ColumnRef { level: 0, column })
                } else {
                    // TODO: calculate the level for a correlated subquery.
                    unimplemented!()
                };
                results.push(converted);
            }
            BoxScalarExpr::Literal(row, typ) => {
                results.push(HirScalarExpr::Literal(row.clone(), typ.clone()))
            }
            BoxScalarExpr::CallUnmaterializable(func) => {
                results.push(HirScalarExpr::CallUnmaterializable(func.clone()))
            }
            BoxScalarExpr::CallUnary { func, .. } => {
                let expr = Box::new(results.pop().unwrap());
                results.push(HirScalarExpr::CallUnary {
                    func: func.clone(),
                    expr,
                })
            }
            BoxScalarExpr::CallBinary { func, .. } => {
                let expr2 = Box::new(results.pop().unwrap());
                let expr1 = Box::new(results.pop().unwrap());
                results.push(HirScalarExpr::CallBinary {
                    func: func.clone(),
                    expr1,
                    expr2,
                })
            }
            BoxScalarExpr::CallVariadic { func, exprs } => {
                let exprs = results.drain((results.len() - exprs.len())..).collect_vec();
                results.push(HirScalarExpr::CallVariadic {
                    func: func.clone(),
                    exprs,
                })
            }
            BoxScalarExpr::If { .. } => {
                let els = Box::new(results.pop().unwrap());
                let then = Box::new(results.pop().unwrap());
                let cond = Box::new(results.pop().unwrap());
                results.push(HirScalarExpr::If { els, then, cond });
            }
            _ => unimplemented!(),
        });
        debug_assert!(results.len() == 1);
        results.pop().unwrap()
    }
}

impl ColumnMap {
    fn new<I>(quantifiers: I, model: &Model) -> Self
    where
        I: Iterator<Item = QuantifierId>,
    {
        let (quantifier_to_input, arities): (HashMap<_, _>, Vec<_>) = quantifiers
            .enumerate()
            .map(|(index, q_id)| ((q_id, index), model.get_quantifier(q_id).output_arity()))
            .unzip();
        let input_mapper = mz_expr::JoinInputMapper::new_from_input_arities(arities.into_iter());
        Self {
            quantifier_to_input,
            input_mapper,
        }
    }

    fn lookup_level_0_col(&self, q_id: QuantifierId, position: usize) -> Option<usize> {
        if let Some(input) = self.quantifier_to_input.get(&q_id) {
            Some(self.input_mapper.map_column_to_global(position, *input))
        } else {
            None
        }
    }

    #[inline(always)]
    fn arity(&self) -> usize {
        self.input_mapper.total_columns()
    }
}
