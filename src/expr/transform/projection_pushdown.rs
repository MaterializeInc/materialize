// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Push projections down the query tree.

use crate::transform::util::IndexTracker;
use crate::{RelationExpr, ScalarExpr};
use repr::RelationType;

#[derive(Debug)]
pub struct ProjectionPushdown;

impl super::Transform for ProjectionPushdown {
    fn transform(&self, relation: &mut RelationExpr, metadata: &RelationType) {
        self.transform(relation, metadata)
    }
}

impl ProjectionPushdown {
    pub fn transform(&self, relation: &mut RelationExpr, _metadata: &RelationType) {
        relation.visit_mut_pre(&mut |e| {
            self.action(e, &e.typ());
        });
    }

    pub fn action(&self, relation: &mut RelationExpr, _metadata: &RelationType) {
        // In general, we want to push projections as far down the tree as
        // possible, so that we can immediately shed unnecessary data. The big
        // exception is when a projection duplicates columns. In that case, we
        // want to leave the projection as high up the tree as possible. In
        // fact, we should probably have a separate optimization that hoists
        // projections with duplicates.

        if let RelationExpr::Project { input, outputs } = relation {
            match &mut **input {
                // Handled by `FoldConstants`.
                RelationExpr::Constant { .. } => (),

                // Nothing to be done.
                RelationExpr::Get { .. } => (),
                RelationExpr::Let { .. } => (),

                // Handled by `fusion::Project`.
                RelationExpr::Project { .. } => (),

                RelationExpr::Map { input, scalars } => {
                    let input_arity = input.arity();

                    // Compute the smaller projection that reorders and drops
                    // columns only from from the inner relation. We need to
                    // retain any columns from the inner relation that are used
                    // in the map or are preserved by the outer projection. We
                    // might reorder columns, so we keep a mapping from the
                    // original input column index to the post-inner-projection
                    // column index.
                    //
                    // The inner projection outputs columns in the same order as
                    // the outer projection, modulo any duplicate columns and
                    // modulo any columns from the map expression that may be
                    // interleaved. This can sometimes allow us to elide the
                    // outer projection entirely.
                    let mut reordering = Reordering::new();
                    let mut scalar_deps = vec![false; input_arity];
                    for i in &*outputs {
                        if *i < input_arity {
                            reordering.push(*i);
                        } else {
                            scalars[*i - input_arity].0.visit(&mut |e| {
                                if let ScalarExpr::Column(i) = e {
                                    assert!(*i < input_arity);
                                    scalar_deps[*i] = true;
                                }
                            });
                        }
                    }
                    for (i, needed) in scalar_deps.iter().enumerate() {
                        if *needed {
                            reordering.push(i)
                        }
                    }

                    // Rebuild the outer projection on top of the projected
                    // inner relation. We also need to rewrite the map
                    // expressions in terms of the new inner column indices.
                    // Note that we take the opportunity to reorder the map
                    // scalars to match the outer projection order, in the hopes
                    // of eliding the outer projection.
                    let mut new_outputs = Vec::new();
                    let mut new_scalars = Vec::new();
                    for i in &*outputs {
                        if *i < input_arity {
                            new_outputs.push(reordering.map(*i));
                        } else {
                            new_outputs.push(reordering.len() + new_scalars.len());
                            let mut scalar = scalars[*i - input_arity].clone();
                            scalar.0.visit_mut(&mut |e| {
                                if let ScalarExpr::Column(ref mut i) = e {
                                    *i = reordering.map(*i);
                                }
                            });
                            new_scalars.push(scalar);
                        }
                    }

                    *input = Box::new(input.take().project(reordering.into_outputs()));
                    *scalars = new_scalars;
                    *outputs = new_outputs;
                }

                RelationExpr::Filter { .. } => {
                    // TODO.
                }

                RelationExpr::Join { inputs, variables } => {
                    let tracker = IndexTracker::for_inputs(&*inputs);

                    let mut reorderings = vec![Reordering::new(); inputs.len()];
                    for i in &*outputs {
                        let ri = tracker.relation_of(*i);
                        let ci = tracker.local_of(*i);
                        reorderings[ri].push(ci);
                    }
                    for (ri, ci) in variables.iter().flatten() {
                        reorderings[*ri].push(*ci);
                    }

                    let new_tracker = IndexTracker::new(reorderings.iter().map(|r| r.len()));
                    let mut new_outputs = Vec::new();
                    for i in &*outputs {
                        let ri = tracker.relation_of(*i);
                        let ci = tracker.local_of(*i);
                        let ci = reorderings[ri].map(ci);
                        new_outputs.push(new_tracker.global_of(ri, ci));
                    }
                    for (ri, ci) in variables.iter_mut().flatten() {
                        *ci = reorderings[*ri].map(*ci);
                    }

                    for (ri, reordering) in reorderings.into_iter().enumerate() {
                        inputs[ri] = inputs[ri].take().project(reordering.into_outputs());
                    }
                    *outputs = new_outputs;
                }

                RelationExpr::Reduce { .. } => {
                    // TODO.
                }

                RelationExpr::TopK { .. } => {
                    // TODO.
                }

                RelationExpr::OrDefault { .. } => {
                    // TODO.
                }

                RelationExpr::Negate { .. } => {
                    // TODO.
                }

                // Cannot push projection through a distinct.
                RelationExpr::Distinct { .. } => (),

                RelationExpr::Threshold { .. } => {
                    // TODO.
                }

                RelationExpr::Union { .. } => {
                    // TODO.
                }
            }
        }
    }
}

/// Helper to keep track of indices when pushing a projection down.
#[derive(Clone, Debug, Default)]
struct Reordering {
    outputs: Vec<usize>,
    map: Vec<Option<usize>>,
}

impl Reordering {
    /// Constructs a new [`Reordering`].
    fn new() -> Reordering {
        Default::default()
    }

    /// Pushes an index onto the end of the reordering. If index already exists
    /// in the reordering, it is not added.
    fn push(&mut self, i: usize) {
        if !self.outputs.contains(&i) {
            if self.map.len() <= i {
                self.map.resize(i + 1, None);
            }
            self.map[i] = Some(self.outputs.len());
            self.outputs.push(i);
        }
    }

    /// Converts an index from the old order into an index in the new order.
    fn map(&self, i: usize) -> usize {
        self.map[i].unwrap()
    }

    /// The number of indices in the new order.
    fn len(&self) -> usize {
        self.outputs.len()
    }

    /// Consumes `self`, returning the new order.
    fn into_outputs(self) -> Vec<usize> {
        self.outputs
    }
}

#[cfg(test)]
mod tests {
    use crate::transform::Optimizer;
    use crate::{RelationExpr, ScalarExpr};
    use repr::{ColumnType, Datum, RelationType, ScalarType};

    #[test]
    fn test_projection_pushdown() {
        const ZERO: Datum = Datum::Int64(0);
        const INT64: ColumnType = ColumnType {
            name: None,
            nullable: false,
            scalar_type: ScalarType::Int64,
        };
        let base = || {
            RelationExpr::constant(
                vec![vec![ZERO, ZERO, ZERO]],
                RelationType::new(vec![INT64, INT64, INT64]),
            )
        };

        let test_cases =
            vec![
            (
                "projected-away map is entirely elided",
                base()
                    .map(vec![(ScalarExpr::Literal(ZERO), INT64)])
                    .project(vec![1, 2]),
                base().project(vec![1, 2]),
            ),
            (
                "projected-away scalars are elided from map",
                base()
                    .map(vec![
                        (ScalarExpr::Literal(Datum::Int64(3)), INT64),
                        (ScalarExpr::Literal(Datum::Int64(4)), INT64),
                        (ScalarExpr::Literal(Datum::Int64(5)), INT64),
                    ])
                    .project(vec![0, 1, 2, 5, 3]),
                base().map(vec![
                    (ScalarExpr::Literal(Datum::Int64(5)), INT64),
                    (ScalarExpr::Literal(Datum::Int64(3)), INT64),
                ]),
            ),
            (
                "column indexes in mapping expressions are rewritten if input columns are elided",
                base()
                    .map(vec![
                        (ScalarExpr::Column(2), INT64),
                        (ScalarExpr::Column(1), INT64),
                    ])
                    .project(vec![3, 4]),
                base()
                    .project(vec![1, 2])
                    .map(vec![
                        (ScalarExpr::Column(1), INT64),
                        (ScalarExpr::Column(0), INT64),
                    ])
                    .project(vec![2, 3]),
            ),
            (
                "projections that interleave maps with inner columns remain",
                base()
                    .map(vec![
                        (ScalarExpr::Column(2), INT64),
                        (ScalarExpr::Column(0), INT64),
                    ])
                    .project(vec![0, 3, 2, 4]),
                base()
                    .project(vec![0, 2])
                    .map(vec![
                        (ScalarExpr::Column(1), INT64),
                        (ScalarExpr::Column(0), INT64),
                    ])
                    .project(vec![0, 2, 1, 3]),
            ),
            (
                "projected-away map columns do not introduce dependencies",
                base()
                    .map(vec![
                        (ScalarExpr::Column(1), INT64),
                        (ScalarExpr::Column(2), INT64),
                    ])
                    .project(vec![0, 4]),
                base()
                    .project(vec![0, 2])
                    .map(vec![(ScalarExpr::Column(1), INT64)])
                    .project(vec![0, 2]),
            ),
            (
                "repeated columns in projection are not pushed through map",
                base()
                    .map(vec![(ScalarExpr::Column(2), INT64)])
                    .project(vec![0, 0, 0, 3]),
                base()
                    .project(vec![0, 2])
                    .map(vec![(ScalarExpr::Column(1), INT64)])
                    .project(vec![0, 0, 0, 2]),
            ),
            (
                "push through join with no variables",
                RelationExpr::join(vec![base(), base(), base()], vec![])
                    .project(vec![0, 1, 6, 7, 8]),
                RelationExpr::join(
                    vec![base().project(vec![0, 1]), base().project(vec![]), base()],
                    vec![],
                ),
            ),
            (
                "push through join with variables",
                RelationExpr::join(vec![base(), base(), base()], vec![vec![(0, 0), (1, 1)]])
                    .project(vec![0, 1, 6, 7, 8]),
                RelationExpr::join(
                    vec![base().project(vec![0, 1]), base().project(vec![1]), base()],
                    vec![vec![(0, 0), (1, 0)]],
                )
                .project(vec![0, 1, 3, 4, 5]),
            ),
            (
                "push through join with intra-input reordering only elides outer project",
                RelationExpr::join(vec![base(), base(), base()], vec![
                        vec![(0, 0), (1, 0), (2, 0)],
                        vec![(0, 1), (1, 1), (2, 1)],
                        vec![(0, 2), (1, 2), (2, 2)],
                    ])
                    .project(vec![2, 1, 0, 4, 3, 5, 8, 6, 7]),
                RelationExpr::join(
                    vec![
                        base().project(vec![2, 1, 0]),
                        base().project(vec![1, 0, 2]),
                        base().project(vec![2, 0, 1]),
                    ],
                    vec![
                        vec![(0, 2), (1, 1), (2, 1)],
                        vec![(0, 1), (1, 0), (2, 2)],
                        vec![(0, 0), (1, 2), (2, 0)],
                    ],
                ),
            ),
        ];

        let mut optimizer = Optimizer::from_transforms(vec![
            Box::new(super::ProjectionPushdown),
            Box::new(crate::transform::empty_map::EmptyMap),
            Box::new(crate::transform::fusion::project::Project),
        ]);
        for (name, mut expr, expected) in test_cases {
            println!("---- {}", name);
            let typ = expr.typ();
            optimizer.optimize(&mut expr, &typ);
            assert!(
                expr == expected,
                "{}:\n{}\nvs\n{}",
                name,
                expr.pretty(),
                expected.pretty()
            );
        }
    }
}
