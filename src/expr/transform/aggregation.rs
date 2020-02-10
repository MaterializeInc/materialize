// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::RelationExpr;

#[derive(Debug)]
pub struct FractureReduce;

impl super::Transform for FractureReduce {
    fn transform(&self, relation: &mut RelationExpr) {
        self.transform(relation);
        panic!("FractureReduce currently incorrect; do not use");
    }
}

impl FractureReduce {
    pub fn transform(&self, relation: &mut RelationExpr) {
        relation.visit_mut_pre(&mut |e| {
            self.action(e);
        });
    }
    pub fn action(&self, relation: &mut RelationExpr) {
        if let RelationExpr::Reduce {
            input,
            group_key,
            aggregates,
        } = relation
        {
            if aggregates.len() > 1 {
                let keys = group_key.len();
                let mut projection = group_key.clone();
                projection.push(input.arity());

                let mut to_join = Vec::new();
                let aggregates_len = aggregates.len();
                for (agg, typ) in aggregates.drain(..) {
                    let temp = input
                        .clone()
                        // .map(vec![(agg.expr, typ.clone())]) // TODO: correct for Average?
                        // .project(projection.clone())
                        ;

                    // // TODO: this is not always a win, but it seemed like a way to
                    // // elicit more opportunities for re-use.
                    // if agg.distinct {
                    //     temp = temp.distinct();
                    //     agg.distinct = false;
                    // }

                    let single_reduce = temp.reduce(
                        // (0..keys).collect(),
                        group_key.clone(),
                        vec![(
                            crate::AggregateExpr {
                                func: agg.func,
                                // expr: ScalarExpr::Column(keys),
                                expr: agg.expr,
                                distinct: agg.distinct,
                            },
                            typ,
                        )],
                    );
                    to_join.push(single_reduce);
                }

                // All pairs of aggregate, key_column.
                let variables = (0..group_key.len())
                    .map(|k| (0..to_join.len()).map(|a| (a, k)).collect::<Vec<_>>())
                    .collect::<Vec<_>>();

                let mut projection = (0..keys).collect::<Vec<_>>();
                for i in 0..aggregates_len {
                    projection.push((keys + 1) * i + keys);
                }

                *relation = RelationExpr::join(to_join, variables).project(projection);
            }
        }
    }
}

#[derive(Debug)]
pub struct AbelianReduce;

impl super::Transform for AbelianReduce {
    fn transform(&self, relation: &mut RelationExpr) {
        self.transform(relation)
    }
}

impl AbelianReduce {
    pub fn transform(&self, relation: &mut RelationExpr) {
        relation.visit_mut_pre(&mut |e| {
            self.action(e);
        });
    }
    pub fn action(&self, relation: &mut RelationExpr) {
        if let RelationExpr::Reduce {
            input: _,
            group_key: _,
            aggregates: _,
        } = relation
        {}
    }
}

#[cfg(test)]
mod tests {
    use crate::{AggregateExpr, AggregateFunc, RelationExpr, ScalarExpr};
    use repr::{ColumnType, RelationType, ScalarType};

    #[test]
    fn transform() {
        let typ1 = RelationType::new(vec![
            ColumnType::new(ScalarType::Int64),
            ColumnType::new(ScalarType::Int64),
            ColumnType::new(ScalarType::Int64),
            ColumnType::new(ScalarType::Int64),
        ]);

        let data = RelationExpr::constant(vec![], typ1);

        let agg0 = AggregateExpr {
            func: AggregateFunc::MaxInt64,
            expr: ScalarExpr::Column(0),
            distinct: false,
        };
        let agg1 = AggregateExpr {
            func: AggregateFunc::SumInt64,
            expr: ScalarExpr::Column(2),
            distinct: false,
        };
        let agg2 = AggregateExpr {
            func: AggregateFunc::Count,
            expr: ScalarExpr::Column(1),
            distinct: true,
        };
        let agg3 = AggregateExpr {
            func: AggregateFunc::MinInt64,
            expr: ScalarExpr::Column(0),
            distinct: false,
        };

        let data = data.reduce(
            vec![1, 3],
            vec![
                (agg0, ColumnType::new(ScalarType::Int64)),
                (agg1, ColumnType::new(ScalarType::Int64)),
                (agg2, ColumnType::new(ScalarType::Int64)),
                (agg3, ColumnType::new(ScalarType::Int64)),
            ],
        );

        let mut new_data = data.clone();
        let fracture_reduce = super::FractureReduce;

        let typ2 = RelationType::new(vec![
            ColumnType::new(ScalarType::Int64),
            ColumnType::new(ScalarType::Int64),
            ColumnType::new(ScalarType::Int64),
            ColumnType::new(ScalarType::Int64),
            ColumnType::new(ScalarType::Int64),
            ColumnType::new(ScalarType::Int64),
        ]);

        fracture_reduce.transform(&mut new_data, &typ2);

        println!("Input: {:#?}", data);
        println!("Optimized: {:#?}", new_data);

        // assert_eq!(new_data, data);
    }
}
