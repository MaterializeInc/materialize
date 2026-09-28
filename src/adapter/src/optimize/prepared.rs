// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Parameterized programs for indexed reads. Compilation never observes bind values.

use std::collections::BTreeSet;

use itertools::Itertools;
use mz_expr::RowSetFinishing;
use mz_expr::visit::Visit;
use mz_expr::{
    BinaryFunc, Columns, Eval, Id, MapFilterProject, MirScalarExpr, SafeMfpPlan, UnaryFunc,
    UnmaterializableFunc, VariadicFunc, permutation_for_arrangement,
};
use mz_ore::num::NonNeg;
use mz_repr::{Datum, GlobalId, ReprScalarType, Row, RowArena, SqlScalarType};
use mz_sql::plan::{HirRelationExpr, HirScalarExpr, HirToMirConfig as Config, Params};

use crate::coord::peek::FastPathPlan;
use crate::optimize::OptimizerError;

#[derive(Debug, serde::Serialize)]
pub struct FinishingTemplate {
    program: RowSetFinishing<MirScalarExpr, MirScalarExpr>,
}

impl FinishingTemplate {
    pub fn compile(
        finishing: &RowSetFinishing<HirScalarExpr, HirScalarExpr>,
        parameter_count: usize,
    ) -> Result<Option<Self>, OptimizerError> {
        let lower = |expr: &HirScalarExpr| {
            lower_scalar(expr, &[], 0, parameter_count, &[], Config::default())
        };
        let limit = match &finishing.limit {
            Some(limit) => match lower(limit)? {
                Some(limit) => Some(limit),
                None => return Ok(None),
            },
            None => None,
        };
        let Some(offset) = lower(&finishing.offset)? else {
            return Ok(None);
        };
        Ok(Some(Self {
            program: RowSetFinishing {
                limit,
                offset,
                order_by: finishing.order_by.clone(),
                project: finishing.project.clone(),
            },
        }))
    }

    /// Error and NULL OFFSET cases use ordinary binding to preserve its diagnostics.
    pub fn instantiate(&self, params: &Params) -> Option<RowSetFinishing> {
        let datums: Vec<_> = params.datums.iter().collect();
        let arena = RowArena::new();
        let limit = match &self.program.limit {
            None => None,
            Some(expr) => match expr.eval(&datums, &arena).ok()? {
                Datum::Null => None,
                Datum::Int64(value) => Some(NonNeg::try_from(value).ok()?),
                _ => return None,
            },
        };
        let Datum::Int64(offset) = self.program.offset.eval(&datums, &arena).ok()? else {
            return None;
        };
        Some(RowSetFinishing {
            limit,
            offset: usize::try_from(offset).ok()?,
            order_by: self.program.order_by.clone(),
            project: self.program.project.clone(),
        })
    }
}

/// A linear query with typed parameter inputs, independent of any index layout.
///
/// The symbolic MFP input is `[collection columns, parameter slots, dynamic slots]`. Slots are
/// real, typed input positions while compiling, never sample literal values.
#[derive(Debug)]
pub struct LinearQueryTemplate {
    collection_id: GlobalId,
    collection_arity: usize,
    parameter_types: Vec<SqlScalarType>,
    dynamic_functions: Vec<UnmaterializableFunc>,
    mfp: SafeMfpPlan,
}

/// An immutable, index-specific query program with no execution-owned resources.
#[derive(Debug, serde::Serialize)]
pub struct IndexedQueryTemplate {
    collection_id: GlobalId,
    index_id: GlobalId,
    parameter_types: Vec<SqlScalarType>,
    dynamic_functions: Vec<UnmaterializableFunc>,
    key: Vec<MirScalarExpr>,
    mfp: SafeMfpPlan,
    scan_mfp: SafeMfpPlan,
}

impl LinearQueryTemplate {
    /// Compiles a Get/Map/Filter/Project expression, or declines unsupported HIR.
    /// Subqueries and other relational operators use the custom planner.
    pub fn compile(
        source: &HirRelationExpr,
        parameter_types: &[SqlScalarType],
        config: Config,
    ) -> Result<Option<Self>, OptimizerError> {
        let mut operators = Vec::new();
        let mut input = source;
        let (collection_id, collection_arity) = loop {
            match input {
                HirRelationExpr::Get {
                    id: Id::Global(id),
                    typ,
                } => break (*id, typ.arity()),
                HirRelationExpr::Map { input: next, .. }
                | HirRelationExpr::Filter { input: next, .. }
                | HirRelationExpr::Project { input: next, .. } => {
                    operators.push(input);
                    input = next;
                }
                _ => return Ok(None),
            }
        };
        let mut dynamic_functions = BTreeSet::new();
        for operator in &operators {
            let scalars = match operator {
                HirRelationExpr::Map { scalars, .. } => scalars.as_slice(),
                HirRelationExpr::Filter { predicates, .. } => predicates.as_slice(),
                _ => &[],
            };
            for scalar in scalars {
                scalar.visit_post(&mut |expr| {
                    if let HirScalarExpr::CallUnmaterializable(func, _) = expr {
                        dynamic_functions.insert(func.clone());
                    }
                });
            }
        }
        let dynamic_functions: Vec<_> = dynamic_functions.into_iter().collect();
        let slot_end = collection_arity + parameter_types.len() + dynamic_functions.len();
        let mut mfp = MapFilterProject::new(slot_end).project(0..collection_arity);
        for operator in operators.into_iter().rev() {
            match operator {
                HirRelationExpr::Project { outputs, .. } => {
                    mfp = mfp.project(outputs.iter().copied())
                }
                HirRelationExpr::Map { scalars, .. } => {
                    for scalar in scalars {
                        let Some(scalar) = lower_scalar(
                            scalar,
                            &mfp.projection,
                            collection_arity,
                            parameter_types.len(),
                            &dynamic_functions,
                            config,
                        )?
                        else {
                            return Ok(None);
                        };
                        // Scalar columns already refer to the symbolic MFP's layout.
                        mfp.expressions.push(scalar);
                        mfp.projection
                            .push(mfp.input_arity + mfp.expressions.len() - 1);
                    }
                }
                HirRelationExpr::Filter { predicates, .. } => {
                    for predicate in predicates {
                        let Some(predicate) = lower_scalar(
                            predicate,
                            &mfp.projection,
                            collection_arity,
                            parameter_types.len(),
                            &dynamic_functions,
                            config,
                        )?
                        else {
                            return Ok(None);
                        };
                        let before = predicate.support().into_iter().max().map_or(0, |c| c + 1);
                        mfp.predicates.push((before, predicate));
                    }
                }
                _ => unreachable!("validated linear operator"),
            }
        }
        // All MFP optimization happens on the symbolic program, before any bind.
        let mfp = mfp
            .into_plan()
            .map_err(OptimizerError::InternalUnsafeMfpPlan)?
            .into_nontemporal()
            .map_err(|_| OptimizerError::InternalUnsafeMfpPlan("temporal prepared MFP".into()))?;
        Ok(Some(Self {
            collection_id,
            collection_arity,
            parameter_types: parameter_types.to_vec(),
            dynamic_functions,
            mfp,
        }))
    }

    pub fn collection_id(&self) -> GlobalId {
        self.collection_id
    }

    /// Selects an index whose complete key is constrained by parameter-only expressions.
    /// The caller must validate the index's collection, cluster and availability.
    pub fn for_index(
        &self,
        index_id: GlobalId,
        index_key: &[MirScalarExpr],
    ) -> Option<IndexedQueryTemplate> {
        if index_key.is_empty() {
            return None;
        }
        let mut key = Vec::with_capacity(index_key.len());
        for key_expr in index_key {
            let mut constraint = None;
            let mut predicates: Vec<_> = self.mfp.predicates.iter().map(|(_, p)| p).collect();
            while let Some(predicate) = predicates.pop() {
                match predicate {
                    MirScalarExpr::CallVariadic {
                        func: VariadicFunc::And(_),
                        exprs,
                    } => predicates.extend(exprs),
                    MirScalarExpr::CallBinary {
                        func: BinaryFunc::Eq(_),
                        expr1,
                        expr2,
                    } => {
                        let value = if **expr1 == *key_expr {
                            expr2
                        } else if **expr2 == *key_expr {
                            expr1
                        } else {
                            continue;
                        };
                        if value
                            .support()
                            .iter()
                            .all(|c| *c >= self.collection_arity && *c < self.mfp.input_arity)
                        {
                            let mut value = (**value).clone();
                            value.visit_mut_post(&mut |expr| {
                                if let MirScalarExpr::Column(c, _) = expr {
                                    *c -= self.collection_arity;
                                }
                            });
                            constraint = Some(value);
                            break;
                        }
                    }
                    _ => (),
                }
            }
            key.push(constraint?);
        }
        let (permutation, thinning) = permutation_for_arrangement(index_key, self.collection_arity);
        let index_arity = index_key.len() + thinning.len();
        let mut mfp = self.mfp.clone();
        mfp.permute_fn(
            |c| {
                if c < self.collection_arity {
                    permutation[c]
                } else {
                    index_arity + c - self.collection_arity
                }
            },
            index_arity + self.parameter_types.len() + self.dynamic_functions.len(),
        );
        let scan_mfp = mfp.clone();
        // Literal-constrained peeks append the matching key after the index's
        // key/value columns, as an IndexedFilter join would. Reserve that input
        // suffix before the bind slots. Scans do not receive the extra key.
        mfp.permute_fn(
            |c| {
                if c < index_arity {
                    c
                } else {
                    c + index_key.len()
                }
            },
            mfp.input_arity + index_key.len(),
        );
        Some(IndexedQueryTemplate {
            collection_id: self.collection_id,
            index_id,
            parameter_types: self.parameter_types.clone(),
            dynamic_functions: self.dynamic_functions.clone(),
            key,
            mfp,
            scan_mfp,
        })
    }
}

impl IndexedQueryTemplate {
    /// Instantiates concrete compute input without SQL planning or MFP optimization.
    /// Different execution types require the custom path's SQL cast planning.
    /// `resolve` must use this execution's timestamp/session context and return
    /// the literal that ordinary one-shot expression preparation would produce.
    pub fn instantiate<F>(
        &self,
        params: &Params,
        mut resolve: F,
    ) -> Result<Option<FastPathPlan>, OptimizerError>
    where
        F: FnMut(&UnmaterializableFunc) -> Result<MirScalarExpr, OptimizerError>,
    {
        if params.expected_types != self.parameter_types
            || params.execute_types != self.parameter_types
        {
            return Ok(None);
        }
        let mut datums: Vec<_> = params.datums.iter().collect();
        if datums.len() != self.parameter_types.len() {
            return Ok(None);
        }
        let mut dynamic_values = self
            .dynamic_functions
            .iter()
            .map(&mut resolve)
            .collect::<Result<Vec<_>, _>>()?;
        for (value, func) in dynamic_values.iter().zip_eq(&self.dynamic_functions) {
            match value {
                MirScalarExpr::Literal(Ok(row), typ)
                    if typ.scalar_type == func.output_type().scalar_type =>
                {
                    datums.push(row.unpack_first())
                }
                _ => {
                    return Err(OptimizerError::Internal(
                        "prepared dynamic slot did not resolve to a typed literal".into(),
                    ));
                }
            }
        }
        let arena = RowArena::new();
        // A failing key expression must not become an eager error on empty input.
        // Keep the residual predicate and let the ordinary MFP evaluate it on rows.
        let key = self
            .key
            .iter()
            .map(|expr| expr.eval(&datums, &arena))
            .collect::<Result<Vec<_>, _>>()
            .ok()
            .map(|values| vec![Row::pack(values)]);
        let mut mfp = if key.is_some() {
            &self.mfp
        } else {
            &self.scan_mfp
        }
        .clone()
        .into_mfp();
        mfp.input_arity -= datums.len();
        // Replacing the symbolic input suffix by leading literal maps preserves
        // every column number, including predicate evaluation boundaries.
        let mut expressions: Vec<_> = datums
            .into_iter()
            // Dynamic datums follow parameters and are appended as typed
            // expressions below, rather than converted a second time here.
            .take(self.parameter_types.len())
            .zip_eq(&self.parameter_types)
            .map(|(datum, typ)| MirScalarExpr::literal_ok(datum, ReprScalarType::from(typ)))
            .collect();
        expressions.append(&mut dynamic_values);
        expressions.append(&mut mfp.expressions);
        mfp.expressions = expressions;
        Ok(Some(FastPathPlan::PeekExisting(
            self.collection_id,
            self.index_id,
            key,
            SafeMfpPlan::from_mfp(mfp),
        )))
    }
}

fn lower_scalar(
    scalar: &HirScalarExpr,
    projection: &[usize],
    slot_start: usize,
    parameter_count: usize,
    dynamic_functions: &[UnmaterializableFunc],
    config: Config,
) -> Result<Option<MirScalarExpr>, OptimizerError> {
    let mut scalar = scalar.clone();
    let result = scalar.try_visit_mut_post(&mut |expr| {
        match expr {
            HirScalarExpr::Column(column, _) if column.level == 0 => {
                column.column = *projection.get(column.column).ok_or(())?;
            }
            HirScalarExpr::Parameter(n, _) if *n > 0 && *n <= parameter_count => {
                *expr = HirScalarExpr::column(slot_start + *n - 1);
            }
            HirScalarExpr::CallUnmaterializable(func, _) => {
                let slot = dynamic_functions.binary_search(func).map_err(|_| ())?;
                *expr = HirScalarExpr::column(slot_start + parameter_count + slot);
            }
            // These unsafe functions have effects beyond their returned datum.
            // In particular, extracting a lookup key must not evaluate them again.
            HirScalarExpr::CallUnary {
                func: UnaryFunc::Sleep(_) | UnaryFunc::Panic(_),
                ..
            } => return Err(()),
            HirScalarExpr::Column(..)
            | HirScalarExpr::Parameter(..)
            | HirScalarExpr::Exists(..)
            | HirScalarExpr::Select(..)
            | HirScalarExpr::Windowing(..) => return Err(()),
            _ => (),
        }
        Ok(())
    });
    if result.is_err() {
        return Ok(None);
    }
    Ok(Some(scalar.lower_uncorrelated(config)?))
}

#[cfg(test)]
mod tests {
    use mz_expr::func;
    use mz_expr::{CollectionPlan, EvalError};
    use mz_ore::assert_none;
    use mz_ore::collections::CollectionExt;
    use mz_repr::{Datum, SqlRelationType};

    use super::*;

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn compiles_retained_sql_analysis() {
        crate::catalog::Catalog::with_debug(|catalog| async move {
            let conn_catalog = catalog.for_system_session();
            let sql = "SELECT id, name || $2 FROM mz_catalog.mz_tables WHERE id = $1";
            let stmt = mz_sql::parse::parse(sql)
                .expect("test fixture must be valid")
                .into_element()
                .ast;
            let (stmt, _) =
                mz_sql::names::resolve(&conn_catalog, stmt).expect("test fixture must be valid");
            let analysis = mz_sql::plan::describe_analyzed(
                &mz_sql::plan::PlanContext::zero(),
                &conn_catalog,
                stmt,
                &[],
            )
            .expect("test fixture must be valid");
            let source = analysis
                .select
                .as_ref()
                .expect("test fixture must be valid")
                .source();
            let linear =
                LinearQueryTemplate::compile(source, &analysis.desc.param_types, Config::default())
                    .expect("test fixture must be valid")
                    .unwrap_or_else(|| panic!("unsupported SQL HIR: {source:?}"));
            assert!(source.depends_on().contains(&linear.collection_id()));
            let template = linear
                .for_index(GlobalId::User(42), &MirScalarExpr::columns(&[0]))
                .expect("test fixture must be valid");
            for key in ["u1", "u2", "u3"] {
                let params = Params {
                    datums: Row::pack([Datum::String(key), Datum::String("-suffix")]),
                    expected_types: analysis.desc.param_types.clone(),
                    execute_types: analysis.desc.param_types.clone(),
                };
                let FastPathPlan::PeekExisting(_, _, keys, mfp) =
                    instantiate(&template, &params).expect("test fixture must be valid")
                else {
                    unreachable!()
                };
                assert_eq!(keys, Some(vec![Row::pack([Datum::String(key)])]));
                let mut datums = vec![Datum::Null; linear.collection_arity];
                datums[0] = Datum::String(key);
                datums[3] = Datum::String("table");
                datums.extend(keys.as_ref().expect("test fixture must be valid")[0].iter());
                let row = mfp
                    .evaluate_iter(&mut datums, &RowArena::new())
                    .unwrap_or_else(|error| panic!("{error}: {mfp:?}"))
                    .map(Row::pack);
                assert_eq!(
                    row,
                    Some(Row::pack([
                        Datum::String(key),
                        Datum::String("table-suffix"),
                    ]))
                );
            }
            catalog.expire().await;
        })
        .await;
    }

    fn column(column: usize) -> HirScalarExpr {
        HirScalarExpr::column(column)
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn finishing_matches_sql_binding() {
        crate::catalog::Catalog::with_debug(|catalog| async move {
            let conn_catalog = catalog.for_system_session();
            let stmt = mz_sql::parse::parse("SELECT $1::int LIMIT $2 + 1 OFFSET $3")
                .expect("test fixture must be valid")
                .into_element()
                .ast;
            let (stmt, ids) =
                mz_sql::names::resolve(&conn_catalog, stmt).expect("test fixture must be valid");
            let pcx = mz_sql::plan::PlanContext::zero();
            let analysis = mz_sql::plan::describe_analyzed(&pcx, &conn_catalog, stmt, &[])
                .expect("test fixture must be valid");
            let analyzed = analysis.select.expect("test fixture must be valid");
            let finishing = FinishingTemplate::compile(analyzed.finishing(), 3)
                .expect("test fixture must be valid")
                .expect("test fixture must be valid");
            for (limit, offset) in [
                (Some(0), Some(0)),
                (Some(4), Some(2)),
                (None, Some(0)),
                (Some(-1), Some(0)),
                (Some(-2), Some(0)),
                (Some(i64::MAX), Some(0)),
                (Some(1), Some(-1)),
                (Some(1), None),
            ] {
                let params = Params {
                    datums: Row::pack([
                        Datum::Int32(7),
                        limit.map(Datum::Int64).unwrap_or(Datum::Null),
                        offset.map(Datum::Int64).unwrap_or(Datum::Null),
                    ]),
                    expected_types: analysis.desc.param_types.clone(),
                    execute_types: analysis.desc.param_types.clone(),
                };
                let ordinary = mz_sql::plan::plan_analyzed_select(
                    &pcx,
                    &conn_catalog,
                    &analyzed,
                    &params,
                    &ids,
                );
                match finishing.instantiate(&params) {
                    Some(actual) => assert_eq!(
                        actual,
                        ordinary.expect("test fixture must be valid").0.finishing
                    ),
                    None => assert!(ordinary.is_err(), "unexpected finishing fallback"),
                }
            }
            catalog.expire().await;
        })
        .await;
    }

    fn parameter(n: usize) -> HirScalarExpr {
        HirScalarExpr::Parameter(n, Default::default())
    }

    fn binary(func: BinaryFunc, left: HirScalarExpr, right: HirScalarExpr) -> HirScalarExpr {
        HirScalarExpr::CallBinary {
            func,
            expr1: Box::new(left),
            expr2: Box::new(right),
            name: Default::default(),
        }
    }

    fn get(arity: usize) -> HirRelationExpr {
        HirRelationExpr::Get {
            id: Id::Global(GlobalId::User(1)),
            typ: SqlRelationType::new(vec![SqlScalarType::Int32.nullable(true); arity]),
        }
    }

    fn params(values: &[Option<i32>]) -> Params {
        Params {
            datums: Row::pack(
                values
                    .iter()
                    .map(|v| v.map(Datum::Int32).unwrap_or(Datum::Null)),
            ),
            expected_types: vec![SqlScalarType::Int32; values.len()],
            execute_types: vec![SqlScalarType::Int32; values.len()],
        }
    }

    fn evaluate(
        template: &IndexedQueryTemplate,
        params: &Params,
        row: &[i32],
    ) -> Result<Option<Row>, EvalError> {
        let FastPathPlan::PeekExisting(_, _, keys, mfp) =
            instantiate(template, params).expect("test fixture must be valid")
        else {
            unreachable!()
        };
        let mut datums: Vec<_> = row.iter().map(|v| Datum::Int32(*v)).collect();
        if let Some(keys) = &keys {
            datums.extend(keys[0].iter());
        }
        mfp.evaluate_iter(&mut datums, &RowArena::new())
            .map(|row| row.map(Row::pack))
    }

    fn instantiate(template: &IndexedQueryTemplate, params: &Params) -> Option<FastPathPlan> {
        template
            .instantiate(params, |_| unreachable!("no dynamic slots"))
            .expect("test fixture must be valid")
    }

    #[mz_ore::test]
    fn binds_changing_values_and_nulls_without_recompiling() {
        let source = get(2)
            .filter(vec![binary(func::Eq.into(), column(0), parameter(1))])
            .map(vec![
                binary(func::AddInt32.into(), column(1), parameter(2)),
                parameter(2),
            ])
            .project(vec![2, 0, 3]);
        let linear = LinearQueryTemplate::compile(
            &source,
            &[const { SqlScalarType::Int32 }; 2],
            Config::default(),
        )
        .expect("test fixture must be valid")
        .expect("test fixture must be valid");
        let template = linear
            .for_index(GlobalId::User(2), &[MirScalarExpr::column(0)])
            .expect("test fixture must be valid");
        let original = format!("{template:?}");
        for (key, addend) in [(1, 10), (2, 42), (1, -10)] {
            let params = params(&[Some(key), Some(addend)]);
            let FastPathPlan::PeekExisting(_, _, Some(keys), _) =
                instantiate(&template, &params).expect("test fixture must be valid")
            else {
                panic!("missing lookup key")
            };
            assert_eq!(keys, vec![Row::pack([Datum::Int32(key)])]);
            assert_eq!(
                evaluate(&template, &params, &[key, 20]).expect("test fixture must be valid"),
                Some(Row::pack([
                    Datum::Int32(20 + addend),
                    Datum::Int32(key),
                    Datum::Int32(addend)
                ]))
            );
            assert_none!(
                evaluate(&template, &params, &[key + 1, 20]).expect("test fixture must be valid")
            );
            assert_eq!(format!("{template:?}"), original);
        }
        assert_none!(
            evaluate(&template, &params(&[None, Some(10)]), &[1, 20])
                .expect("test fixture must be valid")
        );
        assert_eq!(
            evaluate(&template, &params(&[Some(1), None]), &[1, 20])
                .expect("test fixture must be valid"),
            Some(Row::pack([Datum::Null, Datum::Int32(1), Datum::Null]))
        );
        let mut different_type = params(&[Some(1), Some(10)]);
        different_type.execute_types[0] = SqlScalarType::Int64;
        assert_none!(instantiate(&template, &different_type));
    }

    #[mz_ore::test]
    fn composite_index_permutation_preserves_parameter_slots() {
        let source = get(3)
            .filter(vec![
                binary(func::Eq.into(), column(1), parameter(1)),
                binary(func::Eq.into(), parameter(2), column(0)),
            ])
            .map(vec![parameter(2)])
            .project(vec![2, 0, 1, 3]);
        let linear = LinearQueryTemplate::compile(
            &source,
            &[const { SqlScalarType::Int32 }; 2],
            Config::default(),
        )
        .expect("test fixture must be valid")
        .expect("test fixture must be valid");
        let template = linear
            .for_index(GlobalId::User(2), &MirScalarExpr::columns(&[1, 0]))
            .expect("test fixture must be valid");
        let params = params(&[Some(7), Some(5)]);
        let FastPathPlan::PeekExisting(_, _, Some(keys), _) =
            instantiate(&template, &params).expect("test fixture must be valid")
        else {
            panic!("missing lookup key")
        };
        assert_eq!(keys, vec![Row::pack([Datum::Int32(7), Datum::Int32(5)])]);
        assert_eq!(
            evaluate(&template, &params, &[7, 5, 100]).expect("test fixture must be valid"),
            Some(Row::pack([
                Datum::Int32(100),
                Datum::Int32(5),
                Datum::Int32(7),
                Datum::Int32(5)
            ]))
        );
        assert_none!(linear.for_index(GlobalId::User(3), &MirScalarExpr::columns(&[2])));
    }

    #[mz_ore::test]
    fn failing_key_expressions_are_not_eager_bind_errors() {
        let source = get(1).filter(vec![binary(
            func::Eq.into(),
            column(0),
            binary(func::DivInt32.into(), parameter(1), parameter(2)),
        )]);
        let linear = LinearQueryTemplate::compile(
            &source,
            &[const { SqlScalarType::Int32 }; 2],
            Config::default(),
        )
        .expect("test fixture must be valid")
        .expect("test fixture must be valid");
        let template = linear
            .for_index(GlobalId::User(2), &MirScalarExpr::columns(&[0]))
            .expect("test fixture must be valid");
        let params = params(&[Some(1), Some(0)]);
        let FastPathPlan::PeekExisting(_, _, keys, _) =
            instantiate(&template, &params).expect("test fixture must be valid")
        else {
            unreachable!()
        };
        assert_none!(keys);
        assert!(evaluate(&template, &params, &[1]).is_err());
    }

    #[mz_ore::test]
    fn dynamic_slots_are_resolved_for_each_execution() {
        let source = get(1)
            .filter(vec![binary(func::Eq.into(), column(0), parameter(1))])
            .map(vec![
                HirScalarExpr::CallUnmaterializable(
                    UnmaterializableFunc::MzNow,
                    Default::default(),
                ),
                HirScalarExpr::CallUnmaterializable(
                    UnmaterializableFunc::CurrentUser,
                    Default::default(),
                ),
            ]);
        let linear =
            LinearQueryTemplate::compile(&source, &[SqlScalarType::Int32], Config::default())
                .expect("test fixture must be valid")
                .expect("test fixture must be valid");
        let template = linear
            .for_index(GlobalId::User(2), &MirScalarExpr::columns(&[0]))
            .expect("test fixture must be valid");
        for (timestamp, user) in [(100_u64, "first"), (200, "second")] {
            let mut calls = Vec::new();
            let plan = template
                .instantiate(&params(&[Some(7)]), |func| {
                    calls.push(func.clone());
                    let value = match func {
                        UnmaterializableFunc::MzNow => Datum::MzTimestamp(timestamp.into()),
                        UnmaterializableFunc::CurrentUser => Datum::String(user),
                        _ => unreachable!(),
                    };
                    Ok(MirScalarExpr::literal_ok(
                        value,
                        func.output_type().scalar_type,
                    ))
                })
                .expect("test fixture must be valid")
                .expect("test fixture must be valid");
            assert_eq!(calls.len(), 2);
            let FastPathPlan::PeekExisting(_, _, keys, mfp) = plan else {
                unreachable!()
            };
            let mut datums = vec![Datum::Int32(7)];
            datums.extend(keys.as_ref().expect("test fixture must be valid")[0].iter());
            let row = mfp
                .evaluate_iter(&mut datums, &RowArena::new())
                .expect("test fixture must be valid")
                .map(Row::pack);
            assert_eq!(
                row,
                Some(Row::pack([
                    Datum::Int32(7),
                    Datum::MzTimestamp(timestamp.into()),
                    Datum::String(user)
                ]))
            );
        }
    }

    #[mz_ore::test]
    fn remaps_projections_and_successive_maps() {
        let source = get(3)
            .project(vec![2, 0, 2])
            .filter(vec![binary(func::Eq.into(), column(1), parameter(1))])
            .map(vec![binary(func::AddInt32.into(), column(0), parameter(2))])
            .map(vec![binary(func::AddInt32.into(), column(3), parameter(2))])
            .project(vec![4, 3, 0, 2]);
        let linear = LinearQueryTemplate::compile(
            &source,
            &[const { SqlScalarType::Int32 }; 2],
            Config::default(),
        )
        .expect("test fixture must be valid")
        .expect("test fixture must be valid");
        let template = linear
            .for_index(GlobalId::User(2), &MirScalarExpr::columns(&[0]))
            .expect("test fixture must be valid");
        for addend in [-3, 0, 7] {
            let row = evaluate(&template, &params(&[Some(1), Some(addend)]), &[1, 2, 3])
                .expect("test fixture must be valid");
            assert_eq!(
                row,
                Some(Row::pack([
                    Datum::Int32(3 + 2 * addend),
                    Datum::Int32(3 + addend),
                    Datum::Int32(3),
                    Datum::Int32(3)
                ]))
            );
        }
    }

    #[mz_ore::test]
    fn preserves_conditional_error_evaluation() {
        let source = get(2)
            .filter(vec![binary(func::Eq.into(), column(0), parameter(1))])
            .map(vec![HirScalarExpr::If {
                cond: Box::new(binary(
                    func::Eq.into(),
                    parameter(2),
                    HirScalarExpr::literal(Datum::Int32(0), SqlScalarType::Int32),
                )),
                then: Box::new(HirScalarExpr::literal(
                    Datum::Int32(42),
                    SqlScalarType::Int32,
                )),
                els: Box::new(binary(func::DivInt32.into(), column(1), parameter(2))),
                name: Default::default(),
            }])
            .project(vec![2]);
        let linear = LinearQueryTemplate::compile(
            &source,
            &[const { SqlScalarType::Int32 }; 2],
            Config::default(),
        )
        .expect("test fixture must be valid")
        .expect("test fixture must be valid");
        let template = linear
            .for_index(GlobalId::User(2), &MirScalarExpr::columns(&[0]))
            .expect("test fixture must be valid");
        for (denominator, expected) in [(0, 42), (2, 3), (-1, -6)] {
            let row = evaluate(&template, &params(&[Some(1), Some(denominator)]), &[1, 6])
                .expect("test fixture must be valid");
            assert_eq!(row, Some(Row::pack([Datum::Int32(expected)])));
        }
    }

    #[mz_ore::test]
    fn declines_unsupported_relations_and_effectful_functions() {
        let source = HirRelationExpr::Distinct {
            input: Box::new(get(1)),
        };
        assert_none!(
            LinearQueryTemplate::compile(&source, &[], Config::default())
                .expect("test fixture must be valid")
        );
        let source = get(1).map(vec![HirScalarExpr::CallUnary {
            func: func::Sleep.into(),
            expr: Box::new(parameter(1)),
            name: Default::default(),
        }]);
        assert_none!(
            LinearQueryTemplate::compile(&source, &[SqlScalarType::Float64], Config::default())
                .expect("test fixture must be valid")
        );
    }
}
