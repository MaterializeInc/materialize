use itertools::Itertools;
use mz_repr::{ColumnType, RelationType, Row, ScalarType};
use std::collections::BTreeMap;
use tracing::warn;

use crate::{AggregateExpr, Id, JoinImplementation, MirRelationExpr, MirScalarExpr};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum TypeError<'a> {
    Unbound {
        source: &'a MirRelationExpr,
        id: Id,
        typ: RelationType,
    },
    NoSuchColumn {
        source: &'a MirRelationExpr,
        expr: &'a MirScalarExpr,
        col: usize,
    },
    MismatchColumn {
        source: &'a MirRelationExpr,
        got: ColumnType,
        expected: ColumnType,
        message: String,
    },
    MismatchColumns {
        source: &'a MirRelationExpr,
        got: Vec<ColumnType>,
        expected: Vec<ColumnType>,
        message: String,
    },
    BadConstantRow {
        source: &'a MirRelationExpr,
        got: Row,
        expected: Vec<ColumnType>,
    },
    BadProject {
        source: &'a MirRelationExpr,
        got: Vec<usize>,
        input_type: Vec<ColumnType>,
    },
    BadTopKGroupKey {
        source: &'a MirRelationExpr,
        key: usize,
        input_type: Vec<ColumnType>,
    },
    BadTopKOrdering {
        source: &'a MirRelationExpr,
    },
}

type Ctx = BTreeMap<Id, Vec<ColumnType>>;

fn columns_match(t1: &[ColumnType], t2: &[ColumnType]) -> bool {
    if t1.len() != t2.len() {
        return false;
    }

    t1.iter()
        .zip_eq(t2.iter())
        .all(|(c1, c2)| c1.nullable == c2.nullable && c1.scalar_type.base_eq(&c2.scalar_type))
}

impl MirRelationExpr {
    /// Returns the type of a relation expression or a type error.
    ///
    /// This function is careful to check validity, not just find out the type.
    ///
    /// It should be linear in the size of the AST.
    ///
    /// ??? should we also compute keys and return a `RelationType`?
    pub fn typecheck(&self, ctx: &mut Ctx) -> Result<Vec<ColumnType>, TypeError> {
        use MirRelationExpr::*;

        match self {
            Constant { typ, rows } => {
                if let Ok(rows) = rows {
                    for (row, _id) in rows {
                        let datums = row.unpack();

                        // correct length
                        if datums.len() != typ.column_types.len() {
                            return Err(TypeError::BadConstantRow {
                                source: self,
                                got: row.clone(),
                                expected: typ.column_types.clone(),
                            });
                        }

                        // correct types
                        if datums
                            .iter()
                            .zip_eq(typ.column_types.iter())
                            .any(|(d, ty)| !d.is_instance_of(ty))
                        {
                            return Err(TypeError::BadConstantRow {
                                source: self,
                                got: row.clone(),
                                expected: typ.column_types.clone(),
                            });
                        }
                    }
                }

                Ok(typ.column_types.clone())
            }
            Get { typ, id } => {
                if let Id::Global(global_id) = id {
                    if !ctx.contains_key(id) {
                        // TODO where can we find these types
                        // TODO how should we warn folks about it?
                        warn!("unknown global: {}", global_id);
                        return Ok(typ.column_types.clone());
                    }
                }

                let ctx_typ = ctx.get(id).ok_or_else(|| TypeError::Unbound {
                    source: self,
                    id: id.clone(),
                    typ: typ.clone(),
                })?;

                // the ascribed type must be a subtype of the actual type in the context
                if !columns_match(&typ.column_types, ctx_typ) {
                    return Err(TypeError::MismatchColumns {
                        source: self,
                        got: ctx_typ.clone(),
                        expected: typ.column_types.clone(),
                        message: format!(
                            "{:?}'s annotation did not match context type {:?}",
                            self, ctx_typ
                        ),
                    });
                }

                Ok(typ.column_types.clone())
            }
            Project { input, outputs } => {
                let t_in = input.typecheck(ctx)?;

                for x in outputs {
                    if *x >= t_in.len() {
                        return Err(TypeError::BadProject {
                            source: self,
                            got: outputs.clone(),
                            input_type: t_in.into(),
                        });
                    }
                }

                Ok(outputs.iter().map(|col| t_in[*col].clone()).collect())
            }
            Map { input, scalars } => {
                let mut t_in = input.typecheck(ctx)?;

                for expr in scalars.iter() {
                    t_in.push(expr.typecheck(self, &t_in)?);
                }

                Ok(t_in)
            }
            FlatMap { input, func, exprs } => {
                let mut t_in = input.typecheck(ctx)?;

                let mut t_exprs = Vec::with_capacity(exprs.len());
                for expr in exprs {
                    t_exprs.push(expr.typecheck(self, &t_in)?);
                }
                // TODO check t_exprs agrees with `func`'s input type (where is that recorded?)

                let t_out = func.output_type().column_types;

                // ??? why does col_with_input_cols include the input types in the output of FlatMap
                t_in.extend(t_out);
                Ok(t_in)
            }
            Filter {
                input,
                predicates: _,
            } => {
                let t_in = input.typecheck(ctx)?;

                // col_with_input_cols does a analysis to determine which columns will never be null when the predicate is passed
                //
                // but the analysis is ad hoc, and will miss things:
                //
                // materialize=> create table a(x int, y int);
                // CREATE TABLE
                // materialize=> explain with(types) select x from a where (y=x and y is not null) or x is not null;
                // Optimized Plan
                // --------------------------------------------------------------------------------------------------------
                // Explained Query:                                                                                      +
                // Project (#0) // { types: "(integer?)" }                                                             +
                // Filter ((#0) IS NOT NULL OR ((#1) IS NOT NULL AND (#0 = #1))) // { types: "(integer?, integer?)" }+
                // Get materialize.public.a // { types: "(integer?, integer?)" }                                   +
                //                                                                           +
                // Source materialize.public.a                                                                           +
                // filter=(((#0) IS NOT NULL OR ((#1) IS NOT NULL AND (#0 = #1))))                                     +
                //
                // (1 row)

                // we're skipping the analysis here just to be conservative

                Ok(t_in)
            }
            Join {
                inputs,
                equivalences,
                implementation,
            } => {
                let mut t_in = Vec::new();

                for input in inputs.iter() {
                    t_in.extend(input.typecheck(ctx)?);
                }

                for eq_class in equivalences {
                    let mut t_exprs: Vec<ColumnType> = Vec::with_capacity(eq_class.len());

                    for expr in eq_class {
                        let t_expr = expr.typecheck(self, &t_in)?;

                        if let Some(t_first) = t_exprs.get(0) {
                            // ??? do we care about matching nullability?
                            if !t_expr.scalar_type.base_eq(&t_first.scalar_type) {
                                return Err(TypeError::MismatchColumn {
                                    source: self,
                                    got: t_expr,
                                    expected: t_first.clone(),
                                    message: "equivalence class members do not match".into(),
                                });
                            }
                        }
                        t_exprs.push(t_expr);
                    }
                }

                // check that the join implementation is consistent
                match implementation {
                    JoinImplementation::Differential((_, first_keys, _), others) => {
                        if let Some(keys) = first_keys {
                            for expr in keys {
                                let _ = expr.typecheck(self, &t_in)?;
                            }
                        }

                        for (_, keys, _) in others {
                            for expr in keys {
                                let _ = expr.typecheck(self, &t_in)?;
                            }
                        }
                    }
                    JoinImplementation::DeltaQuery(plans) => {
                        for plan in plans {
                            for (_, keys, _) in plan {
                                for expr in keys {
                                    let _ = expr.typecheck(self, &t_in)?;
                                }
                            }
                        }
                    }
                    JoinImplementation::IndexedFilter(_global_id, keys, consts) => {
                        let typ: Vec<ColumnType> = keys
                            .iter()
                            .map(|expr| expr.typecheck(self, &t_in))
                            .collect::<Result<Vec<ColumnType>, TypeError>>()?;

                        for row in consts {
                            let datums = row.unpack();

                            // correct length
                            if datums.len() != typ.len() {
                                return Err(TypeError::BadConstantRow {
                                    source: self,
                                    got: row.clone(),
                                    expected: typ,
                                });
                            }

                            // correct types
                            if datums
                                .iter()
                                .zip_eq(typ.iter())
                                .any(|(d, ty)| !d.is_instance_of(ty))
                            {
                                return Err(TypeError::BadConstantRow {
                                    source: self,
                                    got: row.clone(),
                                    expected: typ,
                                });
                            }
                        }
                    }
                    JoinImplementation::Unimplemented => (),
                }

                Ok(t_in)
            }
            Reduce {
                input,
                group_key,
                aggregates,
                monotonic: _,
                expected_group_size: _,
            } => {
                let t_in = input.typecheck(ctx)?;

                let t_keys = group_key
                    .iter()
                    .map(|expr| expr.typecheck(self, &t_in))
                    .collect::<Result<Vec<_>, _>>()?;

                for agg in aggregates {
                    let _ = agg.typecheck(self, &t_in)?;
                }

                Ok(t_keys)
            }
            TopK {
                input,
                group_key,
                order_key,
                limit: _,
                offset: _,
                monotonic: _,
            } => {
                let t_in = input.typecheck(ctx)?;

                for &key in group_key {
                    if key >= t_in.len() {
                        return Err(TypeError::BadTopKGroupKey {
                            source: self,
                            key: key,
                            input_type: t_in,
                        });
                    }
                }

                if group_key.len() != order_key.len() {
                    return Err(TypeError::BadTopKOrdering { source: self });
                }

                Ok(t_in)
            }
            Negate { input } => input.typecheck(ctx),
            Threshold { input } => input.typecheck(ctx),
            Union { base, inputs } => {
                let mut t_base = base.typecheck(ctx)?;

                for input in inputs {
                    let t_input = input.typecheck(ctx)?;

                    if t_base.len() != t_input.len() {
                        return Err(TypeError::MismatchColumns {
                            source: self,
                            got: t_base.clone(),
                            expected: t_input.clone(),
                            message: "union branches have different numbers of columns".into(),
                        });
                    }

                    for (base_col, input_col) in t_base.iter_mut().zip_eq(t_input) {
                        *base_col =
                            base_col
                                .union(&input_col)
                                .map_err(|e| TypeError::MismatchColumn {
                                    source: self,
                                    got: input_col,
                                    expected: base_col.clone(),
                                    message: format!(
                                        "couldn't compute union of column types in union: {:?}",
                                        e
                                    ),
                                })?;
                    }
                }

                Ok(t_base)
            }
            Let { id, value, body } => {
                let t_value = value.typecheck(ctx)?;

                let mut body_ctx = ctx.clone();
                body_ctx.insert(Id::Local(*id), t_value);

                body.typecheck(&mut body_ctx)
            }
            LetRec { .. } => {
                // TODO temporary hack: steal info from the Gets inside to learn the expected types
                // or, more cleanly, add type information to LetRec
                unimplemented!("can't typecheck letrec without constraints or something else fancy")
            }
            ArrangeBy { input, keys } => {
                let t_in = input.typecheck(ctx)?;

                for cols in keys {
                    for col in cols {
                        let _ = col.typecheck(self, &t_in)?;
                    }
                }

                Ok(t_in)
            }
        }
    }
}

impl MirScalarExpr {
    fn typecheck<'a>(
        &'a self,
        source: &'a MirRelationExpr,
        column_types: &[ColumnType],
    ) -> Result<ColumnType, TypeError<'a>> {
        match self {
            MirScalarExpr::Column(i) => match column_types.get(*i) {
                Some(ty) => Ok(ty.clone()),
                None => Err(TypeError::NoSuchColumn {
                    source,
                    expr: self,
                    col: *i,
                }),
            },
            MirScalarExpr::Literal(row, typ) => {
                if let Ok(row) = row {
                    let datums = row.unpack();

                    if datums.len() != 1 || !datums[0].is_instance_of(typ) {
                        return Err(TypeError::BadConstantRow {
                            source,
                            got: row.clone(),
                            expected: vec![typ.clone()],
                        });
                    }
                }

                Ok(typ.clone())
            }
            MirScalarExpr::CallUnmaterializable(func) => Ok(func.output_type()),
            MirScalarExpr::CallUnary { expr, func } => {
                Ok(func.output_type(expr.typecheck(source, column_types)?))
            }
            MirScalarExpr::CallBinary { expr1, expr2, func } => Ok(func.output_type(
                expr1.typecheck(source, column_types)?,
                expr2.typecheck(source, column_types)?,
            )),
            MirScalarExpr::CallVariadic { exprs, func } => Ok(func.output_type(
                exprs
                    .iter()
                    .map(|e| e.typecheck(source, column_types))
                    .collect::<Result<Vec<_>, TypeError>>()?,
            )),
            MirScalarExpr::If { cond, then, els } => {
                let cond_type = cond.typecheck(source, column_types)?;

                // condition must be boolean
                // ??? does nullability matter? ignoring it here (on purpose)
                if cond_type.scalar_type != ScalarType::Bool {
                    return Err(TypeError::MismatchColumn {
                        source,
                        got: cond_type,
                        expected: ColumnType {
                            scalar_type: ScalarType::Bool,
                            nullable: true,
                        },
                        message: "expected boolean condition".into(),
                    });
                }

                let then_type = then.typecheck(source, column_types)?;
                let else_type = els.typecheck(source, column_types)?;
                then_type
                    .union(&else_type)
                    .map_err(|e| TypeError::MismatchColumn {
                        source,
                        got: then_type,
                        expected: else_type,
                        message: format!("couldn't compute union of column types for if: {:?}", e),
                    })
            }
        }
    }
}

impl AggregateExpr {
    pub fn typecheck<'a>(
        &'a self,
        source: &'a MirRelationExpr,
        column_types: &[ColumnType],
    ) -> Result<ColumnType, TypeError<'a>> {
        let t_in = self.expr.typecheck(source, column_types)?;

        // TODO check that t_in is actually acceptable for `func`

        Ok(self.func.output_type(t_in))
    }
}

impl<'a> TypeError<'a> {
    pub fn source(&self) -> &'a MirRelationExpr {
        match self {
            TypeError::Unbound { source, .. } => source,
            TypeError::NoSuchColumn { source, .. } => source,
            TypeError::MismatchColumn { source, .. } => source,
            TypeError::MismatchColumns { source, .. } => source,
            TypeError::BadConstantRow { source, .. } => source,
            TypeError::BadProject { source, .. } => source,
            TypeError::BadTopKGroupKey { source, .. } => source,
            TypeError::BadTopKOrdering { source } => source,
        }
    }
}

impl<'a> std::fmt::Display for TypeError<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "TYPE ERROR")?;
        writeln!(f, "{}\n", self.source().pretty())?;

        use TypeError::*;
        match self {
            Unbound { source: _, id, typ } => {
                writeln!(f, "{} is unbound\ndeclared type {:?}", id, typ)?
            }
            NoSuchColumn {
                source: _,
                expr,
                col,
            } => writeln!(
                f,
                "{} references non-existent column {}",
                expr,
                *col
            )?,
            MismatchColumn {
                source: _,
                got,
                expected,
                message,
            } => writeln!(f, "mismatched column types: {}\ngot {:?}\nexpected {:?}\n", message, got, expected)?,
            MismatchColumns {
                source: _,
                got,
                expected,
                message,
            } => writeln!(f, "mismatched relation types: {}\ngot {:?}\nexpected {:?}", message, got, expected)?,
            BadConstantRow {
                source: _,
                got,
                expected,
            } => writeln!(f, "bad constant row\ngot {}\nexpected row of type {:?}", got, expected)?,
            BadProject {
                source: _,
                got,
                input_type,
            } => writeln!(f, "projection of non-existant columns {:?} from type {:?}", got, input_type)?,
            BadTopKGroupKey {
                source: _,
                key,
                input_type,
            } => writeln!(f, "TopK group key {} references invalid group\ngroups {:?}", key, input_type)?,
            BadTopKOrdering { source: _ } => writeln!(f, "TopK group keys and orderings have different lengths")?,
        }

        Ok(())
    }
}
