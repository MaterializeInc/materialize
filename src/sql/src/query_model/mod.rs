// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cell::{Ref, RefCell, RefMut};
use std::collections::BTreeSet;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;

use expr::BinaryFunc;
use ore::id_gen::Gen;
use repr::ColumnType;

pub mod dot;
mod hir;
mod lowering;
mod scalar_expr;
#[cfg(test)]
mod test;

pub use scalar_expr::*;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QuantifierId(u64);
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BoxId(u64);

impl std::fmt::Display for QuantifierId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::fmt::Display for BoxId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<u64> for QuantifierId {
    fn from(value: u64) -> Self {
        QuantifierId(value)
    }
}

impl From<u64> for BoxId {
    fn from(value: u64) -> Self {
        BoxId(value)
    }
}

pub type QuantifierSet = BTreeSet<QuantifierId>;

/// A Query Graph Model instance represents a SQL query.
/// See [the design doc](https://github.com/MaterializeInc/materialize/blob/main/doc/developer/design/20210707_qgm_sql_high_level_representation.md) for details.
///
/// In this representation, SQL queries are represented as a graph of operators,
/// represented as boxes, that are connected via quantifiers. A top-level box
/// represents the entry point of the query.
///
/// Each non-leaf box has a set of quantifiers, which are the inputs of the
/// operation it represents. The quantifier adds information about how the
/// relation represented by its input box is consumed by the parent box.
#[derive(Debug)]
pub struct Model {
    /// The ID of the box representing the entry-point of the query.
    pub top_box: BoxId,
    /// All boxes in the query graph model.
    boxes: HashMap<BoxId, Box<RefCell<QueryBox>>>,
    /// Used for assigning unique IDs to query boxes.
    box_id_gen: Gen<BoxId>,
    /// All quantifiers in the query graph model.
    quantifiers: HashMap<QuantifierId, Box<RefCell<Quantifier>>>,
    /// Used for assigning unique IDs to quantifiers.
    quantifier_id_gen: Gen<QuantifierId>,
}

/// A semantic operator within a Query Graph.
#[derive(Debug)]
pub struct QueryBox {
    /// uniquely identifies the box within the model
    pub id: BoxId,
    /// the type of the box
    pub box_type: BoxType,
    /// the projection of the box
    pub columns: Vec<Column>,
    /// the input quantifiers of the box
    pub quantifiers: QuantifierSet,
    /// quantifiers ranging over this box
    pub ranging_quantifiers: QuantifierSet,
    /// list of unique keys exposed by this box. Each unique key is made by
    /// a list of column positions. Must be re-computed every time the box
    /// is modified.
    pub unique_keys: Vec<Vec<usize>>,
    /// whether this box must enforce the uniqueness of its output, it is
    /// guaranteed by structure of the box or it must preserve duplicated
    /// rows from its input boxes. See [DistinctOperation].
    pub distinct: DistinctOperation,
}

/// A column projected by a `QueryBox`.
#[derive(Debug)]
pub struct Column {
    pub expr: BoxScalarExpr,
    pub alias: Option<Ident>,
}

/// Enum that describes the DISTINCT property of a `QueryBox`.
#[derive(Debug, PartialEq, Eq)]
pub enum DistinctOperation {
    /// Distinctness of the output of the box must be enforced by
    /// the box.
    Enforce,
    /// Distinctness of the output of the box is required, but
    /// guaranteed by the structure of the box.
    Guaranteed,
    /// Distinctness of the output of the box is not required.
    Preserve,
}

pub use sql_parser::ast::Ident;

#[derive(Debug)]
pub struct Quantifier {
    /// uniquely identifiers the quantifier within the model
    pub id: QuantifierId,
    /// the type of the quantifier
    pub quantifier_type: QuantifierType,
    /// the input box of this quantifier
    pub input_box: BoxId,
    /// the box that owns this quantifier
    pub parent_box: BoxId,
    /// alias for name resolution purposes
    pub alias: Option<Ident>,
}

#[derive(Debug, Eq, PartialEq)]
pub enum QuantifierType {
    /// An ALL subquery.
    All,
    /// An existential subquery (IN SELECT/EXISTS/ANY).
    Existential,
    /// A regular join operand where each row from its input
    /// box must be consumed by the parent box operator.
    Foreach,
    /// The preserving side of an outer join. Only valid in
    /// OuterJoin boxes.
    PreservedForeach,
    /// A scalar subquery that produces one row at most.
    Scalar,
}

#[derive(Debug)]
pub enum BoxType {
    /// A table from the catalog.
    Get(Get),
    /// SQL's except operator
    Except,
    /// GROUP BY operator.
    Grouping(Grouping),
    /// SQL's intersect operator
    Intersect,
    /// OUTER JOIN operator. Contains one preserving quantifier
    /// at most: exactly one for LEFT/RIGHT OUTER JOIN, none
    /// for FULL OUTER JOIN.
    OuterJoin(OuterJoin),
    /// An operator that performs join, filter and project in
    /// that order.
    Select(Select),
    /// The invocation of table function from the catalog.
    TableFunction(TableFunction),
    /// SQL's union operator
    Union,
    /// Operator that produces a set of rows, with potentially
    /// correlated values.
    Values(Values),
}

#[derive(Debug)]
pub struct Get {
    id: expr::GlobalId,
}

/// The content of a Grouping box.
#[derive(Debug, Default)]
pub struct Grouping {
    pub key: Vec<BoxScalarExpr>,
}

/// The content of a OuterJoin box.
#[derive(Debug, Default)]
pub struct OuterJoin {
    /// The predices in the ON clause of the outer join.
    pub predicates: Vec<BoxScalarExpr>,
}

/// The content of a Select box.
#[derive(Debug, Default)]
pub struct Select {
    /// The list of predicates applied by the box.
    pub predicates: Vec<BoxScalarExpr>,
    /// An optional ORDER BY key
    pub order_key: Option<Vec<BoxScalarExpr>>,
    /// An optional LIMIT clause
    pub limit: Option<BoxScalarExpr>,
    /// An optional OFFSET clause
    pub offset: Option<BoxScalarExpr>,
}

#[derive(Debug, Default)]
pub struct TableFunction {
    pub parameters: Vec<BoxScalarExpr>,
    // @todo function metadata from the catalog
}

#[derive(Debug, Default)]
pub struct Values {
    pub rows: Vec<Vec<BoxScalarExpr>>,
}

impl Model {
    fn new() -> Self {
        Self {
            top_box: BoxId(0),
            boxes: HashMap::new(),
            box_id_gen: Default::default(),
            quantifiers: HashMap::new(),
            quantifier_id_gen: Default::default(),
        }
    }

    fn make_box(&mut self, box_type: BoxType) -> BoxId {
        let id = self.box_id_gen.allocate_id();
        let b = Box::new(RefCell::new(QueryBox {
            id,
            box_type,
            columns: Vec::new(),
            quantifiers: QuantifierSet::new(),
            ranging_quantifiers: QuantifierSet::new(),
            unique_keys: Vec::new(),
            distinct: DistinctOperation::Preserve,
        }));
        self.boxes.insert(id, b);
        id
    }

    fn make_select_box(&mut self) -> BoxId {
        self.make_box(BoxType::Select(Select::default()))
    }

    /// Get an immutable reference to the box identified by `box_id`.
    fn get_box(&self, box_id: BoxId) -> Ref<'_, QueryBox> {
        self.boxes
            .get(&box_id)
            .expect("a valid box identifier")
            .borrow()
    }

    /// Get a mutable reference to the box identified by `box_id`.
    fn get_mut_box(&self, box_id: BoxId) -> RefMut<'_, QueryBox> {
        self.boxes
            .get(&box_id)
            .expect("a valid box identifier")
            .borrow_mut()
    }

    /// Create a new quantifier and adds it to the parent box
    fn make_quantifier(
        &mut self,
        quantifier_type: QuantifierType,
        input_box: BoxId,
        parent_box: BoxId,
    ) -> QuantifierId {
        let id = self.quantifier_id_gen.allocate_id();
        let q = Box::new(RefCell::new(Quantifier {
            id,
            quantifier_type,
            input_box,
            parent_box,
            alias: None,
        }));
        self.quantifiers.insert(id, q);
        self.get_mut_box(parent_box).quantifiers.insert(id);
        self.get_mut_box(input_box).ranging_quantifiers.insert(id);
        id
    }

    /// Get an immutable reference to the box identified by `box_id`.
    fn get_quantifier(&self, quantifier_id: QuantifierId) -> Ref<'_, Quantifier> {
        self.quantifiers
            .get(&quantifier_id)
            .expect("a valid quantifier identifier")
            .borrow()
    }

    /// Get a mutable reference to the box identified by `box_id`.
    #[allow(dead_code)]
    fn get_mut_quantifier(&self, quantifier_id: QuantifierId) -> RefMut<'_, Quantifier> {
        self.quantifiers
            .get(&quantifier_id)
            .expect("a valid quantifier identifier")
            .borrow_mut()
    }

    /// Visit boxes in the query graph in pre-order starting from `self.top_box`.
    fn visit_pre_boxes<'a, F, E>(&'a self, f: &mut F) -> Result<(), E>
    where
        F: FnMut(Ref<'a, QueryBox>) -> Result<(), E>,
    {
        self.visit_pre_boxes_in_subgraph(f, self.top_box)
    }

    /// Visit boxes in the query graph in pre-order
    fn visit_pre_boxes_in_subgraph<'a, F, E>(&'a self, f: &mut F, start_box: BoxId) -> Result<(), E>
    where
        F: FnMut(Ref<'a, QueryBox>) -> Result<(), E>,
    {
        let mut visited = HashSet::new();
        let mut stack = vec![start_box];
        while !stack.is_empty() {
            let box_id = stack.pop().unwrap();
            if visited.insert(box_id) {
                let query_box = self.boxes.get(&box_id).expect("a valid box identifier");
                f(query_box.borrow())?;

                stack.extend(
                    query_box
                        .borrow()
                        .quantifiers
                        .iter()
                        .rev()
                        .map(|q| self.get_quantifier(*q).input_box),
                );
            }
        }
        Ok(())
    }

    /// Removes unreferenced objects from the model. May be invoked
    /// several times during query rewrites.
    #[allow(dead_code)]
    fn garbage_collect(&mut self) {
        let mut visited_boxes = HashSet::new();
        let mut visited_quantifiers: HashSet<QuantifierId> = HashSet::new();

        let _ = self.visit_pre_boxes(&mut |b| -> Result<(), ()> {
            visited_boxes.insert(b.id);
            visited_quantifiers.extend(b.quantifiers.iter());
            Ok(())
        });
        self.boxes.retain(|b, _| visited_boxes.contains(b));
        self.quantifiers
            .retain(|q, _| visited_quantifiers.contains(q));
    }
}

impl QueryBox {
    /// Add all columns from the non-subquery input quantifiers of the box to the
    /// projection of the box.
    fn add_all_input_columns(&mut self, model: &Model) {
        for quantifier_id in self.quantifiers.iter() {
            let q = model.get_quantifier(*quantifier_id);
            if !q.quantifier_type.is_subquery() {
                let input_box = model.get_box(q.input_box);
                for (position, c) in input_box.columns.iter().enumerate() {
                    let expr = BoxScalarExpr::ColumnReference(ColumnReference {
                        quantifier_id: *quantifier_id,
                        position,
                    });
                    self.columns.push(Column {
                        expr,
                        alias: c.alias.clone(),
                    });
                }
            }
        }
    }

    /// Append the given expression as a new column without an explicit alias in
    /// the projection of the box.
    fn add_column(&mut self, expr: BoxScalarExpr) -> usize {
        let position = self.columns.len();
        self.columns.push(Column { expr, alias: None });
        position
    }

    /// Append the given expression as a new column in the projection of the box
    /// if there isn't already a column with the same expression. Returns the
    /// position of the first column in the projection with the same expression.
    fn add_column_if_not_exists(&mut self, expr: BoxScalarExpr) -> usize {
        if let Some(position) = self.columns.iter().position(|c| c.expr == expr) {
            position
        } else {
            self.add_column(expr)
        }
    }

    /// Returns the type of a given column projected by this box.
    fn column_type(&self, model: &Model, column: usize) -> ColumnType {
        let the_expr = &self.columns[column].expr;
        let mut column_type = the_expr.typ(model);
        match &self.box_type {
            BoxType::Select(select) => {
                if column_type.nullable {
                    // Check whether the box contains predicates that reject
                    // nulls for all the non null requeriments of the expression.
                    let mut non_null_requirements = HashSet::new();
                    the_expr.non_null_requirements(&mut non_null_requirements);
                    let mut non_null_columns = HashSet::new();
                    for predicate in select.predicates.iter() {
                        // Deal with unnormalized Select boxes where conjunctive
                        // predicates are not represented as different predicates.
                        if let BoxScalarExpr::CallBinary {
                            func: BinaryFunc::And,
                            expr1,
                            expr2,
                        } = predicate
                        {
                            // AND is not a null propagating operation in SQL since
                            // NULL AND FALSE is FALSE, not NULL. However, in order
                            // for the AND operation to be TRUE, both sides must be
                            // non-null.
                            expr1.non_null_requirements(&mut non_null_columns);
                            expr2.non_null_requirements(&mut non_null_columns);
                        } else {
                            predicate.non_null_requirements(&mut non_null_columns);
                        }
                    }
                    // If all the non-null requirements are met, either because
                    // there is a null rejecting predicate on the input column or
                    // the input column itself is non-nullable, then the projected
                    // expression is non-nullable.
                    if non_null_requirements
                        .iter()
                        .all(|col| non_null_columns.contains(col) || !col.typ(model).nullable)
                    {
                        column_type.nullable = false;
                    }
                }
            }
            BoxType::OuterJoin(_) => {
                if !column_type.nullable {
                    // The expression becomes nullable if it references any column
                    // from the non-preserving side of the outer join
                    let mut non_null_requirements = HashSet::new();
                    the_expr.non_null_requirements(&mut non_null_requirements);
                    column_type.nullable = non_null_requirements.iter().any(|col| {
                        model.get_quantifier(col.quantifier_id).quantifier_type
                            == QuantifierType::Foreach
                    });
                }
            }
            BoxType::Union => {
                // TODO columns in the projection of a Union box always refer to
                // the first input quantifier, but the projected column is nullable
                // if any of the inputs has a nullable column in that position
                unreachable!()
            }
            _ => (),
        }
        column_type
    }

    /// Visit all the expressions in this query box.
    fn visit_expressions<F, E>(&self, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&BoxScalarExpr) -> Result<(), E>,
    {
        for c in self.columns.iter() {
            f(&c.expr)?;
        }
        match &self.box_type {
            BoxType::Select(select) => {
                for p in select.predicates.iter() {
                    f(p)?;
                }
                if let Some(order_key) = &select.order_key {
                    for p in order_key.iter() {
                        f(p)?;
                    }
                }
                if let Some(limit) = &select.limit {
                    f(limit)?;
                }
                if let Some(offset) = &select.offset {
                    f(offset)?;
                }
            }
            BoxType::OuterJoin(outer_join) => {
                for p in outer_join.predicates.iter() {
                    f(p)?;
                }
            }
            BoxType::Grouping(grouping) => {
                for p in grouping.key.iter() {
                    f(p)?;
                }
            }
            BoxType::Values(values) => {
                for row in values.rows.iter() {
                    for value in row.iter() {
                        f(value)?;
                    }
                }
            }
            BoxType::TableFunction(table_function) => {
                for p in table_function.parameters.iter() {
                    f(p)?;
                }
            }
            BoxType::Except | BoxType::Union | BoxType::Intersect | BoxType::Get(_) => {}
        }
        Ok(())
    }

    fn is_select(&self) -> bool {
        matches!(self.box_type, BoxType::Select(_))
    }

    /// Correlation information of the quantifiers in this box. Returns a map
    /// containing, for each quantifier, the column references from sibling
    /// quantifiers they are correlated with.
    fn correlation_info(&self, model: &Model) -> BTreeMap<QuantifierId, HashSet<ColumnReference>> {
        let mut correlation_info = BTreeMap::new();
        for q_id in self.quantifiers.iter() {
            // collect the column references from the current context within
            // the subgraph under the current quantifier
            let mut column_refs = HashSet::new();
            let mut f = |inner_box: Ref<'_, QueryBox>| -> Result<(), ()> {
                inner_box.visit_expressions(&mut |expr: &BoxScalarExpr| -> Result<(), ()> {
                    expr.collect_column_references_from_context(
                        &self.quantifiers,
                        &mut column_refs,
                    );
                    Ok(())
                })
            };
            let q = model.get_quantifier(*q_id);
            model
                .visit_pre_boxes_in_subgraph(&mut f, q.input_box)
                .unwrap();
            if !column_refs.is_empty() {
                correlation_info.insert(*q_id, column_refs);
            }
        }
        correlation_info
    }
}

impl BoxType {
    pub fn get_box_type_str(&self) -> &'static str {
        match self {
            BoxType::Except => "Except",
            BoxType::Get(..) => "Get",
            BoxType::Grouping(..) => "Grouping",
            BoxType::Intersect => "Intersect",
            BoxType::OuterJoin(..) => "OuterJoin",
            BoxType::Select(..) => "Select",
            BoxType::TableFunction(..) => "TableFunction",
            BoxType::Union => "Union",
            BoxType::Values(..) => "Values",
        }
    }
}

impl QuantifierType {
    fn is_subquery(&self) -> bool {
        match self {
            QuantifierType::All | QuantifierType::Existential | QuantifierType::Scalar => true,
            _ => false,
        }
    }
}

impl fmt::Display for QuantifierType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            QuantifierType::Foreach => write!(f, "F"),
            QuantifierType::PreservedForeach => write!(f, "P"),
            QuantifierType::Existential => write!(f, "E"),
            QuantifierType::All => write!(f, "A"),
            QuantifierType::Scalar => write!(f, "S"),
        }
    }
}
