// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use itertools::Itertools;
use std::cell::{Ref, RefCell, RefMut};
use std::collections::BTreeSet;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;

use ore::id_gen::Gen;

pub mod dot;
mod hir;
mod lowering;
pub mod rewrite_engine;
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
#[derive(Debug, Clone)]
pub struct Column {
    pub expr: BoxScalarExpr,
    pub alias: Option<Ident>,
}

/// Enum that describes the DISTINCT property of a `QueryBox`.
#[derive(Debug, PartialEq, Eq, Copy, Clone)]
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

#[derive(Debug, Eq, PartialEq, Clone)]
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

#[derive(Debug, Clone)]
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
    /// Windowing operator
    Windowing,
}

#[derive(Debug, Clone)]
pub struct Get {
    id: expr::GlobalId,
}

/// The content of a Grouping box.
#[derive(Debug, Default, Clone)]
pub struct Grouping {
    pub key: Vec<BoxScalarExpr>,
}

/// The content of a OuterJoin box.
#[derive(Debug, Default, Clone)]
pub struct OuterJoin {
    /// The predices in the ON clause of the outer join.
    pub predicates: Vec<BoxScalarExpr>,
}

/// The content of a Select box.
#[derive(Debug, Default, Clone)]
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

#[derive(Debug, Default, Clone)]
pub struct TableFunction {
    pub parameters: Vec<BoxScalarExpr>,
    // @todo function metadata from the catalog
}

#[derive(Debug, Default, Clone)]
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

    /// Performs a shallow clone of the box
    /// Note: correlated referenced are not updated, because that would require a deep clone.
    fn clone_box(&mut self, box_id: BoxId) -> BoxId {
        let new_id = self.box_id_gen.allocate_id();

        let (new_box, quantifiers_to_clone) = {
            let source_box = self.get_box(box_id);
            let b = Box::new(RefCell::new(QueryBox {
                id: new_id,
                box_type: source_box.box_type.clone(),
                columns: source_box.columns.clone(),
                quantifiers: QuantifierSet::new(),
                ranging_quantifiers: QuantifierSet::new(),
                unique_keys: source_box.unique_keys.clone(),
                distinct: source_box.distinct,
            }));
            (b, source_box.quantifiers.clone())
        };
        // Insert the box in order for `make_quantifier` to find it.
        self.boxes.insert(new_id, new_box);
        let cloned_quantifiers = quantifiers_to_clone
            .iter()
            .map(|q| {
                let (quantifier_type, input_box) = {
                    let q = self.get_quantifier(*q);
                    (q.quantifier_type.clone(), q.input_box)
                };
                self.make_quantifier(quantifier_type, input_box, new_id)
            })
            .collect::<QuantifierSet>();
        // Update any column reference within the cloned box to point to the
        // new quantifiers
        let quantifier_map = quantifiers_to_clone
            .iter()
            .zip(cloned_quantifiers.iter())
            .map(|(old, new)| (*old, *new))
            .collect::<HashMap<_, _>>();
        let mut cloned_box = self.get_mut_box(new_id);
        cloned_box.remap_column_references(&quantifier_map);
        cloned_box.quantifiers = cloned_quantifiers;
        new_id
    }

    fn swap_quantifiers(&mut self, b1: BoxId, b2: BoxId) {
        let mut b1_b = self.get_mut_box(b1);
        let mut b2_b = self.get_mut_box(b2);
        std::mem::swap(&mut b1_b.quantifiers, &mut b2_b.quantifiers);
        for q_id in b1_b.quantifiers.iter() {
            self.get_mut_quantifier(*q_id).parent_box = b1;
        }
        for q_id in b2_b.quantifiers.iter() {
            self.get_mut_quantifier(*q_id).parent_box = b2;
        }
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

    /// Sets the input box of the given quantifier
    fn update_input_box(&self, quantifier_id: QuantifierId, input_box: BoxId) {
        let old_input_box = {
            let mut q = self.get_mut_quantifier(quantifier_id);
            let old_input_box = q.input_box;
            q.input_box = input_box;
            old_input_box
        };

        self.get_mut_box(old_input_box)
            .ranging_quantifiers
            .remove(&quantifier_id);

        self.get_mut_box(input_box)
            .ranging_quantifiers
            .insert(quantifier_id);
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

    /// Visit boxes in the query graph in pre-order
    fn visit_pre_boxes_in_subgraph_mut<'a, F, E>(
        &'a self,
        f: &mut F,
        start_box: BoxId,
    ) -> Result<(), E>
    where
        F: FnMut(RefMut<'a, QueryBox>) -> Result<(), E>,
    {
        let mut visited = HashSet::new();
        let mut stack = vec![start_box];
        while !stack.is_empty() {
            let box_id = stack.pop().unwrap();
            if visited.insert(box_id) {
                let query_box = self.boxes.get(&box_id).expect("a valid box identifier");
                f(query_box.borrow_mut())?;

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

    /// Updates the IDs of all the objects in the graph
    fn update_ids(&mut self) {
        self.box_id_gen = Default::default();
        self.quantifier_id_gen = Default::default();

        let quantifier_map = self
            .quantifiers
            .iter()
            .map(|(q_id, _)| *q_id)
            .sorted()
            .map(|q_id| (q_id, self.quantifier_id_gen.allocate_id()))
            .collect::<HashMap<QuantifierId, QuantifierId>>();
        let box_map = self
            .boxes
            .iter()
            .map(|(box_id, _)| *box_id)
            .sorted()
            .map(|box_id| (box_id, self.box_id_gen.allocate_id()))
            .collect::<HashMap<BoxId, BoxId>>();

        self.quantifiers = self
            .quantifiers
            .drain()
            .map(|(q_id, q)| {
                let new_id = *quantifier_map.get(&q_id).unwrap();
                let mut b_q = q.borrow_mut();
                b_q.id = new_id;
                b_q.input_box = *box_map.get(&b_q.input_box).unwrap();
                drop(b_q);
                (new_id, q)
            })
            .collect();
        self.boxes = self
            .boxes
            .drain()
            .map(|(box_id, b)| {
                let new_id = *box_map.get(&box_id).unwrap();
                let mut b_b = b.borrow_mut();
                b_b.id = new_id;
                b_b.quantifiers = b_b
                    .quantifiers
                    .iter()
                    .map(|q_id| *quantifier_map.get(q_id).unwrap())
                    .collect();
                b_b.remap_column_references(&quantifier_map);
                drop(b_b);
                (new_id, b)
            })
            .collect();

        self.top_box = *box_map.get(&self.top_box).unwrap();
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
            BoxType::Windowing
            | BoxType::Except
            | BoxType::Union
            | BoxType::Intersect
            | BoxType::Get(_) => {}
        }
        Ok(())
    }

    /// Visit all the expressions in this query box.
    fn visit_expressions_mut<F, E>(&mut self, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&mut BoxScalarExpr) -> Result<(), E>,
    {
        for c in self.columns.iter_mut() {
            f(&mut c.expr)?;
        }
        match &mut self.box_type {
            BoxType::Select(select) => {
                for p in select.predicates.iter_mut() {
                    f(p)?;
                }
                if let Some(order_key) = &mut select.order_key {
                    for p in order_key.iter_mut() {
                        f(p)?;
                    }
                }
                if let Some(limit) = &mut select.limit {
                    f(limit)?;
                }
                if let Some(offset) = &mut select.offset {
                    f(offset)?;
                }
            }
            BoxType::OuterJoin(outer_join) => {
                for p in outer_join.predicates.iter_mut() {
                    f(p)?;
                }
            }
            BoxType::Grouping(grouping) => {
                for p in grouping.key.iter_mut() {
                    f(p)?;
                }
            }
            BoxType::Values(values) => {
                for row in values.rows.iter_mut() {
                    for value in row.iter_mut() {
                        f(value)?;
                    }
                }
            }
            BoxType::TableFunction(table_function) => {
                for p in table_function.parameters.iter_mut() {
                    f(p)?;
                }
            }
            BoxType::Windowing
            | BoxType::Except
            | BoxType::Union
            | BoxType::Intersect
            | BoxType::Get(_) => {}
        }
        Ok(())
    }

    fn remap_column_references(&mut self, quantifier_map: &HashMap<QuantifierId, QuantifierId>) {
        let _ = self.visit_expressions_mut(&mut |expr: &mut BoxScalarExpr| -> Result<(), ()> {
            expr.visit_mut(&mut |expr| {
                if let BoxScalarExpr::ColumnReference(c) = expr {
                    if let Some(new_quantifier) = quantifier_map.get(&c.quantifier_id) {
                        c.quantifier_id = *new_quantifier;
                    }
                }
            });
            Ok(())
        });
    }

    fn is_select(&self) -> bool {
        matches!(self.box_type, BoxType::Select(_))
    }

    /// Note: data sources can only contain BaseColumns in their projections.
    fn is_data_source(&self) -> bool {
        matches!(
            self.box_type,
            BoxType::Get(_) | BoxType::TableFunction(_) | BoxType::Values(_)
        )
    }

    fn add_predicate(&mut self, predicate: BoxScalarExpr) {
        match &mut self.box_type {
            BoxType::Select(select) => select.predicates.push(predicate),
            BoxType::OuterJoin(outer_join) => outer_join.predicates.push(predicate),
            _ => unreachable!(),
        }
    }

    fn get_predicates(&self) -> Option<&Vec<BoxScalarExpr>> {
        match &self.box_type {
            BoxType::Select(select) => Some(&select.predicates),
            BoxType::OuterJoin(outer_join) => Some(&outer_join.predicates),
            _ => None,
        }
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
            BoxType::Windowing => "Windowing",
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

impl Select {
    fn new() -> Self {
        Self {
            predicates: Vec::new(),
            order_key: None,
            limit: None,
            offset: None,
        }
    }
}
