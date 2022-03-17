// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The Query Graph Model.
//!
//! The public interface consists of the following items:
//! * [`Model`]
//! * [`BoxId`]
//! * [`QuantifierId`]
//! * [`DistinctOperation`]
//!
//! All other types are crate-private.

use super::super::attribute::core::Attributes;
use super::scalar::*;
use itertools::Itertools;
use mz_expr::TableFunc;
use mz_ore::id_gen::Gen;
use mz_sql_parser::ast::Ident;
use std::cell::{Ref, RefCell, RefMut};
use std::collections::BTreeSet;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::ops::{Deref, DerefMut};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QuantifierId(pub u64);
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BoxId(pub u64);

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

pub(crate) type QuantifierSet = BTreeSet<QuantifierId>;

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
#[derive(Debug, Default)]
pub struct Model {
    /// The ID of the box representing the entry-point of the query.
    pub(crate) top_box: BoxId,
    /// All boxes in the query graph model.
    boxes: HashMap<BoxId, Box<RefCell<QueryBox>>>,
    /// Used for assigning unique IDs to query boxes.
    box_id_gen: Gen<BoxId>,
    /// All quantifiers in the query graph model.
    quantifiers: HashMap<QuantifierId, Box<RefCell<Quantifier>>>,
    /// Used for assigning unique IDs to quantifiers.
    quantifier_id_gen: Gen<QuantifierId>,
}

/// A mutable reference to an object of type `T` (a [`QueryBox`] or a [`Quantifier`])
/// bound to a specific [`Model`].
#[derive(Debug)]
pub(crate) struct BoundRef<'a, T> {
    model: &'a Model,
    r#ref: Ref<'a, T>,
}

impl<T> Deref for BoundRef<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.r#ref.deref()
    }
}

/// A mutable reference to an object of type `T` (a [`QueryBox`] or a [`Quantifier`])
/// bound to a specific [`Model`].
#[derive(Debug)]
pub(crate) struct BoundRefMut<'a, T> {
    model: &'a mut Model,
    r#ref: RefMut<'a, T>,
}

impl<T> Deref for BoundRefMut<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.r#ref.deref()
    }
}

impl<T> DerefMut for BoundRefMut<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        self.r#ref.deref_mut()
    }
}

/// A semantic operator within a Query Graph.
#[derive(Debug)]
pub(crate) struct QueryBox {
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
    /// whether this box must enforce the uniqueness of its output, it is
    /// guaranteed by structure of the box or it must preserve duplicated
    /// rows from its input boxes. See [DistinctOperation].
    pub distinct: DistinctOperation,
    /// Derived attributes
    pub attributes: Attributes,
}

/// A column projected by a `QueryBox`.
#[derive(Debug)]
pub(crate) struct Column {
    pub expr: BoxScalarExpr,
    pub alias: Option<Ident>,
}

/// Enum that describes the DISTINCT property of a `QueryBox`.
#[derive(Debug, Eq, Hash, PartialEq)]
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

#[derive(Debug)]
pub(crate) struct Quantifier {
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

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
pub(crate) enum QuantifierType {
    /// An ALL subquery.
    All = 0b00000001,
    /// An existential subquery (IN SELECT/EXISTS/ANY).
    Existential = 0b00000010,
    /// A regular join operand where each row from its input
    /// box must be consumed by the parent box operator.
    Foreach = 0b00000100,
    /// The preserving side of an outer join. Only valid in
    /// OuterJoin boxes.
    PreservedForeach = 0b00001000,
    /// A scalar subquery that produces one row at most.
    Scalar = 0b00010000,
}

/// A bitmask that matches the discriminant of every possible [`QuantifierType`].
pub(crate) const ARBITRARY_QUANTIFIER: usize = 0b00000000
    | (QuantifierType::All as usize)
    | (QuantifierType::Existential as usize)
    | (QuantifierType::Foreach as usize)
    | (QuantifierType::PreservedForeach as usize)
    | (QuantifierType::Scalar as usize);

/// A bitmask that matches the discriminant of a subquery [`QuantifierType`].
pub(crate) const SUBQUERY_QUANTIFIER: usize = 0b00000000
    | (QuantifierType::All as usize)
    | (QuantifierType::Existential as usize)
    | (QuantifierType::Scalar as usize);

#[derive(Debug, Clone)]
pub(crate) enum BoxType {
    /// A table from the catalog.
    Get(Get),
    /// SQL's except operator
    #[allow(dead_code)]
    Except,
    /// GROUP BY operator.
    Grouping(Grouping),
    /// SQL's intersect operator
    #[allow(dead_code)]
    Intersect,
    /// OUTER JOIN operator. Contains one preserving quantifier
    /// at most: exactly one for LEFT/RIGHT OUTER JOIN, none
    /// for FULL OUTER JOIN.
    OuterJoin(OuterJoin),
    /// An operator that performs join, filter and project in
    /// that order.
    Select(Select),
    /// The invocation of table function from the catalog.
    CallTable(CallTable),
    /// SQL's union operator
    Union,
    /// Operator that produces a set of rows, with potentially
    /// correlated values.
    Values(Values),
}

#[derive(Debug, Clone)]
pub(crate) struct Get {
    pub id: mz_expr::GlobalId,
    pub unique_keys: Vec<Vec<usize>>,
}

impl From<Get> for BoxType {
    fn from(get: Get) -> Self {
        BoxType::Get(get)
    }
}

/// The content of a Grouping box.
#[derive(Debug, Default, Clone)]
pub(crate) struct Grouping {
    pub key: Vec<BoxScalarExpr>,
}

impl From<Grouping> for BoxType {
    fn from(grouping: Grouping) -> Self {
        BoxType::Grouping(grouping)
    }
}

/// The content of a OuterJoin box.
#[derive(Debug, Clone, Default)]
pub(crate) struct OuterJoin {
    /// The predices in the ON clause of the outer join.
    pub predicates: Vec<BoxScalarExpr>,
}

impl From<OuterJoin> for BoxType {
    fn from(outer_join: OuterJoin) -> Self {
        BoxType::OuterJoin(outer_join)
    }
}

/// The content of a Select box.
#[derive(Debug, Clone, Default)]
pub(crate) struct Select {
    /// The list of predicates applied by the box.
    pub predicates: Vec<BoxScalarExpr>,
    /// An optional ORDER BY key
    pub order_key: Option<Vec<BoxScalarExpr>>,
    /// An optional LIMIT clause
    pub limit: Option<BoxScalarExpr>,
    /// An optional OFFSET clause
    pub offset: Option<BoxScalarExpr>,
}

impl From<Select> for BoxType {
    fn from(select: Select) -> Self {
        BoxType::Select(select)
    }
}

impl Select {
    pub fn new(predicates: Vec<BoxScalarExpr>) -> Select {
        Select {
            predicates,
            order_key: None,
            limit: None,
            offset: None,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CallTable {
    pub func: TableFunc,
    pub exprs: Vec<BoxScalarExpr>,
}

impl CallTable {
    pub fn new(func: TableFunc, exprs: Vec<BoxScalarExpr>) -> CallTable {
        CallTable { func, exprs }
    }
}

impl From<CallTable> for BoxType {
    fn from(table_funct: CallTable) -> Self {
        BoxType::CallTable(table_funct)
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct Values {
    pub rows: Vec<Vec<BoxScalarExpr>>,
}

impl From<Values> for BoxType {
    fn from(values: Values) -> Self {
        BoxType::Values(values)
    }
}

impl Model {
    pub(crate) fn make_box(&mut self, box_type: BoxType) -> BoxId {
        let id = self.box_id_gen.allocate_id();
        let b = Box::new(RefCell::new(QueryBox {
            id,
            box_type,
            columns: Vec::new(),
            quantifiers: QuantifierSet::new(),
            ranging_quantifiers: QuantifierSet::new(),
            distinct: DistinctOperation::Preserve,
            attributes: Attributes::new(),
        }));
        self.boxes.insert(id, b);
        id
    }

    pub(crate) fn make_select_box(&mut self) -> BoxId {
        self.make_box(BoxType::Select(Select::default()))
    }

    /// An iterator over immutable references to the [`QueryBox`] instances in this [`Model`].
    pub(crate) fn boxes_iter(&self) -> impl Iterator<Item = BoundRef<'_, QueryBox>> {
        self.boxes.keys().map(|box_id| BoundRef {
            model: self,
            r#ref: self
                .boxes
                .get(&box_id)
                .expect("a valid box identifier")
                .borrow(),
        })
    }

    /// Get an immutable reference to the box identified by `box_id` bound to this [`Model`].
    pub(crate) fn get_box(&self, box_id: BoxId) -> BoundRef<'_, QueryBox> {
        BoundRef {
            model: self,
            r#ref: self
                .boxes
                .get(&box_id)
                .expect("a valid box identifier")
                .borrow(),
        }
    }

    /// Get a mutable reference to the box identified by `box_id` bound to this [`Model`].
    pub(crate) fn get_mut_box(&mut self, box_id: BoxId) -> BoundRefMut<'_, QueryBox> {
        let model_ptr = self as *mut Self;
        unsafe {
            let reference = (*model_ptr)
                .boxes
                .get(&box_id)
                .expect("a valid box identifier")
                .borrow_mut();

            BoundRefMut {
                model: &mut *model_ptr,
                r#ref: reference,
            }
        }
    }

    /// Create a new quantifier and adds it to the parent box
    pub(crate) fn make_quantifier(
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

    /// Get an immutable reference to the box identified by `box_id` bound to this [`Model`].
    pub(crate) fn get_quantifier<'a>(
        &'a self,
        quantifier_id: QuantifierId,
    ) -> BoundRef<'a, Quantifier> {
        BoundRef {
            model: self,
            r#ref: self
                .quantifiers
                .get(&quantifier_id)
                .expect("a valid quantifier identifier")
                .borrow(),
        }
    }

    /// Get a mutable reference to the box identified by `box_id` bound to this [`Model`].
    pub(crate) fn get_mut_quantifier<'a>(
        &'a mut self,
        quantifier_id: QuantifierId,
    ) -> BoundRefMut<'a, Quantifier> {
        let model_ptr = self as *mut Self;
        unsafe {
            let reference = (*model_ptr)
                .quantifiers
                .get(&quantifier_id)
                .expect("a valid quantifier identifier")
                .borrow_mut();

            BoundRefMut {
                model: &mut *model_ptr,
                r#ref: reference,
            }
        }
    }

    /// Traverse the entire graph using depth-first traversal.
    ///
    /// The function `pre` runs on a parent [`QueryBox`] before it runs on any
    /// of its children.
    /// The function `post` runs on all children [`QueryBox`]es before it runs
    /// on a parent.
    pub(crate) fn try_visit_pre_post<'a, F, G, E>(
        &'a self,
        pre: &mut F,
        post: &mut G,
    ) -> Result<(), E>
    where
        F: FnMut(&Model, &BoxId) -> Result<(), E>,
        G: FnMut(&Model, &BoxId) -> Result<(), E>,
    {
        self.try_visit_pre_post_descendants(pre, post, self.top_box)
    }

    /// Traverse a subgraph using depth-first traversal starting at `root`.
    ///
    /// The function `pre` runs on a parent [`QueryBox`] before it runs on any
    /// of its children.
    /// The function `post` runs on all children [`QueryBox`]es before it runs
    /// on a parent.
    pub(crate) fn try_visit_pre_post_descendants<'a, F, G, E>(
        &'a self,
        pre: &mut F,
        post: &mut G,
        root: BoxId,
    ) -> Result<(), E>
    where
        F: FnMut(&Model, &BoxId) -> Result<(), E>,
        G: FnMut(&Model, &BoxId) -> Result<(), E>,
    {
        mz_ore::graph::try_nonrecursive_dft(
            self,
            root,
            &mut |model, box_id| {
                pre(model, box_id)?;
                Ok(model
                    .get_box(*box_id)
                    .input_quantifiers()
                    .map(|q| q.input_box)
                    .collect())
            },
            post,
        )
    }

    /// Same as [`Model::try_visit_pre_post`], but permits mutating the model.
    pub(crate) fn try_visit_mut_pre_post<'a, F, G, E>(
        &'a mut self,
        pre: &mut F,
        post: &mut G,
    ) -> Result<(), E>
    where
        F: FnMut(&mut Model, &BoxId) -> Result<(), E>,
        G: FnMut(&mut Model, &BoxId) -> Result<(), E>,
    {
        self.try_visit_mut_pre_post_descendants(pre, post, self.top_box)
    }

    /// Same as [`Model::try_visit_pre_post_descendants`], but permits mutating the model.
    pub(crate) fn try_visit_mut_pre_post_descendants<'a, F, G, E>(
        &'a mut self,
        pre: &mut F,
        post: &mut G,
        root: BoxId,
    ) -> Result<(), E>
    where
        F: FnMut(&mut Model, &BoxId) -> Result<(), E>,
        G: FnMut(&mut Model, &BoxId) -> Result<(), E>,
    {
        mz_ore::graph::try_nonrecursive_dft_mut(
            self,
            root,
            &mut |model, box_id| {
                pre(model, box_id)?;
                Ok(model
                    .get_box(*box_id)
                    .input_quantifiers()
                    .map(|q| q.input_box)
                    .collect())
            },
            post,
        )
    }

    /// Removes unreferenced objects from the model.
    ///
    /// May be invoked several times during query rewrites.
    pub(crate) fn garbage_collect(&mut self) {
        let mut visited_boxes = HashSet::new();
        let mut visited_quantifiers: HashSet<QuantifierId> = HashSet::new();

        let _ = self.try_visit_pre_post(
            &mut |m, box_id| -> Result<(), ()> {
                visited_boxes.insert(*box_id);
                let b = m.get_box(*box_id);
                visited_quantifiers.extend(b.input_quantifiers().map(|q| q.id));
                Ok(())
            },
            &mut |_, _| Ok(()),
        );
        self.boxes.retain(|b, _| visited_boxes.contains(b));
        self.quantifiers
            .retain(|q, _| visited_quantifiers.contains(q));
    }

    /// Renumbers all the boxes and quantifiers in the model starting from 0.
    ///
    /// Intended to be called after [Model::garbage_collect].
    ///
    /// Renumbering the model does not save memory or improve the performance of
    /// traversing the model, but it does make the Dot graph easier to parse.
    pub(crate) fn update_ids(&mut self) {
        // Reset the id generators.
        self.box_id_gen = Default::default();
        self.quantifier_id_gen = Default::default();

        // Figure out new ids for each quantifier and box.
        let updated_quantifier_ids = self
            .quantifiers
            .keys()
            .sorted()
            .map(|q_id| (*q_id, self.quantifier_id_gen.allocate_id()))
            .collect::<HashMap<_, _>>();
        let updated_box_ids = self
            .boxes
            .keys()
            .sorted()
            .map(|box_id| (*box_id, self.box_id_gen.allocate_id()))
            .collect::<HashMap<_, _>>();

        // Change all ids to their new versions.
        self.quantifiers = self
            .quantifiers
            .drain()
            .map(|(q_id, q)| {
                let new_id = updated_quantifier_ids[&q_id];
                let mut b_q = q.borrow_mut();
                b_q.id = new_id;
                b_q.input_box = updated_box_ids[&b_q.input_box];
                b_q.parent_box = updated_box_ids[&b_q.parent_box];
                drop(b_q);
                (new_id, q)
            })
            .collect();
        self.boxes = self
            .boxes
            .drain()
            .map(|(box_id, b)| {
                let new_id = updated_box_ids[&box_id];
                let mut b_b = b.borrow_mut();
                b_b.id = new_id;
                b_b.quantifiers = b_b
                    .quantifiers
                    .iter()
                    .map(|q_id| updated_quantifier_ids[q_id])
                    .collect();
                b_b.ranging_quantifiers = b_b
                    .ranging_quantifiers
                    .iter()
                    .map(|q_id| updated_quantifier_ids[q_id])
                    .collect();
                b_b.update_column_quantifiers(&updated_quantifier_ids);
                drop(b_b);
                (new_id, b)
            })
            .collect();
        self.top_box = *updated_box_ids.get(&self.top_box).unwrap();
    }
}

impl QueryBox {
    /// Append the given expression as a new column without an explicit alias in
    /// the projection of the box.
    pub fn add_column(&mut self, expr: BoxScalarExpr) -> usize {
        let position = self.columns.len();
        self.columns.push(Column { expr, alias: None });
        position
    }

    /// Append the given expression as a new column in the projection of the box
    /// if there isn't already a column with the same expression. Returns the
    /// position of the first column in the projection with the same expression.
    pub fn add_column_if_not_exists(&mut self, expr: BoxScalarExpr) -> usize {
        if let Some(position) = self.columns.iter().position(|c| c.expr == expr) {
            position
        } else {
            self.add_column(expr)
        }
    }

    /// Returns a vector of [`ColumnReference`] if all column expressions projected
    /// by this [`QueryBox`] of that form.
    pub fn columns_as_refs(&self) -> Option<Vec<&ColumnReference>> {
        self.columns
            .iter()
            .map(|c| match &c.expr {
                BoxScalarExpr::ColumnReference(cr) => Some(cr),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()
    }

    /// Visit all the expressions in this query box.
    pub fn visit_expressions<F, E>(&self, f: &mut F) -> Result<(), E>
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
            BoxType::CallTable(table_function) => {
                for p in table_function.exprs.iter() {
                    f(p)?;
                }
            }
            BoxType::Except | BoxType::Union | BoxType::Intersect | BoxType::Get(_) => {}
        }
        Ok(())
    }

    /// Mutably visit all the expressions in this query box.
    pub fn visit_expressions_mut<F, E>(&mut self, f: &mut F) -> Result<(), E>
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
            BoxType::CallTable(table_function) => {
                for p in table_function.exprs.iter_mut() {
                    f(p)?;
                }
            }
            BoxType::Except | BoxType::Union | BoxType::Intersect | BoxType::Get(_) => {}
        }
        Ok(())
    }

    pub fn is_select(&self) -> bool {
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
            let mut f = |m: &Model, box_id: &BoxId| -> Result<(), ()> {
                let inner_box = m.get_box(*box_id);
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
                .try_visit_pre_post_descendants(&mut f, &mut |_, _| Ok(()), q.input_box)
                .unwrap();
            if !column_refs.is_empty() {
                correlation_info.insert(*q_id, column_refs);
            }
        }
        correlation_info
    }

    /// For every expression in this box, update the quantifier ids of every
    /// referenced column.
    ///
    /// `updated_quantifier_ids` should be a map of the old [QuantifierId] to
    /// the new one.
    pub fn update_column_quantifiers(
        &mut self,
        updated_quantifier_ids: &HashMap<QuantifierId, QuantifierId>,
    ) {
        // TODO: handle errors.
        self.visit_expressions_mut(&mut |expr: &mut BoxScalarExpr| -> Result<(), ()> {
            expr.visit_mut_post(&mut |expr| {
                if let BoxScalarExpr::ColumnReference(c) = expr {
                    if let Some(new_quantifier) = updated_quantifier_ids.get(&c.quantifier_id) {
                        c.quantifier_id = *new_quantifier;
                    }
                }
            });
            Ok(())
        })
        .unwrap();
    }
}

/// [`Model`]-dependent methods.
///
/// Publicly visible delegates to these methods are defined for
/// `BoundRef<'_, QueryBox>` and `BoundRefMut<'_, QueryBox>`.
impl QueryBox {
    /// Resolve the input quantifiers of this box as an iterator of immutable
    /// bound references.
    fn input_quantifiers<'a>(
        &'a self,
        model: &'a Model,
    ) -> impl Iterator<Item = BoundRef<'a, Quantifier>> {
        self.quantifiers
            .iter()
            .map(|q_id| model.get_quantifier(*q_id))
    }

    /// Resolve the quantifiers ranging over this box as an iterator of immutable
    /// bound references.
    fn ranging_quantifiers<'a>(
        &'a self,
        model: &'a Model,
    ) -> impl Iterator<Item = BoundRef<'a, Quantifier>> {
        self.ranging_quantifiers
            .iter()
            .map(|q_id| model.get_quantifier(*q_id))
    }
}

/// Immutable [`QueryBox`] methods that depend on their enclosing [`Model`].
impl<'a> BoundRef<'a, QueryBox> {
    /// Delegate to `QueryBox::input_quantifiers` with the enclosing model.
    pub fn input_quantifiers(&self) -> impl Iterator<Item = BoundRef<'_, Quantifier>> {
        self.deref().input_quantifiers(self.model)
    }

    /// Delegate to `QueryBox::ranging_quantifiers` with the enclosing model.
    pub fn ranging_quantifiers(&self) -> impl Iterator<Item = BoundRef<'_, Quantifier>> {
        self.deref().ranging_quantifiers(self.model)
    }

    pub fn correlation_info(&self) -> BTreeMap<QuantifierId, HashSet<ColumnReference>> {
        self.deref().correlation_info(self.model)
    }
}

/// Mutable [`QueryBox`] methods that depend on their enclosing [`Model`].
impl<'a> BoundRefMut<'a, QueryBox> {
    /// Add all columns from the non-subquery input quantifiers of the box to the
    /// projection of the box.
    pub fn add_all_input_columns(&mut self) {
        let mut all_input_columns = vec![];
        for quantifier_id in self.quantifiers.iter() {
            let q = self.model.get_quantifier(*quantifier_id);
            if !q.quantifier_type.is_subquery() {
                let input_box = self.model.get_box(q.input_box);
                for (position, c) in input_box.columns.iter().enumerate() {
                    let expr = BoxScalarExpr::ColumnReference(ColumnReference {
                        quantifier_id: *quantifier_id,
                        position,
                    });
                    all_input_columns.push(Column {
                        expr,
                        alias: c.alias.clone(),
                    });
                }
            }
        }
        self.columns.append(&mut all_input_columns);
    }

    /// Delegate to `QueryBox::input_quantifiers` with the enclosing model.
    #[allow(dead_code)]
    pub fn input_quantifiers(&self) -> impl Iterator<Item = BoundRef<'_, Quantifier>> {
        self.deref().input_quantifiers(self.model)
    }

    #[allow(dead_code)]
    /// Delegate to `QueryBox::ranging_quantifiers` with the enclosing model.
    pub fn ranging_quantifiers(&self) -> impl Iterator<Item = BoundRef<'_, Quantifier>> {
        self.deref().ranging_quantifiers(self.model)
    }
}

/// Immutable [`Quantifier`] methods that depend on their enclosing [`Model`].
#[allow(dead_code)]
impl<'a> BoundRef<'a, Quantifier> {
    /// Resolve a bound reference to the input box of this quantifier.
    pub fn input_box(&'a self) -> BoundRef<'a, QueryBox> {
        self.model.get_box(self.input_box)
    }

    /// Resolve a bound reference to the parent box of this quantifier.
    pub fn parent_box(&self) -> BoundRef<'_, QueryBox> {
        self.model.get_box(self.parent_box)
    }
}

/// Mutable [`Quantifier`] methods that depend on their enclosing [`Model`].
#[allow(dead_code)]
impl<'a> BoundRefMut<'a, Quantifier> {
    /// Resolve a bound reference to the input box of this quantifier.
    pub fn input_box(&self) -> BoundRef<'_, QueryBox> {
        self.model.get_box(self.input_box)
    }

    /// Resolve a bound reference to the input box of this quantifier.
    pub fn input_box_mut(&mut self) -> BoundRefMut<'_, QueryBox> {
        self.model.get_mut_box(self.input_box)
    }

    /// Resolve a bound reference to the parent box of this quantifier.
    pub fn parent_box(&self) -> BoundRef<'_, QueryBox> {
        self.model.get_box(self.parent_box)
    }

    /// Resolve a bound reference to the parent box of this quantifier.
    pub fn parent_box_mut(&mut self) -> BoundRefMut<'_, QueryBox> {
        self.model.get_mut_box(self.parent_box)
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
            BoxType::CallTable(..) => "CallTable",
            BoxType::Union => "Union",
            BoxType::Values(..) => "Values",
        }
    }
}

impl QuantifierType {
    pub fn is_subquery(&self) -> bool {
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

/// Exposes the internals of [`Model`]. Only to be used by tests.
#[cfg(test)]
pub(crate) mod model_test_util {
    use crate::query_model::model::Quantifier;
    use crate::query_model::Model;
    use std::cell::RefCell;

    impl Model {
        /// Return an iterator over all quantifiers in the model
        pub(crate) fn quantifiers(&self) -> impl Iterator<Item = &Box<RefCell<Quantifier>>> {
            self.quantifiers.values()
        }
    }
}
