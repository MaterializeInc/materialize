// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;

use ore::id_gen::IdGen;

mod dot;
mod hir;
mod lowering;
mod scalar_expr;
#[cfg(test)]
mod test;

pub use scalar_expr::*;

pub type QuantifierId = u64;
pub type BoxId = u64;
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
    box_id_gen: IdGen,
    /// All quantifiers in the query graph model.
    quantifiers: HashMap<QuantifierId, Box<RefCell<Quantifier>>>,
    /// Used for assigning unique IDs to quantifiers.
    quantifier_id_gen: IdGen,
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
    BaseTable(BaseTable),
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
pub struct BaseTable {/* @todo table metadata from the catalog */}

/// The content of a Grouping box.
#[derive(Debug, Default)]
pub struct Grouping {
    pub key: Vec<Box<BoxScalarExpr>>,
}

/// The content of a OuterJoin box.
#[derive(Debug, Default)]
pub struct OuterJoin {
    /// The predices in the ON clause of the outer join.
    pub predicates: Vec<Box<BoxScalarExpr>>,
}

/// The content of a Select box.
#[derive(Debug, Default)]
pub struct Select {
    /// The list of predicates applied by the box.
    pub predicates: Vec<Box<BoxScalarExpr>>,
    /// An optional ORDER BY key
    pub order_key: Option<Vec<Box<BoxScalarExpr>>>,
    /// An optional LIMIT clause
    pub limit: Option<BoxScalarExpr>,
    /// An optional OFFSET clause
    pub offset: Option<BoxScalarExpr>,
}

#[derive(Debug, Default)]
pub struct TableFunction {
    pub parameters: Vec<Box<BoxScalarExpr>>,
    // @todo function metadata from the catalog
}

#[derive(Debug, Default)]
pub struct Values {
    pub rows: Vec<Vec<Box<BoxScalarExpr>>>,
}

impl Model {
    fn new() -> Self {
        Self {
            top_box: 0,
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
            distinct: DistinctOperation::Preserve,
        }));
        self.boxes.insert(id, b);
        id
    }

    fn make_select_box(&mut self) -> BoxId {
        self.make_box(BoxType::Select(Select::default()))
    }

    fn get_box(&self, box_id: BoxId) -> &RefCell<QueryBox> {
        self.boxes.get(&box_id).expect("a valid box identifier")
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
        self.get_box(parent_box).borrow_mut().quantifiers.insert(id);
        self.get_box(input_box)
            .borrow_mut()
            .ranging_quantifiers
            .insert(id);
        id
    }

    fn get_quantifier(&self, quantifier_id: QuantifierId) -> &RefCell<Quantifier> {
        self.quantifiers
            .get(&quantifier_id)
            .expect("a valid quantifier identifier")
    }

    /// Visit boxes in the query graph in pre-order starting from `self.top_box`.
    fn visit_pre_boxes<F, E>(&self, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&RefCell<QueryBox>) -> Result<(), E>,
    {
        self.visit_pre_boxes_in_subgraph(f, self.top_box)
    }

    /// Visit boxes in the query graph in pre-order
    fn visit_pre_boxes_in_subgraph<F, E>(&self, f: &mut F, start_box: BoxId) -> Result<(), E>
    where
        F: FnMut(&RefCell<QueryBox>) -> Result<(), E>,
    {
        let mut visited = HashSet::new();
        let mut stack = vec![start_box];
        while !stack.is_empty() {
            let box_id = stack.pop().unwrap();
            if visited.insert(box_id) {
                let query_box = self.get_box(box_id);
                f(query_box)?;

                stack.extend(
                    query_box
                        .borrow()
                        .quantifiers
                        .iter()
                        .rev()
                        .map(|q| self.get_quantifier(*q).borrow().input_box),
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
            let b = b.borrow();
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
            let bq = q.borrow();
            if !bq.quantifier_type.is_subquery() {
                let input_box = model.get_box(bq.input_box).borrow();
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
            BoxType::Except | BoxType::Union | BoxType::Intersect | BoxType::BaseTable(_) => {}
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
            let mut f = |inner_box: &RefCell<QueryBox>| -> Result<(), ()> {
                inner_box.borrow().visit_expressions(
                    &mut |expr: &BoxScalarExpr| -> Result<(), ()> {
                        expr.collect_column_references_from_context(
                            &self.quantifiers,
                            &mut column_refs,
                        );
                        Ok(())
                    },
                )
            };
            let q = model.get_quantifier(*q_id).borrow();
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
            BoxType::BaseTable(..) => "BaseTable",
            BoxType::Except => "Except",
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
