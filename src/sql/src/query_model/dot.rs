// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Generates a graphviz graph from a Query Graph Model.

use crate::query_model::{BoxScalarExpr, BoxType, Model, Quantifier, QuantifierId, QueryBox};
use itertools::Itertools;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashSet};

/// Generates a graphviz graph from a Query Graph Model, defined in the DOT language.
/// See <https://graphviz.org/doc/info/lang.html>.
pub(crate) struct DotGenerator {
    output: String,
    indent: u32,
}

impl DotGenerator {
    pub fn new() -> Self {
        Self {
            output: String::new(),
            indent: 0,
        }
    }

    /// Generates a graphviz graph for the given model, labeled with `label`.
    pub fn generate(mut self, model: &Model, label: &str) -> Result<String, anyhow::Error> {
        self.new_line("digraph G {");
        self.inc();
        self.new_line("compound = true");
        self.new_line("labeljust = l");
        self.new_line(&format!("label=\"{}\"", label.trim()));
        self.new_line("node [ shape = box ]");

        // list of quantifiers for adding the edges connecting them to their
        // input boxes after all boxes have been processed
        let mut quantifiers = Vec::new();

        model
            .visit_pre_boxes(&mut |b: &RefCell<QueryBox>| -> Result<(), ()> {
                let b = b.borrow();
                let box_id = b.id;

                self.new_line(&format!("subgraph cluster{} {{", box_id));
                self.inc();
                self.new_line(&format!(
                    "label = \"Box{}:{}\"",
                    box_id,
                    Self::get_box_title(&b)
                ));
                self.new_line(&format!(
                    "boxhead{} [ shape = record, label=\"{{ {} }}\" ]",
                    box_id,
                    Self::get_box_head(&b)
                ));

                self.new_line("{");
                self.inc();
                self.new_line("rank = same");

                if b.quantifiers.len() > 0 {
                    self.new_line("node [ shape = circle ]");
                }

                for q_id in b.quantifiers.iter() {
                    quantifiers.push(*q_id);

                    let q = model.get_quantifier(*q_id).borrow();
                    self.new_line(&format!(
                        "Q{0} [ label=\"Q{0}({1}){2}\" ]",
                        q_id,
                        q.quantifier_type,
                        Self::get_quantifier_alias(&q)
                    ));
                }

                self.add_correlation_info(model, &b);

                self.dec();
                self.new_line("}");
                self.dec();
                self.new_line("}");

                Ok(())
            })
            .unwrap();

        if quantifiers.len() > 0 {
            self.new_line("edge [ arrowhead = none, style = dashed ]");
            for q_id in quantifiers.iter() {
                let q = model.get_quantifier(*q_id).borrow();
                self.new_line(&format!(
                    "Q{0} -> boxhead{1} [ lhead = cluster{1} ]",
                    q_id, q.input_box
                ));
            }
        }

        self.dec();
        self.new_line("}");
        self.new_line(""); // final empty line
        Ok(self.output)
    }

    fn get_box_title(b: &QueryBox) -> &'static str {
        b.box_type.get_box_type_str()
    }

    fn get_box_head(b: &QueryBox) -> String {
        let mut r = String::new();

        r.push_str(&format!("Distinct: {:?}", b.distinct));

        // The projection of the box
        for (i, c) in b.columns.iter().enumerate() {
            r.push_str(&format!("| {}: {}", i, c.expr));
            if let Some(c) = &c.alias {
                r.push_str(&format!(" as {}", c.as_str()));
            }
        }

        // Per-type internal properties.
        match &b.box_type {
            BoxType::Select(select) => {
                if let Some(order_key) = &select.order_key {
                    r.push_str("| ORDER BY: ");
                    for (i, key_item) in order_key.iter().enumerate() {
                        if i > 0 {
                            r.push_str(", ");
                        }
                        // @todo direction
                        r.push_str(&format!("{}", key_item));
                    }
                }
            }
            BoxType::Grouping(grouping) => {
                if !grouping.key.is_empty() {
                    r.push_str("| GROUP BY: ");
                    for (i, key_item) in grouping.key.iter().enumerate() {
                        if i > 0 {
                            r.push_str(", ");
                        }
                        r.push_str(&format!("{}", key_item));
                    }
                }
            }
            BoxType::Values(values) => {
                for (i, row) in values.rows.iter().enumerate() {
                    r.push_str(&format!("| ROW {}: ", i));
                    for (i, value) in row.iter().enumerate() {
                        if i > 0 {
                            r.push_str(", ");
                        }
                        r.push_str(&format!("{}", value));
                    }
                }
            }
            _ => {}
        }

        // @todo predicates as arrows
        if let Some(predicates) = match &b.box_type {
            BoxType::Select(select) => Some(&select.predicates),
            BoxType::OuterJoin(outer_join) => Some(&outer_join.predicates),
            _ => None,
        } {
            for predicate in predicates.iter() {
                r.push_str(&format!("| {}", predicate));
            }
        }

        r
    }

    /// Adds red arrows from correlated quantifiers to the sibling quantifiers they
    /// are correlated with.
    fn add_correlation_info(&mut self, model: &Model, b: &QueryBox) {
        let mut correlation_info: BTreeMap<QuantifierId, Vec<QuantifierId>> = BTreeMap::new();
        for q_id in b.quantifiers.iter() {
            // collect the column references from the current context within
            // the subgraph under the current quantifier
            let mut column_refs = HashSet::new();
            let mut f = |inner_box: &RefCell<QueryBox>| -> Result<(), ()> {
                inner_box.borrow().visit_expressions(
                    &mut |expr: &BoxScalarExpr| -> Result<(), ()> {
                        expr.collect_column_references_from_context(
                            &b.quantifiers,
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
            // collect the unique quantifiers referenced by the subgraph
            // under the current quantifier
            correlation_info.insert(
                q.id,
                column_refs
                    .iter()
                    .map(|c| c.quantifier_id)
                    .sorted()
                    .unique()
                    .collect::<Vec<_>>(),
            );
        }

        for (correlated_q, quantifiers) in correlation_info.iter() {
            for q in quantifiers.iter() {
                self.new_line(&format!(
                    "Q{0} -> Q{1} [ label = \"correlation\", style = filled, color = red ]",
                    correlated_q, q
                ));
            }
        }
    }

    fn get_quantifier_alias(q: &Quantifier) -> String {
        if let Some(alias) = &q.alias {
            format!(" as {}", alias)
        } else {
            "".to_string()
        }
    }

    fn inc(&mut self) {
        self.indent += 1;
    }

    fn dec(&mut self) {
        self.indent -= 1;
    }

    fn new_line(&mut self, s: &str) {
        if !self.output.is_empty() && self.output.rfind('\n') != Some(self.output.len()) {
            self.end_line();
            for _ in 0..self.indent * 4 {
                self.output.push(' ');
            }
        }
        self.output.push_str(s);
    }

    fn end_line(&mut self) {
        self.output.push('\n');
    }
}
