// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Generates a graphviz graph from a Query Graph Model.
//!
//! The public interface consists of the [`Model::as_dot`] method.

use crate::query_model::model::{
    BoxId, BoxType, ColumnReference, Quantifier, QuantifierId, QueryBox,
};
use crate::query_model::Model;
use itertools::Itertools;
use mz_expr::ExprHumanizer;
use mz_ore::str::separated;
use std::collections::{BTreeMap, HashSet};
use std::fmt::{self, Write};

use super::attribute::core::{Attribute, RequiredAttributes};
use super::attribute::relation_type::RelationType;

impl Model {
    pub fn as_dot<'a>(
        &mut self,
        label: &str,
        expr_humanizer: &'a dyn ExprHumanizer,
        with_types: bool,
    ) -> Result<String, anyhow::Error> {
        DotGenerator::new(expr_humanizer, with_types).generate(self, label)
    }
}

/// Generates a graphviz graph from a Query Graph Model, defined in the DOT language.
/// See <https://graphviz.org/doc/info/lang.html>.
#[derive(Debug)]
struct DotGenerator<'a> {
    output: String,
    indent: u32,
    expr_humanizer: &'a dyn ExprHumanizer,
    with_types: bool,
}

/// Generates a label for a graphviz graph.
#[derive(Debug)]
enum DotLabel<'a> {
    /// Plain label
    SingleRow(&'a str),
    /// A single-column table that has a row for each string in the array.
    MultiRow(&'a [String]),
}

/// Generates a string that escapes characters that would problematic inside the
/// specification for a label.
/// The set of escaped characters is "|{}.
struct DotLabelEscapedString<'a>(&'a str);

impl<'a> DotGenerator<'a> {
    fn new(expr_humanizer: &'a dyn ExprHumanizer, with_types: bool) -> Self {
        Self {
            output: String::new(),
            indent: 0,
            expr_humanizer,
            with_types,
        }
    }

    /// Derive attributes required for rendering the given [`Model`].
    fn derive_required_attributes(&self, model: &mut Model, start_box: BoxId) {
        // collect a set of required attributes for rendering
        let mut attributes = HashSet::new();
        if self.with_types {
            attributes.insert(Box::new(RelationType) as Box<dyn Attribute>);
        }
        // derive the required derived attributes
        RequiredAttributes::from(attributes).derive(model, start_box);
    }

    /// Generates a graphviz graph for the given model, labeled with `label`.
    fn generate(self, model: &mut Model, label: &str) -> Result<String, anyhow::Error> {
        self.generate_subgraph(model, model.top_box, label)
    }

    /// Generates a graphviz graph for the given subgraph of the model, labeled with `label`.
    fn generate_subgraph(
        mut self,
        model: &mut Model,
        start_box: BoxId,
        label: &str,
    ) -> Result<String, anyhow::Error> {
        self.derive_required_attributes(model, start_box);

        self.new_line("digraph G {");
        self.inc();
        self.new_line("compound = true");
        self.new_line("labeljust = l");
        self.new_line(&DotLabel::SingleRow(label.trim()).to_string());
        self.new_line("node [ shape = box ]");

        // list of quantifiers for adding the edges connecting them to their
        // input boxes after all boxes have been processed
        let mut quantifiers = Vec::new();

        model
            .try_visit_pre_post_descendants(
                &mut |m, box_id| -> Result<(), ()> {
                    let b = m.get_box(*box_id);
                    self.new_line(&format!("subgraph cluster{} {{", box_id));
                    self.inc();
                    self.new_line(
                        &DotLabel::SingleRow(&format!("Box{}:{}", box_id, Self::get_box_title(&b)))
                            .to_string(),
                    );
                    self.new_line(&format!(
                        "boxhead{} [ shape = record, {} ]",
                        box_id,
                        self.get_box_head(&b)
                    ));

                    self.new_line("{");
                    self.inc();
                    self.new_line("rank = same");

                    if b.input_quantifiers().count() > 0 {
                        self.new_line("node [ shape = circle ]");
                    }

                    for q in b.input_quantifiers() {
                        quantifiers.push(q.id);

                        self.new_line(&format!(
                            "Q{0} [ {1} ]",
                            q.id,
                            DotLabel::SingleRow(&format!(
                                "Q{0}({1}){2}",
                                q.id,
                                q.quantifier_type,
                                Self::get_quantifier_alias(&q)
                            ))
                        ));
                    }

                    self.add_correlation_info(b.correlation_info());

                    self.dec();
                    self.new_line("}");
                    self.dec();
                    self.new_line("}");

                    Ok(())
                },
                &mut |_, _| Ok(()),
                start_box,
            )
            .unwrap();

        if quantifiers.len() > 0 {
            self.new_line("edge [ arrowhead = none, style = dashed ]");
            for q_id in quantifiers.iter() {
                let q = model.get_quantifier(*q_id);
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

    fn get_box_head(&self, b: &QueryBox) -> String {
        let mut rows = Vec::new();

        rows.push(format!("Distinct: {:?}", b.distinct));

        // The projection of the box
        if self.with_types {
            let relation_type = b.attributes.get::<RelationType>();
            for (i, c) in b.columns.iter().enumerate() {
                let typ = self.expr_humanizer.humanize_column_type(&relation_type[i]);
                if let Some(alias) = &c.alias {
                    rows.push(format!("{}: {} ({}) as {}", i, c.expr, typ, alias.as_str()));
                } else {
                    rows.push(format!("{}: {} ({})", i, c.expr, typ));
                }
            }
        } else {
            // The projection of the box
            for (i, c) in b.columns.iter().enumerate() {
                if let Some(alias) = &c.alias {
                    rows.push(format!("{}: {} as {}", i, c.expr, alias.as_str()));
                } else {
                    rows.push(format!("{}: {}", i, c.expr));
                }
            }
        }

        // Per-type internal properties.
        match &b.box_type {
            BoxType::Select(select) => {
                if let Some(order_key) = &select.order_key {
                    rows.push(format!("ORDER BY: {}", separated(", ", order_key.iter())))
                }
            }
            BoxType::Grouping(grouping) => {
                if !grouping.key.is_empty() {
                    rows.push(format!(
                        "GROUP BY: {}",
                        separated(", ", grouping.key.iter())
                    ))
                }
            }
            BoxType::Values(values) => {
                for row in values.rows.iter() {
                    rows.push(format!("ROW: {}", separated(", ", row.iter())))
                }
            }
            BoxType::CallTable(call_table) => {
                // display function call
                let func = &call_table.func;
                let args = &call_table.exprs;
                rows.push(format!("CALL: {}({})", func, separated(", ", args)));
            }
            _ => {}
        }

        // @todo predicates as arrows
        if let Some(predicates) = match &b.box_type {
            BoxType::Select(select) => Some(&select.predicates),
            BoxType::OuterJoin(outer_join) => Some(&outer_join.predicates),
            _ => None,
        } {
            rows.extend(predicates.iter().map(|p| p.to_string()));
        }

        if let Some(unique_keys) = get_unique_keys(b) {
            for key in unique_keys {
                let key = key.iter().map(|column| format!("C{}", column));
                rows.push(format!("UNIQUE KEY: {}", separated(", ", key)));
            }
        }

        DotLabel::MultiRow(&rows).to_string()
    }

    /// Adds red arrows from correlated quantifiers to the sibling quantifiers they
    /// are correlated with.
    fn add_correlation_info(
        &mut self,
        correlation_info: BTreeMap<QuantifierId, HashSet<ColumnReference>>,
    ) {
        let q_correlation_info = correlation_info.into_iter().map(|(id, column_refs)| {
            (
                id,
                column_refs
                    .iter()
                    .map(|c| c.quantifier_id)
                    .sorted()
                    .unique()
                    .collect::<Vec<_>>(),
            )
        });

        for (correlated_q, quantifiers) in q_correlation_info {
            for q in quantifiers.iter() {
                self.new_line(&format!(
                    "Q{0} -> Q{1} [ {2}, style = filled, color = red ]",
                    correlated_q,
                    q,
                    DotLabel::SingleRow("correlation")
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

fn get_unique_keys(b: &QueryBox) -> Option<Vec<Vec<usize>>> {
    // TODO: return value of UniqueKeys attribute if present and
    // fallback to the Get and CallTable base cases otherwise
    // (TBD once the attribute has been added to the codebase)
    match &b.box_type {
        BoxType::CallTable(call_table) => Some(call_table.func.output_type().keys),
        BoxType::Get(get) => Some(get.unique_keys.clone()),
        _ => None,
    }
}

impl<'a> fmt::Display for DotLabelEscapedString<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for c in self.0.chars() {
            match c {
                '"' => f.write_str("\\\"")?,
                '|' => f.write_str("\\|")?,
                '{' => f.write_str("\\{")?,
                '}' => f.write_str("\\}")?,
                '>' => f.write_str("\\>")?,
                '<' => f.write_str("\\<")?,
                _ => f.write_char(c)?,
            }
        }
        Ok(())
    }
}

impl<'a> fmt::Display for DotLabel<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("label = \"")?;
        match self {
            DotLabel::SingleRow(str) => f.write_str(&DotLabelEscapedString(str).to_string()),
            DotLabel::MultiRow(strs) => {
                f.write_str("{ ")?;
                f.write_str(&format!(
                    "{}",
                    separated("| ", strs.into_iter().map(|str| DotLabelEscapedString(str)))
                ))?;
                f.write_str(" }")
            }
        }?;
        f.write_char('\"')
    }
}
