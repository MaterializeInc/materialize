// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `EXPLAIN ... AS TEXT` support for LIR structures.

use std::{collections::HashMap, fmt, ops::Deref};

use mz_compute_client::plan::{
    reduce::{AccumulablePlan, BasicPlan, CollationPlan, HierarchicalPlan},
    AvailableCollections, Plan,
};
use mz_expr::{explain::Indices, Id, MapFilterProject, MirScalarExpr};
use mz_ore::str::{bracketed, separated, IndentLike, StrExt};
use mz_repr::explain_new::{fmt_text_constant_rows, separated_text, DisplayText};

use crate::explain_new::{Displayable, PlanRenderingContext};

impl<'a> DisplayText<PlanRenderingContext<'_, Plan>> for Displayable<'a, Plan> {
    fn fmt_text(
        &self,
        f: &mut fmt::Formatter<'_>,
        ctx: &mut PlanRenderingContext<'_, Plan>,
    ) -> fmt::Result {
        use Plan::*;

        match &self.0 {
            Constant { rows } => match rows {
                Ok(rows) => {
                    writeln!(f, "{}Constant", ctx.indent)?;
                    ctx.indented(|ctx| {
                        fmt_text_constant_rows(
                            f,
                            rows.iter().map(|(data, _, diff)| (data, diff)),
                            &mut ctx.indent,
                        )
                    })?;
                }
                Err(err) => {
                    writeln!(f, "{}Error {}", ctx.indent, err.to_string().quoted())?;
                }
            },
            Get { id, keys, plan } => {
                ctx.indent.set(); // mark the current indent level

                // Resolve the id as a string.
                let id = match id {
                    Id::Local(id) => id.to_string(),
                    Id::Global(id) => ctx.humanizer.humanize_id(*id).ok_or(fmt::Error)?,
                };
                // Render plan-specific fields.
                use mz_compute_client::plan::GetPlan;
                match plan {
                    GetPlan::PassArrangements => {
                        writeln!(f, "{}Get::PassArrangements {}", ctx.indent, id)?;
                        ctx.indent += 1;
                    }
                    GetPlan::Arrangement(key, val, mfp) => {
                        writeln!(f, "{}Get::Arrangement {}", ctx.indent, id)?;
                        ctx.indent += 1;
                        Displayable::from(mfp).fmt_text(f, ctx)?;
                        {
                            let key = separated_text(", ", key.iter().map(Displayable::from));
                            writeln!(f, "{}key={}", ctx.indent, key)?;
                        }
                        if let Some(val) = val {
                            writeln!(f, "{}val={}", ctx.indent, val)?;
                        }
                    }
                    GetPlan::Collection(mfp) => {
                        writeln!(f, "{}Get::Collection {}", ctx.indent, id)?;
                        ctx.indent += 1;
                        Displayable::from(mfp).fmt_text(f, ctx)?;
                    }
                }

                // Render plan-agnostic fields (common for all plans for this variant).
                Displayable::from(keys).fmt_text(f, ctx)?;

                ctx.indent.reset(); // reset the original indent level
            }
            Let { id, value, body } => {
                let mut bindings = vec![(id, value.as_ref())];
                let mut head = body.as_ref();

                // Render Let-blocks nested in the body an outer Let-block in one step
                // with a flattened list of bindings
                while let Let { id, value, body } = head {
                    bindings.push((id, value.as_ref()));
                    head = body.as_ref();
                }

                // The body comes first in the text output format in order to
                // align with the format convention the dataflow is rendered
                // top to bottom
                writeln!(f, "{}Let", ctx.indent)?;
                ctx.indented(|ctx| {
                    Displayable::from(head).fmt_text(f, ctx)?;
                    writeln!(f, "{}Where", ctx.indent)?;
                    ctx.indented(|ctx| {
                        for (id, value) in bindings.iter().rev() {
                            writeln!(f, "{}{} =", ctx.indent, *id)?;
                            ctx.indented(|ctx| Displayable::from(*value).fmt_text(f, ctx))?;
                        }
                        Ok(())
                    })
                })?;
            }
            Mfp {
                input,
                mfp,
                input_key_val,
            } => {
                writeln!(f, "{}Mfp", ctx.indent)?;
                ctx.indented(|ctx| {
                    Displayable::from(mfp).fmt_text(f, ctx)?;
                    if let Some((key, val)) = input_key_val {
                        {
                            let key = separated_text(", ", key.iter().map(Displayable::from));
                            writeln!(f, "{}input_key={}", ctx.indent, key)?;
                        }
                        if let Some(val) = val {
                            writeln!(f, "{}input_val={}", ctx.indent, val)?;
                        }
                    }
                    Displayable::from(input.as_ref()).fmt_text(f, ctx)
                })?;
            }
            FlatMap {
                input,
                func,
                exprs,
                mfp,
                input_key,
            } => {
                let exprs = separated_text(", ", exprs.iter().map(Displayable::from));
                writeln!(f, "{}FlatMap {}({})", ctx.indent, func, exprs)?;
                ctx.indented(|ctx| {
                    Displayable::from(mfp).fmt_text(f, ctx)?;
                    if let Some(key) = input_key {
                        let key = separated_text(", ", key.iter().map(Displayable::from));
                        writeln!(f, "{}input_key={}", ctx.indent, key)?;
                    }
                    Displayable::from(input.as_ref()).fmt_text(f, ctx)
                })?;
            }
            Join { inputs: _, plan: _ } => {
                // todo
            }
            Reduce {
                input,
                key_val_plan,
                plan,
                input_key,
            } => {
                use mz_compute_client::plan::reduce::ReducePlan;
                match plan {
                    ReducePlan::Distinct => {
                        writeln!(f, "{}Reduce::Distinct", ctx.indent)?;
                    }
                    ReducePlan::DistinctNegated => {
                        writeln!(f, "{}Reduce::DistinctNegated", ctx.indent)?;
                    }
                    ReducePlan::Accumulable(plan) => {
                        writeln!(f, "{}Reduce::Accumulable", ctx.indent)?;
                        ctx.indented(|ctx| Displayable::from(plan).fmt_text(f, ctx))?;
                    }
                    ReducePlan::Hierarchical(plan) => {
                        writeln!(f, "{}Reduce::Hierarchical", ctx.indent)?;
                        ctx.indented(|ctx| Displayable::from(plan).fmt_text(f, ctx))?;
                    }
                    ReducePlan::Basic(plan) => {
                        writeln!(f, "{}Reduce::Basic", ctx.indent)?;
                        ctx.indented(|ctx| Displayable::from(plan).fmt_text(f, ctx))?;
                    }
                    ReducePlan::Collation(plan) => {
                        writeln!(f, "{}Reduce::Collation", ctx.indent)?;
                        ctx.indented(|ctx| Displayable::from(plan).fmt_text(f, ctx))?;
                    }
                }
                ctx.indented(|ctx| {
                    if key_val_plan.val_plan.deref().is_identity() {
                        writeln!(f, "{}val_plan=id", ctx.indent)?;
                    } else {
                        writeln!(f, "{}val_plan", ctx.indent)?;
                        ctx.indented(|ctx| {
                            Displayable::from(key_val_plan.val_plan.deref()).fmt_text(f, ctx)
                        })?;
                    }
                    if key_val_plan.key_plan.deref().is_identity() {
                        writeln!(f, "{}key_plan=id", ctx.indent)?;
                    } else {
                        writeln!(f, "{}key_plan", ctx.indent)?;
                        ctx.indented(|ctx| {
                            Displayable::from(key_val_plan.key_plan.deref()).fmt_text(f, ctx)
                        })?;
                    }
                    if let Some(key) = input_key {
                        let key = separated_text(", ", key.iter().map(Displayable::from));
                        writeln!(f, "{}input_key={}", ctx.indent, key)?;
                    }
                    Displayable::from(input.as_ref()).fmt_text(f, ctx)
                })?;
            }
            TopK { input, top_k_plan } => {
                use mz_compute_client::plan::top_k::TopKPlan;
                match top_k_plan {
                    TopKPlan::MonotonicTop1(plan) => {
                        write!(f, "{}TopK::MonotonicTop1", ctx.indent)?;
                        if plan.group_key.len() > 0 {
                            let group_by = Indices(&plan.group_key);
                            write!(f, " group_by=[{}]", group_by)?;
                        }
                        if plan.order_key.len() > 0 {
                            let order_by = separated(", ", &plan.order_key);
                            write!(f, " order_by=[{}]", order_by)?;
                        }
                    }
                    TopKPlan::MonotonicTopK(plan) => {
                        write!(f, "{}TopK::MonotonicTopK", ctx.indent)?;
                        if plan.group_key.len() > 0 {
                            let group_by = Indices(&plan.group_key);
                            write!(f, " group_by=[{}]", group_by)?;
                        }
                        if plan.order_key.len() > 0 {
                            let order_by = separated(", ", &plan.order_key);
                            write!(f, " order_by=[{}]", order_by)?;
                        }
                        if let Some(limit) = &plan.limit {
                            write!(f, " limit={}", limit)?;
                        }
                    }
                    TopKPlan::Basic(plan) => {
                        write!(f, "{}TopK::Basic", ctx.indent)?;
                        if plan.group_key.len() > 0 {
                            let group_by = Indices(&plan.group_key);
                            write!(f, " group_by=[{}]", group_by)?;
                        }
                        if plan.order_key.len() > 0 {
                            let order_by = separated(", ", &plan.order_key);
                            write!(f, " order_by=[{}]", order_by)?;
                        }
                        if let Some(limit) = &plan.limit {
                            write!(f, " limit={}", limit)?;
                        }
                        if &plan.offset > &0 {
                            write!(f, " offset={}", plan.offset)?;
                        }
                    }
                }
                writeln!(f)?;
                ctx.indented(|ctx| Displayable::from(input.as_ref()).fmt_text(f, ctx))?;
            }
            Negate { input } => {
                writeln!(f, "{}Negate", ctx.indent)?;
                ctx.indented(|ctx| Displayable::from(input.as_ref()).fmt_text(f, ctx))?;
            }
            Threshold {
                input,
                threshold_plan,
            } => {
                use mz_compute_client::plan::threshold::ThresholdPlan;
                match threshold_plan {
                    ThresholdPlan::Basic(plan) => {
                        let ensure_arrangement = Arrangement::from(&plan.ensure_arrangement);
                        write!(f, "{}Threshold::Basic", ctx.indent)?;
                        writeln!(f, " ensure_arrangement={}", ensure_arrangement)?;
                    }
                    ThresholdPlan::Retractions(plan) => {
                        let ensure_arrangement = Arrangement::from(&plan.ensure_arrangement);
                        write!(f, "{}Threshold::Retractions", ctx.indent)?;
                        writeln!(f, " ensure_arrangement={}", ensure_arrangement)?;
                    }
                };
                ctx.indented(|ctx| Displayable::from(input.as_ref()).fmt_text(f, ctx))?;
            }
            Union { inputs } => {
                writeln!(f, "{}Union", ctx.indent)?;
                ctx.indented(|ctx| {
                    for input in inputs.iter() {
                        Displayable::from(input).fmt_text(f, ctx)?;
                    }
                    Ok(())
                })?;
            }
            ArrangeBy {
                input,
                forms,
                input_key,
                input_mfp,
            } => {
                writeln!(f, "{}ArrangeBy", ctx.indent)?;
                ctx.indented(|ctx| {
                    if let Some(key) = input_key {
                        let key = separated_text(", ", key.iter().map(Displayable::from));
                        writeln!(f, "{}input_key=[{}]", ctx.indent, key)?;
                    }
                    Displayable::from(input_mfp).fmt_text(f, ctx)?;
                    Displayable::from(forms).fmt_text(f, ctx)?;
                    // Render input
                    Displayable::from(input.as_ref()).fmt_text(f, ctx)
                })?;
            }
        }

        Ok(())
    }
}

impl<'a> DisplayText<PlanRenderingContext<'_, Plan>> for Displayable<'a, AvailableCollections> {
    fn fmt_text(
        &self,
        f: &mut fmt::Formatter<'_>,
        ctx: &mut PlanRenderingContext<'_, Plan>,
    ) -> fmt::Result {
        // raw field
        let raw = &self.0.raw;
        writeln!(f, "{}raw={}", ctx.indent, raw)?;
        // arranged field
        for (i, arrangement) in (&self.0.arranged).iter().enumerate() {
            let arrangement = Arrangement::from(arrangement);
            writeln!(f, "{}arrangements[{}]={}", ctx.indent, i, arrangement)?;
        }

        Ok(())
    }
}

impl<'a> DisplayText<PlanRenderingContext<'_, Plan>> for Displayable<'a, MapFilterProject> {
    fn fmt_text(
        &self,
        f: &mut fmt::Formatter<'_>,
        ctx: &mut PlanRenderingContext<'_, Plan>,
    ) -> fmt::Result {
        let (scalars, predicates, outputs, input_arity) = (
            &self.0.expressions,
            &self.0.predicates,
            &self.0.projection,
            &self.0.input_arity,
        );

        // render `project` field iff not the identity projection
        if &outputs.len() != input_arity || outputs.iter().enumerate().any(|(i, p)| i != *p) {
            let outputs = Indices(&outputs);
            writeln!(f, "{}project=({})", ctx.indent, outputs)?;
        }
        // render `filter` field iff predicates are present
        if !predicates.is_empty() {
            let predicates = predicates.iter().map(|(_, p)| Displayable::from(p));
            let predicates = separated_text(" AND ", predicates);
            writeln!(f, "{}filter=({})", ctx.indent, predicates)?;
        }
        // render `map` field iff scalars are present
        if !scalars.is_empty() {
            let scalars = scalars.iter().map(Displayable::from);
            let scalars = separated_text(", ", scalars);
            writeln!(f, "{}map=({})", ctx.indent, scalars)?;
        }

        Ok(())
    }
}

impl<'a> DisplayText<PlanRenderingContext<'_, Plan>> for Displayable<'a, AccumulablePlan> {
    fn fmt_text(
        &self,
        f: &mut fmt::Formatter<'_>,
        ctx: &mut PlanRenderingContext<'_, Plan>,
    ) -> fmt::Result {
        // full_aggrs (skipped because they are repeated in simple_aggrs ∪ distinct_aggrs)
        // for (i, aggr) in self.0.full_aggrs.iter().enumerate() {
        //     write!(f, "{}full_aggrs[{}]=", ctx.indent, i)?;
        //     Displayable::from(aggr).fmt_text(f, &mut ())?;
        //     writeln!(f)?;
        // }
        // simple_aggrs
        for (i, (i_aggs, i_datum, agg)) in self.0.simple_aggrs.iter().enumerate() {
            write!(f, "{}simple_aggrs[{}]=", ctx.indent, i)?;
            write!(f, "({}, {}, ", i_aggs, i_datum)?;
            Displayable::from(agg).fmt_text(f, &mut ())?;
            writeln!(f, ")")?;
        }
        // distinct_aggrs
        for (i, (i_aggs, i_datum, agg)) in self.0.distinct_aggrs.iter().enumerate() {
            write!(f, "{}distinct_aggrs[{}]=", ctx.indent, i)?;
            write!(f, "({}, {}, ", i_aggs, i_datum)?;
            Displayable::from(agg).fmt_text(f, &mut ())?;
            writeln!(f, ")")?;
        }
        Ok(())
    }
}

impl<'a> DisplayText<PlanRenderingContext<'_, Plan>> for Displayable<'a, HierarchicalPlan> {
    fn fmt_text(
        &self,
        f: &mut fmt::Formatter<'_>,
        ctx: &mut PlanRenderingContext<'_, Plan>,
    ) -> fmt::Result {
        match self.0 {
            HierarchicalPlan::Monotonic(plan) => {
                let aggr_funcs = separated(", ", &plan.aggr_funcs);
                writeln!(f, "{}aggr_funcs=[{}]", ctx.indent, aggr_funcs)?;
                let skips = separated(", ", &plan.skips);
                writeln!(f, "{}skips=[{}]", ctx.indent, skips)?;
            }
            HierarchicalPlan::Bucketed(plan) => {
                let aggr_funcs = separated(", ", &plan.aggr_funcs);
                writeln!(f, "{}aggr_funcs=[{}]", ctx.indent, aggr_funcs)?;
                let skips = separated(", ", &plan.skips);
                writeln!(f, "{}skips=[{}]", ctx.indent, skips)?;
                let buckets = separated(", ", &plan.skips);
                writeln!(f, "{}buckets=[{}]", ctx.indent, buckets)?;
            }
        }
        Ok(())
    }
}

impl<'a> DisplayText<PlanRenderingContext<'_, Plan>> for Displayable<'a, BasicPlan> {
    fn fmt_text(
        &self,
        f: &mut fmt::Formatter<'_>,
        ctx: &mut PlanRenderingContext<'_, Plan>,
    ) -> fmt::Result {
        match self.0 {
            BasicPlan::Single(idx, agg) => {
                write!(f, "{}aggr=[({}, ", ctx.indent, idx)?;
                Displayable::from(agg).fmt_text(f, &mut ())?;
                writeln!(f, ")")?;
            }
            BasicPlan::Multiple(aggs) => {
                for (i, (i_datum, agg)) in aggs.iter().enumerate() {
                    write!(f, "{}aggrs[{}]=({}, ", ctx.indent, i, i_datum)?;
                    Displayable::from(agg).fmt_text(f, &mut ())?;
                    writeln!(f, ")")?;
                }
            }
        }
        Ok(())
    }
}

impl<'a> DisplayText<PlanRenderingContext<'_, Plan>> for Displayable<'a, CollationPlan> {
    fn fmt_text(
        &self,
        f: &mut fmt::Formatter<'_>,
        ctx: &mut PlanRenderingContext<'_, Plan>,
    ) -> fmt::Result {
        {
            use mz_compute_client::plan::reduce::ReductionType;
            let aggregate_types = &self
                .0
                .aggregate_types
                .iter()
                .map(|reduction_type| match reduction_type {
                    ReductionType::Accumulable => "a".to_string(),
                    ReductionType::Hierarchical => "h".to_string(),
                    ReductionType::Basic => "b".to_string(),
                })
                .collect::<Vec<_>>();
            let aggregate_types = separated(", ", aggregate_types);
            writeln!(f, "{}aggregate_types=[{}]", ctx.indent, aggregate_types)?;
        }
        if let Some(plan) = &self.0.accumulable {
            writeln!(f, "{}accumulable", ctx.indent)?;
            ctx.indented(|ctx| Displayable::from(plan).fmt_text(f, ctx))?;
        }
        if let Some(plan) = &self.0.hierarchical {
            writeln!(f, "{}hierarchical", ctx.indent)?;
            ctx.indented(|ctx| Displayable::from(plan).fmt_text(f, ctx))?;
        }
        if let Some(plan) = &self.0.basic {
            writeln!(f, "{}basic", ctx.indent)?;
            ctx.indented(|ctx| Displayable::from(plan).fmt_text(f, ctx))?;
        }
        Ok(())
    }
}

/// Helper struct for rendering an arrangement.
struct Arrangement<'a> {
    key: &'a Vec<MirScalarExpr>,
    permutation: Permutation<'a>,
    thinning: &'a Vec<usize>,
}

impl<'a> From<&'a (Vec<MirScalarExpr>, HashMap<usize, usize>, Vec<usize>)> for Arrangement<'a> {
    fn from(
        (key, permutation, thinning): &'a (Vec<MirScalarExpr>, HashMap<usize, usize>, Vec<usize>),
    ) -> Self {
        Arrangement {
            key,
            permutation: Permutation(permutation),
            thinning,
        }
    }
}

impl<'a> fmt::Display for Arrangement<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // prepare key
        let key = separated_text(", ", self.key.iter().map(Displayable::from));
        let key = bracketed("[", "]", key);
        // prepare perumation map
        let permutation = &self.permutation;
        // prepare thinning
        let thinning = Indices(&self.thinning);
        // write the arrangement spec
        write!(
            f,
            "{{ key={}, permutation={}, thinning=({}) }}",
            key, permutation, thinning
        )
    }
}

/// Helper struct for rendering a permutation.
struct Permutation<'a>(&'a HashMap<usize, usize>);

impl<'a> fmt::Display for Permutation<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut pairs = vec![];
        for (x, y) in self.0.iter() {
            if x != y {
                pairs.push(format!("#{}: #{}", x, y))
            }
        }

        if pairs.len() > 0 {
            bracketed("{", "}", separated(", ", pairs)).fmt(f)
        } else {
            separated("", vec!["id".to_string()]).fmt(f)
        }
    }
}
