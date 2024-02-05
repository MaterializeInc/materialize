// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `EXPLAIN ... AS TEXT` support for LIR structures.
//!
//! The format adheres to the following conventions:
//! 1. In general, every line that starts with an uppercase character
//!    corresponds to a [`Plan`] variant.
//! 2. Whenever the variant has an attached `~Plan`, the printed name is
//!    `$V::$P` where `$V` identifies the variant and `$P` the plan.
//! 3. The fields of a `~Plan` struct attached to a [`Plan`] are rendered as if
//!    they were part of the variant themself.
//! 4. Non-recursive parameters of each sub-plan are written as `$key=$val`
//!    pairs on the same line or as lowercase `$key` fields on indented lines.
//! 5. A single non-recursive parameter can be written just as `$val`.

use std::collections::BTreeMap;
use std::fmt;
use std::ops::Deref;

use itertools::{izip, Itertools};
use mz_expr::explain::{fmt_text_constant_rows, HumanizedExplain, HumanizerMode};
use mz_expr::{Id, MirScalarExpr};
use mz_ore::str::{separated, IndentLike, StrExt};
use mz_repr::explain::text::DisplayText;
use mz_repr::explain::{
    CompactScalarSeq, CompactScalars, ExplainConfig, Indices, PlanRenderingContext,
};

use crate::plan::join::delta_join::{DeltaPathPlan, DeltaStagePlan};
use crate::plan::join::linear_join::LinearStagePlan;
use crate::plan::join::{DeltaJoinPlan, JoinClosure, LinearJoinPlan};
use crate::plan::reduce::{AccumulablePlan, BasicPlan, CollationPlan, HierarchicalPlan};
use crate::plan::{AvailableCollections, NodeId, Plan};

impl DisplayText<PlanRenderingContext<'_, Plan>> for Plan {
    fn fmt_text(
        &self,
        f: &mut fmt::Formatter<'_>,
        ctx: &mut PlanRenderingContext<'_, Plan>,
    ) -> fmt::Result {
        use Plan::*;

        let mode = HumanizedExplain::new(ctx.config.redacted);
        let annotations = PlanAnnotations::new(ctx.config.clone(), self);

        match &self {
            Constant { rows, node_id: _ } => match rows {
                Ok(rows) => {
                    if !rows.is_empty() {
                        writeln!(f, "{}Constant{}", ctx.indent, annotations)?;
                        ctx.indented(|ctx| {
                            fmt_text_constant_rows(
                                f,
                                rows.iter().map(|(data, _, diff)| (data, diff)),
                                &mut ctx.indent,
                                ctx.config.redacted,
                            )
                        })?;
                    } else {
                        writeln!(f, "{}Constant <empty>{}", ctx.indent, annotations)?;
                    }
                }
                Err(err) => {
                    if mode.redacted() {
                        writeln!(f, "{}Error █{}", ctx.indent, annotations)?;
                    } else {
                        writeln!(
                            f,
                            "{}Error {}{}",
                            ctx.indent,
                            err.to_string().quoted(),
                            annotations
                        )?;
                    }
                }
            },
            Get {
                id,
                keys,
                plan,
                node_id: _,
            } => {
                ctx.indent.set(); // mark the current indent level

                // Resolve the id as a string.
                let id = match id {
                    Id::Local(id) => id.to_string(),
                    Id::Global(id) => ctx
                        .humanizer
                        .humanize_id(*id)
                        .unwrap_or_else(|| id.to_string()),
                };
                // Render plan-specific fields.
                use crate::plan::GetPlan;
                match plan {
                    GetPlan::PassArrangements => {
                        writeln!(
                            f,
                            "{}Get::PassArrangements {}{}",
                            ctx.indent, id, annotations
                        )?;
                        ctx.indent += 1;
                    }
                    GetPlan::Arrangement(key, val, mfp) => {
                        writeln!(f, "{}Get::Arrangement {}{}", ctx.indent, id, annotations)?;
                        ctx.indent += 1;
                        mode.expr(mfp, None).fmt_text(f, ctx)?;
                        {
                            let key = mode.seq(key, None);
                            let key = CompactScalars(key);
                            writeln!(f, "{}key={}", ctx.indent, key)?;
                        }
                        if let Some(val) = val {
                            let val = mode.expr(val, None);
                            writeln!(f, "{}val={}", ctx.indent, val)?;
                        }
                    }
                    GetPlan::Collection(mfp) => {
                        writeln!(f, "{}Get::Collection {}{}", ctx.indent, id, annotations)?;
                        ctx.indent += 1;
                        mode.expr(mfp, None).fmt_text(f, ctx)?;
                    }
                }

                // Render plan-agnostic fields (common for all plans for this variant).
                keys.fmt_text(f, ctx)?;

                ctx.indent.reset(); // reset the original indent level
            }
            Let {
                id,
                value,
                body,
                node_id: _,
            } => {
                let mut bindings = vec![(id, value.as_ref())];
                let mut head = body.as_ref();

                // Render Let-blocks nested in the body an outer Let-block in one step
                // with a flattened list of bindings
                while let Let {
                    id,
                    value,
                    body,
                    node_id: _,
                } = head
                {
                    bindings.push((id, value.as_ref()));
                    head = body.as_ref();
                }

                writeln!(f, "{}Return{}", ctx.indent, annotations)?;
                ctx.indented(|ctx| head.fmt_text(f, ctx))?;
                writeln!(f, "{}With", ctx.indent)?;
                ctx.indented(|ctx| {
                    for (id, value) in bindings.iter().rev() {
                        writeln!(f, "{}cte {} =", ctx.indent, *id)?;
                        ctx.indented(|ctx| value.fmt_text(f, ctx))?;
                    }
                    Ok(())
                })?;
            }
            LetRec {
                ids,
                values,
                limits,
                body,
                node_id: _,
            } => {
                let bindings = izip!(ids.iter(), values, limits).collect_vec();
                let head = body.as_ref();

                writeln!(f, "{}Return{}", ctx.indent, annotations)?;
                ctx.indented(|ctx| head.fmt_text(f, ctx))?;
                writeln!(f, "{}With Mutually Recursive", ctx.indent)?;
                ctx.indented(|ctx| {
                    for (id, value, limit) in bindings.iter().rev() {
                        if let Some(limit) = limit {
                            writeln!(f, "{}cte {} {} =", ctx.indent, limit, *id)?;
                        } else {
                            writeln!(f, "{}cte {} =", ctx.indent, *id)?;
                        }
                        ctx.indented(|ctx| value.fmt_text(f, ctx))?;
                    }
                    Ok(())
                })?;
            }
            Mfp {
                input,
                mfp,
                input_key_val,
                node_id: _,
            } => {
                writeln!(f, "{}Mfp{}", ctx.indent, annotations)?;
                ctx.indented(|ctx| {
                    mode.expr(mfp, None).fmt_text(f, ctx)?;
                    if let Some((key, val)) = input_key_val {
                        {
                            let key = mode.seq(key, None);
                            let key = CompactScalars(key);
                            writeln!(f, "{}input_key={}", ctx.indent, key)?;
                        }
                        if let Some(val) = val {
                            let val = mode.expr(val, None);
                            writeln!(f, "{}input_val={}", ctx.indent, val)?;
                        }
                    }
                    input.fmt_text(f, ctx)
                })?;
            }
            FlatMap {
                input,
                func,
                exprs,
                mfp_after,
                input_key,
                node_id: _,
            } => {
                let exprs = mode.seq(exprs, None);
                let exprs = CompactScalars(exprs);
                writeln!(
                    f,
                    "{}FlatMap {}({}){}",
                    ctx.indent, func, exprs, annotations
                )?;
                ctx.indented(|ctx| {
                    if !mfp_after.is_identity() {
                        writeln!(f, "{}mfp_after", ctx.indent)?;
                        ctx.indented(|ctx| mode.expr(mfp_after, None).fmt_text(f, ctx))?;
                    }
                    if let Some(key) = input_key {
                        let key = mode.seq(key, None);
                        let key = CompactScalars(key);
                        writeln!(f, "{}input_key={}", ctx.indent, key)?;
                    }
                    input.fmt_text(f, ctx)
                })?;
            }
            Join {
                inputs,
                plan,
                node_id: _,
            } => {
                use crate::plan::join::JoinPlan;
                match plan {
                    JoinPlan::Linear(plan) => {
                        writeln!(f, "{}Join::Linear{}", ctx.indent, annotations)?;
                        ctx.indented(|ctx| plan.fmt_text(f, ctx))?;
                    }
                    JoinPlan::Delta(plan) => {
                        writeln!(f, "{}Join::Delta{}", ctx.indent, annotations)?;
                        ctx.indented(|ctx| plan.fmt_text(f, ctx))?;
                    }
                }
                ctx.indented(|ctx| {
                    for input in inputs {
                        input.fmt_text(f, ctx)?;
                    }
                    Ok(())
                })?;
            }
            Reduce {
                input,
                key_val_plan,
                plan,
                input_key,
                mfp_after,
                node_id: _,
            } => {
                use crate::plan::reduce::ReducePlan;
                match plan {
                    ReducePlan::Distinct => {
                        writeln!(f, "{}Reduce::Distinct{}", ctx.indent, annotations)?;
                    }
                    ReducePlan::Accumulable(plan) => {
                        writeln!(f, "{}Reduce::Accumulable{}", ctx.indent, annotations)?;
                        ctx.indented(|ctx| plan.fmt_text(f, ctx))?;
                    }
                    ReducePlan::Hierarchical(plan) => {
                        writeln!(f, "{}Reduce::Hierarchical{}", ctx.indent, annotations)?;
                        ctx.indented(|ctx| plan.fmt_text(f, ctx))?;
                    }
                    ReducePlan::Basic(plan) => {
                        writeln!(f, "{}Reduce::Basic{}", ctx.indent, annotations)?;
                        ctx.indented(|ctx| plan.fmt_text(f, ctx))?;
                    }
                    ReducePlan::Collation(plan) => {
                        writeln!(f, "{}Reduce::Collation{}", ctx.indent, annotations)?;
                        ctx.indented(|ctx| plan.fmt_text(f, ctx))?;
                    }
                }
                ctx.indented(|ctx| {
                    if key_val_plan.val_plan.deref().is_identity() {
                        writeln!(f, "{}val_plan=id", ctx.indent)?;
                    } else {
                        writeln!(f, "{}val_plan", ctx.indent)?;
                        ctx.indented(|ctx| {
                            let val_plan = mode.expr(key_val_plan.val_plan.deref(), None);
                            val_plan.fmt_text(f, ctx)
                        })?;
                    }
                    if key_val_plan.key_plan.deref().is_identity() {
                        writeln!(f, "{}key_plan=id", ctx.indent)?;
                    } else {
                        writeln!(f, "{}key_plan", ctx.indent)?;
                        ctx.indented(|ctx| {
                            let key_plan = mode.expr(key_val_plan.key_plan.deref(), None);
                            key_plan.fmt_text(f, ctx)
                        })?;
                    }
                    if let Some(key) = input_key {
                        let key = mode.seq(key, None);
                        let key = CompactScalars(key);
                        writeln!(f, "{}input_key={}", ctx.indent, key)?;
                    }
                    if !mfp_after.is_identity() {
                        writeln!(f, "{}mfp_after", ctx.indent)?;
                        ctx.indented(|ctx| mode.expr(mfp_after, None).fmt_text(f, ctx))?;
                    }

                    input.fmt_text(f, ctx)
                })?;
            }
            TopK {
                input,
                top_k_plan,
                node_id: _,
            } => {
                use crate::plan::top_k::TopKPlan;
                match top_k_plan {
                    TopKPlan::MonotonicTop1(plan) => {
                        write!(f, "{}TopK::MonotonicTop1", ctx.indent)?;
                        if plan.group_key.len() > 0 {
                            let group_by = mode.seq(&plan.group_key, None);
                            let group_by = CompactScalars(group_by);
                            write!(f, " group_by=[{}]", group_by)?;
                        }
                        if plan.order_key.len() > 0 {
                            let order_by = mode.seq(&plan.order_key, None);
                            let order_by = separated(", ", order_by);
                            write!(f, " order_by=[{}]", order_by)?;
                        }
                        if plan.must_consolidate {
                            write!(f, " must_consolidate")?;
                        }
                    }
                    TopKPlan::MonotonicTopK(plan) => {
                        write!(f, "{}TopK::MonotonicTopK", ctx.indent)?;
                        if plan.group_key.len() > 0 {
                            let group_by = mode.seq(&plan.group_key, None);
                            let group_by = CompactScalars(group_by);
                            write!(f, " group_by=[{}]", group_by)?;
                        }
                        if plan.order_key.len() > 0 {
                            let order_by = mode.seq(&plan.order_key, None);
                            let order_by = separated(", ", order_by);
                            write!(f, " order_by=[{}]", order_by)?;
                        }
                        if let Some(limit) = &plan.limit {
                            let limit = mode.expr(limit, None);
                            write!(f, " limit={}", limit)?;
                        }
                        if plan.must_consolidate {
                            write!(f, " must_consolidate")?;
                        }
                    }
                    TopKPlan::Basic(plan) => {
                        write!(f, "{}TopK::Basic", ctx.indent)?;
                        if plan.group_key.len() > 0 {
                            let group_by = mode.seq(&plan.group_key, None);
                            let group_by = CompactScalars(group_by);
                            write!(f, " group_by=[{}]", group_by)?;
                        }
                        if plan.order_key.len() > 0 {
                            let order_by = mode.seq(&plan.order_key, None);
                            let order_by = separated(", ", order_by);
                            write!(f, " order_by=[{}]", order_by)?;
                        }
                        if let Some(limit) = &plan.limit {
                            let limit = mode.expr(limit, None);
                            write!(f, " limit={}", limit)?;
                        }
                        if &plan.offset > &0 {
                            write!(f, " offset={}", plan.offset)?;
                        }
                    }
                }
                writeln!(f, "{}", annotations)?;
                ctx.indented(|ctx| input.fmt_text(f, ctx))?;
            }
            Negate { input, node_id: _ } => {
                writeln!(f, "{}Negate{}", ctx.indent, annotations)?;
                ctx.indented(|ctx| input.fmt_text(f, ctx))?;
            }
            Threshold {
                input,
                threshold_plan,
                node_id: _,
            } => {
                use crate::plan::threshold::ThresholdPlan;
                match threshold_plan {
                    ThresholdPlan::Basic(plan) => {
                        let ensure_arrangement = Arrangement::from(&plan.ensure_arrangement);
                        write!(f, "{}Threshold::Basic", ctx.indent)?;
                        write!(f, " ensure_arrangement=")?;
                        ensure_arrangement.fmt_text(f, ctx)?;
                        writeln!(f, "{}", annotations)?;
                    }
                };
                ctx.indented(|ctx| input.fmt_text(f, ctx))?;
            }
            Union {
                inputs,
                consolidate_output,
                node_id: _,
            } => {
                if *consolidate_output {
                    writeln!(
                        f,
                        "{}Union consolidate_output={}{}",
                        ctx.indent, consolidate_output, annotations
                    )?;
                } else {
                    writeln!(f, "{}Union{}", ctx.indent, annotations)?;
                }
                ctx.indented(|ctx| {
                    for input in inputs.iter() {
                        input.fmt_text(f, ctx)?;
                    }
                    Ok(())
                })?;
            }
            ArrangeBy {
                input,
                forms,
                input_key,
                input_mfp,
                node_id: _,
            } => {
                writeln!(f, "{}ArrangeBy{}", ctx.indent, annotations)?;
                ctx.indented(|ctx| {
                    if let Some(key) = input_key {
                        let key = mode.seq(key, None);
                        let key = CompactScalars(key);
                        writeln!(f, "{}input_key=[{}]", ctx.indent, key)?;
                    }
                    mode.expr(input_mfp, None).fmt_text(f, ctx)?;
                    forms.fmt_text(f, ctx)?;
                    // Render input
                    input.fmt_text(f, ctx)
                })?;
            }
        }

        Ok(())
    }
}

impl DisplayText<PlanRenderingContext<'_, Plan>> for AvailableCollections {
    fn fmt_text(
        &self,
        f: &mut fmt::Formatter<'_>,
        ctx: &mut PlanRenderingContext<'_, Plan>,
    ) -> fmt::Result {
        // raw field
        let raw = &self.raw;
        writeln!(f, "{}raw={}", ctx.indent, raw)?;
        // arranged field
        for (i, arrangement) in self.arranged.iter().enumerate() {
            let arrangement = Arrangement::from(arrangement);
            write!(f, "{}arrangements[{}]=", ctx.indent, i)?;
            arrangement.fmt_text(f, ctx)?;
            writeln!(f, "")?;
        }
        // types field
        if let Some(types) = self.types.as_ref() {
            if types.len() > 0 {
                write!(f, "{}types=[", ctx.indent)?;
                write!(
                    f,
                    "{}",
                    separated(
                        ", ",
                        types.iter().map(|c| ctx.humanizer.humanize_column_type(c))
                    )
                )?;
                writeln!(f, "]")?;
            }
        }
        Ok(())
    }
}

impl DisplayText<PlanRenderingContext<'_, Plan>> for LinearJoinPlan {
    fn fmt_text(
        &self,
        f: &mut fmt::Formatter<'_>,
        ctx: &mut PlanRenderingContext<'_, Plan>,
    ) -> fmt::Result {
        let mode = HumanizedExplain::new(ctx.config.redacted);
        let plan = self;
        if let Some(closure) = plan.final_closure.as_ref() {
            if !closure.is_identity() {
                writeln!(f, "{}final_closure", ctx.indent)?;
                ctx.indented(|ctx| closure.fmt_text(f, ctx))?;
            }
        }
        for (i, plan) in plan.stage_plans.iter().enumerate().rev() {
            writeln!(f, "{}linear_stage[{}]", ctx.indent, i)?;
            ctx.indented(|ctx| plan.fmt_text(f, ctx))?;
        }
        if let Some(closure) = plan.initial_closure.as_ref() {
            if !closure.is_identity() {
                writeln!(f, "{}initial_closure", ctx.indent)?;
                ctx.indented(|ctx| closure.fmt_text(f, ctx))?;
            }
        }
        match &plan.source_key {
            Some(source_key) => {
                let source_key = mode.seq(source_key, None);
                let source_key = CompactScalars(source_key);
                writeln!(
                    f,
                    "{}source={{ relation={}, key=[{}] }}",
                    ctx.indent, &plan.source_relation, source_key
                )?
            }
            None => writeln!(
                f,
                "{}source={{ relation={}, key=[] }}",
                ctx.indent, &plan.source_relation
            )?,
        };
        Ok(())
    }
}

impl DisplayText<PlanRenderingContext<'_, Plan>> for LinearStagePlan {
    fn fmt_text(
        &self,
        f: &mut fmt::Formatter<'_>,
        ctx: &mut PlanRenderingContext<'_, Plan>,
    ) -> fmt::Result {
        let mode = HumanizedExplain::new(ctx.config.redacted);

        let plan = self;
        if !plan.closure.is_identity() {
            writeln!(f, "{}closure", ctx.indent)?;
            ctx.indented(|ctx| plan.closure.fmt_text(f, ctx))?;
        }
        {
            let lookup_relation = &plan.lookup_relation;
            let lookup_key = CompactScalarSeq(&plan.lookup_key);
            writeln!(
                f,
                "{}lookup={{ relation={}, key=[{}] }}",
                ctx.indent, lookup_relation, lookup_key
            )?;
        }
        {
            let stream_key = mode.seq(&plan.stream_key, None);
            let stream_key = CompactScalars(stream_key);
            let stream_thinning = Indices(&plan.stream_thinning);
            writeln!(
                f,
                "{}stream={{ key=[{}], thinning=({}) }}",
                ctx.indent, stream_key, stream_thinning
            )?;
        }
        Ok(())
    }
}

impl DisplayText<PlanRenderingContext<'_, Plan>> for DeltaJoinPlan {
    fn fmt_text(
        &self,
        f: &mut fmt::Formatter<'_>,
        ctx: &mut PlanRenderingContext<'_, Plan>,
    ) -> fmt::Result {
        for (i, plan) in self.path_plans.iter().enumerate() {
            writeln!(f, "{}plan_path[{}]", ctx.indent, i)?;
            ctx.indented(|ctx| plan.fmt_text(f, ctx))?;
        }
        Ok(())
    }
}

impl DisplayText<PlanRenderingContext<'_, Plan>> for DeltaPathPlan {
    fn fmt_text(
        &self,
        f: &mut fmt::Formatter<'_>,
        ctx: &mut PlanRenderingContext<'_, Plan>,
    ) -> fmt::Result {
        let mode = HumanizedExplain::new(ctx.config.redacted);
        let plan = self;
        if let Some(closure) = plan.final_closure.as_ref() {
            if !closure.is_identity() {
                writeln!(f, "{}final_closure", ctx.indent)?;
                ctx.indented(|ctx| closure.fmt_text(f, ctx))?;
            }
        }
        for (i, plan) in plan.stage_plans.iter().enumerate().rev() {
            writeln!(f, "{}delta_stage[{}]", ctx.indent, i)?;
            ctx.indented(|ctx| plan.fmt_text(f, ctx))?;
        }
        if !plan.initial_closure.is_identity() {
            writeln!(f, "{}initial_closure", ctx.indent)?;
            ctx.indented(|ctx| plan.initial_closure.fmt_text(f, ctx))?;
        }
        {
            let source_relation = &plan.source_relation;
            let source_key = mode.seq(&plan.source_key, None);
            let source_key = CompactScalars(source_key);
            writeln!(
                f,
                "{}source={{ relation={}, key=[{}] }}",
                ctx.indent, source_relation, source_key
            )?;
        }
        Ok(())
    }
}

impl DisplayText<PlanRenderingContext<'_, Plan>> for DeltaStagePlan {
    fn fmt_text(
        &self,
        f: &mut fmt::Formatter<'_>,
        ctx: &mut PlanRenderingContext<'_, Plan>,
    ) -> fmt::Result {
        let mode = HumanizedExplain::new(ctx.config.redacted);
        let plan = self;
        if !plan.closure.is_identity() {
            writeln!(f, "{}closure", ctx.indent)?;
            ctx.indented(|ctx| plan.closure.fmt_text(f, ctx))?;
        }
        {
            let lookup_relation = &plan.lookup_relation;
            let lookup_key = mode.seq(&plan.lookup_key, None);
            let lookup_key = CompactScalars(lookup_key);
            writeln!(
                f,
                "{}lookup={{ relation={}, key=[{}] }}",
                ctx.indent, lookup_relation, lookup_key
            )?;
        }
        {
            let stream_key = mode.seq(&plan.stream_key, None);
            let stream_key = CompactScalars(stream_key);
            let stream_thinning = mode.seq(&plan.stream_thinning, None);
            let stream_thinning = CompactScalars(stream_thinning);
            writeln!(
                f,
                "{}stream={{ key=[{}], thinning=({}) }}",
                ctx.indent, stream_key, stream_thinning
            )?;
        }
        Ok(())
    }
}

impl DisplayText<PlanRenderingContext<'_, Plan>> for JoinClosure {
    fn fmt_text(
        &self,
        f: &mut fmt::Formatter<'_>,
        ctx: &mut PlanRenderingContext<'_, Plan>,
    ) -> fmt::Result {
        let mode = HumanizedExplain::new(ctx.config.redacted);
        mode.expr(self.before.deref(), None).fmt_text(f, ctx)?;
        if !self.ready_equivalences.is_empty() {
            let equivalences = separated(
                " AND ",
                self.ready_equivalences
                    .iter()
                    .map(|equivalence| separated(" = ", mode.seq(equivalence, None))),
            );
            writeln!(f, "{}ready_equivalences={}", ctx.indent, equivalences)?;
        }
        Ok(())
    }
}

impl DisplayText<PlanRenderingContext<'_, Plan>> for AccumulablePlan {
    fn fmt_text(
        &self,
        f: &mut fmt::Formatter<'_>,
        ctx: &mut PlanRenderingContext<'_, Plan>,
    ) -> fmt::Result {
        let mode = HumanizedExplain::new(ctx.config.redacted);
        // full_aggrs (skipped because they are repeated in simple_aggrs ∪ distinct_aggrs)
        // for (i, aggr) in self.full_aggrs.iter().enumerate() {
        //     write!(f, "{}full_aggrs[{}]=", ctx.indent, i)?;
        //     aggr.fmt_text(f, &mut ())?;
        //     writeln!(f)?;
        // }
        // simple_aggrs
        for (i, (i_aggs, i_datum, agg)) in self.simple_aggrs.iter().enumerate() {
            let agg = mode.expr(agg, None);
            write!(f, "{}simple_aggrs[{}]=", ctx.indent, i)?;
            writeln!(f, "({}, {}, {})", i_aggs, i_datum, agg)?;
        }
        // distinct_aggrs
        for (i, (i_aggs, i_datum, agg)) in self.distinct_aggrs.iter().enumerate() {
            let agg = mode.expr(agg, None);
            write!(f, "{}distinct_aggrs[{}]=", ctx.indent, i)?;
            writeln!(f, "({}, {}, {})", i_aggs, i_datum, agg)?;
        }
        Ok(())
    }
}

impl DisplayText<PlanRenderingContext<'_, Plan>> for HierarchicalPlan {
    fn fmt_text(
        &self,
        f: &mut fmt::Formatter<'_>,
        ctx: &mut PlanRenderingContext<'_, Plan>,
    ) -> fmt::Result {
        let mode = HumanizedExplain::new(ctx.config.redacted);
        match self {
            HierarchicalPlan::Monotonic(plan) => {
                let aggr_funcs = mode.seq(&plan.aggr_funcs, None);
                let aggr_funcs = separated(", ", aggr_funcs);
                writeln!(f, "{}aggr_funcs=[{}]", ctx.indent, aggr_funcs)?;
                let skips = separated(", ", &plan.skips);
                writeln!(f, "{}skips=[{}]", ctx.indent, skips)?;
                writeln!(f, "{}monotonic", ctx.indent)?;
                if plan.must_consolidate {
                    writeln!(f, "{}must_consolidate", ctx.indent)?;
                }
            }
            HierarchicalPlan::Bucketed(plan) => {
                let aggr_funcs = mode.seq(&plan.aggr_funcs, None);
                let aggr_funcs = separated(", ", aggr_funcs);
                writeln!(f, "{}aggr_funcs=[{}]", ctx.indent, aggr_funcs)?;
                let skips = separated(", ", &plan.skips);
                writeln!(f, "{}skips=[{}]", ctx.indent, skips)?;
                let buckets = separated(", ", &plan.buckets);
                writeln!(f, "{}buckets=[{}]", ctx.indent, buckets)?;
            }
        }
        Ok(())
    }
}

impl DisplayText<PlanRenderingContext<'_, Plan>> for BasicPlan {
    fn fmt_text(
        &self,
        f: &mut fmt::Formatter<'_>,
        ctx: &mut PlanRenderingContext<'_, Plan>,
    ) -> fmt::Result {
        let mode = HumanizedExplain::new(ctx.config.redacted);
        match self {
            BasicPlan::Single(idx, agg) => {
                let agg = mode.expr(agg, None);
                writeln!(f, "{}aggr=({}, {})", ctx.indent, idx, agg)?;
            }
            BasicPlan::Multiple(aggs) => {
                for (i, (i_datum, agg)) in aggs.iter().enumerate() {
                    let agg = mode.expr(agg, None);
                    writeln!(f, "{}aggrs[{}]=({}, {})", ctx.indent, i, i_datum, agg)?;
                }
            }
        }
        Ok(())
    }
}

impl DisplayText<PlanRenderingContext<'_, Plan>> for CollationPlan {
    fn fmt_text(
        &self,
        f: &mut fmt::Formatter<'_>,
        ctx: &mut PlanRenderingContext<'_, Plan>,
    ) -> fmt::Result {
        {
            use crate::plan::reduce::ReductionType;
            let aggregate_types = &self
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
        if let Some(plan) = &self.accumulable {
            writeln!(f, "{}accumulable", ctx.indent)?;
            ctx.indented(|ctx| plan.fmt_text(f, ctx))?;
        }
        if let Some(plan) = &self.hierarchical {
            writeln!(f, "{}hierarchical", ctx.indent)?;
            ctx.indented(|ctx| plan.fmt_text(f, ctx))?;
        }
        if let Some(plan) = &self.basic {
            writeln!(f, "{}basic", ctx.indent)?;
            ctx.indented(|ctx| plan.fmt_text(f, ctx))?;
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

impl<'a> From<&'a (Vec<MirScalarExpr>, BTreeMap<usize, usize>, Vec<usize>)> for Arrangement<'a> {
    fn from(
        (key, permutation, thinning): &'a (Vec<MirScalarExpr>, BTreeMap<usize, usize>, Vec<usize>),
    ) -> Self {
        Arrangement {
            key,
            permutation: Permutation(permutation),
            thinning,
        }
    }
}

impl<'a> DisplayText<PlanRenderingContext<'_, Plan>> for Arrangement<'a> {
    fn fmt_text(
        &self,
        f: &mut fmt::Formatter<'_>,
        ctx: &mut PlanRenderingContext<'_, Plan>,
    ) -> fmt::Result {
        let mode = HumanizedExplain::new(ctx.config.redacted);
        // prepare key
        let key = mode.seq(self.key, None);
        let key = CompactScalars(key);
        // prepare perumation map
        let permutation = &self.permutation;
        // prepare thinning
        let thinning = Indices(self.thinning);
        // write the arrangement spec
        write!(
            f,
            "{{ key=[{}], permutation={}, thinning=({}) }}",
            key, permutation, thinning
        )
    }
}

/// Helper struct for rendering a permutation.
struct Permutation<'a>(&'a BTreeMap<usize, usize>);

impl<'a> fmt::Display for Permutation<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut pairs = vec![];
        for (x, y) in self.0.iter().filter(|(x, y)| x != y) {
            pairs.push(format!("#{}: #{}", x, y));
        }

        if pairs.len() > 0 {
            write!(f, "{{{}}}", separated(", ", pairs))
        } else {
            write!(f, "id")
        }
    }
}

/// Annotations for physical plans.
struct PlanAnnotations {
    config: ExplainConfig,
    node_id: NodeId,
}

// The current implementation deviates from the `AnnotatedPlan` used in `Mir~`-based plans. This is
// fine, since at the moment the only attribute we are going to explain is the `node_id`, which at
// the moment is kept inline with the `Plan` variants. If at some point in the future we want to
// start deriving and printing attributes that are derived ad-hoc, however, we might want to adopt
// `AnnotatedPlan` here as well.
impl PlanAnnotations {
    fn new(config: ExplainConfig, plan: &Plan) -> Self {
        let node_id = plan.node_id();
        Self { config, node_id }
    }
}

impl fmt::Display for PlanAnnotations {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.config.node_ids {
            f.debug_struct(" //")
                .field("node_id", &self.node_id)
                .finish()
        } else {
            // No physical plan annotations enabled.
            Ok(())
        }
    }
}
