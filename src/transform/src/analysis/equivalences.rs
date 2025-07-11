// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An analysis that reports all known-equivalent expressions for each relation.
//!
//! Expressions are equivalent at a relation if they are certain to evaluate to
//! the same `Datum` for all records in the relation.
//!
//! Equivalences are recorded in an `EquivalenceClasses`, which lists all known
//! equivalences classes, each a list of equivalent expressions.

use std::collections::BTreeMap;
use std::fmt::Formatter;

use mz_expr::explain::{HumanizedExplain, HumanizerMode};
use mz_expr::{AggregateFunc, Id, MirRelationExpr, MirScalarExpr};
use mz_ore::str::{bracketed, separated};
use mz_repr::{ColumnType, Datum};

use crate::analysis::{Analysis, Lattice};
use crate::analysis::{Arity, RelationType};
use crate::analysis::{Derived, DerivedBuilder};

/// Pulls up and pushes down predicate information represented as equivalences
#[derive(Debug, Default)]
pub struct Equivalences;

impl Analysis for Equivalences {
    // A `Some(list)` indicates a list of classes of equivalent expressions.
    // A `None` indicates all expressions are equivalent, including contradictions;
    // this is only possible for the empty collection, and as an initial result for
    // unconstrained recursive terms.
    type Value = Option<EquivalenceClasses>;

    fn announce_dependencies(builder: &mut DerivedBuilder) {
        builder.require(Arity);
        builder.require(RelationType); // needed for expression reduction.
    }

    fn derive(
        &self,
        expr: &MirRelationExpr,
        index: usize,
        results: &[Self::Value],
        depends: &Derived,
    ) -> Self::Value {
        let mut equivalences = match expr {
            MirRelationExpr::Constant { rows, typ } => {
                // Trawl `rows` for any constant information worth recording.
                // Literal columns may be valuable; non-nullability could be too.
                let mut equivalences = EquivalenceClasses::default();
                if let Ok([(row, _cnt), rows @ ..]) = rows.as_deref() {
                    // Vector of `Option<Datum>` which becomes `None` once a column has a second datum.
                    let len = row.iter().count();
                    let mut common = Vec::with_capacity(len);
                    common.extend(row.iter().map(Some));
                    // Prep initial nullability information.
                    let mut nullable_cols = common
                        .iter()
                        .map(|datum| datum == &Some(Datum::Null))
                        .collect::<Vec<_>>();

                    for (row, _cnt) in rows.iter() {
                        for ((datum, common), nullable) in row
                            .iter()
                            .zip(common.iter_mut())
                            .zip(nullable_cols.iter_mut())
                        {
                            if Some(datum) != *common {
                                *common = None;
                            }
                            if datum == Datum::Null {
                                *nullable = true;
                            }
                        }
                    }
                    for (index, common) in common.into_iter().enumerate() {
                        if let Some(datum) = common {
                            equivalences.classes.push(vec![
                                MirScalarExpr::column(index),
                                MirScalarExpr::literal_ok(
                                    datum,
                                    typ.column_types[index].scalar_type.clone(),
                                ),
                            ]);
                        }
                    }
                    // If any columns are non-null, introduce this fact.
                    if nullable_cols.iter().any(|x| !*x) {
                        let mut class = vec![MirScalarExpr::literal_false()];
                        for (index, nullable) in nullable_cols.iter().enumerate() {
                            if !*nullable {
                                class.push(MirScalarExpr::column(index).call_is_null());
                            }
                        }
                        equivalences.classes.push(class);
                    }
                }
                Some(equivalences)
            }
            MirRelationExpr::Get { id, typ, .. } => {
                let mut equivalences = Some(EquivalenceClasses::default());
                // Find local identifiers, but nothing for external identifiers.
                if let Id::Local(id) = id {
                    if let Some(offset) = depends.bindings().get(id) {
                        // It is possible we have derived nothing for a recursive term
                        if let Some(result) = results.get(*offset) {
                            equivalences.clone_from(result);
                        } else {
                            // No top element was prepared.
                            // This means we are executing pessimistically,
                            // but perhaps we must because optimism is off.
                        }
                    }
                }
                // Incorporate statements about column nullability.
                let mut non_null_cols = vec![MirScalarExpr::literal_false()];
                for (index, col_type) in typ.column_types.iter().enumerate() {
                    if !col_type.nullable {
                        non_null_cols.push(MirScalarExpr::column(index).call_is_null());
                    }
                }
                if non_null_cols.len() > 1 {
                    if let Some(equivalences) = equivalences.as_mut() {
                        equivalences.classes.push(non_null_cols);
                    }
                }

                equivalences
            }
            MirRelationExpr::Let { .. } => results.get(index - 1).unwrap().clone(),
            MirRelationExpr::LetRec { .. } => results.get(index - 1).unwrap().clone(),
            MirRelationExpr::Project { outputs, .. } => {
                // restrict equivalences, and introduce equivalences for repeated outputs.
                let mut equivalences = results.get(index - 1).unwrap().clone();
                equivalences
                    .as_mut()
                    .map(|e| e.project(outputs.iter().cloned()));
                equivalences
            }
            MirRelationExpr::Map { scalars, .. } => {
                // introduce equivalences for new columns and expressions that define them.
                let mut equivalences = results.get(index - 1).unwrap().clone();
                if let Some(equivalences) = &mut equivalences {
                    let input_arity = depends.results::<Arity>()[index - 1];
                    for (pos, expr) in scalars.iter().enumerate() {
                        equivalences
                            .classes
                            .push(vec![MirScalarExpr::column(input_arity + pos), expr.clone()]);
                    }
                }
                equivalences
            }
            MirRelationExpr::FlatMap { .. } => results.get(index - 1).unwrap().clone(),
            MirRelationExpr::Filter { predicates, .. } => {
                let mut equivalences = results.get(index - 1).unwrap().clone();
                if let Some(equivalences) = &mut equivalences {
                    let mut class = predicates.clone();
                    class.push(MirScalarExpr::literal_ok(
                        Datum::True,
                        mz_repr::ScalarType::Bool,
                    ));
                    equivalences.classes.push(class);
                }
                equivalences
            }
            MirRelationExpr::Join { equivalences, .. } => {
                // Collect equivalences from all inputs;
                let expr_index = index;
                let mut children = depends
                    .children_of_rev(expr_index, expr.children().count())
                    .collect::<Vec<_>>();
                children.reverse();

                let arity = depends.results::<Arity>();
                let mut columns = 0;
                let mut result = Some(EquivalenceClasses::default());
                for child in children.into_iter() {
                    let input_arity = arity[child];
                    let equivalences = results[child].clone();
                    if let Some(mut equivalences) = equivalences {
                        let permutation = (columns..(columns + input_arity)).collect::<Vec<_>>();
                        equivalences.permute(&permutation);
                        result
                            .as_mut()
                            .map(|e| e.classes.extend(equivalences.classes));
                    } else {
                        result = None;
                    }
                    columns += input_arity;
                }

                // Fold join equivalences into our results.
                result
                    .as_mut()
                    .map(|e| e.classes.extend(equivalences.iter().cloned()));
                result
            }
            MirRelationExpr::Reduce {
                group_key,
                aggregates,
                ..
            } => {
                let input_arity = depends.results::<Arity>()[index - 1];
                let mut equivalences = results.get(index - 1).unwrap().clone();
                if let Some(equivalences) = &mut equivalences {
                    // Introduce keys column equivalences as if a map, then project to those columns.
                    // This should retain as much information as possible about these columns.
                    for (pos, expr) in group_key.iter().enumerate() {
                        equivalences
                            .classes
                            .push(vec![MirScalarExpr::column(input_arity + pos), expr.clone()]);
                    }

                    // Having added classes to `equivalences`, we should minimize the classes to fold the
                    // information in before applying the `project`, to set it up for success.
                    equivalences.minimize(None);

                    // Grab a copy of the equivalences with key columns added to use in aggregate reasoning.
                    let extended = equivalences.clone();
                    // Now project down the equivalences, as we will extend them in terms of the output columns.
                    equivalences.project(input_arity..(input_arity + group_key.len()));

                    // TODO: MIN, MAX, ANY, ALL aggregates pass through all certain properties of their columns.
                    // They also pass through equivalences of them and other constant columns (e.g. key columns).
                    // However, it is not correct to simply project onto these columns, as relationships amongst
                    // aggregate columns may no longer be preserved. MAX(col) != MIN(col) even though col = col.
                    // The correct thing to do is treat the reduce as a join between single-aggregate reductions,
                    // where each single MIN/MAX/ANY/ALL aggregate propagates equivalences.
                    for (index, aggregate) in aggregates.iter().enumerate() {
                        if aggregate_is_input(&aggregate.func) {
                            let mut temp_equivs = extended.clone();
                            temp_equivs.classes.push(vec![
                                MirScalarExpr::column(input_arity + group_key.len()),
                                aggregate.expr.clone(),
                            ]);
                            temp_equivs.minimize(None);
                            temp_equivs.project(input_arity..(input_arity + group_key.len() + 1));
                            let columns = (0..group_key.len())
                                .chain(std::iter::once(group_key.len() + index))
                                .collect::<Vec<_>>();
                            temp_equivs.permute(&columns[..]);
                            equivalences.classes.extend(temp_equivs.classes);
                        }
                    }
                }
                equivalences
            }
            MirRelationExpr::TopK { .. } => results.get(index - 1).unwrap().clone(),
            MirRelationExpr::Negate { .. } => results.get(index - 1).unwrap().clone(),
            MirRelationExpr::Threshold { .. } => results.get(index - 1).unwrap().clone(),
            MirRelationExpr::Union { .. } => {
                let expr_index = index;
                let mut child_equivs = depends
                    .children_of_rev(expr_index, expr.children().count())
                    .flat_map(|c| &results[c]);
                if let Some(first) = child_equivs.next() {
                    Some(first.union_many(child_equivs))
                } else {
                    None
                }
            }
            MirRelationExpr::ArrangeBy { .. } => results.get(index - 1).unwrap().clone(),
        };

        let expr_type = depends.results::<RelationType>()[index].as_ref();
        equivalences
            .as_mut()
            .map(|e| e.minimize(expr_type.map(|x| &x[..])));
        equivalences
    }

    fn lattice() -> Option<Box<dyn Lattice<Self::Value>>> {
        Some(Box::new(EQLattice))
    }
}

struct EQLattice;

impl Lattice<Option<EquivalenceClasses>> for EQLattice {
    fn top(&self) -> Option<EquivalenceClasses> {
        None
    }

    fn meet_assign(
        &self,
        a: &mut Option<EquivalenceClasses>,
        b: Option<EquivalenceClasses>,
    ) -> bool {
        match (&mut *a, b) {
            (_, None) => false,
            (None, b) => {
                *a = b;
                true
            }
            (Some(a), Some(b)) => {
                let mut c = a.union(&b);
                std::mem::swap(a, &mut c);
                a != &mut c
            }
        }
    }
}

/// A compact representation of classes of expressions that must be equivalent.
///
/// Each "class" contains a list of expressions, each of which must be `Eq::eq` equal.
/// Ideally, the first element is the "simplest", e.g. a literal or column reference,
/// and any other element of that list can be replaced by it.
///
/// The classes are meant to be minimized, with each expression as reduced as it can be,
/// and all classes sharing an element merged.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Default, Debug)]
pub struct EquivalenceClasses {
    /// Multiple lists of equivalent expressions, each representing an equivalence class.
    ///
    /// The first element should be the "canonical" simplest element, that any other element
    /// can be replaced by.
    /// These classes are unified whenever possible, to minimize the number of classes.
    /// They are only guaranteed to form an equivalence relation after a call to `minimize`,
    /// which refreshes both `self.classes` and `self.remap`.
    pub classes: Vec<Vec<MirScalarExpr>>,

    /// An expression simplification map.
    ///
    /// This map reflects an equivalence relation based on a prior version of `self.classes`.
    /// As users may add to `self.classes`, `self.remap` may become stale. We refresh `remap`
    /// only in `self.refresh()`, to the equivalence relation that derives from `self.classes`.
    ///
    /// It is important to `self.remap.clear()` if you invalidate it by mutating rather than
    /// appending to `self.classes`. This will be corrected in the next call to `self.refresh()`,
    /// but until then `remap` could be arbitrarily wrong. This should be improved in the future.
    remap: BTreeMap<MirScalarExpr, MirScalarExpr>,
}

/// Raw printing of [`EquivalenceClasses`] with default expression humanization.
/// Don't use this in `EXPLAIN`! For redaction, column name support, etc., see
/// [`HumanizedEquivalenceClasses`].
impl std::fmt::Display for EquivalenceClasses {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        HumanizedEquivalenceClasses {
            equivalence_classes: self,
            cols: None,
            mode: HumanizedExplain::default(),
        }
        .fmt(f)
    }
}

/// Wrapper struct for human-readable printing of expressions inside [`EquivalenceClasses`].
/// (Similar to `HumanizedExpr`. Unfortunately, we can't just use `HumanizedExpr` here, because
/// we'd need to `impl Display for HumanizedExpr<'a, EquivalenceClasses, M>`, but neither
/// `Display` nor `HumanizedExpr` is defined in this crate.)
#[derive(Debug)]
pub struct HumanizedEquivalenceClasses<'a, M = HumanizedExplain> {
    /// The [`EquivalenceClasses`] to be humanized.
    pub equivalence_classes: &'a EquivalenceClasses,
    /// An optional vector of inferred column names to be used when rendering
    /// column references in expressions.
    pub cols: Option<&'a Vec<String>>,
    /// The rendering mode to use. See `HumanizerMode` for details.
    pub mode: M,
}

impl std::fmt::Display for HumanizedEquivalenceClasses<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // Only show `classes`.
        // (The following hopefully avoids allocating any of the intermediate composite strings.)
        let classes = self.equivalence_classes.classes.iter().map(|class| {
            format!(
                "{}",
                bracketed(
                    "[",
                    "]",
                    separated(
                        ", ",
                        class.iter().map(|expr| self.mode.expr(expr, self.cols))
                    )
                )
            )
        });
        write!(f, "{}", bracketed("[", "]", separated(", ", classes)))
    }
}

impl EquivalenceClasses {
    /// Comparator function for the complexity of scalar expressions. Simpler expressions are
    /// smaller. Can be used when we need to decide which of several equivalent expressions to use.
    pub fn mir_scalar_expr_complexity(
        e1: &MirScalarExpr,
        e2: &MirScalarExpr,
    ) -> std::cmp::Ordering {
        use MirScalarExpr::*;
        use std::cmp::Ordering::*;
        match (e1, e2) {
            (Literal(_, _), Literal(_, _)) => e1.cmp(e2),
            (Literal(_, _), _) => Less,
            (_, Literal(_, _)) => Greater,
            (Column(_, _), Column(_, _)) => e1.cmp(e2),
            (Column(_, _), _) => Less,
            (_, Column(_, _)) => Greater,
            (x, y) => {
                // General expressions should be ordered by their size,
                // to ensure we only simplify expressions by substitution.
                // If same size, then fall back to the expressions' Ord.
                match x.size().cmp(&y.size()) {
                    Equal => x.cmp(y),
                    other => other,
                }
            }
        }
    }

    /// Sorts and deduplicates each class, removing literal errors.
    ///
    /// This method does not ensure equivalence relation structure, but instead performs
    /// only minimal structural clean-up.
    fn tidy(&mut self) {
        for class in self.classes.iter_mut() {
            // Remove all literal errors, as they cannot be equated to other things.
            class.sort_by(Self::mir_scalar_expr_complexity);
            class.dedup();
        }
        self.classes.retain(|c| c.len() > 1);
        self.classes.sort();
        self.classes.dedup();
    }

    /// Restore equivalence relation structure to `self.classes` and refresh `self.remap`.
    ///
    /// This method takes roughly linear time, and returns true iff `self.remap` has changed.
    /// This is the only method that refreshes `self.remap`, and is a perfect place to decide
    /// whether the equivalence classes it represents have experienced any changes since the
    /// last refresh.
    fn refresh(&mut self) -> bool {
        self.tidy();

        // remap may already be the correct answer, and if so we should avoid the work of rebuilding it.
        // If it contains the same number of expressions as `self.classes`, and for every expression in
        // `self.classes` the two agree on the representative, they are identical.
        if self.remap.len() == self.classes.iter().map(|c| c.len()).sum::<usize>()
            && self
                .classes
                .iter()
                .all(|c| c.iter().all(|e| self.remap.get(e) == Some(&c[0])))
        {
            // No change, so return false.
            return false;
        }

        // Optimistically build the `remap` we would want.
        // Note if any unions would be required, in which case we have further work to do,
        // including re-forming `self.classes`.
        let mut union_find = BTreeMap::default();
        let mut dirtied = false;
        for class in self.classes.iter() {
            for expr in class.iter() {
                if let Some(other) = union_find.insert(expr.clone(), class[0].clone()) {
                    // A merge is required, but have the more complex expression point at the simpler one.
                    // This allows `union_find` to end as the `remap` for the new `classes` we form, with
                    // the only required work being compressing all the paths.
                    if Self::mir_scalar_expr_complexity(&other, &class[0])
                        == std::cmp::Ordering::Less
                    {
                        union_find.union(&class[0], &other);
                    } else {
                        union_find.union(&other, &class[0]);
                    }
                    dirtied = true;
                }
            }
        }
        if dirtied {
            let mut classes: BTreeMap<_, Vec<_>> = BTreeMap::default();
            for class in self.classes.drain(..) {
                for expr in class {
                    let root: MirScalarExpr = union_find.find(&expr).unwrap().clone();
                    classes.entry(root).or_default().push(expr);
                }
            }
            self.classes = classes.into_values().collect();
            self.tidy();
        }

        let changed = self.remap != union_find;
        self.remap = union_find;
        changed
    }

    /// Update `self` to maintain the same equivalences which potentially reducing along `Ord::le`.
    ///
    /// Informally this means simplifying constraints, removing redundant constraints, and unifying equivalence classes.
    pub fn minimize(&mut self, columns: Option<&[ColumnType]>) {
        // Repeatedly, we reduce each of the classes themselves, then unify the classes.
        // This should strictly reduce complexity, and reach a fixed point.
        // Ideally it is *confluent*, arriving at the same fixed point no matter the order of operations.

        // We should not rely on nullability information present in `column_types`. (Doing this
        // every time just before calling `reduce` was found to be a bottleneck during incident-217,
        // so now we do this nullability tweaking only once here.)
        let mut columns = columns.map(|x| x.to_vec());
        let mut nonnull = Vec::new();
        if let Some(columns) = columns.as_mut() {
            for (index, col) in columns.iter_mut().enumerate() {
                let is_null = MirScalarExpr::column(index).call_is_null();
                if !col.nullable
                    && self
                        .remap
                        .get(&is_null)
                        .map(|e| !e.is_literal_false())
                        .unwrap_or(true)
                {
                    nonnull.push(is_null);
                }
                col.nullable = true;
            }
        }
        if !nonnull.is_empty() {
            nonnull.push(MirScalarExpr::literal_false());
            self.classes.push(nonnull);
        }

        // Ensure `self.classes` and `self.remap` are equivalence relations.
        // Users are allowed to mutate `self.classes`, so we must perform this normalization at least once.
        // We have also likely mutated `self.classes` just above with non-nullability information.
        self.refresh();

        // Termination will be detected by comparing to the map of equivalence classes.
        let mut previous = Some(self.remap.clone());
        while let Some(prev) = previous {
            // Attempt to add new equivalences.
            let novel = self.expand();
            if !novel.is_empty() {
                self.classes.extend(novel);
                self.refresh();
            }

            // We continue as long as any simplification has occurred.
            // An expression can be simplified, a duplication found, or two classes unified.
            let mut stable = false;
            while !stable {
                stable = !self.minimize_once(columns.as_ref().map(|x| &x[..]));
            }

            // Termination detection.
            if prev != self.remap {
                previous = Some(self.remap.clone());
            } else {
                previous = None;
            }
        }
    }

    /// Proposes new equivalences that are likely to be novel.
    ///
    /// This method invokes `self.implications()` to propose equivalences, and then judges them to be
    /// novel or not based on existing knowledge, reducing the equivalences down to their novel core.
    /// This method may produce non-novel equivalences, due to its inability to perform `MSE::reduce`.
    /// We can end up with e.g. constant expressions that cannot be found until they are so reduced.
    /// The novelty detection is best-effort, and meant to provide a clearer signal and minimize the
    /// number of times we call and amount of work we do in `self.refresh()`.
    fn expand(&self) -> Vec<Vec<MirScalarExpr>> {
        // Consider expanding `self.classes` with novel equivalences.
        let mut novel = self.implications();
        for class in novel.iter_mut() {
            // reduce each expression to its canonical form.
            for expr in class.iter_mut() {
                self.remap.reduce_expr(expr);
            }
            class.sort();
            class.dedup();
            // for a class to be interesting we require at least two elements that do not reference the same root.
            let common_class = class
                .iter()
                .map(|x| self.remap.get(x))
                .reduce(|prev, this| if prev == this { prev } else { None });
            if class.len() == 1 || common_class != Some(None) {
                class.clear();
            }
        }
        novel.retain(|c| !c.is_empty());
        novel
    }

    /// Derives potentially novel equivalences without regard for minimization.
    ///
    /// This is an opportunity to explore equivalences that do not correspond to expression minimization,
    /// and therefore should not be used in `minimize_once`. They are still potentially important, but
    /// required additional guardrails to ensure we reach a fixed point.
    ///
    /// The implications will be introduced into `self.classes` and will prompt a round of minimization,
    /// making it somewhat polite to avoid producing outputs that cannot result in novel equivalences.
    /// For example, before producing a new equivalence, one could check that the involved terms are not
    /// already present in the same class.
    fn implications(&self) -> Vec<Vec<MirScalarExpr>> {
        let mut new_equivalences = Vec::new();

        // If we see `false == IsNull(foo)` we can add the non-null implications of `foo`.
        let mut non_null = std::collections::BTreeSet::default();
        for class in self.classes.iter() {
            if Self::class_contains_literal(class, |e| e == &Ok(Datum::False)) {
                for e in class.iter() {
                    if let MirScalarExpr::CallUnary {
                        func: mz_expr::UnaryFunc::IsNull(_),
                        expr,
                    } = e
                    {
                        expr.non_null_requirements(&mut non_null);
                    }
                }
            }
        }
        // If we see `true == foo` we can add the non-null implications of `foo`.
        // TODO: generalize to arbitrary non-null, non-error literals; at the moment `true == pred` is
        // an important idiom to identify for how we express predicates.
        for class in self.classes.iter() {
            if Self::class_contains_literal(class, |e| e == &Ok(Datum::True)) {
                for expr in class.iter() {
                    expr.non_null_requirements(&mut non_null);
                }
            }
        }
        // Only keep constraints that are not already known.
        // Known constraints will present as `COL(_) IS NULL == false`,
        // which can only happen if `false` is present, and both terms
        // map to the same canonical representative>
        let lit_false = MirScalarExpr::literal_false();
        let target = self.remap.get(&lit_false);
        if target.is_some() {
            non_null.retain(|c| {
                let is_null = MirScalarExpr::column(*c).call_is_null();
                self.remap.get(&is_null) != target
            });
        }
        if !non_null.is_empty() {
            let mut class = Vec::with_capacity(non_null.len() + 1);
            class.push(MirScalarExpr::literal_false());
            class.extend(
                non_null
                    .into_iter()
                    .map(|c| MirScalarExpr::column(c).call_is_null()),
            );
            new_equivalences.push(class);
        }

        // If we see records formed from other expressions, we can equate the expressions with
        // accessors applied to the class of the record former. In `minimize_once` we reduce by
        // equivalence class representative before we perform expression simplification, so we
        // shoud be able to just use the expression former, rather than find its representative.
        // The risk, potentially, is that we would apply accessors to the record former and then
        // just simplify it away learning nothing.
        for class in self.classes.iter() {
            for expr in class.iter() {
                // Record-forming expressions can equate their accessors and their members.
                if let MirScalarExpr::CallVariadic {
                    func: mz_expr::VariadicFunc::RecordCreate { .. },
                    exprs,
                } = expr
                {
                    for (index, e) in exprs.iter().enumerate() {
                        new_equivalences.push(vec![
                            e.clone(),
                            expr.clone().call_unary(mz_expr::UnaryFunc::RecordGet(
                                mz_expr::func::RecordGet(index),
                            )),
                        ]);
                    }
                }
            }
        }

        // Return all newly established equivalences.
        new_equivalences
    }

    /// A single iteration of minimization, which we expect to repeat but benefit from factoring out.
    ///
    /// This invocation should take roughly linear time.
    /// It starts with equivalence class invariants maintained (closed under transitivity), and then
    ///   1. Performs per-expression reduction, including the class structure to replace subexpressions.
    ///   2. Applies idiom detection to e.g. unpack expressions equivalence to literal true or false.
    ///   3. Restores the equivalence class invariants.
    fn minimize_once(&mut self, columns: Option<&[ColumnType]>) -> bool {
        // 1. Reduce each expression
        //
        // This reduction first looks for subexpression substitutions that can be performed,
        // and then applies expression reduction if column type information is provided.
        for class in self.classes.iter_mut() {
            for expr in class.iter_mut() {
                self.remap.reduce_child(expr);
                if let Some(columns) = columns {
                    expr.reduce(columns);
                }
            }
        }

        // 2. Identify idioms
        //    E.g. If Eq(x, y) must be true, we can introduce classes `[x, y]` and `[false, IsNull(x), IsNull(y)]`.
        let mut to_add = Vec::new();
        for class in self.classes.iter_mut() {
            if Self::class_contains_literal(class, |e| e == &Ok(Datum::True)) {
                for expr in class.iter() {
                    // If Eq(x, y) must be true, we can introduce classes `[x, y]` and `[false, IsNull(x), IsNull(y)]`.
                    // This substitution replaces a complex expression with several smaller expressions, and cannot
                    // cycle if we follow that practice.
                    if let MirScalarExpr::CallBinary {
                        func: mz_expr::BinaryFunc::Eq,
                        expr1,
                        expr2,
                    } = expr
                    {
                        to_add.push(vec![*expr1.clone(), *expr2.clone()]);
                        to_add.push(vec![
                            MirScalarExpr::literal_false(),
                            expr1.clone().call_is_null(),
                            expr2.clone().call_is_null(),
                        ]);
                    }
                }
                // Remove the more complex form of the expression.
                class.retain(|expr| {
                    if let MirScalarExpr::CallBinary {
                        func: mz_expr::BinaryFunc::Eq,
                        ..
                    } = expr
                    {
                        false
                    } else {
                        true
                    }
                });
                for expr in class.iter() {
                    // If TRUE == NOT(X) then FALSE == X is a simpler form.
                    if let MirScalarExpr::CallUnary {
                        func: mz_expr::UnaryFunc::Not(_),
                        expr: e,
                    } = expr
                    {
                        to_add.push(vec![MirScalarExpr::literal_false(), (**e).clone()]);
                    }
                }
                class.retain(|expr| {
                    if let MirScalarExpr::CallUnary {
                        func: mz_expr::UnaryFunc::Not(_),
                        ..
                    } = expr
                    {
                        false
                    } else {
                        true
                    }
                });
            }
            if Self::class_contains_literal(class, |e| e == &Ok(Datum::False)) {
                for expr in class.iter() {
                    // If FALSE == NOT(X) then TRUE == X is a simpler form.
                    if let MirScalarExpr::CallUnary {
                        func: mz_expr::UnaryFunc::Not(_),
                        expr: e,
                    } = expr
                    {
                        to_add.push(vec![MirScalarExpr::literal_true(), (**e).clone()]);
                    }
                }
                class.retain(|expr| {
                    if let MirScalarExpr::CallUnary {
                        func: mz_expr::UnaryFunc::Not(_),
                        ..
                    } = expr
                    {
                        false
                    } else {
                        true
                    }
                });
            }
        }
        self.classes.extend(to_add);

        // 3. Restore equivalence relation structure and observe if any changes result.
        self.refresh()
    }

    /// Produce the equivalences present in both inputs.
    pub fn union(&self, other: &Self) -> Self {
        self.union_many([other])
    }

    /// The equivalence classes of terms equivalent in all inputs.
    ///
    /// This method relies on the `remap` member of each input, and bases the intersection on these rather than `classes`.
    /// This means one should ensure `minimize()` has been called on all inputs, or risk getting a stale, but conservatively
    /// correct, result.
    ///
    /// This method currently misses opportunities, because it only looks for exactly matches in expressions,
    /// which may not include all possible matches. For example, `f(#1) == g(#1)` may exist in one class, but
    /// in another class where `#0 == #1` it may exist as `f(#0) == g(#0)`.
    pub fn union_many<'a, I>(&self, others: I) -> Self
    where
        I: IntoIterator<Item = &'a Self>,
    {
        // List of expressions in the intersection, and a proxy equivalence class identifier.
        let mut intersection: Vec<(&MirScalarExpr, usize)> = Default::default();
        // Map from expression to a proxy equivalence class identifier.
        let mut rekey: BTreeMap<&MirScalarExpr, usize> = Default::default();
        for (key, val) in self.remap.iter() {
            if !rekey.contains_key(val) {
                rekey.insert(val, rekey.len());
            }
            intersection.push((key, rekey[val]));
        }
        for other in others {
            // Map from proxy equivalence class identifier and equivalence class expr to a new proxy identifier.
            let mut rekey: BTreeMap<(usize, &MirScalarExpr), usize> = Default::default();
            intersection.retain_mut(|(key, idx)| {
                if let Some(val) = other.remap.get(key) {
                    if !rekey.contains_key(&(*idx, val)) {
                        rekey.insert((*idx, val), rekey.len());
                    }
                    *idx = rekey[&(*idx, val)];
                    true
                } else {
                    false
                }
            });
        }
        let mut classes: BTreeMap<_, Vec<MirScalarExpr>> = Default::default();
        for (key, vals) in intersection {
            classes.entry(vals).or_default().push(key.clone())
        }
        let classes = classes.into_values().collect::<Vec<_>>();
        let mut equivalences = EquivalenceClasses {
            classes,
            remap: Default::default(),
        };
        equivalences.minimize(None);
        equivalences
    }

    /// Permutes each expression, looking up each column reference in `permutation` and replacing with what it finds.
    pub fn permute(&mut self, permutation: &[usize]) {
        for class in self.classes.iter_mut() {
            for expr in class.iter_mut() {
                expr.permute(permutation);
            }
        }
        self.remap.clear();
        self.minimize(None);
    }

    /// Subject the constraints to the column projection, reworking and removing equivalences.
    ///
    /// This method should also introduce equivalences representing any repeated columns.
    pub fn project<I>(&mut self, output_columns: I)
    where
        I: IntoIterator<Item = usize> + Clone,
    {
        // Retain the first instance of each column, and record subsequent instances as duplicates.
        let mut dupes = Vec::new();
        let mut remap = BTreeMap::default();
        for (idx, col) in output_columns.into_iter().enumerate() {
            if let Some(pos) = remap.get(&col) {
                dupes.push((*pos, idx));
            } else {
                remap.insert(col, idx);
            }
        }

        // Some expressions may be "localized" in that they only reference columns in `output_columns`.
        // Many expressions may not be localized, but may reference canonical non-localized expressions
        // for classes that contain a localized expression; in that case we can "backport" the localized
        // expression to give expressions referencing the canonical expression a shot at localization.
        //
        // Expressions should only contain instances of canonical expressions, and so we shouldn't need
        // to look any further than backporting those. Backporting should have the property that the simplest
        // localized expression in each class does not contain any non-localized canonical expressions
        // (as that would make it non-localized); our backporting of non-localized canonicals with localized
        // expressions should never fire a second

        // Let's say an expression is "localized" once we are able to rewrite its support in terms of `output_columns`.
        // Not all expressions can be localized, although some of them may be equivalent to localized expressions.
        // As we find localized expressions, we can replace uses of their equivalent representative with them,
        // which may allow further expression localization.
        // We continue the process until no further classes can be localized.

        // A map from representatives to our first localization of their equivalence class.
        let mut localized = false;
        while !localized {
            localized = true;
            let mut current_map = BTreeMap::default();
            for class in self.classes.iter_mut() {
                if !class[0].support().iter().all(|c| remap.contains_key(c)) {
                    if let Some(pos) = class
                        .iter()
                        .position(|e| e.support().iter().all(|c| remap.contains_key(c)))
                    {
                        class.swap(0, pos);
                        localized = false;
                    }
                }
                for expr in class[1..].iter() {
                    current_map.insert(expr.clone(), class[0].clone());
                }
            }

            // attempt to replace representatives with equivalent localizeable expressions.
            for class_index in 0..self.classes.len() {
                for index in 0..self.classes[class_index].len() {
                    current_map.reduce_child(&mut self.classes[class_index][index]);
                }
            }
            // NB: Do *not* `self.minimize()`, as we are developing localizable rather than canonical representatives.
        }

        // Localize all localizable expressions and discard others.
        for class in self.classes.iter_mut() {
            class.retain(|e| e.support().iter().all(|c| remap.contains_key(c)));
            for expr in class.iter_mut() {
                expr.permute_map(&remap);
            }
        }
        self.classes.retain(|c| c.len() > 1);
        // If column repetitions, introduce them as equivalences.
        // We introduce only the equivalence to the first occurrence, and rely on minimization to collect them.
        for (col1, col2) in dupes {
            self.classes.push(vec![
                MirScalarExpr::column(col1),
                MirScalarExpr::column(col2),
            ]);
        }
        self.remap.clear();
        self.minimize(None);
    }

    /// True if any equivalence class contains two distinct non-error literals.
    pub fn unsatisfiable(&self) -> bool {
        for class in self.classes.iter() {
            let mut literal_ok = None;
            for expr in class.iter() {
                if let MirScalarExpr::Literal(Ok(row), _) = expr {
                    if literal_ok.is_some() && literal_ok != Some(row) {
                        return true;
                    } else {
                        literal_ok = Some(row);
                    }
                }
            }
        }
        false
    }

    /// Returns a map that can be used to replace (sub-)expressions.
    pub fn reducer(&self) -> &BTreeMap<MirScalarExpr, MirScalarExpr> {
        &self.remap
    }

    /// Examines the prefix of `class` of literals, looking for any satisfying `predicate`.
    ///
    /// This test bails out as soon as it sees a non-literal, and may have false negatives
    /// if the data are not sorted with literals at the front.
    fn class_contains_literal<P>(class: &[MirScalarExpr], mut predicate: P) -> bool
    where
        P: FnMut(&Result<Datum, &mz_expr::EvalError>) -> bool,
    {
        class
            .iter()
            .take_while(|e| e.is_literal())
            .filter_map(|e| e.as_literal())
            .any(move |e| predicate(&e))
    }
}

/// A type capable of simplifying `MirScalarExpr`s.
pub trait ExpressionReducer {
    /// Attempt to replace `expr` itself with another expression.
    /// Returns true if it does so.
    fn replace(&self, expr: &mut MirScalarExpr) -> bool;
    /// Attempt to replace any subexpressions of `expr` with other expressions.
    /// Returns true if it does so.
    fn reduce_expr(&self, expr: &mut MirScalarExpr) -> bool {
        let mut simplified = false;
        simplified = simplified || self.reduce_child(expr);
        simplified = simplified || self.replace(expr);
        simplified
    }
    /// Attempt to replace any subexpressions of `expr`'s children with other expressions.
    /// Returns true if it does so.
    fn reduce_child(&self, expr: &mut MirScalarExpr) -> bool {
        let mut simplified = false;
        match expr {
            MirScalarExpr::CallBinary { expr1, expr2, .. } => {
                simplified = self.reduce_expr(expr1) || simplified;
                simplified = self.reduce_expr(expr2) || simplified;
            }
            MirScalarExpr::CallUnary { expr, .. } => {
                simplified = self.reduce_expr(expr) || simplified;
            }
            MirScalarExpr::CallVariadic { exprs, .. } => {
                for expr in exprs.iter_mut() {
                    simplified = self.reduce_expr(expr) || simplified;
                }
            }
            MirScalarExpr::If { cond: _, then, els } => {
                // Do not simplify `cond`, as we cannot ensure the simplification
                // continues to hold as expressions migrate around.
                simplified = self.reduce_expr(then) || simplified;
                simplified = self.reduce_expr(els) || simplified;
            }
            _ => {}
        }
        simplified
    }
}

impl ExpressionReducer for BTreeMap<&MirScalarExpr, &MirScalarExpr> {
    /// Perform any exact replacement for `expr`, report if it had an effect.
    fn replace(&self, expr: &mut MirScalarExpr) -> bool {
        if let Some(other) = self.get(expr) {
            if other != &expr {
                expr.clone_from(other);
                return true;
            }
        }
        false
    }
}

impl ExpressionReducer for BTreeMap<MirScalarExpr, MirScalarExpr> {
    /// Perform any exact replacement for `expr`, report if it had an effect.
    fn replace(&self, expr: &mut MirScalarExpr) -> bool {
        if let Some(other) = self.get(expr) {
            if other != expr {
                expr.clone_from(other);
                return true;
            }
        }
        false
    }
}

trait UnionFind<T> {
    /// Sets `self[x]` to the root from `x`, and returns a reference to the root.
    fn find<'a>(&'a mut self, x: &T) -> Option<&'a T>;
    /// Ensures that `x` and `y` have the same root.
    fn union(&mut self, x: &T, y: &T);
}

impl<T: Clone + Ord> UnionFind<T> for BTreeMap<T, T> {
    fn find<'a>(&'a mut self, x: &T) -> Option<&'a T> {
        if !self.contains_key(x) {
            None
        } else {
            if self[x] != self[&self[x]] {
                // Path halving
                let mut y = self[x].clone();
                while y != self[&y] {
                    let grandparent = self[&self[&y]].clone();
                    *self.get_mut(&y).unwrap() = grandparent;
                    y.clone_from(&self[&y]);
                }
                *self.get_mut(x).unwrap() = y;
            }
            Some(&self[x])
        }
    }

    fn union(&mut self, x: &T, y: &T) {
        match (self.find(x).is_some(), self.find(y).is_some()) {
            (true, true) => {
                if self[x] != self[y] {
                    let root_x = self[x].clone();
                    let root_y = self[y].clone();
                    self.insert(root_x, root_y);
                }
            }
            (false, true) => {
                self.insert(x.clone(), self[y].clone());
            }
            (true, false) => {
                self.insert(y.clone(), self[x].clone());
            }
            (false, false) => {
                self.insert(x.clone(), x.clone());
                self.insert(y.clone(), x.clone());
            }
        }
    }
}

/// True iff the aggregate function returns an input datum.
fn aggregate_is_input(aggregate: &AggregateFunc) -> bool {
    match aggregate {
        AggregateFunc::MaxInt16
        | AggregateFunc::MaxInt32
        | AggregateFunc::MaxInt64
        | AggregateFunc::MaxUInt16
        | AggregateFunc::MaxUInt32
        | AggregateFunc::MaxUInt64
        | AggregateFunc::MaxMzTimestamp
        | AggregateFunc::MaxFloat32
        | AggregateFunc::MaxFloat64
        | AggregateFunc::MaxBool
        | AggregateFunc::MaxString
        | AggregateFunc::MaxDate
        | AggregateFunc::MaxTimestamp
        | AggregateFunc::MaxTimestampTz
        | AggregateFunc::MinInt16
        | AggregateFunc::MinInt32
        | AggregateFunc::MinInt64
        | AggregateFunc::MinUInt16
        | AggregateFunc::MinUInt32
        | AggregateFunc::MinUInt64
        | AggregateFunc::MinMzTimestamp
        | AggregateFunc::MinFloat32
        | AggregateFunc::MinFloat64
        | AggregateFunc::MinBool
        | AggregateFunc::MinString
        | AggregateFunc::MinDate
        | AggregateFunc::MinTimestamp
        | AggregateFunc::MinTimestampTz
        | AggregateFunc::Any
        | AggregateFunc::All => true,
        _ => false,
    }
}
