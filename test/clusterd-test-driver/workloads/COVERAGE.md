corpus: 16 workloads kept from 1687 draws, covering 26 surface cells

covered cells:
  Constant/Rows
  Get/PassRaw
  Get/PassArranged
  Get/ArrangementScan
  Get/Collection
  Mfp/Plain/Stream
  Mfp/Plain/Arranged
  Join/Linear/2
  Join/Linear/3
  Join/Linear/4
  Join/Delta/3
  Join/Delta/4
  Reduce/Distinct/Direct/NoMfp
  Reduce/Accumulable/Direct/NoMfp
  Reduce/Bucketed/Direct/NoMfp
  TopK/Basic/Direct
  Negate
  Threshold
  Union/Plain/Direct
  Union/Consolidating/Direct
  ArrangeBy/RawOnly/Direct/Arranged
  ArrangeBy/EmptyKey/Direct/Stream
  ArrangeBy/EmptyKey/Direct/Arranged
  ArrangeBy/One/Direct/Stream
  ArrangeBy/One/Direct/Arranged
  Let

known gaps (cells random MIR cannot reach, with cause):
  Constant/Error
      gen_scalar emits error literals inside expressions, but gen_rel never roots a collection at an error Constant
  Get/ArrangementLookup
      needs literal constraints over an imported index key; gen_rel imports nothing and its Gets carry no key
  Mfp/Temporal/*
      needs an mz_now() predicate; gen_scalar has no unmaterializable functions
  Mfp/*/Lookup
      same as Get/ArrangementLookup: no keyed input to seek into
  FlatMap/*
      gen_rel has no FlatMap arm, so no table function is ever planned
  Reduce/Monotonic*, TopK/Monotonic*
      needs a monotonic input; gen_rel marks every leaf non-monotonic and nothing in the plan establishes monotonicity
  Reduce/BasicSingle, Reduce/BasicMultiple
      needs a non-accumulable, non-hierarchical aggregate (jsonb_agg, string_agg); gen_aggregate's set is all accumulable or hierarchical
  */Bucketed (ArrangementStrategy::TemporalBucketing)
      lowering only chooses it for plans with mz_now() temporal filters, which gen_scalar cannot express
  ArrangeBy/Several
      needs one collection arranged by several keys at once, which the optimizer forms for a join over multiple keys; not reached by the drawn join shapes
  LetRec/*
      gen_rel has no LetRec arm, so no recursive binding is ever planned. Note this is also where the fold oracle goes blind, so these cells need the incremental oracle instead
