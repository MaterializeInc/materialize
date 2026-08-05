corpus: 23 workloads kept from 1687 draws, covering 35 surface cells

covered cells:
  Constant/Rows
  Constant/Error
  Get/PassRaw
  Get/PassArranged
  Get/ArrangementScan
  Get/Collection
  Mfp/Plain/Stream
  Mfp/Plain/Arranged
  FlatMap/Stream/NoMfp
  FlatMap/Stream/MfpAfter
  Join/Linear/2
  Join/Linear/3
  Join/Linear/4
  Join/Delta/3
  Join/Delta/4
  Reduce/Distinct/Direct/NoMfp
  Reduce/Accumulable/Direct/NoMfp
  Reduce/Monotonic/Direct/NoMfp
  Reduce/Bucketed/Direct/NoMfp
  TopK/MonotonicTop1/Direct
  TopK/MonotonicTopKLimited/Direct
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
  LetRec/Unbounded
  LetRec/Limited
  LetRec/LimitedReturnAt

known gaps (cells the corpus does not reach, with cause):
  Get/ArrangementLookup
      needs literal constraints over an imported index key. The workload format has no index imports: every input is a persist source, so no Get carries a key to seek into
  Mfp/Plain/Lookup
      same as Get/ArrangementLookup, no keyed input to seek into
  Mfp/Temporal
      needs an mz_now() predicate. gen_scalar has no unmaterializable functions, and adding one makes the result depend on wall-clock time, which breaks the export-invariance and strategy-invariance oracles unless the workload pins mz_now through the dataflow's `until`
  Reduce/BasicSingle
      needs a non-accumulable, non-hierarchical aggregate. Every Basic aggregate (jsonb_agg, string_agg, the window functions) takes jsonb, text, or a record argument, and the workload format's column types are int4/int8/bool
  Reduce/BasicMultiple
      as Reduce/BasicSingle: no Basic aggregate is expressible over the supported column types
  Reduce/MonotonicConsolidating
      the consolidating variant of a monotonic hierarchical reduce. Lowering sets must_consolidate from its own analysis, and the monotonic shape does not land on the branch that asks for it
  TopK/MonotonicTopK/
      the unlimited monotonic Top-K. A TopK with no limit and a monotonic input is not a shape SQL produces, since LIMIT is what creates a TopK
  FlatMap/Arranged
      needs a table function reading an arrangement rather than a stream, which requires an index import (see Get/ArrangementLookup)
  FlatMap/Lookup
      as FlatMap/Arranged, plus a literal constraint to seek with
  ArrangeBy/Several
      needs one collection carrying several arrangements at once. The multi-key join shape asks for it, but the optimizer decides the arrangements and currently plans that join without it
  Bucketed
      every ArrangementStrategy::TemporalBucketing cell, across Reduce, TopK, Union, and ArrangeBy. Lowering picks it only for a plan carrying future-stamped updates, which means mz_now(); see Mfp/Temporal
