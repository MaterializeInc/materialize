# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# lir_plan scenario: a user-provided LIR plan, built and validated as
# `CREATE MATERIALIZED VIEW ... FROM PLAN` builds it, then rendered. The plan is
# the optimizer's plan of `SELECT pt.a, pt.b, pu.c FROM pt JOIN pu ON pt.a = pu.a`,
# which reads `pt` from persist and `pu` through its index `pu_a`.
create-instance
----
ok

initialization-complete
----
ok

define-schema name=pt
  a bigint
  b bigint nullable
----
ok

define-schema name=pu
  a bigint
  c text nullable
----
ok

# The plan's output, for reading it back.
define-schema name=out
  a bigint
  b bigint nullable
  c text nullable
----
ok

write-rows shard=pt schema=pt ts=0
  1 10
  2 null
  3 30
  3 31
----
wrote 4

write-rows shard=pu schema=pu ts=0
  1 one
  3 three
  4 null
----
wrote 3

# The index the plan imports as `materialize.public.pu_a`.
define-index source=1001 index=1002 shard=pu schema=pu key=[0] as-of=0 upper=1
----
ok

schedule id=1002
----
ok

# Without a `schema`, the sink's schema is the type of the plan's output. `pu` has
# no shard binding, so it takes the type of its index.
create-lir-dataflow name=lir-join sink=3000 shard=out as-of=0
  relation name=materialize.public.pt id=1000 shard=pt schema=pt upper=1
  relation name=materialize.public.pu id=1001
  index name=materialize.public.pu_a index=1002
  plan
    {"lir_version":1,"imports":[{"id":{"Transient":1},"source":{"Name":"materialize.public.pt"},"persist":true,"indexes":[]},{"id":{"Transient":2},"source":{"Name":"materialize.public.pu"},"persist":false,"indexes":[{"Name":"materialize.public.pu_a"}]}],"objects":[{"id":{"Transient":3},"plan":{"lir_id":4,"node":{"Join":{"inputs":[{"lir_id":2,"node":{"ArrangeBy":{"input_key":null,"input":{"lir_id":1,"node":{"Get":{"id":{"Global":{"Transient":1}},"keys":{"raw":true,"arranged":[]},"plan":"PassArrangements"}}},"input_mfp":{"mfp":{"mfp":{"expressions":[],"predicates":[],"projection":[0,1],"input_arity":2}},"lower_bounds":[],"upper_bounds":[]},"forms":{"raw":true,"arranged":[[[{"Column":[0,"a"]}],[0,1],[1]]]},"strategy":"Direct"}}},{"lir_id":3,"node":{"Get":{"id":{"Global":{"Transient":2}},"keys":{"raw":false,"arranged":[[[{"Column":[0,"a"]}],[0,1],[1]]]},"plan":"PassArrangements"}}}],"plan":{"Linear":{"source_relation":1,"source_key":[{"Column":[0,"a"]}],"initial_closure":null,"stage_plans":[{"lookup_relation":0,"stream_key":[{"Column":[0,"a"]}],"stream_thinning":[1],"lookup_key":[{"Column":[0,"a"]}],"closure":{"ready_equivalences":[],"before":{"mfp":{"expressions":[],"predicates":[],"projection":[0,2,1],"input_arity":3}}}}],"final_closure":null}}}}}}],"output":{"Transient":3}}
----
ok

schedule id=3000
----
ok

allow-writes id=3000
----
ok

# Keys 1 and 3 match, 3 twice.
peek id=3000 schema=out ts=0
----
x

# The same plan with a join closure that projects column 9 of a 3-column row. The
# validator rejects it, and nothing is submitted.
create-lir-dataflow name=lir-join-bad sink=3001 shard=out-bad as-of=0
  relation name=materialize.public.pt id=1000 shard=pt schema=pt upper=1
  relation name=materialize.public.pu id=1001
  index name=materialize.public.pu_a index=1002
  plan
    {"lir_version":1,"imports":[{"id":{"Transient":1},"source":{"Name":"materialize.public.pt"},"persist":true,"indexes":[]},{"id":{"Transient":2},"source":{"Name":"materialize.public.pu"},"persist":false,"indexes":[{"Name":"materialize.public.pu_a"}]}],"objects":[{"id":{"Transient":3},"plan":{"lir_id":4,"node":{"Join":{"inputs":[{"lir_id":2,"node":{"ArrangeBy":{"input_key":null,"input":{"lir_id":1,"node":{"Get":{"id":{"Global":{"Transient":1}},"keys":{"raw":true,"arranged":[]},"plan":"PassArrangements"}}},"input_mfp":{"mfp":{"mfp":{"expressions":[],"predicates":[],"projection":[0,1],"input_arity":2}},"lower_bounds":[],"upper_bounds":[]},"forms":{"raw":true,"arranged":[[[{"Column":[0,"a"]}],[0,1],[1]]]},"strategy":"Direct"}}},{"lir_id":3,"node":{"Get":{"id":{"Global":{"Transient":2}},"keys":{"raw":false,"arranged":[[[{"Column":[0,"a"]}],[0,1],[1]]]},"plan":"PassArrangements"}}}],"plan":{"Linear":{"source_relation":1,"source_key":[{"Column":[0,"a"]}],"initial_closure":null,"stage_plans":[{"lookup_relation":0,"stream_key":[{"Column":[0,"a"]}],"stream_thinning":[1],"lookup_key":[{"Column":[0,"a"]}],"closure":{"ready_equivalences":[],"before":{"mfp":{"expressions":[],"predicates":[],"projection":[0,9,1],"input_arity":3}}}}],"final_closure":null}}}}}}],"output":{"Transient":3}}
----
x
