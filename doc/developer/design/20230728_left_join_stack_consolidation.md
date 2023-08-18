- Feature name: Consolidation in LEFT JOIN stacks
- Associated: [#20828](https://github.com/MaterializeInc/materialize/issues/20828)

# Summary
[summary]: #summary

Stacks of LEFT JOINs seem to be a pretty common pattern in user queries. Our plans for these turned out to have a problematic feature: at each of the LEFT JOINs we have a Union and a Negate (resulting in records that would cancel out with a consolidation), but there is a dataflow path with no consolidation between one Union-Negate and the Union-Negate for the next LEFT JOIN. This means that each of the LEFT JOINs we will accumulate more unconsolidated intermediate data, resulting in a lot of extra work for downstream operators.

We propose a simple fix: We insert a consolidation at each of the Union-Negates to immediately make the negated records cancel out. This fix will unfortunately result in some increased memory usage (especially when processing snapshots), but we argue that the simplicity of this solution is worth the extra memory usage for now. Later we can reconsider some more complicated alternative solutions, which we also briefly discuss at the end of this document.

# Motivation
[motivation]: #motivation

Examine the following query and its plan. The `// x, y` numbers show how many records we have at those points in the plan if we don't consolidate at the Union-Negates or if we consolidate immediately after each Union-Negate, respectively. We set up the experiment in a way that each of the input relations have exactly 1 record, and all the records match, i.e., that there are no records in the outer sides of LEFT JOINs (which is the worst case for the consolidation problem).

```SQL
create table foo(x int, y int);
create table bar(x int, y int);

create view bar_keyed as select distinct on(x) * from bar;

create table more1(x int, y int);
create table more2(x int, y int);

create view more1_keyed as select distinct on(x) * from more1;
create view more2_keyed as select distinct on(x) * from more2;

insert into foo values (0,0);
insert into bar values (0,0);
insert into more1 values (0,0);
insert into more2 values (0,0);

explain
select * from
  foo
  LEFT JOIN bar_keyed ON foo.x = bar_keyed.x
  LEFT JOIN more1_keyed ON foo.x = more1_keyed.x
  LEFT JOIN more2_keyed ON foo.x = more2_keyed.x;
```

```
                Optimized Plan
----------------------------------------------
 Explained Query:                            +
   Return                                    +
     Union                                   + // 7, 1
       Map (null, null)                      +
         Union                               + // 6, 0  <----- Union-Negate
           Negate                            +
             Project (#0..=#5)               +
               Get l4                        + // 1, 1
           Get l3                            + // 5, 1
       Project (#0..=#5, #0, #6)             +
         Get l4                              + // 1, 1
   With                                      +
     cte l4 =                                +
       Project (#0..=#5, #7)                 +
         Join on=(#0 = #6) type=differential + // 1, 1
           ArrangeBy keys=[[#0]]             +
             Filter (#0) IS NOT NULL         +
               Get l3                        + // 5, 1
           ArrangeBy keys=[[#0]]             +
             TopK group_by=[#0] limit=1      +
               Filter (#0) IS NOT NULL       +
                 Get materialize.public.more2+ // 1, 1
     cte l3 =                                +
       Union                                 + // 5, 1
         Map (null, null)                    +
           Union                             + // 4, 0  <----- Union-Negate
             Negate                          +
               Project (#0..=#3)             +
                 Get l2                      + // 1, 1
             Get l1                          + // 3, 1
         Project (#0..=#3, #0, #4)           +
           Get l2                            + // 1, 1
     cte l2 =                                +
       Project (#0..=#3, #5)                 +
         Join on=(#0 = #4) type=differential + // 1, 1
           ArrangeBy keys=[[#0]]             +
             Filter (#0) IS NOT NULL         +
               Get l1                        + // 3, 1
           ArrangeBy keys=[[#0]]             +
             TopK group_by=[#0] limit=1      +
               Filter (#0) IS NOT NULL       +
                 Get materialize.public.more1+ // 1, 1
     cte l1 =                                +
       Union                                 + // 3, 1
         Map (null, null)                    +
           Union                             + // 2, 0  <----- Union-Negate
             Negate                          +
               Project (#0, #1)              +
                 Get l0                      + // 1, 1
             Get materialize.public.foo      + // 1, 1
         Project (#0, #1, #0, #2)            +
           Get l0                            + // 1, 1
     cte l0 =                                +
       Project (#0, #1, #3)                  +
         Join on=(#0 = #2) type=differential + // 1, 1
           ArrangeBy keys=[[#0]]             +
             Filter (#0) IS NOT NULL         +
               Get materialize.public.foo    +
           ArrangeBy keys=[[#0]]             +
             TopK group_by=[#0] limit=1      +
               Filter (#0) IS NOT NULL       +
                 Get materialize.public.bar  + // 1, 1
                                             +
 Source materialize.public.bar               +
   filter=((#0) IS NOT NULL)                 +
 Source materialize.public.more1             +
   filter=((#0) IS NOT NULL)                 +
 Source materialize.public.more2             +
   filter=((#0) IS NOT NULL)                 +
```
For each of the 3 LEFT JOINs, the plan has one `Union-Negate`: In `l1`, `l3`, and the `Let` body. The problem is that there is a dataflow path that has no consolidation between these Union-Negates: For example, the problematic path between the first two Union-Negates is as follows: We start from the Union-Negate in `l1`, then we have a Map, another Union (with no Negate), then we go to the first usage of `l1` (the second usage is fine, since it goes into a Join), which goes into the Union-Negate in `l3`.

The Union-Negate pattern computes the outer part of a LEFT JOIN: the negated input to these unions is the collection of matched records. So, we have the most waste when all records have a match, in which case a consolidation would eliminate all records coming out from the Union-Negate. We can see that if we don't consolidate, then the record count increases by `2N`, where `N` is the size of one input, whereas if we consolidate, then record counts stay at `N`. Note that the increase is _not_ exponential (we have N, 3N, 5N, 7N, ...), but it will still get quite bad for big stacks. ([We've seen several big stacks](https://www.notion.so/materialize/Left-Join-Feedback-1d921cbe0296431b96b97b2480808cf9?d=e38c0c85241c48a98e8414f75e2c37e0). One customer [has a stack of 28 LEFT JOINs in one view, and 12 and 13 in two other views](https://materializeinc.slack.com/archives/C05GRCC4K1C/p1690293441722169).)

Note that the problem occurs regardless of whether SemijoinIdempotence kicks in.

# Explanation
[explanation]: #explanation

As mentioned above, our proposed solution is to simply perform a consolidation immediately after each of the Union-Negates. More specifically, we'll do an LIR refinement that checks each Union whether it has an input that has a Negate at the top, and if yes, then we set a `consolidate` flag on the Union, which will make the rendering insert a `consolidate` call on the Union's result. This will immediately make the negated records cancel out.

## Extra memory usage

How `consolidate` works is that it buffers up some data before it actually consolidates. In steady state this won't result in much extra memory usage, because the consolidation happens inside a single timestamp. When processing a snapshot, we will buffer up more data temporarily. This amount is bounded by the snapshot size in the worst case, but is often less, due to compacting in power-of-2-sized chunks already as the data is coming in. (Although, the two inputs of the Union might come in one after the other (due to one having an extra join compared to the other), so we might need to reach large powers of 2 before some consolidation can happen.) Note that if there are several LEFT JOINs, then only two of these `consolidate`s will simultaneously take up memory, because they are pipeline breakers.

The total effect of the feature on memory usage will still be beneficial, as the unconsolidated records are currently consolidated several times when they go into joins (e.g., the 2 unconsolidated records coming from `l1` are currently consolidated both when forming the arrangement for the join in `l2` and also similarly in `l4`). One situation when we might have a net increase in memory usage is when there is only one LEFT JOIN, and there are no other operators downstream that would benefit from a consolidation, but this is probably not so common. Another situation when the consolidation is not helping much is if most records from the left side of a LEFT JOIN don't have a match on the right side.

Also note that the ArrangeBy of the Join that the Union-Negate pattern feeds (e.g., the second usage of `l1`) will have a smaller memory usage after the consolidation.

# Testing and Rollout
[rollout]: #rollout

We should add tests that check that the physical plan has the `consolidate` flag on Unions as we expect. We should also look at the hierarchical memory visualizer to confirm that record counts decrease as expected. We might be able to also add an automated test for the record counts: We'll create a testdrive test that has a long chain of LEFT JOINs (that don't change the cardinality), which will make the difference in total message counts between the old and new versions quite noticeable.

We will also add a feature flag for the optimization as a precaution, but it will be enabled by default.

# Drawbacks
[drawbacks]: #drawbacks

The above approach increases memory usage in some situations, as explained above.

# Conclusion and alternatives
[conclusion-and-alternatives]: #conclusion-and-alternatives

Overall, I think the above solution strikes a good balance between complexity and performance considerations. An alternative would be adding an MIR transform to insert ArrangeBys that "blend in" with existing ArrangeBys, plus adding an LIR transform that sets a flag on these ArrangeBys to make them also consolidate the raw collection. We discuss this alternative approach in the Future work section below, and we argue that for now the performance benefits of this alternative are not worth the added complexity.

# Future work
[future-work]: #future-work

We now discuss some alternative approaches that are more complicated than the proposed approach above, but solve the problem with slightly better performance.

Examine the usages of `l1` in the above plan: The first usage is the problematic one where we want consolidation. Notice that the second usage is almost immediately arranged (going into a Join), which involves a consolidation. We could conceivably make the needed consolidation "blend in" with the arrangement creation of the Join in `l2`. In fact, if an MIR transform inserted an ArrangeBy at the top of `l1`, then the next run of `JoinImplementation` would 1) lift away the Filter that is between the Join in `l2` and the new ArrangeBy, and 2) not insert the ArrangeBy that is currently just below the Join.

The above observation could solve the consolidation problem with 0 added memory usage, but there would be considerable added complexity. We would need an MIR transform that detects Union-Negate chains without consolidation and inserts ArrangeBys at all but the last Union-Negate. This MIR transform would also need to pay attention that in this case the consolidation shouldn't be right at the top of the Union-Negate, but we need to be mindful of the Join that is hiding behind the second usage of `l1` and place the ArrangeBy in a way that that Join can re-use it.

There is also a second complication when we consider some messy implementation details of LIR ArrangeBy: One might think that an LIR ArrangeBy automatically means in all cases that the next downstream operator will consume the data in arranged and consolidated form, but unfortunately this is not always the case: An LIR ArrangeBy just makes the arranged form available to the next operator, but the next operator is free to consume the raw, unarranged, and unconsolidated form of the collection. In fact, operators that don't need an arranged form actually do consume the raw collection. There are several ways to solve this, but all of these either add considerable complexity or unclear overheads:
- Make ArrangeBy create the raw collection form from an arranged form even when the input to the ArrangeBy is already available in raw form. The arrange-to-unarranged conversion introduces some overhead, but it's unclear how much. It's also unclear how often this overhead would happen: one could say that usually when we have an arranged form then it's because we actually need the arranged form, so the arranged-to-unarranged wouldn't be a very common occurrence.
- We could have a separate LIR transform that detects a similar Union-Negate pattern as the above MIR transform and tweaks relevant ArrangeBys by adding a `consolidate_raw` flag, which would signal to the rendering that the raw collection in the output of the `ArrangeBy` should be made from an arranged form. However, we generally want to avoid non-trivial LIR transforms.
- We could add a similar `consolidate_raw` flag on the MIR ArrangeBy to avoid adding a non-trivial LIR transform. However, implementation flags on MIR operators tend to have the problem that it's too much work to keep them from falling off during irrelevant MIR transforms. There is another subtlety with the semantics of this new flag on the MIR ArrangeByt: The problem is that an MIR ArrangeBy is not a guarantee that the output is consolidated even when the next operator consumes the arranged form: when the MIR ArrangeBy is signalling the re-use of an external index, then timestamps are teleported to the `as_of` of the dataflow, and thus we can get unconsolidated data across timestamps that are distinct in the index-creation dataflow but are the same in our dataflow.
