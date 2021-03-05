# Purpose

This is a running checklist of things to think about when a non-engineer (a "user"
hereafter in the document) reports an issue.

I find it is useful to think about user issues holistically because:
1. The user's description of what is wrong is based on the user's perception of
   how Materialize works, which may not line up with an engineer's perception.
2. Especially in the case of a performance issue, there are multiple places
   where a bug causing the user's issue could lie. Having a checklist can save
   time going down the wrong rabbit hole.
3. Especially in the case of a performance issue, there could be multiple bugs,
   when added on top of each other, results in the behavior that the user sees.
4. A user-reported issue could reveal not just a problem with performance or
   correctness but also user-experience problems like with documentation or the
   existence of footguns.

Some of the checklist may also be helpful to remind engineers as well on the
various ways that can cause Materialize to behave not as expected.

# Checklist

Are the user that is running Materialize, the user that set up
Materialize, and the user reporting the issue all the same person?
- If not, try to get in touch with the user actually running Materialize or
  get readouts of what the user running Materialize is actually seeing in the client.

Is the version of Materialize:
- an old one? Check the date and/or SHA, or use `select version()`.
  If it is an old version, check if the issue reported existed back then.
  If the issue existed but then was fixed, tell user to update to a newer version.
- based on an actual merge to main? If the user is using a docker image, make
  sure it is not an `mzbuild-...` because that's somebody's random PR.

Are the command line arguments reasonable?

In the case of a slow select query:
* is the user actually selecting from a materialized
  view/source? Occasionally, the user can become confused. You can check
  `materialized.log` for the creation/destruction of indexes, or you can ask the
  user to run `SHOW INDEXES ON <source/view name>` or `SHOW FULL VIEWS|SOURCES`.
* Is the select query taking the fast path or slow path?

What is the user doing?
- Does it make sense to do the thing in a streaming environment?
  Because we have a SQL interface, sometimes users will try to
  copy over their usual SQL queries without considering that
  our operators have different performance profiles compared to their DB
  counterparts.
- Would the thing that the user is doing be something that would be ever be done
  in a production use case or a realistic POC case?
- If the problem lies with a materialized view, does it make sense to
  maintain all the information that the materialized view does?

What is the user's goal?
- Is the user running toy queries to get a sense of performance? Does the toy
  query actually reflect the path the user desires to measure?

What are the column types?
- Are two numbers being joined on actually the same type? Join plans can be
  messed up by implicit casting from int4 (resp. float4) to int8 (resp. float8)
  See [#4171](https://github.com/MaterializeInc/materialize/issues/4171)

Is the plan sane?
- Can an expensive operator be pushed down?
- Can two joins reuse the same arrangement?

Is the rendering sane?
- How often is Differential Dataflow recomputing results? How much recomputation
  is happening?
  - How frequent are the timestamps changing at different stages of rendering?
    * Tables currently have very frequent timestamps compared to other types of sources.
  - Note that `TopK` needs to reorder the entire set with every new timestamp.

Is one worker (or a few workers) behind the others?
- Is the data really skewed towards one (or a few workers)?
  - Is there a cross join?
  - Is there a inner join on vacuous columns? (i.e. the columns
    have very few unique values)?
  - Is the data materialized as an index on vacuous columns?
- Is there a `TopK` or a `Reduce` operator on blank keys or vacuous columns?
- Is there a `TopK` or `Reduce` that is really busy on those workers?

Is the failover behavior sane?

In the case of OOM or Materialize falling over:
- Can a join be made smaller?
  - If an input A of the join depends on a source/view B, and the record
    count of A is greater than the record count of B, consider joining
    against B instead of A.
  - If there is a group-by around the join, consider reduction pushdown.
    - Does the query involving joining on columns ending with '...Id'?
      Reduction pushdown may be especially helpful in this case because columns
      whose names end with '...Id' tend to be unique keys.

In the case of a performance problem gradually observed getting worse over a substantial period of time with prometheus:
- Is the problem reproduceable if prometheus is not running? (and performance is measured via top)

Does Materialize behave like we think it does?
