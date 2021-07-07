# Systems challenges in Materialize

## Summary

<!--
// Brief, high-level overview. A few sentences long.
// Be sure to capture the customer impact - framing this as a release note may be useful.
-->

Materialize shows a sublinear behavior when scaling the number of worker threads.
While some of this is expected, we need to gain a better understanding of how much this affects as and potential remedies to improve the scaling behavior.
This document outlines problems and suggests solutions that can help avoid the problems in the future.

## Goals

<!--
// Enumerate the concrete goals that are in scope for the project.
-->

We present a set of challenges and provide ideas on how to solve them.

## Non-Goals

<!--
// Enumerate potential goals that are explicitly out of scope for the project
// ie. what could we do or what do we want to do in the future - but are not doing now
-->

The goal is to provide insights into Materialize's behavior assuming a fixed set of queries.
It is a non-goal to improve the performance within the SQL/optimization layer.

## Description

<!--
// Describe the approach in detail. If there is no clear frontrunner, feel free to list all approaches in alternatives.
// If applicable, be sure to call out any new testing/validation that will be required
-->

We need to validate the following three challenges.

1. Materialize's memory consumption increases superlinearly with the number of worker threads.
   The number of worker threads influences buffer allocations for exchange channels.
   The communication between workers follows an all-to-all pattern which means that the buffer space alone increases quadratically with the number of workers.

2. Timely communicates updates to operator capabilities through broadcasts to all worker threads.
   In some situations this results in many fine-grained updates carrying only little information.
   A downside is that each update might be a small allocation.
   We should investigate if this is a problem and what magnitude it has.

3. Materialize needs to transfer relatively large pieces of data between operators, including across thread boundaries.
   It has the potential impact of stressing the memory allocator, which needs to pass back memory to other threads.

For the first and second challenge, Timely does not yet expose the right mechanisms to provide a different policy.
The last problem could be addressed by not passing owned data but instead only revealing references to data.

The set of challenges are based on experience and observations, but not on reproducible tests.
Before we tackle any of them, we should establish a benchmark that clearly highlights the problem and can be used to validate any solution.

Let's look at the problems in detail.

### Memory consumption when scaling worker threads

### Progress tracking overhead

### Memory allocator stress

## Alternatives

* An obvious alternative is not to do anything, because Materialize currently works fine.

<!--
// Similar to the Description section. List of alternative approaches considered, pros/cons or why they were not chosen
-->

## Open questions

<!--
// Anything currently unanswered that needs specific focus. This section may be expanded during the doc meeting as
// other unknowns are pointed out.
// These questions may be technical, product, or anything in-between.
-->
