## Materialize Platform: User Experience Design

⚠️ **WARNING!** ⚠️ This doc is a work in progress!

The intended user experience drives some of the concepts that we'll want to reflect in our code.
The intended user experience in turn reflects the practical realities of the tools we have access to.

The coarsest level of granularity of Materialize Platform is the ACCOUNT.
Within an ACCOUNT there may be multiple REGIONs, which correspond roughly to availability zones.
A REGION is the unit within which it is reasonable to transfer data, and across which that expectation doesn't exist.

Within each REGION there may be multiple CLUSTERs, which correspond roughly to timely dataflow instances.
A CLUSTER contains indexes and is the unit within which it is possible to share indexes.

Within each REGION there may be multiple TIMELINEs, which correspond roughly to coordinator timelines.
A TIMELINE contains collections, and is the unit within which it is possible to query across multiple collections

The main restrictions we plan to impose on users is that their access to data is always associated with a REGION, CLUSTER, and TIMELINE, and attempts to access data across these may result in poorer performance, or explicit query rejection.

There is some work to do to present these concepts to users, but prior work exists with e.g. SQL `database`s for TIMELINE and Snowflake's `USE WAREHOUSE` command to pilot CLUSTER.
