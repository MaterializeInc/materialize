# Timelines

## Summary

Some sources consist of data that doesn't have any timestamp information and we
assign that data "real time" timestamps. Other kinds of sources, might have stronger
opinions about the timestamps that data get bound to. In those cases, source
implementations have a tradeoff. If they remap upstream timestamps to "real time"
then the data from this source can be joined against other real time sources easily.
However, this makes doing other operations, like temporal filters, on the source
more difficult because now timestamps within the data (like a "delete-at-this-time" column)
might not be meaningful when compared to the timestamp Materialized assigned the data.

This design doc proposes to:

  * Introduce the concept of `source timeline`s or `source time domain` (name TBD).
    A `timeline` means roughly the same thing it does colloquially when people say
    for example "I wonder what it would be like in an alternate timeline where there
    was no COVID". For our purpose more specifically, a `source timeline` means "a
    set of sources whose timestamps are related to each other."

  * Teach the Coordinator about timelines and have each source implementation
    announce what `timeline` they belong to.

  * Teach the Coordinator to complain if a query uses sources / views that span
    multiple timelines. Note that currently, such queries either succeed by sheer
    chance or potentially block forever.

## Goals

The only goal of this project is to unblock source family and linearizability work,
both of which needs some guidance on either "what timestamps should I use here" or
"which timestamps are comparable with which other timestamps"

## Non-Goals

  * Exposing timelines as a SQL-language visible concept. For now, each implementation of a
    source should declare what timeline it belongs to. In the future, source implementations
    might be flexible enough to accomodate multiple timelines and leave the choice up to a user
    (e.g. using Kafka you could either assign real time timestamps or event time timestamps).

## Description

<!--
// Describe the approach in detail. If there is no clear frontrunner, feel free to list all approaches in alternatives.
// If applicable, be sure to call out any new testing/validation that will be required
-->

This is a pretty simple proposal so the actual details of implemting it are relatively trivial. Basically, we need:

  1. An enum for `Timeline`s.
  2. A function that can take an `ExternalSourceConnector` and return the `Timeline` it belongs to.
  3. The Coordinator to keep a map from `GlobalId`s -> `Timeline`s
  4. For the Coordinator to complain whenever a dataflow would use `GlobalId`s belonging to multiple timelines.

## Alternatives

<!--
// Similar to the Description section. List of alternative approaches considered, pros/cons or why they were not chosen
-->

TODO: flesh this out more.

The chief alternatives as far as I can tell are to change the timestamp type in the dataflow layer, and encode the event
and system time there. I still think it's useful for the Coordinator to know which event times are comparable and which are
not.

## Open questions

<!--
// Anything currently unanswered that needs specific focus. This section may be expanded during the doc meeting as
// other unknowns are pointed out.
// These questions may be technical, product, or anything in-between.
-->

TODO: flesh this out more.
