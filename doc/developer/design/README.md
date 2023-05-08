# Design Process

## What's the purpose of a design document?

A design doc is a tool for the author to explain and gather feedback on a
technical decision, providing an opportunity for anything they hadn't thought of
to be surfaced early on and to have their thinking validated. It also serves as
historical documentation for why a thing was done a certain way. The idea is to
go slower at the beginning and get everything out on the table so we can go fast
when implementing.

Design docs go hand in hand with a discussion meeting. Asynchronous, text-only
communication tends to drag out the process and doesn't get the same kind of
broad feedback as having a short, in-person meeting.

It's not expected that all questions will be answered prior to the meeting, but
the meeting should not be an open-ended brainstorming process. There is upfront
work that goes into the doc (see [template](./00000000_template.md)) to allow
for a productive meeting, and it's best if attendees have time to read and
digest the doc prior to the meeting.

Note that design doc meetings are not an approval board or review body and the
process is not design by committee. The author is responsible for driving to a
conclusion. That doesn't mean that the author is solely responsible for making a
decision, but does mean that the author is responsible for getting the knowledge
they need to understand the space (from e.g., talking to peers), ensuring that
critical concerns or open questions are addressed, and determining at what point
a good decision has been reached. Not every design doc will lead to complete
consensus.

Examples:
* [Cluster SQL API](https://github.com/MaterializeInc/materialize/pull/10680)
* [`WITH MUTUALLY RECURSIVE`](https://github.com/MaterializeInc/materialize/pull/16445)

## Goals

Summary of the goals of a design document:

 - Record context and assumptions.
 - Achieve a (reasonable) level of consensus around a design.
 - Record decisions.
 - Document a chosen design and lay out _some_ of the technical details: this
   is sufficiently vague and requires some _intuition_ from the writer. This
   point depends on the target audience and the nature of the design/change
   that is being proposed.

## Non-Goals

These are not explicitly required for a good design document. You should,
however, use your own judgement to determine what is and isn't required for
your specific case.

 - Allow a drive-by commenter without any context to fully understand the
   design: the intention is that a group of people with common context can come
   together, understand the design and come to a decision. Some context might
   be gathered from previous design documents, from reference documentation, or
   from looking at the code.
 - Provide a fully spec'ed reference implementation: depending on the topic, it
   can be very helpful to prepare a prototype. Both for validating the design
   and for presenting it to others. However, it is not always a required part
   of the design.

## When should you make a design document?

Design docs should be written for all changes where an implementation does not
immediately follow from the problem.

Some specific times when you should write a design document:

Definitely write a design document:

1. If it's going to involve multiple people and/or teams coordinating changes.
2. If there are multiple alternative implementations and no clear best option.
3. If it changes a customer-facing/public API, or a major private API (for
   example, the API for implementing new sinks).

Consider writing a design document:

1. If the change is large/cross-cutting, e.g., will be spread over multiple PRs.
2. If the change will take more than a week to implement or will proceed in
   phases that need clearly delimited scope.

Many smaller changes do still benefit from a quick design doc to clarify
thinking. Err on the side of writing a design document.

## How should you make a design document?

### Creation

1. Copy the [template](./00000000_template.md) to a new date-prefixed file in
   `doc/developer/design` and fill it in.
2. Submit a pull request -- this makes it easy for others to add written
   comments.
3. "Socialize" the design with relevant stakeholders. Typically, there is a
   small set of people who have a vested interest in the area the design
   touches. If it's not clear who that is, ask your TL, EM, or in #eng-general.
4. (Optional) Announce that the design doc is ready for review in #eng-announce,
   if you think it's helpful to surface the design to a larger set of people.

### Iteration

5. Address comments, discuss, and iterate on the document in the PR.
6. Gather explicit approvals from relevant stakeholders. This should be the set
   of people that you identified at step 3. or folks that noticed your design
   document from the (optional) announcement.

### Finalization

7. If no comments have raised new issues or if no one has asked for additional
   time to review, merge the design document. Do _not_ assume _silent
   consensus_: if you did not hear back from important stakeholders, don't take
   this as approval. Make sure the relevant people have in fact seen your design
   and had a chance to object or propose changes.

## How should you push back on aspects of a design document?

Consult the draft [Raising an objection about a design](https://rustacean-principles.netlify.app/how_to_rustacean/show_up/raising_an_objection.html) Rustacean doc for guidance.
