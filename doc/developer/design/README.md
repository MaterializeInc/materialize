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

Examples (some "in spirit", because the design doc template changed over time):
* [External Introspection](./20230227_external_introspection.md)
* [`WITH MUTUALLY RECURSIVE`](./20221204_with_mutually_recursive.md)
* [Postgres Sources](./20210412_postgres_sources.md)

## Goals

Summary of the goals of a design document:

 - Record context and assumptions.
 - Achieve a (reasonable) level of consensus around a design.
 - Record decisions.
 - Document a chosen design and lay out _some_ of the technical details.

The last point is intentionally vague and requires some _intuition_ from the
writer. The appropriate level of technical detail depends on the target
reviewers and the nature of the design/change that is being proposed.

Ask your tech lead if you are unsure about the right level of technical detail.
A good rule of thumb is that you should strive for the *minimum* level of
detail that fully communicates the proposal to your reviewers. Going into too
much detail has a cost: it takes longer to write the document and longer to
review the document, plus the finest details are those that are most likely to
change during implementation.

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

1. If the change is large/cross-cutting, e.g., will be spread over multiple PRs
   and/or multiple teams.
2. If there are multiple alternative implementations and no clear best option.
3. If it changes a customer-facing/public API, or a major private API (for
   example, the API for implementing new sinks).

Consider writing a design document:

1. If it's going to involve multiple people and/or teams coordinating changes.
   You have to use your gut feeling/common sense here. A change that involves
   two people from the same team might not need a document.
2. If the change will take more than a week to implement or will proceed in
   phases that need clearly delimited scope.

Many smaller changes do still benefit from a quick design doc to clarify
thinking. Err on the side of writing a design document. If in doubt, keep it
short.

## How should you make a design document?

### Creation

1. Copy the [template](./00000000_template.md) to a new date-prefixed file in
   `doc/developer/design` and fill it in. There is a separate template that the
   COMPUTE team prefers: [template_compute](./00000000_template_compute.md).
2. Submit a pull request -- this makes it easy for others to add written
   comments.
3. "Socialize" the design with relevant stakeholders. Typically, there is a
   small set of people who have a vested interest in the area the design
   touches. If it's not clear who that is, ask your TL, EM, or in #eng-general.
4. Announce that the design doc is ready for review in #eng-announce, if you
   think it's helpful to surface the design to a larger set of people. This
   step is optional. We have an automation that posts new ready-for-review
   design docs to #eng-design-docs and in many cases that will be sufficient
   to inform any interested parties.

### Iteration

5. Address comments, discuss, and iterate on the document in the PR.
6. Gather explicit approvals from relevant stakeholders. This should be the set
   of people that you identified at step 3. or folks that noticed your design
   document from the (optional) announcement.

### Request approval

7. If no comments have raised new issues or if no one has asked for additional
   time to review, merge the design document. Do _not_ assume _silent
   consensus_: if you did not hear back from important stakeholders, don't take
   this as approval. Make sure the relevant people have in fact seen your design
   and had a chance to object or propose changes.

## Refinement

If you discover substantial flaws in your design during implementation, update
the design document, open a PR with the changes, and repeat steps 3-7.

For minor errors, just correct the design document in place in your
implementation PRs.

Use your judgment as to which scenario applies.

## Finalization

You should **not** maintain the design doc forever. The doc is meant to be a
point-in-time artifact that describes the historical context for the design.
Once the described work is complete, you can stop updating the design doc.

Instead, invest in evergreen content. Add comments in the code itself (module
comments can be particularly valuable) and reference documentation in
[doc/developer/reference](/doc/developer/reference/README.md). For larger
components, consider putting together an architecture presentation (see, for
example, the [`persist` lectures
(internal)](https://www.notion.so/materialize/6f83baae1f5348eb87334a53daa63066));
slides and a talk track can be a more efficient means of communicating some
types of technical content.

## How should you review a design document?

As a reviewer, you play a key role in making the design review process a
pleasant and productive experience:

 - Keep the goals and non-goals outlined above in mind. We need authors and
   reviewers aligned on the overall expectations for design documents in order
   for the overall process to be productive.
 - If you can only provide input on parts of the design doc, point that out
   explicitly and focus on those areas.
 - Avoid [bikeshedding](https://bikeshed.com). Bikeshedding can drain the
   author's energy without adding any value.

If you don't know how to productively push back on aspects of a design
document, consult the draft [Raising an objection about a
design](https://rustacean-principles.netlify.app/how_to_rustacean/show_up/raising_an_objection.html)
Rustacean doc for guidance.
