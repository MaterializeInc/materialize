# Design Process

## What's the purpose of a design document?

A design doc is a tool for the author to explain and gather feedback on a
technical decision, providing an opportunity for anything they hadn't thought of
to be surfaced early on and to have their thinking validated. It also serves as
historical documentation for why a thing was done a certain way. The idea is to
go slower at the beginning and get everything out on the table so we can go fast
when implementing.

Design docs should contain enough information that a coworker could both justify
and do the implementation rather than the author: this is a step along the path
of transferring the work that makes it more likely "we", collectively,
understand what we are looking at. It should record enough information that a
coworker could review, diagnose, and debug the implementation of the design. A
great design would have this property, that it acts as a map for understanding
and fixing the feature, as well as implementing it.

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

## When should you make a design document?

Design docs should be written for all changes where an implementation does not
immediately follow from the problem.

Some specific times when you should write a design document:

1. If the change is large/cross-cutting, e.g., will be spread over multiple PRs.
2. If the change will take more than a week to implement or will proceed in
   phases that need clearly delimited scope.
3. If there are multiple alternative implementations and no clear best option.
4. If it's going to involve multiple people coordinating changes.
5. If it changes a customer-facing/public API, or a major private API (for
   example, the API for implementing new sinks).

Many smaller changes do still benefit from a quick design doc to clarify
thinking. Err on the side of writing a design document.

## How should you make a design document?

### Creation

1. Copy the [template](./00000000_template.md) to a new date-prefixed file in
   `doc/developer/design` and fill it in.
2. Submit a pull request -- this makes it easy for others to add written
   comments.
3. Announce that the design doc is ready for review in #eng-announce.

### Discussion

4. Address comments, discuss, and iterate on the document in the PR.
5. Gather explicit approvals from relevant stakeholders. Typically, there is a
   small set of people who have a vested interest in the area the design
   touches. If it's not clear who that is, ask a TL, EM, or in #eng-general.
6. Keep the design doc open while working on the feature and keep it updated as
   you're refining the design.

### Finalization

6. Announce the intent to close commenting on the design document in
   #eng-announce. Tag people that should be aware of the design and ask for
   their feedback.
7. Allow two business days for any final comments.
8. If no comments have raised new issues or if no one has asked for additional
   time to review, merge the design document.

## How should you push back on aspects of a design document?

Consult the draft [Raising an objection about a design](https://rustacean-principles.netlify.app/how_to_rustacean/show_up/raising_an_objection.html) Rustacean doc for guidance.
