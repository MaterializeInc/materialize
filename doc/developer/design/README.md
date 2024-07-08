# Design Process

## What's the purpose of a design document?

A design document is a tool we use to thoroughly discover problems and
examine potential solutions before moving into the delivery phase of a project.
Design documents can tackle different classes of problems, including customer
needs and internal technical challenges. The important part of a design
document is being crisp about the exact problem we're tackling, explaining
all of the options being considered for tackling that problem, and recording why
we ended up moving forward with a particular solution.

Writing a design document takes time. It requires a certain amount of
legwork to get started, and it can require a meaningful amount of
collaboration to get to a shareable state. We actively are making a
choice here: to move slower at the beginning of the project management process
in order to move faster and more confidently when we enter the delivery phase.

Once a design document is written, it is required to host a discussion
meeting. Asynchronous, text-only communication tends to drag out the
process and doesn't get the same kind of broad feedback that live
meetings solicit. It is expected that attendees are given time to read, digest,
and engage with the design document before the meeting. It is not
expected that all open questions will be answered before the meeting starts.

Design document meetings are not meant for open-ended brainstorming,
nor are they meant to act as a review body. The intention of this
process is not to design by committee. Instead, the author is
responsible for driving the process to a conclusion that is satisfactory
to the relevant stakeholders. Not every design document will result
in complete consensus. Outcomes from the meeting are recorded as a comment
on the respective PR.

Examples (some "in spirit", because the design doc template changed over time):
* [External Introspection](./20230227_external_introspection.md)
* [`WITH MUTUALLY RECURSIVE`](./20221204_with_mutually_recursive.md)
* [Postgres Sources](./20210412_postgres_sources.md)

## Goals

As stated earlier, the goal of a design document is to thoroughly
discover problems and examine potential solutions before moving
into the delivery phase of a project. In order to do this, each
design document should:

- Clearly and succinctly state the problem it aims to solve,
  along with any required context.
- Document evidence of the problem via customer interviews,
  metrics, or any other means.
- Crisply articulate the author's preferred solution to the
  problem, and why.
- Document all alternative solutions that were considered,
  and why they weren't chosen.
- Document any dependencies that may need to break or change
  as a result of this work.
- Record any decisions that resulted from the document.

If any of these goals are unclear to you, or if the specific
requirements of the document are unclear once you kick off a
design process, reach out to your manager.

## Non-Goals

These are not explicitly required for a good design document.
You should, however, use your own judgment to determine what
is and isn't required for your specific case.

- Allow a drive-by commenter without any context to fully understand the
  design: the intention is that a group of people with common context can come
  together, understand the design and come to a decision. Some context might
  be gathered from previous design documents, from reference documentation, or
  from looking at the code.
- Provide a fully spec'ed reference implementation. Prototypes on the other
  hand are crucial for de-risking the design as early as possible and a minimal
  viable prototype is required in most cases.

## When should you make a design document?

Design docs should be written for all changes where an implementation
does not immediately follow from the problem.

Definitely write a design document:

1. If there are multiple alternative implementations and no clear best option.
2. If it changes a customer-facing/public API, or a major private API (for
   example, the API for implementing new sinks).
3. If the change is large/cross-cutting, e.g., will be spread over multiple PRs
   and/or multiple teams.

Consider writing a design document:

1. If it's going to involve multiple people and/or teams coordinating changes.
   You have to use your gut feeling/common sense here. A change that involves
   two people from the same team might not need a document.
2. If the change will take more than a week to implement or will proceed in
   phases that need clearly delimited scope.

Many smaller changes do still benefit from a quick design doc to clarify
thinking. Err on the side of writing a design document. If in doubt, keep it
short.

Feel free to do prototyping or experimental work before writing a design
document. Prototypes and experiments can be an effective way to further clarify
your thinking and inform the writing process.

## How should you make a design document?

### Creation

1. Copy the design document [template](./00000000_template.md) to a new
   date-prefixed file in the `design` directory of the repository you're
   working in, and fill it in.
2. Submit a pull request of your filled in design document. This
   makes it easy for others to add written comments.
3. Identify your key stakeholders and socialize the design document
   with them. Typically, there is a small set of people who have a vested
   interest in the area the design touches. If it's not clear who your
   stakeholders for the design document are, check in with your manager.
4. Schedule a meeting to discuss the design document live with your
   stakeholders. Rule of thumb is that you should give your reviewers
   about a week to thoroughly review and engage with your document.
5. Optional: announce that the design document is ready for review in
   #epd-announce. It may be useful to get extra eyes on your design, but
   remember that this process is not intended to design by committee.
   Use your judgment in engaging with feedback from outside of your group
   of stakeholders. Note that we have a bot that automatically posts
   notifications about new design documents in #rnd-design-docs.

### Iteration

6. As you begin to get feedback on your document, address the comments
   and answer the questions on your PR. Continue to iterate.
7. Drive the design document meeting and attempt to gather explicit
   approvals from your stakeholders, the set of people that you
   identified in Step 3. Record relevant outcomes from the meeting as
   a PR comment.
8. If you do not get explicit approvals in the meeting, keep iterating
   until you do. You can do this asynchronously or synchronously.
   If you've gone through multiple iterations and are struggling to get
   the required approvals, get help from your manager.

### Approval

9. Once you have gotten the required approvals, merge the design
   document. Do not assume silent consensus. If you did not hear back
   from important stakeholders, do not take this as approval. Make
   sure the relevant people have in fact seen your design and had
   a chance to object or propose changes.

## Refinement

Once you've had your design document approved, you will move into
the [delivery phase of the project management process](../project-management.md).
On the happy path, your design will work as planned and the
implementation will match what you've already documented. In that case,
your design document will not require any refinement. On the unhappy
path, you might discover that the design you proposed needs to be updated.

If you discover major flaws in your design during implementation, you
will need to update your design document and get a new round of approvals
(effectively repeating Steps 3-9 above). If you discover minor flaws,
you can correct the design document alongside the implementation
changes, instead.

Use your judgment as to which scenario applies, and reach out to your
manager if you need guidance.

## Finalization

You should **not** maintain the design doc forever. The doc is meant to be a
point-in-time artifact that describes the historical context for the design.
Once the described work is complete, you can stop updating the design doc.

Instead, invest in evergreen content. Add comments in the code itself (module
comments can be particularly valuable) and reference documentation. For larger
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
