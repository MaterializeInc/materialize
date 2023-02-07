# Scaling our design process

## Summary

<!--
// Brief, high-level overview. A few sentences long.
// Be sure to capture the customer impact - framing this as a release note may be useful.
-->

The current design process we're using at Materialize shows its limitations.
Engineers write designs in Notion because it's easy to start, but we lack any established review process, and following discussions is difficult.
Writing designs as pull-requests is perceived to be more tedious than simply writing the content in Notion.
To address this, we analyze the current shortcomings and propose updates to the existing process.

## Goals

<!--
// Enumerate the concrete goals that are in scope for the project.
-->

We aim at creating a design process that is lightweight, captures reviews and specific snapshots in time, is open, while providing a clear benefit to engineers.
Let's break down these aspects.

A design review process needs to empower engineers to focus on the task at hands.
It helps to talk about goals, how they fit into the product roadmap, and outline solutions.
An engineer can present different solutions, and it's expected that new alternatives come up while working on a design.
Explaining trade-offs helps to motivate a specific solution.

Creating a design is an iterative process.
Capturing discussions is an important aspect to understand how an opinion was formed and how designs changed during the review.
We need to capture both, and Notion doesn't give us a good way to capture discussions and specific versions.

Materialize defaults to leading discussions in the open, unless it's not permissible.
This ensures that we uphold a high standard, and it increases the visibility of each engineer's contributions.
Designs in Notion do not achieve this goal.

The most important aspect is that this process provides a benefit to the engineer and the company.
It allows to create confidence that their work fits into the overall architecture and that dependencies are surfaced.
Additionally, a good design document serves as a point-in-time documentation, helping others understand trade-offs and seemingly odd choices.

## Non-Goals

<!--
// Enumerate potential goals that are explicitly out of scope for the project
// ie. what could we do or what do we want to do in the future - but are not doing now
-->

Updating the design process does not aim at introducing an RFC-style process.
It is something to consider in the future, but seems to be too heavyweight at the moment.
Additionally, it would require tooling to enforce certain state transitions, which we don't have the capacity for at the moment.

Notable examples for RFC-style processes follow:
* Rust's RFC process aims at creating high-quality designs, while giving authors the confidence that they get the attention they deserve.
  It is rather heavy-weight and has an own bot to ensure proper state transitions and to nag reviewers.
  We can learn from it that good guidance seems to result in better design.
* Ember's design process follows Rust's process, but is less formal.

## Description

<!--
// Describe the approach in detail. If there is no clear frontrunner, feel free to list all approaches in alternatives.
// If applicable, be sure to call out any new testing/validation that will be required
-->

The current design process fulfills most of the requirements outlined above.
It is lightweight, encourages open discussions and has short review cycles.
This raises the question of why we did not follow it recently.

To address this, we propose the following steps, some of which are part of the change around this document:
* Revisit the design doc [README](./README.md) to make expectations clearer.
* Update the [pull request template](/.github/pull_request_template.md) to include a step reminding engineers to think about design documents.

Specifically, the current interpretation of design documents is to write one for large changes, where it is not clear what large means.
Instead, we propose that each change should come with a design doc unless it is small enough to be non-contentious or immediately clear what needs to be done.
By default, engineers should write design docs.

## Alternatives

<!--
// Similar to the Description section. List of alternative approaches considered, pros/cons or why they were not chosen
-->

### Design docs in Notion

One alternative is to not change the current design doc process and leave it as-is, which defaults to engineers creating designs in Notion.
The benefit is that we don't add (percieved) overhead, at the expense of the problems mentioned here.
Specifically, it doesn't capture discussions well, and does not permit external users to understand design choices.

### RFC-style process

Rust's [RFC process](https://rust-lang.github.io/rfcs/0002-rfc-process.html) demonstrates that guidelines can encourage good designs.
It requires engineers to follow a specific set of steps to shepherd their proposals from an initial idea through reviews to a final design.

These steps seem to be involved, and require tooling/awareness to be carried out correctly.
We see this as a viable alternative once the company grows further and needs to make technically difficult decisions, but it seems to heavyweight to implement it at this point.

## Open questions

<!--
// Anything currently unanswered that needs specific focus. This section may be expanded during the doc meeting as
// other unknowns are pointed out.
// These questions may be technical, product, or anything in-between.
-->
