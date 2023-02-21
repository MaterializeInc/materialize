- Feature name: designs-revisited
- Associated issues and PRs: [#17482][#17482]

[#17482]: https://github.com/MaterializeInc/materialize/pull/17482

# Summary
[summary]: #summary

We're updating the design review process to make it easier for engineers to write design documents, allow for better reviews, and clearly document expectations.
We accompany the update with refined design document requirements.

# Motivation
[motivation]: #motivation

The current design process we're using at Materialize shows its limitations.
Engineers write designs in Notion because it's easy to start, but we lack any established review process, and following discussions is difficult.
Writing designs as pull-requests is perceived to be more tedious than simply writing the content in Notion.
To address this, we analyze the current shortcomings and propose updates to the existing process.

# Explanation
[explanation]: #explanation

We aim at creating a design process that is lightweight, captures reviews and specific snapshots in time, is open, while providing a clear benefit to engineers.
Let's break down these aspects.

A design review process needs to empower engineers to focus on the task at hand.
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
It allows us to create confidence that their work fits into the overall architecture and that dependencies are surfaced.
Additionally, a good design document serves as a point-in-time documentation, helping others understand trade-offs and seemingly odd choices.

# Reference explanation
[reference-explanation]: #reference-explanation

The current design process fulfills most of the requirements outlined above.
It is lightweight, encourages open discussions and has short review cycles.
This raises the question of why we did not follow it recently.

To address this, we propose the following steps, some of which are part of the change around this document:
* Revisit the design doc [README](./README.md) to make expectations clearer.
* Update the [template](./00000000_template.md) to match what we're outlining in this design.
* Update the [pull request template](/.github/pull_request_template.md) to include a step reminding engineers to write a design document.
* Mark the design documents in Notion as deprecated.
  We considered moving the design documents from Notion to GitHub, but decided against it due to the lack of rigorousness and missing discussions.

Specifically, the current interpretation of design documents is to write one for large changes, where it is not clear what large means.
Instead, we propose that each change should come with a design doc unless it is small enough to be non-contentious or immediately clear what needs to be done.
By default, engineers should write design docs.

# Conclusion and alternatives
[conclusion-and-alternatives]: #conclusion-and-alternatives

Updating the design process does not aim at introducing an RFC-style process.
It is something to consider in the future, but seems to be too heavyweight at the moment.
Additionally, it would require tooling to enforce certain state transitions, which we don't have the capacity for at the moment.

Notable examples for RFC-style processes follow:
* Rust's [RFC process](https://rust-lang.github.io/rfcs/) aims at creating high-quality designs, while giving authors the confidence that they get the attention they deserve.
  It is rather heavy-weight and has an own bot to ensure proper state transitions and to nag reviewers.
  We can learn from it that good guidance seems to result in better design.
  Rust's [RFC process](https://rust-lang.github.io/rfcs/0002-rfc-process.html) demonstrates that guidelines can encourage good designs.
  It requires engineers to follow a specific set of steps to shepherd their proposals from an initial idea through reviews to a final design.

  These steps seem to be involved, and require tooling/awareness to be carried out correctly.
  We see this as a viable alternative once the company grows further and needs to make technically difficult decisions, but it seems to heavyweight to implement it at this point.
* Ember's [design process](https://rfcs.emberjs.com/) follows Rust's process, but is less formal.

One alternative is to not change the current design doc process and leave it as-is, which defaults to engineers creating designs in Notion.
The benefit is that we don't add (perceived) overhead, but at the expense of the problems mentioned here.
Specifically, it doesn't capture discussions well, and does not permit external users to understand design choices.

# Unresolved questions
[unresolved-questions]: #unresolved-questions

It is unclear if the changes we suggest will solve the problems around our current design document approach.
We belive that it is a first step into the right direction, and with more experience we can decide whether it fulfills our goals or whether we need to refine the process.

# Future work
[future-work]: #future-work

In the future, we might want to have a more formalized design document process, where we have set of states a design needs to advance through to be approved.
This could be supported by appropriate tooling.
