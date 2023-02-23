- Feature name: (Insert name of feature)
- Associated: (Insert list of associated epics, issues, or PRs)

# Summary
[summary]: #summary

One paragraph to explain the feature: What's wrong, what's the problem, what's the fix, what are the consequences?

# Motivation
[motivation]: #motivation

Why are we doing this? What problems does it solve? What use cases does it enable? What is the desired outcome?

# Explanation
[explanation]: #explanation

Explain the design as if it were part of Materialize and you were teaching the team about it.
This can mean:

- Introduce new named concepts.
- Explain the feature using examples that demonstrate product-level changes.
- Explain how it builds on the current architecture.
- Explain how engineers and users should think about this change, and how it influences how everyone uses the product.
- If needed, talk though errors, backwards-compatibility, or migration strategies.
- Discuss how this affects maintainability, or whether it introduces concepts that might be hard to change in the future.

# Reference explanation
[reference-explanation]: #reference-explanation

Focus on the implementation of the feature.
This is the technical part of the design.

- Is it reasonably clear how the feature is implemented?
- What dependencies does the feature have and introduce?
- Focus on corner cases.
- How can we test the feature and protect against regressions?

# Rollout
[rollout]: #rollout

Describe what steps are necessary to enable this feature for users.
How do we validate that the feature performs as expected? What monitoring and observability does it require?

## Testing and observability
[testing-and-observability]: #testing-and-observability

Testability and explainability are top-tier design concerns!
Describe how you will test and roll out the implementation.
When the deliverable is a refactoring, the existing tests may be sufficient.
When the deliverable is a new feature, new tests are imperative.

Describe what metrics can be used to monitor and observe the feature.
What information do we need to expose internally, and what information is interesting to the user?
How do we expose the information?

Basic guidelines:

* Nearly every feature requires either Rust unit tests, sqllogictest tests, or testdrive tests.
* Features that interact with Kubernetes additionally need a cloudtest test.
* Features that interact with external systems additionally should be tested manually in a staging environment.
* Features or changes to performance-critical parts of the system should be load tested.

## Lifecycle
[lifecycle]: #lifecycle

If the design is risky or has the potential to be destabilizing, you should plan to roll the implementation out behind a feature flag.
List all feature flags, their behavior and when it is safe to change their value.
Describe the [lifecycle of the feature](https://www.notion.so/Feature-lifecycle-2fb13301803b4b7e9ba0868238bd4cfb).
Will it start as an alpha feature behind a feature flag?
What level of testing will be required to promote to beta?
To stable?

# Drawbacks
[drawbacks]: #drawbacks

Why should we *not* do this?

# Conclusion and alternatives
[conclusion-and-alternatives]: #conclusion-and-alternatives

- Why is the design the best to solve the problem?
- What other designs have been considered, and what were the reasons to not pick any other?
- What is the impact of not implementing this design?

# Unresolved questions
[unresolved-questions]: #unresolved-questions

- What questions need to be resolved to finalize the design?
- What questions will need to be resolved during the implementation of the design?
- What questions does this design raise that we should address separately?

# Future work
[future-work]: #future-work

Describe what work should follow from this design, which new aspects it enables, and how it might affect individual parts of Materialize.
Think in larger terms.
This section can also serve as a place to dump ideas that are related but not part of the design.

If you can't think of any, please note this down.
