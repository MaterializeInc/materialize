# Engineering best practices

In addition to the concrete rules, such as for Rust, we also have some more informal style
guidelines that we try to follow. These are not as strictly enforced as the
other rules, but you should still try to follow them as much as possible.

## Know your code's scaling limits

When writing code, you should have a good understanding of how it will scale in the future.
For example, when you build a feature that targets the current scale, you should also think
about how it will behave when the data size is 10x, 100x, or even 1000x larger.
What are the bottlenecks? Will it still be performant? Will it still be maintainable?

This doesn't mean that the code you write today has to be perfect for all future scales, but it should be easy to change and adapt as the scale changes.
In fact, all code has scaling limits, and we'd like to make conscious decisions one way or another.
Documentation should point out any assumptions or limitations that the code has regarding scale.
Leave breadcrumbs for your future self and others who will work on the code later.

A common way to think about scaling is to keep time and space complexity in mind.
For example, if you are writing a function that takes 50 microseconds per item, it is
fine to write a O(n^2) algorithm for very small n, but it will quickly become a problem.
The following table demonstrates how different time complexities scale with increasing input sizes:

| Input Size (n) | O(1)     | O(log n) | O(n)  | O(n log n) | O(n^2)     |
|----------------|----------|----------|-------|------------|------------|
| 10             | constant | 166us    | 500us | 1.66ms     | 5ms        |
| 100            | constant | 333us    | 5ms   | 33ms       | 500ms      |
| 1,000          | constant | 500us    | 50ms  | ~500ms     | 50s        |
| 10,000         | constant | 665us    | 500ms | ~6.7s      | 84 minutes |
| 100,000        | constant | 831us    | 5s    | ~83s       | 139h       |
| 1,000,000      | constant | 1ms      | 50s   | ~1000s     | 1.6 years  |

## Prefer simplicity to complexity

When writing code, you should always prefer simplicity to complexity. Rely on Rust's type system
to enforce invariants, rather than writing complex logic to check for them at run time.

* If a function gets too hard to explain, consider breaking it up into smaller functions.
* If a change requires extensive documentation to explain, consider simplifying the change.
* Consider separating a function into two functions if it has multiple responsibilities.
  An indicator could be a Boolean argument and if-else branches that depend on it.

## Write code for others, not only for yourself

(Adapted from @frankmcsherry's document on technical leadership.)
Here are some tests that I use for my own work.
These are likely applicable to your work too, and worth thinking through.
A common theme is that they are all about other people.
Those other people may be users, colleagues, or even (and most often) my future self.

1. Can I explain how to use my work effectively to someone who could benefit?

    This is a test of whether I've actually reduced complexity.
    If it's less complicated to just stick with the incumbent approach than to switch to what I've built, I haven't reduced complexity.
    If it is hard to understand when it would be appropriate to use my work, I've introduced a new complexity.
    Roughly half the time I write a blog post about a thing I've built, I realize midway that it is inoperable and I need to redo some part of the work before I tell the world about it.
    This is great, and a great process, as the goal is to get to an output that folks find valuable.

2. Do I need to be present for other people to succeed?

    This tests whether I've built something that reduces complexity for others, or reduces complexity for myself in service of others.
    A dashboard may make it easy to see how well your stuff is working, but if others don't know about or don't understand the dashboard, you've made your life easier rather than theirs.
    If errors and logs announce the offending lines of code, rather than how the user can start to fix things, your presence may still be required.
    For as long as this is the case, your solution involves your participation, which may not seem that complicated to you but is very complicated for others.

3. Can I explain how my work operates in simple words and a short amount of time to an interested person?

    If not, I may not understand it fully, and it is unlikely that a new person approaching the work would be able to understand it either.
    If nothing else, it may mean that the work is a soup of complexity, even if it presents outwards as simple.
    Think of this as "architectural" documentation, in service of the next person who will have to work with or maintain your code.
    That person may currently be you, and it may currently feel fine, but it will eventually be a later version of you or someone else entirely, both of whom will thank you.

These tests have been humbling, but very valuable for me.
They are not necessarily comprehensive, and ideally we are able to add even more structure to recipes for upleveling technical success.

## Listen for feedback

Language is ambiguous, and different people have different interpretations of the same words.
When you receive feedback on your change, try to understand the perspective of the person giving the feedback.
Ask questions if you don't understand their point of view.
It's better to ask questions and clarify misunderstandings than to assume you know what they mean.

## Solving problems over quick fixes

We sometimes need to ship fixes quickly, but we should always try to solve the underlying problem.
Staggering a fix into multiple changes is OK, as long as we don't forget halfway through.
Accumulating technical debt is easy, but it makes future changes harder and more error-prone.

## Release blockers

Release blockers should be reserved for critical issues that must be resolved before a release can proceed.
Each release blocker causes work for whoever is doing the release, and should be used sparingly.
Do not use release-blockers to backport features into releases that missed the deadline.
