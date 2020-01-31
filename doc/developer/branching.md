# Branching

## Motivation

The following code changes will happen in parallel:

1. New feature development
2. Resolving issues
3. Paying down technical debt (e.g. refactoring and cleanup)

Some customers will want access to new changes quickly, especially if they've
reported an issue that's blocking their use of Materialize. Others will want
stability.

## Branch structure

We will now have at most two active branches at any time:

1. All development will continue to happen on `master`.
2. Each release will have a corresponding `release-x.y` branch, branched from
   `master`. Only bug fixes should be merged into this branch.

All PRs should merge into `master`. Periodically (e.g. once a week), an engineer
will cherry-pick bug fixes from master to merge into the current `release-x.y`
branch. Most of the time, changes should merge reasonably cleanly into
`release-x.y`. When the modified code diverges significantly between the
branches, the author of the PR will help resolve the merge conflicts.

```
  release-0.1            release-0.2            release-x.y
 +-------------------   +-------------------   +-------------------
 | ^  ^  merge  ^  ^    | ^  ^  merge  ^  ^    |  ^  ^  merge  ^  ^
 | |  |  fixes  |  |    | |  |  fixes  |  |    |  |  |  fixes  |  |
-+----------------------+----------------------+-------------------> master
```

To minimize the cost of branch management, we will cut new release branches as
late into the development cycle for the release as we can manage.

Customers who want the newest channels can use nightly `master` builds. Those
who want more stable code can opt to use nightly builds of `release-*`.

## Branch timing

* We'd cut the branch `release-0.1` (our first public beta) 1-2 days before
  the public announcement.
* We'd create `release-0.2` 1-2 days before the public announcement.

## Merge rotation

We will rotate merge duties on a weekly basis across the engineering team. With
some lightweight coordination amongst engineers, we will avoid most severe merge
conflicts.
