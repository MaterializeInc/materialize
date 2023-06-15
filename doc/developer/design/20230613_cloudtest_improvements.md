# Cloudtest improvements

Associated:
* https://github.com/MaterializeInc/materialize/issues/19626

## Summary

The materialize repository and cloud repository both contain their own collection of cloudtests. These tests orchestrate
cloud resources and verify the behavior of Materialize running in the cloud. However, their setup is different and does
not share a common base.

This document proposes an improvement to these cloudtests by unifying their test setup across both repositories and
sharing existing test framework functionality.

## Goals & Non-Goals

### Goals
* Have a single, unified setup and test framework for cloudtests in both repos, thereby
  * reduce efforts to get familiar with both code bases
  * reduce code duplication and resulting maintenance efforts
* Provide all relevant test framework functionalities to both repositories
* Lay the ground for future improvements

### Non-Goals
* Rework all existing cloudtests

## Description

The unified test framework shall be located in the materialize repo to allow access from both repos. It will be present
in the cloud repo through the inclusion of the materialize repo as git submodule. This submodule is synchronized
occasionally (usually as part of the release process).

The existing cloudtest framework in the materialize repo provides an instance of class `MaterializeApplication` as
context to each test case. This class inherits `Application` and brings functionality to access involved cloud
resources and interact with kubernetes control.

A further class `CloudtestApplication` inheriting `Application` shall be introduced in the cloud repo. It will expose
existing functionality from materialize repo's cloudtests. In addition, it will provide further utility functions to
interact with the controllers of the cloud repo. Where necessary and sensible, functionality from
`MaterializeApplication` shall be pulled up into the shared base class.

A few sample tests using the unified framework will be created in the cloud repo. Their execution shall be integrated
into the CI.

## Drawbacks

* Changes to the test framework, which resides in the materialize repo, might break test execution in cloud repo; in
particular when a developer is not aware that the logic is used there as well. This will be mitigated to some degree
by the build which is executed when synchronizing the materialize repo to the cloud repo.
* Changes to the test framework will cause some overhead when tests in both repositories need to be adapted.
* Maintenance of the test framework might cause additional efforts to work around potential dependency conflicts.
* The complexity of the test framework will be higher due to higher generalization.

## Alternatives

### Keep it as it is

Do not conduct any changes.

### Move all cloudtests to one repository

Moving all cloudtests *to the cloud repo* might reduce some downsides (e.g., overhead to keep both repos in sync,
pain caused by dependency conflicts) but comes with other downsides. Most importantly, it will no longer be possible to
execute cloudtests in the pipeline of the materialize repo so that regressions can only be detected when the materialize
repo is synchronized to the cloud repo.

Moving all tests *to the materialize repo* will not be possible because it won't contain all necessary functionality to
be tested by (current) cloud repo cloudtests.

## Resolved questions

### How to handle semver dependency conflict?

#### Problem
The existing cloudtest framework in the materialize repo uses and depends on `semver >= 3.0.0`. However, the cloud repo
uses `semver == 2.13.0` and some dependencies (`launchdarkly-server-sdk`, `pulumi`) require `semver < 3.0.0` in their
most recent version.

#### Possible Solutions

* upgrade semver in cloud repo => does not seem realistic/possible due to requirements of external dependencies
* try to isolate parts of the test framework that touch semver by handing in or injecting some repo-specific helper
class => this might be a larger refactoring with effects on the code structure
* other

#### Fix

Use a conditional import, which imports and renames the semver's `Version` class depending on the semver dependency
version, to work around a breaking name change
([Migrating from semver2 to semver3](https://python-semver.readthedocs.io/en/latest/migration/migratetosemver3.html)).
This seems to allow running the code with an older version of semver.

## Unresolved questions

Currently none.

## Future work

Once the groundwork is in place, it will be possible to
* migrate existing test cases in cloud repo to the new, shared test framework
* work on running cloudtests from the materialize repo against real cloud environments instead of kind
