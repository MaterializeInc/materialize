<!--
Describe the contents of the PR briefly but completely.

If you write detailed commit messages, it is acceptable to copy/paste them
here, or write "see commit messages for details." If there is only one commit
in the PR, GitHub will have already added its commit message above.
-->

### Motivation

<!--
Which of the following best describes the motivation behind this PR?

  * This PR fixes a recognized bug.

    [Ensure issue is linked somewhere.]

  * This PR adds a known-desirable feature.

    [Ensure issue is linked somewhere.]

  * This PR fixes a previously unreported bug.

    [Describe the bug in detail, as if you were filing a bug report.]

  * This PR adds a feature that has not yet been specified.

    [Write a brief specification for the feature, including justification
     for its inclusion in Materialize, as if you were writing the original
     feature specification.]

   * This PR refactors existing code.

    [Describe what was wrong with the existing code, if it is not obvious.]
-->

### Tips for reviewer

<!--
Leave some tips for your reviewer, like:

    * The diff is much smaller if viewed with whitespace hidden.
    * [Some function/module/file] deserves extra attention.
    * [Some function/module/file] is pure code movement and only needs a skim.

Delete this section if no tips.
-->

### Checklist

- [ ] This PR has adequate test coverage / QA involvement has been duly considered.
- [ ] This PR has an associated up-to-date [design doc](https://github.com/MaterializeInc/materialize/blob/main/doc/developer/design/README.md), is a design doc ([template](https://github.com/MaterializeInc/materialize/blob/main/doc/developer/design/00000000_template.md)), or is sufficiently small to not require a design.
  <!-- Reference the design in the description. -->
- [ ] This PR evolves [an existing `$T ⇔ Proto$T` mapping](https://github.com/MaterializeInc/materialize/blob/main/doc/developer/command-and-response-binary-encoding.md) (possibly in a backwards-incompatible way) and therefore is tagged with a `T-proto` label.
- [ ] If this PR will require changes to cloud orchestration, there is a companion cloud PR to account for those changes that is tagged with the release-blocker label ([example](https://github.com/MaterializeInc/cloud/pull/5021)).
  <!-- Ask in #team-cloud on Slack if you need help preparing the cloud PR. -->
- [ ] This PR includes the following [user-facing behavior changes](https://github.com/MaterializeInc/materialize/blob/main/doc/developer/guide-changes.md#what-changes-require-a-release-note):
  - <!-- Add release notes here or explicitly state that there are no user-facing behavior changes. -->
