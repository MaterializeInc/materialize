# Project management

*Last updated February 18, 2020.*

This note addresses a scary subject: project management. The current strategy
is to impose the bare minimum amount of process to make sure everyone knows what
to work on next.

A problem I've had in other jobs is that the backlog is either non-durable,
existing only in engineers' minds, or durable but essentially infinite, where no
matter how many issues you close in a month, the issue list continues to grow.

Filing copious GitHub issues neatly solves the durability problem, but
introduces a new problem. Issues are filed for problems big and small, and
sometimes filed not for problems at all, but for open-ended discussion.
As soon as the issue list exceeds some small number, like 50 or 100, it becomes
nearly impossible to keep track of what's important and what's not. How many
of the open issues are scheduled for the next release? How many of those
are actual bugs, and how many are just new features? How should I figure out,
as an engineer, what to work on next?

The following sections detail the elements of our project management process
that are visible through GitHub.

## Project Boards

**If you're looking for your next task, start here.**

We currently use [organization-wide project boards](https://github.com/orgs/MaterializeInc/projects)
to track prioritized tasks using a kanban style of organization. Tasks flow from
left to right. The project descriptions should contain more detail. Note that
you must be a member of the MateralizeInc organization to see the project
boards.

## Milestones

As each issue is triaged, we will assign a milestone. A milestone should be
treated as an upper bound for when the issue should be resolved. Everyone will
be quite happy if you merge a PR that resolves an issue any time before the date
of the milestone. We aim to have releases roughly once a week, so your merged
PRs will go out in the next release. Marketing for new features may wait until
the milestone.

If a required change is unlikely to merge before the associated milestone,
please make that clear in a comment in the issue as early as possible.

## Issue labels

The last piece of the puzzle is fastidiously assigning labels to issues. The
scheme is documented in the next section. The idea here is to make it possible
to ask questions like "are there any outstanding bugs in this release?" or "what
scheduled SQL features are missing from this release?" without doing a full
table scan.

There are presently four classes of GitHub labels:

* **C**ategory labels, like **C-bug**, identify the type of issue. There are
  four categories: bugs, refactorings, features, and musings.

  Bugs, refactorings, and features should be quite familiar: bugs are defects in
  existing code, refactorings are inelegant or suboptimal pieces of code, and
  features dictate new code to be written. The line between a bug, a
  refactoring, and a feature is occasionally blurry. To distinguish, ask
  yourself, "would we be embarrased to document this issue as a known
  limitation?" If the answer is yes, it's a bug or refactoring. If the answer is
  no, it's a feature. To distinguish between bug and refactoring consider
  whether we believe the current code to be working as intended and the scope
  of the change. If it is working as extended and the scope is on the larger
  side, then chances are it's a refactoring. Also, we might occasionally
  choose to ship a release with an embarrassing bug documented as a known
  limitation, but this is usually a good litmus test nonetheless.

  Musings are issues that are intended to start discussion or record some
  thoughts, rather than serving directly as a work item. Issues are not
  necessarily the optimal forum for such discussions. You might also want to
  consider starting a mailing list thread or scheduling an in-person discussion
  at the next engineering huddle.

* **A**rea labels, like **A-sql**, identify the area of the codebase that a
  given issue impacts.

  As mentioned below, the area labels are not intended to be an exact promise of
  what modules are impacted. They're just meant to guide engineers looking for
  the next work item in a given region of the code, and to give management
  a rough sense of how many open bugs/feature requests a given area of the code
  has.

  As a result, area labels are not particularly specific. We'll need to split
  things up over time (e.g., the "integrations" area will almost certainly
  evolve to mention specific integrations), but getting too specific too soon
  increases the burden when filing an issue without much benefit.

* Theme labels, like **T-performance**, that identify the nature of the work
  required.

  Themes cut across categories and areas. For example, performance work might
  be a bug or a feature, depending upon how bad the current performance is, and
  can occur in any area.

  The intent is to help find projects for an engineer who knows what sort of
  work they're interested in (e.g., "I want to do some perf hacking; what's
  available?").

* Automatically generated labels, like **Epic** and **dependencies**. Our
  external tools insist upon adding these labels, and we don't get control over
  their text. Typically just ignore these.

* Standard labels, like **good early issue**. GitHub has several of these that
  come default with new repositories. They don't quite conform to our naming
  scheme, but it seems worthwhile to keep the default names so that external
  contributors can more easily find their way around when we make the
  repository public.

You can see the most up to date list of labels here:
https://github.com/MaterializeInc/materialize/labels.

When filing an issue, these are the rules to adhere to:

1. Assign exactly *one* category label. Is it a bug, a feature, or a musing?

2. If the category is not "musing," assign at least one area label, or more if
   the issue touches multiple areas.

   Don't worry about being exact with the area labels. If an issue is 90% a
   problem with the dataflow layer, but will have some small changes in the
   SQL layer and the glue layer, feel free to just assign **A-dataflow**.

3. Assign **good first issue** if the issue would make a good starter project
   for a new employee. You will be soundly thanked for this when the next
   employee starts!

## Communicating changes

When landing large or substantial changes, we want to make sure users are aware of the work you're doing! This means we require:

- Coordination with a technical writer to generate or update user-facing documentation that will show up on <materialize.io/docs>. If you have questions, open an issue with the `A-docs` tag.
- Generating release notes by describing your change in `doc/user/release-notes.md`. If there any questions about which version the feature will be released in, consult <materialize.io/docs/versions> or chat with us.

### Changes that require documentation

- All new features
- All API changes
- Large bug fixes
