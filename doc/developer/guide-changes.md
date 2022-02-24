# Developer guide: submitting and reviewing changes

As an engineer at Materialize, the bulk of your day-to-day activities will
revolve around submitting and reviewing code. This guide documents our code
review practices and expectations, both for when you are reviewing code and when
you are receiving a code review.

Much of this guide derives from Google's [Code Review Developer
Guide][google-guide], which is well worth a read if you are unfamiliar. There
are no notable conflicts between Google's guide and ours, but see the
[Deviations from Google](#deviations) section for details.

## Overview

We use [GitHub pull requests](https://github.com/MaterializeInc/materialize/pulls)
("PRs") to manage changes to Materialize. Every change, no matter how big or
small, must be submitted as a pull request, and that pull request must pass a
battery of automated tests before it merges.

Any substantial PR must additionally be reviewed by one or more engineers who
are familiar with the areas of the codebase that are touched in the PR. The
goals of code review are threefold:

1. To ensure the health of the codebase improves over time.

2. To mentor engineers who are new to the company or a region of the codebase.

3. To socialize changes with other engineers on the team.

Once a PR is approved and merged to the `main` branch, CI will automatically
deploy a new `materialized` binary to <https://binaries.materialize.com> and the
[materialized Docker image][docker].

While we encourage customers to use the latest stable release in production,
many of our prospective customers run their proofs-of-concept (POCs) off these
latest unstable builds, so it is important to keep the `main` branch reasonably
stable. Any correctness or performance regressions on the `main` branch are
considered urgent to fix.

## Submitting changes

### Commit strategy

The first step to submitting a change to Materialize is, of course, to actually
write the code involved. See earlier sections of the developer guide for help
with the logistics, like cloning the repository and running the test suites.

When you are ready to submit your change for review, the first step is to break
it up into commits. There are two schools of thought regarding commit hygiene.

  * **Semantic commits.** In the semantic commits approach, *each commit* is the
    unit of review. Every commit should have a clear purpose and a well-written
    commit message.

    If you use this approach, you simply write "see commit messages for details"
    in your PR description, though giving a quick overview can be helpful if
    there are many commits involved. When you merge a PR, use the standard merge
    strategy so that the semantic commits are preserved on the `main` branch.

    For an example of this approach, see PR [#3982].

  * **Semantic PRs.** In the semantic PRs approach, your *entire PR* is the unit
    of review, and there is no meaning to the division of commits. It is common
    to simply commit as you go with this approach, with messages like "feature
    works", "tests passing", "apply cargo fmt and clippy."

    If you use the semantic PRs approach, apply your explanatory efforts to
    writing a good PR description, rather than writing good commit messages.
    Then, be sure to use the "squash" strategy when you merge. That way, your
    commits will be replaced with a neat, squashed commit on the `main` branch
    containing your PR description when you merge.

    For an example of this approach, see PR [#3808].

You should endeavor to limit each *review unit* (RU) to just one semantic
change. When a single RU contains multiple unrelated changes, it becomes much
harder for your reviewer to understand, and the quality of the review suffers.

A good test of whether you've included just "one thing" in your change is to
try to write the description for the change, as described in the following
section. If your description reads like "do X and do Y and do Z", you've tried
to do three things, rather than just one.

A good example of what *not* to do is fixing a bug and adding a feature in the
same RU. This happens frequently, as often the process of adding a new feature
requires fixing a bug, and it is quite convenient to fix that bug in the same
RU. But doing so makes it very difficult to view the code for the bugfix or
feature in isolation. This can be particularly problematic if the bugfix or
feature needs to be reverted—it is much more difficult to revert just one or the
other if they are in the same RU.

### Change descriptions

Writing good commit messages (or PR descriptions, if you use the semantic PRs
strategy) is essential to leaving a public record of *why* a change was made to
the codebase.

Most Git commit conventions agree on at least the following form:

     Summarize the commit in the [imperative]

     A detailed description follows the summary here, after a blank line, and
     explains the rationale for the change. It should be wrapped at 72
     characters, with blank lines between paragraphs.

That is, the first line of a commit message is a short sentence, phrased as a
command, that summarizes the contents of the commit. Then a detailed description
follows after a blank line, which provides more context for the change. Why was
the change necessary? What alternatives were considered? Is there obvious future
work that will be necessary? Consider linking to any relevant issues or
benchmarks. Note that the description does not need to be written in imperative
tense.

[`29f8f46b9`] is a great example of the form:

```
coord: add integer_datetimes server var

This variable has been hardcoded to on since postgres 10. It affects how
jdbc (and perhaps other drivers) interpret binary encoded timestamps. The
encoding we are using corresponds to the hardcoded on setting.
````

If the commit message closes an issue, you can tell GitHub to automatically
close the issue when the PR lands by including the magic phrase

    Fixes #1234.

in a commit or PR description. There are several other [supported
phrasings][pr-phrasings], if you prefer.

A classic example of a bad commit message is

    Fix bug.

*What* bugs were fixed, and why?

### Release notes

If your PR contains one or more user-visible changes, be sure to add a
preliminary release note to the PR description in the marked location in the
template.

Your release note may sound very similar to your commit message. That's okay!

#### What changes require a release note?

Any behavior change to a stable, user-visible API requires a release note.
Roughly speaking, Materialize's stable APIs are:

  * The syntax and semantics of all documented SQL statements.
  * The observable behavior of any source or sink.
  * The behavior of all documented command-line flags.

For details, see the [backwards compatibility policy](https://materialize.com/docs//versions/#Backwards-compatibility).

Notably, changes to experimental or unstable APIs should *not* have release
notes. The point of having experimental and unstable APIs is to decrease the
engineering burden when those features are changed. Instead, write a release
note introducing the feature for the release in which the feature is
de-experimentalized.

Examples of changes that require release notes:

  * The addition of a new, documented SQL function.
  * The stabilization of a new source type.
  * A bug fix that fixes a panic in any component.
  * A bug fix that changes the output format of a particular data type in a
    sink.

Examples of changes that do not require release notes:

  * **An improvement to the build system.** The build system is not user
    visible.
  * **The addition of a feature to testdrive.** Similarly, our test frameworks
    are not user visible.
  * **An upgrade of an internal Rust dependency.** Most dependency upgrades
    do not result in user-visible changes. (If they do, document the change
    itself, not the fact that the dependency was upgraded. Users care about
    the visible behavior of Materialize, not its implementation!)

Performance improvements are a borderline case. A small performance improvement
does not need a release note, but a large performance improvement may warrant
one if it results in a noticeable improvement for a large class of users or
unlocks new use cases for Materialize. Examples of performance improvements
that warrant a release note include:

  * Improving the speed of Avro decoding by 2x
  * Converting an O(n<sup>2</sup>) algorithm in the optimizer to an O(n)
    algorithm so that queries with several dozen `UNION` operations can be
    planned in a reasonable amount of time

#### How to write a good release note

Every release note should be phrased in the imperative mood, like a Git
commit message. They should complete the sentence, "This release will...".

Good release notes:

  - [This release will...] Require the `-w` / `--workers` command-line option.
  - [This release will...] In the event of a crash, print the stack trace.

Misbehaved release notes:

  - Users must now specify the `-w` / `-threads` command line option.
  - Materialize will print a stack trace if it crashes.
  - Instead of limiting SQL statements to 8KiB, limit them to 1024KiB instead.

Link to at least one page where users can learn more about either the change or
the area which the change was made. Notes about new features can be concise if
the new feature has comprehensive documentation. Notes about changes to features
must be more detailed, as the note is likely the only documentation of the
change in behavior. Consider linking to a GitHub issue or pull request via the
`gh` shortcode if there is no good section of the documentation to link to.

Strive for some variety of verbs. "Support new feature" gets boring as a release
note.

Use relative links (/path/to/doc), not absolute links
(https://materialize.com/docs/path/to/doc).

Wrap your release notes at the 80 character mark.

#### Internal note order

Put breaking changes before other release notes, and mark them with
`**Breaking change.**` at the start.

List new features before bug fixes.

### PR size limits

There is no "one-size fits all rule" for an acceptable number of modified lines
in a PR. Your reviewer may ask you to break your PR into smaller units, even if
the original PR could be considered a single semantic change. In our experience,
reviewers rarely request to break up PRs when fewer than 500 lines are changed
and frequently request reviews broken into smaller PRs when the size exceeds
1000 lines.

Why are small PRs important? Small PRs:

  * **Are faster and easier to review.** When less code changes, your reviewer
    will be able to understand it more easily and more quickly.

  * **Get better reviews.** The better your reviewer understands the
    PR, the better a review you'll get. A reviewer is more able to
    offer API design suggestions and determine whether the test coverage
    is complete when the amount of code that is changed is small.

  * **Have fewer merge conflicts.** The more code a PR touches, the
    more likely it is to rot when other PRs land first.

  * **Are easier to revert.** If a problem is discovered with a PR down the
    line, it is much easier to revert if it is small and target.

Exceptions:

  * **Generated code.** When generated code is checked into the repository,
    changes to the generated code can often result in hundreds or thousands
    of lines of changes. Those changes don't need to count against the limit,
    provided the reason for the change is clear (e.g., updating the version of
    the dependency that generates the code).

    Where possible, though, we avoid checking in generated code to the
    repository

  * **Declarative tests.** Declarative tests, like sqllogictest, testdrive, and
    datadriven, are in general much easier to review than code. A PR that
    changes 1000 lines of code is more likely to be acceptable if 750 of the
    changed lines results are new test cases.

  * **Prose.** Written text is less dense than code. The appropriate maximum
    size for a PR that adds only prose (e.g., a PR adding the documentation for
    a new feature) is probably around 5,000 _words_ (not lines!).

This sentiment from the Google guide is particularly important to keep in mind:

> Keep in mind that although you have been intimately involved with your code
> from the moment you started to write it, the reviewer often has no context.
> What seems like an acceptably-sized PR to you might be overwhelming to your
> reviewer. When in doubt, write PRs that are smaller than you think you need to
> write. Reviewers rarely complain about getting PRs that are too small.

If you're finding it hard to explain a particular PR in writing, especially for
large PRs, consider scheduling a brief meeting with your reviewer to walk them
through the PR. Similarly, as a reviewer, if you have trouble understanding a
PR, consider scheduling a meeting with your reviewee, rather than continuing to
communicate by text. The downside is that your discussions won't be recorded for
posterity, but the increased bandwidth of verbal communication can often make
the tradeoff worthwhile.

Some PRs, especially large refactors, are nearly impossible to split into
pieces. If you find yourself in this situation, have a conversation with your
reviewers ahead of time to get their buy in.

### Early feedback

You should strive to send code out for review frequently. A good rule of thumb
is that you should not spend more than a day or two on a PR before submitting it
for review. If your reviewers suggest fundamental changes to your approach, it
is much less painful to rework (or, in the worst cast, throw away) a day or two
of work than it is to throw away a week or two of work. Plus, PRs that are more
than a few days of work are almost certain to violate the [PR size
limits](#pr-size-limits).

The larger your PR is, the more of an up-front buy-in you should seek from your
reviewers.

If you are about to embark on a large refactor, for example, describe the
refactor verbally or in prose to your reviewer to make sure they're on board
with the change.

If you are about to embark on a project that will take weeks to months to
complete (e.g., source persistence), start by writing a design document and
seeking feedback on _that_ first. Small design docs can be as simple as a
comment in a GitHub issue; large design docs should be written as Markdown files
in the repository or Google Docs so your reviewers can leave feedback on
individual sections. As with PRs, for large design docs, you should seek
feedback early and often. If designing the details will take a week, spend a day
writing up the high-level design and get feedback on _that_ first.

As with many parts of the job, knowing when to seek feedback relies quite a bit
on intuition. As you gain more experience with an area of the codebase, you'll
be able to predict what kind of feedback you'll receive and preemptively address
it, meaning you can embark on larger projects while seeking less up-front
feedback. But you must still obey the [PR size limits](#pr-size-limits) for the
sake of your reviewer!

### Polish requirements

PRs that are submitted for review should meet your own standards for
"mergeable." Before you click the "Create PR" button, or immediately afterwards,
do a self-review of your PR to make sure it is up to snuff.

The self-review doesn't need to take more than a few minutes. The goal is to
save your reviewer some time by filtering out glaring errors. Did you leave
any comments like `// XXX: fix this before merging`? Did you leave any
commented-out code or debugging `println!`s strewn about? Did you add any dead
code? Did you add any blank lines to unrelated files? Did you remember to add
all the tests you intended to? Have you made the PR as simple and small as it
can be?

The self-review is _not_ meant to add a large emotional burden before submitting
a PR. You should not feel like PRs must be perfect before you can submit them!
If your reviewers catch obvious typos or todos, that is not a failure—that's
exactly why we have code review. The goal is only to reduce the probability that
your reviewer notices glaring errors, not to drive the probability to zero.

Note also that most reviewers will not look at a PR that fails CI. Reviewers
can't easily identify whether a failing test indicates a fundamental problem
with the PR that will require significant refactoring, or just a cosmetic issue
with a test that requires only a small change to fix. Most reviewers will err on
the side of caution by assuming the PR needs substantial reworking, and will put
off the review until it passes tests. If you want your reviewers to look at your
PR despite failing tests, you'll need to drop them a note stating as such. Doing
so usually only makes sense if you've determined the test failures will be
trivial to fix, but don't have the time to actually push up a fixed version of
the PR.

If you are intentionally looking for a review on a PR that is not mergeable—e.g.,
because you are looking for [early feedback](#early-feedback) on high-level
design questions—mark your PR as a [draft][draft-pr] so that your reviewers
know to read past the unpolished parts.

If your PR seems to meet the "mergeable" bar, but there are areas of the change
that you are particularly unsure about, drop your reviewers a note on the PR
indicating the danger zones so they can pay closer attention.

### Choosing reviewers

At the moment, choosing reviewers is a bit of a dark art. The best reviewer for
a PR is the person besides you who is most familiar with the code that is
changed. If you are not sure who that is, ask in #engineering.

If you are more experienced, one additional aspect of choosing a reviewer to
keep in mind is [mentoring](#mentoring). Asking a less experienced engineer to
review your code is a great way for them to learn—often they'll pick up on
tricks and habits that you've developed subconciously and might not think to
mention in the reverse situation, when you're reviewing their code. This
strategy works best when you are fairly confident in the change. If you're not
confident in the change, consider assigning _both_ an experienced an
inexperienced reviewer.

Coming soon: a breakdown of which tech lead is responsible for each area of the
codebase. The tech lead needn't review every PR in their area of responsibility,
but can help direct code review requests to the appropriate engineer.

### Merging

#### When to merge

For sufficiently trivial changes, you can consider merging without review. This
should be somewhat rare, however, and only when you have high confidence that a)
the change is correct, and b) the change is uncontroversial. When in doubt, get
a review!

When a reviewer approves your PR, they will either press the "approve" button
in GitHub's code review interface, leaving a big green checkmark on your PR,
or say something like "LGTM" ("looks good to me"). If they leave no other
comments, then you are cleared to merge your PR!

Sometimes a reviewer will leave a conditional approval. This is usually
explicitly spelled out, as in "LGTM if you make the fix to the `foo` function;
all the other comments are optional." If it is not clear whether an approval
is conditional on addressing other comments in the review, err on the side of
caution and consider them all blocking comments unless you confirm otherwise
with your reviewer.

On large PRs, you may receive only partial approvals, e.g., "the changes to the
SQL layer LGTM, but please get someone else to review the docs changes." In this
case you must use your discretion as to when you have received a covering set
of approvals. (This is a great reason to prefer small PRs when possible, since
you can gather full approvals from various owners in parallel.)

#### Stalled PRs

In some cases, code review reveals that a PR should neither be merged or
discarded. Typically, this means the fundamental approach in the PR is sound,
but:

  * there was substantial review feedback, and you got distracted by other,
    higher-priority projects before they could address all the feedback, or

  * your PR is blocked on another work item.

In either case, mark the PR as a draft if it is stalled for more than several
days. This is a helpful signal to your reviewer that no action is required on
their part. When work resumes on the PR, convert it back to an active PR.

In a similar vein, if code review reveals that a PR needs to be scrapped
entirely, close the PR rather than leaving it open. Even if the PR should serve
useful example of an approach that didn't pan out, it can serve in that role
while closed. Consider linking it an the associated issue for posterity, if
applicable.

#### Merge skew

Sometimes, two PRs are fine when tested in isolation, but the combination of the
two PRs landing together on `main` can cause tests to fail—or even cause
Materialize to fail to compile entirely. We call this situation "merge skew."

When this happens, Buildkite will send a Slack message to #engineering
indicating that tests failed on `main`. If your PR is implicated in the failure,
jump on fixing the merge skew as soon as possible. If you're unable to fix the
merge skew, e.g., because you're about to step into a meeting or just left the
office, drop a quick note in #engineering mentioning when you'll be able to
look into it. For example:

> Looks like merge skew with my latest PR. I've got meetings until 3:30pm. I'll
> look right after unless someone else can fix it first!

If you see a message like the above in Slack, you are very encouraged to pitch
in to fix the merge skew! Just please respond to the message to indicate that
you are picking up the torch to avoid duplicating work.

Merge skew usually has the following form. One PR will add a new call to an
existing function, while another PR adjusts the signature of that function
definition. Both changes pass tests in isolation. The changes won't conflict
according to Git's merge algorithm, because they don't touch the same lines. But
when both changes are present, the code won't compile, because the new call site
uses the old signature for the function. The analogous situation arises when
one PR adds a new test while another PR adjusts the behavior tested by that
new test.

Fixing merge skew is usually as simple as identifying the above situation
and adjusting the new function call or test case to account for the change
in the other PR.

While there _are_ tools that can prevent merge skew, like [Bors], they introduce
quite a bit of latency and flakiness when merging a PR. As long as merge skew is
fairly rare—right now it seems to happen once every month or two—net
productivity is still higher without Bors. If, however, the incidence of merge
skew increases to about once per week, it's probably time to reevaluate this
decision and consider a tool like Bors.

#### Branch restrictions

In very rare cases, it may be necessary to merge a PR whose tests are failing.
This situation typically only occurs with chicken-and-egg CI configuration
issues, where PR builds are broken until a fix can be merged to `main`.

To perform a "god mode" merge, you can temporarily lift the branch restrictions
on the `main` branch. Navigate to the [branch protection rules for the `main`
branch][branch-protection] and uncheck the "Include administrators" box. Do
*not* delete the rules or uncheck any other boxes, as it is much harder to
reverse those changes.

Your PR will be mergeable after you uncheck the "Include administrators" box,
though you will have to click through several warning screens. Once the PR is
merged, be sure to re-enable the "Include administrators" setting to prevent
accidents.

## Reviewing changes

Google's overriding principle for code reviewers is as follows:

> Reviewers should favor approving a PR once it definitely improves the overall
> health of the codebase, even if it is not perfect.

This principle does a good job balancing the need of engineers to be
productive with the need of code owners to keep the codebase maintainable.

### Tips On Giving PR Feedback

* Review the code, not the author.
* Explain the 'why' behind your suggestions and comments.
* Err on the side of giving too much rather than too little feedback, even if
  this slows things down in the short-term.
* Strive for in-depth feedback, but don't let that stop you from giving
  feedback. Suggested solutions, sample code, etc. are all great, but not
  required.
* If something isn't clear and you need it explained, then the code needs to be
  better documented or rewritten for clarity. Push for documentation rather
  than PR conversations/explanations.
* Aim for continuous incremental improvement of the codebase, not necessarily
  perfection on every diff.

### Tips On Receiving PR Feedback

* The code is not you.
* Take time to understand the reasoning behind feedback you receive as opposed
  to only the changes that need to be made.
* When appropriate, give reviewers feedback on their feedback.

### Ensuring codebase health

See the dedicated [Software engineering
principles](#software-engineering-principles) section for the general software
engineering principle we strive to enforce. When applying our software
engineering principles indicates that there is clear room for improvement on a
PR, leave a comment to that effect!

Remember that codebase health often degrades over time as a result of a number
of small changes, each of which introduce a small amount of complexity. The
amount of complexity can seem justified when viewed in isolation, but over time,
the complexity can accrete to result in very confusing, unmaintainable code.

Some matters can be more subjective. When you have a preference, but reasonable
folks can disagree, prefix your comment with "nit:" to indicate that it is not
a hard requirement for the PR to merge.

### Mentoring

When someone is new to Rust, code review is one of the primary components of
their feedback loop. Working through the various Rust tutorials and blog posts
is important, but attempting to put those learnings into practice, and then
getting feedback from someone more experienced, can really accelerate their
learning curve.

The same philosophy applies to someone who is experienced with Rust, but new to
a region of the codebase. Each subsystem has a unique architecture and set of
tradeoffs to manage, and code review is an important component in communicating
these particulars to engineers who are entering that region of the codebase for
the first time. (Ideally these points are also documented somewhere, but even
when they are, it is unrealistic to expect new engineers to read and absorb all
of these points before submitting their first PR.) For example, the `sql` crate
is not particularly allocation-sensitive at the moment, but the `dataflow` has a
number of hot loops that are extremely allocation sensitive. Engineers
unfamiliar with the `dataflow` crate will need to be reminded of this in their
first few PRs.

When reviewing code from a newcomer, spend some extra time rendering feedback on
these various design points. Make sure to mark comments as "nits" where
appropriate to avoid overwhelming the reviewee. It is important that newcomers
are able to make forward progress while they are learning the ropes.

### Review tools

One of the key tenets of code review is that engineers must be permitted to
be productive. Therefore, they are allowed to choose whichever commit strategy
they prefer—and you as reviewer must cope.

The flip side of this is that you may use either GitHub reviews or Reviewable as
the reviewer, whichever you prefer. Beware that with the semantic commit
strategy, Reviewable works much better, as it can track a set of commits across
a force push.

### Latency expectations

One of the most frustrating parts of code review arises when reviewers are slow
to respond. Slow code reviews decrease the velocity of the entire engineering
organization and breed resentment for the code review process.

As a rule, **PRs should be reviewed as soon as possible after they are
submitted.**

The exception to this rule is if you are actively in a flow state. Don't
interrupt your "maker time" to review PRs. But if you are already interrupted,
e.g. because of a meeting or because of a lunch break, try to clear out your
review queue when you get back to your desk, before you get back into the flow.

As a hard upper limit, **the time to first response should never exceed one
business day**.

For complicated PRs, you may need more than one day to complete the review. In
that case, you should still try to inform the PR author of the situation ASAP.
Mention when you think you will complete the review, and whether there is
anything the PR author can do to make the review easier. Perhaps the PR can be
split into chunks, or perhaps the author can walk you through it more
efficiently in a face-to-face meeting.

Remember, as a reviewer, you have the right to request a PR to be split into
chunks if it exceeds the [size limits](#pr-size-limits). Sometimes splitting a
PR can be unreasonably difficult, but in that case the onus is on the PR author
to convince the reviewer that the difficulty of splitting the PR outweighs the
difficulty of reviewing the monolithic PR.

As mentioned in [Polish requirements](#polish-requirements), you can also
ignore PRs if they do not pass CI. Unless the PR author has explicitly mentioned
that the CI failures are trivial, the onus is on them to prove that there are no
fundamental issues with the PR by getting it through CI.

The initial review of a PR is typically the most time intensive—a particularly
complicated PR might take several hours to review—so understandably you have
flexibility as the reviewer in finding time to review it. If you request changes
on a PR, however, the latency expectations for reviewing those changes are
stricter. Prioritize reviewing those changes as soon as you are not in a flow
state. Since you are already familiar with the PR, and typically the changes
requested are small, reviewing the changes will typically very quick, in the
5-15 minute range.

Consider: if you're able to respond to a PR within an hour, that PR can go
through three rounds of changes in an afternoon and land before EOD, freeing the
author up to move on to something else the next day. If you only respond to a PR
once a day, it will take the better part of a week to land the PR.

## Software engineering principles

This section details the software engineering principles we follow.
Upholding these principles is a responsibility shared between the PR author and
reviewer. The PR author should strive to adhere to these principles before
submitting a PR for review, as much as is possible, and the reviewer should
serve as a check to ensure these principles are followed. In particular, the
reviewer should seek to contextualize these principles for the subsystem that
is being changed, as these putting these principles into practice means
different things in different parts of the codebase.

### Simplicity

The most important property a code can have is simplicity, bar none. Simple code
is easier to understand and less likely to have bugs.

Some amount of complexity is inevitable. After all, Materialize provides value
to its customers by absorbing complexity. But the goal is to strip away all
*incidental* complexity, leaving only the *essential* commplexity of the problem
domain.

The challenge is often defining what "simple" means. Which of these code samples
is simpler?

```rust
let widgets: Vec<_> = gadgets
    .into_iter()
    .map(|g| g.frobulate())
    .collect();
```
```rust
let mut widgets = vec![];
for g in gadgets {
    widgets.push(g.frobulate());
}
```

There are many experienced programmers on both sides of this argument, and the
debate rages on.

In many cases, however, simplicity is clear cut. Don't write this:

```rust
fn is_cold(temp: i32) -> bool {
    if temp < 62 {
        true
    } else {
        false
    }
}
```

Write this:

```rust
fn is_cold(temp: i32) -> bool {
    temp < 62
}
```

Whenever you prepare a PR, ask yourself: is the code I've written as simple as
it could possibly be?

A good mindset to have is that getting the code to work is only half the battle.
You are only just getting started when the tests pass. If it takes you half a
day to get the tests passing, it is entirely reasonable—desirable, even—to
spend the next half of the day making the code as simple as possible and adding
documentation/explanations.

While ultimately our jobs involve adding features to Materialize, we are
collectively responsible for minimizing the complexity of the system while we do
so. Otherwise, eventually, the complexity spirals out of control, and adding
new features becomes impossible.

### Avoid special casing

One common source of complexity is excessive special casing. As APIs evolve,
they often start to grow extra parameters...

```rust
fn open_file(
    fs: &mut Filesystem,
    uid: Option<u64>,
    gid: Option<u64>,
    path: String,
) -> Result<FileHandle, anyhow::Error> {
    if fs.type != "FAT32" && !(uid.is_some() && gid.is_some()) {
        bail!("permissions checks are required on non-FAT32 systems");
    } else {
        if (fs.type == "FAT32" || fs.type == "NTFS") && path.chars().any(|ch| ch == '/') {
            bail!("windows paths use backslashes, not slashes");
        } else {
            // Handle opening the file...
        }
    }
}
```

...and eventually there are several dozen methods which special case `uid`,
`gid`, and `path`. Special casing becomes particularly problematic when the
features are interdependent, as the number of special cases is then equal to the
cross-product of features.

Special cases aren't always as obvious as new `bool` parameters. Adding a few
new methods to an interface or several new interfaces to a crate can also
be a sign of special casing.

Special cases are often the fastest way to add a feature in the short term, but
in the long term they harm the maintainability of the codebase. Put another way:
adding the first special case is easy, but adding the tenth special case is
nearly impossible.

Usually, special casing a sign that the interface is wrong. Can you redesign the
interface to instead consist of several composable, non-interdependent pieces?
This sort of refactor can be _really_ tricky to design, but if you can get it
right, it pays dividends.

### Encapsulation

For every line of code you write, you should ask yourself, "is this really the
right spot for this code?" Every function, struct, module, and crate should have
a well-defined boundary of responsibility. The clearer this boundary is, the
easier the code is to reason about in isolation.

Put another way: **the easiest place to put code is frequently the wrong place
to put that code.**

There is a tricky distinction here that relies quite a bit on intuition. When
you've nailed an API and you're adding a feature that fits the existing spirit
of that API, the easy spot to add that feature will *also* be the right place
to add it.

But more often than not, the API you're modifying was a bit rough around the
edges to start, or the feature you're adding wasn't accounted for when the API
was originally designed. In these cases, the easiest path forward is to jam in
some [special cases](#avoid-special-cases) to support the feature. This is the
exactly moment where you want to consider restructuring code for better
encapsulation. Maybe you can introduce a new module to handle the new
responsibility. Maybe you've placed the code in the wrong place, and there is a
more natural fit in another module.

For example, the `sql-parser` and `sql` crates are entirely separate. The former
crate tries very hard to limit its scope to *only* the grammar of the SQL
language, without encoding any knowledge of the semantics of the SQL language.
The parser knows that `SELECT * FROM bar` is well formed and `SELECT * FROM
'nope'` is not, but it does not know that the former query means to fetch all of
the rows and columns from the database object named `bar`, nor how to determine
in what database/schema the object named `bar` lives, etc.

When adding new SQL features, it is often tempting to slip some semantic
analysis into the `sql-parser` crate. For example, it might be tempting to
reject invalid limit clauses in the parser, as in `SELECT ... LIMIT 'foo'`. This
violates encapsulation, however, as the grammar allows for `LIMIT <expr>`, where
`<expr>` is _any_ valid SQL expression, including strings. The `sql` crate has
complicated coercion logic to allow `LIMIT '1'` to be accepted while `LIMIT
'foo'` is rejected. Attempting to perform this sort of semantic analysis in the
`sql-parser` means you don't have access to this coercion logic, and you'll be
forced to hand-roll a less precise and inconsistent version of the same logic.

Even though it can be more work, and sometimes even a bit more code, to add the
validation in the `sql` crate instead, separating concerns like this it makes
the SQL parser more understandable and more reusable.

### Dependencies

One of the great dangers of the Rust ecosystem is the ease with which you can
reach for a new dependency. Remember, every new dependency is code that we will
indirectly have to maintain.

Fixing a bug in a dependency is much harder than fixing a bug in core
Materialize code. You'll have to check out the repository, spin up on the
codebase, fix the bug, use Cargo's somewhat buggy [patch feature][cargo-patch]
to verify the fix in Materialize, then send a PR to upstream. If upstream is
responsive, then you just need to wait for a new version with the fix on
crates.io. If upstream is not responsive, you'll need to fork the dependency,
then keep that fork up-to-date if upstream ever becomes active again. Each step
is a small papercut, but they add up.

The best case is when a dependency is as actively maintained as Materialize.
Dependencies that fall into this category include [Tokio], [rust-postgres],
and [reqwest]. These dependencies have largely had no bugs at all. When they
have had bugs, the upstream maintainers have been very quick to fix them.

Use caution with dependencies that are not actively maintained. Materialize
often winds up taking over maintainership of these dependencies. This can make
sense for dependencies within our core competencies, like [rust-rdkafka]
and [sqlparser], but it's a real drag for peripherals, like [rust-prometheus].

### Documentation

Documentation means at least two things in the repository: code documentation
and user-facing documentation. Both of these are important, but have different
roles. While the act of documentation may feel less important than the production
of code, the enduring value of the code is greatly diminished without clear
documentation. Clear documentation is a required component of a code contribution.

Code documentation is important to explain the intended behavior of your code
without requiring a reader to extract this from your code directly. It serves
as a contract between your implementation and the users of your code. The value
of clear documentation lies in the extent to which it can reduce the time others
must spend inspecting your code to determine its appropriateness. Documentation
also provides for continuity as others may need to work with and take ownership
of code that you have written.

A PR without appropriate code documentation may reasonably be rejected. Imagine
that the reviewer would first like to read a precis of your new code, and only
the read the code itself. This approximates the experience of most people new
to your new code.

User documentation is also important, but may be outside the tradecraft that
you are trained for. If you are creating a new user-facing feature, please
ensure that appropriate documentation is produced, either as part of the pull
request if that is possible, or scheduled appropriately as the feature is
released.

### Testing

With perfect testing, introducing a bug in any line of any function in the
codebase would cause at least one test to fail. Meeting this standard would
unacceptably impair velocity, but it's a useful framing for what ideal test
coverage means.

The weaker property we strive for is that every change in behavior should
require modifying or adding at _least_ one test. If a PR changes, say, the
behavior of a SQL function and there are no changes to any test files, then
there are certainly some missing tests.

Whenever possible, tests should validate observable behavior, not implementation
details. Over-testing of trivial internal functions creates its own maintenance
burden, as tests frequently have to be changed because of innocuous refactors,
causing people to be less likely to attempt refactors, or worse, less likely to
take test failures seriously.

If a piece of code is large and complex enough, it might warrant tests to
validate its behavior, despite not being externally observable. Even in this
case, tests should validate the behavior at a clean API boundary of this code,
rather than its internal details. Always use your best judgment.

Our testing philosophy is covered in much greater detail in
[Developer guide: testing](guide-testing.md).

## Deviations from Google

As mentioned at the start of this guide, much of the above advice derives from
Google's [Code Review Developer Guide][google-guide]. There are no notable
conflicts between Google's guide and ours.

The only major deviation is that we permit PRs to contain multiple commits
when using the ["semantic commits" strategy](#commit-strategy), whereas Google's
PR equivalent, the CL ("change list"), always contains exactly one commit.

Why, then, did we write our own guide? The biggest reason was to allow our
engineering practices to evolve independently. Has this guide skewed with
reality? Then please submit a PR to update it! Frustated with something about
our engineering process? Then submit a PR with your proposed fix and ask for
feedback!

The other benefit of this guide is that it uses Materialize terminology and
intersperses Materialize-specific details. This means you don't waste time
mentally mapping e.g. our GitHub-based workflow onto Google's Gerrit-based
workflow, or come to a different conclusion than another engineer about the
details of that mapping. If something in this guide is unclear, please
submit a PR to fix it!

## Summary

* Limit each review unit (RU) to one semantic change.

* Write a thorough description for each RU.

* Never include refactoring and behavior changes in the same commit.

* Keep PRs small, and ideally less than 500 lines.

* Always initiate PR reviews within one business day, and sooner if possible.

* Verify that every PR has, at a minimum:

  * [ ] Adequate testing. Every change in behavior should result in the addition
    or modification of one test at a very bare minimum.

  * [ ] One or more release notes, if there are user-visible changes.

* Accept PRs that improve the overall health of the codebase, even if they
  are not perfect.

[`29f8f46b9`]: https://github.com/MaterializeInc/materialize/commit/29f8f46b92280071c96b294d414675aa626f9403
[#3808]: https://github.com/MaterializeInc/materialize/pull/3808
[#3982]: https://github.com/MaterializeInc/materialize/pull/3982
[bors]: https://bors.tech
[branch-protection]: https://github.com/MaterializeInc/materialize/settings/branch_protection_rules/5953288
[cargo-patch]: https://doc.rust-lang.org/edition-guide/rust-2018/cargo-and-crates-io/replacing-dependencies-with-patch.html
[draft-pr]: https://github.blog/2019-02-14-introducing-draft-pull-requests/
[docker]: https://hub.docker.com/r/materialize/materialized
[google-guide]: https://google.github.io/eng-practices/review/
[imperative]: https://en.wikipedia.org/wiki/Imperative_mood
[pr-phrasings]: https://docs.github.com/en/github/managing-your-work-on-github/linking-a-pull-request-to-an-issue#linking-a-pull-request-to-an-issue-using-a-keyword
[release notes]: https://materialize.com/docs/release-notes/
[reqwest]: https://github.com/seanmonstar/reqwest
[rust-rdkafka]: https://github.com/fede1024/rust-rdkafka
[rust-prometheus]: https://github.com/MaterializeInc/rust-prometheus
[rust-postgres]: https://github.com/sfackler/rust-postgres
[sqlparser]: https://github.com/ballista-compute/sqlparser-rs
[Tokio]: https://tokio.rs
