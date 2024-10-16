# Contributing to Materialize

Thank you for your interest in Materialize! Contributions of many kinds are encouraged and most welcome.

If you have questions, please [create a Github discussion](https://github.com/MaterializeInc/materialize/discussions/new/choose).

## Getting started

The best place to start using Materialize is probably the documentation [instructions](https://materialize.com/docs).

## Developing Materialize

After using Materialize, if you'd like to contribute, extensive documentation on
our development process is available in the [doc/developer](doc/developer) folder.

Materialize is written entirely in Rust. Rust API documentation is hosted at
<https://dev.materialize.com/api/rust/>.

If you're interested in contributing to Materialize, please [create a Github
discussion](https://github.com/MaterializeInc/materialize/discussions/new?category=contribute-to-materialize)
describing the work you're planning to pick up. Prospective code contributors might
find the [`D-good for external contributors` tag](https://github.com/MaterializeInc/materialize/discussions/categories/contribute-to-materialize?discussions_q=is%3Aopen+category%3A%22Contribute+to+Materialize%22+label%3A%22D-good+for+external+contributors%22) useful.

If you start working on a contribution before hearing back from a Materialize
engineer, there is a risk that we may not be able to accept your changes. The
team may not have bandwidth to review your changes, or the code in question may
require specialized knowledge to modify without introducing bugs. When in
doubt, wait for a Materialize engineer to respond before starting work!

Bug reports are welcome, and the most effective way to report a bug is to [file
a discussion](https://github.com/MaterializeInc/materialize/discussions/new?category=bug-reports). As
Materialize is under rapid development, it is helpful if you report the version
of Materialize that you are using, and if it a crash report, the stack trace.

### Landing PRs + communicating changes

When landing large or substantial changes, we want to make sure users are aware of the work you're doing! This means we require:

- Coordination with a technical writer to generate or update user-facing documentation that will show up on <materialize.com/docs>. If you have questions, open an issue with the `A-docs` tag.
- Generating release notes by describing your change in `doc/user/release-notes.md`. If there any questions about which version the feature will be released in, consult <materialize.com/docs/versions> or chat with us.

#### Changes that require documentation

- All new features
- All API changes
- Large bug fixes

### Where to contribute
Some areas that are well suited for external contributions are:
- Adding SQL functions
- Adding pg_catalog tables to support more [tools and integrations](https://materialize.com/docs/integrations/)

Areas that are not well suited for external contributions:
- The [coordinator](https://github.com/MaterializeInc/materialize/tree/main/src/adapter/src/coord)
- [Persist](https://github.com/MaterializeInc/materialize/tree/main/src/persist)
- [Compute](https://github.com/MaterializeInc/materialize/tree/main/src/compute)
