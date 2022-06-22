# Contributing to Materialize

Thank you for your interest in Materialize! Contributions of many kinds are encouraged and most welcome.

If you have questions, please [create a Github issue](https://github.com/MaterializeInc/materialize/issues/new/choose).

## Getting started

The best place to start using Materialize is probably the documentation [instructions](https://materialize.com/docs).

## Developing Materialize

After using Materialize, if you'd like to contribute, extensive documentation on
our development process is available in the [doc/developer](doc/developer) folder.

Materialize is written entirely in Rust. Rust API documentation is hosted at
<https://dev.materialize.com/api/rust/>.

Prospective code contributors might find the [good first issue tag](https://github.com/MaterializeInc/materialize/issues?q=is%3Aopen+is%3Aissue+label%3A%22D-good+first+issue%22) useful.

Bug reports are welcome, and the most effective way to report a bug is to [file
an issue](https://github.com/MaterializeInc/materialize/issues/new/choose). As
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
