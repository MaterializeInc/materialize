---
title: "Versions"
description: "Materialize's version information"
menu: "main"
weight: 300
---

## Stable releases

Binary tarballs for all stable releases are provided below. Other installation
options are available for the latest stable release on the [Install
page](/install).

{{< version-list >}}

Binary tarballs require a recent version of their stated platform:

* macOS binary tarballs require macOS 10.14 and later.
* Linux binary tarballs require a glibc-based Linux distribution whose kernel
  and glibc versions are compatible with Ubuntu Xenial. Glibc-based Linux
  distributions released mid-2016 or later are likely to be compatible.

## Unstable builds

Binary tarballs are built for every merge to the [main branch on
GitHub][github]. These tarballs are not suitable for use in production.
**Run unstable builds at your own risk.**

| Method       | Available at                        |
|--------------|-------------------------------------|
| Tarball      | [Linux] / [macOS]                   |
| Docker image | `materialize/materialized:unstable` |

To get an arbitrary commit for:

- **Tarballs**, replace `latest` in the URL with the full 40-character commit hash.
- **Docker images**, append `-[40-char hash]` to the image name, e.g.
  `materialize/materialized:unstable-edcedc23df0cc29f1d9f77a656444a00b53cfcb1`

## Support

We offer support for the two most recent versions of Materialize. The
currently supported versions are indicated in the table at the top of the page.

To engage with our community support team:

  * File bug reports and feature requests on our [GitHub issue
    tracker](https://github.com/MaterializeInc/materialize). We take bug reports
    very seriously and usually provide an initial response within one business
    day.

  * Start discussions or ask questions on our [Slack workspace](https://materialize.com/s/chat).

We do not investigate issues with unsupported versions of Materialize. If you
are using an unsupported version, please check that the issue reproduces on a
supported version before engaging with our support team.

Additional support options, including guaranteed SLAs, can be arranged upon
request. Please reach out to our sales team at <https://materialize.com/contact/>.

## Versioning policy

### Schedule

We issue a new release of Materialize every two weeks. Most releases are *timed
releases*, which are cut on schedule, irrespective of what features and bugfixes
have been merged. In rare cases, if severe regressions are discovered, we may
skip a timed release.

Approximately every six weeks, we designate the regularly-scheduled timed
release as a *milestone release*. These releases indicate the completion of a
[planned milestone](https://github.com/MaterializeInc/materialize/milestones),
and may be delayed as necessary to allow for the completion of all scheduled
tasks. Milestone releases are accompanied by a blog post that showcase the
use cases enabled by the features added since the last milestone release.

Every year or two, we expect to designate a milestone release as a *major
release* to mark a new era in Materialize's development. The first major release
of Materialize will be v1.0.0 and will bring improved stability,
backward-compatibility, and support guarantees. We do not yet have a planned
release date for v1.0.0.

### Version numbering

* Major releases increment the first component of the version number, as in
  v0.4.0 to v1.0.0.
* Milestone releases increment the middle component of the version number, as in
  v0.3.1 to v0.4.0.
* Timed releases increment the last component of the version number, as
  in v0.3.0 to v0.3.1.

### Upgrading

We recommend that you upgrade to the latest version of Materialize as quickly
as your schedule permits.

You should not assume that milestone releases will be more stable than timed
releases or vice versa. We do not issue "bugfix-only" releases; any release,
whether timed, milestone, or major, may include behavior changes and new
features in addition to bugfixes.

Before upgrading, you should peruse the [release notes](/release-notes) for
the new release to ensure your applications will not be affected adversely
by any of the changes in the release.

Note that Materialize is not forwards compatible. Once you have upgraded to a
newer version of Materialize, it may be impossible to roll back to an earlier
version. Therefore, we recommend that you test upgrades in a staging cluster
before upgrading your production cluster.

### Backwards compatibility

Materialize maintains backward compatibility whenever possible. Applications
that work with the current version of Materialize can expect to work with
all future versions of Materialize with virtually no changes. Similarly,
the [data directory](/cli/#data-directory) created by the current version of
Materialize will be understood by all future versions of Materialize.

Very occasionally, a bugfix may require breaking backwards compatibility. These
changes are approved only after weighing the severity of the bug against the
number of users that will be affected by the backwards-incompatible change.
Backwards-incompatible changes are always clearly marked as such in
the [release notes](/release-notes).

There are several aspects of the product that are not considered part of
Materialize's stable interface:

  * Features that require [experimental mode](/cli/#experimental-mode)
  * Features that are in beta (labeled as such in their documentation)
  * The [Grafana monitoring dashboard](/ops/monitoring)
  * Any HTTP interfaces, including:
    * Memory profiling tools
    * The [health check endpoint](/ops/monitoring#health-check)
    * [Prometheus metrics](/ops/monitoring#prometheus)
  * Objects in the [system catalog](/sql/system-tables)
  * The [`EXPLAIN`](/sql/explain) SQL statement
  * Any undocumented features or behavior

These unstable interfaces are not subject to the backwards-compatibility policy.
If you choose to use these unstable interfaces, you do so at your own risk.
Backwards-incompatible changes may be made to these unstable interfaces at any
time and without mention in the release notes.

[Linux]: https://binaries.materialize.com/materialized-latest-x86_64-unknown-linux-gnu.tar.gz
[macOS]: https://binaries.materialize.com/materialized-latest-x86_64-apple-darwin.tar.gz
[github]: https://github.com/MaterializeInc/materialize
[Semantic Versioning]: https://semver.org
