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

{{< warning >}}
Support for the ARM CPU architecture is in beta. You may encounter performance
and stability issues. Running Materialize on ARM in production is not yet
recommended.
{{< /warning >}}

{{< version-list >}}

Binary tarballs require a recent version of their stated platform:

* macOS Intel binary tarballs require:
    * macOS 10.13+ from v0.1.0 – v0.9.0
    * macOS 10.14+ from v0.9.1 – v0.13.0
    * macOS 10.15+ from v0.14 onwards
* macOS ARM binary tarballs require macOS 11+.
* Linux Intel binary tarballs require a glibc-based Linux distribution with a
  kernel version of v2.6.32+ and a glibc version of 2.12.1+.
* Linux ARM binary tarballs require a glibc-based Linux distribution with a
  kernel version of v3.10+ and a glibc version of 2.17+.

## Unstable builds

Binary tarballs are built for every merge to the [main branch on
GitHub][github]. These tarballs are not suitable for use in production.
**Run unstable builds at your own risk.**

| Method       | Available at                                              |
|--------------|-----------------------------------------------------------|
| Tarball      | [Linux Intel] / [Linux ARM] / [macOS Intel] / [macOS ARM] |
| Docker image | `materialize/materialized:unstable`                       |

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

We issue a new release of Materialize every week. Most releases are *timed
releases*, which are cut on schedule, irrespective of what features and bug
fixes have been merged. In rare cases, if severe regressions are discovered, we
may skip a timed release.

Occasionally, we may issue an *emergency release* to address a severe bug
or security vulnerability. Emergency releases are based on the most recent
release and contain only the code changes necessary to address the bug or
security vulnerability. We do not backport emergency fixes to older releases.

Every year or two, we expect to issue a *major release* to mark a new era in
Materialize's development. The first major release of Materialize will be v1.0.0
and will bring improved stability, backwards-compatibility, and support
guarantees. We do not yet have a planned release date for v1.0.0.

{{< version-changed v0.10.0 >}}
Releases of Materialize prior to v0.10.0 followed a different schedule and
versioning scheme.
{{< /version-changed >}}

### Version numbering

* Major releases increment the first component of the version number, as in
  v0.11.0 to v1.0.0.
* Timed releases increment the middle component of the version number, as in
  v0.10.1 to v0.11.0.
* Emergency releases increment the last component of the version number, as
  in v0.10.0 to v0.10.1.

### Upgrading

We recommend that you upgrade to the latest version of Materialize as quickly
as your schedule permits.

Before upgrading, you should peruse the [release notes](/release-notes) for
the new release to ensure your applications will not be affected adversely
by any of the changes in the release.

Upgrading to a new emergency release (e.g., from v0.10.0 to v0.10.2) should be
considered lower risk than upgrading to a new timed release (e.g., from v0.10.2
to v0.11.0), as emergency releases contain only the code changes required to fix
the bug or security vulnerability that warranted the emergency release.

Note that Materialize is not forwards compatible. Once you have upgraded to a
newer version of Materialize, it may be impossible to roll back to an earlier
version. Therefore, we recommend that you test upgrades in a staging cluster
before upgrading your production cluster.

### Backwards compatibility

Materialize maintains backwards compatibility whenever possible. Applications
that work with the current version of Materialize can expect to work with all
future versions of Materialize with only minor changes to the application's
code. Similarly, the [data directory](/cli/#data-directory) created by the
current version of Materialize will be understood by all future versions of
Materialize.

Very occasionally, a bug fix may require breaking backwards compatibility. These
changes are approved only after weighing the severity of the bug against the
number of users that will be affected by the backwards-incompatible change.
Backwards-incompatible changes are always clearly marked as such in the [release
notes](/release-notes).

Note that there is no correspondence between the versioning scheme and
backwards-incompatible changes. Any new release of Materialize may contain
backwards-incompatible changes as described above. Even emergency releases may
contain backwards-incompatible changes if they are necessary to address the bug
or security vulnerability that warranted the emergency release.

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

[Linux Intel]: https://binaries.materialize.com/materialized-latest-x86_64-unknown-linux-gnu.tar.gz
[Linux ARM]: https://binaries.materialize.com/materialized-latest-aarch64-unknown-linux-gnu.tar.gz
[macOS Intel]: https://binaries.materialize.com/materialized-latest-x86_64-apple-darwin.tar.gz
[macOS ARM]: https://binaries.materialize.com/materialized-latest-aarch64-apple-darwin.tar.gz
[github]: https://github.com/MaterializeInc/materialize
[Semantic Versioning]: https://semver.org
