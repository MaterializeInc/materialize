---
title: "Releases"
description: "How Materialize is released"
disable_list: true
menu:
  main:
    parent: releases-previews
    weight: 5
    identifier: releases
---

We are continually improving Materialize with new features and bug fixes. We
release these improvements to your Materialize account weekly. This page
describes the changes in each release and the process by which they are
deployed.

## Release notes

{{< version-list >}}

For versions that predate cloud-native Materialize, see our
historical [release notes](https://materialize.com/docs/lts/release-notes/)
and [documentation](https://materialize.com/docs/lts/).

## Schedule

Materialize upgrades all regions to the latest release each week. Materialize
may occasionally deploy additional releases to fix urgent bugs as well.

When your region is upgraded, clients connected to the region will experience a
short period of unavailability. During this period, connection requests may fail
and queries may stall. Most regions experience less than 10 seconds of
unavailability. Regions with a large number of database objects (i.e., sources,
views, indexes, etc.) may experience a longer period of unavailability, but
typically less than 30 seconds.

We do not currently make public commitments about the precise timing of region
upgrades. If you have specific needs around the timing of upgrades, please [file
a support ticket](/support).

## Versioning

Each release is associated with an internal version number. You can determine
what release your Materialize region is running by executing:

```
SELECT mz_version();
```

Scheduled weekly releases increase the middle component of the version number
and may change the final component to any number (e.g., v0.26.2 -> v0.27.0 or v0.26.2 -> v0.27.5).
Unscheduled releases increase the final component of the version number (e.g., v0.27.0 -> v0.27.1).

## Backwards compatibility

Materialize maintains backwards compatibility whenever possible. You can expect
applications that work with the current version of Materialize to work with all
future versions of Materialize with only minor changes to the application's
code.

Very occasionally, a bug fix may require breaking backwards compatibility. These
changes are approved only after weighing the severity of the bug against the
number of users that will be affected by the backwards-incompatible change.
Backwards-incompatible changes are always clearly marked as such in the [release
notes](#release-notes).

There are several aspects of the product that are not considered part of
Materialize's stable interface:

  * Features that are in <a href="https://materialize.com/preview-terms/">public or private preview</a> (labeled as such in the documentation)
  * The [`EXPLAIN PLAN`](/sql/explain-plan) and [`EXPLAIN TIMESTAMP`](/sql/explain-timestamp) statements
  * Objects in the [`mz_internal` schema](/sql/system-catalog/mz_internal)
  * Any undocumented features or behavior

These unstable interfaces are not subject to the backwards-compatibility policy.
If you choose to use these unstable interfaces, you should be aware of the risk
of backwards-incompatible changes. Backwards-incompatible changes may be made to
these unstable interfaces at any time and without mention in the release notes.

[America/New_York]: https://time.is/New_York
[Europe/Dublin]: https://time.is/Dublin
