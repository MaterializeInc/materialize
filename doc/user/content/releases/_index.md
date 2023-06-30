---
title: "Releases"
description: "How Materialize is released"
disable_list: true
menu:
  main:
    parent: 'about'
    weight: 5
---

We are continually improving Materialize with new features and bug fixes. We
periodically release these improvements to your Materialize account. This page
describes the changes in each release and the process by which they are
deployed.

## Release notes

{{< version-list >}}

For versions that predate cloud-native Materialize, see our
historical [release notes](https://materialize.com/docs/lts/release-notes/)
and [documentation](https://materialize.com/docs/lts/).

## Schedule

Each week, Materialize upgrades all regions to the latest version according to
the following schedule:

Region        | Upgrade window
--------------|------------------------------
aws/eu-west-1 | 2100-2300 [Europe/Dublin]
aws/us-east-1 | 0500-0700 [America/New_York]

{{< note >}}
Upgrade windows follow any daylight saving time or summer time rules
for their indicated time zone.
{{< /note >}}

Upgrade windows were chosen to be outside of business hours in the most
representative time zone for the region.

Materialize may occasionally deploy unscheduled releases to fix urgent bugs as well.

You can find details about upcoming and current maintenance on the [status
page](https://status.materialize.com). You can also use the [status page API](https://status.materialize.com/api) to programmatically access this information.

When your region is upgraded, you’ll experience just a few minutes of downtime. After the initial downtime, the new version of Materialize will begin rehydrating your indexes and materialized views. This takes time proportional to data volume and query complexity. Indexes and materialized views with large amounts of data will take longer to rehydrate than indexes and materialized views with small amounts of data. Similarly, indexes and materialized views for complex queries will take longer to rehydrate than indexes and materialized views for simple queries.

## Versioning

Each release is associated with an internal version number. You can determine
what release your Materialize region is running by executing:

```
SELECT mz_version();
```

Scheduled weekly releases increase the middle component of the version number
and reset the final component to zero (e.g., v0.26.2 -> v0.27.0). Unscheduled
releases increase the final component of the version number (e.g., v0.27.0 -> v0.27.1).

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

  * Features that are in alpha (labeled as such in the documentation)
  * The [`EXPLAIN`](/sql/explain) statement
  * Objects in the [`mz_internal` schema](/sql/system-catalog/mz_internal)
  * Any undocumented features or behavior

These unstable interfaces are not subject to the backwards-compatibility policy.
If you choose to use these unstable interfaces, you should be aware of the risk
of backwards-incompatible changes. Backwards-incompatible changes may be made to
these unstable interfaces at any time and without mention in the release notes.

[America/New_York]: https://time.is/New_York
[Europe/Dublin]: https://time.is/Dublin
