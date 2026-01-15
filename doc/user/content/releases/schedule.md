---
title: "Release Schedule"
description: "Release schedule for Materialize Cloud and Self-Managed"
disable_list: true
menu:
  main:
    parent: "releases"
    weight: 50
aliases:
  - /releases/cloud-upgrade-schedule/
---

Starting with the v26.1.0 release, Materialize releases on a weekly schedule for
both Cloud and Self-Managed.

## Cloud upgrade schedule

In general, Materialize Cloud uses the following weekly schedule to upgrade all
regions to the latest release, the listed times may vary based on operational needs:

Region        | Day of week | Time
--------------|-------------|-----------------------------
aws/eu-west-1 | Wednesday   | 2100-2300 [Europe/Dublin]
aws/us-east-1 | Thursday    | 0500-0700 [America/New_York]
aws/us-west-2 | Thursday    | 0500-0700 [America/New_York]


During an upgrade, clients may experience brief connection interruptions, but
the service otherwise remains fully available. Upgrade windows were chosen to be
outside of business hours in the most representative time zone for the region.

{{< note >}}

- Materialize may occasionally deploy unscheduled releases to fix urgent bugs.

- Actual cutover time may fall outside of the upgrade window.

- Releases may skip some weeks.

- Upgrade windows follow any daylight saving time or summer time rules
for their indicated time zone.
{{< /note >}}

[America/New_York]: https://time.is/New_York
[Europe/Dublin]: https://time.is/Dublin

## Self-Managed release schedule

In general, Materialize releases new Self-Managed versions on Friday.

{{< note >}}

- Materialize may occasionally have unscheduled releases to fix urgent bugs.

- Releases may skip some weeks.

{{< /note >}}
