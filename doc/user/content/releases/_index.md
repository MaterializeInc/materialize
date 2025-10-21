---
title: "Release Notes"
description: "Section containining release info."
disable_list: true
menu:
  main:
    identifier: "releases"
    weight: 80
---

## Releases


### Self-Managed release v25.3.0

#### License keys

#### Swap support

#### SQL Server



## Materialize Cloud Upgrade Schedule

Materialize upgrades all regions to the latest release each week according to
the following schedule:

Region        | Day of week | Time
--------------|-------------|-----------------------------
aws/eu-west-1 | Wednesday   | 2100-2300 [Europe/Dublin]
aws/us-east-1 | Thursday    | 0500-0700 [America/New_York]
aws/us-west-2 | Thursday    | 0500-0700 [America/New_York]

{{< note >}}
Upgrade windows follow any daylight saving time or summer time rules
for their indicated time zone.
{{< /note >}}

Upgrade windows were chosen to be outside of business hours in the most
representative time zone for the region.

Materialize may occasionally deploy unscheduled releases to fix urgent bugs as well.

You can find details about upcoming and current maintenance on the [status
page](https://status.materialize.com). You can also use the [status page API](https://status.materialize.com/api) to programmatically access this information.

During an upgrade, clients may experience brief connection interruptions, but the service otherwise remains fully available.

[America/New_York]: https://time.is/New_York
[Europe/Dublin]: https://time.is/Dublin
