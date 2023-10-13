---
title: "Static IP addresses"
description: "Materialize uses static IP addresses for outbound connections from sources and sinks"
menu:
  main:
    parent: network-security
    weight: 10
aliases:
  - /ops/network-security/static-ips/
  - /connect-sources/static-ips/
---

Your Materialize region initiates connections from a static set of IP addresses.
When connecting Materialize to services in your private networks (e.g., Kafka
clusters or PostgreSQL databases), you must configure any firewalls to allow
connections from these IP addresses.

## Details

Your Materialize region is associated with a static set of egress IP addresses.
Most regions have four egress IP addresses, but this is not guaranteed. All
connections to the public internet initiated by a source or sink in your region
will originate from one of these egress IP addresses.

We do not allocate unique IP addresses for each Materialize region. Multiple
regions may share the same egress IP addresses.

When connecting Materialize to services in your private networks (e.g., Kafka
clusters or PostgreSQL databases), you must configure any firewalls to allow
connections from your region's egress IP addresses. You must allow connections
from all egress IP addresses associated with your region. Connections may
originate from any one of the egress IP addresses.

To find the egress IP addresses associated with your region, you can query the
[`mz_egress_ips`](/sql/system-catalog/mz_catalog/#mz_egress_ips) system table.

{{< note >}}
On rare occasion, we may need to change the egress IP addresses associated with
a region. We make every effort to provide advance notice of such changes.
{{< /note >}}


## Example

Show the static egress IPs associated with a region:

```sql
SELECT * FROM mz_egress_ips;
```

```nofmt
   egress_ip
----------------
 1.2.3.4
 5.6.7.8
```
