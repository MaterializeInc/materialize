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

Each materialize region is associated with a unique set of static egress [Classless Inter-Domain Routing (CIDR)](https://aws.amazon.com/what-is/cidr/) blocks.
All connections to the public internet initiated by your Materialize region will
originate from an address in the provided blocks.

When connecting Materialize to services in your private networks (e.g., Kafka
clusters or PostgreSQL databases), you must configure any firewalls to allow
connections from all CIDR blocks associated with your region. **Connections may
originate from any address in the region**.

Region          | CIDR
----------------|------------
`aws/us-east-1` | 98.80.4.128/27
`aws/us-east-1` | 3.215.237.176/32
`aws/us-west-2` | 44.242.185.160/27
`aws/us-west-2` | 52.37.108.9/32
`aws/eu-west-1` | 108.128.128.96/27
`aws/eu-west-1` | 54.229.252.215/32

{{< note >}}
On rare occasion, we may need to change the static egress CIDR blocks associated with
a region. We make every effort to provide advance notice of such changes.
{{< /note >}}


## Fetch static egress IPs
You can fetch the static egress address blocks associated with your region by querying
the [`mz_egress_ips`](/sql/system-catalog/mz_catalog/#mz_egress_ips) system catalog
table.

```mzsql
SELECT * FROM mz_egress_ips;
```

<p></p>

```nofmt
  egress_ip    | prefix_length |      cidr
---------------+---------------+-----------------
 3.215.237.176 |            32 | 3.215.237.176/32
 98.80.4.128   |            27 | 98.80.4.128/27
```

As an alternative, you can also submit an HTTP request to Materialize's
[SQL API](/integrations/http-api/) querying the [`mz_egress_ips`](/sql/system-catalog/mz_catalog/#mz_egress_ips)
system catalog table. In the request, specify the username, app password, and
host address of your Materialize region:


```
curl -s 'https://<host-address>/api/sql' \
    --header 'Content-Type: application/json' \
    --user '<username:app-password>' \
    --data '{ "query": "SELECT cidr from mz_egress_ips;" }' |\
    jq -r '.results[].rows[][]'
```
