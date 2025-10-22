---
title: "Kafka Proxies"
description: "How to connect a Kafka proxy as a source"
menu:
  main:
    parent: "kafka"
    name: "Kafka Proxies"
---

## Proxy Configurations

There are multiple ways to utilize a Kafka proxy in conjunction with Materialize:

- **Kafka Authentication Proxy/Gateway**: A Kafka proxy can be setup to allow
unauthenticated local connections within a private network to an external
Kafka cluster that requires authentication. In this setup the proxy handles
the authentication to the external cluster.

- Kafka Reverse Proxy: A proxy may also be used to handle authentication
into a private Kafka instance. In this setup, the proxy has trusted access into
a private Kafka cluster and then handles the authentication to outside parties.

{{< tabs tabID="1" >}}
{{< tab "kafka-proxy" >}}

### Working with [kafka-proxy](https://github.com/grepplabs/kafka-proxy)

When working with [kafka-proxy](https://github.com/grepplabs/kafka-proxy)
there are a few patterns to consider when it comes to listener configuration
and network security.  

#### Dynamic Listeners (random port)

[kafka-proxy](https://github.com/grepplabs/kafka-proxy) by default is configured
to handle multiple brokers in the upstream kafka topic by dynamically creating
listeners on new ports on the proxy instance. It is then expected for kafka
clients to start reading off of these ports. Utilizing an
[SSH Bastion](/ingest-data/network-security/ssh-tunnel/) will allow
Materialize to properly respond to the dynamic ports as they are created.

#### Static Listeners

If an [SSH Bastion](/ingest-data/network-security/ssh-tunnel/) is not an
option, an alternative approach is to configure
[kafka-proxy](https://github.com/grepplabs/kafka-proxy) without dynamic port
allocation. With this setup, each kafka broker will need to be set to a static
port. Once this is configured, Materialize from here can be configured to
either connect over AWS PrivateLink (as a load balancer can be configured to
point to the configured set of ports) or can be configured with an
[SSH Bastion](/ingest-data/network-security/ssh-tunnel/) as in the
[Dynamic Listeners](#dynamic-listeners-random-port) case.

#### SSL Configurations

[kafka-proxy](https://github.com/grepplabs/kafka-proxy) by default does not
configure TLS on the exposed endpoint. When leveraging this with the Kafka
source, it will be necessary to specify `SECURITY PROTOCOL = 'PLAINTEXT'` as
documented in [kafka-options](/sql/create-connection/#kafka-options). If SSL is
required, [kafka-proxy](https://github.com/grepplabs/kafka-proxy) can be
configured with TLS support and the connection can be setup with
`SECURITY PROTOCOL = 'SSL'` along with the `SSL KEY`, `SSL CERTIFICATE`, and
`SSL CERTIFICATE AUTHORITY` options.

{{< /tab >}}

{{< /tabs >}}
