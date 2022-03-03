# Materialize Platform: Cloud Architecture

TODO: This document will describe the architecture of the CLOUD OPERATOR
component that spins up the STORAGE, COMPUTE, and CONTROL layers described in
the [Database Architecture](architecture-db.md).

## How to read this document

⚠️ **WARNING!** ⚠️ This document is a work in progress! At this stage it is a
loose collection of raw thoughts.

## Customer regions

What's called a ["region"](ux.md#region) in user-facing code will be internally
called an "environment", to avoid confusion with a cloud-provider region.

Each region will correspond to a namespace in Kubernetes.

@petrosagg [points out][letsencrypt-limit] that Let's Encrypt will only issue 50
certificates per root domain per week, which we'll _definitely_ run up against.
We'll need a strategy to work around this.

## CLOUD OPERATOR

The CLOUD OPERATOR is the evolution of today's deployment controller.

TODO: Spec out the interface between CLOUD OPERATOR and STORAGE, COMPUTE, and
CONTROL.

TODO: engage cloud team.

### Service creation

TODO.

### Secrets

TODO.

## Terraform provider

Building a Terraform provider that manages clusters, roles, databases, schemas,
and secrets should be straightforward. Because all of these objects are managed
via SQL, our Terraform provider can be a fork of [terraform-provider-postgresql]
with only minor modifications.

Supporting sources and sinks in the Terraform provider will be more challenging,
because it amounts to remapping the `CREATE SOURCE` and `CREATE SINK` syntax
to Terraform resources, and that syntax is extremely complex.

[letsencrypt-limit]: https://github.com/MaterializeInc/materialize/pull/10319#discussion_r795500101
