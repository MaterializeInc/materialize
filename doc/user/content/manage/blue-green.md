---
title: "Blue/Green deployments"
description: "How to perform Blue/Green Deployments in Materialize."
menu:
  main:
    parent: manage
    name: "Blue/Green deployments"
    weight: 13
---

Materialize offers some helpful tools to manage Blue/Green deployments. We
recommend using the Blue/Green pattern any time you need to deploy changes to
the definition of objects in Materialize in production environments.

## Structuring your environment

{{< note >}}
We recommend using [dbt](../dbt/) to manage deployments, but you can recreate this
workflow using your own custom scripts.
{{</ note >}}

1. Use schemas and clusters to isolate your changes. Schemas should be the
primary container for database objects (views, indexes, and materialized
views), while clusters are the compute resources that will perform view
maintenance and serve queries.

1. Configure `profiles.yml` to set up the different targets. Use consistent
naming across the clusters and schemas that will be deployed together. In this
example, we'll switch between two targets: `prod` and `prod_deploy`.

  ```yaml
  default:
    outputs:

      prod:
        type: materialize
        threads: 1
        host: <host>
        port: 6875
        user: <user@domain.com>
        pass: <password>
        database: materialize
        schema: prod
        cluster: prod
        sslmode: require
      prod_deploy:
        type: materialize
        threads: 1
        host: <host>
        port: 6875
        user: <user@domain.com>
        pass: <password>
        database: materialize
        schema: prod_deploy
        cluster: prod_deploy
        sslmode: require

  target: prod_deploy
  ```

1. Leave your sources in a separate schema (e.g. `public`) and don't touch them.
Instead, define them as `sources` in a `schema.yml` file. Since the same source
can be shared across your production views and clusters, you won’t need to
recreate them.

1. Create cluster `prod` and schema `prod` in Materialize and deploy your
production objects here.

## Deploying changes to production

1. When introducing changes, deploy the updated views to a separate cluster
`prod_deploy` in schema `prod_deploy`. We recommend using a CI/CD Workflow and
validating all changes on a staging environment before building them in
production.

  ```bash
   dbt run --exclude config.materialized:source --target prod_deploy
  ```

1. For multi-cluster deployments, co-locate clusters with dependent views or
indexes in the same schema. For instance:

    - Schema: prod
        - Cluster: prod_compute
        - Cluster: prod_serve
        - Views: static_view
        - Materialized View: maintained_view
        - Indexes: index_on_mainained_view_idx

## Cutting over

{{< note >}}
We're working on allowing programmatic hydration checks {{% gh 22166 %}}.
{{</ note >}}

1. Wait for all views on `prod_deploy` to hydrate. You can look at the lag in
the Workflow graph in the [Materialize Console](https://console.materialize.com)
to get a rough sense of when rehydration is complete. The view will appear
as “caught up”, and you can compare both the `prod` and `prod_deploy` versions
by viewing the Workflow graph from a common source or other upstream
Materialization.

1. Perform your end-to-end application tests on `prod_deploy` objects to ensure
it is safe to cut over.

1. Use the `SWAP` operation to atomically rename your objects in a way that is
transparent to clients.

  ```sql
  BEGIN;
  ALTER SCHEMA prod SWAP WITH prod_deploy;
  ALTER CLUSTER prod SWAP WITH prod_deploy;
  COMMIT;
  ```

1. Now that changes are running in `prod` and the legacy version is in
`prod_deploy`, you can drop the prod_deploy compute objects and schema.

  ```sql
  DROP CLUSTER prod_deploy CASCADE;
  DROP SCHEMA prod_deploy CASCADE;
  ```

## Additional customizations

### Fine-grained `ALTER ... RENAME`

Schemas and clusters are the most common units for swapping out. But you can
also use `ALTER...RENAME` operations on:

- Schemas
- Coming soon: [Databases](https://github.com/MaterializeInc/materialize/issues/3680)
- Tables
- Views
- Materialized Views
- Indexes

  ```sql
  BEGIN;
  -- Swap schemas
  ALTER SCHEMA prod RENAME TO temp;
  ALTER SCHEMA prod_deploy RENAME TO prod;
  ALTER SCHEMA temp RENAME TO prod_deploy;
  -- Swap multiple clusters
  ALTER CLUSTER prod_serve RENAME TO temp_compute;
  ALTER CLUSTER prod_deploy_serve RENAME TO prod_serve;
  ALTER CLUSTER temp_serve RENAME TO prod_deploy_serve;

  ALTER CLUSTER prod_compute RENAME TO temp_compute;
  ALTER CLUSTER prod_deploy_compute RENAME TO prod_compute;
  ALTER CLUSTER temp_compute RENAME TO prod_deploy_compute;
  COMMIT;
  ```

These operations can be interspersed with `ALTER SWAP` operations as well.

### Canary deployments

Deploy a canary instance of your application that looks in schema `prod_deploy`
and connects to cluster `prod_deploy`. Slowly move traffic to the canary until
you are confident in the new deployment. See an example for [traffic shifting
in AWS Lambda](https://aws.amazon.com/blogs/compute/implementing-canary-deployments-of-aws-lambda-functions-with-alias-traffic-shifting/).
