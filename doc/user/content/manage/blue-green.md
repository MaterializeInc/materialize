---
title: "Blue/Green Deployments"
description: "How to perform Blue/Green Deployments in Materialize."
menu:
  main:
    parent: manage
    name: "Blue/Green Deployments"
    weight: 11
---

Materialize offers some helpful tools to manage Blue/Green deployments. We recommend using a Blue/Green style workflow any time you need to deploy schema or view definition changes to production views.

## Structuring your environment

{{< note >}}
We highly recommend using dbt to manage deployments, but this workflow can be recreated using your own custom scripts.
{{</ note >}}

1. Use schemas and clusters to isolate your changes. Schemas should be the primary container for database objects (views, indexes, materialized views), while clusters are the compute resources that will perform view maintenance and serve queries.
2. Configure `profiles.yml` to set up the different targets. Use consistent naming across the clusters and schemas that will be deployed together.

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
      deploy:
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
  
  target: deploy
  ```

3. Leave your sources in the public schema and don't touch them. Instead, define them as `sources` in a `schema.yml` file. Since the same source can be shared across your production views and clusters, you won’t need to recreate them.
4. Create cluster `prod` and schema `prod` in Materialize and deploy your production objects here.

## Deploying changes to production
1. When introducing changes, deploy the updated views to a separate cluster `prod_deploy` in schema `prod_deploy`. We recommend using a CI/CD Workflow and validating all changes on a staging environment before building them in production.

  ```bash
   dbt run --exclude config.materialized:source --target prod_deploy
  ```

2. For multi-cluster deployments, co-locate clusters with dependent views or indexes in the same schema. For instance:
    - Schema: prod
        - Cluster: prod_compute
        - Cluster: prod_serve
        - Views: static_view
        - Materialized View: maintained_view
        - Indexes: index_on_mainained_view_idx

## Cutting over
1. Wait for all views on `prod_deploy` to rehydrate. You can look at the lag in the Workflow graph in the [Materialize Console](https://console.materialize.com) to get a rough sense of when rehydration is complete. The view will appear as “caught up”, and you can compare both the `prod` and `prod_deploy` versions by viewing the Workflow graph from a common source or other upstream Materialization.
2. Perform your end-to-end application tests on `prod_deploy` objects to ensure it is safe to cut over.
3. Use the `SWAP` operation to atomically rename your objects in a way that is transparent to clients. (Under the hood, this is series of `ALTER … RENAME` operations).

  ```sql
  BEGIN; 
  ALTER SCHEMA SWAP prod prod_deploy; 
  ALTER CLUSTER SWAP prod prod_deploy; 
  COMMIT;
  ```

4. Now that changes are running in `prod` and the legacy version is in `prod_deploy`, you can drop the prod_deploy compute objects and schema.

  ```sql
  DROP CLUSTER prod_deploy CASCADE;
  DROP SCHEMA prod_deploy CASCADE;
  ```

## Additional customizations

### Fine-grained `ALTER ... RENAME`
Schemas and clusters are the most common units for swapping out. But you can also use `ALTER...RENAME`operations on:

- Schemas
- Coming soon: [Databases](https://github.com/MaterializeInc/materialize/issues/3680)
- Tables
- Views
- Materialized Views
- Indexes

  ```sql
  BEGIN;
  -- Swap schemas
  ALTER SCHEMA prod RENAME to temp;
  ALTER SCHEMA prod_deploy RENAME to prod;
  ALTER SCHEMA temp RENAME to prod_deploy; 
  -- Swap multiple clusters
  ALTER CLUSTER prod_serve RENAME to temp_compute;
  ALTER CLUSTER prod_deploy_serve RENAME to prod_serve;
  ALTER CLUSTER temp_serve RENAME to prod_deploy_serve;
  
  ALTER CLUSTER prod_compute RENAME to temp_compute;
  ALTER CLUSTER prod_deploy_compute RENAME to prod_compute;
  ALTER CLUSTER temp_compute RENAME to prod_deploy_compute;
  COMMIT;
  ```

These operations can be interspersed with ALTER SWAP operations as well.

### Canary deployments
Deploy a canary instance of your application that looks in schema `prod_deploy` and connects to cluster `prod_deploy`. Slowly move traffic to the canary until you are confident in the new deployment. See an example for [traffic shifting in AWS Lambda](https://aws.amazon.com/blogs/compute/implementing-canary-deployments-of-aws-lambda-functions-with-alias-traffic-shifting/).
