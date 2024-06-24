---
title: "Power BI"
description: "How to create dashboards with Power BI"
aliases:
  - /third-party/power-bi/
  - /integrations/power-bi/
menu:
  main:
    parent: "bi-tools"
    name: "Power BI"
    weight: 10
---

You can use [Power BI](https://powerbi.microsoft.com/) to create dashboards
based on the data maintained in Materialize.

## Database connection details

To set up a connection from Power BI to Materialize, use the native
[PostgreSQL database driver](https://learn.microsoft.com/en-us/power-query/connectors/postgresql#connect-to-a-postgresql-database-from-power-query-desktop)
with the following parameters:

Field                  | Value
---------------------- | ----------------
Database type          | **PostgreSQL database**
Server                 | Your Materialize host name followed by `:6875`<br> For example: `id.us-east-1.aws.materialize.cloud:6875`
Database               | **materialize**
Data Connectivity mode | **DirectQuery**
Database username      | Materialize user.
Database password      | App-specific password.

![Connect using the credentials provided in the Materialize console](https://github-production-user-asset-6210df.s3.amazonaws.com/21223421/266625944-de7dfdc6-7a94-4e87-ac0a-01e104512ffe.png)

## Troubleshooting

Errors like the following indicate that there is a problem with the connection settings:

1. `A non-recoverable error happened during a database lookup.`

    If you see this error, check that the server name is correct and that you are using port `6875`. Note that the server name should not include the protocol (`http://` or `https://`), and should not include a trailing slash (`/`) or the database name.

2. `PostgreSQL: No password has been provided but the backend requires one (in cleartext)`

    If you see this error, check that you have entered the correct password. If you are using an app-specific password, make sure that you have not included any spaces or other characters that are not part of the password. If the issue persists, try the following:

    - Go to **File**
    - Options and settings
    - Data source settings
    - Delete any references for the Materialize host from the data source list
    - Try to connect again

For more details and troubleshooting, check the
[Power BI documentation](https://learn.microsoft.com/en-us/power-query/connectors/postgresql#troubleshooting).

## Known limitations

When you connect to Materialize from Power BI, you will get a list of your tables and views.

However, [Power BI does not display materialized views](https://ideas.fabric.microsoft.com/ideas/idea/?ideaid=92420736-afdc-45b9-8962-743a53acfa66) in that list.

To work around this Power BI limitation, you can use one of the following options:

1. Create a view that selects from the materialized view, and then use the view in Power BI.

    For example, if you have a materialized view called `my_view`, you can create a view called `my_view_bi` with the following SQL:

    ```mzsql
    CREATE VIEW my_view_bi AS SELECT * FROM my_view;
    ```

    Then, in Power BI, you can use the `my_view_bi` view.

2. If applicable, instead of using a materialized view, create a view with an [index](/sql/create-index) instead.

3. Use the [Power BI Native query folding](https://learn.microsoft.com/en-us/power-query/connectors/postgresql#native-query-folding) to write your own query rather than using the Power BI UI. For example:

    ```
    = Value.NativeQuery(Source, "select * from my_view;")
    ```
