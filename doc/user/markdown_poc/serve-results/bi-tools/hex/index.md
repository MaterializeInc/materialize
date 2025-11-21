<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/)  /  [Serve results](/docs/serve-results/)  /  [Use
BI/data collaboration tools](/docs/serve-results/bi-tools/)

</div>

# Hex

This guide walks you through the steps required to use the collaborative
data notebook [Hex](https://hex.tech/) with Materialize.

## Create an integration

1.  Sign in to **[Hex](https://hex.tech/)**.

2.  Go to an existing project or create a new one.

3.  Go to
    ![](data:image/svg+xml;base64,PHN2ZyBkYXRhLWljb249ImRhdGEtc291cmNlcyIgaGVpZ2h0PSIxMyIgdmlld2JveD0iMCAwIDE1IDE1IiB3aWR0aD0iMTMiPjxnPjxwYXRoIGQ9Ik0xMSAyLjVMNS41IDkuNUg5LjVMOSAxMy41TDE1IDYuNUgxMC41TDExIDIuNVoiIGZpbGw9ImN1cnJlbnRDb2xvciIgb3BhY2l0eT0iMC4yIiAvPjxwYXRoIGNsaXAtcnVsZT0iZXZlbm9kZCIgZD0iTTIuMTQ5MTQgMC42MDkzODNMMi4xOTIzNCAwLjU5NTY4MUMzLjM3MTc5IDAuMjIxNTkxIDQuMDcwNDQgMCA3IDBDOC4wNDYyOCAwIDkuMDExNjUgMC4wNjQyMDgyIDkuODY3NyAwLjE4MTkwOEw5LjE5NDI5IDEuMTA3ODVDOC41MjgyMyAxLjAzODM2IDcuNzkyODcgMSA3IDFDNC4yMTc0NCAxIDMuNTc5MjQgMS4yMDMzNCAyLjU4MDg0IDEuNTIxNDNMMi40NTEzNiAxLjU2MjYyQzEuOTA5OTcgMS43MzQyNyAxLjUyMTY1IDEuOTI3NDkgMS4yNzkxIDIuMTE2M0MxLjAzMzE2IDIuMzA3NzUgMSAyLjQ0MTIxIDEgMi41QzEgMi41NTc5MSAxLjAzMjczIDIuNjkwOTEgMS4yNzg4IDIuODgyMTZDMS41MjEzNiAzLjA3MDY4IDEuOTA5NzMgMy4yNjM2NCAyLjQ1MTE4IDMuNDM1MDdMMi41ODA3NyAzLjQ3NjI0QzMuNTc5MTggMy43OTM5IDQuMjE3NTEgMy45OTcgNyAzLjk5N0w3LjA5MzIyIDMuOTk2ODJMNi4zNjg1OSA0Ljk5MzE4QzMuOTYxNTUgNC45NjI0MiAzLjI4MjI1IDQuNzQ3MjYgMi4xOTI4MiA0LjQwMjJMMi4xNDkzMiA0LjM4ODQzQzEuNzEyODYgNC4yNTAyNCAxLjMyMzIxIDQuMDg3MDIgMSAzLjg5NzA1VjcuNDk2QzEgNy41NTQ3OSAxLjAzMzE2IDcuNjg4MjUgMS4yNzkxIDcuODc5N0MxLjUyMTY1IDguMDY4NTEgMS45MDk5NyA4LjI2MTczIDIuNDUxMzYgOC40MzMzOEwyLjU4MDg0IDguNDc0NTdMMi41ODA4NSA4LjQ3NDU3QzIuOTM2NTcgOC41ODc5IDMuMjQ2NTYgOC42ODY2NyAzLjYyNDEgOC43NjY4NkwyLjk4ODc4IDkuNjQwNDNDMi43MjYxOCA5LjU2OTY0IDIuNDcyODYgOS40ODkyOSAyLjE5MjM0IDkuNDAwMzJMMi4xNDkxNCA5LjM4NjYyQzEuNzEyNzYgOS4yNDgyNiAxLjMyMzE4IDkuMDg0ODUgMSA4Ljg5NDY2VjEyLjQ5MkMxIDEyLjU1MDcgMS4wMzMxMiAxMi42ODQxIDEuMjc5MDMgMTIuODc1NUMxLjUyMTU1IDEzLjA2NDIgMS45MDk4NyAxMy4yNTczIDIuNDUxMjcgMTMuNDI4OUwyLjU4MDQ3IDEzLjQ2OTlDMy40NzU2MiAxMy43NTQ5IDQuMDgxMDkgMTMuOTQ3NyA2LjIwMTU0IDEzLjk4NDZMNi4xMDE3MSAxNC45ODI5QzMuOTA4OTIgMTQuOTM5OSAzLjI0MDU2IDE0LjcyOCAyLjE5MjA5IDE0LjM5NTdMMi4xNDkyMyAxNC4zODIxQzEuNTQ4ODggMTQuMTkxOSAxLjAzNzA3IDEzLjk1NDMgMC42NjQ5MSAxMy42NjQ3QzAuMjk2MTM0IDEzLjM3NzcgMCAxMi45ODUzIDAgMTIuNDkyVjIuNUMwIDIuMDA2NzkgMC4yOTYwOTQgMS42MTQyNSAwLjY2NDgzNSAxLjMyNzJDMS4wMzY5OCAxLjAzNzUxIDEuNTQ4NzggMC43OTk3MyAyLjE0OTE0IDAuNjA5MzgzWk00IDEwTDEyIDBMMTEgNkgxNkw4IDE2TDkgMTBINFpNOS44MTk1NCA3SDEzLjkxOTRMOS42MTc1OSAxMi4zNzcyTDEwLjE4MDUgOUg2LjA4MDYyTDEwLjM4MjQgMy42MjI3N0w5LjgxOTU0IDdaIiBmaWxsPSJjdXJyZW50Q29sb3IiIGZpbGwtcnVsZT0iZXZlbm9kZCIgLz48L2c+PC9nPjwvc3ZnPg==)
    **Data Sources \> +Add \> Create data connection… \> Materialize**.

4.  Search and click the **Materialize** option.

5.  Enter the connection fields as follows:

    | Field               | Value                                             |
    |---------------------|---------------------------------------------------|
    | Name                | Materialize.                                      |
    | Description         | A description you prefer.                         |
    | Host & Port         | Materialize host name, and **6875** for the port. |
    | Database            | **materialize**                                   |
    | Authentication type | Choose the **Password** option.                   |
    | Username            | Materialize user.                                 |
    | Password            | App-specific password.                            |

6.  Click the **Create connection** button.

## Configure a custom cluster

To direct queries to a specific cluster, [set the cluster at the role
level](/docs/sql/alter-role) using the following SQL statement:

<div class="highlight">

``` chroma
ALTER ROLE <your_user> SET CLUSTER = <custom_cluster>;
```

</div>

Replace `<your_user>` with the name of your Materialize role and
`<custom_cluster>` with the name of the cluster you want to use.

Once set, all new sessions for that user will automatically run in the
specified cluster, eliminating the need to manually specify it in each
query or connection.

## Execute and visualize a query

1.  Create a new SQL cell.

2.  Inside the cell, select the new **Materialize** connection and paste
    the following query:

    <div class="highlight">

    ``` chroma
    SELECT
        number,
        row_num
    FROM (
        SELECT
            power(series_number, 2) AS number,
            row_number()
                OVER
                (ORDER BY series_number ASC, series_number DESC)
            AS row_num
        FROM (
            SELECT generate_series(0, 1000) AS series_number
        ) AS subquery
    );
    ```

    </div>

    This query generates a series of 1000 numbers squared and assigns
    row numbers to each.

3.  Click the
    ![](data:image/svg+xml;base64,PHN2ZyBjb2xvcj0iIzg2OGVhNCIgZGF0YS1pY29uPSJydW4iIGhlaWdodD0iMTYiIHZpZXdib3g9IjAgMCAxMyAxMyIgd2lkdGg9IjE2Ij48ZGVzYz5keW5hbWljPC9kZXNjPjxnIHN0cm9rZS13aWR0aD0iMSI+PGc+PHBhdGggZD0iTTQuNSAxMkM0LjUgMTIuMTg0NCA0LjYwMTQ5IDEyLjM1MzggNC43NjQwNyAxMi40NDA4QzQuOTI2NjUgMTIuNTI3OCA1LjEyMzkyIDEyLjUxODMgNS4yNzczNSAxMi40MTZMMTEuMjc3MyA4LjQxNjAzQzExLjQxNjQgOC4zMjMyOSAxMS41IDguMTY3MTggMTEuNSA4QzExLjUgNy44MzI4MiAxMS40MTY0IDcuNjc2NzEgMTEuMjc3MyA3LjU4Mzk3TDUuMjc3MzUgMy41ODM5N0M1LjEyMzkyIDMuNDgxNjkgNC45MjY2NSAzLjQ3MjE1IDQuNzY0MDcgMy41NTkxNkM0LjYwMTQ5IDMuNjQ2MTcgNC41IDMuODE1NiA0LjUgNFYxMloiIGZpbGw9ImN1cnJlbnRDb2xvciIgZmlsbC1vcGFjaXR5PSIwLjMiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWpvaW49InJvdW5kIiAvPjwvZz48L2c+PC9zdmc+)
    **Run** button.

4.  Inside the cell, click the **Chart** button and configure as
    follows:

    1.  In the **X Axis** options, select the **row_num** column.
    2.  In the **Y Axis** options, select the **number** column.

    <img
    src="https://github.com/MaterializeInc/materialize/assets/11491779/2da93aad-9332-4d7c-a407-c068a856b9ed"
    width="1091" alt="Hex" />

### Related pages

For more information about Hex and data connections, visit [their
documentation.](https://learn.hex.tech/docs/connect-to-data/data-connections/overview)

</div>

<a href="#top" class="back-to-top">Back to top ↑</a>

<div class="theme-switcher">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzeXN0ZW0iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+U3lzdGVtIFRoZW1lPC90aXRsZT4KICAgICAgICA8cGF0aCBkPSJNMjU2IDE3NmE4MCA4MCAwIDEwODAgODAgODAuMjQgODAuMjQgMCAwMC04MC04MHptMTcyLjcyIDgwYTE2NS41MyAxNjUuNTMgMCAwMS0xLjY0IDIyLjM0bDQ4LjY5IDM4LjEyYTExLjU5IDExLjU5IDAgMDEyLjYzIDE0Ljc4bC00Ni4wNiA3OS41MmExMS42NCAxMS42NCAwIDAxLTE0LjE0IDQuOTNsLTU3LjI1LTIzYTE3Ni41NiAxNzYuNTYgMCAwMS0zOC44MiAyMi42N2wtOC41NiA2MC43OGExMS45MyAxMS45MyAwIDAxLTExLjUxIDkuODZoLTkyLjEyYTEyIDEyIDAgMDEtMTEuNTEtOS41M2wtOC41Ni02MC43OEExNjkuMyAxNjkuMyAwIDAxMTUxLjA1IDM5M0w5My44IDQxNmExMS42NCAxMS42NCAwIDAxLTE0LjE0LTQuOTJMMzMuNiAzMzEuNTdhMTEuNTkgMTEuNTkgMCAwMTIuNjMtMTQuNzhsNDguNjktMzguMTJBMTc0LjU4IDE3NC41OCAwIDAxODMuMjggMjU2YTE2NS41MyAxNjUuNTMgMCAwMTEuNjQtMjIuMzRsLTQ4LjY5LTM4LjEyYTExLjU5IDExLjU5IDAgMDEtMi42My0xNC43OGw0Ni4wNi03OS41MmExMS42NCAxMS42NCAwIDAxMTQuMTQtNC45M2w1Ny4yNSAyM2ExNzYuNTYgMTc2LjU2IDAgMDEzOC44Mi0yMi42N2w4LjU2LTYwLjc4QTExLjkzIDExLjkzIDAgMDEyMDkuOTQgMjZoOTIuMTJhMTIgMTIgMCAwMTExLjUxIDkuNTNsOC41NiA2MC43OEExNjkuMyAxNjkuMyAwIDAxMzYxIDExOWw1Ny4yLTIzYTExLjY0IDExLjY0IDAgMDExNC4xNCA0LjkybDQ2LjA2IDc5LjUyYTExLjU5IDExLjU5IDAgMDEtMi42MyAxNC43OGwtNDguNjkgMzguMTJhMTc0LjU4IDE3NC41OCAwIDAxMS42NCAyMi42NnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="system" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzdW4iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+TGlnaHQgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0yMzQgMjZoNDR2OTJoLTQ0ek0yMzQgMzk0aDQ0djkyaC00NHpNMzM4LjAyNSAxNDIuODU3bDY1LjA1NC02NS4wNTQgMzEuMTEzIDMxLjExMy02NS4wNTQgNjUuMDU0ek03Ny44MTUgNDAzLjA3NGw2NS4wNTQtNjUuMDU0IDMxLjExMyAzMS4xMTMtNjUuMDU0IDY1LjA1NHpNMzk0IDIzNGg5MnY0NGgtOTJ6TTI2IDIzNGg5MnY0NEgyNnpNMzM4LjAyOSAzNjkuMTRsMzEuMTEyLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMiAzMS4xMTJ6TTc3LjgwMiAxMDguOTJsMzEuMTEzLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMyAzMS4xMTJ6TTI1NiAzNThhMTAyIDEwMiAwIDExMTAyLTEwMiAxMDIuMTIgMTAyLjEyIDAgMDEtMTAyIDEwMnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="sun" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJtb29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgPHRpdGxlPkRhcmsgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0xNTIuNjIgMTI2Ljc3YzAtMzMgNC44NS02Ni4zNSAxNy4yMy05NC43N0M4Ny41NCA2Ny44MyAzMiAxNTEuODkgMzIgMjQ3LjM4IDMyIDM3NS44NSAxMzYuMTUgNDgwIDI2NC42MiA0ODBjOTUuNDkgMCAxNzkuNTUtNTUuNTQgMjE1LjM4LTEzNy44NS0yOC40MiAxMi4zOC02MS44IDE3LjIzLTk0Ljc3IDE3LjIzLTEyOC40NyAwLTIzMi42MS0xMDQuMTQtMjMyLjYxLTIzMi42MXoiIC8+CiAgICAgIDwvc3ZnPg=="
class="moon" />

</div>

<div>

<a
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/serve-results/bi-tools/hex.md"
class="btn-ghost"><img
src="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTgiIGhlaWdodD0iMTgiIHZpZXdib3g9IjAgMCAyMyAyMyIgZmlsbD0iY3VycmVudENvbG9yIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPgogICAgICAgIDxwYXRoIGQ9Ik0yMC44OTQ1IDExLjQ5NjhDMjAuODk0NSAxMC4yMzk0IDIwLjYxNTEgOS4wNTE5IDIwLjEyNjEgNy44NjQzN0MxOS42MzcxIDYuNzQ2NjkgMTguOTM4NSA1LjY5ODg4IDE4LjE3MDEgNC45MzA0N0MxNy40MDE3IDQuMTYyMDcgMTYuMzUzOSAzLjQ2MzUgMTUuMjM2MiAyLjk3NDUyQzE0LjExODUgMi40ODU1MyAxMi44NjExIDIuMjA2MTMgMTEuNjAzOCAyLjIwNjEzQzEwLjM0NjQgMi4yMDYxMyA5LjE1ODg0IDIuNDg1NTMgNy45NzEzIDIuOTc0NTJDNi44NTM2MiAzLjQ2MzUgNS44MDU3OSA0LjE2MjA3IDUuMDM3MzggNC45MzA0N0M0LjI2ODk4IDUuNjk4ODggMy41NzA0NCA2Ljc0NjY5IDMuMDgxNDUgNy44NjQzN0MyLjU5MjQ3IDguOTgyMDUgMi4zMTMwNCAxMC4yMzk0IDIuMzEzMDQgMTEuNDk2OEMyLjMxMzA0IDEzLjUyMjYgMi45NDE3NCAxNS4zMzg5IDQuMTI5MjggMTcuMDE1NEM1LjMxNjgxIDE4LjY5MTkgNi45MjM0NyAxOS44MDk2IDguODA5NTYgMjAuMzY4NFYxNy45MjM1QzguMjUwNzIgMTcuOTkzNCA3Ljk3MTI5IDE3Ljk5MzMgNy44MzE1OCAxNy45OTMzQzYuNzgzNzYgMTcuOTkzMyA2LjAxNTM1IDE3LjUwNDQgNS41OTYyMiAxNi41MjY0QzUuNDU2NTEgMTYuMTc3MSA1LjI0Njk1IDE1LjgyNzggNS4wMzczOCAxNS42MTgzQzQuOTY3NTMgMTUuNTQ4NCA0Ljg5NzY4IDE1LjQ3ODYgNC43NTc5NyAxNS4zMzg5QzQuNjE4MjYgMTUuMTk5MiA0LjQ3ODU0IDE1LjEyOTMgNC4zMzg4MyAxNC45ODk2QzQuMTk5MTIgMTQuODQ5OSA0LjEyOTI4IDE0Ljc4IDQuMTI5MjggMTQuNzhDNC4xMjkyOCAxNC42NDAzIDQuMjY4OTggMTQuNjQwMyA0LjU0ODQgMTQuNjQwM0M0LjgyNzgyIDE0LjY0MDMgNS4xMDcyNCAxNC43MTAyIDUuMzE2ODEgMTQuODQ5OUM1LjUyNjM3IDE0Ljk4OTYgNS43MzU5NCAxNS4xMjkzIDUuODc1NjUgMTUuMzM4OUM2LjAxNTM2IDE1LjU0ODQgNi4xNTUwNyAxNS43NTggNi4zNjQ2MyAxNS45Njc2QzYuNTA0MzQgMTYuMTc3MSA2LjcxMzkxIDE2LjMxNjggNi45MjM0OCAxNi40NTY1QzcuMTMzMDQgMTYuNTk2MyA3LjQxMjQ2IDE2LjY2NjEgNy43NjE3MyAxNi42NjYxQzguMTgwODYgMTYuNjY2MSA4LjUzMDE0IDE2LjU5NjMgOC45NDkyNyAxNi40NTY1QzkuMDg4OTggMTUuODk3NyA5LjQzODI1IDE1LjQ3ODYgOS44NTczOCAxNS4xMjkzQzguMjUwNzIgMTQuOTg5NiA3LjA2MzE4IDE0LjU3MDUgNi4yOTQ3NyAxMy45NDE4QzUuNTI2MzcgMTMuMzEzMSA1LjEwNzI0IDEyLjE5NTQgNS4xMDcyNCAxMC42NTg2QzUuMTA3MjQgOS41NDA4OSA1LjQ1NjUyIDguNTYyOTQgNi4xNTUwNyA3Ljc5NDUzQzYuMDE1MzYgNy4zNzU0IDUuOTQ1NSA2Ljk1NjI2IDUuOTQ1NSA2LjUzNzEzQzUuOTQ1NSA1Ljk3ODI5IDYuMDg1MjEgNS40MTk0NiA2LjM2NDYzIDQuOTMwNDdDNi45MjM0NyA0LjkzMDQ3IDcuNDEyNDUgNS4wMDAzMiA3LjgzMTU4IDUuMjA5ODlDOC4yNTA3MSA1LjQxOTQ1IDguNzM5NyA1LjY5ODg2IDkuMjk4NTQgNi4xMTc5OUMxMC4wNjY5IDUuOTc4MjggMTAuODM1NCA1LjgzODU4IDExLjc0MzUgNS44Mzg1OEMxMi41MTE5IDUuODM4NTggMTMuMjgwMyA1LjkwODQ1IDEzLjk3ODggNi4wNDgxNkMxNC41Mzc3IDUuNjI5MDMgMTUuMDI2NyA1LjM0OTYgMTUuNDQ1OCA1LjIwOTg5QzE1Ljg2NDkgNS4wMDAzMiAxNi4zNTM5IDQuOTMwNDcgMTYuOTEyNyA0LjkzMDQ3QzE3LjE5MjIgNS40MTk0NiAxNy4zMzE5IDUuOTc4MjkgMTcuMzMxOSA2LjUzNzEzQzE3LjMzMTkgNi45NTYyNiAxNy4yNjIgNy4zNzU0IDE3LjEyMjMgNy43MjQ2N0MxNy44MjA5IDguNDkzMDggMTguMTcwMSA5LjQ3MTA1IDE4LjE3MDEgMTAuNTg4N0MxOC4xNzAxIDEyLjEyNTUgMTcuNzUxIDEzLjE3MzQgMTYuOTgyNiAxMy44NzE5QzE2LjIxNDIgMTQuNTcwNSAxNS4wMjY2IDE0LjkxOTcgMTMuNDIgMTUuMDU5NEMxNC4xMTg1IDE1LjU0ODQgMTQuMzk4IDE2LjE3NzEgMTQuMzk4IDE2Ljk0NTVWMjAuMjI4N0MxNi4zNTM5IDE5LjYgMTcuODkwNyAxOC40ODIzIDE5LjA3ODIgMTYuODc1N0MyMC4yNjU4IDE1LjMzODkgMjAuODk0NSAxMy41MjI2IDIwLjg5NDUgMTEuNDk2OFpNMjIuNzEwNyAxMS40OTY4QzIyLjcxMDcgMTMuNTIyNiAyMi4yMjE3IDE1LjQwODcgMjEuMjQzOCAxNy4wODUyQzIwLjI2NTggMTguODMxNiAxOC44Njg3IDIwLjE1ODggMTcuMTkyMiAyMS4xMzY4QzE1LjQ0NTggMjIuMTE0OCAxMy42Mjk2IDIyLjYwMzggMTEuNjAzOCAyMi42MDM4QzkuNTc3OTYgMjIuNjAzOCA3LjY5MTg4IDIyLjExNDggNi4wMTUzNiAyMS4xMzY4QzQuMjY4OTggMjAuMTU4OCAyLjk0MTc0IDE4Ljc2MTggMS45NjM3NyAxNy4wODUyQzAuOTg1Nzk2IDE1LjMzODkgMC40OTY4MDcgMTMuNTIyNiAwLjQ5NjgwNyAxMS40OTY4QzAuNDk2ODA3IDkuNDcxMDQgMC45ODU3OTYgNy41ODQ5NiAxLjk2Mzc3IDUuOTA4NDRDMi45NDE3NCA0LjE2MjA2IDQuMzM4ODQgMi44MzQ4MyA2LjAxNTM2IDEuODU2ODZDNy43NjE3MyAwLjg3ODg4NiA5LjU3Nzk2IDAuMzg5ODk3IDExLjYwMzggMC4zODk4OTdDMTMuNjI5NiAwLjM4OTg5NyAxNS41MTU2IDAuODc4ODg2IDE3LjE5MjIgMS44NTY4NkMxOC45Mzg1IDIuODM0ODMgMjAuMjY1OCA0LjIzMTkyIDIxLjI0MzggNS45MDg0NEMyMi4yMjE3IDcuNTg0OTYgMjIuNzEwNyA5LjQ3MTA0IDIyLjcxMDcgMTEuNDk2OFoiIC8+CiAgICAgIDwvc3ZnPg==" />
Edit this page</a>

</div>

<div class="footer-links">

[Home](https://materialize.com) [Status](https://status.materialize.com)
[GitHub](https://github.com/MaterializeInc/materialize)
[Blog](https://materialize.com/blog)
[Contact](https://materialize.com/contact)

Cookie Preferences

[Privacy Policy](https://materialize.com/privacy-policy/)

</div>

© 2025 Materialize Inc.

</div>
