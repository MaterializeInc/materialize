## dbt's `jaffle_shop` + Materialize

If you've used dbt, odds are that you've run across dbt's beloved [`jaffle_shop`] demo project.
`jaffle_shop` allows users to quickly get up and running with dbt, using some spoofed, static
data for a fictional [jaffle shop].

At [Materialize], we specialize in maintaining fast and efficient views over your streaming data.
While we work on hosting a public source of demo streaming data for analytics, we wanted to provide
those familiar with dbt with an easy way to get up and running with our [`dbt-materialize`] adapter and
`jaffle_shop`'s static data.

> Note: This demo won't highlight what's powerful about Materialize. For that, check out our
  [`wikirecent-dbt` demo] or [our docs]!

[`jaffle_shop`]: https://github.com/fishtown-analytics/jaffle_shop
[jaffle shop]: https://australianfoodtimeline.com.au/jaffle-craze/
[Materialize]: https://materialize.com/
[`dbt-materialize`]: https://pypi.org/project/dbt-materialize/
[`wikirecent-dbt` demo]: https://github.com/MaterializeInc/materialize/blob/main/play/wikirecent-dbt/README.md
[our docs]: https://materialize.com/docs/

## Setting up a `jaffle_shop` with Materialize

Setting up the `jaffle_shop` project with Materialize is similar to setting it up with any other
data warehouse. The following instructions are based off the [traditional `jaffle_shop`] steps with a few
Materialize-specific modifications:

1. Follow [the first three steps of the `jaffle_shop` instructions], install dbt, clone the `jaffle_shop`
   repository, and navigate to the cloned repo on your machine.

1. In your cloned `dbt_project.yml`, make the following changes to the [model materializations]:
    ```nofmt
    models:
        jaffle_shop:
            marts:
                core:
                    materialized: materializedview
                    intermediate:
                        materialized: view
            staging:
                materialized: view
                tags: ["staging", "hourly"]
    ```
    Tip: Only materializing your `core` business models as materialized views, without materializing
    your intermediate or staging views, ensures that you're only using the memory you need in
    Materialize.

1. Install the [dbt-materialize plugin]. You may wish to do this within a Python virtual environment on your machine:
    ```bash
    python3 -m venv dbt-venv
    source dbt-venv/bin/activate
    pip install dbt-materialize
    ```

1. [Install and run Materialize]. The linked instructions will guide you through running a Materialize
   instance on your local machine. (Our cloud offering is being developed, but is not yet available!)

1. Create a `jaffle_shop` [dbt profile] that will connect to Materialize. The following profile will
   connect to a Materialize instance running locally on your machine. The `host` parameter will need
   to be updated if it's self-hosted in the cloud or run with Docker:
    ```nofmt
    jaffle_shop:
        outputs:
            dev:
            type: materialize
            threads: 1
            host: localhost
            port: 6875
            user: materialize
            pass: password
            dbname: materialize
            schema: jaffle_shop

        target: dev
    ```
    > If the `profiles.yml` you're using for this project is not located at `~/.dbt/`, you will have to provide [additional information] to use the `dbt` commands later on.

1. Check that your newly created `jaffle_shop` profile can connect to your Materialize instance:
   ```bash
   dbt debug
   ```

1. Load the static `jaffle_shop` data into Materialize:
    ```bash
    dbt seed
    ```

1. Run the provided models:
    ```bash
    dbt run
    ```

1. In a new shell, connect to Materialize to check out the `jaffle_shop` data you just loaded:
    ```bash
    # Connect to Materialize
    psql -U materialize -h localhost -p 6875
    ```

    ```bash
    # See all the newly created views
    materialize=> SHOW VIEWS IN jaffle_shop;
    # Output:
        name
    -------------------
    customer_orders
    customer_payments
    dim_customers
    fct_orders
    order_payments
    raw_customers
    raw_orders
    raw_payments
    stg_customers
    stg_orders
    stg_payments

    # See only the materialized views
    materialize=> SHOW MATERIALIZED VIEWS IN jaffle_shop;
    # Output:
        name
    ---------------
    dim_customers
    fct_orders
    raw_customers
    raw_orders
    raw_payments

    # Check out data in one of your core models
    materialize=> SELECT * FROM jaffle_shop.dim_customers WHERE customer_id = 1;
    # Output:
    customer_id | first_order | most_recent_order | number_of_orders | customer_lifetime_value
    ------------+-------------+-------------------+------------------+-------------------------
              1 | 2018-01-01  | 2018-02-10        |                2 |                      33
    ```
    To see what else you can do with your data in Materialize, [check out our docs].

1. Test the newly created models:
    ```bash
    dbt test
    ```

1. Generate and view the documentation for your `jaffle_shop` project:
    ```bash
    dbt docs generate
    dbt docs serve
    ```

[traditional `jaffle_shop`]: https://github.com/fishtown-analytics/jaffle_shop
[the usual `jaffle_shop` instructions]: https://github.com/fishtown-analytics/jaffle_shop
[model materializations]: https://docs.getdbt.com/docs/building-a-dbt-project/building-models/materializations
[dbt-materialize plugin]: https://pypi.org/project/dbt-materialize/
[Install and run Materialize]: https://materialize.com/docs/install/
[dbt profile]: https://docs.getdbt.com/dbt-cli/configure-your-profile
[additional information]: https://docs.getdbt.com/dbt-cli/configure-your-profile#advanced-profile-configuration
[check out our docs]: https://materialize.com/docs/
