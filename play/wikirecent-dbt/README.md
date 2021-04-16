## dbt + Materialize

Welcome! This demo project illustrates how to turn [streaming Wikipedia data](https://stream.wikimedia.org/?doc)
into materialized views in [Materialize](https://materialize.com/product/) using [dbt](https://www.getdbt.com/).

*Note*: the [`dbt-materialize`](https://github.com/MaterializeInc/dbt-materialize) adapter used for this
project is still a work in progress! If you hit a snag along the way, please open an issue or submit a PR.


To get everything you need to run dbt with Materialize, do the following:

1. Git clone this repo.

   ```
   git clone https://github.com/MaterializeInc/materialize
   ```

1. Install the `dbt-materialize` plugin. You may wish to do this within a Python virtual environment on your machine:
    ```bash
    python3 -m venv dbt-venv
    source dbt-venv/bin/activate
    pip install dbt-materialize
    ```

1. Replace or add the following profile to your `~/.dbt/profiles.yml` file:
    ```nofmt
    default:
      outputs:
        dev:
          type: materialize
          threads: 1
          host: localhost
          port: 6875
          user: materialize
          pass: pass
          dbname: materialize
          schema: analytics

      target: dev
    ```

1. Spin up a Materialize instance, [instructions are here](https://materialize.com/docs/install/). If your Materialize
   instance is not running at `localhost:6875`, update your `profiles.yml` information accordingly.

   If (and only if!) you're running Materialize via a Docker container, you will need to install `curl` on the
   container before you can run the demo. Once your Materialized container is running, get the container id with the following:
   ```bash
   docker ps | grep materialize
   ```

   Using the output container id, install `curl`:
   ```bash
   docker exec -it [materialized container id] /bin/sh

   apt-get update
   apt-get install curl
   ```

Once you've completed these steps, you're ready to run dbt with Materialize!

### dbt + Materialize demo

*Note: This project is a dbt-twist of our ["Getting Started"](https://materialize.com/docs/get-started/#create-a-real-time-stream)
demo. If you want to try creating these materialized views manually, check it out!*

Materialize is built to handle streams of data, and provide incredibly low-latency answers to queries over that data.
To show off that capability, we're going to create [materialized views](https://materialize.com/docs/sql/create-materialized-view/#main)
on top of streaming Wikipedia data using dbt.

1. To start, let's set up a stream of Wikipedia's recent changes, and simply write all the data we see
   to a file.

   > If (and only if!) you are running Materialize via a Docker container, run `docker exec -it [materialized container id] /bin/sh` before `curl`-ing to create this file directly within the Materialize container.

   From a new shell, run:
   ```nofmt
   while true; do
     curl --max-time 9999999 -N https://stream.wikimedia.org/v2/stream/recentchange >> wikirecent
   done
   ```
   Note the absolute path of the location of `wikirecent`, which we’ll need in the next step.

1. [Connect to your Materialize instance](https://materialize.com/docs/connect/cli/) from your shell:
   ```bash
   psql -U materialize -h localhost -p 6875 materialize
   ```

   Then, [create a source](https://materialize.com/docs/sql/create-source/text-file/#main) using your `wikirecent` file:
   ```nofmt
   CREATE SCHEMA wikimedia;

   CREATE SOURCE wikimedia.wikirecent
   FROM FILE '[path to wikirecent]' WITH (tail = true)
   FORMAT REGEX '^data: (?P<data>.*)';
   ```
   This source takes the lines from the stream, finds those that begins with `data:`, and then captures the rest of the
   line in a column called `data`.

   You can see the columns that get generated for this source:
   ```nofmt
    > SHOW SOURCES FROM wikimedia;
      name
    ------------
     wikirecent

    > SHOW COLUMNS FROM wikimedia.wikirecent;
      name    | nullable | type
     ------------+----------+------
    data       | t        | text
    mz_line_no | f        | int8
   ```

1. Now we can use dbt to create materialized views on top of `wikirecent`. In your shell, navigate to
   `play/wikirecent-dbt` within the clone of this repo on your local machine. Once
   you're there, run the following [dbt command](https://docs.getdbt.com/reference/dbt-commands/):
   ```nofmt
   dbt run
   ```
   > If the `profiles.yml` you're using for this project is not located in at `~/.dbt/`,
     you will have to provide [additional information](https://docs.getdbt.com/dbt-cli/configure-your-profile#advanced-profile-configuration) to `dbt run`.

   This command generates executable SQL from our model files (found in the `models` directory
   of this project), and executes that SQL against the target database, creating
   our materialized views.

   Note: If you installed `dbt-materialize` in a virtual environment, make sure it's activated.
   If you don't have it installed, please revisit the [setup](#setup-dbt--materialize) above.

1. Congratulations! You just used dbt to create materialized views in Materialize. You can verify the
   views were created from your `psql` shell connected to Materialize:
      ```nofmt
      > SHOW VIEWS FROM analytics;
           name
      ---------------
       recentchanges
       top10
       useredits
      ```

   More importantly, you can now query each of the views you created interactively. For example:
   ```nofmt
   > SELECT * FROM analytics.top10;
        user      | changes
   ---------------+---------
    Fæ            |   10834
    Sic19         |    7970
    Lockal        |   10450
    Akbarali      |    9631
    SuccuBot      |    8862
    Merge bot     |    4019
    Edoderoobot   |    3953
    Sarri.greek   |    3900
    BotMultichill |    4391
    SchlurcherBot |    8005
   (10 rows)
   ```

1. Now that you have your views, let's test expectations about those views. We believe that the `user` field in our `useredits` and `top10` models should never have null values. Run:
    ```nofmt
    dbt test
    ```

1. Finally, let's generate their docs using dbt. From your virtual environment, run:
   ```nofmt
   dbt docs generate
   dbt docs serve
   ```

    `dbt docs generate` generates this project's documentation website. `dbt docs serve` makes those
    docs available at http://localhost:8080.

    Once the local docs site is available, click into `materialize_wikirecent/models` to inspect the documentation
    for each of the created views.

### Resources:
- Learn more about Materialize [in the docs](https://materialize.com/docs/)
- Join Materialize's [chat](https://materializecommunity.slack.com/join/shared_invite/zt-jjwe1t45-klG9k7V7xibdtqA6bcFpyQ#/) on Slack for live discussions and support
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Join dbt's [chat](http://slack.getdbt.com/) on Slack for live discussions and support
