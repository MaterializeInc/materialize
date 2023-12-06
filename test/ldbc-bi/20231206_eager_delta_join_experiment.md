I have run results comparing eager delta joins (plan delta joins when you would create fewer new arrangements than a differential join) to our current approach on LDBC BI.

Eager delta joins are a big win, at least for one-shot selects/peeks.

Peak memory usage in our current approach is **45.5GB**. Peak memory usage with eager delta joins is **9.4GB**.

Overall benchmark time is **20:43** in our current approach, compared to **10:03** in eager delta joins. Speedups are not uniform (Q15 and Q17 are _much_ faster with eager delta joins; other are slower).

More details. 🧵

# Methodology

There are eight queries (10, 11, 15, 16, 17, 18, 19, and 20) for which we _could_ plan delta joins but currently do not. (Queries 19 and 20 use auxiliary materialized views, making them particularly interesting tests---the others are just testing peak behavior.)

I run `\i run_delta_join_experiment.sql`, which does a fresh load of the entire database and then runs each query three times. I round the ms timing of each and then take the mean (manually, in post processing)

Summary of latency timings: https://www.notion.so/materialize/aa6608e725c74d6993072dfcc82df22e?v=2359e0b9580d44d7b20fefaecb85e90e&pvs=4

Grafana logs for the eager delta joins: https://grafana.dev.materialize.com/goto/Pbfs7ZDIR?orgId=1
Grafana logs for our current setup: https://grafana.dev.materialize.com/goto/1tVsnZvIg?orgId=1
