{% materialization incremental, adapter='materialize' %}
   -- Todo@jldlaughlin: Fail in a useful way!

   -- TL;DR: dbt-materialize does not support incremental models, use materializedview
   -- models instead.

   -- Longer explanation:
   -- Incremental models are useful because instead of having to rebuild the entire table
   -- in your data warehouse, dbt will only apply your transformations to new data.
   -- Luckily, this is exactly what Materialize's materialized views do! As new data streams
   -- in, Materialize only performs work on that new data. And, all this happens without
   -- extra configurations or scheduled refreshes.
   -- For more information, please visit: https://materialize.com/docs/
{% endmaterialization %}
