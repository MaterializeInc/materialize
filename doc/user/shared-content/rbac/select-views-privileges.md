If a relation is a view or a materialized view, `SELECT` privileges are required
on **both** the view/materialized view itself and all underlying relations
referenced in its definition in order to `SELECT`  from the view/materialized
view. Even users with `SELECT` privileges on the underlying relations in the
view/materialized view defintion (including those with **superuser** privileges)
must have `SELECT` privileges on the view/materialized view itself.
