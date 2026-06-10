If you are using **stacked views** (i.e., views whose definition depends
on other views) to reduce SQL complexity, generally, only the topmost
view (i.e., the view whose results will be served) should be a
materialized view. The underlying views that do not serve results do not
need to be materialized.
