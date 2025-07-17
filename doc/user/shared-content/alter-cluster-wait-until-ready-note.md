{{% note %}}

Using `WAIT UNTIL READY` requires that the session remain open: you need to
make sure the Console tab remains open or that your `psql` connection remains
stable.

Any interruption will cause a cancellation, no cluster changes will take
effect.

{{% /note %}}
