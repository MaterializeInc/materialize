# Query Graph Model

This module contains the implementation of the Query Graph Model as specified
[here](../../../../doc/developer/design/20210707_qgm_sql_high_level_representation.md).

## Testing

Tests around the Query Graph Model, for both the model generation logic and the
model transformat logic, are usually `datadriven` tests that result in multiple
`graphviz` graphs, that need to be rendered and visually validated. The following
shell function extracts all the `graphviz` graphs containing in one of these tests
and renders them as PNG files. The snippet after it shows how it can be used with
one of these tests.


```sh
extract_graph() {
    sed -n "/^digraph G {$/,/^}$/p" $1 | csplit --prefix $1- --suffix-format "%04d.dot" --elide-empty-files - '/^digraph G {$/' '{*}'
    for i in $1-*.dot; do
        dot -Tpng -O $i
    done
}
```

```
$ extract_graph src/sql/tests/querymodel/basic
...
$ eog src/sql/tests/querymodel/basic*.png &
```
