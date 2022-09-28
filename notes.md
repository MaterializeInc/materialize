I have two concerns, which are really just around how things are named/the API
design. If that ship has sailed, please ignore.

## Subsources

I think this notion of a `subsource` is a little odd; for instance, the behavior
of these components is identical to the proposed design of `StorageCollection`
(`StoreManagedCollection`, or `StorageManagedTable`) where it's a  collection
written by someone else, but has table-like semantics AFAICT.

I wonder if there's some neater way to unify these concepts. The thing that
jumps to mind is calling it a `Collection` from the users' perspective; users
can expect to query `Collection`s, but cannot write directly to them.

## `FOR TABLE`

This exemplified what I find odd about the notion of `FOR TABLE` to be the
syntax to create subsources:

```rust
impl<T: AstInfo> AstDisplay for CreateSourceSubsources<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            Self::Subset(subsources) => {
                f.write_str("FOR TABLES (");
                f.write_node(&display::comma_separated(subsources));
                f.write_str(")");
            }
            Self::All => f.write_str("FOR ALL TABLES"),
        }
    }
}
impl_display_t!(CreateSourceSubsources);
```
