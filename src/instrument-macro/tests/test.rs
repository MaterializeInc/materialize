use mz_instrument_macro::instrument;

#[instrument(name = "my_span", level = "trace", ret, fields(next = 1))]
fn test_instrument() {}

#[instrument(skip_all)]
fn test_instrument_skipall() {}
