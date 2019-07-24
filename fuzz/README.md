# fuzz

WIP [fuzz testing]. Not super useful yet.

## Installing `cargo-fuzz`

Until [rust-fuzz/cargo-fuzz#176] lands, you'll need to compile Nikhil's
fork from source:

```shell
git clone git@github.com:benesch/cargo-fuzz.git
cd cargo-fuzz
git checkout manifest-detection
cargo install --path .
```

Once that PR lands, these installation instructions can be shortened to:

```
cargo install cargo-fuzz
```

## Fuzzing sqllogictest

Before first use:

```shell
cd fuzz
cargo run --bin=build_corpus
```

To fuzz stuff:

```shell
RUSTFLAGS='-C codegen-units=1' cargo +nightly fuzz run --release fuzz_sqllogictest -- -dict=fuzz/sql.dict
```

Failing tests are added to `./fuzz/artifacts/fuzz_sqllogictest` and can be rerun
with `cargo test -p sqllogictest`.

TODO(jamii): figure out why sqllogictest::test::fuzz_artifacts can't be run by
name.

## Fuzzing testdrive

Before first use:

```shell
mkdir -p fuzz/corpus/fuzz_testdrive
cp -R test/* fuzz/corpus/fuzz_testdrive
```

To fuzz stuff:

```shell
RUSTFLAGS='-C codegen-units=1' cargo +nightly fuzz run --release fuzz_testdrive
```

Failing tests are added to `./fuzz/artifacts/fuzz_testdrive` and can be rerun
with `cargo run --bin testdrive ./fuzz/artificats/fuzz_testdrive/<FAILING_TEST>`.

[fuzz testing]: https://en.wikipedia.org/wiki/Fuzzing
[rust-fuzz/cargo-fuzz#176]: https://github.com/rust-fuzz/cargo-fuzz/pull/176
