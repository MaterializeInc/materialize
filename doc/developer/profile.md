# Generating allocation flamegraphs on Linux

1. Download the `jeheap` tool. This is a Python script that converts jemalloc heap dunps to stack samples in the format expected by flamegraph.pl
```
git clone https://github.com/MaterializeInc/jeheap
```
2. Download the `flamegraph.pl` tool. This renders stack samples as flamegraphs in SVG.
```
git clone https://github.com/brendangregg/FlameGraph
```
3. Run `materialized` with allocation sampling enabled.
```
_RJEM_MALLOC_CONF=background_thread:true,prof:true,prof_prefix:jeprof.out,prof_final:true,lg_prof_sample:20,lg_prof_interval:35 cargo run --bin materialized --release -- -w30
```
4. Run your workflow in `materialized`. This will cause a stack trace to be taken every 2^20 bytes of allocations, and
a profile of the heap dumped every 2^35 allocations (32 GB). These will have names
like `jeprof.out.2606918.16.i16.heap`. If you need dumps more or less frequently, tune the `lg_prof_interval` parameter.
5. Convert to flamegraph.pl stack sample format.
```
jeheap/parse.py < jeprof.out.2606918.16.i16.heap > collated.heap
```
6. Convert to svg.
```
FlameGraph/flamegraph.pl < collated.heap > fg.svg
```
7. View in browser.
```
firefox fg.svg
```
