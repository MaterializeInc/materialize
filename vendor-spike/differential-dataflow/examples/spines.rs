//! End-to-end spines benchmark over `key`, `val`, `vec`, and `col` arrangements
//! — an `arrange` + `join` of `String` keys, loaded then queried round by round:
//!
//! * `key` / `val` — the ord-neu `OrdKeySpine` / `OrdValSpine` traces.
//! * `vec` / `col` — the `Vec`- and columnar-trie-backed `Chunk` traces
//!   (`VecChunk` / `ColChunk`), both arranged through the generic `Chunk`
//!   harness via a `ContainerChunker`.
//!
//! Run as `cargo run --release --example spines -- <keys> <size> <mode>`.
//!
//! All four modes feed the same way (each insert allocates a `String`), so the
//! `vec`/`col` comparison is apples-to-apples on chunk layout.

use timely::dataflow::operators::probe::Handle;

use differential_dataflow::input::{Input, InputSession};

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// Feeds the dataflow round-by-round. Every mode arranges `String` keys, so they
/// all share this one input harness (each insert allocates a `String`).
struct Workload {
    data_input: InputSession<u64, String, isize>,
    keys_input: InputSession<u64, String, isize>,
}
impl Workload {
    fn insert_data(&mut self, val: usize) { self.data_input.insert(format!("{:?}", val)); }
    fn insert_keys(&mut self, val: usize) { self.keys_input.insert(format!("{:?}", val)); }
    fn advance_to(&mut self, t: u64) {
        self.data_input.advance_to(t); self.data_input.flush();
        self.keys_input.advance_to(t); self.keys_input.flush();
    }
}

fn main() {

    let keys: usize = std::env::args().nth(1).unwrap().parse().unwrap();
    let size: usize = std::env::args().nth(2).unwrap().parse().unwrap();

    let mode: String = std::env::args().nth(3).unwrap();

    println!("Running [{:?}] arrangement", mode);

    let timer1 = ::std::time::Instant::now();
    let timer2 = timer1.clone();

    timely::execute_from_args(std::env::args(), move |worker| {

        let mut probe = Handle::new();
        let mut workload: Workload = worker.dataflow(|scope| {

            use differential_dataflow::operators::arrange::Arrange;

            match mode.as_str() {
                "key" => {
                    use differential_dataflow::trace::implementations::ord_neu::{OrdKeyBatcher, RcOrdKeyBuilder, OrdKeySpine};
                    let (data_input, data) = scope.new_collection::<String, isize>();
                    let (keys_input, keys) = scope.new_collection::<String, isize>();
                    let data = data.arrange::<OrdKeyBatcher<String,_,isize>, RcOrdKeyBuilder<String,_,isize>, OrdKeySpine<String,_,isize>>();
                    let keys = keys.arrange::<OrdKeyBatcher<String,_,isize>, RcOrdKeyBuilder<String,_,isize>, OrdKeySpine<String,_,isize>>();
                    keys.join_core(data, |_k, &(), &()| Option::<()>::None)
                        .probe_with(&mut probe);
                    Workload { data_input, keys_input }
                },
                "val" => {
                    use differential_dataflow::trace::implementations::ord_neu::{OrdValBatcher, RcOrdValBuilder, OrdValSpine};
                    let (data_input, data) = scope.new_collection::<String, isize>();
                    let (keys_input, keys) = scope.new_collection::<String, isize>();
                    let data = data.map(|x| (x, ())).arrange::<OrdValBatcher<String,(),_,isize>, RcOrdValBuilder<String,(),_,isize>, OrdValSpine<String,(),_,isize>>();
                    let keys = keys.map(|x| (x, ())).arrange::<OrdValBatcher<String,(),_,isize>, RcOrdValBuilder<String,(),_,isize>, OrdValSpine<String,(),_,isize>>();
                    keys.join_core(data, |_k, &(), &()| Option::<()>::None)
                        .probe_with(&mut probe);
                    Workload { data_input, keys_input }
                },
                "col" => {
                    // The columnar (`UpdatesTyped` trie) `Chunk` trace, arranged through
                    // the same generic `Chunk` harness as `vec` via a
                    // `ContainerChunker<ColChunk>`.
                    use differential_dataflow::Hashable;
                    use differential_dataflow::columnar::trace::{Batcher, Builder, Spine, ColChunk};
                    use differential_dataflow::trace::implementations::chunker::ContainerChunker;
                    use differential_dataflow::operators::arrange::arrangement::arrange_core;
                    use timely::dataflow::channels::pact::Exchange;

                    let (data_input, data) = scope.new_collection::<String, isize>();
                    let (keys_input, keys) = scope.new_collection::<String, isize>();
                    let data = data.map(|x| (x, ()));
                    let keys = keys.map(|x| (x, ()));

                    type Ba = Batcher<String, (), u64, isize>;
                    type Bu = Builder<String, (), u64, isize>;
                    type Sp = Spine<String, (), u64, isize>;
                    type Chu = ContainerChunker<ColChunk<(String, (), u64, isize)>>;
                    let exchange = || Exchange::new(|u: &((String, ()), u64, isize)| (u.0).0.hashed().into());
                    let data = arrange_core::<_, _, Chu, Ba, Bu, Sp>(data.inner, exchange(), "DataArrange");
                    let keys = arrange_core::<_, _, Chu, Ba, Bu, Sp>(keys.inner, exchange(), "KeysArrange");
                    // `ColChunk`'s cursor yields `Val = columnar::Ref<()> = ()`, not `&()`.
                    keys.join_core(data, |_k, _, _| Option::<()>::None)
                        .probe_with(&mut probe);
                    Workload { data_input, keys_input }
                },
                "vec" => {
                    // The `Vec`-backed `Chunk` trace, fed like the row modes (each
                    // insert allocates a `String`) but arranged through the `Chunk`
                    // harness via a `ContainerChunker<VecChunk>`.
                    use differential_dataflow::Hashable;
                    use differential_dataflow::trace::chunk::vec::{ChunkBatcher, ChunkBuilder, ChunkSpine, VecChunk};
                    use differential_dataflow::trace::implementations::chunker::ContainerChunker;
                    use differential_dataflow::operators::arrange::arrangement::arrange_core;
                    use timely::dataflow::channels::pact::Exchange;

                    let (data_input, data) = scope.new_collection::<String, isize>();
                    let (keys_input, keys) = scope.new_collection::<String, isize>();
                    let data = data.map(|x| (x, ()));
                    let keys = keys.map(|x| (x, ()));

                    type Ba = ChunkBatcher<String, (), u64, isize>;
                    type Bu = ChunkBuilder<String, (), u64, isize>;
                    type Sp = ChunkSpine<String, (), u64, isize>;
                    type Chu = ContainerChunker<VecChunk<String, (), u64, isize>>;
                    let exchange = || Exchange::new(|u: &((String, ()), u64, isize)| (u.0).0.hashed().into());
                    let data = arrange_core::<_, _, Chu, Ba, Bu, Sp>(data.inner, exchange(), "DataArrange");
                    let keys = arrange_core::<_, _, Chu, Ba, Bu, Sp>(keys.inner, exchange(), "KeysArrange");
                    keys.join_core(data, |_k, &(), &()| Option::<()>::None)
                        .probe_with(&mut probe);
                    Workload { data_input, keys_input }
                },
                _ => {
                    panic!("unrecognized mode: {:?} (expected `key`, `val`, `vec`, or `col`)", mode);
                }
            }
        });

        // Load up data in batches.
        let mut counter = 0;
        let mut t: u64 = 1;
        while counter < 10 * keys {
            let mut i = worker.index();
            while i < size {
                let val = (counter + i) % keys;
                workload.insert_data(val);
                i += worker.peers();
            }
            counter += size;
            workload.advance_to(t);
            while probe.less_than(&t) {
                worker.step();
            }
            t += 1;
        }
        println!("{:?}\tloading complete", timer1.elapsed());

        let mut queries = 0;

        while queries < 10 * keys {
            let mut i = worker.index();
            while i < size {
                let val = (queries + i) % keys;
                workload.insert_keys(val);
                i += worker.peers();
            }
            queries += size;
            workload.advance_to(t);
            while probe.less_than(&t) {
                worker.step();
            }
            t += 1;
        }

        println!("{:?}\tqueries complete", timer1.elapsed());

    }).unwrap();

    println!("{:?}\tshut down", timer2.elapsed());

}
