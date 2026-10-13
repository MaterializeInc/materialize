use timely::dataflow::operators::*;
use timely::dataflow::operators::capture::Extract;
use timely::progress::frontier::AntichainRef;

use differential_dataflow::input::InputSession;
use differential_dataflow::collection::AsCollection;
use differential_dataflow::trace::TraceReader;
use itertools::Itertools;

type Result = std::sync::mpsc::Receiver<timely::dataflow::operators::capture::Event<usize, Vec<((u64, i64), usize, i64)>>>;

fn run_test<T>(test: T, expected: Vec<(usize, Vec<((u64, i64), i64)>)>) -> ()
where
    T: FnOnce(Vec<Vec<((u64, u64), i64)>>)-> Result + ::std::panic::UnwindSafe,
{
    let input_epochs: Vec<Vec<((u64, u64), i64)>> = vec![
        vec![((2, 0), 1), ((1, 0), 1), ((1, 3), 1), ((4, 2), 1)],
        vec![((2, 0), -1), ((1, 0), -1)],
        vec![((2, 0), 1), ((1, 3), -1)],
        vec![((1, 0), 1), ((1, 3), 1), ((2, 0), -1), ((4, 2), -1)],
        vec![((2, 0), 1), ((4, 2), 1), ((1, 3), -1)],
        vec![((1, 3), 1), ((4, 2), -1)],
    ];
    let captured = (test)(input_epochs);
    let mut results = captured.extract().into_iter().flat_map(|(_, data)| data).collect::<Vec<_>>();
    results.sort_by_key(|&(_, t, _)| t);
    let out =
    results
        .into_iter()
        .chunk_by(|&(_, t, _)| t)
        .into_iter()
        .map(|(t, vals)| {
            let mut vec = vals.map(|(v, _, w)| (v, w)).collect::<Vec<_>>();
            vec.sort();
            (t, vec)
        }).collect::<Vec<_>>();
    // println!("out: {:?}", out);
    assert_eq!(out, expected);
}

#[test]
fn test_import_vanilla() {

    use differential_dataflow::trace::implementations::{ValBuilder, ValSpine};

    run_test(|input_epochs| {
        timely::execute(timely::Config::process(4), move |worker| {
            let ref input_epochs = input_epochs;
            let index = worker.index();
            let peers = worker.peers();

            let (mut input, mut trace) = worker.dataflow(|scope| {
                let (input, edges) = scope.new_input();
                let arranged = edges.as_collection()
                                    .arrange_by_key();
                (input, arranged.trace.clone())
            });
            let (captured,) = worker.dataflow(move |scope| {
                let imported = trace.import(scope);
                ::std::mem::drop(trace);
                let captured =
                imported
                    .reduce_abelian::<_,ValBuilder<_,_,_,_>,ValSpine<u64,i64,_,_>,_,_>(
                        "Reduce",
                        |_k, s, t| t.push((s.iter().map(|&(_, w)| w).sum(), 1i64)),
                        |vec, key, upds| { vec.clear(); vec.extend(upds.drain(..).map(|(v,t,r)| ((key.clone(), v),t,r))); },
                    )
                    .as_collection(|k,v| (k.clone(), v.clone()))
                    .inner
                    .exchange(|_| 0)
                    .capture();
                (captured,)
            });

            for (t, changes) in input_epochs.into_iter().enumerate() {
                if &t != input.time() {
                    input.advance_to(t);
                }
                let &time = input.time();
                for &((src, dst), w) in changes.into_iter().filter(|&&((src, _), _)| (src as usize) % peers == index) {
                    input.send(((src, dst), time, w));
                }
            }
            input.close();

            captured
        }).unwrap().join().into_iter().map(|x| x.unwrap()).next().unwrap()
    }, vec![
        (0, vec![
             ((1, 2), 1), ((2, 1), 1), ((4, 1), 1)]),
        (1, vec![
             ((1, 1), 1), ((1, 2), -1), ((2, 1), -1)]),
        (2, vec![
             ((1, 1), -1), ((2, 1), 1)]),
        (3, vec![
             ((1, 2), 1), ((2, 1), -1), ((4, 1), -1)]),
        (4, vec![
             ((1, 1), 1), ((1, 2), -1), ((2, 1), 1), ((4, 1), 1)]),
        (5, vec![
             ((1, 1), -1), ((1, 2), 1), ((4, 1), -1)]),
    ]);
}

#[test]
fn test_import_completed_dataflow() {

    use differential_dataflow::trace::implementations::{ValBuilder, ValSpine};

    // Runs the first dataflow to completion before constructing the subscriber.
    run_test(|input_epochs| {
        timely::execute(timely::Config::process(4), move |worker| {
            let ref input_epochs = input_epochs;
            let index = worker.index();
            let peers = worker.peers();

            let (mut input, mut trace, probe) = worker.dataflow(|scope| {
                let (input, edges) = scope.new_input();
                let arranged = edges.as_collection()
                                    .arrange_by_key();
                let (probe, _) = arranged.stream.probe();
                (input, arranged.trace.clone(), probe)
            });

            for (t, changes) in input_epochs.into_iter().enumerate() {
                if &t != input.time() {
                    input.advance_to(t);
                }
                let &time = input.time();
                for &((src, dst), w) in changes.into_iter().filter(|&&((src, _), _)| (src as usize) % peers == index) {
                    input.send(((src, dst), time, w));
                }
            }
            input.close();

            worker.step_while(|| !probe.done());

            let (_probe2, captured,) = worker.dataflow(move |scope| {
                let imported = trace.import(scope);
                ::std::mem::drop(trace);
                let stream =
                imported
                    .reduce_abelian::<_,ValBuilder<_,_,_,_>,ValSpine<u64,i64,_,_>,_,_>(
                        "Reduce",
                        |_k, s, t| t.push((s.iter().map(|&(_, w)| w).sum(), 1i64)),
                        |vec, key, upds| { vec.clear(); vec.extend(upds.drain(..).map(|(v,t,r)| ((key.clone(), v),t,r)));
                    })
                    .as_collection(|k,v| (k.clone(), v.clone()))
                    .inner
                    .exchange(|_| 0);
                let (probe, stream) = stream.probe();
                let captured = stream.capture();
                (probe, captured,)
            });

            captured
        }).unwrap().join().into_iter().map(|x| x.unwrap()).next().unwrap()
    }, vec![
        (0, vec![
             ((1, 2), 1), ((2, 1), 1), ((4, 1), 1)]),
        (1, vec![
             ((1, 1), 1), ((1, 2), -1), ((2, 1), -1)]),
        (2, vec![
             ((1, 1), -1), ((2, 1), 1)]),
        (3, vec![
             ((1, 2), 1), ((2, 1), -1), ((4, 1), -1)]),
        (4, vec![
             ((1, 1), 1), ((1, 2), -1), ((2, 1), 1), ((4, 1), 1)]),
        (5, vec![
             ((1, 1), -1), ((1, 2), 1), ((4, 1), -1)]),
    ]);
}

#[test]
fn test_import_stalled_dataflow() {
    // Runs the first dataflow to completion before constructing the subscriber.
    timely::execute(timely::Config::thread(), move |worker| {

        let mut input = InputSession::new();

        let (mut trace, probe1) = worker.dataflow(|scope| {

            let arranged =
            input
                .to_collection(scope)
                .arrange_by_self();

            let (probe, _) = arranged.stream.probe();
            (arranged.trace, probe)
        });

        input.insert("Hello".to_owned());
        input.advance_to(1);
        input.flush();

        worker.step_while(|| probe1.less_than(input.time()));

        input.advance_to(2);
        input.flush();

        worker.step_while(|| probe1.less_than(input.time()));

        let probe2 = worker.dataflow(|scope| {
            let (probe, _) = trace.import(scope).stream.probe();
            probe
        });

        worker.step();
        worker.step();
        worker.step();
        worker.step();

        println!("input:\t{:?}", input.time());
        probe2.with_frontier(|f| println!("{:?}", &f[..]));

        assert!(!probe2.less_than(input.time()));

    }).expect("Timely computation failed");
}

#[ignore]
#[test]
fn import_skewed() {

    run_test(|_input| {
        let captured = timely::execute(timely::Config::process(4), |worker| {
            let index = worker.index();
            let peers = worker.peers();

            let (mut input, mut trace) = worker.dataflow(|scope| {
                let (input, edges) = scope.new_input();
                let arranged = edges.as_collection()
                                    .arrange_by_key();
                (input, arranged.trace.clone())
            });

            input.send(((index as u64, 1), index, 1));
            input.close();

            trace.set_logical_compaction(AntichainRef::new(&[peers - index]));

            let (captured,) = worker.dataflow(move |scope| {
                let imported = trace.import(scope);
                let captured = imported
                      .as_collection(|k: &u64, c: &i64| (k.clone(), *c))
                      .inner.exchange(|_| 0)
                      .capture();
                (captured,)
            });

            captured
        }).unwrap().join().into_iter().map(|x| x.unwrap()).next().unwrap();

        // Worker `index` sent `index` at time `index`, but advanced its handle to `peers - index`.
        // As its data should be shuffled back to it (we used an UnsignedWrapper) this means that
        // `index` should be present at time `max(index, peers-index)`.

        captured
    }, vec![
        (2, vec![((2, 1), 1)]),
        (3, vec![((1, 1), 1), ((3, 1), 1)]),
        (4, vec![((0, 1), 1)]),
    ]);
}
