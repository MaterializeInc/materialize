use std::time::Duration;

use clap::Parser;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Inspect;

use mz_timely_util::builder_async::{Event, OperatorBuilder};

#[derive(Debug, Parser)]
#[clap(name = "gus")]
struct Cli {}

fn main() {
    let args = Cli::parse();
    let runtime = tokio::runtime::Runtime::new().unwrap();

    timely::example(move |scope| {
        let _guard = runtime.enter();

        let mut in_op = OperatorBuilder::new("gus".to_string(), scope.clone());
        let (mut in_output, in_stream) = in_op.new_output();
        in_op.build(move |mut caps| async move {
            let mut cap = caps.pop().unwrap();

            for i in 0..10 {
                {
                    let mut out = in_output.activate();
                    out.session(&cap).give(i);
                }
                cap.downgrade(&(cap.time() + 1));
            }
        });

        let mut async_op = OperatorBuilder::new("gus".to_string(), scope.clone());
        let (mut async_op_output, async_op_stream) = async_op.new_output();
        let mut async_in = async_op.new_input(&in_stream, Pipeline);
        async_op.build(move |_caps| async move {
            let _guard = runtime.enter();

            let mut temp: Vec<u64> = Vec::new();
            loop {
                match async_in.next().await {
                    Some(Event::Progress(_frontier)) => {
                        println!("made progress")
                    }
                    Some(Event::Data(cap, data)) => {
                        data.swap(&mut temp);

                        println!("starting sleep with {:?}", &temp);
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        println!("ending sleep with {:?}", &temp);

                        temp = temp.into_iter().map(|x| x + 100).collect();

                        let mut async_op_output = async_op_output.activate();
                        async_op_output.session(&cap).give_vec(&mut temp);
                    }
                    None => {
                        break;
                    }
                }
            }
        });

        async_op_stream.inspect_time(|t, x| println!("produced: {:?} at {}", x, t));
    });
}
