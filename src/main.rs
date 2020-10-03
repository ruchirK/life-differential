use std::sync::mpsc::{sync_channel, SyncSender};

use differential_dataflow::input::Input;
use differential_dataflow::operators::iterate::Iterate;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::{Consolidate, Join, Reduce};
use differential_dataflow::operators::reduce::{Count, Threshold};
use differential_dataflow::trace::Cursor;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::Collection;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::ProbeHandle;
use timely::dataflow::Scope;
use timely::dataflow::operators::Operator;
use timely::order::Product;

fn game_of_life(output: SyncSender<((i32, i32), u32, isize)>) {
    timely::execute(timely::Configuration::Thread, move |worker| {
        let mut probe = ProbeHandle::new();

        let mut input = worker.dataflow(|scope| {
            let mut output = Some(output.clone());
            let (input_handle, input) = scope.new_collection();
            let final_state = run_game_of_life(&input)
                .inspect(|((x, y), time, diff)| println!("x: {}, y: {}, time: {} diff: {}", x, y, time, diff))
                .probe_with(&mut probe)
                .inner
                .sink(Pipeline, "game-of-life-sink", move |input| {
                    let output = output.as_mut().unwrap();
                    input.for_each(move |_, rows| {
                        for row in rows.iter() {
                            output.send(*row).unwrap();
                        }
                    });
                });

            input_handle
        });

        let inputs = vec![(2, 2), (2, 3), (2, 4), (3, 4), (4, 2)];
        for i in inputs.iter() {
            input.insert(*i);
        }

        input.advance_to(1 as u32);

        input.flush();

        worker.step_while(|| probe.less_than(input.time()));
    })
    .expect("completed without errors");
}

fn run_game_of_life<G: Input + Scope>(
    live_cells: &Collection<G, (i32, i32)>,
) -> Collection<G, (i32, i32)>
where
    G::Timestamp: Lattice + Ord,
{
    let generate = live_cells.scope().new_collection_from(vec![(-1, -1), (-1, 0)]).1
        .map(|delta| ((), delta));

    live_cells.iterate(|live| {
        let generate_neighbors = generate.enter(&live.scope());
        let neighbors = live.map(|x| ((), x)).join(&generate_neighbors)
            .map(|(_key, ((x, y), (dx, dy)))| ((x + dx, y + dy), (x, y)));

        let reborn_neighbors = neighbors.map(|(neighbor, _)| neighbor)
            .count()
            .filter(|(_, count)| *count == 3)
            .map(|(cell, _)| cell);

        live.map(|cell| (cell, ())).join(&neighbors)
            .map(|(_, ((), (x, y)))| (x, y))
            .count()
            .filter(|(_, count)| *count == 2 || *count == 3)
            .map(|(cell, _)| cell)
            .concat(&reborn_neighbors)
            .distinct()
            //.inspect(|((x, y), time, diff)| println!("x: {}, y: {}, time: {:?} diff: {}", x, y, time, diff))
    })
}

fn main() {
    let (send, recv) = sync_channel(1_000_000);

    println!("Game of Life!");
    game_of_life(send);

    println!("back in main");
    loop {
        if let Ok(((x, y), time, diff)) = recv.try_recv() {
            println!("recv: x {} y {} time {} diff {}", x, y, time, diff);
        }
    };

}
