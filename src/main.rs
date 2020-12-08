use differential_dataflow::input::Input;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::iterate::Iterate;
use differential_dataflow::operators::reduce::{Count, Threshold};
use differential_dataflow::operators::Join;
use differential_dataflow::Collection;
use timely::dataflow::{ProbeHandle, Scope};

fn main() {
    timely::execute(timely::Configuration::Thread, move |worker| {
        let mut probe = ProbeHandle::new();

        let mut input = worker.dataflow(|scope| {
            let (input_handle, input) = scope.new_collection();
            run_game_of_life(&input)
                .inspect(|((x, y), time, diff)| {
                    println!("x: {}, y: {}, time: {} diff: {}", x, y, time, diff)
                })
                .probe_with(&mut probe);

            input_handle
        });

        let inputs = vec![(2, 2), (2, 3), (2, 4), (3, 2)];
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
    live_cells.iterate(|live| {
        let maybe_live_cells = live
            .flat_map(|(x, y)| {
                [
                    (-1, -1),
                    (-1, 0),
                    (-1, 1),
                    (0, -1),
                    (0, 1),
                    (1, -1),
                    (1, 0),
                    (1, 1),
                ]
                .iter()
                .map(move |(dx, dy)| ((x + dx, y + dy)))
            })
            .count();

        let live_with_three_neighbors = maybe_live_cells
            .filter(|(_, count)| *count == 3)
            .map(|(cell, _)| cell);
        let live_with_two_neighbors = maybe_live_cells
            .filter(|(_, count)| *count == 2)
            .semijoin(&live)
            .map(|(cell, _)| cell);

        let live_next_round = live_with_two_neighbors
            .concat(&live_with_three_neighbors)
            .distinct();

        live_next_round
    })
}
