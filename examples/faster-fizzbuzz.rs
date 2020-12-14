use differential_dataflow::input::Input;
use differential_dataflow::operators::Iterate;

fn main() {
    let limit: u64 = std::env::args()
        .nth(1)
        .map(|x| x.parse().expect("limit must be a valid u64"))
        .unwrap_or(100);

    timely::execute_directly(move |worker| {
        worker.dataflow::<u32, _, _>(|scope| {
            // Use integers instead of strings for "FizzBuzz" to avoid pointless
            // churning on allocations.
            let initial = scope.new_collection_from(vec![(1, 0)].into_iter()).1;

            // Start iterating with the empty collection (the filter is a handy
            // idiom for that). This simplifies the logic between iterations
            // and lets us avoid a consolidate.
            let result = initial.filter(|_| false).iterate(|input| {
                // Only use a logarithmic level of iterations by making sure
                // that each iteration does twice as much work as the iteration
                // before it. Still O(limit) total work but we get to amortize
                // the cost of iteration better.
                let successors = input.flat_map(|(x, _)| vec![2 * x, 2 * x + 1]).map(|x| {
                    let str = if x % 3 == 0 && x % 5 == 0 {
                        3
                    } else if x % 5 == 0 {
                        2
                    } else if x % 3 == 0 {
                        1
                    } else {
                        0
                    };

                    (x, str)
                });

                // Always only concatenate the initial element rather than the
                // prior full set. This lets us ensure there will be no duplicates
                // and so we don't have to do any stateful operators like distinct
                // or consolidate.
                let output = initial.enter(&input.scope()).concat(&successors);
                output.filter(move |(x, _)| *x <= limit)
            });

            result.inspect(|(x, time, m)| {
                println!("x: {:?} time: {:?} multiplicity: {}", x, time, m)
            });
        });
    })
}
