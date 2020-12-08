use differential_dataflow::input::Input;
use differential_dataflow::operators::{Iterate, Threshold};

fn main() {
    let limit: u64 = std::env::args()
        .nth(1)
        .map(|x| x.parse().expect("limit must be a valid u64"))
        .unwrap_or(100);

    timely::execute_directly(move |worker| {
        worker.dataflow::<u32, _, _>(|scope| {
            let initial = scope
                .new_collection_from(vec![(1, "".to_string())].into_iter())
                .1;

            let result = initial.iterate(|input| {
                let successors = input.map(|(x, _)| x + 1).map(|x| {
                    let str = if x % 3 == 0 && x % 5 == 0 {
                        "Fizz Buzz"
                    } else if x % 5 == 0 {
                        "Buzz"
                    } else if x % 3 == 0 {
                        "Fizz"
                    } else {
                        ""
                    };

                    (x, str.to_string())
                });
                let output = input.concat(&successors).distinct();
                output.filter(move |(x, _)| *x <= limit)
            });

            result.inspect(|(x, time, m)| {
                println!("x: {:?} time: {:?} multiplicity: {}", x, time, m)
            });
        });
    })
}
