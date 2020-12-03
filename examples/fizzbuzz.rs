use differential_dataflow::input::Input;
use differential_dataflow::operators::{Iterate, Threshold};
use differential_dataflow::Collection;
use timely::dataflow::scopes::Child;

fn main() {
    timely::execute_directly(move |worker| {
        worker.dataflow(|scope| {
            let integers: Collection<Child<'_, _, u32>, (i32, String)> = scope
                .new_collection_from(vec![(1, "".to_string())].into_iter())
                .1;

            let result = integers.iterate(|input| {
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
                output.filter(|(x, _)| *x <= 100)
            });

            result
                .inspect(|(x, time, diff)| println!("x: {:?} time: {:?} diff: {}", x, time, diff));
        });
    })
}
