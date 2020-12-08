use differential_dataflow::input::Input;
use differential_dataflow::operators::{Join, Threshold};

fn main() {
    timely::execute_directly(move |worker| {
        let (mut first, mut second) = worker.dataflow(|scope| {
            let (first_handle, first) = scope.new_collection();
            let (second_handle, second) = scope.new_collection();

            let output = first
                .map(|x| (x, ()))
                .semijoin(&second)
                .map(|(x, _)| x)
                .distinct();
            output.inspect(|(x, time, m)| println!("x: {} time: {} multiplicity: {}", x, time, m));

            (first_handle, second_handle)
        });

        // Send some sample data to our dataflow
        for i in 0..10 {
            // Advance logical time to i
            first.advance_to(i);
            second.advance_to(i);

            for x in i..(i + 10) {
                first.insert(x);
                second.insert(x + 5);
            }
        }
    })
}
