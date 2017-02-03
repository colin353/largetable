#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate protobuf;
extern crate linefeed;
extern crate glob;

mod query;
mod mtable;
mod dtable;
mod generated;
mod base;

use linefeed::{Reader, ReadResult};

fn main() {
    let mut database = base::Base::new();
    database.load("./data/").unwrap();

    println!("largetable-cli v{}", env!("CARGO_PKG_VERSION"));
    let mut reader = Reader::new("largetable").unwrap();
    reader.set_prompt("largetable> ");

    while let Ok(ReadResult::Input(input)) = reader.read_line() {
        // Record the command history, if the string isn't blank.
        if !input.trim().is_empty() {
            reader.add_history(input.clone());
        }

        // Read the input and process the query.
        match &input {
            x if x == "exit" => {
                println!("bye!");
                break;
            }
            x => {
                match query::Query::parse(&x) {
                    Ok(_)   => println!("valid!"),
                    Err(_)  => println!("that didn't parse.")
                }
            }
        }
    }
}
