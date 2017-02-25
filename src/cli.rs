#![cfg_attr(feature="clippy", feature(plugin))]
#![cfg_attr(feature="clippy", plugin(clippy))]
extern crate protobuf;
extern crate linefeed;
extern crate glob;
extern crate regex;
extern crate byteorder;
extern crate time;
extern crate rand;
extern crate hyper;

extern crate largeclient;

use largeclient::query as query;

mod generated;

use linefeed::{Reader, ReadResult};

fn main() {
    println!("largetable-cli v{}", env!("CARGO_PKG_VERSION"));
    let mut reader = Reader::new("largetable").unwrap();
    reader.set_prompt("largetable> ");

    let client = largeclient::LargeClient::new("http://localhost:8080").unwrap();

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
            },
            x => {
                match query::Query::parse(x) {
                    Ok(q)   => {
                        // Submit the query to the database.
                        println!("response <-, {}", client.query(q));
                    }
                    Err(_)  => println!("That didn't parse.")
                }
            }
        }
    }
}
