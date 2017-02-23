#![cfg_attr(feature="clippy", feature(plugin))]
#![cfg_attr(feature="clippy", plugin(clippy))]
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate protobuf;
extern crate linefeed;
extern crate glob;
extern crate regex;
extern crate byteorder;
extern crate time;
extern crate rand;
extern crate hyper;

mod query;
mod mtable;
mod dtable;
mod generated;

use std::io::Read;
use linefeed::{Reader, ReadResult};

fn main() {
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
            },
            x => {
                match query::Query::parse(x) {
                    Ok(query)   => {
                        // Submit the query to the database.
                        let mut req = hyper::client::request::Request::new(
                            hyper::method::Method::Post,
                            hyper::Url::parse("http://localhost:8080").unwrap()
                        ).unwrap().start().unwrap();
                        query.write_to_writer(&mut req);
                        let mut response = String::new();
                        req.send().unwrap().read_to_string(&mut response).unwrap();
                        println!("response <- {}", response);
                    }
                    Err(_)  => println!("That didn't parse.")
                }
            }
        }
    }
}
