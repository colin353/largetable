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
extern crate getopts;
extern crate largeclient;

use largeclient::query as query;
use std::env;

mod generated;

use linefeed::{Reader, ReadResult};

fn print_usage(program: &str, opts: getopts::Options) {
    let brief = format!("Usage: {} HOSTNAME:PORT [options]", program);
    print!("{}", opts.usage(&brief));
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();

    let mut opts = getopts::Options::new();
    opts.optflag("h", "help", "print this help menu");
    opts.optflag("v", "version", "print the version number");

    let matches = match opts.parse(&args[1..]) {
        Ok(m) => { m }
        Err(f) => { panic!(f.to_string()) }
    };

    if matches.opt_present("h") {
        print_usage(&program, opts);
        return;
    }
    if matches.opt_present("v") {
        println!("{}", env!("CARGO_PKG_VERSION"));
        return;
    }
    let hostname = if !matches.free.is_empty() {
        matches.free[0].clone()
    } else {
        print_usage(&program, opts);
        return;
    };

    // If we reach this section of the code, the arguments appear valid.
    // So we'll start the CLI and connect to the provided hostname.
    println!("largetable-cli v{}", env!("CARGO_PKG_VERSION"));
    let mut reader = Reader::new("largetable").unwrap();
    reader.set_prompt("largetable> ");

    let client = largeclient::LargeClient::new(hostname.as_str()).unwrap();

    while let Ok(ReadResult::Input(input)) = reader.read_line() {
        // Record the command history, if the command isn't blank.
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
