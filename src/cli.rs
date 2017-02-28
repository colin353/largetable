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
use std::io;

mod generated;

use linefeed::{Reader, ReadResult};

fn print_usage(program: &str, opts: getopts::Options) {
    let brief = format!("Usage: {} HOSTNAME:PORT [options]", program);
    print!("{}", opts.usage(&brief));
}

struct StdinSource {}

trait LineSource {
    fn next_line(&mut self) -> Option<String>;
}

impl LineSource for StdinSource {
    fn next_line(&mut self) -> Option<String> {
        let mut line = String::new();
        match io::stdin().read_line(&mut line) {
            Ok(0) | Err(_) => None,
            Ok(_) => {
                Some(line)
            }
        }
    }
}

impl StdinSource {
    fn new() -> StdinSource {
        StdinSource{}
    }
}

struct CLISource {
    reader: Reader<linefeed::terminal::DefaultTerminal>
}

impl LineSource for CLISource {
    fn next_line(&mut self) -> Option<String> {
        match self.reader.read_line() {
            Ok(ReadResult::Input(input)) => {
                // Record the command history, if the command isn't blank.
                if !input.trim().is_empty() {
                    self.reader.add_history(input.clone());
                }
                Some(input)
            },
            _ => None
        }
    }
}

impl CLISource {
    fn new() -> CLISource {
        println!("largetable-cli v{}", env!("CARGO_PKG_VERSION"));
        let mut reader = Reader::new("largetable").unwrap();
        reader.set_prompt("largetable> ");
        CLISource{
            reader: reader
        }
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();

    let mut opts = getopts::Options::new();
    opts.optflag("s", "stdin", "read input from stdin");
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

    let mut source: Box<LineSource> = if matches.opt_present("s") {
        Box::new(StdinSource::new())
    } else {
        Box::new(CLISource::new())
    };

    let client = largeclient::LargeClient::new(hostname.as_str()).unwrap();

    while let Some(ref input) = source.next_line() {
        // Read the input and process the query.
        match input.as_str() {
            x if x == "exit" => {
                println!("bye!");
                break;
            },
            x => {
                match query::Query::parse(x) {
                    Ok(q)   => {
                        // Submit the query to the database.
                        println!("{}", client.query(q));
                    }
                    Err(_)  => println!("That didn't parse.")
                }
            }
        }
    }
}
