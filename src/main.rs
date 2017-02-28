mod dtable;
mod mtable;
mod query;

extern crate protobuf;

#[macro_use]
extern crate serde_derive;
extern crate serde_yaml;
extern crate serde_json;
extern crate rand;
extern crate time;
extern crate regex;
extern crate glob;
extern crate byteorder;

extern crate hyper;
use hyper::server::{Server, Request, Response, Handler};
use hyper::status::StatusCode;

use std::io::Write;
use std::sync::Mutex;

use protobuf::Message;

mod base;
mod config;
mod generated;

struct RequestHandler {
    database: Mutex<base::Base>,
    config: config::ApplicationConfig
}

impl Handler for RequestHandler {
    fn handle(&self, mut req: Request, mut res: Response) {
        match req.method {
            hyper::Post => {
                match query::Query::from_bytes(&mut req) {
                    Ok(q)   => {
                        println!("query: {}", q);
                        let result = self.database.lock().unwrap().query_now(q);
                        result.into_generated().write_to_writer(&mut res.start().unwrap()).unwrap();
                    },
                    Err(_)  => {
                        println!("query: invalid data");
                        res.start().unwrap().write_all(b"invalid data").unwrap();
                    }
                };
            },
            _ => *res.status_mut() = StatusCode::MethodNotAllowed
        }
    }
}

fn main() {
    println!("largetable v{}", env!("CARGO_PKG_VERSION"));

    println!("loading config file ./config/config.yml");
    let config = config::ApplicationConfig::from_yaml(
        "./config/config.yml"
    ).unwrap();

    println!("loading database, mode = {}", config.mode);
    let mut database = match config.mode {
        config::Mode::Testing       => base::Base::new_stub(),
        config::Mode::Production    => base::Base::new(config.datadirectory.as_str())
    };

    database.load().unwrap();

    let h = RequestHandler{
        database: Mutex::new(database),
        config: config
    };

    println!("Listening on port {}.", h.config.port);
    Server::http(format!("0.0.0.0:{}", h.config.port)).unwrap().handle(h).unwrap();
}
