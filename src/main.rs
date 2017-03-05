/*
    main.rs

    This is the main entrypoint for the largetable server.
*/
#![feature(test)]

#[macro_use]
extern crate log;

#[macro_use]
extern crate serde_derive;

#[cfg(test)]
extern crate test;

extern crate protobuf;
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
mod mtable;
mod dtable;
mod query;
mod logger;

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
                        let result = self.database.lock().unwrap().query_now(q);
                        result.into_generated().write_to_writer(&mut res.start().unwrap()).unwrap();
                    },
                    Err(_)  => {
                        info!("received query with invalid data");
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
    logger::ApplicationLogger::init().unwrap();
    info!("loading config file ./config/config.yml");
    let config = config::ApplicationConfig::from_yaml(
        "./config/config.yml"
    ).unwrap();

    info!("loading database, mode = {}", config.mode);
    let mut database = match config.mode {
        config::Mode::Testing       => {
            let mut base = base::Base::new_stub();
            base.disktable_limit = config.disktable_limit;
            base.memtable_size_limit = config.memtable_size_limit;
            base
        },
        config::Mode::Production    => base::Base::new(
            config.datadirectory.as_str(),
            config.memtable_size_limit,
            config.disktable_limit
        )
    };

    database.load().unwrap();

    let h = RequestHandler{
        database: Mutex::new(database),
        config: config
    };

    info!("Listening on port {}.", h.config.port);
    Server::http(format!("0.0.0.0:{}", h.config.port)).unwrap().handle(h).unwrap();
}
