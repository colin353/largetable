mod dtable;
mod mtable;
mod query;

extern crate protobuf;

#[macro_use]
extern crate serde_derive;
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
mod generated;

struct RequestHandler {
    database: Mutex<base::Base>
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
                        res.start().unwrap().write("invalid data".as_bytes()).unwrap();
                    }
                };
            },
            _ => *res.status_mut() = StatusCode::MethodNotAllowed
        }
    }
}

fn main() {
    let mut database = base::Base::new("./data/");
    database.load().unwrap();

    let h = RequestHandler{
        database: Mutex::new(database)
    };


    println!("largetable v{}", env!("CARGO_PKG_VERSION"));

    Server::http("0.0.0.0:8080").unwrap().handle(h).unwrap();
}
