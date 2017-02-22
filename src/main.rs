mod dtable;
mod mtable;
mod query;

extern crate protobuf;

#[macro_use]
extern crate serde_derive;
extern crate serde_json;

extern crate hyper;
use hyper::server::{Server, Request, Response};
use hyper::status::StatusCode;

use std::io;
use std::io::Write;

mod generated;

fn hello(mut req: Request, mut res: Response) {
    match req.method {
        hyper::Get => {
            res.start().unwrap().write("hello, world".as_bytes());
        },
        _ => *res.status_mut() = StatusCode::MethodNotAllowed
    }
}

fn main() {
    Server::http("0.0.0.0:8080").unwrap().handle(hello).unwrap();
}
