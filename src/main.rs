mod dtable;
mod mtable;
mod query;

extern crate protobuf;

#[macro_use]
extern crate nickel;

use nickel::{Nickel, HttpRouter, Request, Response, MiddlewareResult};

#[macro_use]
extern crate serde_derive;
extern crate serde_json;

mod generated;

// Read a query request in JSON format.
fn handle_json_query<'mw>(req: &mut Request, res: Response<'mw>) -> MiddlewareResult<'mw> {
    let query: Result<query::Query, serde_json::Error> = serde_json::from_reader(&mut req.origin);
    match query {
        Ok(q)   => res.send(format!("{}", &q)),
        Err(_)  => res.send("Parsing error.")
    }
}

fn main() {
    let mut server = Nickel::new();
    server.post("/json", handle_json_query);
    server.listen("127.0.0.1:6767").unwrap();
}
