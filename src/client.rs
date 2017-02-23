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

use protobuf::Message;

mod query;
mod generated;

struct LargeClient {
    hostname: hyper::Url
}

enum ClientError {
    ConfigurationError
}

impl LargeClient {
    fn new(hostname: &str) -> Result<LargeClient, ClientError> {
        Ok(LargeClient{
            hostname: hyper::Url::parse(hostname)
                .map_err(|_| ClientError::ConfigurationError)?
        })
    }

    fn query(&self, q: query::Query) -> query::QueryResult {
        let req = match hyper::client::request::Request::new(
            hyper::method::Method::Post,
            self.hostname.clone()
        ) {
            Ok(r) => r,
            Err(_) => return query::QueryResult::NetworkError
        };

        let mut w = match req.start() {
            Ok(writer)  => writer,
            Err(_)      => return query::QueryResult::NetworkError
        };

        if q.write_to_writer(&mut w).is_err() {
            return query::QueryResult::NetworkError;
        }

        let mut read = match w.send() {
            Ok(r)   => r,
            Err(_)  => return query::QueryResult::NetworkError
        };

        match protobuf::parse_from_reader::<generated::query::QueryResult>(&mut read) {
            Ok(result) => query::QueryResult::Done,
            Err(_) => query::QueryResult::InternalError
        }
    }
}
