#[macro_use]
extern crate serde_derive;
extern crate serde_json;
#[cfg(test)]
extern crate serde_yaml;
extern crate protobuf;
extern crate linefeed;
extern crate glob;
extern crate regex;
extern crate byteorder;
extern crate time;
extern crate rand;
extern crate hyper;

pub mod query;
mod generated;

pub struct LargeClient {
    hostname: hyper::Url
}

#[derive(Debug)]
pub enum ClientError {
    ConfigurationError
}

impl LargeClient {
    pub fn new(hostname: &str) -> Result<LargeClient, ClientError> {
        Ok(LargeClient{
            hostname: hyper::Url::parse(format!("http://{}",hostname).as_str())
                .map_err(|_| ClientError::ConfigurationError)?
        })
    }

    pub fn query(&self, q: query::Query) -> query::QueryResult {
        let req = match hyper::client::request::Request::new(
            hyper::method::Method::Post,
            self.hostname.to_owned()
        ) {
            Ok(r) => r,
            Err(e) => {
                println!("failed to create request: {} (hostname={})", e, self.hostname.clone());
                return query::QueryResult::NetworkError
            }
        };

        let mut w = match req.start() {
            Ok(writer)  => writer,
            Err(_)      => {
                println!("failed to connect to host");
                return query::QueryResult::NetworkError
            }
        };

        if q.write_to_writer(&mut w).is_err() {
            println!("failed to write message to host.");
            return query::QueryResult::NetworkError;
        }

        let mut read = match w.send() {
            Ok(r)   => r,
            Err(_)  => return query::QueryResult::NetworkError
        };

        match protobuf::parse_from_reader::<generated::query::QueryResult>(&mut read) {
            Ok(result) => query::QueryResult::from_generated(result),
            Err(_) => query::QueryResult::InternalError
        }
    }
}
