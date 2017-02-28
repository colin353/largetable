/*
    docker_test_client.rs

    This integration test assumes that a largetable service
    is running on the default port (localhost/8080), and tries
    to communicate with it via the client library.
*/

extern crate largeclient;

#[test]
fn connection_should_fail() {
    let client = largeclient::LargeClient::new("fake_domain:9999").unwrap();
    match client.query(largeclient::query::Query::parse(r#"{
            "select": { "row": "fake", "get": []}
        }"#).unwrap())
    {
        largeclient::query::QueryResult::NetworkError => (),
        _ => panic!("Expected to get NetworkError, but didn't.")
    }
}

#[test]
fn panics_invalid_connection_string() {
    assert!(largeclient::LargeClient::new("$!@#$").is_err());
    assert!(largeclient::LargeClient::new("localhost:test").is_err());
}

#[test]
fn can_connect_to_server() {
    let hostname = option_env!("LARGETABLE_DOCKER_SERVICE").unwrap_or("localhost:8080");
    println!("Trying to connect on hostname: {}", hostname);
    let client = largeclient::LargeClient::new(hostname).unwrap();
    match client.query(largeclient::query::Query::parse(r#"{
            "select": { "row": "fake", "get": []}
        }"#).unwrap())
    {
        largeclient::query::QueryResult::Data{columns: c} => assert_eq!(c.len(), 0),
        largeclient::query::QueryResult::RowNotFound => (),
        e => panic!("Wrong response: {}. Probably the server isn't running?", e)
    };

    match client.query(largeclient::query::Query::parse(r#"{
            "insert": { "row": "fake", "set": {"field": "value"}}
        }"#).unwrap())
    {
        largeclient::query::QueryResult::Done => (),
        e => panic!("Query didn't return expected result: {}", e)
    };
}
