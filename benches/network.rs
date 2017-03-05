/*
    network.rs

    This code tries to connect to a docker instance
    and checks the time required to perform queries.
*/

#![feature(test)]
extern crate test;
extern crate largeclient;
extern crate rand;

// This function generates a 25 character long ASCII-printable string.
fn random_string() -> String {
    (0..25).map(|_| (0x20u8 + (rand::random::<f32>() * 96.0) as u8) as char).collect()
}

// This function generates 25 random bytes of data to write to the
// database.
fn random_bytes() -> Vec<u8> {
    (0..25).map(|_| rand::random::<u8>()).collect::<Vec<_>>()
}

#[bench]
fn insert_network(b: &mut test::Bencher) {
    let hostname = option_env!("LARGETABLE_DOCKER_SERVICE").unwrap_or("localhost:8080");
    let client = largeclient::LargeClient::new(hostname).unwrap();

    b.iter(|| {
        client.query(largeclient::query::Query::new_insert(
            random_string().as_str(),
            vec![largeclient::query::MUpdate::new(
                random_string().as_str(),
                random_bytes()
            )]
        ))
    });
}

#[bench]
fn select_network(b: &mut test::Bencher) {
    let hostname = option_env!("LARGETABLE_DOCKER_SERVICE").unwrap_or("localhost:8080");
    let client = largeclient::LargeClient::new(hostname).unwrap();

    b.iter(|| {
        client.query(largeclient::query::Query::new_select(
            random_string().as_str(),
            &[random_string().as_str()]
        ))
    })
}
