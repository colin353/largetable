/*
    query.rs

    This library parses queries from strings and creates
    query objects.
*/

use std::fmt;
use std::io;
use std::collections::HashMap as Map;

use serde_json;
use protobuf;
use protobuf::Message;

use mtable;
use generated;

#[derive(Debug)]
pub enum QError {
    ParseError
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Query {
    #[serde(rename = "select")]
    Select { row: String, get: Vec<String> },
    #[serde(rename = "update")]
    Update { row: String, set: Map<String, Vec<u8>> },
    #[serde(rename = "insert")]
    Insert { row: String, set: Map<String, Vec<u8>> },
}

#[derive(Serialize, Debug)]
pub enum QueryResult {
    NotImplemented,
    RowNotFound,
    RowAlreadyExists,
    InternalError,
    Done,
    PartialCommit,
    Data{ columns: Vec<Option<Vec<u8>>> }
}

impl Query {
    pub fn new_select(row: &str, get: &[&str]) -> Query {
        Query::Select{
            row: row.to_string(),
            get: get.iter().map(|s| s.to_string()).collect()
        }
    }

    pub fn new_update(row: &str, set: Vec<mtable::MUpdate>) -> Query {
        Query::Update{
            row: row.to_string(),
            set: set.into_iter().map(|u| (u.key, u.value)).collect()
        }
    }

    pub fn new_insert(row: &str, set: Vec<mtable::MUpdate>) -> Query {
        Query::Insert{
            row: row.to_string(),
            set: set.into_iter().map(|u| (u.key, u.value)).collect()
        }
    }

    // Create a query from a protobuf query.
    pub fn from_bytes(mut reader: &mut io::Read) -> Result<Query, QError> {
        let mut q = protobuf::parse_from_reader::<generated::query::Query>(&mut reader).map_err(|_| QError::ParseError)?;
        match q.get_field_type() {
            generated::query::QueryType::SELECT => Ok(Query::Select{
                row: q.take_row(),
                get: q.take_columns().into_vec()
            }),
            _ => Err(QError::ParseError)
        }
    }

    // Turn the query into a protobuf, and then write it to a writer.
    pub fn write_to_writer(self, mut writer: &mut io::Write) -> Result<(), QError> {
        let mut q = generated::query::Query::new();
        match self {
            Query::Select{row: r, get: g} => {
                q.set_field_type(generated::query::QueryType::SELECT);
                q.set_row(r);
                q.set_columns(protobuf::RepeatedField::from_vec(g))
            },
            Query::Insert{row: r, set: s} => {
                q.set_field_type(generated::query::QueryType::INSERT);
                q.set_row(r);
                q.set_values(s);
            },
            Query::Update{row: r, set: s} => {
                q.set_field_type(generated::query::QueryType::UPDATE);
                q.set_row(r);
                q.set_values(s);
            }
        };
        q.write_to_writer(writer).map_err(|_| QError::ParseError)
    }

    // This function parses an arbitrary string and returns
    // a query or an error.
    pub fn parse(input: &str) -> Result<Query, QError> {
        serde_json::from_str(input).map_err(|_| QError::ParseError)
    }

    // Return the query as a JSON object.
    pub fn as_json(&self) -> Result<String, QError> {
        serde_json::to_string(&self).map_err(|_| QError::ParseError)
    }
}

impl fmt::Display for Query {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.as_json() {
            Ok(s)   => write!(f, "{}", s),
            Err(_)  => write!(f, "<Unable to parse query>")
        }
    }
}

impl fmt::Display for QueryResult {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            QueryResult::Done             => write!(f, "OK."),
            QueryResult::RowNotFound      => write!(f, "Row not found."),
            QueryResult::RowAlreadyExists => write!(f, "Row already exists."),
            QueryResult::InternalError    => write!(f, "Internal error."),
            QueryResult::NotImplemented   => write!(f, "Not implemented."),
            QueryResult::PartialCommit    => write!(f, "Partial commit (!)"),
            QueryResult::Data{columns: ref c} => {
                write!(f, "Data: [{}]", c.iter().map(|s| match *s {
                    Some(ref x) => {
                        format!(
                            "\"{}\"",
                            String::from_utf8(x.clone())
                            .unwrap_or(String::from("Err"))
                        )
                    },
                    None        => String::from("None")
                }).collect::<Vec<_>>().join(", "))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use mtable;

    #[test]
    fn can_print_select() {
        let q = super::Query::new_select(
            "row1",
            &["test", "column2", "col3"]
        );

        assert_eq!(
            format!("{}", q),
            r#"{"select":{"row":"row1","get":["test","column2","col3"]}}"#
        )
    }

    #[test]
    fn can_display_queryresults() {
        assert_eq!(
            format!("{}", super::QueryResult::NotImplemented),
            "Not implemented."
        );

        assert_eq!(
            format!("{}", super::QueryResult::RowAlreadyExists),
            "Row already exists."
        );

        assert_eq!(
            format!("{}", super::QueryResult::InternalError),
            "Internal error."
        );

        assert_eq!(
            format!("{}", super::QueryResult::PartialCommit),
            "Partial commit (!)"
        );
    }

    #[test]
    fn can_print_update() {
        let q = super::Query::new_update(
            "row1",
            vec![mtable::MUpdate::new("test", vec![120, 121])]
        );

        assert_eq!(
            format!("{}", q),
            r#"{"update":{"row":"row1","set":{"test":"xy"}}}"#
        );
    }

    #[test]
    fn can_print_insert() {
        let q = super::Query::new_insert(
            "row1",
            vec![mtable::MUpdate::new("test", vec![120, 121])]
        );

        assert_eq!(
            format!("{}", q),
            r#"{"insert":{"row":"row1","set":{"test":"xy"}}}"#
        );
    }

    #[test]
    fn can_parse_queries() {
        super::Query::parse(r#"{"select": { "row": "test 1 2 3", "get": [] }}"#).unwrap();
        super::Query::parse(r#"{"select": { "row": "row1", "get": [ "col5" ] }}"#).unwrap();
        super::Query::parse(r#"{"update": { "row": "row1", "set": {} }}"#).unwrap();
        super::Query::parse(r#"{"update": { "row": "row1", "set": { "col5": "value" } }}"#).unwrap();
        super::Query::parse(r#"{"insert": { "row": "row1", "set": { "col5": "value", "col7": "value" } }}"#).unwrap();
    }
}
