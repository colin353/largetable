/*
    query.rs

    This library parses queries from strings and creates
    query objects.
*/

use std::fmt;
use std::collections::BTreeMap as Map;

use serde_json;

use mtable;

#[derive(Debug)]
pub enum QError {
    ParseError
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Query {
    #[serde(rename = "select")]
    Select { row: String, get: Vec<String> },
    #[serde(rename = "update")]
    Update { row: String, set: Map<String, String> },
    #[serde(rename = "insert")]
    Insert { row: String, set: Map<String, String> },
}

#[derive(Serialize, Debug)]
pub enum QueryResult {
    NotImplemented,
    RowNotFound,
    RowAlreadyExists,
    InternalError,
    Done,
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
            set: set.into_iter().map(|u| (u.key, String::from_utf8(u.value).unwrap())).collect()
        }
    }

    pub fn new_insert(row: &str, set: Vec<mtable::MUpdate>) -> Query {
        Query::Insert{
            row: row.to_string(),
            set: set.into_iter().map(|u| (u.key, String::from_utf8(u.value).unwrap())).collect()
        }
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
        super::Query::parse(r#"{"select": { "row": "row1", "get": [ "col5" ] }}"#).unwrap();
        super::Query::parse(r#"{"update": { "row": "row1", "set": { "col5": "value" } }}"#).unwrap();
        super::Query::parse(r#"{"insert": { "row": "row1", "set": { "col5": "value" } }}"#).unwrap();
    }
}
