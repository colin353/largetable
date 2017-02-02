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
    ParseError,
    NotAllowed,
}

#[derive(Serialize, Deserialize, Debug)]
enum Query {
    #[serde(rename = "select")]
    Select { row: String, get: Vec<String> },
    #[serde(rename = "update")]
    Update { row: String, set: Map<String, Vec<u8>> },
    #[serde(rename = "insert")]
    Insert { row: String, set: Map<String, Vec<u8>> },
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

    // This function parses an arbitrary string and returns
    // a query or an error.
    pub fn parse(input: &str) -> Result<Query, QError> {
        serde_json::from_str(input).map_err(|e| QError::ParseError)
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
            "SELECT test, column2, col3 WHERE KEY = row1"
        )
    }

    #[test]
    fn can_print_update() {
        let q = super::Query::new_update(
            "row1",
            vec![mtable::MUpdate::new("test", vec![1, 2])]
        );

        assert_eq!(
            format!("{}", q),
            "UPDATE test = [1, 2] WHERE KEY = row1"
        );

        assert_eq!(
            q.as_json().unwrap(),
            "{action: {select}}"
        )
    }

    #[test]
    fn can_parse_query() {
        super::Query::parse("select: {test, west} where key = fest").unwrap();
    }
}
