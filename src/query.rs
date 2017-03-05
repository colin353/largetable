/*
    query.rs

    This library parses queries from strings and creates
    query objects.
*/

use std::fmt;
use std::io;
use std::collections::HashMap as Map;
use std::iter::FromIterator;

use serde_json;
use protobuf;
use protobuf::Message;

use generated;

#[derive(Debug)]
pub enum QError {
    ParseError
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MUpdate {
    pub value: Vec<u8>,
    pub key: String
}

impl MUpdate {
    pub fn new(key: &str, value: Vec<u8>) -> MUpdate {
        MUpdate{
            key: key.to_string(),
            value: value
        }
    }

    // Calculate the size of an update.
    pub fn size(&self) -> usize {
        self.value.len() + self.key.len()
    }
}

// In order to support JSON parsing of queries, this struct is created
// which has Strings instead of Vec<u8> in the value of the HashMap.
// In order to be applied to the database, these QueryStrings must be
// converted into regular Queries using .into_query().
#[derive(Serialize, Deserialize, Debug)]
pub enum QueryString {
    #[serde(rename = "select")]
    Select { row: String, get: Vec<String> },
    #[serde(rename = "update")]
    Update { row: String, set: Map<String, String> },
    #[serde(rename = "insert")]
    Insert { row: String, set: Map<String, String> },
}

impl QueryString {
    fn into_query(self) -> Query {
        fn convert_map(input: Map<String, String>) -> Map<String, Vec<u8>> {
            Map::from_iter(
                input.into_iter().map(|(k, v)| (k, v.into_bytes()))
            )
        }
        match self {
            QueryString::Select{row: r, get: g} => Query::Select{row: r, get: g},
            QueryString::Update{row: r, set: s} => Query::Update{row: r, set: convert_map(s)},
            QueryString::Insert{row: r, set: s} => Query::Insert{row: r, set: convert_map(s)}
        }
    }
}

pub enum Query {
    Select { row: String, get: Vec<String> },
    Update { row: String, set: Map<String, Vec<u8>> },
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
    NetworkError,
    Data{ columns: Vec<Option<Vec<u8>>> }
}

impl Query {
    pub fn new_select(row: &str, get: &[&str]) -> Query {
        Query::Select{
            row: row.to_string(),
            get: get.iter().map(|s| s.to_string()).collect()
        }
    }

    pub fn as_query_string(&self) -> QueryString {
        fn convert_map(input: &Map<String, Vec<u8>>) -> Map<String, String> {
            Map::from_iter(
                input.iter().map(|(k, v)| (k.clone(), String::from_utf8(v.to_vec()).unwrap()))
            )
        }

        match *self {
            Query::Select{row: ref r, get: ref g} => QueryString::Select{row: r.clone(), get: g.clone()},
            Query::Update{row: ref r, set: ref s} => QueryString::Update{row: r.clone(), set: convert_map(s)},
            Query::Insert{row: ref r, set: ref s} => QueryString::Insert{row: r.clone(), set: convert_map(s)}
        }
    }

    pub fn new_update(row: &str, set: Vec<MUpdate>) -> Query {
        Query::Update{
            row: row.to_string(),
            set: set.into_iter().map(|u| (u.key, u.value)).collect()
        }
    }

    pub fn new_insert(row: &str, set: Vec<MUpdate>) -> Query {
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
            generated::query::QueryType::INSERT => Ok(Query::Insert{
                row: q.take_row(),
                set: q.take_values()
            }),
            generated::query::QueryType::UPDATE => Ok(Query::Update{
                row: q.take_row(),
                set: q.take_values()
            })
        }
    }

    // Turn the query into a protobuf, and then write it to a writer.
    pub fn write_to_writer(self, mut writer: &mut io::Write) -> Result<(), QError> {
        let mut q = generated::query::Query::new();
        match self {
            Query::Select{row: r, get: g} => {
                q.set_field_type(generated::query::QueryType::SELECT);
                q.set_row(r);
                q.set_columns(protobuf::RepeatedField::from_vec(g));
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
        let qs: QueryString = serde_json::from_str(input).map_err(|_| QError::ParseError)?;
        Ok(qs.into_query())
    }

    // Return the query as a JSON object.
    pub fn as_json(&self) -> Result<String, QError> {
        serde_json::to_string(&self.as_query_string()).map_err(|_| QError::ParseError)
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

impl QueryResult {
    pub fn from_generated(mut q: generated::query::QueryResult) -> QueryResult {
        let field_type = q.get_field_type();
        match field_type {
            generated::query::QueryResultType::OK => QueryResult::Done,
            generated::query::QueryResultType::ROW_NOT_FOUND => QueryResult::RowNotFound,
            generated::query::QueryResultType::ROW_ALREADY_EXISTS => QueryResult::RowAlreadyExists,
            generated::query::QueryResultType::PARTIAL_COMMIT => QueryResult::PartialCommit,
            generated::query::QueryResultType::INTERNAL_ERROR => QueryResult::InternalError,
            generated::query::QueryResultType::NOT_IMPLEMENTED => QueryResult::NotImplemented,
            generated::query::QueryResultType::NETWORK_ERROR => QueryResult::NetworkError,
            generated::query::QueryResultType::DATA =>
                QueryResult::Data{
                    columns: q.take_columns().into_iter()
                        .map(|mut r| {
                            if r.get_has_data() {
                                Some(r.take_data())
                            } else {
                                None
                            }
                        }).collect::<Vec<_>>()
                },
        }
    }

    pub fn into_generated(self) -> generated::query::QueryResult {
        let mut output = generated::query::QueryResult::new();
        match self {
            QueryResult::Done               => output.set_field_type(generated::query::QueryResultType::OK),
            QueryResult::RowNotFound        => output.set_field_type(generated::query::QueryResultType::ROW_NOT_FOUND),
            QueryResult::RowAlreadyExists   => output.set_field_type(generated::query::QueryResultType::ROW_ALREADY_EXISTS),
            QueryResult::PartialCommit      => output.set_field_type(generated::query::QueryResultType::PARTIAL_COMMIT),
            QueryResult::NotImplemented     => output.set_field_type(generated::query::QueryResultType::NOT_IMPLEMENTED),
            QueryResult::NetworkError       => output.set_field_type(generated::query::QueryResultType::NETWORK_ERROR),
            QueryResult::InternalError      => output.set_field_type(generated::query::QueryResultType::INTERNAL_ERROR),
            QueryResult::Data{columns: c}   => {
                output.set_columns(protobuf::RepeatedField::from_iter(
                    c.into_iter()
                        .map(|c| {
                            let mut x = generated::query::ResultColumn::new();
                            x.set_has_data(c.is_some());
                            if let Some(data) = c {
                                x.set_data(data);
                            }
                            x
                        }
                )));
                output.set_field_type(generated::query::QueryResultType::DATA);
            }
        }
        output
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
            QueryResult::NetworkError     => write!(f, "Network error."),
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
    use std::collections::HashMap as Map;
    use std::iter::FromIterator;
    use protobuf;
    use protobuf::Message;
    use generated;

    use test;

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

    // This function takes a query, converts it back and forth to a
    // protobuf-compatible query, and checks that it is still the same.
    fn query_conversion_is_valid(q: super::Query) {
        let repr = format!("{}", q);
        let mut bytes = vec![];
        q.write_to_writer(&mut bytes).unwrap();
        let recovered = super::Query::from_bytes(&mut bytes.as_slice()).unwrap();
        assert_eq!(
            repr,
            format!("{}", recovered)
        );
        let mut bytes2 = vec![];
        recovered.write_to_writer(&mut bytes2).unwrap();
        assert_eq!(bytes, bytes2);
    }

    fn queryresult_conversion_is_valid(q: super::QueryResult) {
        let repr = format!("{}", q);
        let mut bytes = vec![];
        q.into_generated().write_to_writer(&mut bytes).unwrap();
        let converted_generated = protobuf::parse_from_bytes::<generated::query::QueryResult>(&mut bytes.as_slice()).unwrap();
        let recovered = super::QueryResult::from_generated(converted_generated);
        assert_eq!(
            repr,
            format!("{}", recovered)
        );
        let mut bytes2 = vec![];
        recovered.into_generated().write_to_writer(&mut bytes2).unwrap();
        assert_eq!(bytes, bytes2);
    }

    #[test]
    fn can_convert_queryresult_to_bytes() {
        queryresult_conversion_is_valid(super::QueryResult::Done);
        queryresult_conversion_is_valid(super::QueryResult::RowNotFound);
        queryresult_conversion_is_valid(super::QueryResult::RowAlreadyExists);
        queryresult_conversion_is_valid(super::QueryResult::NetworkError);
        queryresult_conversion_is_valid(super::QueryResult::InternalError);
        queryresult_conversion_is_valid(super::QueryResult::NotImplemented);
        queryresult_conversion_is_valid(super::QueryResult::PartialCommit);
        queryresult_conversion_is_valid(super::QueryResult::Data{columns: vec![Some(String::from("this is a test").into_bytes())]});
        queryresult_conversion_is_valid(super::QueryResult::Data{columns: vec![None]});
    }

    #[test]
    fn can_convert_query_to_bytes() {
        query_conversion_is_valid(super::Query::Insert{row: String::from("test"), set: Map::new()});

        let data = vec![
            ("c@#$%^&*()".to_string(),  String::from("caDS{").into_bytes())
        ];
        let set = Map::<String, Vec<u8>>::from_iter(data);
        query_conversion_is_valid(super::Query::Insert{row: String::from("QW_#F)A"), set: set.clone()});
        query_conversion_is_valid(super::Query::Update{row: String::from("!@)#!!D"), set: set.clone()});
        query_conversion_is_valid(super::Query::Select{row: String::from("!@)#!!D"), get: vec![String::from("abcdef")]});
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
            vec![super::MUpdate::new("test", vec![120, 121])]
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
            vec![super::MUpdate::new("test", vec![120, 121])]
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

    #[bench]
    fn query_parsing(b: &mut test::Bencher) {
        b.iter(|| {
            super::Query::parse(r#"{"select": { "row": "test 1 2 3", "get": [] }}"#).unwrap();
            super::Query::parse(r#"{"select": { "row": "row1", "get": [ "col5" ] }}"#).unwrap();
            super::Query::parse(r#"{"update": { "row": "row1", "set": {} }}"#).unwrap();
            super::Query::parse(r#"{"update": { "row": "row1", "set": { "col5": "value" } }}"#).unwrap();
            super::Query::parse(r#"{"insert": { "row": "row1", "set": { "col5": "value", "col7": "value" } }}"#).unwrap();
        })
    }
}
