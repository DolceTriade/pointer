use thiserror::Error;

#[derive(Debug, PartialEq)]
pub enum Filter {
    Language(String),
    Repository(String),
    Path(String),
}

#[derive(Debug, PartialEq)]
pub struct Query {
    pub filters: Vec<Filter>,
    pub terms: Vec<String>,
}

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("invalid filter: {0}")]
    InvalidFilter(String),
}

pub fn parse_query(query_str: &str) -> Result<Query, ParseError> {
    let mut filters = Vec::new();
    let mut terms = Vec::new();

    for part in query_str.split_whitespace() {
        if let Some((key, value)) = part.split_once(':') {
            match key {
                "lang" => filters.push(Filter::Language(value.to_string())),
                "repo" => filters.push(Filter::Repository(value.to_string())),
                "path" => filters.push(Filter::Path(value.to_string())),
                _ => return Err(ParseError::InvalidFilter(key.to_string())),
            }
        } else {
            terms.push(part.to_string());
        }
    }

    Ok(Query { filters, terms })
}
