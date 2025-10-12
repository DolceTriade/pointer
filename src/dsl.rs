use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Filter {
    Content(String),
    Repo(String),
    File(String),
    Lang(String),
    Branch(String),
    Symbol(String),
    Regex(String),
    Archived(bool),
    Fork(bool),
    Public(bool),
    CaseSensitive(CaseSensitivity),
    Type(ResultType),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum CaseSensitivity {
    Yes,
    No,
    Auto,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ResultType {
    FileMatch,
    FileName,
    File,
    Repo,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum QueryNode {
    Filter(Filter),
    Term(String),
    Not(Box<QueryNode>),
    And(Vec<QueryNode>),
    Or(Vec<QueryNode>),
    Group(Box<QueryNode>),
}

impl fmt::Display for Filter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Filter::Content(s) => write!(f, "content:\"{}\"", s),
            Filter::Repo(s) => write!(f, "repo:\"{}\"", s),
            Filter::File(s) => write!(f, "file:\"{}\"", s),
            Filter::Lang(s) => write!(f, "lang:\"{}\"", s),
            Filter::Branch(s) => write!(f, "branch:\"{}\"", s),
            Filter::Symbol(s) => write!(f, "sym:\"{}\"", s),
            Filter::Regex(s) => write!(f, "regex:\"{}\"", s),
            Filter::Archived(b) => write!(f, "archived:{}", if *b { "yes" } else { "no" }),
            Filter::Fork(b) => write!(f, "fork:{}", if *b { "yes" } else { "no" }),
            Filter::Public(b) => write!(f, "public:{}", if *b { "yes" } else { "no" }),
            Filter::CaseSensitive(cs) => {
                match cs {
                    CaseSensitivity::Yes => write!(f, "case:yes"),
                    CaseSensitivity::No => write!(f, "case:no"),
                    CaseSensitivity::Auto => write!(f, "case:auto"),
                }
            }
            Filter::Type(rt) => {
                match rt {
                    ResultType::FileMatch => write!(f, "type:filematch"),
                    ResultType::FileName => write!(f, "type:filename"),
                    ResultType::File => write!(f, "type:file"),
                    ResultType::Repo => write!(f, "type:repo"),
                }
            }
        }
    }
}

impl fmt::Display for QueryNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryNode::Filter(filter) => write!(f, "{}", filter),
            QueryNode::Term(term) => write!(f, "{}", term),
            QueryNode::Not(node) => write!(f, "-({})", node),
            QueryNode::And(nodes) => {
                write!(f, "(")?;
                for (i, node) in nodes.iter().enumerate() {
                    if i > 0 {
                        write!(f, " AND ")?;
                    }
                    write!(f, "{}", node)?;
                }
                write!(f, ")")
            }
            QueryNode::Or(nodes) => {
                write!(f, "(")?;
                for (i, node) in nodes.iter().enumerate() {
                    if i > 0 {
                        write!(f, " OR ")?;
                    }
                    write!(f, "{}", node)?;
                }
                write!(f, ")")
            }
            QueryNode::Group(node) => write!(f, "({})", node),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum ParseError {
    InvalidFilter(String),
    UnmatchedParenthesis,
    EmptyQuery,
}

// A simple recursive descent parser for the Zoekt query language
pub struct QueryParser {
    tokens: Vec<String>,
    pos: usize,
}

impl QueryParser {
    pub fn new(query_str: &str) -> Self {
        let tokens = tokenize_query(query_str);
        QueryParser { tokens, pos: 0 }
    }

    fn peek(&self) -> Option<&str> {
        self.tokens.get(self.pos).map(|s| s.as_str())
    }

    fn consume(&mut self) -> Option<String> {
        if self.pos < self.tokens.len() {
            let token = self.tokens[self.pos].clone();
            self.pos += 1;
            Some(token)
        } else {
            None
        }
    }



    fn parse_filter(&mut self, filter_type: &str, value: String) -> Result<Filter, ParseError> {
        match filter_type {
            "content" => Ok(Filter::Content(value)),
            "c" => Ok(Filter::Content(value)),  // alias for content
            "repo" | "r" => Ok(Filter::Repo(value)),
            "file" => Ok(Filter::File(value)),
            "f" => Ok(Filter::File(value)),  // alias for file
            "lang" | "l" => Ok(Filter::Lang(value)),
            "branch" | "b" => Ok(Filter::Branch(value)),
            "sym" => Ok(Filter::Symbol(value)),
            "regex" => Ok(Filter::Regex(value)),
            "archived" | "a" => match value.as_str() {
                "yes" => Ok(Filter::Archived(true)),
                "no" => Ok(Filter::Archived(false)),
                _ => Err(ParseError::InvalidFilter(format!(
                    "archived must be yes or no, got {}",
                    value
                ))),
            },
            "fork" => match value.as_str() {
                "yes" => Ok(Filter::Fork(true)),
                "no" => Ok(Filter::Fork(false)),
                _ => Err(ParseError::InvalidFilter(format!(
                    "fork must be yes or no, got {}",
                    value
                ))),
            },
            "public" => match value.as_str() {
                "yes" => Ok(Filter::Public(true)),
                "no" => Ok(Filter::Public(false)),
                _ => Err(ParseError::InvalidFilter(format!(
                    "public must be yes or no, got {}",
                    value
                ))),
            },
            "case" => match value.as_str() {
                "yes" => Ok(Filter::CaseSensitive(CaseSensitivity::Yes)),
                "no" => Ok(Filter::CaseSensitive(CaseSensitivity::No)),
                "auto" => Ok(Filter::CaseSensitive(CaseSensitivity::Auto)),
                _ => Err(ParseError::InvalidFilter(format!(
                    "case must be yes, no, or auto, got {}",
                    value
                ))),
            },
            "type" | "t" => match value.as_str() {
                "filematch" => Ok(Filter::Type(ResultType::FileMatch)),
                "filename" => Ok(Filter::Type(ResultType::FileName)),
                "file" => Ok(Filter::Type(ResultType::File)),
                "repo" => Ok(Filter::Type(ResultType::Repo)),
                _ => Err(ParseError::InvalidFilter(format!(
                    "type must be filematch, filename, file, or repo, got {}",
                    value
                ))),
            },
            _ => Err(ParseError::InvalidFilter(filter_type.to_string())),
        }
    }

    fn parse_term(&mut self) -> Result<QueryNode, ParseError> {
        if let Some(token) = self.consume() {
            if token.starts_with('-') {
                // Handle negation
                let inner_token = token[1..].to_string();
                if inner_token.starts_with('(') {
                    // -(...) case
                    let inner_expr = self.parse_group(&inner_token[1..])?;
                    Ok(QueryNode::Not(Box::new(inner_expr)))
                } else if let Some((filter_type, value)) = inner_token.split_once(':') {
                    // -filter:value case
                    let filter = self.parse_filter(filter_type, value.to_string())?;
                    Ok(QueryNode::Not(Box::new(QueryNode::Filter(filter))))
                } else {
                    // -term case
                    Ok(QueryNode::Not(Box::new(QueryNode::Term(inner_token))))
                }
            } else if token.starts_with('(') {
                // Handle group
                self.parse_group(&token[1..])
            } else if let Some((filter_type, value)) = token.split_once(':') {
                // Handle filter
                let filter = self.parse_filter(filter_type, value.to_string())?;
                Ok(QueryNode::Filter(filter))
            } else {
                // Regular term
                Ok(QueryNode::Term(token))
            }
        } else {
            Err(ParseError::EmptyQuery)
        }
    }

    fn parse_group(&mut self, initial_content: &str) -> Result<QueryNode, ParseError> {
        // This is a simplified approach - in a real implementation we'd need more sophisticated parsing
        let mut group_content = initial_content.to_string();

        // If the initial content doesn't end with ')', we need to collect more tokens
        if !initial_content.contains(')') {
            // Look for the matching parenthesis
            let mut paren_count = 1; // We already have one '(' from initial_content
            while let Some(token) = self.consume() {
                if token.contains('(') {
                    paren_count += token.matches('(').count();
                }
                if token.contains(')') {
                    paren_count -= token.matches(')').count();
                    if paren_count == 0 {
                        // Found the matching parenthesis
                        group_content.push_str(&format!(" {}", token));
                        break;
                    }
                }
                group_content.push_str(&format!(" {}", token));
            }
        }

        // Extract content between parentheses
        let end_paren_pos = group_content.find(')').unwrap_or(group_content.len());
        let inner_content = &group_content[..end_paren_pos];

        // Parse the inner content (simplified - in a real implementation this would be recursive)
        let inner_query = parse_query(inner_content)?;

        // Handle OR operator inside the group if present
        if inner_content.contains(" or ") {
            let parts: Vec<&str> = inner_content.split(" or ").collect();
            let mut or_nodes = Vec::new();
            for part in parts {
                let part_query = parse_query(part.trim())?;
                or_nodes.push(QueryNode::Group(Box::new(part_query)));
            }
            Ok(QueryNode::Or(or_nodes))
        } else {
            Ok(QueryNode::Group(Box::new(inner_query)))
        }
    }

    fn parse_expression(&mut self) -> Result<QueryNode, ParseError> {
        let mut nodes = Vec::new();

        while let Some(peeked) = self.peek() {
            if peeked == ")" {
                break;
            }

            let term = self.parse_term()?;
            nodes.push(term);

            // Check for OR operator
            if let Some(next) = self.peek() {
                if next == "or" || next == "OR" {
                    self.consume(); // consume "or"
                    // For simplicity in this implementation, we'll handle OR at a higher level
                    break;
                }
            }
        }

        if nodes.len() == 1 {
            Ok(nodes.into_iter().next().unwrap())
        } else {
            Ok(QueryNode::And(nodes))
        }
    }

    pub fn parse(mut self) -> Result<QueryNode, ParseError> {
        let mut expressions = Vec::new();

        while self.pos < self.tokens.len() {
            let expr = self.parse_expression()?;
            expressions.push(expr);

            // Check for OR operator between expressions
            if let Some(token) = self.peek() {
                if token == "or" || token == "OR" {
                    self.consume(); // consume "or"
                    continue; // continue to parse more expressions for OR
                }
            }
        }

        if expressions.is_empty() {
            return Err(ParseError::EmptyQuery);
        } else if expressions.len() == 1 {
            Ok(expressions.into_iter().next().unwrap())
        } else {
            Ok(QueryNode::And(expressions))
        }
    }
}

// Simple tokenizer that handles quoted strings and basic tokens
fn tokenize_query(query: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut chars = query.chars().peekable();
    let mut current_token = String::new();
    let mut in_quotes = false;
    let mut quote_char = '"';

    while let Some(ch) = chars.next() {
        match ch {
            '"' | '\'' => {
                if !in_quotes {
                    in_quotes = true;
                    quote_char = ch;
                } else if ch == quote_char {
                    in_quotes = false;
                    if !current_token.is_empty() {
                        tokens.push(current_token.clone());
                        current_token.clear();
                    }
                } else {
                    current_token.push(ch);
                }
            }
            ':' if !in_quotes => {
                current_token.push(ch);
                if let Some(&next_ch) = chars.peek() {
                    if next_ch != '"' && next_ch != '\'' {
                        // This is a filter, so collect the value
                        while let Some(&next_ch) = chars.peek() {
                            if next_ch.is_whitespace() {
                                break;
                            }
                            current_token.push(chars.next().unwrap());
                        }
                        tokens.push(current_token.clone());
                        current_token.clear();
                    }
                }
            }
            ' ' | '\t' | '\n' | '\r' if !in_quotes => {
                if !current_token.is_empty() {
                    tokens.push(current_token.clone());
                    current_token.clear();
                }
            }
            '(' | ')' if !in_quotes => {
                if !current_token.is_empty() {
                    tokens.push(current_token.clone());
                    current_token.clear();
                }
                tokens.push(ch.to_string());
            }
            _ => {
                current_token.push(ch);
            }
        }
    }

    if !current_token.is_empty() {
        tokens.push(current_token);
    }

    // Handle 'or' operator as separate token
    let mut final_tokens = Vec::new();
    for token in tokens {
        if token == "or" || token == "OR" {
            final_tokens.push("or".to_string());
        } else {
            final_tokens.push(token);
        }
    }

    final_tokens
}

pub fn parse_query(query_str: &str) -> Result<QueryNode, ParseError> {
    let parser = QueryParser::new(query_str);
    parser.parse()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_content() {
        let result = parse_query("hello world");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_repo_filter() {
        let result = parse_query("repo:myrepo");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_content_filter() {
        let result = parse_query("content:\"hello world\"");
        assert!(result.is_ok());
    }

    #[test]
    fn test_tokenize_quotes() {
        let tokens = tokenize_query("content:\"hello world\" repo:myrepo");
        assert_eq!(tokens, vec!["content:\"hello world\"", "repo:myrepo"]);
    }
}
