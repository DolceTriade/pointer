use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Filter {
    Content(String),
    Repo(String),
    File(String),
    Lang(String),
    Branch(String),
    Regex(String),
    CaseSensitive(CaseSensitivity),
    Type(ResultType),
    Historical(bool),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
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
            Filter::Regex(s) => write!(f, "regex:\"{}\"", s),
            Filter::CaseSensitive(cs) => match cs {
                CaseSensitivity::Yes => write!(f, "case:yes"),
                CaseSensitivity::No => write!(f, "case:no"),
                CaseSensitivity::Auto => write!(f, "case:auto"),
            },
            Filter::Type(rt) => match rt {
                ResultType::FileMatch => write!(f, "type:filematch"),
                ResultType::FileName => write!(f, "type:filename"),
                ResultType::File => write!(f, "type:file"),
                ResultType::Repo => write!(f, "type:repo"),
            },
            Filter::Historical(flag) => {
                if *flag {
                    write!(f, "historical:yes")
                } else {
                    write!(f, "historical:no")
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
#[derive(Debug, Clone)]
struct Token {
    value: String,
    first_colon_in_quotes: bool,
}

impl Token {
    fn new(value: String, first_colon_in_quotes: bool) -> Self {
        Self {
            value,
            first_colon_in_quotes,
        }
    }
}

pub struct QueryParser {
    tokens: Vec<Token>,
    pos: usize,
}

impl QueryParser {
    pub fn new(query_str: &str) -> Self {
        let tokens = tokenize_query(query_str);
        QueryParser { tokens, pos: 0 }
    }

    fn peek(&self) -> Option<&str> {
        self.tokens.get(self.pos).map(|token| token.value.as_str())
    }

    fn consume(&mut self) -> Option<Token> {
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
            "c" => Ok(Filter::Content(value)), // alias for content
            "repo" | "r" => Ok(Filter::Repo(value)),
            "file" => Ok(Filter::File(value.clone())),
            "f" => Ok(Filter::File(value.clone())), // alias for file
            "path" => Ok(Filter::File(value)),
            "lang" | "l" => Ok(Filter::Lang(value)),
            "branch" | "b" => Ok(Filter::Branch(value)),
            "regex" => Ok(Filter::Regex(preprocess_regex_pattern(&value)?)),
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
            "historical" => match value.to_ascii_lowercase().as_str() {
                "yes" | "true" | "1" => Ok(Filter::Historical(true)),
                "no" | "false" | "0" => Ok(Filter::Historical(false)),
                _ => Err(ParseError::InvalidFilter(format!(
                    "historical must be yes or no, got {}",
                    value
                ))),
            },
            _ => Err(ParseError::InvalidFilter(filter_type.to_string())),
        }
    }

    fn parse_term(&mut self) -> Result<QueryNode, ParseError> {
        if let Some(token) = self.consume() {
            let token_value = token.value;
            if token_value.starts_with('-') {
                // Handle negation
                let inner_token = token_value[1..].to_string();
                if inner_token.starts_with('(') {
                    // -(...) case
                    let inner_expr = self.parse_group(&inner_token[1..])?;
                    Ok(QueryNode::Not(Box::new(inner_expr)))
                } else if !token.first_colon_in_quotes {
                    if let Some((filter_type, value)) = inner_token.split_once(':') {
                        // -filter:value case
                        let filter = self.parse_filter(filter_type, value.to_string())?;
                        Ok(QueryNode::Not(Box::new(QueryNode::Filter(filter))))
                    } else {
                        // -term case
                        Ok(QueryNode::Not(Box::new(QueryNode::Term(inner_token))))
                    }
                } else {
                    // -term case
                    Ok(QueryNode::Not(Box::new(QueryNode::Term(inner_token))))
                }
            } else if token_value.starts_with('(') {
                // Handle group
                self.parse_group(&token_value[1..])
            } else if !token.first_colon_in_quotes {
                if let Some((filter_type, value)) = token_value.split_once(':') {
                    // Handle filter
                    let filter = self.parse_filter(filter_type, value.to_string())?;
                    Ok(QueryNode::Filter(filter))
                } else {
                    // Regular term
                    Ok(QueryNode::Term(token_value))
                }
            } else {
                // Regular term
                Ok(QueryNode::Term(token_value))
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
                let token_value = token.value;
                if token_value.contains('(') {
                    paren_count += token_value.matches('(').count();
                }
                if token_value.contains(')') {
                    paren_count -= token_value.matches(')').count();
                    if paren_count == 0 {
                        // Found the matching parenthesis
                        group_content.push_str(&format!(" {}", token_value));
                        break;
                    }
                }
                group_content.push_str(&format!(" {}", token_value));
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

fn preprocess_regex_pattern(raw: &str) -> Result<String, ParseError> {
    let mut decoded = String::with_capacity(raw.len());
    let mut chars = raw.chars();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            match chars.next() {
                Some('n') => decoded.push('\n'),
                Some('r') => decoded.push('\r'),
                Some('t') => decoded.push('\t'),
                Some('\\') => decoded.push('\\'),
                Some(other) => {
                    decoded.push('\\');
                    decoded.push(other);
                }
                None => {
                    return Err(ParseError::InvalidFilter(
                        "regex has an incomplete escape sequence".to_string(),
                    ));
                }
            }
        } else {
            decoded.push(ch);
        }
    }

    if decoded.contains('\n') || decoded.contains('\r') {
        return Err(ParseError::InvalidFilter(
            "regex cannot contain newline escapes".to_string(),
        ));
    }

    let (normalized, start_anchored, end_anchored) = normalize_line_anchors(&decoded);
    let prefix = if start_anchored { "" } else { "(.*)" };
    let suffix = if end_anchored { "" } else { "(.*)" };
    Ok(format!(
        "(?:\n|^){prefix}{core}{suffix}(\n|$)",
        prefix = prefix,
        core = normalized,
        suffix = suffix
    ))
}

fn normalize_line_anchors(pattern: &str) -> (String, bool, bool) {
    let mut result = String::with_capacity(pattern.len());
    let mut characters = pattern.chars();
    let mut escaped = false;
    let mut in_char_class = false;
    let mut start_anchored = false;
    let mut end_anchored = false;

    while let Some(ch) = characters.next() {
        if escaped {
            result.push(ch);
            escaped = false;
            continue;
        }

        match ch {
            '\\' => {
                result.push('\\');
                escaped = true;
            }
            '[' => {
                in_char_class = true;
                result.push(ch);
            }
            ']' => {
                in_char_class = false;
                result.push(ch);
            }
            '^' if !in_char_class => {
                start_anchored = true;
            }
            '$' if !in_char_class => {
                end_anchored = true;
            }
            _ => result.push(ch),
        }
    }

    (result, start_anchored, end_anchored)
}

// Simple tokenizer that handles quoted strings and basic tokens
fn tokenize_query(query: &str) -> Vec<Token> {
    fn push_token(
        tokens: &mut Vec<Token>,
        token: &mut String,
        first_colon_in_quotes: &mut Option<bool>,
    ) {
        if !token.is_empty() {
            tokens.push(Token::new(
                token.clone(),
                first_colon_in_quotes.unwrap_or(false),
            ));
            token.clear();
            *first_colon_in_quotes = None;
        }
    }

    let mut tokens = Vec::new();
    let mut chars = query.chars().peekable();
    let mut current_token = String::new();
    let mut in_quotes = false;
    let mut quote_char = '"';
    let mut first_colon_in_quotes = None;
    let mut escape_next = false;

    while let Some(ch) = chars.next() {
        if escape_next {
            current_token.push(ch);
            escape_next = false;
            continue;
        }

        match ch {
            '"' | '\'' => {
                if !in_quotes {
                    in_quotes = true;
                    quote_char = ch;
                } else if ch == quote_char {
                    in_quotes = false;
                    push_token(&mut tokens, &mut current_token, &mut first_colon_in_quotes);
                } else {
                    current_token.push(ch);
                }
            }
            '\\' if in_quotes => {
                escape_next = true;
            }
            ':' => {
                if first_colon_in_quotes.is_none() {
                    first_colon_in_quotes = Some(in_quotes);
                }
                current_token.push(ch);
                if !in_quotes {
                    if let Some(&next_ch) = chars.peek() {
                        if next_ch != '"' && next_ch != '\'' {
                            while let Some(&next_ch) = chars.peek() {
                                if next_ch.is_whitespace() {
                                    break;
                                }
                                current_token.push(chars.next().unwrap());
                            }
                            push_token(&mut tokens, &mut current_token, &mut first_colon_in_quotes);
                        }
                    }
                }
            }
            ' ' | '\t' | '\n' | '\r' if !in_quotes => {
                push_token(&mut tokens, &mut current_token, &mut first_colon_in_quotes);
            }
            '(' | ')' if !in_quotes => {
                push_token(&mut tokens, &mut current_token, &mut first_colon_in_quotes);
                tokens.push(Token::new(ch.to_string(), false));
            }
            _ => {
                current_token.push(ch);
            }
        }
    }

    push_token(&mut tokens, &mut current_token, &mut first_colon_in_quotes);

    let mut final_tokens = Vec::with_capacity(tokens.len());
    for token in tokens {
        if token.value == "or" || token.value == "OR" {
            final_tokens.push(Token::new("or".to_string(), false));
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

pub const DEFAULT_PAGE_SIZE: u32 = 25;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ContentPredicate {
    Plain(String),
    Regex(String),
}

#[derive(Debug, Clone)]
pub struct TextSearchPlan {
    pub required_terms: Vec<ContentPredicate>,
    pub excluded_terms: Vec<ContentPredicate>,
    pub repos: Vec<String>,
    pub excluded_repos: Vec<String>,
    pub file_globs: Vec<String>,
    pub excluded_file_globs: Vec<String>,
    pub langs: Vec<String>,
    pub excluded_langs: Vec<String>,
    pub branches: Vec<String>,
    pub excluded_branches: Vec<String>,
    pub case_sensitivity: Option<CaseSensitivity>,
    pub highlight_pattern: String,
    pub result_type: Option<ResultType>,
    pub include_historical: bool,
}

#[derive(Debug, Clone)]
pub struct TextSearchRequest {
    pub original_query: String,
    pub plans: Vec<TextSearchPlan>,
    pub page: u32,
    pub page_size: u32,
}

#[derive(Debug, PartialEq)]
pub enum QueryPlanError {
    Parse(ParseError),
    EmptyPlan,
    Unsupported(String),
    Invalid(String),
}

impl fmt::Display for QueryPlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryPlanError::Parse(err) => write!(f, "failed to parse query: {:?}", err),
            QueryPlanError::EmptyPlan => write!(f, "query did not produce any executable plan"),
            QueryPlanError::Unsupported(msg) => write!(f, "unsupported query: {}", msg),
            QueryPlanError::Invalid(msg) => write!(f, "invalid query: {}", msg),
        }
    }
}

impl std::error::Error for QueryPlanError {}

impl From<ParseError> for QueryPlanError {
    fn from(value: ParseError) -> Self {
        QueryPlanError::Parse(value)
    }
}

impl TextSearchRequest {
    pub fn from_query_str(query: &str) -> Result<Self, QueryPlanError> {
        Self::from_query_str_with_page(query, 1, DEFAULT_PAGE_SIZE)
    }

    pub fn from_query_str_with_page(
        query: &str,
        page: u32,
        page_size: u32,
    ) -> Result<Self, QueryPlanError> {
        let ast = parse_query(query)?;
        let flats = flatten_query(&ast)?;
        if flats.is_empty() {
            return Err(QueryPlanError::EmptyPlan);
        }

        let page = page.max(1);
        let page_size = page_size.max(1);

        let mut plans = Vec::with_capacity(flats.len());
        for flat in flats {
            let plan = TextSearchPlan::try_from(flat)?;
            plans.push(plan);
        }

        Ok(TextSearchRequest {
            original_query: query.to_string(),
            plans,
            page,
            page_size,
        })
    }

    pub fn limit_plus_one(&self) -> i64 {
        (self.page_size + 1) as i64
    }

    pub fn offset(&self) -> i64 {
        let page_index = self.page.saturating_sub(1) as i64;
        page_index * self.page_size as i64
    }
}

impl TextSearchPlan {
    fn highlight_from_terms(terms: &[ContentPredicate]) -> String {
        let mut regex_terms = Vec::new();
        for term in terms {
            match term {
                ContentPredicate::Regex(pattern) => regex_terms.push(pattern.clone()),
                ContentPredicate::Plain(value) => regex_terms.push(regex_escape(value)),
            }
        }
        if regex_terms.is_empty() {
            // Fallback highlight that matches anything; this should not happen because
            // we require at least one required term before constructing a plan.
            String::from(".+")
        } else {
            regex_terms.join("|")
        }
    }
}

impl TryFrom<FlatQuery> for TextSearchPlan {
    type Error = QueryPlanError;

    fn try_from(mut value: FlatQuery) -> Result<Self, Self::Error> {
        if value.required_terms.is_empty() {
            return Err(QueryPlanError::Invalid(
                "query requires at least one search term".to_string(),
            ));
        }

        if let Some(result_type) = &value.result_type {
            match result_type {
                ResultType::FileMatch => {}
                other => {
                    return Err(QueryPlanError::Unsupported(format!(
                        "type:{} is not yet supported",
                        match other {
                            ResultType::FileMatch => "filematch",
                            ResultType::FileName => "filename",
                            ResultType::File => "file",
                            ResultType::Repo => "repo",
                        }
                    )));
                }
            }
        }

        let highlight_pattern = TextSearchPlan::highlight_from_terms(&value.required_terms);

        value.required_terms = dedup_content_terms(value.required_terms);
        value.excluded_terms = dedup_content_terms(value.excluded_terms);
        dedup_vec(&mut value.repos);
        dedup_vec(&mut value.excluded_repos);
        dedup_vec(&mut value.file_globs);
        dedup_vec(&mut value.excluded_file_globs);
        dedup_vec(&mut value.langs);
        dedup_vec(&mut value.excluded_langs);
        dedup_vec(&mut value.branches);
        dedup_vec(&mut value.excluded_branches);

        Ok(TextSearchPlan {
            highlight_pattern,
            required_terms: value.required_terms,
            excluded_terms: value.excluded_terms,
            repos: value.repos,
            excluded_repos: value.excluded_repos,
            file_globs: value.file_globs,
            excluded_file_globs: value.excluded_file_globs,
            langs: value.langs,
            excluded_langs: value.excluded_langs,
            branches: value.branches,
            excluded_branches: value.excluded_branches,
            case_sensitivity: value.case_sensitivity,
            result_type: value.result_type,
            include_historical: value.include_historical.unwrap_or(false),
        })
    }
}

#[derive(Debug, Clone)]
struct FlatQuery {
    required_terms: Vec<ContentPredicate>,
    excluded_terms: Vec<ContentPredicate>,
    repos: Vec<String>,
    excluded_repos: Vec<String>,
    file_globs: Vec<String>,
    excluded_file_globs: Vec<String>,
    langs: Vec<String>,
    excluded_langs: Vec<String>,
    branches: Vec<String>,
    excluded_branches: Vec<String>,
    case_sensitivity: Option<CaseSensitivity>,
    result_type: Option<ResultType>,
    include_historical: Option<bool>,
}

impl Default for FlatQuery {
    fn default() -> Self {
        Self {
            required_terms: Vec::new(),
            excluded_terms: Vec::new(),
            repos: Vec::new(),
            excluded_repos: Vec::new(),
            file_globs: Vec::new(),
            excluded_file_globs: Vec::new(),
            langs: Vec::new(),
            excluded_langs: Vec::new(),
            branches: Vec::new(),
            excluded_branches: Vec::new(),
            case_sensitivity: None,
            result_type: None,
            include_historical: None,
        }
    }
}

impl FlatQuery {
    fn merge(mut self, other: &FlatQuery) -> Result<Self, QueryPlanError> {
        self.required_terms
            .extend(other.required_terms.iter().cloned());
        self.excluded_terms
            .extend(other.excluded_terms.iter().cloned());

        self.repos.extend(other.repos.iter().cloned());
        self.excluded_repos
            .extend(other.excluded_repos.iter().cloned());

        self.file_globs.extend(other.file_globs.iter().cloned());
        self.excluded_file_globs
            .extend(other.excluded_file_globs.iter().cloned());

        self.langs.extend(other.langs.iter().cloned());
        self.excluded_langs
            .extend(other.excluded_langs.iter().cloned());

        self.branches.extend(other.branches.iter().cloned());
        self.excluded_branches
            .extend(other.excluded_branches.iter().cloned());

        self.case_sensitivity = merge_case(self.case_sensitivity, other.case_sensitivity.clone())?;
        self.result_type = merge_result_type(self.result_type, other.result_type.clone())?;
        self.include_historical = merge_bool(self.include_historical, other.include_historical)?;

        Ok(self)
    }

    fn from_filter(filter: &Filter, negate: bool) -> Result<Self, QueryPlanError> {
        let mut base = FlatQuery::default();
        match filter {
            Filter::Content(value) => {
                let predicate = ContentPredicate::Plain(value.clone());
                if negate {
                    base.excluded_terms.push(predicate);
                } else {
                    base.required_terms.push(predicate);
                }
            }
            Filter::Repo(value) => {
                if negate {
                    base.excluded_repos.push(value.clone());
                } else {
                    base.repos.push(value.clone());
                }
            }
            Filter::File(value) => {
                let pattern = glob_to_sql_like(value);
                if negate {
                    base.excluded_file_globs.push(pattern);
                } else {
                    base.file_globs.push(pattern);
                }
            }
            Filter::Lang(value) => {
                if negate {
                    base.excluded_langs.push(value.clone());
                } else {
                    base.langs.push(value.clone());
                }
            }
            Filter::Branch(value) => {
                if negate {
                    base.excluded_branches.push(value.clone());
                } else {
                    base.branches.push(value.clone());
                }
            }
            Filter::Regex(pattern) => {
                let predicate = ContentPredicate::Regex(pattern.clone());
                if negate {
                    base.excluded_terms.push(predicate);
                } else {
                    base.required_terms.push(predicate);
                }
            }
            Filter::CaseSensitive(cs) => {
                if negate {
                    return Err(QueryPlanError::Invalid(
                        "negating case: filters is not supported".to_string(),
                    ));
                }
                base.case_sensitivity = Some(cs.clone());
            }
            Filter::Type(kind) => {
                if negate {
                    return Err(QueryPlanError::Unsupported(
                        "negating type: filters is not supported".to_string(),
                    ));
                }
                base.result_type = Some(kind.clone());
            }
            Filter::Historical(flag) => {
                if negate {
                    return Err(QueryPlanError::Unsupported(
                        "negating historical: filters is not supported".to_string(),
                    ));
                }
                base.include_historical = Some(*flag);
            }
        }
        Ok(base)
    }

    fn from_term(term: &str, negate: bool) -> Result<Self, QueryPlanError> {
        if term.is_empty() {
            return Err(QueryPlanError::Invalid("empty search term".to_string()));
        }
        let mut base = FlatQuery::default();
        let predicate = ContentPredicate::Plain(term.to_string());
        if negate {
            base.excluded_terms.push(predicate);
        } else {
            base.required_terms.push(predicate);
        }
        Ok(base)
    }
}

fn flatten_query(node: &QueryNode) -> Result<Vec<FlatQuery>, QueryPlanError> {
    match node {
        QueryNode::Filter(filter) => Ok(vec![FlatQuery::from_filter(filter, false)?]),
        QueryNode::Term(term) => Ok(vec![FlatQuery::from_term(term, false)?]),
        QueryNode::Group(inner) => flatten_query(inner),
        QueryNode::Not(inner) => match inner.as_ref() {
            QueryNode::Filter(filter) => Ok(vec![FlatQuery::from_filter(filter, true)?]),
            QueryNode::Term(term) => Ok(vec![FlatQuery::from_term(term, true)?]),
            _ => Err(QueryPlanError::Unsupported(
                "complex negations are not supported yet".to_string(),
            )),
        },
        QueryNode::And(nodes) => {
            let mut acc = vec![FlatQuery::default()];
            for child in nodes {
                let flattened = flatten_query(child)?;
                let mut next = Vec::new();
                for existing in acc.into_iter() {
                    for add in &flattened {
                        next.push(existing.clone().merge(add)?);
                    }
                }
                acc = next;
            }
            Ok(acc)
        }
        QueryNode::Or(nodes) => {
            let mut results = Vec::new();
            for child in nodes {
                results.extend(flatten_query(child)?);
            }
            Ok(results)
        }
    }
}

fn merge_case(
    left: Option<CaseSensitivity>,
    right: Option<CaseSensitivity>,
) -> Result<Option<CaseSensitivity>, QueryPlanError> {
    match (left, right) {
        (None, other) => Ok(other),
        (other, None) => Ok(other),
        (Some(CaseSensitivity::Auto), other) => Ok(other),
        (other, Some(CaseSensitivity::Auto)) => Ok(other),
        (Some(a), Some(b)) if a == b => Ok(Some(a)),
        (Some(a), Some(b)) => Err(QueryPlanError::Invalid(format!(
            "conflicting case sensitivity filters: {:?} vs {:?}",
            a, b
        ))),
    }
}

fn merge_result_type(
    left: Option<ResultType>,
    right: Option<ResultType>,
) -> Result<Option<ResultType>, QueryPlanError> {
    match (left, right) {
        (None, other) => Ok(other),
        (other, None) => Ok(other),
        (Some(a), Some(b)) if a == b => Ok(Some(a)),
        (Some(a), Some(b)) => Err(QueryPlanError::Invalid(format!(
            "conflicting type filters: {:?} vs {:?}",
            a, b
        ))),
    }
}

fn merge_bool(left: Option<bool>, right: Option<bool>) -> Result<Option<bool>, QueryPlanError> {
    match (left, right) {
        (None, other) => Ok(other),
        (other, None) => Ok(other),
        (Some(a), Some(b)) if a == b => Ok(Some(a)),
        (Some(a), Some(b)) => Err(QueryPlanError::Invalid(format!(
            "conflicting historical filters: {} vs {}",
            a, b
        ))),
    }
}

fn regex_escape(input: &str) -> String {
    let mut escaped = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '\\' | '.' | '+' | '*' | '?' | '^' | '$' | '(' | ')' | '[' | ']' | '{' | '}' | '|' => {
                escaped.push('\\');
                escaped.push(ch);
            }
            other => escaped.push(other),
        }
    }
    escaped
}

fn glob_to_sql_like(input: &str) -> String {
    let mut pattern = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '*' => pattern.push('%'),
            '?' => pattern.push('_'),
            '%' | '_' | '\\' => {
                pattern.push('\\');
                pattern.push(ch);
            }
            other => pattern.push(other),
        }
    }
    pattern
}

fn dedup_vec(values: &mut Vec<String>) {
    let mut seen = HashSet::new();
    values.retain(|val| seen.insert(val.clone()));
}

fn dedup_content_terms(values: Vec<ContentPredicate>) -> Vec<ContentPredicate> {
    let mut seen = HashSet::new();
    values
        .into_iter()
        .filter(|term| seen.insert(term.clone()))
        .collect()
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
        let values: Vec<_> = tokens.into_iter().map(|t| t.value).collect();
        assert_eq!(values, vec!["content:hello world", "repo:myrepo"]);
    }

    #[test]
    fn tokenize_marks_colon_inside_quotes() {
        let tokens = tokenize_query("\"foo:bar\"");
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].value, "foo:bar");
        assert!(tokens[0].first_colon_in_quotes);
    }

    #[test]
    fn tokenize_supports_escaped_quotes() {
        let tokens = tokenize_query(r#"content:"foo \"bar\" baz""#);
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].value, r#"content:foo "bar" baz"#);
    }

    #[test]
    fn quoted_term_with_colon_parses_as_term() {
        let result = parse_query("\"foo:bar\"").expect("query should parse");
        match result {
            QueryNode::Term(term) => assert_eq!(term, "foo:bar"),
            other => panic!("expected term node, got {:?}", other),
        }
    }

    #[test]
    fn filter_with_quoted_value_still_parses() {
        let result = parse_query("repo:\"foo:bar\"").expect("query should parse");
        match result {
            QueryNode::Filter(Filter::Repo(repo)) => assert_eq!(repo, "foo:bar"),
            other => panic!("expected repo filter, got {:?}", other),
        }
    }

    #[test]
    fn content_filter_with_escaped_quotes_parses() {
        let result = parse_query(r#"content:"foo \"bar\" baz""#).expect("query should parse");
        match result {
            QueryNode::Filter(Filter::Content(value)) => {
                assert_eq!(value, r#"foo "bar" baz"#)
            }
            other => panic!("expected content filter, got {:?}", other),
        }
    }

    #[test]
    fn preprocess_regex_basic_pattern() {
        let pattern = preprocess_regex_pattern("void").expect("should preprocess");
        assert_eq!(pattern, "(?:\n|^)(.*)void(.*)(\n|$)");
    }

    #[test]
    fn preprocess_regex_with_tab_escape() {
        let pattern = preprocess_regex_pattern("\\tfoo").expect("should preprocess");
        assert_eq!(pattern, "(?:\n|^)(.*)\tfoo(.*)(\n|$)");
    }

    #[test]
    fn preprocess_regex_rejects_newline_escape() {
        match preprocess_regex_pattern("\\nfoo") {
            Err(ParseError::InvalidFilter(msg)) => {
                assert!(msg.contains("newline"), "unexpected message: {}", msg);
            }
            other => panic!("expected newline error, got {:?}", other),
        }
    }

    #[test]
    fn preprocess_regex_incomplete_escape() {
        match preprocess_regex_pattern("\\") {
            Err(ParseError::InvalidFilter(msg)) => {
                assert!(msg.contains("incomplete"), "unexpected message: {}", msg);
            }
            other => panic!("expected incomplete escape error, got {:?}", other),
        }
    }

    #[test]
    fn preprocess_regex_preserves_line_anchors() {
        let pattern = preprocess_regex_pattern("^foo$").expect("should preprocess");
        assert_eq!(pattern, "(?:\n|^)foo(\n|$)");
    }

    #[test]
    fn preprocess_regex_start_anchor_only() {
        let pattern = preprocess_regex_pattern("^foo").expect("should preprocess");
        assert_eq!(pattern, "(?:\n|^)foo(.*)(\n|$)");
    }

    #[test]
    fn preprocess_regex_end_anchor_only() {
        let pattern = preprocess_regex_pattern("foo$").expect("should preprocess");
        assert_eq!(pattern, "(?:\n|^)(.*)foo(\n|$)");
    }
}
