use std::convert::TryFrom;
use std::env;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::{ArgAction, Args, Parser, Subcommand};
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::config::IndexerConfig;
use crate::engine::Indexer;
use crate::output;
use crate::upload;
use crate::utils;

#[derive(Debug, Parser)]
#[command(
    name = "pointer-indexer",
    version,
    about = "Pointer indexing and query CLI"
)]
pub struct Cli {
    /// Increase logging verbosity (use -vv for trace level).
    #[arg(short, long, action = ArgAction::Count, global = true)]
    pub verbose: u8,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Index a repository and produce/upload search metadata.
    Index(IndexArgs),
    /// Query a running backend for symbols.
    Query(QueryArgs),
}

#[derive(Debug, Args)]
pub struct IndexArgs {
    /// Human-readable repository identifier (defaults to the repo directory name).
    #[arg(long, env = "POINTER_REPOSITORY")]
    pub repository: Option<String>,
    /// Path to the repository root to index.
    #[arg(long = "repo", default_value = ".")]
    pub repo_path: PathBuf,
    /// Commit SHA to associate with the produced metadata. Defaults to HEAD.
    #[arg(long)]
    pub commit: Option<String>,
    /// Branch name associated with the commit. Defaults to the current branch when available.
    #[arg(long)]
    pub branch: Option<String>,
    /// Directory where JSON artifacts will be written.
    #[arg(long, default_value = "index-output")]
    pub output_dir: PathBuf,
    /// URL of the backend ingestion endpoint. When provided, the generated index will be uploaded.
    #[arg(long)]
    pub upload_url: Option<String>,
    /// API key used when uploading to the backend (sent as a Bearer token).
    #[arg(long)]
    pub upload_api_key: Option<String>,
}

#[derive(Debug, Args)]
pub struct QueryArgs {
    /// Backend search endpoint (e.g. http://localhost:8080/api/v1/search)
    #[arg(long, env = "POINTER_QUERY_URL")]
    pub url: String,
    /// Optional bearer token used for authorization when querying.
    #[arg(long)]
    pub api_key: Option<String>,
    /// Case-insensitive substring match on the symbol name.
    #[arg(long)]
    pub name: Option<String>,
    /// Case-insensitive regex match on the symbol name.
    #[arg(long)]
    pub name_regex: Option<String>,
    /// Exact namespace match.
    #[arg(long)]
    pub namespace: Option<String>,
    /// Namespace prefix match.
    #[arg(long)]
    pub namespace_prefix: Option<String>,
    /// Symbol kinds to include (repeat flag or comma-delimited).
    #[arg(long, value_delimiter = ',')]
    pub kind: Vec<String>,
    /// Languages to include (repeat flag or comma-delimited).
    #[arg(long, value_delimiter = ',')]
    pub language: Vec<String>,
    /// Filter to a repository ID.
    #[arg(long)]
    pub repository: Option<String>,
    /// Filter to a commit.
    #[arg(long)]
    pub commit_sha: Option<String>,
    /// Substring match on file path.
    #[arg(long)]
    pub path: Option<String>,
    /// Regex match on file path.
    #[arg(long)]
    pub path_regex: Option<String>,
    /// Include reference locations in the response.
    #[arg(long)]
    pub include_references: bool,
    /// Maximum number of results (default 100, max 1000).
    #[arg(long)]
    pub limit: Option<i64>,
    /// Print raw JSON instead of a pretty text summary.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Serialize)]
struct SearchRequest<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    name_regex: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    namespace: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    namespace_prefix: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    kind: Option<&'a [String]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    language: Option<&'a [String]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    repository: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    commit_sha: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    path_regex: Option<&'a str>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    include_paths: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    excluded_paths: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    path_hint: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    include_references: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<i64>,
}

#[derive(Debug, Deserialize, Serialize)]
struct SearchResponse {
    symbols: Vec<SymbolResult>,
}

#[derive(Debug, Deserialize, Serialize)]
struct SymbolResult {
    symbol: String,
    namespace: Option<String>,
    kind: Option<String>,
    fully_qualified: String,
    repository: String,
    commit_sha: String,
    file_path: String,
    language: Option<String>,
    line: Option<usize>,
    column: Option<usize>,
    references: Option<Vec<ReferenceResult>>,
    score: f64,
}

#[derive(Debug, Deserialize, Serialize)]
struct ReferenceResult {
    name: String,
    namespace: Option<String>,
    kind: Option<String>,
    fully_qualified: String,
    line: usize,
    column: usize,
}

#[derive(Debug, Serialize)]
struct SnippetRequest<'a> {
    repository: &'a str,
    commit_sha: &'a str,
    file_path: &'a str,
    line: u32,
    context: u32,
}

#[derive(Debug, Deserialize)]
struct SnippetResponse {
    start_line: u32,
    highlight_line: u32,
    total_lines: u32,
    lines: Vec<String>,
    truncated: bool,
}

const SNIPPET_CONTEXT: u32 = 2;

pub fn run() -> Result<()> {
    let cli = Cli::parse();
    utils::init_tracing(cli.verbose)?;

    match cli.command {
        Commands::Index(args) => run_index(args),
        Commands::Query(args) => run_query(args),
    }
}

fn run_index(args: IndexArgs) -> Result<()> {
    let repo_path = resolve_repo_path(&args.repo_path)?;
    let repository = args
        .repository
        .clone()
        .unwrap_or_else(|| utils::default_repo_name(&repo_path));
    let output_dir = resolve_output_dir(&args.output_dir)?;

    let repo_meta =
        utils::resolve_repo_metadata(&repo_path, args.commit.clone(), args.branch.clone())?;

    let config = IndexerConfig::new(
        repo_path.clone(),
        repository.clone(),
        repo_meta.branch,
        repo_meta.commit,
        output_dir.clone(),
    );

    let indexer = Indexer::new(config);
    let artifacts = indexer.run()?;
    output::write_report(&output_dir, &artifacts)?;

    if let Some(url) = args.upload_url.as_deref() {
        info!(%url, "uploading index to backend");
        upload::upload_index(url, args.upload_api_key.as_deref(), &artifacts)?;
    }

    info!(repo = repository, output = ?output_dir, files = artifacts.file_pointer_count(), "indexing complete");

    Ok(())
}

fn run_query(args: QueryArgs) -> Result<()> {
    let request = SearchRequest {
        name: args.name.as_deref(),
        name_regex: args.name_regex.as_deref(),
        namespace: args.namespace.as_deref(),
        namespace_prefix: args.namespace_prefix.as_deref(),
        kind: if args.kind.is_empty() {
            None
        } else {
            Some(&args.kind)
        },
        language: if args.language.is_empty() {
            None
        } else {
            Some(&args.language)
        },
        repository: args.repository.as_deref(),
        commit_sha: args.commit_sha.as_deref(),
        path: args.path.as_deref(),
        path_regex: args.path_regex.as_deref(),
        include_paths: Vec::new(),
        excluded_paths: Vec::new(),
        path_hint: None,
        include_references: if args.include_references {
            Some(true)
        } else {
            None
        },
        limit: args.limit,
    };

    let client = Client::new();
    let snippet_url = snippet_endpoint(&args.url);
    let mut req = client.post(&args.url).json(&request);

    if let Some(key) = args.api_key.as_ref() {
        req = req.bearer_auth(key);
    }

    let response = req.send().context("failed to query backend")?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().unwrap_or_default();
        anyhow::bail!("query failed with status {status}: {body}");
    }

    let response: SearchResponse = response
        .json()
        .context("failed to parse backend response")?;

    if args.json {
        println!("{}", serde_json::to_string_pretty(&response)?);
        return Ok(());
    }

    if response.symbols.is_empty() {
        println!("No results.");
        return Ok(());
    }

    for symbol in response.symbols {
        println!(
            "{} ({}):",
            symbol.symbol,
            symbol.kind.unwrap_or_else(|| "unknown".to_string())
        );
        if let Some(namespace) = &symbol.namespace {
            println!("  namespace: {}", namespace);
        }
        println!("  fully qualified: {}", symbol.fully_qualified);
        println!(
            "  location: {}@{} -> {}",
            symbol.repository, symbol.commit_sha, symbol.file_path
        );
        if let Some(lang) = &symbol.language {
            println!("  language: {}", lang);
        }

        if args.include_references {
            match symbol.references {
                Some(refs) if !refs.is_empty() => {
                    println!("  references:");
                    for reference in refs {
                        println!(
                            "    - {} (line {}, column {})",
                            reference.fully_qualified, reference.line, reference.column
                        );
                        let line = match u32::try_from(reference.line) {
                            Ok(line) if line > 0 => line,
                            _ => {
                                println!("      (invalid line number)");
                                continue;
                            }
                        };

                        match fetch_snippet(
                            &client,
                            &snippet_url,
                            args.api_key.as_deref(),
                            &symbol.repository,
                            &symbol.commit_sha,
                            &symbol.file_path,
                            line,
                            SNIPPET_CONTEXT,
                        ) {
                            Ok(snippet) => print_snippet(&snippet),
                            Err(err) => println!("      (snippet unavailable: {err})"),
                        }
                    }
                }
                _ => println!("  references: none"),
            }
        }

        println!();
    }

    Ok(())
}

fn fetch_snippet(
    client: &Client,
    url: &str,
    api_key: Option<&str>,
    repository: &str,
    commit_sha: &str,
    file_path: &str,
    line: u32,
    context: u32,
) -> Result<SnippetResponse> {
    let payload = SnippetRequest {
        repository,
        commit_sha,
        file_path,
        line,
        context,
    };

    let mut req = client.post(url).json(&payload);
    if let Some(key) = api_key {
        req = req.bearer_auth(key);
    }

    let response = req
        .send()
        .with_context(|| format!("failed to request snippet from {}", url))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().unwrap_or_default();
        anyhow::bail!("snippet request failed with status {status}: {body}");
    }

    Ok(response
        .json()
        .context("failed to parse snippet response")?)
}

fn snippet_endpoint(search_url: &str) -> String {
    let trimmed = search_url.trim_end_matches('/');
    if let Some((base, last)) = trimmed.rsplit_once('/') {
        if last.eq_ignore_ascii_case("search") {
            format!("{}/files/snippet", base)
        } else {
            format!("{}/files/snippet", trimmed)
        }
    } else {
        format!("{}/files/snippet", trimmed)
    }
}

fn print_snippet(snippet: &SnippetResponse) {
    if snippet.lines.is_empty() {
        println!("      (no snippet content)");
        return;
    }

    for (idx, line) in snippet.lines.iter().enumerate() {
        let line_number = snippet.start_line + idx as u32;
        let marker = if line_number == snippet.highlight_line {
            '>'
        } else {
            ' '
        };
        println!("      {} {:>6} | {}", marker, line_number, line);
    }

    if snippet.truncated {
        println!("      ... ({} total lines)", snippet.total_lines);
    }
}

fn resolve_repo_path(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(env::current_dir()?.join(path))
    }
}

fn resolve_output_dir(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(env::current_dir()?.join(path))
    }
}
