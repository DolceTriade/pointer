use axum::{
    Json, Router,
    body::Body,
    extract::Extension,
    http::{Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use leptos::config::LeptosOptions;
use leptos::prelude::provide_context;
use leptos::reactive::{computed::ScopedFuture, owner::Owner};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::mcp::tools::{
    execute_file_content, execute_file_list, execute_path_search, execute_repo_branches,
    execute_repositories, execute_search, execute_symbol_insights, normalize_tool_error,
    tool_file_content, tool_file_list, tool_path_search, tool_repo_branches, tool_repositories,
    tool_search, tool_symbol_insights,
};
use crate::mcp::types::{
    API_SURFACE, ApiResponse, FileContentToolRequest, FileListToolRequest, PathSearchToolRequest,
    RepoBranchesToolRequest, RepositoriesToolRequest, SearchToolRequest, SymbolInsightsToolRequest,
};
use crate::server::GlobalAppState;

pub fn router(state: GlobalAppState) -> Router<LeptosOptions> {
    let mcp_routes = Router::<LeptosOptions>::new()
        .route("/", post(mcp_rpc).get(mcp_info))
        .route("/docs", get(mcp_docs))
        .route("/healthz", get(healthz))
        .route("/tools/search", post(tool_search))
        .route("/tools/repo_branches", post(tool_repo_branches))
        .route("/tools/repositories", post(tool_repositories))
        .route("/tools/file_content", post(tool_file_content))
        .route("/tools/file_list", post(tool_file_list))
        .route("/tools/path_search", post(tool_path_search))
        .route("/tools/symbol_insights", post(tool_symbol_insights))
        .layer(middleware::from_fn(mcp_leptos_context_middleware))
        .layer(Extension(state));

    Router::<LeptosOptions>::new().nest("/mcp/v1", mcp_routes)
}

async fn mcp_leptos_context_middleware(
    Extension(state): Extension<GlobalAppState>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let owner = Owner::new();
    owner
        .with(|| {
            ScopedFuture::new(async move {
                provide_context(state);
                next.run(req).await
            })
        })
        .await
}

async fn healthz() -> impl IntoResponse {
    let payload = ApiResponse::success(serde_json::json!({
        "status": "ok",
        "api_surface": API_SURFACE,
    }));
    (StatusCode::OK, Json(payload))
}

async fn mcp_info() -> impl IntoResponse {
    let payload = ApiResponse::success(json!({
        "status": "ok",
        "api_surface": API_SURFACE,
        "transport": "json-rpc-over-http",
        "endpoint": "/mcp/v1",
        "docs_endpoint": "/mcp/v1/docs",
    }));
    (StatusCode::OK, Json(payload))
}

async fn mcp_docs() -> impl IntoResponse {
    let payload = ApiResponse::success(json!({
        "dsl": {
            "semantics": "AND-first",
            "or_support": "For OR semantics, issue multiple search calls and merge/deduplicate client-side.",
            "wildcards": "Plain search terms do not support wildcard matching. Use regex mode for pattern matching.",
            "regex": "Use regex:<pattern> to enable regex matching.",
            "path_search_behavior": "path_search requires a non-empty query and is for fuzzy path matching only.",
            "file_list_behavior": "file_list enumerates directories and files with optional recursive depth and limit.",
            "recency_workflow": "For recent or older change questions: repositories -> repo_branches -> search by branch and compare indexed_at or is_live.",
            "filters": [
                "repo:<name>",
                "branch:<name>",
                "lang:<language>",
                "path:<glob>",
                "file:<glob>",
                "regex:<pattern>",
                "case:yes|no|auto",
                "historical:yes|no"
            ],
            "troubleshooting": [
                "No results with repo filter: call repositories and use exact repo key.",
                "No branch results: call repo_branches and use exact branch name.",
                "Need OR behavior: run multiple search calls and merge client-side.",
                "Need regex matching: use regex:<pattern> instead of wildcard plain terms.",
                "Need directory listing: use file_list instead of path_search with empty query."
            ]
        },
        "cookbook": [
            "1) repositories(limit=20) to discover repo keys",
            "2) repo_branches(repo) to discover branch names and freshness",
            "3) search(query='repo:<repo> branch:<branch> <term>') for scoped search",
            "4) search with historical:yes for older snapshots",
            "5) search with regex:<pattern> for pattern matching",
            "6) file_list(repo, branch, path, depth, limit) for enumeration",
            "7) path_search(repo, branch, query) for fuzzy path lookup",
            "8) file_content(repo, branch, path) for raw source text",
            "9) symbol_insights(params) for definitions and references",
            "10) OR behavior: send multiple search calls or one batch search with queries[]",
            "11) For no results, broaden filters and retry per branch"
        ]
    }));
    (StatusCode::OK, Json(payload))
}

#[derive(Debug, Deserialize)]
struct JsonRpcRequest {
    jsonrpc: Option<String>,
    id: Option<Value>,
    method: String,
    params: Option<Value>,
}

#[derive(Debug, Serialize)]
struct JsonRpcResponse {
    jsonrpc: &'static str,
    id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<JsonRpcError>,
}

#[derive(Debug, Serialize)]
struct JsonRpcError {
    code: i64,
    message: String,
}

#[derive(Debug, Deserialize)]
struct ToolCallParams {
    name: String,
    #[serde(default)]
    arguments: Value,
}

async fn mcp_rpc(Json(req): Json<JsonRpcRequest>) -> Response {
    if req.jsonrpc.as_deref() != Some("2.0") {
        return jsonrpc_error(req.id, -32600, "jsonrpc must be \"2.0\"");
    }

    // Notifications do not require a response.
    if req.id.is_none() && req.method.starts_with("notifications/") {
        return StatusCode::NO_CONTENT.into_response();
    }

    match req.method.as_str() {
        "initialize" => {
            let result = json!({
                "protocolVersion": "2024-11-05",
                "capabilities": {
                    "tools": { "listChanged": false }
                },
                "serverInfo": {
                    "name": "pointer-mcp",
                    "version": env!("CARGO_PKG_VERSION"),
                },
                "instructions": "Use tools to query indexed code and symbol information. Operational flow: repositories -> repo_branches -> file_list/path_search -> file_content/search/symbol_insights. For recency/version questions like 'recent change', call repo_branches first, then run search repeatedly with explicit branch filters (e.g. branch:main, branch:release/*), compare branch-level results and indexed_at/is_live metadata, and add historical:yes when historical snapshots should be included. DSL behavior is AND-first; do not rely on OR in a single query. For OR intent, issue multiple search calls (one query per alternative) and merge/deduplicate client-side. Plain terms do not support wildcard matching; use regex:\"...\" for pattern matching. path_search requires a non-empty query and is not a directory listing endpoint; use file_list for enumeration. search supports batch mode with queries:[...] and optional dedupe mode.",
            });
            jsonrpc_result(req.id, result)
        }
        "tools/list" => {
            let result = json!({ "tools": mcp_tools() });
            jsonrpc_result(req.id, result)
        }
        "tools/call" => {
            let Some(raw_params) = req.params else {
                return jsonrpc_error(req.id, -32602, "missing tools/call params");
            };
            let params: ToolCallParams = match serde_json::from_value(raw_params) {
                Ok(v) => v,
                Err(err) => return jsonrpc_error(req.id, -32602, format!("invalid params: {err}")),
            };

            let tool_output = match execute_tool_call(&params.name, params.arguments).await {
                Ok(value) => json!({
                    "content": [{
                        "type": "text",
                        "text": serde_json::to_string_pretty(&value).unwrap_or_else(|_| value.to_string()),
                    }],
                    "structuredContent": value,
                    "isError": false
                }),
                Err(message) => {
                    let (code, error_message, suggestion) =
                        normalize_tool_error(&params.name, message);
                    json!({
                        "content": [{
                            "type": "text",
                            "text": format!(
                                "{}{}",
                                error_message,
                                suggestion
                                    .as_ref()
                                    .map(|s| format!(" Suggestion: {s}"))
                                    .unwrap_or_default()
                            ),
                        }],
                        "isError": true,
                        "structuredContent": {
                            "error_code": code,
                            "message": error_message,
                            "suggestion": suggestion,
                        }
                    })
                }
            };

            jsonrpc_result(req.id, tool_output)
        }
        "notifications/initialized" => StatusCode::NO_CONTENT.into_response(),
        _ => jsonrpc_error(req.id, -32601, format!("method not found: {}", req.method)),
    }
}

async fn execute_tool_call(name: &str, arguments: Value) -> Result<Value, String> {
    match name {
        "search" => {
            let payload: SearchToolRequest =
                serde_json::from_value(arguments).map_err(|err| err.to_string())?;
            execute_search(payload).await
        }
        "repo_branches" => {
            let payload: RepoBranchesToolRequest =
                serde_json::from_value(arguments).map_err(|err| err.to_string())?;
            let data = execute_repo_branches(payload).await?;
            serde_json::to_value(data).map_err(|err| err.to_string())
        }
        "repositories" => {
            let payload: RepositoriesToolRequest =
                serde_json::from_value(arguments).map_err(|err| err.to_string())?;
            let data = execute_repositories(payload).await?;
            serde_json::to_value(data).map_err(|err| err.to_string())
        }
        "file_content" => {
            let payload: FileContentToolRequest =
                serde_json::from_value(arguments).map_err(|err| err.to_string())?;
            let data = execute_file_content(payload).await?;
            serde_json::to_value(data).map_err(|err| err.to_string())
        }
        "file_list" => {
            let payload: FileListToolRequest =
                serde_json::from_value(arguments).map_err(|err| err.to_string())?;
            let data = execute_file_list(payload).await?;
            serde_json::to_value(data).map_err(|err| err.to_string())
        }
        "path_search" => {
            let payload: PathSearchToolRequest =
                serde_json::from_value(arguments).map_err(|err| err.to_string())?;
            let data = execute_path_search(payload).await?;
            serde_json::to_value(data).map_err(|err| err.to_string())
        }
        "symbol_insights" => {
            let payload: SymbolInsightsToolRequest =
                serde_json::from_value(arguments).map_err(|err| err.to_string())?;
            let data = execute_symbol_insights(payload).await?;
            serde_json::to_value(data).map_err(|err| err.to_string())
        }
        _ => Err(format!("unknown tool: {name}")),
    }
}

fn jsonrpc_result(id: Option<Value>, result: Value) -> Response {
    let response = JsonRpcResponse {
        jsonrpc: "2.0",
        id: id.unwrap_or(Value::Null),
        result: Some(result),
        error: None,
    };
    (StatusCode::OK, Json(response)).into_response()
}

fn jsonrpc_error(id: Option<Value>, code: i64, message: impl Into<String>) -> Response {
    let response = JsonRpcResponse {
        jsonrpc: "2.0",
        id: id.unwrap_or(Value::Null),
        result: None,
        error: Some(JsonRpcError {
            code,
            message: message.into(),
        }),
    };
    (StatusCode::OK, Json(response)).into_response()
}

fn mcp_tools() -> Vec<Value> {
    vec![
        json!({
            "name": "search",
            "description": "Search indexed source code (from Pointer index, not local filesystem) using Pointer DSL query syntax. Supports branch-scoped and historical queries (e.g. 'repo:linux branch:main symbol' or 'repo:linux branch:main historical:yes symbol'). Plain terms use substring matching and do NOT support wildcard syntax; use regex:\"...\" for pattern queries. Treat query terms/filters as AND semantics. For OR semantics, run multiple search calls and merge/deduplicate results client-side. Supports optional batch mode with queries:[...] plus dedupe controls.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": { "type": "string", "description": "Single-query mode. Provide either query or queries." },
                    "queries": {
                        "type": "array",
                        "items": { "type": "string" },
                        "minItems": 1,
                        "maxItems": 8,
                        "description": "Batch mode. Each query is executed separately and merged."
                    },
                    "page": { "type": "integer", "minimum": 1, "description": "Single-query page number." },
                    "dedupe": {
                        "type": "string",
                        "enum": ["repo_path_line", "repo_path", "none"],
                        "description": "Batch dedupe strategy."
                    },
                    "max_results_per_query": { "type": "integer", "minimum": 1, "maximum": 100 }
                },
                "anyOf": [
                    { "required": ["query"] },
                    { "required": ["queries"] }
                ],
                "additionalProperties": false
            }
        }),
        json!({
            "name": "repositories",
            "description": "List indexed repositories available for search. Use this first to discover exact repository keys before applying repo:<name> filters. Results include index_freshness metadata.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "limit": { "type": "integer", "minimum": 1, "maximum": 50 }
                },
                "additionalProperties": false
            }
        }),
        json!({
            "name": "repo_branches",
            "description": "List indexed branches/heads for a repository, including commit_sha, indexed_at, and is_live. Use this before branch-by-branch comparisons for 'newer/older/recent change' questions and then run search with explicit branch:<name> filters. Results include per-branch freshness ages.",
            "inputSchema": {
                "type": "object",
                "properties": { "repo": { "type": "string" } },
                "required": ["repo"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": "file_content",
            "description": "Read raw indexed file content (no syntax highlighting) for an exact repo/branch/path from the index. Use this after file_list/path_search or when you already know the exact file path. Includes branch freshness metadata.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "repo": { "type": "string" },
                    "branch": { "type": "string" },
                    "path": { "type": "string" }
                },
                "required": ["repo", "branch", "path"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": "file_list",
            "description": "Enumerate files/directories under a path for a repository+branch from the index. Supports bounded recursive listing with depth and limit. Use this for directory listing workflows. Response includes truncated flag, branch freshness, and stable paths.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "repo": { "type": "string" },
                    "branch": { "type": "string" },
                    "path": { "type": "string" },
                    "depth": { "type": "integer", "minimum": 1, "maximum": 10 },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 5000 }
                },
                "required": ["repo", "branch"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": "path_search",
            "description": "Search file and directory paths within a repository and branch using a non-empty query. This matches paths only (fuzzy path lookup) and does not enumerate full directory contents; use file_list for enumeration. Includes freshness metadata.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "repo": { "type": "string" },
                    "branch": { "type": "string" },
                    "query": { "type": "string" },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 50 }
                },
                "required": ["repo", "branch", "query"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": "symbol_insights",
            "description": "Find symbol definitions and references with snippets in indexed code. For scoped analysis, set params.scope (repository/directory/file/custom) and optional include_paths/excluded_paths. Includes freshness metadata for the selected branch.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "params": {
                        "type": "object",
                        "properties": {
                            "repo": { "type": "string" },
                            "branch": { "type": "string" },
                            "path": { "type": "string" },
                            "symbol": { "type": "string" },
                            "language": { "type": "string" },
                            "scope": {
                                "type": "string",
                                "enum": ["repository", "directory", "file", "custom"]
                            },
                            "include_paths": {
                                "type": "array",
                                "items": { "type": "string" }
                            },
                            "excluded_paths": {
                                "type": "array",
                                "items": { "type": "string" }
                            }
                        },
                        "required": ["repo", "branch", "symbol", "scope"],
                        "additionalProperties": false
                    }
                },
                "required": ["params"],
                "additionalProperties": false
            }
        }),
    ]
}
