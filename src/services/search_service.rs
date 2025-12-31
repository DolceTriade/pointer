use leptos::prelude::*;

#[cfg(feature = "ssr")]
use crate::db::Database;
use crate::db::models::{SearchResultsPage, SymbolSuggestion};
#[cfg(feature = "ssr")]
use crate::db::postgres::PostgresDb;
#[cfg(feature = "ssr")]
use crate::dsl::{DEFAULT_PAGE_SIZE, TextSearchRequest};

#[server]
pub async fn search(query: String, page: u32) -> Result<SearchResultsPage, ServerFnError> {
    let normalized_page = page.max(1);
    let request =
        TextSearchRequest::from_query_str_with_page(&query, normalized_page, DEFAULT_PAGE_SIZE)
            .map_err(|e| ServerFnError::new(e.to_string()))?;
    let state = expect_context::<crate::server::GlobalAppState>();
    let state = state.lock().await;
    let db = PostgresDb::new(state.pool.clone());
    db.text_search(&request)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))
}

#[server]
pub async fn autocomplete_repositories(
    term: String,
    limit: i64,
) -> Result<Vec<String>, ServerFnError> {
    let state = expect_context::<crate::server::GlobalAppState>();
    let state = state.lock().await;
    let db = PostgresDb::new(state.pool.clone());
    let normalized_limit = limit.max(1).min(20);
    db.autocomplete_repositories(term.trim(), normalized_limit)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))
}

#[server]
pub async fn autocomplete_paths(
    term: String,
    repositories: Vec<String>,
    limit: i64,
) -> Result<Vec<String>, ServerFnError> {
    let state = expect_context::<crate::server::GlobalAppState>();
    let state = state.lock().await;
    let db = PostgresDb::new(state.pool.clone());
    let normalized_limit = limit.max(1).min(20);
    let repos: Vec<String> = repositories
        .into_iter()
        .map(|repo| repo.trim().to_string())
        .filter(|repo| !repo.is_empty())
        .collect();
    db.autocomplete_paths(&repos, term.trim(), normalized_limit)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))
}

#[server]
pub async fn autocomplete_symbols(
    term: String,
    limit: i64,
) -> Result<Vec<SymbolSuggestion>, ServerFnError> {
    let trimmed = term.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    let state = expect_context::<crate::server::GlobalAppState>();
    let state = state.lock().await;
    let db = PostgresDb::new(state.pool.clone());
    let normalized_limit = limit.max(1).min(20);
    db.autocomplete_symbols(trimmed, normalized_limit)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))
}

#[server]
pub async fn autocomplete_languages(
    term: String,
    repositories: Vec<String>,
    limit: i64,
) -> Result<Vec<String>, ServerFnError> {
    let state = expect_context::<crate::server::GlobalAppState>();
    let state = state.lock().await;
    let db = PostgresDb::new(state.pool.clone());
    let normalized_limit = limit.max(1).min(20);
    let repos: Vec<String> = repositories
        .into_iter()
        .map(|repo| repo.trim().to_string())
        .filter(|repo| !repo.is_empty())
        .collect();
    db.autocomplete_languages(&repos, term.trim(), normalized_limit)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))
}

#[server]
pub async fn autocomplete_branches(
    term: String,
    repositories: Vec<String>,
    limit: i64,
) -> Result<Vec<String>, ServerFnError> {
    let state = expect_context::<crate::server::GlobalAppState>();
    let state = state.lock().await;
    let db = PostgresDb::new(state.pool.clone());
    let normalized_limit = limit.max(1).min(20);
    let repos: Vec<String> = repositories
        .into_iter()
        .map(|repo| repo.trim().to_string())
        .filter(|repo| !repo.is_empty())
        .collect();
    db.autocomplete_branches(&repos, term.trim(), normalized_limit)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))
}

#[server]
pub async fn autocomplete_files(
    term: String,
    repositories: Vec<String>,
    limit: i64,
) -> Result<Vec<String>, ServerFnError> {
    let state = expect_context::<crate::server::GlobalAppState>();
    let state = state.lock().await;
    let db = PostgresDb::new(state.pool.clone());
    let normalized_limit = limit.max(1).min(20);
    let repos: Vec<String> = repositories
        .into_iter()
        .map(|repo| repo.trim().to_string())
        .filter(|repo| !repo.is_empty())
        .collect();
    db.autocomplete_files(&repos, term.trim(), normalized_limit)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))
}
