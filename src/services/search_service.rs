use leptos::prelude::*;

#[cfg(feature = "ssr")]
use crate::db::Database;
use crate::db::models::SearchResultsPage;
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
