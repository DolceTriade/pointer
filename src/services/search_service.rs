use leptos::prelude::*;

#[cfg(feature = "ssr")]
use crate::db::Database;
use crate::db::models::SearchResult;
#[cfg(feature = "ssr")]
use crate::db::postgres::PostgresDb;
use crate::dsl::TextSearchRequest;

#[server]
pub async fn search(query: String) -> Result<Vec<SearchResult>, ServerFnError> {
    let request =
        TextSearchRequest::from_query_str(&query).map_err(|e| ServerFnError::new(e.to_string()))?;
    let state = expect_context::<crate::server::GlobalAppState>();
    let state = state.lock().await;
    let db = PostgresDb::new(state.pool.clone());
    db.text_search(&request)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))
}
