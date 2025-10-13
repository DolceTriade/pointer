use leptos::prelude::*;

use crate::db::models::SearchResult;
#[cfg(feature = "ssr")]
use crate::db::postgres::PostgresDb;
#[cfg(feature = "ssr")]
use crate::db::Database;

#[server]
pub async fn search(query: String) -> Result<Vec<SearchResult>, ServerFnError> {
    let state = expect_context::<crate::server::GlobalAppState>();
    let state = state.lock().await;
    let db = PostgresDb::new(state.pool.clone());
    db.text_search(&query)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))
}