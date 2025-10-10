use crate::db::RepoSummary;
use leptos::prelude::*;

#[cfg(feature = "ssr")]
use crate::db::{postgres::PostgresDb, Database};

#[server]
pub async fn get_repositories(limit: usize) -> Result<Vec<RepoSummary>, ServerFnError> {
    let state = expect_context::<crate::server::GlobalAppState>();
    let state = state.lock().await;

    // Create a database instance using the pool
    let db = PostgresDb::new(state.pool.clone());

    // Get all repositories from the database
    let repos = db.get_all_repositories().await?;

    // Take only the first 10 repos
    let repos = repos.into_iter().take(limit.clamp(1, 50)).collect();

    Ok(repos)
}
