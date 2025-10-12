use leptos::prelude::*;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct SearchResult {
    pub repository: String,
    pub commit_sha: String,
    pub file_path: String,
    pub start_line: i32,
    pub end_line: i32,
    pub content_text: String,
}

#[server]
pub async fn search(query: String) -> Result<Vec<SearchResult>, ServerFnError> {
    let state = expect_context::<crate::server::GlobalAppState>();
    let state = state.lock().await;
    let pool = &state.pool;

    #[derive(sqlx::FromRow, Debug)]
    struct SearchResultRow {
        repository: String,
        commit_sha: String,
        file_path: String,
        start_line: i32,
        line_count: i32,
        content_text: String,
    }

    let results: Vec<SearchResultRow> = sqlx::query_as(
        r#"
        SELECT
            fc.repository,
            fc.commit_sha,
            fc.file_path,
            fc.start_line,
            fc.line_count,
            ts_headline('simple', safe_bytea_to_utf8(c.data), websearch_to_tsquery('simple', $1), 'StartSel=<mark>, StopSel=</mark>') as content_text
        FROM
            chunks c
        JOIN
            file_chunks fc ON c.hash = fc.chunk_hash
        WHERE
            c.content_tsv @@ websearch_to_tsquery('simple', $1)
        "#,
    )
    .bind(&query)
    .fetch_all(pool)
    .await
    .map_err(|e| ServerFnError::new(e.to_string()))?;

    let search_results = results
        .into_iter()
        .map(|row| SearchResult {
            repository: row.repository,
            commit_sha: row.commit_sha,
            file_path: row.file_path,
            start_line: row.start_line,
            end_line: row.start_line + row.line_count - 1,
            content_text: row.content_text,
        })
        .collect();

    Ok(search_results)
}
