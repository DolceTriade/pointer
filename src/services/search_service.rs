use leptos::prelude::*;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct SearchResult {
    pub repository: String,
    pub commit_sha: String,
    pub file_path: String,
    pub start_line: i32,
    pub end_line: i32,
    pub match_line: i32,  // The actual line where the match occurs
    pub content_text: String,
}

#[server]
pub async fn search(query: String) -> Result<Vec<SearchResult>, ServerFnError> {
    let state = expect_context::<crate::server::GlobalAppState>();
    let state = state.lock().await;
    let pool = &state.pool;

    // Use basic search with trigram support
    // The DSL parsing is available but not yet fully integrated
    basic_search(pool, &query).await
}

#[cfg(feature = "ssr")]
async fn basic_search(pool: &sqlx::PgPool, query: &str) -> Result<Vec<SearchResult>, ServerFnError> {
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
            OR c.data_text % $1  -- Using pg_trgm fuzzy matching on pre-computed text column
        "#,
    )
    .bind(query)
    .fetch_all(pool)
    .await
    .map_err(|e| ServerFnError::new(e.to_string()))?;

    let search_results = results
        .into_iter()
        .map(|row| {
            // Find the first line in the chunk that contains a highlighted match
            let highlighted_lines: Vec<&str> = row.content_text.lines().collect();
            
            // Find the first line that contains <mark> tags (highlighted content)
            let mut match_line_offset = 0;
            for (i, line) in highlighted_lines.iter().enumerate() {
                if line.contains("<mark>") {
                    match_line_offset = i as i32;
                    break;
                }
            }
            
            // Calculate the actual line number in the file
            let actual_match_line = row.start_line + match_line_offset;
            
            SearchResult {
                repository: row.repository,
                commit_sha: row.commit_sha,
                file_path: row.file_path,
                start_line: row.start_line,
                end_line: row.start_line + row.line_count - 1,
                match_line: actual_match_line,
                content_text: row.content_text,
            }
        })
        .collect();

    Ok(search_results)
}