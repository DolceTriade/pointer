use std::cmp::min;
use std::io::Write;
use std::time::Duration;

use anyhow::{Context, Result};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use reqwest::blocking::Client;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use serde::Serialize;
use tracing::info;
use uuid::Uuid;
use zstd::stream::Encoder;

use crate::models::IndexReport;

const CHUNK_SIZE: usize = 8 * 1024 * 1024; // 8 MiB
const CHUNK_TIMEOUT: Duration = Duration::from_secs(600);
const FINALIZE_TIMEOUT: Duration = Duration::from_secs(3600);

#[derive(Serialize)]
struct ChunkUpload {
    upload_id: String,
    chunk_index: u32,
    total_chunks: u32,
    data: String,
}

#[derive(Serialize)]
struct FinalizeUpload {
    upload_id: String,
}

pub fn upload_report(url: &str, api_key: Option<&str>, report: &IndexReport) -> Result<()> {
    let json = serde_json::to_vec(report).context("failed to serialize index report")?;

    let mut encoder = Encoder::new(Vec::new(), 0)?;
    encoder
        .write_all(&json)
        .context("failed to compress index payload")?;
    let compressed = encoder.finish().context("failed to finalize compression")?;

    let (chunk_endpoint, finalize_endpoint) = build_chunk_endpoints(url);

    let upload_id = Uuid::new_v4().to_string();
    let total_chunks = if compressed.is_empty() {
        1
    } else {
        ((compressed.len() + CHUNK_SIZE - 1) / CHUNK_SIZE) as u32
    };

    let client = Client::builder()
        .timeout(CHUNK_TIMEOUT)
        .build()
        .context("failed to build HTTP client")?;

    for index in 0..total_chunks {
        let start = (index as usize) * CHUNK_SIZE;
        let end = min(start + CHUNK_SIZE, compressed.len());
        let slice = if start < end {
            &compressed[start..end]
        } else {
            &compressed[compressed.len()..compressed.len()]
        };
        let payload = ChunkUpload {
            upload_id: upload_id.clone(),
            chunk_index: index,
            total_chunks,
            data: BASE64.encode(slice),
        };

        send_json(&client, &chunk_endpoint, api_key, &payload, CHUNK_TIMEOUT)
            .with_context(|| format!("failed to upload chunk {}/{}", index + 1, total_chunks))?;

        info!(chunk = index + 1, total = total_chunks, "uploaded chunk");
    }

    let finalize_payload = FinalizeUpload {
        upload_id: upload_id.clone(),
    };

    send_json(
        &client,
        &finalize_endpoint,
        api_key,
        &finalize_payload,
        FINALIZE_TIMEOUT,
    )
    .context("failed to finalize chunked upload")?;

    info!("index upload complete");
    Ok(())
}

fn build_chunk_endpoints(base: &str) -> (String, String) {
    let trimmed = base.trim_end_matches('/');
    (
        format!("{}/chunk", trimmed),
        format!("{}/finalize", trimmed),
    )
}

fn send_json<T: ?Sized + Serialize>(
    client: &Client,
    url: &str,
    api_key: Option<&str>,
    body: &T,
    timeout: Duration,
) -> Result<()> {
    let mut request = client
        .post(url)
        .timeout(timeout)
        .header(CONTENT_TYPE, "application/json")
        .json(body);

    if let Some(key) = api_key {
        request = request.header(AUTHORIZATION, format!("Bearer {}", key));
    }

    let response = request.send()?;
    if !response.status().is_success() {
        let status = response.status();
        let message = response.text().unwrap_or_default();
        anyhow::bail!("request to {url} failed with status {status}: {message}");
    }

    Ok(())
}
