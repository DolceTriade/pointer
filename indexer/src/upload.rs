use std::collections::HashSet;
use std::io::Write;
use std::time::Duration;

use anyhow::{Context, Result};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use reqwest::blocking::{Client, Response};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use tracing::info;
use uuid::Uuid;
use zstd::stream::Encoder;

use crate::models::{ChunkMapping, IndexArtifacts, IndexReport, UniqueChunk};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(600);
const MANIFEST_CHUNK_SIZE: usize = 8 * 1024 * 1024; // 8 MiB

pub fn upload_index(url: &str, api_key: Option<&str>, artifacts: &IndexArtifacts) -> Result<()> {
    let client = Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .build()
        .context("failed to build HTTP client")?;

    let endpoints = Endpoints::new(url);

    // 1. Upload all content blob metadata
    upload_content_blobs(
        &client,
        &endpoints,
        api_key,
        &artifacts.report.content_blobs,
    )?;

    // 2. Check which unique chunks the server needs
    let needed_chunk_hashes =
        request_needed_chunks(&client, &endpoints, api_key, &artifacts.unique_chunks)?;

    // 3. Upload the content of the needed chunks
    if !needed_chunk_hashes.is_empty() {
        upload_unique_chunks(
            &client,
            &endpoints,
            api_key,
            &artifacts.unique_chunks,
            &needed_chunk_hashes,
        )?;
    } else {
        info!("no new chunk content to upload");
    }

    // 4. Upload the mappings for how chunks belong to files
    upload_chunk_mappings(&client, &endpoints, api_key, &artifacts.chunk_mappings)?;

    // 5. Upload the final index manifest
    info!("uploading index report");
    upload_manifest(&client, &endpoints, api_key, &artifacts.report)?;
    info!("index manifest uploaded");

    Ok(())
}

struct Endpoints {
    blobs_upload: String,
    chunks_need: String,
    chunks_upload: String,
    mappings_upload: String,
    manifest_chunk: String,
    manifest_finalize: String,
}

impl Endpoints {
    fn new(base: &str) -> Self {
        let trimmed = base.trim_end_matches('/');
        Self {
            blobs_upload: format!("{}/blobs/upload", trimmed),
            chunks_need: format!("{}/chunks/need", trimmed),
            chunks_upload: format!("{}/chunks/upload", trimmed),
            mappings_upload: format!("{}/mappings/upload", trimmed),
            manifest_chunk: format!("{}/manifest/chunk", trimmed),
            manifest_finalize: format!("{}/manifest/finalize", trimmed),
        }
    }
}

fn upload_content_blobs(
    client: &Client,
    endpoints: &Endpoints,
    api_key: Option<&str>,
    blobs: &[crate::models::ContentBlob],
) -> Result<()> {
    if blobs.is_empty() {
        return Ok(());
    }
    info!(count = blobs.len(), "uploading content blob metadata");
    for batch in blobs.chunks(1000) {
        let payload = ContentBlobUploadRequest {
            blobs: batch.to_vec(),
        };
        post_json(client, &endpoints.blobs_upload, api_key, &payload)?;
    }
    info!("content blob metadata uploaded");
    Ok(())
}

fn request_needed_chunks(
    client: &Client,
    endpoints: &Endpoints,
    api_key: Option<&str>,
    unique_chunks: &[UniqueChunk],
) -> Result<HashSet<String>> {
    if unique_chunks.is_empty() {
        return Ok(HashSet::new());
    }
    info!(count = unique_chunks.len(), "checking for needed chunks");

    let request = ChunkNeedRequest {
        hashes: unique_chunks.iter().map(|c| c.chunk_hash.clone()).collect(),
    };

    let response: ChunkNeedResponse = post_json(client, &endpoints.chunks_need, api_key, &request)?
        .json()
        .context("failed to deserialize chunk need response")?;

    info!(needed = response.missing.len(), "found chunks to upload");
    Ok(response.missing.into_iter().collect())
}

fn upload_unique_chunks(
    client: &Client,
    endpoints: &Endpoints,
    api_key: Option<&str>,
    unique_chunks: &[UniqueChunk],
    needed_hashes: &HashSet<String>,
) -> Result<()> {
    let needed_chunks: Vec<&UniqueChunk> = unique_chunks
        .iter()
        .filter(|c| needed_hashes.contains(&c.chunk_hash))
        .collect();

    if needed_chunks.is_empty() {
        return Ok(());
    }

    info!(
        count = needed_chunks.len(),
        "uploading unique chunk content"
    );
    for batch in needed_chunks.chunks(100) {
        // Chunks can be large, use a smaller batch size
        let payload = UniqueChunkUploadRequest {
            chunks: batch.iter().map(|c| (*c).clone()).collect(),
        };
        post_json(client, &endpoints.chunks_upload, api_key, &payload)?;
    }
    info!("unique chunk content uploaded");

    Ok(())
}

fn upload_chunk_mappings(
    client: &Client,
    endpoints: &Endpoints,
    api_key: Option<&str>,
    mappings: &[ChunkMapping],
) -> Result<()> {
    if mappings.is_empty() {
        return Ok(());
    }
    info!(count = mappings.len(), "uploading chunk mappings");
    for batch in mappings.chunks(1000) {
        let payload = ChunkMappingUploadRequest {
            mappings: batch.to_vec(),
        };
        post_json(client, &endpoints.mappings_upload, api_key, &payload)?;
    }
    info!("chunk mappings uploaded");
    Ok(())
}

fn upload_manifest(
    client: &Client,
    endpoints: &Endpoints,
    api_key: Option<&str>,
    report: &IndexReport,
) -> Result<()> {
    let json = serde_json::to_vec(report).context("failed to serialize index report")?;

    let mut encoder = Encoder::new(Vec::new(), 0)?;
    encoder
        .write_all(&json)
        .context("failed to compress manifest")?;
    let compressed = encoder
        .finish()
        .context("failed to finalize manifest compression")?;

    let upload_id = Uuid::new_v4().to_string();
    let total_chunks = if compressed.is_empty() {
        1
    } else {
        ((compressed.len() + MANIFEST_CHUNK_SIZE - 1) / MANIFEST_CHUNK_SIZE) as u32
    };

    for (index, chunk) in compressed.chunks(MANIFEST_CHUNK_SIZE).enumerate() {
        let payload = ManifestChunkRequest {
            upload_id: upload_id.clone(),
            chunk_index: index as u32,
            total_chunks,
            data: BASE64.encode(chunk),
        };

        post_json(client, &endpoints.manifest_chunk, api_key, &payload)?;
        info!(
            chunk = index + 1,
            total = total_chunks,
            "uploaded manifest chunk"
        );
    }

    let finalize = ManifestFinalizeRequest {
        upload_id,
        compressed: true,
    };
    post_json(client, &endpoints.manifest_finalize, api_key, &finalize)?;
    Ok(())
}

fn post_json<T: Serialize>(
    client: &Client,
    url: &str,
    api_key: Option<&str>,
    body: &T,
) -> Result<Response> {
    let mut request = client
        .post(url)
        .header(CONTENT_TYPE, "application/json")
        .json(body);

    if let Some(key) = api_key {
        request = request.header(AUTHORIZATION, format!("Bearer {}", key));
    }

    let response = request
        .send()
        .with_context(|| format!("failed request to {}", url))?;
    if !response.status().is_success() {
        let status = response.status();
        let message = response.text().unwrap_or_default();
        anyhow::bail!("request to {url} failed with status {status}: {message}");
    }

    Ok(response)
}

#[derive(Serialize)]
struct ContentBlobUploadRequest {
    blobs: Vec<crate::models::ContentBlob>,
}

#[derive(Serialize)]
struct ChunkNeedRequest {
    hashes: Vec<String>,
}

#[derive(Deserialize)]
struct ChunkNeedResponse {
    missing: Vec<String>,
}

#[derive(Serialize)]
struct UniqueChunkUploadRequest {
    chunks: Vec<UniqueChunk>,
}

#[derive(Serialize)]
struct ChunkMappingUploadRequest {
    mappings: Vec<ChunkMapping>,
}

#[derive(Serialize)]
struct ManifestChunkRequest {
    upload_id: String,
    chunk_index: u32,
    total_chunks: u32,
    data: String,
}

#[derive(Serialize)]
struct ManifestFinalizeRequest {
    upload_id: String,
    compressed: bool,
}
