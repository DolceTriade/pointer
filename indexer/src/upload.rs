use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::time::Duration;

use anyhow::{Context, Result};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use reqwest::blocking::{Client, Response};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};
use uuid::Uuid;
use zstd::stream::Encoder;

use crate::models::{ChunkPayload, IndexArtifacts, IndexReport};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(600);
const CHUNK_UPLOAD_BATCH: usize = 128;
const MANIFEST_CHUNK_SIZE: usize = 8 * 1024 * 1024; // 8 MiB

pub fn upload_index(url: &str, api_key: Option<&str>, artifacts: &IndexArtifacts) -> Result<()> {
    let client = Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .build()
        .context("failed to build HTTP client")?;

    let endpoints = Endpoints::new(url);

    let needed = request_missing_chunks(&client, &endpoints, api_key, &artifacts.report)?;
    info!(missing = needed.len(), "chunk diff computed");

    if !needed.is_empty() {
        upload_chunks(&client, &endpoints, api_key, artifacts, &needed)?;
    } else {
        info!("no new chunks to upload");
    }

    upload_manifest(&client, &endpoints, api_key, &artifacts.report)?;
    info!("index manifest uploaded");

    Ok(())
}

struct Endpoints {
    need: String,
    upload: String,
    manifest_chunk: String,
    manifest_finalize: String,
}

impl Endpoints {
    fn new(base: &str) -> Self {
        let trimmed = base.trim_end_matches('/');
        Self {
            need: format!("{}/chunks/need", trimmed),
            upload: format!("{}/chunks/upload", trimmed),
            manifest_chunk: format!("{}/manifest/chunk", trimmed),
            manifest_finalize: format!("{}/manifest/finalize", trimmed),
        }
    }
}

fn request_missing_chunks(
    client: &Client,
    endpoints: &Endpoints,
    api_key: Option<&str>,
    report: &IndexReport,
) -> Result<HashSet<String>> {
    if report.chunk_descriptors.is_empty() {
        return Ok(HashSet::new());
    }

    let request = ChunkNeedRequest {
        chunks: &report.chunk_descriptors,
    };

    let response: ChunkNeedResponse = post_json(client, &endpoints.need, api_key, &request)?
        .json()
        .context("failed to deserialize chunk diff response")?;

    Ok(response.missing.into_iter().collect())
}

fn upload_chunks(
    client: &Client,
    endpoints: &Endpoints,
    api_key: Option<&str>,
    artifacts: &IndexArtifacts,
    needed: &HashSet<String>,
) -> Result<()> {
    let chunk_map: HashMap<&str, &ChunkPayload> = artifacts
        .chunks
        .iter()
        .map(|chunk| (chunk.hash.as_str(), chunk))
        .collect();

    let mut required: Vec<&ChunkPayload> = Vec::new();
    for hash in needed {
        match chunk_map.get(hash.as_str()) {
            Some(chunk) => required.push(*chunk),
            None => warn!(hash = %hash, "referenced chunk missing from artifacts"),
        }
    }
    required.sort_by(|a, b| a.hash.cmp(&b.hash));

    for (batch_index, batch) in required.chunks(CHUNK_UPLOAD_BATCH).enumerate() {
        let payload = ChunkUploadRequest {
            chunks: batch
                .iter()
                .map(|chunk| ChunkUploadItem {
                    hash: chunk.hash.clone(),
                    algorithm: chunk.algorithm.clone(),
                    byte_len: chunk.data.len() as u32,
                    data: BASE64.encode(&chunk.data),
                })
                .collect(),
        };

        post_json(client, &endpoints.upload, api_key, &payload)?;
        info!(
            batch = batch_index + 1,
            total = (required.len() + CHUNK_UPLOAD_BATCH - 1) / CHUNK_UPLOAD_BATCH,
            "uploaded chunk batch"
        );
    }

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
struct ChunkNeedRequest<'a> {
    chunks: &'a [crate::models::ChunkDescriptor],
}

#[derive(Deserialize)]
struct ChunkNeedResponse {
    missing: Vec<String>,
}

#[derive(Serialize)]
struct ChunkUploadRequest {
    chunks: Vec<ChunkUploadItem>,
}

#[derive(Serialize)]
struct ChunkUploadItem {
    hash: String,
    algorithm: String,
    byte_len: u32,
    data: String,
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
