use std::io::Write;

use anyhow::{anyhow, Context, Result};
use reqwest::blocking::Client;
use reqwest::header::{AUTHORIZATION, CONTENT_ENCODING, CONTENT_TYPE};
use tracing::info;
use zstd::stream::Encoder;

use crate::models::IndexReport;

pub fn upload_report(url: &str, api_key: Option<&str>, report: &IndexReport) -> Result<()> {
    let json = serde_json::to_vec(report).context("failed to serialize index report")?;

    let mut encoder = Encoder::new(Vec::new(), 0)?;
    encoder
        .write_all(&json)
        .context("failed to compress index payload")?;
    let compressed = encoder.finish().context("failed to finalize compression")?;

    let client = Client::new();
    let mut request = client
        .post(url)
        .header(CONTENT_TYPE, "application/json")
        .header(CONTENT_ENCODING, "zstd")
        .body(compressed);

    if let Some(key) = api_key {
        request = request.header(AUTHORIZATION, format!("Bearer {}", key));
    }

    let response = request.send().context("failed to upload index payload")?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().unwrap_or_default();
        return Err(anyhow!("upload failed with status {status}: {body}"));
    }

    info!("index upload complete");
    Ok(())
}
