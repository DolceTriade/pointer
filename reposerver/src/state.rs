use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;

use anyhow::{Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tracing::info;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PersistedState {
    #[serde(default)]
    pub branches: HashMap<String, BranchState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BranchState {
    pub last_indexed_commit: String,
    pub last_success_at: String,
}

impl PersistedState {
    pub fn load(path: &Path) -> Result<Self> {
        let start = Instant::now();
        info!(
            stage = "state",
            event = "state.load.begin",
            path = %path.display(),
            "loading persisted state"
        );

        if !path.exists() {
            info!(
                stage = "state",
                event = "state.load.end",
                result = "ok",
                path = %path.display(),
                branch_state_count = 0usize,
                duration_ms = start.elapsed().as_millis(),
                "state file does not exist, using empty state"
            );
            return Ok(Self::default());
        }

        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read state file {}", path.display()))?;

        let state: Self = serde_json::from_str(&raw)
            .with_context(|| format!("failed to parse state file {}", path.display()))?;

        info!(
            stage = "state",
            event = "state.load.end",
            result = "ok",
            path = %path.display(),
            branch_state_count = state.branches.len(),
            duration_ms = start.elapsed().as_millis(),
            "loaded persisted state"
        );

        Ok(state)
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        let start = Instant::now();
        info!(
            stage = "state",
            event = "state.save.begin",
            path = %path.display(),
            branch_state_count = self.branches.len(),
            "saving persisted state"
        );

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("failed to create state directory {}", parent.display())
            })?;
        }

        let tmp_path = path.with_extension("json.tmp");
        let raw = serde_json::to_vec_pretty(self).context("failed to serialize state")?;

        std::fs::write(&tmp_path, raw)
            .with_context(|| format!("failed to write temp state {}", tmp_path.display()))?;

        std::fs::rename(&tmp_path, path).with_context(|| {
            format!(
                "failed to move temp state {} to {}",
                tmp_path.display(),
                path.display()
            )
        })?;

        info!(
            stage = "state",
            event = "state.save.end",
            result = "ok",
            path = %path.display(),
            branch_state_count = self.branches.len(),
            duration_ms = start.elapsed().as_millis(),
            "saved persisted state"
        );

        Ok(())
    }

    pub fn key(repo: &str, branch: &str) -> String {
        format!("{repo}::{branch}")
    }

    pub fn has_commit(&self, repo: &str, branch: &str, commit: &str) -> bool {
        let key = Self::key(repo, branch);
        self.branches
            .get(&key)
            .map(|entry| entry.last_indexed_commit == commit)
            .unwrap_or(false)
    }

    pub fn update_success(&mut self, repo: &str, branch: &str, commit: &str) {
        let key = Self::key(repo, branch);
        self.branches.insert(
            key,
            BranchState {
                last_indexed_commit: commit.to_string(),
                last_success_at: Utc::now().to_rfc3339(),
            },
        );
    }
}
