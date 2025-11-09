use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct SnapshotPolicyConfig {
    pub interval_seconds: u64,
    pub keep_count: u32,
}

#[derive(Debug, Clone)]
pub struct BranchPolicyConfig {
    pub live: Option<bool>,
    pub latest_keep_count: u32,
    pub snapshot_policies: Vec<SnapshotPolicyConfig>,
}

#[derive(Debug, Clone)]
pub struct IndexerConfig {
    pub repo_path: PathBuf,
    pub repository: String,
    pub branch: Option<String>,
    pub commit: String,
    pub output_dir: PathBuf,
    pub branch_policy: Option<BranchPolicyConfig>,
}

impl IndexerConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        repo_path: PathBuf,
        repository: String,
        branch: Option<String>,
        commit: String,
        output_dir: PathBuf,
        branch_policy: Option<BranchPolicyConfig>,
    ) -> Self {
        Self {
            repo_path,
            repository,
            branch,
            commit,
            output_dir,
            branch_policy,
        }
    }
}
