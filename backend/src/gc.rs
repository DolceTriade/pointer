use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::{FromRow, PgPool};
use tracing::warn;

use crate::ApiErrorKind;

#[derive(Debug, Serialize, Default)]
pub struct GcOutcome {
    pub branches_evaluated: usize,
    pub snapshots_removed: usize,
    pub commits_pruned: usize,
}

pub struct GarbageCollector {
    pool: PgPool,
}

impl GarbageCollector {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn run_once(&self) -> Result<GcOutcome, ApiErrorKind> {
        let mut outcome = GcOutcome::default();

        let policies = sqlx::query_as!(
            BranchPolicyRow,
            r#"
            SELECT repository, branch, latest_keep_count
            FROM branch_policies
            "#
        )
        .fetch_all(&self.pool)
        .await
        .map_err(ApiErrorKind::from)?;

        if policies.is_empty() {
            return Ok(outcome);
        }

        let snapshot_policy_rows = sqlx::query_as!(
            SnapshotPolicyRow,
            r#"
            SELECT repository, branch, interval_seconds, keep_count
            FROM branch_snapshot_policies
            "#
        )
        .fetch_all(&self.pool)
        .await
        .map_err(ApiErrorKind::from)?;

        let mut snapshot_policy_map: HashMap<(String, String), Vec<PolicySpec>> = HashMap::new();
        for row in snapshot_policy_rows {
            if row.interval_seconds <= 0 || row.keep_count <= 0 {
                continue;
            }
            snapshot_policy_map
                .entry((row.repository.clone(), row.branch.clone()))
                .or_default()
                .push(PolicySpec {
                    interval_seconds: row.interval_seconds,
                    keep_count: row.keep_count,
                });
        }

        for policy in policies {
            let BranchPolicyRow {
                repository,
                branch,
                latest_keep_count,
            } = policy;

            let snapshots = sqlx::query_as!(
                BranchSnapshotRow,
                r#"
                SELECT commit_sha, indexed_at
                FROM branch_snapshots
                WHERE repository = $1 AND branch = $2
                ORDER BY indexed_at DESC
                "#,
                repository,
                branch
            )
            .fetch_all(&self.pool)
            .await
            .map_err(ApiErrorKind::from)?;

            if snapshots.is_empty() {
                continue;
            }

            let interval_specs = snapshot_policy_map
                .get(&(repository.clone(), branch.clone()))
                .cloned()
                .unwrap_or_default();
            let keep_set = compute_keep_set(&snapshots, latest_keep_count, &interval_specs);
            let mut removals = Vec::new();
            for snapshot in &snapshots {
                if !keep_set.contains(&snapshot.commit_sha) {
                    removals.push(snapshot.commit_sha.clone());
                }
            }

            outcome.branches_evaluated += 1;

            if removals.is_empty() {
                continue;
            }

            sqlx::query(
                "DELETE FROM branch_snapshots
                 WHERE repository = $1 AND branch = $2 AND commit_sha = ANY($3)",
            )
            .bind(&repository)
            .bind(&branch)
            .bind(&removals)
            .execute(&self.pool)
            .await
            .map_err(ApiErrorKind::from)?;

            outcome.snapshots_removed += removals.len();

            for commit in removals {
                if commit_is_protected(&self.pool, &repository, &commit).await? {
                    continue;
                }
                match prune_commit_data(&self.pool, &repository, &commit).await {
                    Ok(true) => outcome.commits_pruned += 1,
                    Ok(false) => {}
                    Err(err) => {
                        warn!(error = ?err, repo = %repository, commit = %commit, "failed to prune commit during GC")
                    }
                }
            }
        }

        Ok(outcome)
    }
}

fn compute_keep_set(
    snapshots: &[BranchSnapshotRow],
    latest_keep_count: i32,
    policies: &[PolicySpec],
) -> HashSet<String> {
    let mut keep = HashSet::new();
    let latest = latest_keep_count.max(1) as usize;

    for snapshot in snapshots.iter().take(latest) {
        keep.insert(snapshot.commit_sha.clone());
    }

    if policies.is_empty() {
        return keep;
    }

    let now = Utc::now();
    for spec in policies {
        if spec.interval_seconds <= 0 || spec.keep_count <= 0 {
            continue;
        }
        let mut buckets_kept = HashSet::new();
        for snapshot in snapshots {
            let elapsed = now.signed_duration_since(snapshot.indexed_at).num_seconds();
            let bucket = if elapsed <= 0 {
                0
            } else {
                elapsed / spec.interval_seconds
            };
            if bucket >= spec.keep_count as i64 {
                continue;
            }
            if buckets_kept.insert(bucket) {
                keep.insert(snapshot.commit_sha.clone());
                if buckets_kept.len() >= spec.keep_count as usize {
                    break;
                }
            }
        }
    }

    keep
}

async fn commit_is_protected(
    pool: &PgPool,
    repository: &str,
    commit_sha: &str,
) -> Result<bool, ApiErrorKind> {
    let has_snapshot: Option<(String,)> = sqlx::query_as(
        "SELECT commit_sha FROM branch_snapshots WHERE repository = $1 AND commit_sha = $2 LIMIT 1",
    )
    .bind(repository)
    .bind(commit_sha)
    .fetch_optional(pool)
    .await
    .map_err(ApiErrorKind::from)?;

    if has_snapshot.is_some() {
        return Ok(true);
    }

    let is_head: Option<(String,)> = sqlx::query_as(
        "SELECT commit_sha FROM branches WHERE repository = $1 AND commit_sha = $2 LIMIT 1",
    )
    .bind(repository)
    .bind(commit_sha)
    .fetch_optional(pool)
    .await
    .map_err(ApiErrorKind::from)?;

    Ok(is_head.is_some())
}

#[derive(FromRow)]
struct BranchPolicyRow {
    repository: String,
    branch: String,
    latest_keep_count: i32,
}

#[derive(FromRow, Clone)]
struct SnapshotPolicyRow {
    repository: String,
    branch: String,
    interval_seconds: i64,
    keep_count: i32,
}

#[derive(FromRow)]
struct BranchSnapshotRow {
    commit_sha: String,
    indexed_at: DateTime<Utc>,
}

#[derive(Clone)]
struct PolicySpec {
    interval_seconds: i64,
    keep_count: i32,
}

pub async fn is_latest_commit_on_any_branch(
    pool: &PgPool,
    repository: &str,
    commit_sha: &str,
) -> Result<bool, ApiErrorKind> {
    let result: Option<(String,)> =
        sqlx::query_as("SELECT commit_sha FROM branches WHERE repository = $1 AND commit_sha = $2")
            .bind(repository)
            .bind(commit_sha)
            .fetch_optional(pool)
            .await
            .map_err(ApiErrorKind::from)?;

    Ok(result.is_some())
}

pub async fn prune_commit_data(
    pool: &PgPool,
    repository: &str,
    commit_sha: &str,
) -> Result<bool, ApiErrorKind> {
    let mut tx = pool.begin().await.map_err(ApiErrorKind::from)?;

    let content_hashes: Vec<(String,)> = sqlx::query_as(
        "SELECT DISTINCT content_hash FROM files WHERE repository = $1 AND commit_sha = $2",
    )
    .bind(repository)
    .bind(commit_sha)
    .fetch_all(&mut *tx)
    .await
    .map_err(ApiErrorKind::from)?;

    let files_deleted_result =
        sqlx::query("DELETE FROM files WHERE repository = $1 AND commit_sha = $2")
            .bind(repository)
            .bind(commit_sha)
            .execute(&mut *tx)
            .await
            .map_err(ApiErrorKind::from)?;

    let files_deleted = files_deleted_result.rows_affected();

    if files_deleted == 0 {
        tx.commit().await.map_err(ApiErrorKind::from)?;
        return Ok(false);
    }

    let hash_refs: Vec<String> = content_hashes.into_iter().map(|(h,)| h).collect();

    if !hash_refs.is_empty() {
        let hashes_to_delete: Vec<String> = sqlx::query_as::<_, (String,)>(
            "SELECT hash FROM content_blobs WHERE hash = ANY($1)
             AND NOT EXISTS (
                SELECT 1 FROM files WHERE content_hash = hash
             )",
        )
        .bind(&hash_refs)
        .fetch_all(&mut *tx)
        .await
        .map_err(ApiErrorKind::from)?
        .into_iter()
        .map(|(hash,)| hash)
        .collect();

        if !hashes_to_delete.is_empty() {
            sqlx::query(
                "DELETE FROM symbol_references WHERE symbol_id IN (
                    SELECT id FROM symbols WHERE content_hash = ANY($1)
                )",
            )
            .bind(&hashes_to_delete)
            .execute(&mut *tx)
            .await
            .map_err(ApiErrorKind::from)?;

            sqlx::query("DELETE FROM symbols WHERE content_hash = ANY($1)")
                .bind(&hashes_to_delete)
                .execute(&mut *tx)
                .await
                .map_err(ApiErrorKind::from)?;

            sqlx::query("DELETE FROM content_blob_chunks WHERE content_hash = ANY($1)")
                .bind(&hashes_to_delete)
                .execute(&mut *tx)
                .await
                .map_err(ApiErrorKind::from)?;

            sqlx::query("DELETE FROM content_blobs WHERE hash = ANY($1)")
                .bind(&hashes_to_delete)
                .execute(&mut *tx)
                .await
                .map_err(ApiErrorKind::from)?;
        }
    }

    sqlx::query("DELETE FROM chunks WHERE ref_count = 0")
        .execute(&mut *tx)
        .await
        .map_err(ApiErrorKind::from)?;

    tx.commit().await.map_err(ApiErrorKind::from)?;

    Ok(files_deleted > 0)
}

pub async fn prune_repository_data(
    pool: &PgPool,
    repository: &str,
    batch_size: i64,
) -> Result<i64, ApiErrorKind> {
    let batch_size = batch_size.max(1);
    let mut total_deleted = 0_i64;

    {
        let mut tx = pool.begin().await.map_err(ApiErrorKind::from)?;
        let branches_deleted = sqlx::query("DELETE FROM branches WHERE repository = $1")
            .bind(repository)
            .execute(&mut *tx)
            .await
            .map_err(ApiErrorKind::from)?
            .rows_affected();

        let policies_deleted = sqlx::query("DELETE FROM branch_policies WHERE repository = $1")
            .bind(repository)
            .execute(&mut *tx)
            .await
            .map_err(ApiErrorKind::from)?
            .rows_affected();

        let live_deleted = sqlx::query("DELETE FROM repo_live_branches WHERE repository = $1")
            .bind(repository)
            .execute(&mut *tx)
            .await
            .map_err(ApiErrorKind::from)?
            .rows_affected();

        let snapshots_deleted = sqlx::query("DELETE FROM branch_snapshots WHERE repository = $1")
            .bind(repository)
            .execute(&mut *tx)
            .await
            .map_err(ApiErrorKind::from)?
            .rows_affected();

        total_deleted = total_deleted
            .saturating_add(branches_deleted as i64)
            .saturating_add(policies_deleted as i64)
            .saturating_add(live_deleted as i64)
            .saturating_add(snapshots_deleted as i64);

        tx.commit().await.map_err(ApiErrorKind::from)?;
    }

    loop {
        let mut tx = pool.begin().await.map_err(ApiErrorKind::from)?;
        let content_hashes: Vec<(String,)> = sqlx::query_as(
            "SELECT DISTINCT content_hash
             FROM files
             WHERE repository = $1
             LIMIT $2",
        )
        .bind(repository)
        .bind(batch_size)
        .fetch_all(&mut *tx)
        .await
        .map_err(ApiErrorKind::from)?;

        if content_hashes.is_empty() {
            tx.commit().await.map_err(ApiErrorKind::from)?;
            break;
        }

        let hash_refs: Vec<String> = content_hashes.into_iter().map(|(h,)| h).collect();

        let files_deleted = sqlx::query(
            "DELETE FROM files
             WHERE repository = $1
               AND content_hash = ANY($2)",
        )
        .bind(repository)
        .bind(&hash_refs)
        .execute(&mut *tx)
        .await
        .map_err(ApiErrorKind::from)?
        .rows_affected();

        total_deleted = total_deleted.saturating_add(files_deleted as i64);

        let hashes_to_delete: Vec<String> = sqlx::query_as::<_, (String,)>(
            "SELECT hash FROM content_blobs WHERE hash = ANY($1)
             AND NOT EXISTS (
                SELECT 1 FROM files WHERE content_hash = hash
             )",
        )
        .bind(&hash_refs)
        .fetch_all(&mut *tx)
        .await
        .map_err(ApiErrorKind::from)?
        .into_iter()
        .map(|(hash,)| hash)
        .collect();

        if !hashes_to_delete.is_empty() {
            sqlx::query(
                "DELETE FROM symbol_references WHERE symbol_id IN (
                    SELECT id FROM symbols WHERE content_hash = ANY($1)
                )",
            )
            .bind(&hashes_to_delete)
            .execute(&mut *tx)
            .await
            .map_err(ApiErrorKind::from)?;

            sqlx::query("DELETE FROM symbols WHERE content_hash = ANY($1)")
                .bind(&hashes_to_delete)
                .execute(&mut *tx)
                .await
                .map_err(ApiErrorKind::from)?;

            sqlx::query("DELETE FROM content_blob_chunks WHERE content_hash = ANY($1)")
                .bind(&hashes_to_delete)
                .execute(&mut *tx)
                .await
                .map_err(ApiErrorKind::from)?;

            sqlx::query("DELETE FROM content_blobs WHERE hash = ANY($1)")
                .bind(&hashes_to_delete)
                .execute(&mut *tx)
                .await
                .map_err(ApiErrorKind::from)?;
        }

        tx.commit().await.map_err(ApiErrorKind::from)?;
    }

    {
        let mut tx = pool.begin().await.map_err(ApiErrorKind::from)?;
        sqlx::query("DELETE FROM chunks WHERE ref_count = 0")
            .execute(&mut *tx)
            .await
            .map_err(ApiErrorKind::from)?;
        tx.commit().await.map_err(ApiErrorKind::from)?;
    }

    Ok(total_deleted)
}
