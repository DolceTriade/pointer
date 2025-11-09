use chrono::Utc;
use leptos::either::EitherOf3;
use leptos::prelude::*;
use leptos_router::components::A;
use leptos_router::hooks::use_params;
use leptos_router::params::Params;
use serde::{Deserialize, Serialize};

#[derive(Params, Debug, PartialEq)]
struct RepoParams {
    repo: String,
}

const MAX_VISIBLE_BRANCHES: usize = 12;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RepoBranchDisplay {
    pub name: String,
    pub commit_sha: String,
    pub indexed_at: Option<String>,
    pub is_live: bool,
}

#[server]
pub async fn get_repo_branches(repo: String) -> Result<Vec<RepoBranchDisplay>, ServerFnError> {
    use crate::db::{Database, postgres::PostgresDb};

    let state = expect_context::<crate::server::GlobalAppState>();
    let state = state.lock().await;
    let db = PostgresDb::new(state.pool.clone());

    let branches = db
        .get_branches_for_repository(&repo)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;

    Ok(branches
        .into_iter()
        .map(|branch| RepoBranchDisplay {
            name: branch.name,
            commit_sha: branch.commit_sha,
            indexed_at: branch.indexed_at,
            is_live: branch.is_live,
        })
        .collect())
}

#[component]
pub fn RepoDetailPage() -> impl IntoView {
    let params = use_params::<RepoParams>();
    let repo_name = move || {
        params.with(|p| match p {
            Ok(params) => params.repo.clone(),
            Err(_) => "Unknown Repository".to_string(),
        })
    };

    let (show_all_branches, set_show_all_branches) = signal(false);
    let branches = Resource::new(repo_name, |repo| get_repo_branches(repo));

    view! {
        <main class="flex-grow flex flex-col items-center justify-start pt-8 p-4 text-slate-900 dark:text-slate-100">
            <div class="w-full max-w-3xl">
                <h1 class="text-2xl font-semibold text-slate-900 dark:text-slate-100">
                    {move || repo_name()}
                </h1>
                <p class="mt-2 text-sm text-slate-600 dark:text-slate-300">
                    "Pick a branch to browse files and code insights."
                </p>

                <Suspense fallback=move || {
                    view! {
                        <p class="mt-6 text-sm text-slate-600 dark:text-slate-300">
                            "Loading branches..."
                        </p>
                    }
                }>
                    {move || {
                        branches
                            .get()
                            .map(|res| match res {
                                Err(e) => {
                                    EitherOf3::A(
                                        view! {
                                            <p class="mt-6 text-sm text-red-500 dark:text-red-300">
                                                "Error loading repository: " {e.to_string()}
                                            </p>
                                        },
                                    )
                                }
                                Ok(branches) if branches.is_empty() => {
                                    EitherOf3::B(
                                        view! {
                                            <p class="mt-6 text-sm text-slate-600 dark:text-slate-300">
                                                "This repository has no indexed branches."
                                            </p>
                                        },
                                    )
                                }
                                Ok(branches) => {
                                    let total = branches.len();
                                    let repo = repo_name();
                                    let show_all = show_all_branches();
                                    let visible: Vec<RepoBranchDisplay> = if show_all {
                                        branches.clone()
                                    } else {
                                        branches
                                            .iter()
                                            .take(MAX_VISIBLE_BRANCHES)
                                            .cloned()
                                            .collect()
                                    };
                                    let visible_count = visible.len();
                                    let has_more = total > MAX_VISIBLE_BRANCHES;
                                    EitherOf3::C(
                                        view! {
                                            <section class="mt-6">
                                                <header class="flex items-center justify-between">
                                                    <div>
                                                        <h2 class="text-lg font-semibold text-slate-900 dark:text-slate-100">
                                                            "Available branches"
                                                        </h2>
                                                        <p class="text-xs text-slate-600 dark:text-slate-300">
                                                            {format!("Showing {} of {} branches", visible_count, total)}
                                                        </p>
                                                    </div>
                                                    <span class="text-xs text-slate-500 dark:text-slate-300">
                                                        {format!("{} total", total)}
                                                    </span>
                                                </header>

                                                <div class="mt-4 border border-slate-200 dark:border-slate-800/80 rounded-lg bg-white/85 dark:bg-slate-900/60 shadow-lg backdrop-blur">
                                                    <ul class="divide-y divide-slate-200 dark:divide-slate-800 max-h-80 overflow-y-auto">
                                                        {visible
                                                            .into_iter()
                                                            .map(|branch| {
                                                                let href = format!("/repo/{}/tree/{}", repo, branch.name);
                                                                let short_commit: String = branch
                                                                    .commit_sha
                                                                    .chars()
                                                                    .take(7)
                                                                    .collect();
                                                                let live_badge = branch
                                                                    .is_live
                                                                    .then(|| {
                                                                        view! {
                                                                            <span class="inline-flex items-center rounded-full bg-emerald-200/70 text-emerald-900 dark:bg-emerald-900/60 dark:text-emerald-100 px-2 py-0.5 text-[11px] uppercase tracking-wide">
                                                                                "Live"
                                                                            </span>
                                                                        }
                                                                    });
                                                                let indexed_badge = branch
                                                                    .indexed_at
                                                                    .as_deref()
                                                                    .and_then(format_indexed_timestamp)
                                                                    .map(|label| {
                                                                        view! {
                                                                            <span class="inline-flex items-center rounded-full bg-slate-200 text-slate-800 dark:bg-slate-800/70 dark:text-slate-200 px-2 py-0.5 text-[11px]">
                                                                                {label}
                                                                            </span>
                                                                        }
                                                                    });
                                                                view! {
                                                                    <li class="last:border-b-0">
                                                                        <A
                                                                            href=href
                                                                            attr:class="flex items-center justify-between gap-3 px-4 py-3 text-left transition-colors text-slate-900 dark:text-slate-100 rounded-md hover:bg-slate-100/90 dark:hover:bg-slate-800/70 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-600 dark:focus-visible:outline-sky-400"
                                                                        >
                                                                            <div class="flex flex-col gap-1 min-w-0">
                                                                                <span class="font-mono text-sm text-slate-900 dark:text-slate-100 break-words">
                                                                                    {branch.name.clone()}
                                                                                </span>
                                                                                <div class="flex flex-wrap items-center gap-2 text-[11px] text-slate-600 dark:text-slate-300">
                                                                                    <span>{format!("Head {}", short_commit)}</span>
                                                                                    {live_badge}
                                                                                    {indexed_badge}
                                                                                </div>
                                                                            </div>
                                                                            <span class="text-xs text-slate-600 dark:text-slate-200">
                                                                                "Open"
                                                                            </span>
                                                                        </A>
                                                                    </li>
                                                                }
                                                            })
                                                            .collect_view()}
                                                    </ul>
                                                </div>

                                                {if has_more {
                                                    let set_show_all = set_show_all_branches.clone();
                                                    Some(
                                                        view! {
                                                            <button
                                                                type="button"
                                                                class="mt-4 text-sm font-medium text-sky-600 dark:text-sky-400 hover:text-sky-500 dark:hover:text-sky-300 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-600 dark:focus-visible:outline-sky-400"
                                                                on:click=move |_| {
                                                                    set_show_all.update(|value| *value = !*value)
                                                                }
                                                            >
                                                                {if show_all {
                                                                    "Show fewer branches".to_string()
                                                                } else {
                                                                    format!("Show all {} branches", total)
                                                                }}
                                                            </button>
                                                        },
                                                    )
                                                } else {
                                                    None
                                                }}

                                                {if !show_all && total > visible_count {
                                                    Some(
                                                        view! {
                                                            <p class="mt-2 text-xs text-slate-600 dark:text-slate-400">
                                                                {format!(
                                                                    "Showing the first {} branches. Use the button above to see the rest.",
                                                                    MAX_VISIBLE_BRANCHES,
                                                                )}
                                                            </p>
                                                        },
                                                    )
                                                } else {
                                                    None
                                                }}
                                            </section>
                                        },
                                    )
                                }
                            })
                    }}
                </Suspense>
            </div>
        </main>
    }
}

fn format_indexed_timestamp(ts: &str) -> Option<String> {
    chrono::DateTime::parse_from_rfc3339(ts).ok().map(|dt| {
        dt.with_timezone(&Utc)
            .format("Indexed %Y-%m-%d %H:%M UTC")
            .to_string()
    })
}
