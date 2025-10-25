use leptos::either::EitherOf3;
use leptos::prelude::*;
use leptos_router::components::A;
use leptos_router::hooks::use_params;
use leptos_router::params::Params;

#[derive(Params, Debug, PartialEq)]
struct RepoParams {
    repo: String,
}

const MAX_VISIBLE_BRANCHES: usize = 12;

#[server]
pub async fn get_repo_branches(repo: String) -> Result<Vec<String>, ServerFnError> {
    use crate::db::{Database, postgres::PostgresDb};

    let state = expect_context::<crate::server::GlobalAppState>();
    let state = state.lock().await;
    let db = PostgresDb::new(state.pool.clone());

    let branches = db
        .get_branches_for_repository(&repo)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;

    Ok(branches)
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
        <main class="flex-grow flex flex-col items-center justify-start pt-8 p-4">
            <div class="w-full max-w-3xl">
                <h1 class="text-2xl font-semibold text-slate-100">{move || repo_name()}</h1>
                <p class="mt-2 text-sm text-slate-400">
                    "Pick a branch to browse files and code insights."
                </p>

                <Suspense fallback=move || {
                    view! { <p class="mt-6 text-sm text-slate-400">"Loading branches..."</p> }
                }>
                    {move || {
                        branches
                            .get()
                            .map(|res| match res {
                                Err(e) => {
                                    EitherOf3::A(
                                        view! {
                                            <p class="mt-6 text-sm text-red-400">
                                                "Error loading repository: " {e.to_string()}
                                            </p>
                                        },
                                    )
                                }
                                Ok(branches) if branches.is_empty() => {
                                    EitherOf3::B(
                                        view! {
                                            <p class="mt-6 text-sm text-slate-400">
                                                "This repository has no indexed branches."
                                            </p>
                                        },
                                    )
                                }
                                Ok(branches) => {
                                    let total = branches.len();
                                    let repo = repo_name();
                                    let show_all = show_all_branches();
                                    let visible: Vec<String> = if show_all {
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
                                                        <h2 class="text-lg font-semibold text-slate-100">
                                                            "Available branches"
                                                        </h2>
                                                        <p class="text-xs text-slate-400">
                                                            {format!("Showing {} of {} branches", visible_count, total)}
                                                        </p>
                                                    </div>
                                                    <span class="text-xs text-slate-500">
                                                        {format!("{} total", total)}
                                                    </span>
                                                </header>

                                                <div class="mt-4 border border-slate-800/80 rounded-lg bg-slate-900/60 shadow-lg">
                                                    <ul class="divide-y divide-slate-800 max-h-80 overflow-y-auto">
                                                        {visible
                                                            .into_iter()
                                                            .map(|branch| {
                                                                let href = format!("/repo/{}/tree/{}", repo, &branch);
                                                                view! {
                                                                    <li class="last:border-b-0">
                                                                        <A
                                                                            href=href
                                                                            attr:class="flex items-center justify-between gap-3 px-4 py-3 text-left transition-colors hover:bg-slate-800 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-slate-500"
                                                                        >
                                                                            <span class="font-mono text-sm text-slate-100 break-words">
                                                                                {branch.clone()}
                                                                            </span>
                                                                            <span class="text-xs text-slate-400">"Open"</span>
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
                                                                class="mt-4 text-sm font-medium text-sky-400 hover:text-sky-300 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-500"
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
                                                            <p class="mt-2 text-xs text-slate-500">
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
