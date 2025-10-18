use crate::db::models::{SearchResult, SearchResultsPage, SearchSnippet};
use crate::dsl::DEFAULT_PAGE_SIZE;
use crate::services::search_service::search;
use leptos::Params;
use leptos::either::EitherOf3;
use leptos::prelude::*;
use leptos_router::hooks::use_navigate;
use leptos_router::hooks::use_query;
use leptos_router::params::Params;

#[derive(Params, PartialEq, Clone, Debug)]
pub struct SearchParams {
    pub q: Option<String>,
    pub page: Option<usize>,
}

#[component]
pub fn SearchPage() -> impl IntoView {
    let query = use_query::<SearchParams>();
    let navigate = use_navigate();

    let search_results = Resource::new(query, |q| async move {
        match q {
            Ok(params) => {
                let page = params.page.unwrap_or(1).max(1);
                let search_text = params.q.clone().unwrap_or_default();
                if search_text.is_empty() {
                    return Ok(SearchResultsPage::empty(
                        search_text,
                        page as u32,
                        DEFAULT_PAGE_SIZE,
                    ));
                }
                search(search_text, page as u32).await
            }
            Err(_) => Ok(SearchResultsPage::empty(
                String::new(),
                1,
                DEFAULT_PAGE_SIZE,
            )),
        }
    });

    view! {
        <div class="w-full max-w-6xl mx-auto px-4 py-8 text-black dark:text-white">
            <div class="overflow-y-auto max-w-full">
                <Suspense fallback=|| {
                    view! { <p class="text-center py-8">"Loading..."</p> }
                }>
                    {move || {
                        search_results
                            .get()
                            .map(|res| {
                                match res {
                                    Ok(results_page) => {
                                        if results_page.results.is_empty() {
                                            EitherOf3::A(
                                                view! {
                                                    <p class="text-center py-8">"No results found."</p>
                                                },
                                            )
                                        } else {
                                            let page = results_page.page as usize;
                                            let has_more = results_page.has_more;
                                            let current_query = results_page.query.clone();
                                            let prev_page = page.saturating_sub(1).max(1);
                                            let next_page = page + 1;
                                            EitherOf3::B(

                                                view! {
                                                    <div>
                                                        <p class="text-sm text-gray-600 dark:text-gray-400 mb-2">
                                                            {format!(
                                                                "Showing page {} ({} results per page)",
                                                                page,
                                                                results_page.page_size,
                                                            )}
                                                        </p>
                                                        {results_page
                                                            .results
                                                            .into_iter()
                                                            .map(|result| view! { <SearchResultCard result=result /> })
                                                            .collect_view()}
                                                        <div class="flex items-center justify-between mt-6">
                                                            <button
                                                                class="px-4 py-2 rounded bg-gray-200 dark:bg-gray-700 hover:bg-gray-300 dark:hover:bg-gray-600 disabled:opacity-50 disabled:cursor-not-allowed"
                                                                disabled=move || page <= 1
                                                                on:click={
                                                                    let navigate = navigate.clone();
                                                                    let query_string = current_query.clone();
                                                                    move |_| {
                                                                        if page > 1 {
                                                                            navigate(
                                                                                &format!("/search?q={}&page={}", query_string, prev_page),
                                                                                Default::default(),
                                                                            );
                                                                        }
                                                                    }
                                                                }
                                                            >
                                                                "Previous"
                                                            </button>
                                                            <span class="text-sm text-gray-600 dark:text-gray-400">
                                                                {format!("Page {}", page)}
                                                            </span>
                                                            <button
                                                                class="px-4 py-2 rounded bg-gray-200 dark:bg-gray-700 hover:bg-gray-300 dark:hover:bg-gray-600 disabled:opacity-50 disabled:cursor-not-allowed"
                                                                disabled=move || !has_more
                                                                on:click={
                                                                    let navigate = navigate.clone();
                                                                    let query_string = current_query.clone();
                                                                    move |_| {
                                                                        if has_more {
                                                                            navigate(
                                                                                &format!("/search?q={}&page={}", query_string, next_page),
                                                                                Default::default(),
                                                                            );
                                                                        }
                                                                    }
                                                                }
                                                            >
                                                                "Next"
                                                            </button>
                                                        </div>
                                                    </div>
                                                },
                                            )
                                        }
                                    }
                                    Err(e) => {
                                        EitherOf3::C(
                                            view! {
                                                <p class="text-red-500 text-center py-8">
                                                    "Error: " {e.to_string()}
                                                </p>
                                            },
                                        )
                                    }
                                }
                            })
                    }}
                </Suspense>
            </div>
        </div>
    }
}

#[component]
fn SearchResultCard(result: SearchResult) -> impl IntoView {
    let SearchResult {
        repository,
        commit_sha,
        file_path,
        start_line,
        end_line,
        match_line,
        content_text,
        branches,
        is_historical,
        snippets,
    } = result;

    let mut snippet_vec = snippets;
    let (primary_snippet, extra_snippets_vec) = if snippet_vec.is_empty() {
        (
            SearchSnippet {
                start_line,
                end_line,
                match_line,
                content_text,
            },
            Vec::new(),
        )
    } else {
        let primary_idx = snippet_vec
            .iter()
            .position(|snippet| {
                snippet.match_line == match_line && snippet.content_text == content_text
            })
            .unwrap_or(0);
        let primary = snippet_vec.swap_remove(primary_idx);
        (primary, snippet_vec)
    };
    let extra_snippets = extra_snippets_vec;
    let extra_count = extra_snippets.len();
    let expanded = RwSignal::new(false);

    let branch_badge = (!branches.is_empty()).then(||{
        let label = format!("Branches: {}", branches.join(", "));
        view! {
            <span class="inline-flex items-center rounded-full bg-blue-100 text-blue-700 dark:bg-blue-900/50 dark:text-blue-100 px-2 py-0.5">
                {label}
            </span>
        }
    });

    let historical_badge = is_historical.then(|| view! {
            <span class="inline-flex items-center rounded-full bg-amber-200 text-amber-900 dark:bg-amber-900/60 dark:text-amber-100 px-2 py-0.5">
                "Historical"
            </span>
        });

    let short_commit: String = commit_sha.chars().take(7).collect();
    let primary_label = format!(
        "{}/{}:{}",
        repository, file_path, primary_snippet.match_line
    );
    let primary_link = format!(
        "/repo/{}/tree/{}/{}#L{}",
        repository, commit_sha, file_path, primary_snippet.match_line,
    );

    let extra_section = (extra_count > 0).then(||{
        let repo = repository.clone();
        let commit = commit_sha.clone();
        let path = file_path.clone();
        let snippets = extra_snippets.clone();
        Some(view! {
            <div class="mt-3 space-y-3">
                <button
                    class="text-sm text-blue-600 dark:text-blue-400 hover:underline"
                    on:click=move |_| {
                        expanded.update(|value| *value = !*value);
                    }
                >
                    {move || {
                        if expanded.get() {
                            match extra_count {
                                1 => "Hide 1 additional match".to_string(),
                                n => format!("Hide {} additional matches", n),
                            }
                        } else {
                            match extra_count {
                                1 => "Show 1 additional match".to_string(),
                                n => format!("Show {} additional matches", n),
                            }
                        }
                    }}
                </button>
                <Show
                    when=move || expanded.get()
                    fallback=move || view! { <></> }
                >
                    {
                        let repo = repo.clone();
                        let commit = commit.clone();
                        let path = path.clone();
                        let snippets = snippets.clone();
                        view! {
                            <div class="space-y-3">
                                {snippets
                                    .iter()
                                    .cloned()
                                    .map(|snippet| {
                                        let location_label = format!(
                                            "{}/{}:{}",
                                            repo,
                                            path,
                                            snippet.match_line,
                                        );
                                        let link = format!(
                                            "/repo/{}/tree/{}/{}#L{}",
                                            repo,
                                            commit,
                                            path,
                                            snippet.match_line,
                                        );
                                        view! {
                                            <div class="border border-dashed border-gray-300 dark:border-gray-700 rounded-md p-3 bg-gray-50 dark:bg-gray-900/40">
                                                <p class="font-mono text-xs break-all">
                                                    <a
                                                        href=link
                                                        class="hover:underline text-blue-600 dark:text-blue-400 break-all"
                                                    >
                                                        {location_label.clone()}
                                                    </a>
                                                </p>
                                                <pre class="bg-gray-100 dark:bg-gray-900 p-2 rounded-md mt-2 text-sm overflow-x-auto max-w-full">
                                                    <code inner_html=snippet.content_text></code>
                                                </pre>
                                            </div>
                                        }
                                    })
                                    .collect_view()}
                            </div>
                        }
                    }
                </Show>
            </div>
        })
    });

    view! {
        <div class="mt-4 p-4 border border-gray-300 dark:border-gray-700 rounded-md bg-white dark:bg-gray-800 break-words max-w-full">
            <p class="font-mono text-sm break-all">
                <a
                    href=primary_link
                    class="hover:underline text-blue-600 dark:text-blue-400 break-all"
                >
                    {primary_label}
                </a>
            </p>
            <div class="flex flex-wrap items-center gap-2 mt-1 text-xs text-gray-600 dark:text-gray-400">
                <span>{format!("Commit {}", short_commit)}</span>
                {branch_badge}
                {historical_badge}
            </div>
            <pre class="bg-gray-100 dark:bg-gray-900 p-2 rounded-md mt-2 text-sm overflow-x-auto max-w-full">
                <code inner_html=primary_snippet.content_text></code>
            </pre>
            {extra_section}
        </div>
    }
}
