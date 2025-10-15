use crate::db::models::SearchResultsPage;
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
                                                            .map(|result| {
                                                                view! {
                                                                    <div class="mt-4 p-4 border border-gray-300 dark:border-gray-700 rounded-md bg-white dark:bg-gray-800 break-words max-w-full">
                                                                        <p class="font-mono text-sm break-all">
                                                                            <a
                                                                                href=format!(
                                                                                    "/repo/{}/tree/{}/{}#L{}",
                                                                                    result.repository,
                                                                                    result.commit_sha,
                                                                                    result.file_path,
                                                                                    result.match_line,
                                                                                )
                                                                                class="hover:underline text-blue-600 dark:text-blue-400 break-all"
                                                                            >
                                                                                {format!(
                                                                                    "{}/{}:{}",
                                                                                    result.repository,
                                                                                    result.file_path,
                                                                                    result.match_line,
                                                                                )}
                                                                            </a>
                                                                        </p>
                                                                        <pre class="bg-gray-100 dark:bg-gray-900 p-2 rounded-md mt-2 text-sm overflow-x-auto max-w-full">
                                                                            <code inner_html=result.content_text></code>
                                                                        </pre>
                                                                    </div>
                                                                }
                                                            })
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
