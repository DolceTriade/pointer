use crate::services::search_service::search;
use leptos::Params;
use leptos::either::EitherOf3;
use leptos::prelude::*;
use leptos_router::hooks::use_query;
use leptos_router::params::Params;

#[derive(Params, PartialEq, Clone, Debug)]
pub struct SearchParams {
    pub q: Option<String>,
}

#[component]
pub fn SearchPage() -> impl IntoView {
    let query = use_query::<SearchParams>();

    let search_results = Resource::new(query, |q| async move {
        if let Some(query) = q.ok().and_then(|q| q.q) {
            return search(query).await;
        }
        Ok(vec![])
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
                                    Ok(results) => {
                                        if results.is_empty() {
                                            EitherOf3::A(
                                                view! {
                                                    <p class="text-center py-8">"No results found."</p>
                                                },
                                            )
                                        } else {
                                            EitherOf3::B(
                                                results
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
                                                                            result.start_line,
                                                                        )
                                                                        class="hover:underline text-blue-600 dark:text-blue-400 break-all"
                                                                    >
                                                                        {format!(
                                                                            "{}/{}:{}",
                                                                            result.repository,
                                                                            result.file_path,
                                                                            result.start_line,
                                                                        )}
                                                                    </a>
                                                                </p>
                                                                <pre class="bg-gray-100 dark:bg-gray-900 p-2 rounded-md mt-2 text-sm overflow-x-auto max-w-full">
                                                                    <code inner_html=result.content_text></code>
                                                                </pre>
                                                            </div>
                                                        }
                                                    })
                                                    .collect_view(),
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
