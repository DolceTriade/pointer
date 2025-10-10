use crate::services::repo_service::get_repositories;
use leptos::{either::Either, prelude::*};
use leptos_router::components::A;

#[component]
pub fn RepositoriesList() -> impl IntoView {
    let repos_resource = Resource::new(|| (), move |_| get_repositories(10));

    view! {
        <div class="w-full max-w-4xl mt-12 px-4">
            <h2 class="text-2xl font-bold mb-6 text-gray-800 dark:text-gray-200">Repositories</h2>
            <Suspense fallback=move || {
                view! { <div class="text-center py-4">"Loading repositories..."</div> }
            }>
                <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                    {move || {
                        repos_resource
                            .get()
                            .map(|result| match result {
                                Ok(repos) => {
                                    Either::Left(
                                        view! {
                                            <For
                                                each=move || repos.clone()
                                                key=|repo| repo.repository.clone()
                                                children=move |repo| {
                                                    let repo_name = repo.repository.clone();
                                                    let file_count = repo.file_count;
                                                    let file_count_text = format!("{} files", file_count);
                                                    let repo_encoded = urlencoding::encode(&repo_name)
                                                        .to_string();
                                                    view! {
                                                        <A href=move || format!("/repo/{}", repo_encoded)>
                                                            <div class="bg-white dark:bg-gray-800 rounded-lg shadow p-4 border border-gray-200 dark:border-gray-700 hover:shadow-md transition-shadow duration-200 cursor-pointer block">
                                                                <h3 class="font-semibold text-lg text-gray-900 dark:text-gray-100">
                                                                    {repo_name.clone()}
                                                                </h3>
                                                                <p class="text-gray-600 dark:text-gray-400 text-sm">
                                                                    {file_count_text}
                                                                </p>
                                                            </div>
                                                        </A>
                                                    }
                                                }
                                            />
                                        },
                                    )
                                }
                                Err(e) => {
                                    Either::Right(
                                        view! {
                                            <div class="bg-white dark:bg-gray-800 rounded-lg shadow p-4 border border-gray-200 dark:border-gray-700">
                                                <h3 class="font-semibold text-lg text-gray-900 dark:text-gray-100">
                                                    "Error"
                                                </h3>
                                                <p class="text-gray-600 dark:text-gray-400 text-sm">
                                                    {e.to_string()}
                                                </p>
                                            </div>
                                        },
                                    )
                                }
                            })
                    }}
                </div>
            </Suspense>
        </div>
    }
}
