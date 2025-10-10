use leptos::prelude::*;
use crate::services::repo_service::get_repositories;

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
                            .map(|result| {
                                match result {
                                    Ok(repos) => {
                                        repos
                                            .into_iter()
                                            .map(|repo| {
                                                let repo_name = repo.repository.clone();
                                                let file_count = repo.file_count;
                                                let file_count_text = format!("{} files", file_count);
                                                let repo_name_str = repo_name;
                                                // Ensure this is a String
                                                view! {
                                                    <div class="bg-white dark:bg-gray-800 rounded-lg shadow p-4 border border-gray-200 dark:border-gray-700">
                                                        <h3 class="font-semibold text-lg text-gray-900 dark:text-gray-100">
                                                            {repo_name_str}
                                                        </h3>
                                                        <p class="text-gray-600 dark:text-gray-400 text-sm">
                                                            {file_count_text}
                                                        </p>
                                                    </div>
                                                }
                                            })
                                            .collect_view()
                                    }
                                    Err(e) => {
                                        let error_msg = e.to_string();
                                        let error_title = "Error".to_string();
                                        vec![
                                            view! {
                                                <div class="bg-white dark:bg-gray-800 rounded-lg shadow p-4 border border-gray-200 dark:border-gray-700">
                                                    <h3 class="font-semibold text-lg text-gray-900 dark:text-gray-100">
                                                        {error_title}
                                                    </h3>
                                                    <p class="text-gray-600 dark:text-gray-400 text-sm">
                                                        {error_msg}
                                                    </p>
                                                </div>
                                            },
                                        ]
                                    }
                                }
                            })
                    }}
                </div>
            </Suspense>
        </div>
    }
}
