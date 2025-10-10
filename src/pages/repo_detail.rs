use leptos::prelude::*;
use leptos_router::components::A;
use leptos_router::hooks::use_params;
use leptos_router::params::Params;

#[derive(Params, Debug, PartialEq)]
struct RepoParams {
    repo: String,
}

#[component]
pub fn RepoDetailPage() -> impl IntoView {
    let params = use_params::<RepoParams>();
    let repo_name = move || {
        params.with(|p| {
            match p {
                Ok(params) => params.repo.clone(),
                Err(_) => "Unknown Repository".to_string(),
            }
        })
    };

    view! {
        <main class="flex-grow flex flex-col items-center justify-start pt-8 p-4">
            <div class="max-w-4xl w-full">
                <div class="mb-6">
                    <A href="/">
                        <span class="btn btn-ghost btn-sm text-primary">Back to Search</span>
                    </A>
                </div>
                <h1 class="text-3xl font-bold text-center mb-6 text-gray-800 dark:text-gray-200">
                    {repo_name()}
                </h1>
                <div class="bg-white dark:bg-gray-800 rounded-lg shadow p-8 border border-gray-200 dark:border-gray-700">
                    <h2 class="text-2xl font-semibold mb-4 text-gray-800 dark:text-gray-200">Repository Details</h2>
                    <p class="text-gray-600 dark:text-gray-400 mb-4">
                        This is a placeholder page for repository: <strong>{repo_name()}</strong>
                    </p>
                    <p class="text-gray-600 dark:text-gray-400">
                        Here you will be able to view files, search within the repository, and see repository-specific information.
                    </p>
                </div>
            </div>
        </main>
    }
}
