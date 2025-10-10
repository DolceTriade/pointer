use leptos::prelude::*;
use crate::components::{SearchBar, RepositoriesList};

pub mod repo_detail;
pub use repo_detail::RepoDetailPage;

#[component]
pub fn HomePage() -> impl IntoView {
    view! {
        <main class="flex-grow flex flex-col items-center justify-start pt-8">
            <SearchBar />
            <RepositoriesList />
        </main>
    }
}

#[component]
pub fn PlaceholderPage() -> impl IntoView {
    view! {
        <main class="flex-grow flex flex-col items-center justify-start pt-8 p-4">
            <div class="max-w-4xl w-full">
                <h1 class="text-3xl font-bold text-center mb-6 text-gray-800 dark:text-gray-200">Placeholder Page</h1>
                <div class="bg-white dark:bg-gray-800 rounded-lg shadow p-8 border border-gray-200 dark:border-gray-700 text-center">
                    <p class="text-xl text-gray-600 dark:text-gray-400">This is a placeholder page</p>
                </div>
            </div>
        </main>
    }
}