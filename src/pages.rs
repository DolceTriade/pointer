use crate::components::{RepositoriesList, SearchBar};
use leptos::prelude::*;

pub mod file_viewer;
pub mod repo_detail;
pub mod search;
pub use file_viewer::FileViewer;
pub use repo_detail::RepoDetailPage;
pub use search::SearchPage;

#[component]
pub fn HomePage() -> impl IntoView {
    view! {
        <main class="flex-grow flex flex-col items-center justify-start pt-8">
            <SearchBar initial_query="".to_string() />
            <RepositoriesList />
        </main>
    }
}
