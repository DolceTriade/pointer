use leptos::prelude::*;

#[component]
pub fn SearchBar() -> impl IntoView {
    view! {
        <div class="w-full h-full flex flex-col items-center justify-center text-black dark:text-white">
            <div class="w-1/2 flex items-center rounded-full border border-gray-300 dark:border-gray-700 shadow-lg overflow-hidden bg-white dark:bg-gray-800">
                <input
                    type="text"
                    placeholder="Search for code..."
                    class="w-full px-8 py-4 bg-transparent focus:outline-none"
                />
                <button class="px-6 py-4 bg-gray-200 dark:bg-gray-700 hover:bg-gray-300 dark:hover:bg-gray-600 transition-colors duration-200">
                    <svg xmlns="http://www.w3.org/2000/svg" class="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
                    </svg>
                </button>
            </div>
        </div>
    }
}
