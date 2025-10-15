use crate::dsl::parse_query;
use leptos::prelude::*;
use leptos_router::hooks::use_navigate;

#[component]
pub fn SearchBar(#[prop(optional)] initial_query: String) -> impl IntoView {
    let (query, set_query) = signal(initial_query);
    let navigate = use_navigate();

    let on_input = move |ev| {
        set_query.set(event_target_value(&ev));
    };

    let on_search = move || {
        let q = query.get();
        if !q.is_empty() {
            navigate(&format!("/search?q={}&page=1", q), Default::default());
        }
    };

    // Create a reactive validation of the query
    let validation = Memo::new(move |_| {
        let q = query.get();
        if q.is_empty() {
            return None;
        }

        match parse_query(&q) {
            Ok(_) => Some(ValidationStatus::Valid),
            Err(_) => Some(ValidationStatus::Invalid),
        }
    });

    // DSL syntax hints
    let dsl_hints = vec![
        ("repo:", "Search in specific repository"),
        ("file:", "Search in specific files"),
        ("lang:", "Search in specific language"),
        ("content:", "Search in file content"),
        ("sym:", "Search for symbols"),
        ("branch:", "Search in specific branch"),
        ("regex:", "Search with regex pattern"),
        ("historical:", "Include historical commits (historical:yes)"),
    ];

    // Example queries for users
    let example_queries = vec![
        "repo:myrepo lang:rust",
        "content:\"async fn\" file:*.rs",
        "sym:MyFunction lang:go",
        "file:README.md content:install historical:yes",
    ];

    view! {
        <div class="w-full max-w-2xl">
            <div class="group relative">
                <div class="flex items-center rounded-full border border-gray-300 dark:border-gray-700 shadow-lg overflow-hidden bg-white dark:bg-gray-800 relative">
                    <input
                        type="text"
                        placeholder="Search for code... (use DSL: repo:myrepo lang:rust)"
                        class="w-full px-8 py-4 bg-transparent focus:outline-none pr-16"
                        prop:value=query
                        on:input=on_input
                        on:keydown={
                            let func = on_search.clone();
                            move |ev| {
                                if ev.key() == "Enter" {
                                    func();
                                }
                            }
                        }
                    />

                    // Validation indicator
                    {move || {
                        validation
                            .get()
                            .map(|status| {
                                match status {
                                    ValidationStatus::Valid => {
                                        view! {
                                            <div class="absolute right-12 top-1/2 transform -translate-y-1/2">
                                                <svg
                                                    class="w-5 h-5 text-green-500"
                                                    fill="none"
                                                    stroke="currentColor"
                                                    viewBox="0 0 24 24"
                                                >
                                                    <path
                                                        stroke-linecap="round"
                                                        stroke-linejoin="round"
                                                        stroke-width="2"
                                                        d="M5 13l4 4L19 7"
                                                    ></path>
                                                </svg>
                                            </div>
                                        }
                                    }
                                    ValidationStatus::Invalid => {
                                        view! {
                                            <div class="absolute right-12 top-1/2 transform -translate-y-1/2">
                                                <svg
                                                    class="w-5 h-5 text-red-500"
                                                    fill="none"
                                                    stroke="currentColor"
                                                    viewBox="0 0 24 24"
                                                >
                                                    <path
                                                        stroke-linecap="round"
                                                        stroke-linejoin="round"
                                                        stroke-width="2"
                                                        d="M6 18L18 6M6 6l12 12"
                                                    ></path>
                                                </svg>
                                            </div>
                                        }
                                    }
                                }
                            })
                    }}

                    <button
                        class="px-6 py-4 bg-gray-200 dark:bg-gray-700 hover:bg-gray-300 dark:hover:bg-gray-600 transition-colors duration-200"
                        on:click=move |_| on_search()
                    >
                        <svg
                            xmlns="http://www.w3.org/2000/svg"
                            class="h-6 w-6"
                            fill="none"
                            viewBox="0 0 24 24"
                            stroke="currentColor"
                        >
                            <path
                                stroke-linecap="round"
                                stroke-linejoin="round"
                                stroke-width="2"
                                d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"
                            />
                        </svg>
                    </button>
                </div>

                // DSL syntax hints that appear when input is focused
                <div class="absolute hidden mt-2 w-full bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg shadow-lg z-10 opacity-0 scale-95 transition-all duration-200 group-focus-within:opacity-100 group-focus-within:scale-100 group-focus-within:block">
                    <div class="p-3 text-sm text-gray-600 dark:text-gray-300">
                        <p class="font-semibold mb-2">DSL Search Syntax:</p>
                        <div class="grid grid-cols-2 gap-2">
                            {dsl_hints
                                .into_iter()
                                .map(|(syntax, description)| {
                                    view! {
                                        <div
                                            class="flex cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-700 p-1 rounded"
                                            on:click=move |_| {
                                                set_query.set(format!("{}{}", query.get(), syntax));
                                            }
                                        >
                                            <span class="font-mono text-blue-600 dark:text-blue-400 font-semibold mr-2">
                                                {syntax}
                                            </span>
                                            <span class="text-gray-600 dark:text-gray-400">
                                                {description}
                                            </span>
                                        </div>
                                    }
                                })
                                .collect_view()}
                        </div>
                        <div class="mt-3 pt-2 border-t border-gray-200 dark:border-gray-700">
                            <p class="font-semibold mb-1">Examples:</p>
                            <div class="space-y-1">
                                {example_queries
                                    .into_iter()
                                    .map(|ex| {
                                        let ex_clone = ex.to_string();
                                        view! {
                                            <div
                                                class="font-mono text-sm bg-gray-100 dark:bg-gray-700 p-2 rounded cursor-pointer hover:bg-gray-200 dark:hover:bg-gray-600"
                                                on:click=move |_| {
                                                    set_query.set(ex_clone.clone());
                                                }
                                            >
                                                {ex}
                                            </div>
                                        }
                                    })
                                    .collect_view()}
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    }
}

#[derive(Clone, PartialEq)]
enum ValidationStatus {
    Valid,
    Invalid,
}
