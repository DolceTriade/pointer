use leptos::prelude::*;
use leptos_router::hooks::use_navigate;

#[component]
pub fn SearchBar(#[prop(optional)] initial_query: Signal<String>) -> impl IntoView {
    let (query, set_query) = signal(initial_query.get());
    let navigate = use_navigate();

    let on_input = move |ev| {
        set_query.set(event_target_value(&ev));
    };

    let on_search = move || {
        let q = query.get();
        if !q.is_empty() {
            navigate(&format!("/search?q={}", q), Default::default());
        }
    };

    view! {
        <div class="w-full max-w-2xl flex items-center rounded-full border border-gray-300 dark:border-gray-700 shadow-lg overflow-hidden bg-white dark:bg-gray-800">
            <input
                type="text"
                placeholder="Search for code..."
                class="w-full px-8 py-4 bg-transparent focus:outline-none"
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
    }
}
