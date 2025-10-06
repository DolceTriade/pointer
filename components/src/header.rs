use leptos::prelude::*;

#[component]
pub fn Header() -> impl IntoView {
    view! {
        <header class="w-full px-4 py-3 bg-gray-800 text-white">
            <div class="container mx-auto">
                <h1 class="text-2xl font-semibold">"Pointer"</h1>
            </div>
        </header>
    }
}
