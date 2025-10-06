use leptos::prelude::*;
use leptos_meta::{provide_meta_context, Title};
use components::{Header, SearchBar};

pub fn shell(options: LeptosOptions) -> impl IntoView {
    provide_meta_context();
    let dark_mode = RwSignal::new(false);
    view! {
        <!DOCTYPE html>
        <html lang="en" class:dark=move || dark_mode.get() >
            <head>
                <meta charset="utf-8"/>
                <meta name="viewport" content="width=device-width, initial-scale=1"/>
                <AutoReload options=options.clone() />
                <HydrationScripts options/>
                <link rel="stylesheet" id="leptos" href="/output.css"/>
                <link rel="shortcut icon" type="image/ico" href="/favicon.ico"/>
                <Title formatter=|text| format!("{} - Pointer", text) text="Search"/>
            </head>

            <body class="bg-white dark:bg-gray-900">
                <App/>
            </body>
        </html>
    }
}

#[component]
pub fn App() -> impl IntoView {
    view! {
        <div class="min-h-screen flex flex-col">
            <Header />
            <main class="flex-grow h-full">
                <SearchBar />
            </main>
        </div>
    }
}
