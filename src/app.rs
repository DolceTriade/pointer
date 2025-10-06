use leptos::prelude::*;
use leptos_meta::{provide_meta_context, Title};
use leptos_darkmode::Darkmode;
use crate::components::{Header, SearchBar};

pub fn shell(options: LeptosOptions) -> impl IntoView {
    provide_meta_context();
    let darkmode = Darkmode::init();
    view! {
        <!DOCTYPE html>
        <html lang="en" class:dark=move || darkmode.get()>
            <head>
                <meta charset="utf-8" />
                <meta name="viewport" content="width=device-width, initial-scale=1" />
                <AutoReload options=options.clone() />
                <HydrationScripts options />
                <link rel="stylesheet" id="leptos" href="/output.css" />
                <link rel="shortcut icon" type="image/ico" href="/favicon.ico" />
                <Title formatter=|text| format!("{} - Pointer", text) text="Search" />
            </head>

            <body class="bg-white dark:bg-gray-900">
                <App />
            </body>
        </html>
    }
}

#[component]
pub fn App() -> impl IntoView {
    view! {
        <div class="flex flex-col min-h-screen">
            <Header />
            <main class="flex-grow flex items-center justify-center">
                <SearchBar />
            </main>
        </div>
    }
}
