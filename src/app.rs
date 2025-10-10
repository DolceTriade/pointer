use crate::components::{Header, SearchBar, RepositoriesList};
use leptos::prelude::*;
use leptos_darkmode::Darkmode;
use leptos_meta::{Html, Title};

pub fn shell(options: LeptosOptions) -> impl IntoView {
    view! {
        <!DOCTYPE html>
        <html lang="en">
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
    let darkmode = expect_context::<Darkmode>();
    view! {
        <Html class:dark=move || darkmode.is_dark() />
        <div class="flex flex-col min-h-screen">
            <Header />
            <main class="flex-grow flex flex-col items-center justify-start pt-8">
                <SearchBar />
                <RepositoriesList />
            </main>
        </div>
    }
}
