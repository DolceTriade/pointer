use crate::components::Header;
use crate::pages::file_viewer::FileViewer;
use crate::pages::{HomePage, RepoDetailPage, SearchPage};
use leptos::prelude::*;
use leptos_darkmode::Darkmode;
use leptos_meta::{Html, Title, provide_meta_context};
use leptos_router::components::{Route, Router, Routes};
use leptos_router::path;

pub fn shell(options: LeptosOptions) -> impl IntoView {
    provide_meta_context();
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
    let darkmode = Darkmode::init();
    view! {
        <Html class:dark=move || darkmode.is_dark() />
        <Router>
            <div class="flex flex-col min-h-screen">
                <Header />
                <Routes fallback=|| "Page not found".into_view()>
                    <Route path=path!("/") view=HomePage />
                    <Route path=path!("/search") view=SearchPage />
                    <Route path=path!("/repo/:repo") view=RepoDetailPage />
                    <Route path=path!("/repo/:repo/tree/:branch/*path") view=FileViewer />
                </Routes>
            </div>
        </Router>
    }
}
