use crate::components::file_tree::{DirectoryIcon, FileIcon};
use crate::pages::file_viewer::search_repo_paths;
use leptos::either::Either;
use leptos::html::Div;
use leptos::prelude::*;
use leptos_router::components::A;
use leptos_router::hooks::use_params;
use web_sys::wasm_bindgen::JsCast;

#[component]
pub fn FileQuickNavigator(repo: Signal<String>, branch: Signal<String>) -> impl IntoView {
    let (query, set_query) = signal(String::new());
    let repo_for_search = repo.clone();
    let branch_for_search = branch.clone();
    let params = use_params::<crate::pages::file_viewer::FileViewerParams>();
    let container_ref = NodeRef::<Div>::new();

    Effect::new(move |_| {
        // Reset the query when the path changes
        params.get().ok();
        set_query.set(String::new());
    });

    Effect::new(move |_| {
        use leptos::leptos_dom::helpers::window_event_listener;
        let handle = window_event_listener(leptos::ev::click, move |ev| {
            let target = ev.target();
            if container_ref.get().is_none() || target.is_none() {
                return;
            }
            let target_node = target.unwrap().dyn_into().unwrap();
            let container = container_ref.get().unwrap();
            if !container.contains(Some(&target_node)) {
                set_query.set(String::new());
            }
        });
        on_cleanup(move || handle.remove());
    });

    let search_resource = Resource::new(
        move || (repo_for_search(), branch_for_search(), query.get()),
        |(repo, branch, query)| async move {
            let trimmed = query.trim().to_string();
            if trimmed.is_empty() {
                Ok(Vec::new())
            } else {
                search_repo_paths(repo, branch, trimmed, Some(10)).await
            }
        },
    );

    view! {
        <div class="relative mb-4" node_ref=container_ref>
            <input
                type="text"
                class="w-full px-3 py-2 text-sm rounded-md border border-slate-200 dark:border-slate-700 bg-white/95 text-slate-900 dark:bg-slate-950/60 dark:text-slate-100 focus-visible:outline focus-visible:outline-2 focus-visible:outline-sky-600 dark:focus-visible:outline-sky-400"
                placeholder="Go to file..."
                prop:value=query
                on:input=move |ev| set_query.set(event_target_value(&ev))
            />
            <Show when=move || !query.get().trim().is_empty() fallback=|| ()>
                <div class="absolute left-0 right-0 z-30 mt-1 bg-white/95 dark:bg-slate-950/85 border border-slate-200 dark:border-slate-800 rounded-md shadow-lg text-slate-900 dark:text-slate-100">
                    <Suspense fallback=move || {
                        view! {
                            <div class="px-3 py-2 text-sm text-slate-600 dark:text-slate-300">
                                "Searching..."
                            </div>
                        }
                    }>
                        {move || {
                            search_resource
                                .get()
                                .map(|result| match result {
                                    Ok(entries) => {
                                        if entries.is_empty() {
                                            view! {
                                                <div class="px-3 py-2 text-sm text-slate-600 dark:text-slate-300">
                                                    "No matches"
                                                </div>
                                            }
                                                .into_any()
                                        } else {
                                            let current_repo = repo.get();
                                            let current_branch = branch.get();
                                            view! {
                                                <ul class="divide-y divide-slate-200 dark:divide-slate-800">
                                                    {entries
                                                        .iter()
                                                        .cloned()
                                                        .map(|entry| {
                                                            let mut href = format!(
                                                                "/repo/{}/tree/{}/{}",
                                                                current_repo,
                                                                current_branch,
                                                                entry.path,
                                                            );
                                                            if entry.kind == "dir" {
                                                                href.push('/');
                                                            }
                                                            let mut display_path = entry.path.clone();
                                                            if entry.kind == "dir" && !display_path.ends_with('/') {
                                                                display_path.push('/');
                                                            }
                                                            let name = entry.name.clone();
                                                            view! {
                                                                <li>
                                                                    <A
                                                                        href=href
                                                                        attr:class="flex items-center gap-2 px-3 py-2 text-sm hover:bg-slate-100 dark:hover:bg-slate-800 transition-colors rounded-md text-slate-900 dark:text-slate-100"
                                                                    >
                                                                        {if entry.kind == "dir" {
                                                                            Either::Left(view! { <DirectoryIcon /> })
                                                                        } else {
                                                                            Either::Right(view! { <FileIcon /> })
                                                                        }}
                                                                        <div class="flex flex-col min-w-0">
                                                                            <span class="font-medium truncate">{name}</span>
                                                                            <span class="text-xs text-slate-600 dark:text-slate-300 truncate">
                                                                                {display_path}
                                                                            </span>
                                                                        </div>
                                                                    </A>
                                                                </li>
                                                            }
                                                                .into_any()
                                                        })
                                                        .collect_view()}
                                                </ul>
                                            }
                                                .into_any()
                                        }
                                    }
                                    Err(e) => {
                                        view! {
                                            <div class="px-3 py-2 text-sm text-red-500">
                                                {"Error: "} {e.to_string()}
                                            </div>
                                        }
                                            .into_any()
                                    }
                                })
                                .unwrap_or_else(|| view! { <div></div> }.into_any())
                        }}
                    </Suspense>
                </div>
            </Show>
        </div>
    }
    .into_any()
}
