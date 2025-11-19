use leptos::html::Details;
use leptos::prelude::*;

#[component]
pub fn PathFilterActions(
    path: String,
    included_paths: RwSignal<Vec<String>>,
    excluded_paths: RwSignal<Vec<String>>,
) -> impl IntoView {
    let file_path = path.clone();
    let directory_path = path.rsplit_once('/').map(|(dir, _)| format!("{dir}/"));

    let menu_ref = NodeRef::<Details>::new();

    view! {
        <details class="relative inline-flex items-center" node_ref=menu_ref>
            <summary
                class="inline-flex items-center justify-center w-6 h-6 rounded-full border border-gray-300 dark:border-gray-600 text-gray-600 dark:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-700 cursor-pointer select-none flex-shrink-0"
                style="list-style: none;"
                title="Path options"
            >
                <span aria-hidden="true">{"▼"}</span>
                <span class="sr-only">Path options</span>
            </summary>
            <div class="absolute right-0 z-10 mt-1 w-44 rounded-md border border-gray-200 dark:border-gray-600 bg-white dark:bg-gray-800 shadow-lg">
                <ul class="py-1 text-xs text-gray-700 dark:text-gray-200">
                    <li>
                        <button
                            class="w-full px-3 py-2 text-left hover:bg-gray-100 dark:hover:bg-gray-700"
                            on:click={
                                let included_paths = included_paths.clone();
                                let value = file_path.clone();
                                let menu_ref = menu_ref.clone();
                                move |ev: leptos::ev::MouseEvent| {
                                    ev.prevent_default();
                                    ev.stop_propagation();
                                    let candidate = value.clone();
                                    included_paths
                                        .update(|paths| {
                                            if !paths.iter().any(|existing| existing == &candidate) {
                                                paths.push(candidate.clone());
                                            }
                                        });
                                    if let Some(menu) = menu_ref.get_untracked() {
                                        menu.set_open(false);
                                    }
                                }
                            }
                        >
                            "Include file"
                        </button>
                    </li>
                    {directory_path
                        .clone()
                        .map(|dir| {
                            let included_paths = included_paths.clone();
                            let dir_value = dir.clone();
                            let menu_ref = menu_ref.clone();
                            view! {
                                <li>
                                    <button
                                        class="w-full px-3 py-2 text-left hover:bg-gray-100 dark:hover:bg-gray-700"
                                        on:click=move |ev: leptos::ev::MouseEvent| {
                                            ev.prevent_default();
                                            ev.stop_propagation();
                                            let candidate = dir_value.clone();
                                            included_paths
                                                .update(|paths| {
                                                    if !paths.iter().any(|existing| existing == &candidate) {
                                                        paths.push(candidate.clone());
                                                    }
                                                });
                                            if let Some(menu) = menu_ref.get_untracked() {
                                                menu.set_open(false);
                                            }
                                        }
                                    >
                                        "Include directory"
                                    </button>
                                </li>
                            }
                        })}
                    <li>
                        <button
                            class="w-full px-3 py-2 text-left hover:bg-gray-100 dark:hover:bg-gray-700"
                            on:click={
                                let excluded_paths = excluded_paths.clone();
                                let value = file_path.clone();
                                let menu_ref = menu_ref.clone();
                                move |ev: leptos::ev::MouseEvent| {
                                    ev.prevent_default();
                                    ev.stop_propagation();
                                    let candidate = value.clone();
                                    excluded_paths
                                        .update(|paths| {
                                            if !paths.iter().any(|existing| existing == &candidate) {
                                                paths.push(candidate.clone());
                                            }
                                        });
                                    if let Some(menu) = menu_ref.get_untracked() {
                                        menu.set_open(false);
                                    }
                                }
                            }
                        >
                            "Exclude file"
                        </button>
                    </li>
                    {directory_path
                        .map(|dir| {
                            let excluded_paths = excluded_paths.clone();
                            let dir_value = dir.clone();
                            let menu_ref = menu_ref.clone();
                            view! {
                                <li>
                                    <button
                                        class="w-full px-3 py-2 text-left hover:bg-gray-100 dark:hover:bg-gray-700"
                                        on:click=move |ev: leptos::ev::MouseEvent| {
                                            ev.prevent_default();
                                            ev.stop_propagation();
                                            let candidate = dir_value.clone();
                                            excluded_paths
                                                .update(|paths| {
                                                    if !paths.iter().any(|existing| existing == &candidate) {
                                                        paths.push(candidate.clone());
                                                    }
                                                });
                                            if let Some(menu) = menu_ref.get_untracked() {
                                                menu.set_open(false);
                                            }
                                        }
                                    >
                                        "Exclude directory"
                                    </button>
                                </li>
                            }
                        })}
                </ul>
            </div>
        </details>
    }
}
