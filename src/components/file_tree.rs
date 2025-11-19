use crate::db::TreeEntry;
use crate::pages::file_viewer::{FileViewerData, get_file_viewer_data};
use leptos::prelude::*;
use leptos_router::components::A;
use std::collections::HashSet;

#[component]
pub fn FileIcon() -> impl IntoView {
    view! {
        <svg
            xmlns="http://www.w3.org/2000/svg"
            class="h-5 w-5 text-gray-500"
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
        >
            <path
                stroke-linecap="round"
                stroke-linejoin="round"
                stroke-width="2"
                d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.707.707V19a2 2 0 01-2 2z"
            ></path>
        </svg>
    }
}

#[component]
pub fn DirectoryIcon() -> impl IntoView {
    view! {
        <svg
            xmlns="http://www.w3.org/2000/svg"
            class="h-5 w-5 text-yellow-500"
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
        >
            <path
                stroke-linecap="round"
                stroke-linejoin="round"
                stroke-width="2"
                d="M3 7v10a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-6l-2-2H5a2 2 0 00-2 2z"
            ></path>
        </svg>
    }
}

#[component]
pub fn FileTreeNodes(
    entries: Vec<TreeEntry>,
    repo: Signal<String>,
    branch: Signal<String>,
    expanded: RwSignal<HashSet<String>>,
) -> impl IntoView {
    view! {
        <ul class="pl-4">
            <For
                each=move || entries.clone()
                key=|child| child.path.clone()
                children=move |child| {
                    view! { <FileTreeNode entry=child repo=repo branch=branch expanded=expanded /> }
                }
            />
        </ul>
    }
    .into_any()
}

#[component]
pub fn FileTreeNode(
    entry: TreeEntry,
    repo: Signal<String>,
    branch: Signal<String>,
    expanded: RwSignal<HashSet<String>>,
) -> impl IntoView {
    let is_dir = entry.kind == "dir";
    let children: RwSignal<Option<Vec<TreeEntry>>> = RwSignal::new(None);

    let path = entry.path.clone();
    let link_path = entry.path.clone();
    let dir_path = entry.path.clone();
    let child_entry = entry.clone();
    let expand_entry = entry.clone();
    let child_resource = Resource::new(
        move || (is_dir, expanded.get().contains(&path), repo(), branch()),
        move |(is_dir, is_expanded, repo, branch)| {
            let entry = child_entry.clone();
            async move {
                if is_dir && is_expanded {
                    return get_file_viewer_data(repo, branch, Some(entry.path.clone() + "/"))
                        .await
                        .ok();
                }
                None
            }
        },
    );

    Effect::new(move |_| {
        let childs = child_resource.read();
        if let Some(FileViewerData::Directory { entries, .. }) = childs.as_ref().flatten() {
            children.set(Some(entries.clone()));
        }
    });

    let on_click = move |_| {
        if is_dir {
            expanded.update(|dirs| {
                if dirs.contains(&entry.path) {
                    dirs.remove(&entry.path);
                } else {
                    dirs.insert(entry.path.clone());
                }
            });
            // Trigger resource loading
            child_resource.refetch();
        }
    };

    let link = move || format!("/repo/{}/tree/{}/{}", repo.get(), branch.get(), link_path);

    view! {
        <li>
            <div
                class="flex items-center cursor-pointer py-1"
                on:click=on_click
                // Use a normal link for files, but handle dirs with the on:click
                role=if is_dir { "button" } else { "" }
            >
                {if is_dir {
                    let icon = move || {
                        if expanded.get().contains(&dir_path) { "▼" } else { "▶" }
                    };
                    let name = entry.name.clone();
                    // "▶" "▼"
                    view! {
                        <span class="w-4 text-gray-500">{icon}</span>
                        <DirectoryIcon />
                        <span class="ml-1 text-blue-600 hover:underline truncate" title=name>
                            {entry.name}
                        </span>
                    }
                        .into_any()
                } else {
                    let name = entry.name.clone();
                    view! {
                        <FileIcon />
                        <span class="w-4"></span>
                        <A
                            href=link
                            attr:class="ml-1 text-blue-600 hover:underline truncate"
                            attr:title=name.clone()
                        >
                            {entry.name}
                        </A>
                    }
                        .into_any()
                }}
            </div>
            {
                let entry = expand_entry.clone();
                move || {
                    (is_dir && expanded.get().contains(&entry.path))
                        .then(|| {
                            children
                                .get()
                                .map(|nodes| {
                                    view! {
                                        <FileTreeNodes
                                            entries=nodes
                                            repo=repo
                                            branch=branch
                                            expanded=expanded
                                        />
                                    }
                                        .into_any()
                                })
                        })
                }
            }
        </li>
    }
}
