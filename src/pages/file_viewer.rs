use crate::db::TreeEntry;
use leptos::{either::Either, prelude::*};
use leptos_router::components::A;
use leptos_router::hooks::use_params;
use leptos_router::params::Params;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Params, PartialEq, Clone, Debug)]
pub struct FileViewerParams {
    pub repo: String,
    pub branch: String,
    pub path: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum FileViewerData {
    File {
        content: String,
    },
    Directory {
        entries: Vec<TreeEntry>,
        readme: Option<String>,
    },
}

#[server]
pub async fn get_file_viewer_data(
    repo: String,
    branch: String,
    path: Option<String>,
) -> Result<FileViewerData, ServerFnError> {
    use crate::db::{Database, RepoTreeQuery, postgres::PostgresDb};

    let state = expect_context::<crate::server::GlobalAppState>();
    let state = state.lock().await;
    let db = PostgresDb::new(state.pool.clone());

    let path_str = path.unwrap_or_default();
    // An empty path or a path ending in '/' is a directory.
    let is_dir = path_str.is_empty() || path_str.ends_with('/');

    if is_dir {
        let tree = db
            .get_repo_tree(
                &repo,
                RepoTreeQuery {
                    commit: branch.clone(),
                    path: Some(path_str),
                },
            )
            .await
            .map_err(|e| ServerFnError::new(e.to_string()))?;

        let readme_path = tree
            .entries
            .iter()
            .find(|e| e.name.to_lowercase() == "readme.md")
            .map(|e| e.path.clone());

        let readme = if let Some(readme_path) = readme_path {
            let file_content = db
                .get_file_content(&repo, &branch, &readme_path)
                .await
                .map_err(|e| ServerFnError::new(e.to_string()))?;
            let mut content = String::new();
            for line in file_content.lines {
                for segment in line.segments {
                    content.push_str(&segment.text);
                }
                content.push('\n');
            }
            Some(content)
        } else {
            None
        };

        Ok(FileViewerData::Directory {
            entries: tree.entries,
            readme,
        })
    } else {
        // This is a file path
        let file_content = db
            .get_file_content(&repo, &branch, &path_str)
            .await
            .map_err(|e| ServerFnError::new(e.to_string()))?;

        let mut content = String::new();
        for line in file_content.lines {
            for segment in line.segments {
                content.push_str(&segment.text);
            }
            content.push('\n');
        }
        Ok(FileViewerData::File { content })
    }
}

pub fn render_markdown(markdown: &str) -> String {
    use pulldown_cmark::{Options, Parser, html};

    let mut options = Options::empty();
    options.insert(Options::ENABLE_TABLES);
    options.insert(Options::ENABLE_FOOTNOTES);
    options.insert(Options::ENABLE_STRIKETHROUGH);
    options.insert(Options::ENABLE_TASKLISTS);

    let parser = Parser::new_ext(markdown, options);

    let mut html_output = String::new();
    html::push_html(&mut html_output, parser);

    html_output
}

#[component]
fn FileTreeNode(
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
        move || (is_dir, expanded.get().contains(&path)),
        move |(is_dir, is_expanded)| {
            let entry = child_entry.clone();
            async move {
                if is_dir && is_expanded {
                    if let Ok(FileViewerData::Directory { entries, .. }) = get_file_viewer_data(
                        repo.get(),
                        branch.get(),
                        Some(entry.path.clone() + "/"),
                    )
                    .await
                    {
                        children.set(Some(entries));
                    }
                }
            }
        },
    );

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

    let link = format!("/repo/{}/tree/{}/{}", repo.get(), branch.get(), link_path);

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
                    Either::Left(
                        // "▶" "▼"
                        view! {
                            <span class="w-4">{icon}</span>
                            <span class="ml-1 text-blue-600 hover:underline">{entry.name}</span>
                        },
                    )
                } else {
                    Either::Right(
                        view! {
                            <span class="w-4"></span>
                            <A href=link.clone() attr:class="ml-1 text-blue-600 hover:underline">
                                {entry.name}
                            </A>
                        },
                    )
                }}
            </div>
            {
                let entry = expand_entry.clone();
                move || {
                    if is_dir && expanded.get().contains(&entry.path) {
                        Either::Left(
                            view! {
                                <ul class="pl-4">
                                    <For
                                        each=move || children.get().unwrap_or_default()
                                        key=|child| child.path.clone()
                                        children=move |child| {
                                            view! {
                                                <FileTreeNode
                                                    entry=child
                                                    repo=repo
                                                    branch=branch
                                                    expanded=expanded
                                                />
                                            }
                                        }
                                    />
                                </ul>
                            },
                        )
                    } else {
                        Either::Right(view! { <ul class="hidden"></ul> })
                    }
                }
            }
        </li>
    }
}

#[component]
pub fn FileViewer() -> impl IntoView {
    let params = use_params::<FileViewerParams>();
    let repo = move || params.with(|p| p.clone().map(|p| p.repo).unwrap_or_default());
    let branch = move || params.with(|p| p.clone().map(|p| p.branch).unwrap_or_default());
    let path = move || params.with(|p| p.clone().map(|p| p.path).unwrap_or_default());

    // Resource for the main content panel (right side)
    let data_resource = Resource::new(
        move || (repo(), branch(), path()),
        |(repo, branch, path)| get_file_viewer_data(repo, branch, path),
    );

    // Resource for the file tree (left side), always fetching the root
    let tree_resource = Resource::new(
        move || (repo(), branch()),
        |(repo, branch)| get_file_viewer_data(repo, branch, Some("".to_string())),
    );

    let expanded_dirs = RwSignal::new(HashSet::<String>::new());

    view! {
        <main class="flex-grow flex flex-col items-center justify-start pt-8 p-4">
            <div class="max-w-7xl w-full">
                <h1 class="text-3xl font-bold mb-6 text-gray-800 dark:text-gray-200">
                    <A href=move || format!("/repo/{}", repo())>{repo()}</A>
                    <span class="text-gray-500">" / "</span>
                    <span>{branch}</span>
                </h1>
                <div class="flex gap-6">
                    // Left Panel: File Tree
                    <div class="w-1/4 bg-white dark:bg-gray-800 rounded-lg shadow p-4 border border-gray-200 dark:border-gray-700 self-start">
                        <h2 class="text-xl font-semibold mb-4 text-gray-800 dark:text-gray-200">
                            "Files"
                        </h2>
                        <Suspense fallback=move || {
                            view! { <p>"Loading tree..."</p> }
                        }>
                            <ul class="font-mono text-sm">
                                {move || {
                                    tree_resource
                                        .get()
                                        .map(|result| match result {
                                            Ok(FileViewerData::Directory { entries, .. }) => {
                                                Either::Left(
                                                    // The fix is here: pass `repo()` and `branch()` directly
                                                    view! {
                                                        <For
                                                            each=move || entries.clone()
                                                            key=|e| e.path.clone()
                                                            children=move |entry| {
                                                                view! {
                                                                    <FileTreeNode
                                                                        entry=entry
                                                                        repo=Signal::derive(repo)
                                                                        branch=Signal::derive(branch)
                                                                        expanded=expanded_dirs
                                                                    />
                                                                }
                                                            }
                                                        />
                                                    },
                                                )
                                            }
                                            _ => {
                                                Either::Right(view! { <p>"Error loading file tree."</p> })
                                            }
                                        })
                                }}
                            </ul>
                        </Suspense>
                    </div>

                    // Right Panel: Content Viewer
                    <div class="w-3/4">
                        <Suspense fallback=move || {
                            view! { <p>"Loading content..."</p> }
                        }>
                            {move || {
                                data_resource
                                    .get()
                                    .map(|result| {
                                        match result {
                                            Ok(data) => {
                                                let view = match data {
                                                    FileViewerData::File { content } => {
                                                        Either::Left(
                                                            view! {
                                                                <div class="bg-white dark:bg-gray-800 rounded-lg shadow border border-gray-200 dark:border-gray-700">
                                                                    <pre class="p-4 overflow-auto text-sm">{content}</pre>
                                                                </div>
                                                            },
                                                        )
                                                    }
                                                    FileViewerData::Directory { entries, readme } => {
                                                        Either::Right(
                                                            view! {
                                                                // Top half: File list
                                                                <div class="bg-white dark:bg-gray-800 rounded-lg shadow p-4 border border-gray-200 dark:border-gray-700 mb-6">
                                                                    <div class="grid grid-cols-2 md:grid-cols-3 gap-4">
                                                                        {entries
                                                                            .into_iter()
                                                                            .map(|entry| {
                                                                                let link = format!(
                                                                                    "/repo/{}/tree/{}/{}",
                                                                                    repo(),
                                                                                    branch(),
                                                                                    entry.path,
                                                                                );
                                                                                let icon = if entry.kind == "dir" {
                                                                                    "📁"
                                                                                } else {
                                                                                    "📄"
                                                                                };
                                                                                view! {
                                                                                    <A
                                                                                        href=link
                                                                                        attr:class="text-blue-600 hover:underline p-2 rounded hover:bg-gray-100 dark:hover:bg-gray-700 flex items-center gap-2"
                                                                                    >
                                                                                        {icon}
                                                                                        {entry.name}
                                                                                    </A>
                                                                                }
                                                                            })
                                                                            .collect_view()}
                                                                    </div>
                                                                </div>
                                                                // Bottom half: README
                                                                {readme
                                                                    .map(|readme_content| {
                                                                        view! {
                                                                            <div class="bg-white dark:bg-gray-800 rounded-lg shadow p-8 border border-gray-200 dark:border-gray-700">
                                                                                <h2 class="text-2xl font-semibold mb-4 text-gray-800 dark:text-gray-200">
                                                                                    "README.md"
                                                                                </h2>
                                                                                <div
                                                                                    class="prose dark:prose-invert max-w-none"
                                                                                    inner_html=render_markdown(&readme_content)
                                                                                ></div>
                                                                            </div>
                                                                        }
                                                                    })}
                                                            },
                                                        )
                                                    }
                                                };
                                                Either::Left(view)
                                            }
                                            Err(e) => {
                                                Either::Right(
                                                    view! {
                                                        <p class="text-red-500">"Error: " {e.to_string()}</p>
                                                    },
                                                )
                                            }
                                        }
                                    })
                            }}
                        </Suspense>
                    </div>
                </div>
            </div>
        </main>
    }
}
