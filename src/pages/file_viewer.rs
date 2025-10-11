use crate::db::TreeEntry;
use leptos::either::EitherOf4;
use leptos::{either::Either, prelude::*};
use leptos_router::components::A;
use leptos_router::hooks::{use_location, use_params};
use leptos_router::params::Params;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use web_sys::wasm_bindgen::JsCast;
use web_sys::wasm_bindgen::UnwrapThrowExt;

#[derive(Params, PartialEq, Clone, Debug)]
pub struct FileViewerParams {
    pub repo: String,
    pub branch: String,
    pub path: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum FileViewerData {
    File {
        lines: Vec<String>,
        line_numbers: Vec<usize>,
    },
    Binary {
        download_url: String,
    },
    Directory {
        entries: Vec<TreeEntry>,
        readme: Option<String>,
    },
}

#[cfg(feature = "ssr")]
fn is_binary(content: &str) -> bool {
    // Simple heuristic: check for NUL byte.
    content.as_bytes().contains(&0)
}

#[server]
pub async fn get_file_viewer_data(
    repo: String,
    branch: String,
    path: Option<String>,
) -> Result<FileViewerData, ServerFnError> {
    use crate::db::{Database, RepoTreeQuery, postgres::PostgresDb};
    use std::path::Path;

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
            Some(file_content.content)
        } else {
            None
        };

        Ok(FileViewerData::Directory {
            entries: tree.entries,
            readme,
        })
    } else {
        let p = Path::new(&path_str);
        // This is a file path
        let file_content = db
            .get_file_content(&repo, &branch, &path_str)
            .await
            .map_err(|e| ServerFnError::new(e.to_string()))?;

        if file_content.language.is_none() && is_binary(&file_content.content) {
            let download_url = format!(
                "/api/download_raw?repo={}&branch={}&path={}",
                repo, branch, path_str
            );
            return Ok(FileViewerData::Binary { download_url });
        }

        // For text files, we'll add line numbers.
        let line_count = file_content.content.lines().count();
        let line_numbers: Vec<usize> = (1..=line_count).collect();

        use autumnus::{HtmlInlineBuilder, formatter::Formatter, languages::Language, themes};
        let lang = p
            .file_name()
            .and_then(|file| file.to_str())
            .map(|file| Language::guess(file, &file_content.content))
            .unwrap_or(Language::PlainText);
        tracing::info!("Guessed {lang:#?} for {path_str}");
        let theme = themes::get("github_light").ok();

        let formatter = HtmlInlineBuilder::new()
            .source(&file_content.content)
            .lang(lang)
            .theme(theme)
            .italic(false)
            .include_highlights(false)
            .build()
            .map_err(|e| ServerFnError::new(format!("failed to build formatter: {e:#?}")))?;

        let mut output = Vec::new();
        formatter
            .format(&mut output)
            .map_err(|e| ServerFnError::new(format!("failed to format: {e:#?}")))?;

        let html = String::from_utf8(output)
            .map_err(|e| ServerFnError::new(format!("Failed to convert to utf8: {e:#?}")))?;

        let lines = html
            .lines()
            .enumerate()
            .map(|(i, line)| format!(r#"<span id="L{}">{}</span>"#, i + 1, line))
            .collect();

        Ok(FileViewerData::File {
            lines,
            line_numbers,
        })
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
fn FileIcon() -> impl IntoView {
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
                d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"
            ></path>
        </svg>
    }
}

#[component]
fn DirectoryIcon() -> impl IntoView {
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
fn Breadcrumbs(
    repo: Signal<String>,
    branch: Signal<String>,
    path: Signal<String>,
) -> impl IntoView {
    let segments = Memo::new(move |_| {
        let mut segs = Vec::new();
        let mut current_path = String::new();
        let path_val = path.get();
        let path_parts: Vec<&str> = path_val.split('/').filter(|s| !s.is_empty()).collect();

        for (i, segment) in path_parts.iter().enumerate() {
            current_path.push_str(segment);
            let is_last = i == path_parts.len() - 1;
            // All non-last segments are directories and need a trailing slash.
            // The last segment is a directory only if the original path ends with a slash.
            if !is_last || path_val.ends_with('/') {
                current_path.push('/');
            }
            segs.push((
                segment.to_string(),
                current_path.clone(),
                is_last && !path_val.ends_with('/'),
            ));
        }
        segs
    });

    view! {
        <div class="text-sm breadcrumbs mb-6">
            <ul>
                <li>
                    <A href=move || format!("/repo/{}", repo())>{move || repo()}</A>
                </li>
                <li>
                    <A href=move || {
                        format!("/repo/{}/tree/{}/", repo(), branch())
                    }>{move || branch()}</A>
                </li>
                <For
                    each=move || segments.get()
                    key=|(_, p, _)| p.clone()
                    children=move |(name, p, is_last)| {
                        let full_path = format!("/repo/{}/tree/{}/{}", repo.get(), branch.get(), p);
                        view! {
                            <li>
                                {if is_last {
                                    Either::Left(view! { <span>{name}</span> })
                                } else {
                                    Either::Right(view! { <A href=full_path>{name}</A> })
                                }}
                            </li>
                        }
                    }
                />
            </ul>
        </div>
    }
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
        move || (is_dir, expanded.get().contains(&path), repo(), branch()),
        move |(is_dir, is_expanded, repo, branch)| {
            let entry = child_entry.clone();
            async move {
                if is_dir && is_expanded {
                    if let Ok(FileViewerData::Directory { entries, .. }) =
                        get_file_viewer_data(repo, branch, Some(entry.path.clone() + "/")).await
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
                    Either::Left(
                        // "▶" "▼"
                        view! {
                            <span class="w-4 text-gray-500">{icon}</span>
                            <DirectoryIcon />
                            <span class="ml-1 text-blue-600 hover:underline truncate" title=name>
                                {entry.name}
                            </span>
                        },
                    )
                } else {
                    let name = entry.name.clone();
                    Either::Right(
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
fn LineHighlighter() -> impl IntoView {
    let location = use_location();

    Effect::new(move |_| {
        let hash = location.hash.get();
        if hash.starts_with("#L") {
            let line_id = &hash[1..];
            if let Some(element) = document().get_element_by_id(line_id) {
                // Remove existing highlights
                let highlighted = document()
                    .query_selector_all(".line-highlight")
                    .unwrap_throw();
                for i in 0..highlighted.length() {
                    if let Some(el) = highlighted
                        .item(i)
                        .and_then(|n| n.dyn_into::<web_sys::Element>().ok())
                    {
                        el.class_list().remove_1("line-highlight").unwrap_throw();
                    }
                }

                // Add new highlight
                element.class_list().add_1("line-highlight").unwrap_throw();

                // Scroll into view
                let options = web_sys::ScrollIntoViewOptions::new();
                options.set_behavior(web_sys::ScrollBehavior::Smooth);
                options.set_block(web_sys::ScrollLogicalPosition::Center);
                element.scroll_into_view_with_scroll_into_view_options(&options);
            }
        }
    });

    // This component doesn't render anything itself
    view! { <div class="hidden"></div> }
}

#[component]
fn FileContent(lines: Vec<String>, line_numbers: Vec<usize>) -> impl IntoView {
    view! {
        <div class="flex font-mono text-sm">
            <div class="text-right text-gray-500 pr-4 select-none">
                {line_numbers
                    .into_iter()
                    .map(|n| {
                        view! {
                            <a href=format!("#L{n}") class="block hover:text-blue-400">
                                {n}
                            </a>
                        }
                    })
                    .collect_view()}
            </div>
            <pre class="flex-grow overflow-auto">
                <code inner_html=lines.join("\n")></code>
            </pre>
        </div>
    }
}

#[component]
pub fn FileViewer() -> impl IntoView {
    let params = use_params::<FileViewerParams>();
    let repo = Memo::new(move |_| {
        params
            .read()
            .as_ref()
            .map(|p| p.repo.clone())
            .ok()
            .unwrap_or_default()
    });
    let branch = Memo::new(move |_| {
        params
            .read()
            .as_ref()
            .map(|p| p.branch.clone())
            .ok()
            .unwrap_or_default()
    });
    let path = Memo::new(move |_| {
        params
            .read()
            .as_ref()
            .map(|p| p.path.clone())
            .ok()
            .flatten()
    });

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
                <Breadcrumbs
                    repo=repo.into()
                    branch=branch.into()
                    path=Signal::derive(move || path().unwrap_or_default())
                />
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
                                                                        repo=repo.into()
                                                                        branch=branch.into()
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
                                    .map(|result| match result {
                                        Ok(data) => {
                                            match data {
                                                FileViewerData::File { lines, line_numbers } => {
                                                    EitherOf4::A(
                                                        view! {
                                                            <LineHighlighter />
                                                            <div class="bg-white dark:bg-gray-800 rounded-lg shadow border border-gray-200 dark:border-gray-700 p-4 overflow-auto">
                                                                <FileContent lines=lines line_numbers=line_numbers />
                                                            </div>
                                                        },
                                                    )
                                                }
                                                FileViewerData::Binary { download_url } => {
                                                    EitherOf4::B(
                                                        view! {
                                                            <div class="bg-white dark:bg-gray-800 rounded-lg shadow p-8 border border-gray-200 dark:border-gray-700 text-center">
                                                                <p class="mb-4">
                                                                    "This is a binary file and cannot be displayed."
                                                                </p>
                                                                <a
                                                                    href=download_url
                                                                    class="bg-blue-500 text-white font-bold py-2 px-4 rounded hover:bg-blue-700"
                                                                >
                                                                    "Download"
                                                                </a>
                                                            </div>
                                                        },
                                                    )
                                                }
                                                FileViewerData::Directory { entries, readme } => {
                                                    EitherOf4::C(
                                                        view! {
                                                            // Top half: File list
                                                            <div class="bg-white dark:bg-gray-800 rounded-lg shadow p-4 border border-gray-200 dark:border-gray-700 mb-6">
                                                                <div class="grid grid-cols-2 md:grid-cols-3 gap-4">
                                                                    {entries
                                                                        .into_iter()
                                                                        .map(move |entry| {
                                                                            let mut link = format!(
                                                                                "/repo/{}/tree/{}/{}",
                                                                                repo(),
                                                                                branch(),
                                                                                entry.path,
                                                                            );
                                                                            if entry.kind == "dir" {
                                                                                link.push('/');
                                                                            }
                                                                            let icon = if entry.kind == "dir" {
                                                                                Either::Left(view! { <DirectoryIcon /> })
                                                                            } else {
                                                                                Either::Right(view! { <FileIcon /> })
                                                                            };
                                                                            let name = entry.name.clone();
                                                                            view! {
                                                                                <A
                                                                                    href=link
                                                                                    attr:class="text-blue-600 hover:underline p-2 rounded hover:bg-gray-100 dark:hover:bg-gray-700 flex items-center gap-2 overflow-hidden"
                                                                                    attr:title=name.clone()
                                                                                >
                                                                                    {icon}
                                                                                    <span class="truncate">{entry.name}</span>
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
                                            }
                                        }
                                        Err(e) => {
                                            EitherOf4::D(
                                                view! {
                                                    <p class="text-red-500">"Error: " {e.to_string()}</p>
                                                },
                                            )
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
