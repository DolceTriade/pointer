#[cfg(feature = "ssr")]
use crate::db::{SnippetRequest, SymbolReferenceRequest};
use crate::db::{
    SnippetResponse, TreeEntry,
    models::{FileReference, SymbolResult},
};
use leptos::either::EitherOf4;
use leptos::html::Div;
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
        html: String,
        line_count: usize,
        language: Option<String>,
    },
    Binary {
        download_url: String,
    },
    Directory {
        entries: Vec<TreeEntry>,
        readme: Option<String>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SymbolSearchScope {
    Repository,
    Directory,
    File,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SymbolInsightsParams {
    pub repo: String,
    pub branch: String,
    pub path: Option<String>,
    pub symbol: String,
    pub language: Option<String>,
    pub scope: SymbolSearchScope,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SymbolInsightsResponse {
    pub symbol: String,
    pub commit: String,
    pub matches: Vec<SymbolMatch>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SymbolMatch {
    pub definition: SymbolResult,
    pub references: Vec<SymbolReferenceWithSnippet>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SymbolReferenceWithSnippet {
    pub reference: FileReference,
    pub snippet: Option<SnippetResponse>,
}

impl SymbolSearchScope {
    fn as_str(&self) -> &'static str {
        match self {
            SymbolSearchScope::Repository => "repository",
            SymbolSearchScope::Directory => "directory",
            SymbolSearchScope::File => "file",
        }
    }

    fn label(&self) -> &'static str {
        match self {
            SymbolSearchScope::Repository => "Current repository",
            SymbolSearchScope::Directory => "Current directory",
            SymbolSearchScope::File => "Current file",
        }
    }

    fn from_str(value: &str) -> Self {
        match value {
            "directory" => SymbolSearchScope::Directory,
            "file" => SymbolSearchScope::File,
            _ => SymbolSearchScope::Repository,
        }
    }
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

    let commit = db
        .resolve_branch_head(&repo, &branch)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?
        .unwrap_or_else(|| branch.clone());

    let path_str = path.unwrap_or_default();
    // An empty path or a path ending in '/' is a directory.
    let is_dir = path_str.is_empty() || path_str.ends_with('/');

    if is_dir {
        let tree = db
            .get_repo_tree(
                &repo,
                RepoTreeQuery {
                    commit: commit.clone(),
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
                .get_file_content(&repo, &commit, &readme_path)
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
            .get_file_content(&repo, &commit, &path_str)
            .await
            .map_err(|e| ServerFnError::new(e.to_string()))?;

        if file_content.language.is_none() && is_binary(&file_content.content) {
            let download_url = format!(
                "/api/download_raw?repo={}&branch={}&path={}",
                repo, commit, path_str
            );
            return Ok(FileViewerData::Binary { download_url });
        }

        // For text files, we'll add line numbers.
        let line_count = file_content.content.lines().count();

        use autumnus::{HtmlInlineBuilder, formatter::Formatter, languages::Language, themes};
        let lang = p
            .file_name()
            .and_then(|file| file.to_str())
            .map(|file| Language::guess(file, &file_content.content))
            .unwrap_or(Language::PlainText);
        let theme = themes::get("catppuccin_mocha").ok();

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

        Ok(FileViewerData::File {
            html,
            line_count,
            language: file_content.language.clone(),
        })
    }
}

#[server]
pub async fn fetch_symbol_insights(
    params: SymbolInsightsParams,
) -> Result<SymbolInsightsResponse, ServerFnError> {
    use crate::db::{Database, SearchRequest, postgres::PostgresDb};

    if params.symbol.trim().is_empty() {
        return Err(ServerFnError::new("symbol cannot be empty"));
    }

    let state = expect_context::<crate::server::GlobalAppState>();
    let state = state.lock().await;
    let db = PostgresDb::new(state.pool.clone());

    let commit = db
        .resolve_branch_head(&params.repo, &params.branch)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?
        .unwrap_or_else(|| params.branch.clone());

    let mut request = SearchRequest {
        q: None,
        name: Some(params.symbol.clone()),
        name_regex: None,
        namespace: None,
        namespace_prefix: None,
        kind: None,
        language: params.language.clone().map(|lang| vec![lang]),
        repository: Some(params.repo.clone()),
        commit_sha: Some(commit.clone()),
        path: None,
        path_regex: None,
        include_references: Some(false),
        limit: Some(50),
    };

    if let Some(filter) = match params.scope {
        SymbolSearchScope::Repository => None,
        SymbolSearchScope::Directory => params.path.as_ref().and_then(|path| {
            let trimmed = path.trim_matches('/');
            if trimmed.is_empty() {
                None
            } else {
                let dir = if path.ends_with('/') {
                    trimmed.to_string()
                } else {
                    trimmed
                        .rsplit_once('/')
                        .map(|(dir, _)| dir.to_string())
                        .unwrap_or_default()
                };

                if dir.is_empty() {
                    None
                } else {
                    Some(format!("{dir}/"))
                }
            }
        }),
        SymbolSearchScope::File => params.path.as_ref().and_then(|path| {
            let trimmed = path.trim_matches('/');
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        }),
    } {
        request.path = Some(filter);
    }

    let search_response = db
        .search_symbols(request)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;

    let mut matches = Vec::with_capacity(search_response.symbols.len());

    for definition in search_response.symbols {
        let references = db
            .get_symbol_references(SymbolReferenceRequest {
                repository: definition.repository.clone(),
                commit_sha: definition.commit_sha.clone(),
                fully_qualified: definition.fully_qualified.clone(),
            })
            .await
            .map_err(|e| ServerFnError::new(e.to_string()))?;

        let mut enriched = Vec::with_capacity(references.references.len());
        for reference in references.references {
            let line = reference.line.max(1) as u32;
            let snippet = match db
                .get_file_snippet(SnippetRequest {
                    repository: reference.repository.clone(),
                    commit_sha: reference.commit_sha.clone(),
                    file_path: reference.file_path.clone(),
                    line,
                    context: Some(2),
                })
                .await
            {
                Ok(snippet) => Some(snippet),
                Err(err) => {
                    tracing::warn!(
                        "Failed to fetch snippet for {}:{}:{}: {err}",
                        reference.repository,
                        reference.file_path,
                        line
                    );
                    None
                }
            };

            enriched.push(SymbolReferenceWithSnippet { reference, snippet });
        }

        matches.push(SymbolMatch {
            definition,
            references: enriched,
        });
    }

    Ok(SymbolInsightsResponse {
        symbol: params.symbol,
        commit,
        matches,
    })
}

#[cfg(feature = "pulldown-cmark")]
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

#[cfg(not(feature = "pulldown-cmark"))]
pub fn render_markdown(markdown: &str) -> String {
    markdown.to_string()
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
fn FileTreeNodes(
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
                            })
                    } else {
                        None
                    }
                }
            }
        </li>
    }
}

#[component]
fn LineHighlighter() -> impl IntoView {
    let location = use_location();
    let refresh = RwSignal::new(());
    Effect::new(move |_| {
        let hash = location.hash.get();
        // Mega-Hack: The inner_html code view doesn't render by the time this effect runs. So,
        // keep retrying until it appears.
        refresh.get();
        if document().get_element_by_id("code-content").is_none() {
            set_timeout(
                move || {
                    refresh.set(());
                },
                std::time::Duration::from_millis(100),
            );
        }
        if hash.starts_with("#L") {
            let line_id = &hash[2..];
            match document().query_selector(&format!("[data-line='{line_id}']")) {
                Ok(Some(element)) => {
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
                    element.class_list().add_1("line-highlight").unwrap_throw();
                    let options = web_sys::ScrollIntoViewOptions::new();
                    options.set_behavior(web_sys::ScrollBehavior::Auto);
                    options.set_block(web_sys::ScrollLogicalPosition::Start);
                    element.scroll_into_view_with_scroll_into_view_options(&options);
                }
                Err(e) => {
                    tracing::warn!("Element not found: {e:#?}");
                }
                _ => {
                    tracing::warn!("Element not found: {hash}");
                }
            }
        }
    });

    // This component doesn't render anything itself
    view! { <div id="mehigh" class="hidden"></div> }
}

#[component]
fn FileContent(
    html: String,
    line_count: usize,
    selected_symbol: RwSignal<Option<String>>,
) -> impl IntoView {
    let on_mouse_up = {
        let selected_symbol = selected_symbol.clone();
        move |_event: leptos::ev::MouseEvent| {
            if let Some(window) = web_sys::window() {
                match window.get_selection() {
                    Ok(Some(selection)) => {
                        if selection.is_collapsed() {
                            selected_symbol.set(None);
                            return;
                        }
                        let raw: String = selection.to_string().into();
                        let trimmed = raw.trim();
                        if trimmed.is_empty()
                            || raw.contains('\n')
                            || trimmed.chars().any(|c| c.is_whitespace())
                            || trimmed.len() > 128
                        {
                            selected_symbol.set(None);
                        } else {
                            selected_symbol.set(Some(trimmed.to_string()));
                        }
                    }
                    _ => selected_symbol.set(None),
                }
            }
        }
    };

    view! {
        <div class="flex font-mono text-sm overflow-x-auto">
            <div class="text-right text-gray-500 pr-4 select-none">
                {(1..=line_count)
                    .map(|n| {
                        view! {
                            <a href=format!("#L{n}") class="block hover:text-blue-400">
                                {n}
                            </a>
                        }
                    })
                    .collect_view()}
            </div>
            <pre class="flex-grow" on:mouseup=on_mouse_up>
                <code id="code-content" inner_html=html />
            </pre>
            <LineHighlighter/>
        </div>
    }
}

#[component]
fn CodeIntelPanel(
    repo: Signal<String>,
    branch: Signal<String>,
    path: Signal<Option<String>>,
    selected_symbol: RwSignal<Option<String>>,
    language: RwSignal<Option<String>>,
) -> impl IntoView {
    let scope: RwSignal<SymbolSearchScope> = RwSignal::new(SymbolSearchScope::Repository);
    let language_filter = RwSignal::new(language.get_untracked());
    let manual_language_override = RwSignal::new(false);

    Effect::new(move |_| {
        selected_symbol.get();
        manual_language_override.set(false);
        language_filter.set(language.get_untracked());
    });

    Effect::new(move |_| {
        let lang = language.read();
        if !manual_language_override.get() {
            language_filter.set(lang.clone());
        }
    });

    let insights_resource = Resource::new(
        move || {
            (
                selected_symbol.get(),
                repo.get(),
                branch.get(),
                path.get(),
                scope.get(),
                language_filter.get(),
            )
        },
        |(symbol_opt, repo, branch, path, scope, language)| async move {
            if let Some(symbol) = symbol_opt {
                fetch_symbol_insights(SymbolInsightsParams {
                    repo,
                    branch,
                    path,
                    symbol,
                    language,
                    scope,
                })
                .await
                .map(Some)
            } else {
                Ok(None)
            }
        },
    );

    let insights_scroll_container = NodeRef::<Div>::new();
    Effect::new({
        let container = insights_scroll_container.clone();
        move |_| {
            // track updates to the resource so we reset scroll when content changes
            let _ = insights_resource.get();
            if let Some(node) = container.get_untracked() {
                node.set_scroll_top(0);
            }
        }
    });

    view! {
        <aside class="w-80 flex-shrink-0 bg-white dark:bg-gray-800 rounded-lg shadow border border-gray-200 dark:border-gray-700 p-4 sticky top-20 max-h-[calc(100vh-6rem)] overflow-hidden">
            <h2 class="text-xl font-semibold mb-4 text-gray-800 dark:text-gray-200">
                "Code Intelligence"
            </h2>
            <div class="text-sm text-gray-600 dark:text-gray-400 mb-4">
                {move || {
                    selected_symbol
                        .get()
                        .map(|symbol| {
                            Either::Left(
                                view! {
                                    <span class="font-mono text-blue-600 dark:text-blue-400">
                                        {symbol}
                                    </span>
                                },
                            )
                        })
                        .unwrap_or_else(|| {
                            Either::Right(
                                view! {
                                    <span>
                                        "Select text in the file to explore indexed symbols."
                                    </span>
                                },
                            )
                        })
                }}
            </div>
            <div
                class="overflow-y-auto pr-1"
                node_ref=insights_scroll_container
                style="max-height: calc(100vh - 12rem);"
            >
                <div class="space-y-4">
                    <div class="flex flex-col gap-1">
                        <label class="text-xs uppercase tracking-wide text-gray-500 dark:text-gray-400">
                            "Scope"
                        </label>
                        <select
                            class="select select-sm select-bordered bg-white dark:bg-gray-800"
                            on:change=move |ev| {
                                let value = event_target_value(&ev);
                                scope.set(SymbolSearchScope::from_str(&value));
                            }
                            prop:value=move || scope.get().as_str().to_string()
                        >
                            <option value="repository">{SymbolSearchScope::Repository.label()}</option>
                            <option value="directory">{SymbolSearchScope::Directory.label()}</option>
                            <option value="file">{SymbolSearchScope::File.label()}</option>
                        </select>
                    </div>
                    <div class="flex flex-col gap-1">
                        <label class="text-xs uppercase tracking-wide text-gray-500 dark:text-gray-400">
                            "Language"
                        </label>
                        <select
                            class="select select-sm select-bordered bg-white dark:bg-gray-800"
                            on:change=move |ev| {
                                let value = event_target_value(&ev);
                                manual_language_override.set(true);
                                if value.is_empty() {
                                    language_filter.set(None);
                                } else {
                                    language_filter.set(Some(value));
                                }
                            }
                            prop:value=move || language_filter.get().unwrap_or_default()
                        >
                            <option value="">"All languages"</option>
                            {move || {
                                language
                                    .get()
                                    .map(|lang| {
                                        let display = format!("File language: {}", lang);
                                        view! { <option value=lang.clone()>{display}</option> }
                                    })
                            }}
                        </select>
                        {move || {
                            manual_language_override
                                .get()
                                .then(|| {
                                    view! {
                                        <button
                                            class="text-xs text-blue-600 dark:text-blue-400 hover:underline text-left"
                                            on:click=move |_| {
                                                manual_language_override.set(false);
                                                language_filter.set(language.get());
                                            }
                                        >
                                            "Reset to file language"
                                        </button>
                                    }
                                })
                        }}
                    </div>
                </div>
                <div class="mt-6">
                <Show
                    when=move || selected_symbol.get().is_some()
                    fallback=move || {
                        view! {
                            <p class="text-sm text-gray-500 dark:text-gray-400">
                                "Highlight a symbol in the editor to see definitions and references."
                            </p>
                        }
                    }
                >
                    <Suspense fallback=move || {
                        view! {
                            <p class="text-sm text-gray-500 dark:text-gray-400">
                                "Gathering symbol data..."
                            </p>
                        }
                    }>
                        {move || {
                            insights_resource
                                .get()
                                .map(|result| match result {
                                    Ok(Some(data)) => {
                                        let SymbolInsightsResponse { commit, matches, .. } = data;
                                        if matches.is_empty() {
                                            view! {
                                                <p class="text-sm text-gray-500 dark:text-gray-400">
                                                    "No indexed symbols matched this selection."
                                                </p>
                                            }
                                            .into_any()
                                        } else {
                                            view! {
                                                <div class="space-y-6">
                                                    {matches
                                                        .into_iter()
                                                        .map(|symbol_match| {
                                                            let definition = symbol_match.definition;
                                                            let references = symbol_match.references;
                                                            let definition_language = definition
                                                                .language
                                                                .clone()
                                                                .unwrap_or_else(|| "unknown".to_string());
                                                            let (definition_line, definition_link) = if let Some(line) = definition.line {
                                                                let link = format!(
                                                                    "/repo/{}/tree/{}/{}#L{}",
                                                                    definition.repository,
                                                                    commit,
                                                                    definition.file_path,
                                                                    line,
                                                                );
                                                                (Some(line), link)
                                                            } else {
                                                                let link = format!(
                                                                    "/repo/{}/tree/{}/{}",
                                                                    definition.repository,
                                                                    commit,
                                                                    definition.file_path,
                                                                );
                                                                (None, link)
                                                            };
                                                            let reference_count = references.len();
                                                            view! {
                                                                <div class="rounded border border-gray-200 dark:border-gray-700 p-3">
                                                                    <div class="flex items-center justify-between gap-2">
                                                                        <span class="font-mono text-sm text-blue-600 dark:text-blue-400">
                                                                            {definition.symbol.clone()}
                                                                        </span>
                                                                        <span class="text-xs text-gray-500 dark:text-gray-400 uppercase">
                                                                            {definition_language}
                                                                        </span>
                                                                    </div>
                                                                    {definition.namespace.as_ref().map(|ns| {
                                                                        view! {
                                                                            <div class="text-xs text-gray-500 dark:text-gray-400 mt-1">
                                                                                {ns.clone()}
                                                                            </div>
                                                                        }
                                                                    })}
                                                                    <A
                                                                        href=definition_link
                                                                        attr:class="block mt-2 text-sm text-blue-600 dark:text-blue-400 hover:underline truncate"
                                                                    >
                                                                        {definition_line
                                                                            .map(|line| format!("{}:{}", definition.file_path, line))
                                                                            .unwrap_or_else(|| definition.file_path.clone())}
                                                                    </A>
                                                                    {definition_line.map(|line| {
                                                                        view! {
                                                                            <p class="text-xs text-gray-500 dark:text-gray-400 mt-1">
                                                                                {format!("Line {}", line)}
                                                                            </p>
                                                                        }
                                                                    })}
                                                                    {definition.kind.as_ref().map(|kind| {
                                                                        view! {
                                                                            <p class="text-xs text-gray-500 dark:text-gray-400 mt-1 uppercase">
                                                                                {kind.clone()}
                                                                            </p>
                                                                        }
                                                                    })}
                                                                    <div class="mt-4">
                                                                        <h3 class="text-xs font-semibold uppercase tracking-wide text-gray-500 dark:text-gray-400">
                                                                            {format!("References ({reference_count})")}
                                                                        </h3>
                                                                        {if references.is_empty() {
                                                                            Either::Left(view! {
                                                                                <p class="text-xs text-gray-500 dark:text-gray-400 mt-2">
                                                                                    "No references were indexed for this symbol."
                                                                                </p>
                                                                            })
                                                                        } else {
                                                                            Either::Right(
                                                                                view! {
                                                                                    <div class="mt-3 space-y-3">
                                                                                        {references
                                                                                            .into_iter()
                                                                                            .map(|entry| {
                                                                                                let reference = entry.reference;
                                                                                                let line_number = reference.line.max(1);
                                                                                                let reference_link = format!(
                                                                                                    "/repo/{}/tree/{}/{}#L{}",
                                                                                                    reference.repository,
                                                                                                    reference.commit_sha,
                                                                                                    reference.file_path,
                                                                                                    line_number,
                                                                                                );
                                                                                                view! {
                                                                                                    <A
                                                                                                        href=reference_link
                                                                                                        attr:class="block rounded border border-gray-200 dark:border-gray-700 hover:border-blue-400 dark:hover:border-blue-400/60 transition-colors overflow-x-none"
                                                                                                    >
                                                                                                        <div class="flex items-center justify-between gap-2 px-3 py-2">
                                                                                                            <div class="min-w-0">
                                                                                                                <p class="text-sm text-blue-600 dark:text-blue-400 truncate">
                                                                                                                    {reference.file_path.clone()}
                                                                                                                </p>
                                                                                                                <p class="text-xs text-gray-500 dark:text-gray-400">
                                                                                                                    {format!(
                                                                                                                        "Line {}  •  Column {}",
                                                                                                                        line_number,
                                                                                                                        reference.column
                                                                                                                    )}
                                                                                                                </p>
                                                                                                            </div>
                                                                                                        </div>
                                                                                                        {entry.snippet.map(|snippet| {
                                                                                                            let highlight_line = snippet.highlight_line;
                                                                                                            let start_line = snippet.start_line;
                                                                                                            view! {
                                                                                                                <div class="bg-gray-50 dark:bg-gray-900/50 border-t border-gray-200 dark:border-gray-700 px-3 py-2 text-xs font-mono">
                                                                                                                    {snippet
                                                                                                                        .lines
                                                                                                                        .into_iter()
                                                                                                                        .enumerate()
                                                                                                                        .map(|(idx, text)| {
                                                                                                                            let current_line = start_line + idx as u32;
                                                                                                                            let is_highlight = current_line == highlight_line;
                                                                                                                            let row_class = if is_highlight {
                                                                                                                                "flex gap-3 bg-blue-100/80 dark:bg-blue-900/40 rounded px-2 py-1"
                                                                                                                            } else {
                                                                                                                                "flex gap-3 px-2 py-1"
                                                                                                                            };
                                                                                                                            view! {
                                                                                                                                <div class=row_class>
                                                                                                                                    <span class="w-12 text-right text-[10px] text-gray-500">
                                                                                                                                        {current_line}
                                                                                                                                    </span>
                                                                                                                                    <span class="whitespace-pre-wrap break-words">
                                                                                                                                        {text}
                                                                                                                                    </span>
                                                                                                                                </div>
                                                                                                                            }
                                                                                                                        })
                                                                                                                        .collect_view()}
                                                                                                                </div>
                                                                                                            }
                                                                                                        })}
                                                                                                    </A>
                                                                                                }
                                                                                            })
                                                                                            .collect_view()}
                                                                                    </div>
                                                                                },
                                                                            )
                                                                        }}
                                                                    </div>
                                                                </div>
                                                            }
                                                        })
                                                        .collect_view()}
                                                </div>
                                            }
                                            .into_any()
                                        }
                                    }
                                    Ok(None) => {
                                        view! {
                                            <p class="text-sm text-gray-500 dark:text-gray-400">
                                                "Select a symbol to see indexed results."
                                            </p>
                                        }
                                            .into_any()
                                    }
                                    Err(err) => {
                                        view! {
                                            <p class="text-sm text-red-500">
                                                "Error loading symbol insights: " {err.to_string()}
                                            </p>
                                        }
                                            .into_any()
                                    }
                                })
                        }}
                    </Suspense>
                </Show>
            </div>
            </div>
        </aside>
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
    let repo_for_data = repo.clone();
    let branch_for_data = branch.clone();
    let path_for_data = path.clone();
    let data_resource = Resource::new(
        move || (repo_for_data(), branch_for_data(), path_for_data()),
        |(repo, branch, path)| get_file_viewer_data(repo, branch, path),
    );

    // Resource for the file tree (left side), always fetching the root
    let repo_for_tree = repo.clone();
    let branch_for_tree = branch.clone();
    let tree_resource = Resource::new(
        move || (repo_for_tree(), branch_for_tree()),
        |(repo, branch)| get_file_viewer_data(repo, branch, Some("".to_string())),
    );

    let expanded_dirs = RwSignal::new(HashSet::<String>::new());
    let selected_symbol = RwSignal::new(None::<String>);
    let file_language = RwSignal::new(None::<String>);

    Effect::new(move |_| {
        if let Some(Ok(fv)) = data_resource.read().as_ref() {
            match fv {
                FileViewerData::File { language, .. } => {
                    file_language.set(language.clone());
                    selected_symbol.set(None);
                }
                _ => {
                    file_language.set(None);
                    selected_symbol.set(None);
                }
            }
        }
    });

    view! {
        <main class="flex-grow flex flex-col justify-start pt-8 p-4">
            <div class="max-w-full w-full">
                <Breadcrumbs
                    repo=repo.into()
                    branch=branch.into()
                    path=Signal::derive(move || path().unwrap_or_default())
                />
                <div class="flex gap-6 items-start">
                    // Left Panel: File Tree
                    <div class="w-64 flex-shrink-0 bg-white dark:bg-gray-800 rounded-lg shadow p-4 border border-gray-200 dark:border-gray-700 self-start">
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

                    <div class="flex-1 min-w-0 flex gap-6 items-start">
                        <div class="flex-1 min-w-0">
                            <Suspense fallback=move || {
                                view! { <p>"Loading content..."</p> }
                            }>
                                {move || {
                                    data_resource
                                        .get()
                                        .map(|result| match result {
                                            Ok(data) => {
                                                match data {
                                                    FileViewerData::File { html, line_count, .. } => {
                                                        EitherOf4::A(
                                                            view! {
                                                                <div class="bg-white dark:bg-gray-800 rounded-lg shadow border border-gray-200 dark:border-gray-700 p-4">
                                                                <FileContent
                                                                    html=html
                                                                    line_count=line_count
                                                                    selected_symbol=selected_symbol
                                                                />
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
                        <CodeIntelPanel
                            repo=repo.into()
                            branch=branch.into()
                            path=path.into()
                            selected_symbol=selected_symbol
                            language=file_language.into()
                        />
                    </div>
                </div>
            </div>
        </main>
    }
}
