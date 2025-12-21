use crate::db::TreeEntry;
use leptos::either::{Either, EitherOf4};
use leptos::prelude::*;
use leptos_router::components::A;
use leptos_router::hooks::use_params;
use leptos_router::params::Params;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use crate::components::breadcrumbs::Breadcrumbs;
use crate::components::code_intel_panel::CodeIntelPanel;
use crate::components::file_content::FileContent;
use crate::components::file_tree::{DirectoryIcon, FileIcon, FileTreeNode};
use crate::components::quick_navigator::FileQuickNavigator;

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
        content: String,
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
    Custom,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SymbolInsightsParams {
    pub repo: String,
    pub branch: String,
    pub path: Option<String>,
    pub symbol: String,
    pub language: Option<String>,
    pub scope: SymbolSearchScope,
    #[serde(default)]
    pub include_paths: Vec<String>,
    #[serde(default)]
    pub excluded_paths: Vec<String>,
}

impl SymbolSearchScope {
    pub fn as_str(&self) -> &'static str {
        match self {
            SymbolSearchScope::Repository => "repository",
            SymbolSearchScope::Directory => "directory",
            SymbolSearchScope::File => "file",
            SymbolSearchScope::Custom => "custom",
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            SymbolSearchScope::Repository => "Current repository",
            SymbolSearchScope::Directory => "Current directory",
            SymbolSearchScope::File => "Current file",
            SymbolSearchScope::Custom => "Custom filter",
        }
    }

    pub fn from_str(value: &str) -> Self {
        match value {
            "directory" => SymbolSearchScope::Directory,
            "file" => SymbolSearchScope::File,
            "custom" => SymbolSearchScope::Custom,
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

        use autumnus::{highlight, OptionsBuilder, HtmlInlineBuilder, languages::Language, themes};

        let lang = p
            .file_name()
            .and_then(|file| file.to_str())
            .map(|file| Language::guess(Some(file), &file_content.content))
            .unwrap_or(Language::PlainText);
        let theme = themes::get("catppuccin_mocha").ok();
        let formatter = HtmlInlineBuilder::new()
            .lang(lang)
            .theme(theme)
            .pre_class(Some("code-block"))
            .italic(false)
            .include_highlights(false)
            .build()
            .unwrap();

        let options = OptionsBuilder::new()
            .formatter(Box::new(formatter))
            .build()
            .unwrap();

        let html = highlight(&file_content.content, options);

        Ok(FileViewerData::File {
            html,
            line_count,
            language: file_content.language.clone(),
            content: file_content.content.clone(),
        })
    }
}

#[server]
pub async fn search_repo_paths(
    repo: String,
    branch: String,
    query: String,
    limit: Option<u16>,
) -> Result<Vec<TreeEntry>, ServerFnError> {
    use crate::db::{Database, postgres::PostgresDb};

    let trimmed = query.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }

    let state = expect_context::<crate::server::GlobalAppState>();
    let state = state.lock().await;
    let db = PostgresDb::new(state.pool.clone());

    let commit = db
        .resolve_branch_head(&repo, &branch)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?
        .unwrap_or_else(|| branch.clone());

    let limit = limit.unwrap_or(10).min(50) as i64;
    db.search_repo_paths(&repo, &commit, trimmed, limit)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))
}

#[server]
pub async fn fetch_symbol_insights(
    params: SymbolInsightsParams,
) -> Result<crate::components::code_intel_panel::SymbolInsightsResponse, ServerFnError> {
    use crate::components::breadcrumbs::directory_prefix;
    use crate::components::code_intel_panel::{
        SymbolInsightsResponse, SymbolMatch, SymbolReferenceWithSnippet,
    };
    use crate::db::{Database, SearchRequest, models::FileReference, postgres::PostgresDb};

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
        path_hint: None,
        include_paths: params.include_paths.clone(),
        excluded_paths: params.excluded_paths.clone(),
        include_references: Some(true),
        limit: Some(50),
    };

    let dir_hint = params.path.as_deref().and_then(directory_prefix);

    let file_hint = params
        .path
        .as_deref()
        .map(|path| path.trim_matches('/'))
        .filter(|trimmed| !trimmed.is_empty())
        .map(str::to_string);

    let (path_filter, path_hint) = match params.scope {
        SymbolSearchScope::Repository => (None, dir_hint.clone().or(file_hint.clone())),
        SymbolSearchScope::Directory => {
            let filter = dir_hint.clone();
            (filter.clone(), filter)
        }
        SymbolSearchScope::File => {
            let filter = file_hint.clone();
            (filter.clone(), filter)
        }
        SymbolSearchScope::Custom => (None, dir_hint.clone().or(file_hint.clone())),
    };

    request.path = path_filter;
    request.path_hint = path_hint;
    if !request.include_paths.is_empty() {
        request.include_paths.sort();
        request.include_paths.dedup();
    }
    if !request.excluded_paths.is_empty() {
        request.excluded_paths.sort();
        request.excluded_paths.dedup();
    }

    let search_response = db
        .search_symbols(request)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;

    let mut matches = Vec::with_capacity(search_response.symbols.len());

    for mut definition in search_response.symbols {
        let references = definition.references.take().unwrap_or_default();

        let mut reference_entries = Vec::with_capacity(references.len());
        let mut snippet_requests = Vec::with_capacity(references.len());

        for reference in references {
            let line = reference.line.max(1);
            let file_reference = FileReference {
                repository: reference.repository.clone(),
                commit_sha: reference.commit_sha.clone(),
                file_path: reference.file_path.clone(),
                namespace: reference.namespace.clone(),
                name: reference.name.clone(),
                kind: reference.kind.clone(),
                line: reference.line.try_into().unwrap_or(i32::MAX),
                column: reference.column.try_into().unwrap_or(i32::MAX),
            };

            snippet_requests.push(crate::db::SnippetRequest {
                repository: file_reference.repository.clone(),
                commit_sha: file_reference.commit_sha.clone(),
                file_path: file_reference.file_path.clone(),
                line: line.max(1) as u32,
                context: Some(1),
                highlight: Some(reference.name.clone()),
                case_sensitive: Some(true),
            });

            reference_entries.push(file_reference);
        }

        let snippet_responses = if snippet_requests.is_empty() {
            Vec::new()
        } else {
            match db.get_file_snippets(snippet_requests).await {
                Ok(snippets) => snippets,
                Err(err) => {
                    tracing::warn!(
                        "Failed to fetch snippets for {} references: {err}",
                        reference_entries.len()
                    );
                    Vec::new()
                }
            }
        };

        let mut enriched = Vec::with_capacity(reference_entries.len());
        for (idx, file_reference) in reference_entries.into_iter().enumerate() {
            let snippet = snippet_responses.get(idx).cloned();
            enriched.push(SymbolReferenceWithSnippet {
                reference: file_reference,
                snippet,
            });
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
    options.insert(Options::ENABLE_GFM);

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
    let included_paths = RwSignal::new(Vec::<String>::new());
    let excluded_paths = RwSignal::new(Vec::<String>::new());

    Effect::new(move |_| {
        let state = data_resource.read();
        let state_ref = state.as_ref();

        let repo_name = repo();
        let branch_name = branch();
        let path_value = path().unwrap_or_default();

        let context_label = if path_value.is_empty() {
            format!("{}@{}", repo_name, branch_name)
        } else {
            format!("{}@{}:{}", repo_name, branch_name, path_value)
        };

        let title = match state_ref {
            Some(Ok(FileViewerData::File { .. })) => format!("{context_label} · Pointer"),
            Some(Ok(FileViewerData::Binary { .. })) => {
                format!("Binary · {context_label} · Pointer")
            }
            Some(Ok(FileViewerData::Directory { .. })) => {
                format!("Directory · {context_label} · Pointer")
            }
            Some(Err(_)) => format!("Error loading {context_label} · Pointer"),
            None => format!("Loading {context_label} · Pointer"),
        };
        document().set_title(&title);

        if let Some(Ok(fv)) = state_ref {
            match fv {
                FileViewerData::File { language, .. } => {
                    file_language.set(language.clone());
                    selected_symbol.set(None);
                    included_paths.set(Vec::new());
                    excluded_paths.set(Vec::new());
                }
                _ => {
                    file_language.set(None);
                    selected_symbol.set(None);
                    included_paths.set(Vec::new());
                    excluded_paths.set(Vec::new());
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
                        <FileQuickNavigator repo=repo.into() branch=branch.into() />
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
                                                    FileViewerData::File {
                                                        html,
                                                        line_count,
                                                        language,
                                                        content,
                                                    } => {
                                                        EitherOf4::A(
                                                            view! {
                                                                <div class="bg-white dark:bg-gray-800 rounded-lg shadow border border-gray-200 dark:border-gray-700 p-4">
                                                                    <FileContent
                                                                        html=html
                                                                        line_count=line_count
                                                                        selected_symbol=selected_symbol
                                                                        content=content
                                                                        language=language
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
                                                                                <article
                                                                                    class="prose dark:prose-invert prose-headings:underline dark:prose-headings:text-gray-200 text-gray-800 dark:text-gray-200 max-w-none"
                                                                                    inner_html=render_markdown(&readme_content)
                                                                                ></article>
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
                            included_paths=included_paths
                            excluded_paths=excluded_paths
                        />
                    </div>
                </div>
            </div>
        </main>
    }
}
