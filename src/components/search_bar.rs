use crate::db::models::SymbolSuggestion;
use crate::dsl::{TextSearchRequest, parse_query, tokenize_for_autocomplete};
use crate::services::search_service::{
    autocomplete_branches, autocomplete_files, autocomplete_languages, autocomplete_paths,
    autocomplete_repositories, autocomplete_symbols,
};
use leptos::either::Either;
use leptos::prelude::*;
use leptos_router::hooks::use_navigate;
use std::rc::Rc;
use web_sys;

#[component]
pub fn SearchBar(
    #[prop(optional)] initial_query: String,
    #[prop(optional)] auto_focus: bool,
    #[prop(optional)] on_complete: Option<Rc<dyn Fn()>>,
    #[prop(optional)] open_in_new_tab: bool,
) -> impl IntoView {
    let (query, set_query) = signal(initial_query);
    let input_ref = NodeRef::<leptos::html::Input>::new();
    let navigate = use_navigate();
    let on_complete_cb = on_complete.clone();
    let has_interacted = RwSignal::new(false);

    let on_search = move || {
        let q = query.get().trim().to_string();
        if !q.is_empty() {
            let encoded = urlencoding::encode(&q);
            let url = format!("/search?q={}&page=1", encoded);
            if open_in_new_tab {
                let opened_in_new_tab = web_sys::window()
                    .and_then(|w| w.open_with_url_and_target(&url, "_blank").ok())
                    .flatten()
                    .is_some();

                if !opened_in_new_tab {
                    navigate(&url, Default::default());
                }
            } else {
                navigate(&url, Default::default());
            }

            if let Some(cb) = on_complete_cb.clone() {
                cb.as_ref()();
            }
        }
    };

    // Focus the input when requested (used by overlay/modal invocations)
    if auto_focus {
        Effect::new({
            let input_ref = input_ref.clone();
            move |_| {
                if let Some(input) = input_ref.get() {
                    let _ = input.focus();
                    let len = input.value().len() as u32;
                    let _ = input.set_selection_range(len, len);
                }
            }
        });
    }

    // Create a reactive validation of the query
    let validation = Memo::new(move |_| {
        if !has_interacted.get() {
            return None;
        }
        let q = query.get();
        if q.is_empty() {
            return None;
        }

        match TextSearchRequest::from_query_str(&q) {
            Ok(_) => Some(ValidationState {
                status: ValidationStatus::Valid,
                message: None,
            }),
            Err(err) => Some(ValidationState {
                status: ValidationStatus::Invalid,
                message: Some(err.to_string()),
            }),
        }
    });

    // DSL syntax hints
    let dsl_hints = vec![
        DslHint {
            syntax: "repo:",
            description: "Search in specific repository",
        },
        DslHint {
            syntax: "path:",
            description: "Search in specific paths",
        },
        DslHint {
            syntax: "file:",
            description: "Search in specific files",
        },
        DslHint {
            syntax: "lang:",
            description: "Search in specific language",
        },
        DslHint {
            syntax: "branch:",
            description: "Search in specific branch",
        },
        DslHint {
            syntax: "regex:",
            description: "Search with regex pattern",
        },
        DslHint {
            syntax: "case:",
            description: "Control case sensitivity (case:yes/no/auto)",
        },
        DslHint {
            syntax: "historical:",
            description: "Include historical commits (historical:yes)",
        },
    ];

    // Example queries for users
    let example_queries = vec![
        "repo:myrepo lang:rust",
        "path:*.rs regex:async",
        "path:README.md lang:markdown historical:yes",
    ];

    let autocomplete_state = Memo::new(move |_| build_autocomplete_state(&query.get()));
    let autocomplete_resource = LocalResource::new(move || {
        let state = autocomplete_state.get();
        async move {
            let limit = 10;
            match state.mode {
                AutocompleteMode::RepoValue => autocomplete_repositories(state.term, limit)
                    .await
                    .map(|repos| AutocompleteResults {
                        repos,
                        ..AutocompleteResults::default()
                    }),
                AutocompleteMode::PathValue => {
                    autocomplete_paths(state.term, state.repo_filters, limit)
                        .await
                        .map(|paths| AutocompleteResults {
                            paths,
                            ..AutocompleteResults::default()
                        })
                }
                AutocompleteMode::Symbol => {
                    autocomplete_symbols(state.term, limit)
                        .await
                        .map(|symbols| AutocompleteResults {
                            symbols,
                            ..AutocompleteResults::default()
                        })
                }
                AutocompleteMode::LangValue => {
                    autocomplete_languages(state.term, state.repo_filters, limit)
                        .await
                        .map(|langs| AutocompleteResults {
                            langs,
                            ..AutocompleteResults::default()
                        })
                }
                AutocompleteMode::BranchValue => {
                    autocomplete_branches(state.term, state.repo_filters, limit)
                        .await
                        .map(|branches| AutocompleteResults {
                            branches,
                            ..AutocompleteResults::default()
                        })
                }
                AutocompleteMode::FileValue => {
                    autocomplete_files(state.term, state.repo_filters, limit)
                        .await
                        .map(|files| AutocompleteResults {
                            files,
                            ..AutocompleteResults::default()
                        })
                }
                _ => Ok(AutocompleteResults::default()),
            }
        }
    });

    let dsl_suggestions = Memo::new({
        let dsl_hints = dsl_hints.clone();
        move |_| {
            let state = autocomplete_state.get();
            if matches!(
                state.mode,
                AutocompleteMode::RepoValue
                    | AutocompleteMode::PathValue
                    | AutocompleteMode::LangValue
                    | AutocompleteMode::BranchValue
                    | AutocompleteMode::FileValue
                    | AutocompleteMode::CaseValue
                    | AutocompleteMode::HistoricalValue
                    | AutocompleteMode::None
            ) {
                return Vec::new();
            }

            let filter = if matches!(state.mode, AutocompleteMode::DslKey) {
                state.term.to_ascii_lowercase()
            } else {
                String::new()
            };

            dsl_hints
                .iter()
                .cloned()
                .filter(|hint| {
                    if filter.is_empty() {
                        true
                    } else {
                        hint.syntax.to_ascii_lowercase().contains(&filter)
                    }
                })
                .collect::<Vec<_>>()
        }
    });

    let show_autocomplete = Memo::new(move |_| !query.get().trim().is_empty());
    let active_index = RwSignal::new(Option::<usize>::None);

    let suggestion_groups = Memo::new(move |_| {
        let state = autocomplete_state.get();
        let results: AutocompleteResults = autocomplete_resource
            .get()
            .and_then(|result| result.ok())
            .unwrap_or_default();
        let mut groups = Vec::new();
        let mut index = 0;

        let active_key = state.active_key.clone().unwrap_or_default();
        let repo_key = if active_key.is_empty() {
            "repo".to_string()
        } else {
            active_key.clone()
        };
        let path_key = if active_key.is_empty() {
            "path".to_string()
        } else {
            active_key
        };

        match state.mode {
            AutocompleteMode::RepoValue => {
                let items = results
                    .repos
                    .into_iter()
                    .map(|repo| {
                        let item = SuggestionItem {
                            label: repo.clone(),
                            replacement: format!("{}:{}", repo_key, repo),
                            subtitle: None,
                            index,
                        };
                        index += 1;
                        item
                    })
                    .collect();
                groups.push(SuggestionGroup {
                    title: "Repositories",
                    items,
                });
            }
            AutocompleteMode::PathValue => {
                let items = results
                    .paths
                    .into_iter()
                    .map(|path| {
                        let item = SuggestionItem {
                            label: path.clone(),
                            replacement: format!("{}:{}", path_key, path),
                            subtitle: None,
                            index,
                        };
                        index += 1;
                        item
                    })
                    .collect();
                groups.push(SuggestionGroup {
                    title: "Paths",
                    items,
                });
            }
            AutocompleteMode::DslKey => {
                let items = dsl_suggestions
                    .get()
                    .into_iter()
                    .map(|hint| {
                        let item = SuggestionItem {
                            label: hint.syntax.to_string(),
                            replacement: hint.syntax.to_string(),
                            subtitle: None,
                            index,
                        };
                        index += 1;
                        item
                    })
                    .collect();
                groups.push(SuggestionGroup {
                    title: "DSL",
                    items,
                });
            }
            AutocompleteMode::Symbol => {
                let symbol_items = results
                    .symbols
                    .into_iter()
                    .map(|symbol| {
                        let item = SuggestionItem {
                            label: symbol.name.clone(),
                            replacement: symbol.name.clone(),
                            subtitle: Some(format!("{}/{}", symbol.repository, symbol.file_path)),
                            index,
                        };
                        index += 1;
                        item
                    })
                    .collect();
                groups.push(SuggestionGroup {
                    title: "Symbols",
                    items: symbol_items,
                });

                let dsl_items = dsl_suggestions
                    .get()
                    .into_iter()
                    .map(|hint| {
                        let item = SuggestionItem {
                            label: hint.syntax.to_string(),
                            replacement: hint.syntax.to_string(),
                            subtitle: None,
                            index,
                        };
                        index += 1;
                        item
                    })
                    .collect();
                groups.push(SuggestionGroup {
                    title: "DSL",
                    items: dsl_items,
                });
            }
            AutocompleteMode::LangValue => {
                let items = results
                    .langs
                    .into_iter()
                    .map(|lang| {
                        let item = SuggestionItem {
                            label: lang.clone(),
                            replacement: format!("lang:{}", lang),
                            subtitle: None,
                            index,
                        };
                        index += 1;
                        item
                    })
                    .collect();
                groups.push(SuggestionGroup {
                    title: "Languages",
                    items,
                });
            }
            AutocompleteMode::BranchValue => {
                let items = results
                    .branches
                    .into_iter()
                    .map(|branch| {
                        let item = SuggestionItem {
                            label: branch.clone(),
                            replacement: format!("branch:{}", branch),
                            subtitle: None,
                            index,
                        };
                        index += 1;
                        item
                    })
                    .collect();
                groups.push(SuggestionGroup {
                    title: "Branches",
                    items,
                });
            }
            AutocompleteMode::FileValue => {
                let items = results
                    .files
                    .into_iter()
                    .map(|file| {
                        let item = SuggestionItem {
                            label: file.clone(),
                            replacement: format!("file:{}", file),
                            subtitle: None,
                            index,
                        };
                        index += 1;
                        item
                    })
                    .collect();
                groups.push(SuggestionGroup {
                    title: "Files",
                    items,
                });
            }
            AutocompleteMode::CaseValue => {
                let term = state.term.to_ascii_lowercase();
                let options = ["yes", "no", "auto"];
                let items = options
                    .iter()
                    .filter(|opt| term.is_empty() || opt.contains(&term))
                    .map(|opt| {
                        let item = SuggestionItem {
                            label: opt.to_string(),
                            replacement: format!("case:{}", opt),
                            subtitle: None,
                            index,
                        };
                        index += 1;
                        item
                    })
                    .collect();
                groups.push(SuggestionGroup {
                    title: "Case",
                    items,
                });
            }
            AutocompleteMode::HistoricalValue => {
                let term = state.term.to_ascii_lowercase();
                let options = ["yes", "no"];
                let items = options
                    .iter()
                    .filter(|opt| term.is_empty() || opt.contains(&term))
                    .map(|opt| {
                        let item = SuggestionItem {
                            label: opt.to_string(),
                            replacement: format!("historical:{}", opt),
                            subtitle: None,
                            index,
                        };
                        index += 1;
                        item
                    })
                    .collect();
                groups.push(SuggestionGroup {
                    title: "Historical",
                    items,
                });
            }
            AutocompleteMode::None => {}
        }

        groups
    });

    let flat_suggestions = Memo::new(move |_| {
        let groups = suggestion_groups.get();
        let mut items = Vec::new();
        for group in groups {
            for item in group.items {
                items.push(item);
            }
        }
        items
    });

    let on_input = move |ev| {
        set_query.set(event_target_value(&ev));
        active_index.set(None);
    };

    let apply_selection = {
        let set_query = set_query.clone();
        move |replacement: &str, active_start: usize| {
            let updated = apply_autocomplete_replacement(&query.get(), active_start, replacement);
            set_query.set(updated);
            active_index.set(None);
        }
    };

    view! {
        <div class="w-full max-w-2xl">
            <div class="group relative">
                <div class=move || {
                    let border = match validation.get().map(|state| state.status) {
                        Some(ValidationStatus::Valid) => {
                            "border-emerald-400 dark:border-emerald-600"
                        }
                        Some(ValidationStatus::Invalid) => "border-rose-400 dark:border-rose-600",
                        None => "border-gray-300 dark:border-gray-700",
                    };
                    format!(
                        "flex items-center rounded-full border shadow-lg overflow-hidden bg-white dark:bg-gray-800 relative transition-colors duration-200 {}",
                        border,
                    )
                }>
                    <input
                        type="text"
                        placeholder="Search for code... (use DSL: repo:myrepo lang:rust)"
                        class="w-full px-8 py-4 bg-transparent focus:outline-none pr-16 text-gray-900 dark:text-gray-100 placeholder-gray-500 dark:placeholder-gray-400"
                        node_ref=input_ref
                        prop:value=query
                        on:input=on_input
                        on:keydown={
                            let func = on_search.clone();
                            move |ev| {
                                if !has_interacted.get() {
                                    has_interacted.set(true);
                                }
                                if ev.key() == "Enter" {
                                    ev.prevent_default();
                                    let suggestions = flat_suggestions.get();
                                    if let Some(idx) = active_index.get() {
                                        if let Some(suggestion) = suggestions.get(idx) {
                                            apply_selection(
                                                &suggestion.replacement,
                                                autocomplete_state.get().active_start,
                                            );
                                            return;
                                        }
                                    }
                                    func();
                                } else if ev.key() == "ArrowDown" {
                                    ev.prevent_default();
                                    let suggestions = flat_suggestions.get();
                                    if !suggestions.is_empty() {
                                        let next = match active_index.get() {
                                            Some(idx) => (idx + 1) % suggestions.len(),
                                            None => 0,
                                        };
                                        active_index.set(Some(next));
                                    }
                                } else if ev.key() == "ArrowUp" {
                                    ev.prevent_default();
                                    let suggestions = flat_suggestions.get();
                                    if !suggestions.is_empty() {
                                        let next = match active_index.get() {
                                            Some(idx) => {
                                                if idx == 0 { suggestions.len() - 1 } else { idx - 1 }
                                            }
                                            None => suggestions.len() - 1,
                                        };
                                        active_index.set(Some(next));
                                    }
                                } else if ev.key() == "Tab" {
                                    let suggestions = flat_suggestions.get();
                                    if let Some(idx) = active_index.get() {
                                        if let Some(suggestion) = suggestions.get(idx) {
                                            ev.prevent_default();
                                            apply_selection(
                                                &suggestion.replacement,
                                                autocomplete_state.get().active_start,
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    />

                    <button
                        class="px-6 py-4 bg-blue-600 text-white shadow-sm hover:bg-blue-700 hover:shadow-md hover:-translate-y-px dark:bg-blue-500 dark:hover:bg-blue-400 transition-all duration-200"
                        on:click=move |_| on_search()
                    >
                        <svg
                            xmlns="http://www.w3.org/2000/svg"
                            class="h-6 w-6"
                            fill="none"
                            viewBox="0 0 24 24"
                            stroke="currentColor"
                        >
                            <path
                                stroke-linecap="round"
                                stroke-linejoin="round"
                                stroke-width="2"
                                d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"
                            />
                        </svg>
                    </button>
                </div>
                <div class="flex items-center justify-between text-xs text-gray-600 dark:text-gray-400 mt-2 px-2">
                    <span class="flex items-center gap-1">
                        <kbd class="px-1.5 py-0.5 rounded border border-gray-300 dark:border-gray-700 bg-gray-100 dark:bg-gray-800 text-gray-700 dark:text-gray-200">
                            /
                        </kbd>
                        <span>"focus search"</span>
                    </span>
                    <span>"Enter opens results in a new tab"</span>
                </div>
                {move || {
                    validation
                        .get()
                        .and_then(|state| {
                            if state.status == ValidationStatus::Invalid {
                                state
                                    .message
                                    .map(|msg| {
                                        let label = format!("Invalid query: {}", msg);
                                        view! {
                                            <div class="mt-2 px-3 py-2 text-xs text-red-700 dark:text-red-200 bg-red-50 dark:bg-red-950/40 border border-red-300 dark:border-red-800 rounded-md flex items-start gap-2">
                                                <span class="font-mono text-red-600 dark:text-red-200">
                                                    "X"
                                                </span>
                                                <span>{label}</span>
                                            </div>
                                        }
                                    })
                            } else {
                                None
                            }
                        })
                }}

                // Autocomplete or tutorial popup
                <div class="absolute hidden mt-2 w-full bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg shadow-lg z-10 opacity-0 scale-95 transition-all duration-200 group-focus-within:opacity-100 group-focus-within:scale-100 group-focus-within:block">
                    <Show
                        when=move || show_autocomplete.get()
                        fallback=move || {
                            view! {
                                <div class="p-3 text-sm text-gray-600 dark:text-gray-300">
                                    <p class="font-semibold mb-2">DSL Search Syntax:</p>
                                    <div class="grid grid-cols-2 gap-2">
                                        {dsl_hints
                                            .iter()
                                            .cloned()
                                            .map(|hint| {
                                                let syntax = hint.syntax.to_string();
                                                let syntax_label = syntax.clone();
                                                let description = hint.description.to_string();
                                                view! {
                                                    <div
                                                        class="flex cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-700 p-1 rounded"
                                                        on:mousedown=move |ev| {
                                                            ev.prevent_default();
                                                            apply_selection(&syntax, query.get().len());
                                                        }
                                                    >
                                                        <span class="font-mono text-blue-600 dark:text-blue-400 font-semibold mr-2">
                                                            {syntax_label}
                                                        </span>
                                                        <span class="text-gray-600 dark:text-gray-400">
                                                            {description}
                                                        </span>
                                                    </div>
                                                }
                                            })
                                            .collect_view()}
                                    </div>
                                    <div class="mt-3 pt-2 border-t border-gray-200 dark:border-gray-700">
                                        <p class="font-semibold mb-1">Examples:</p>
                                        <div class="space-y-1">
                                            {example_queries
                                                .iter()
                                                .cloned()
                                                .map(|ex| {
                                                    view! {
                                                        <div
                                                            class="font-mono text-sm bg-gray-100 dark:bg-gray-700 p-2 rounded cursor-pointer hover:bg-gray-200 dark:hover:bg-gray-600"
                                                            on:mousedown=move |ev| {
                                                                ev.prevent_default();
                                                            set_query.set(ex.to_string());
                                                            active_index.set(None);
                                                        }
                                                    >
                                                        {ex}
                                                    </div>
                                                    }
                                                })
                                                .collect_view()}
                                        </div>
                                    </div>
                                </div>
                            }
                        }
                    >
                        {move || {
                            let groups = suggestion_groups.get();
                            let active_idx = active_index.get();
                            let active_start = autocomplete_state.get().active_start;
                            let current_query = query.get();
                            let set_query = set_query.clone();
                            let mut dsl_group = None;
                            let mut symbol_group = None;
                            let mut other_groups = Vec::new();
                            for group in groups {
                                match group.title {
                                    "DSL" => dsl_group = Some(group),
                                    "Symbols" => symbol_group = Some(group),
                                    _ => other_groups.push(group),
                                }
                            }
                            let symbol_column = symbol_group
                                .map(|group| {
                                        render_group_view(
                                            group,
                                            active_idx,
                                            active_start,
                                            current_query.clone(),
                                            set_query,
                                            active_index,
                                        )
                                    });
                            let dsl_column = dsl_group
                                .map(|group| {
                                    render_group_view(
                                        group,
                                        active_idx,
                                        active_start,
                                        current_query.clone(),
                                        set_query,
                                        active_index,
                                    )
                                });
                            let two_column = if symbol_column.is_some() || dsl_column.is_some() {
                                Some(

                                    view! {
                                        <div class="grid grid-cols-1 md:grid-cols-2 gap-3">
                                            {symbol_column} {dsl_column}
                                        </div>
                                    },
                                )
                            } else {
                                None
                            };

                            view! {
                                <div class="p-3 text-sm text-gray-600 dark:text-gray-300">
                                    <For
                                        each=move || other_groups.clone()
                                        key=|group| group.title
                                        children=move |group| {
                                            render_group_view(
                                                group,
                                                active_idx,
                                                active_start,
                                                current_query.clone(),
                                                set_query,
                                                active_index,
                                            )
                                        }
                                    />
                                    {two_column}
                                </div>
                            }
                        }}
                    </Show>
                </div>
            </div>
        </div>
    }
}

#[derive(Clone, PartialEq)]
enum ValidationStatus {
    Valid,
    Invalid,
}

#[derive(Clone, PartialEq)]
struct ValidationState {
    status: ValidationStatus,
    message: Option<String>,
}

#[derive(Clone, PartialEq)]
struct DslHint {
    syntax: &'static str,
    description: &'static str,
}

#[derive(Clone, PartialEq)]
enum AutocompleteMode {
    None,
    DslKey,
    RepoValue,
    PathValue,
    LangValue,
    BranchValue,
    FileValue,
    CaseValue,
    HistoricalValue,
    Symbol,
}

#[derive(Clone, PartialEq)]
struct AutocompleteState {
    mode: AutocompleteMode,
    term: String,
    active_key: Option<String>,
    repo_filters: Vec<String>,
    active_start: usize,
}

#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
struct AutocompleteResults {
    repos: Vec<String>,
    paths: Vec<String>,
    files: Vec<String>,
    langs: Vec<String>,
    branches: Vec<String>,
    symbols: Vec<SymbolSuggestion>,
}

#[derive(Clone, PartialEq)]
struct SuggestionItem {
    label: String,
    replacement: String,
    subtitle: Option<String>,
    index: usize,
}

#[derive(Clone, PartialEq)]
struct SuggestionGroup {
    title: &'static str,
    items: Vec<SuggestionItem>,
}

fn render_group_view(
    group: SuggestionGroup,
    active_idx: Option<usize>,
    active_start: usize,
    current_query: String,
    set_query: WriteSignal<String>,
    active_index: RwSignal<Option<usize>>,
) -> impl IntoView {
    let group_title = group.title;
    let items = group.items;
    let empty_label = format!("No {} matches.", group_title.to_lowercase());
    let items_view = if items.is_empty() {
        Either::Left(
            view! { <div class="text-xs text-gray-500 dark:text-gray-400">{empty_label}</div> },
        )
    } else {
        let current_query = current_query.clone();
        let rendered = items
            .into_iter()
            .map(|item| {
                let replacement = item.replacement.clone();
                let label = item.label.clone();
                let subtitle = item.subtitle.clone();
                let idx = item.index;
                let is_active = active_idx == Some(idx);
                let row_class = if is_active {
                    "flex cursor-pointer bg-gray-200 dark:bg-gray-700 p-2 rounded"
                } else {
                    "flex cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-700 p-2 rounded"
                };
                let query_value = current_query.clone();
                view! {
                    <div
                        class=row_class
                        on:mousedown=move |ev| {
                            ev.prevent_default();
                            let updated = apply_autocomplete_replacement(
                                &query_value,
                                active_start,
                                &replacement,
                            );
                            set_query.set(updated);
                            active_index.set(None);
                        }
                    >
                        <div>
                            <span class="font-mono text-sm text-gray-900 dark:text-gray-100">
                                {label}
                            </span>
                            {subtitle
                                .map(|text| {
                                    view! {
                                        <div class="text-xs text-gray-500 dark:text-gray-400 truncate">
                                            {text}
                                        </div>
                                    }
                                })}
                        </div>
                    </div>
                }
            })
            .collect_view();
        Either::Right(view! { <div class="contents">{rendered}</div> })
    };

    view! {
        <div class="mb-3 last:mb-0">
            <p class="text-xs uppercase tracking-wide text-gray-500 dark:text-gray-400 mb-2">
                {group_title}
            </p>
            <div class="space-y-1">{items_view}</div>
        </div>
    }
}

const DSL_KEYS: [&str; 8] = [
    "repo:",
    "path:",
    "file:",
    "lang:",
    "branch:",
    "regex:",
    "case:",
    "historical:",
];

fn build_autocomplete_state(query: &str) -> AutocompleteState {
    let tokens = tokenize_for_autocomplete(query);
    let has_trailing_space = query
        .chars()
        .last()
        .map(|ch| ch.is_whitespace())
        .unwrap_or(false);
    let active_token = if has_trailing_space {
        None
    } else {
        tokens.last().cloned()
    };
    let active_start = find_active_token_start(query);

    let mut repo_filters = Vec::new();
    for token in &tokens {
        if token.first_colon_in_quotes {
            continue;
        }
        let raw = token.value.trim();
        let (negated, raw) = if let Some(rest) = raw.strip_prefix('-') {
            (true, rest)
        } else {
            (false, raw)
        };
        if let Some((key, value)) = raw.split_once(':') {
            let key = key.to_ascii_lowercase();
            if !negated && (key == "repo" || key == "r") && !value.is_empty() {
                repo_filters.push(value.to_string());
            }
        }
    }

    let mut mode = AutocompleteMode::None;
    let mut term = String::new();
    let mut active_key = None;

    if let Some(token) = active_token {
        if token.first_colon_in_quotes {
            mode = AutocompleteMode::Symbol;
            term = token.value;
        } else if let Some((key, value)) = token.value.split_once(':') {
            let key_lc = key.to_ascii_lowercase();
            let cleaned = value.trim();
            if key_lc == "repo" || key_lc == "r" {
                mode = AutocompleteMode::RepoValue;
                term = cleaned.to_string();
                active_key = Some(key.to_string());
            } else if key_lc == "path" {
                mode = AutocompleteMode::PathValue;
                term = cleaned.trim_end_matches('*').to_string();
                active_key = Some(key.to_string());
            } else if key_lc == "file" || key_lc == "f" {
                mode = AutocompleteMode::FileValue;
                term = cleaned.to_string();
                active_key = Some(key.to_string());
            } else if key_lc == "lang" || key_lc == "l" {
                mode = AutocompleteMode::LangValue;
                term = cleaned.to_string();
                active_key = Some(key.to_string());
            } else if key_lc == "branch" || key_lc == "b" {
                mode = AutocompleteMode::BranchValue;
                term = cleaned.to_string();
                active_key = Some(key.to_string());
            } else if key_lc == "case" {
                mode = AutocompleteMode::CaseValue;
                term = cleaned.to_string();
                active_key = Some(key.to_string());
            } else if key_lc == "historical" {
                mode = AutocompleteMode::HistoricalValue;
                term = cleaned.to_string();
                active_key = Some(key.to_string());
            } else if key_lc == "regex" || key_lc == "content" || key_lc == "type" {
                mode = AutocompleteMode::None;
            } else {
                mode = AutocompleteMode::Symbol;
                term = token.value;
            }
        } else if token.value.is_empty() {
            mode = AutocompleteMode::DslKey;
        } else {
            let value_lc = token.value.to_ascii_lowercase();
            let _is_key_prefix = DSL_KEYS.iter().any(|key| key.starts_with(&value_lc));
            mode = AutocompleteMode::Symbol;
            term = token.value;
        }
    } else if !query.trim().is_empty() {
        mode = AutocompleteMode::Symbol;
    }

    AutocompleteState {
        mode,
        term,
        active_key,
        repo_filters,
        active_start,
    }
}

fn find_active_token_start(query: &str) -> usize {
    if query
        .chars()
        .last()
        .map(|ch| ch.is_whitespace())
        .unwrap_or(false)
    {
        return query.len();
    }

    let mut token_start = None;
    let mut in_quotes = false;
    let mut quote_char = '\0';
    let mut escape_next = false;

    for (idx, ch) in query.char_indices() {
        if escape_next {
            escape_next = false;
            continue;
        }

        if in_quotes {
            match ch {
                '\\' => {
                    escape_next = true;
                }
                _ if ch == quote_char => {
                    in_quotes = false;
                }
                _ => {}
            }
            continue;
        }

        match ch {
            '"' | '\'' => {
                in_quotes = true;
                quote_char = ch;
                if token_start.is_none() {
                    token_start = Some(idx);
                }
            }
            ch if ch.is_whitespace() => {
                token_start = None;
            }
            _ => {
                if token_start.is_none() {
                    token_start = Some(idx);
                }
            }
        }
    }

    token_start.unwrap_or(query.len())
}

fn apply_autocomplete_replacement(query: &str, active_start: usize, replacement: &str) -> String {
    if active_start >= query.len() {
        let mut next = query.to_string();
        if !next.is_empty() && !next.ends_with(char::is_whitespace) {
            next.push(' ');
        }
        next.push_str(replacement);
        next
    } else {
        let mut next = String::new();
        next.push_str(&query[..active_start]);
        next.push_str(replacement);
        next
    }
}
