use crate::dsl::{parse_query, tokenize_for_autocomplete};
use crate::services::search_service::{
    autocomplete_paths, autocomplete_repositories, autocomplete_symbols,
};
use leptos::either::Either;
use leptos::prelude::*;
use leptos_router::hooks::use_navigate;
use std::rc::Rc;
use std::sync::Arc;
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

    let on_input = move |ev| {
        set_query.set(event_target_value(&ev));
    };

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
        let q = query.get();
        if q.is_empty() {
            return None;
        }

        match parse_query(&q) {
            Ok(_) => Some(ValidationStatus::Valid),
            Err(_) => Some(ValidationStatus::Invalid),
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
            syntax: "content:",
            description: "Search in file content",
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
            syntax: "type:",
            description: "Filter result type (type:filematch)",
        },
        DslHint {
            syntax: "historical:",
            description: "Include historical commits (historical:yes)",
        },
    ];

    // Example queries for users
    let example_queries = vec![
        "repo:myrepo lang:rust",
        "content:\"async fn\" path:*.rs",
        "path:README.md content:install historical:yes",
    ];

    let autocomplete_state = Memo::new(move |_| build_autocomplete_state(&query.get()));
    let autocomplete_resource = Resource::new(
        move || autocomplete_state.get(),
        |state| async move {
            let limit = 20;
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
                _ => Ok(AutocompleteResults::default()),
            }
        },
    );

    let dsl_suggestions = Memo::new({
        let dsl_hints = dsl_hints.clone();
        move |_| {
            let state = autocomplete_state.get();
            if matches!(
                state.mode,
                AutocompleteMode::RepoValue | AutocompleteMode::PathValue | AutocompleteMode::None
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

    view! {
        <div class="w-full max-w-2xl">
            <div class="group relative">
                <div class="flex items-center rounded-full border border-gray-300 dark:border-gray-700 shadow-lg overflow-hidden bg-white dark:bg-gray-800 relative">
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
                                if ev.key() == "Enter" {
                                    func();
                                }
                            }
                        }
                    />

                    // Validation indicator
                    {move || {
                        validation
                            .get()
                            .map(|status| {
                                match status {
                                    ValidationStatus::Valid => {
                                        view! {
                                            <div class="absolute right-12 top-1/2 transform -translate-y-1/2">
                                                <svg
                                                    class="w-5 h-5 text-green-500"
                                                    fill="none"
                                                    stroke="currentColor"
                                                    viewBox="0 0 24 24"
                                                >
                                                    <path
                                                        stroke-linecap="round"
                                                        stroke-linejoin="round"
                                                        stroke-width="2"
                                                        d="M5 13l4 4L19 7"
                                                    ></path>
                                                </svg>
                                            </div>
                                        }
                                    }
                                    ValidationStatus::Invalid => {
                                        view! {
                                            <div class="absolute right-12 top-1/2 transform -translate-y-1/2">
                                                <svg
                                                    class="w-5 h-5 text-red-500"
                                                    fill="none"
                                                    stroke="currentColor"
                                                    viewBox="0 0 24 24"
                                                >
                                                    <path
                                                        stroke-linecap="round"
                                                        stroke-linejoin="round"
                                                        stroke-width="2"
                                                        d="M6 18L18 6M6 6l12 12"
                                                    ></path>
                                                </svg>
                                            </div>
                                        }
                                    }
                                }
                            })
                    }}

                    <button
                        class="px-6 py-4 bg-gray-200 dark:bg-gray-700 hover:bg-gray-300 dark:hover:bg-gray-600 transition-colors duration-200"
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
                                                            let updated = apply_autocomplete_replacement(
                                                                &query.get(),
                                                                query.get().len(),
                                                                &syntax,
                                                            );
                                                            set_query.set(updated);
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
                            let state = autocomplete_state.get();
                            let results = autocomplete_resource
                                .get()
                                .and_then(|result| result.ok())
                                .unwrap_or_default();
                            let active_start = state.active_start;
                            let active_key = state.active_key.clone().unwrap_or_default();
                            let current_query = Arc::new(query.get());
                            let dsl_items = Arc::new(dsl_suggestions.get());
                            let show_repo = matches!(state.mode, AutocompleteMode::RepoValue);
                            let show_path = matches!(state.mode, AutocompleteMode::PathValue);
                            let show_dsl = matches!(
                                state.mode,
                                AutocompleteMode::DslKey | AutocompleteMode::Symbol
                            );
                            let show_symbols = matches!(state.mode, AutocompleteMode::Symbol);
                            let repos = Arc::new(results.repos.clone());
                            let paths = Arc::new(results.paths.clone());
                            let symbols = Arc::new(results.symbols.clone());
                            let repo_query = current_query.clone();
                            let path_query = current_query.clone();
                            let dsl_query = current_query.clone();
                            let symbol_query = current_query.clone();
                            let repo_key = Arc::new(
                                if active_key.is_empty() {
                                    "repo".to_string()
                                } else {
                                    active_key.clone()
                                },
                            );
                            let path_key = Arc::new(
                                if active_key.is_empty() {
                                    "path".to_string()
                                } else {
                                    active_key.clone()
                                },
                            );

                            view! {
                                <div class="p-3 text-sm text-gray-600 dark:text-gray-300">
                                    <Show when=move || show_repo fallback=move || view! { <></> }>
                                        <div class="mb-3">
                                            <p class="text-xs uppercase tracking-wide text-gray-500 dark:text-gray-400 mb-2">
                                                "Repositories"
                                            </p>
                                            <div class="space-y-1">
                                                {if repos.is_empty() {
                                                    Either::Left(
                                                        view! {
                                                            <div class="contents">
                                                                <div class="text-xs text-gray-500 dark:text-gray-400">
                                                                    "No repository matches."
                                                                </div>
                                                            </div>
                                                        },
                                                    )
                                                } else {
                                                    let repos = repos.clone();
                                                    let repo_key = repo_key.clone();
                                                    let repo_query = repo_query.clone();
                                                    Either::Right(
                                                        view! {
                                                            <div class="contents">
                                                                <For
                                                                    each=move || (*repos).clone()
                                                                    key=|repo| repo.clone()
                                                                    children=move |repo| {
                                                                        let replacement = format!("{}:{}", repo_key.as_str(), repo);
                                                                        view! {
                                                                            <div
                                                                                class="flex cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-700 p-2 rounded"
                                                                                on:mousedown={
                                                                                    let query = repo_query.clone();
                                                                                    move |ev| {
                                                                                        ev.prevent_default();
                                                                                        let updated = apply_autocomplete_replacement(
                                                                                            query.as_str(),
                                                                                            active_start,
                                                                                            &replacement,
                                                                                        );
                                                                                        set_query.set(updated);
                                                                                    }
                                                                                }
                                                                            >
                                                                                <span class="font-mono text-sm text-gray-900 dark:text-gray-100">
                                                                                    {repo}
                                                                                </span>
                                                                            </div>
                                                                        }
                                                                    }
                                                                />
                                                            </div>
                                                        },
                                                    )
                                                }}
                                            </div>
                                        </div>
                                    </Show>

                                    <Show when=move || show_path fallback=move || view! { <></> }>
                                        <div class="mb-3">
                                            <p class="text-xs uppercase tracking-wide text-gray-500 dark:text-gray-400 mb-2">
                                                "Paths"
                                            </p>
                                            <div class="space-y-1">
                                                {if paths.is_empty() {
                                                    Either::Left(
                                                        view! {
                                                            <div class="contents">
                                                                <div class="text-xs text-gray-500 dark:text-gray-400">
                                                                    "No path matches."
                                                                </div>
                                                            </div>
                                                        },
                                                    )
                                                } else {
                                                    let paths = paths.clone();
                                                    let path_key = path_key.clone();
                                                    let path_query = path_query.clone();
                                                    Either::Right(
                                                        view! {
                                                            <div class="contents">
                                                                <For
                                                                    each=move || (*paths).clone()
                                                                    key=|path| path.clone()
                                                                    children=move |path| {
                                                                        let replacement = format!("{}:{}", path_key.as_str(), path);
                                                                        view! {
                                                                            <div
                                                                                class="flex cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-700 p-2 rounded"
                                                                                on:mousedown={
                                                                                    let query = path_query.clone();
                                                                                    move |ev| {
                                                                                        ev.prevent_default();
                                                                                        let updated = apply_autocomplete_replacement(
                                                                                            query.as_str(),
                                                                                            active_start,
                                                                                            &replacement,
                                                                                        );
                                                                                        set_query.set(updated);
                                                                                    }
                                                                                }
                                                                            >
                                                                                <span class="font-mono text-sm text-gray-900 dark:text-gray-100">
                                                                                    {path}
                                                                                </span>
                                                                            </div>
                                                                        }
                                                                    }
                                                                />
                                                            </div>
                                                        },
                                                    )
                                                }}
                                            </div>
                                        </div>
                                    </Show>

                                    <Show when=move || show_dsl fallback=move || view! { <></> }>
                                        <div class="mb-3">
                                            <p class="text-xs uppercase tracking-wide text-gray-500 dark:text-gray-400 mb-2">
                                                "DSL"
                                            </p>
                                            <div class="grid grid-cols-2 gap-2">
                                                {dsl_items
                                                    .as_ref()
                                                    .iter()
                                                    .cloned()
                                                    .map(|hint| {
                                                        let dsl_query = dsl_query.clone();
                                                        let replacement = hint.syntax.to_string();
                                                        view! {
                                                            <div
                                                                class="flex cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-700 p-1 rounded"
                                                                on:mousedown=move |ev| {
                                                                    ev.prevent_default();
                                                                    let updated = apply_autocomplete_replacement(
                                                                        dsl_query.as_str(),
                                                                        active_start,
                                                                        &replacement,
                                                                    );
                                                                    set_query.set(updated);
                                                                }
                                                            >
                                                                <span class="font-mono text-blue-600 dark:text-blue-400 font-semibold mr-2">
                                                                    {hint.syntax}
                                                                </span>
                                                                <span class="text-gray-600 dark:text-gray-400">
                                                                    {hint.description}
                                                                </span>
                                                            </div>
                                                        }
                                                    })
                                                    .collect_view()}
                                            </div>
                                        </div>
                                    </Show>

                                    <Show
                                        when=move || show_symbols
                                        fallback=move || view! { <></> }
                                    >
                                        <div>
                                            <p class="text-xs uppercase tracking-wide text-gray-500 dark:text-gray-400 mb-2">
                                                "Symbols"
                                            </p>
                                            <div class="space-y-1">
                                                {if symbols.is_empty() {
                                                    Either::Left(
                                                        view! {
                                                            <div class="contents">
                                                                <div class="text-xs text-gray-500 dark:text-gray-400">
                                                                    "No symbols matched."
                                                                </div>
                                                            </div>
                                                        },
                                                    )
                                                } else {
                                                    let symbols = symbols.clone();
                                                    let symbol_query = symbol_query.clone();
                                                    Either::Right(
                                                        view! {
                                                            <div class="contents">
                                                                <For
                                                                    each=move || (*symbols).clone()
                                                                    key=|symbol| symbol.clone()
                                                                    children=move |symbol| {
                                                                        let replacement = symbol.clone();
                                                                        view! {
                                                                            <div
                                                                                class="flex cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-700 p-2 rounded"
                                                                                on:mousedown={
                                                                                    let query = symbol_query.clone();
                                                                                    move |ev| {
                                                                                        ev.prevent_default();
                                                                                        let updated = apply_autocomplete_replacement(
                                                                                            query.as_str(),
                                                                                            active_start,
                                                                                            &replacement,
                                                                                        );
                                                                                        set_query.set(updated);
                                                                                    }
                                                                                }
                                                                            >
                                                                                <span class="font-mono text-sm text-gray-900 dark:text-gray-100">
                                                                                    {symbol}
                                                                                </span>
                                                                            </div>
                                                                        }
                                                                    }
                                                                />
                                                            </div>
                                                        },
                                                    )
                                                }}
                                            </div>
                                        </div>
                                    </Show>
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
    symbols: Vec<String>,
}

const DSL_KEYS: [&str; 10] = [
    "repo:",
    "path:",
    "file:",
    "lang:",
    "content:",
    "branch:",
    "regex:",
    "case:",
    "type:",
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
            } else if key_lc == "file" || key_lc == "f" || key_lc == "path" {
                mode = AutocompleteMode::PathValue;
                term = cleaned.trim_end_matches('*').to_string();
                active_key = Some(key.to_string());
            } else {
                mode = AutocompleteMode::Symbol;
                term = token.value;
            }
        } else if token.value.is_empty() {
            mode = AutocompleteMode::DslKey;
        } else {
            let value_lc = token.value.to_ascii_lowercase();
            let is_key_prefix = DSL_KEYS.iter().any(|key| key.starts_with(&value_lc));
            if is_key_prefix {
                mode = AutocompleteMode::DslKey;
                term = token.value;
            } else {
                mode = AutocompleteMode::Symbol;
                term = token.value;
            }
        }
    } else if !query.trim().is_empty() {
        mode = AutocompleteMode::DslKey;
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
