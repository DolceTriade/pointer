use crate::components::path_filter_actions::PathFilterActions;
use crate::db::{
    SnippetResponse,
    models::{FileReference, SymbolResult as DbSymbolResult},
};
use crate::pages::file_viewer::{SymbolInsightsParams, SymbolSearchScope, fetch_symbol_insights};
use leptos::either::Either;
use leptos::html::Div;
use leptos::prelude::*;
use leptos_router::components::A;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SymbolInsightsResponse {
    pub symbol: String,
    pub commit: String,
    pub matches: Vec<SymbolMatch>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SymbolMatch {
    pub definition: DbSymbolResult,
    pub references: Vec<SymbolReferenceWithSnippet>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SymbolReferenceWithSnippet {
    pub reference: FileReference,
    pub snippet: Option<SnippetResponse>,
}

#[component]
pub fn CodeIntelPanel(
    repo: Signal<String>,
    branch: Signal<String>,
    path: Signal<Option<String>>,
    selected_symbol: RwSignal<Option<String>>,
    language: RwSignal<Option<String>>,
    included_paths: RwSignal<Vec<String>>,
    excluded_paths: RwSignal<Vec<String>>,
) -> impl IntoView {
    let scope: RwSignal<SymbolSearchScope> = RwSignal::new(SymbolSearchScope::Repository);
    let language_filter = RwSignal::new(language.get_untracked());
    let manual_language_override = RwSignal::new(false);
    let manual_path_input = RwSignal::new(String::new());
    let snippet_filter = RwSignal::new(String::new());

    {
        let snippet_filter = snippet_filter.clone();
        Effect::new(move |_| {
            selected_symbol.get();
            manual_language_override.set(false);
            language_filter.set(language.get_untracked());
            snippet_filter.set(String::new());
        });
    }

    Effect::new({
        let included_paths = included_paths.clone();
        let excluded_paths = excluded_paths.clone();
        move |_| {
            if selected_symbol.get().is_none() {
                included_paths.set(Vec::new());
                excluded_paths.set(Vec::new());
            }
        }
    });

    Effect::new(move |_| {
        let lang = language.read();
        if !manual_language_override.get() {
            language_filter.set(lang.clone());
        }
    });

    let included_paths_for_resource = included_paths.clone();
    let excluded_paths_for_resource = excluded_paths.clone();
    let insights_resource = Resource::new(
        move || {
            (
                selected_symbol.get(),
                repo.get(),
                branch.get(),
                path.get(),
                scope.get(),
                language_filter.get(),
                included_paths_for_resource.get(),
                excluded_paths_for_resource.get(),
            )
        },
        |(symbol_opt, repo, branch, path, scope, language, include_paths, excluded_paths)| async move {
            if let Some(symbol) = symbol_opt {
                fetch_symbol_insights(SymbolInsightsParams {
                    repo,
                    branch,
                    path,
                    symbol,
                    language,
                    scope,
                    include_paths,
                    excluded_paths,
                })
                .await
                .map(Some)
            } else {
                Ok(None)
            }
        },
    );

    let insights_scroll_container = NodeRef::<Div>::new();

    view! {
        <aside class="w-80 flex-shrink-0 bg-white/95 dark:bg-slate-950/70 text-slate-900 dark:text-slate-100 rounded-lg shadow border border-slate-200 dark:border-slate-800 p-4 sticky top-20 backdrop-blur">
            <h2 class="text-xl font-semibold mb-4 text-slate-900 dark:text-white">
                "Code Intelligence"
            </h2>
            <div class="text-sm text-slate-600 dark:text-slate-300 mb-4">
                {move || {
                    selected_symbol
                        .get()
                        .map(|symbol| {
                            Either::Left(
                                view! {
                                    <span class="font-mono text-blue-600 dark:text-blue-300">
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
            <div class="pr-1" node_ref=insights_scroll_container>
                <div class="space-y-4">
                    <div class="flex flex-col gap-1">
                        <label class="text-xs uppercase tracking-wide text-slate-600 dark:text-slate-300">
                            "Scope"
                        </label>
                        <select
                            class="select select-sm select-bordered bg-white/95 text-slate-900 dark:bg-slate-900/70 dark:text-slate-100 border border-slate-200 dark:border-slate-700 focus-visible:outline focus-visible:outline-sky-600 dark:focus-visible:outline-sky-400"
                            on:change=move |ev| {
                                let value = event_target_value(&ev);
                                scope.set(SymbolSearchScope::from_str(&value));
                            }
                            prop:value=move || scope.get().as_str().to_string()
                        >
                            <option value="repository">
                                {SymbolSearchScope::Repository.label()}
                            </option>
                            <option value="directory">
                                {SymbolSearchScope::Directory.label()}
                            </option>
                            <option value="file">{SymbolSearchScope::File.label()}</option>
                            <option value="custom">{SymbolSearchScope::Custom.label()}</option>
                        </select>
                    </div>
                    <div class="flex flex-col gap-1">
                        <label class="text-xs uppercase tracking-wide text-slate-600 dark:text-slate-300">
                            "Language"
                        </label>
                        <select
                            class="select select-sm select-bordered bg-white/95 text-slate-900 dark:bg-slate-900/70 dark:text-slate-100 border border-slate-200 dark:border-slate-700 focus-visible:outline focus-visible:outline-sky-600 dark:focus-visible:outline-sky-400"
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
                                            class="text-xs text-blue-600 dark:text-blue-300 hover:underline text-left"
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
                    <div class="flex flex-col gap-1">
                        <label class="text-xs uppercase tracking-wide text-slate-600 dark:text-slate-300">
                            "Filter snippets"
                        </label>
                        <input
                            class="input input-sm input-bordered bg-white/95 text-slate-900 dark:bg-slate-900/70 dark:text-slate-100 border border-slate-200 dark:border-slate-700 focus-visible:outline focus-visible:outline-sky-600 dark:focus-visible:outline-sky-400"
                            type="text"
                            placeholder="Find text in snippets"
                            prop:value=move || snippet_filter.get()
                            on:input=move |ev| snippet_filter.set(event_target_value(&ev))
                        />
                    </div>
                    <Show
                        when=move || matches!(scope.get(), SymbolSearchScope::Custom)
                        fallback=move || view! { <></> }
                    >
                        <div class="flex flex-col gap-2">
                            <label class="text-xs uppercase tracking-wide text-slate-600 dark:text-slate-300">
                                "Path filters"
                            </label>
                            <input
                                class="input input-sm input-bordered bg-white/95 text-slate-900 dark:bg-slate-900/70 dark:text-slate-100 border border-slate-200 dark:border-slate-700 focus-visible:outline focus-visible:outline-sky-600 dark:focus-visible:outline-sky-400"
                                placeholder="e.g. components/light/ or components/light/domain.py"
                                prop:value=move || manual_path_input.get()
                                on:input=move |ev| manual_path_input.set(event_target_value(&ev))
                            />
                            <div class="flex gap-2">
                                <button
                                    class="text-xs rounded-full border border-slate-300 dark:border-slate-600 px-2 py-1 text-slate-600 hover:bg-slate-100 dark:text-slate-100 dark:hover:bg-slate-800"
                                    on:click={
                                        let manual_path_input = manual_path_input.clone();
                                        let included_paths = included_paths.clone();
                                        move |ev: leptos::ev::MouseEvent| {
                                            ev.prevent_default();
                                            let value = manual_path_input.get();
                                            let trimmed = value.trim();
                                            if trimmed.is_empty() {
                                                return;
                                            }
                                            let candidate = trimmed.to_string();
                                            manual_path_input.set(String::new());
                                            included_paths
                                                .update(|paths| {
                                                    if !paths.iter().any(|existing| existing == &candidate) {
                                                        paths.push(candidate.clone());
                                                    }
                                                });
                                        }
                                    }
                                >
                                    "Add include"
                                </button>
                                <button
                                    class="text-xs rounded-full border border-slate-300 dark:border-slate-600 px-2 py-1 text-slate-600 hover:bg-slate-100 dark:text-slate-100 dark:hover:bg-slate-800"
                                    on:click={
                                        let manual_path_input = manual_path_input.clone();
                                        let excluded_paths = excluded_paths.clone();
                                        move |ev: leptos::ev::MouseEvent| {
                                            ev.prevent_default();
                                            let value = manual_path_input.get();
                                            let trimmed = value.trim();
                                            if trimmed.is_empty() {
                                                return;
                                            }
                                            let candidate = trimmed.to_string();
                                            manual_path_input.set(String::new());
                                            excluded_paths
                                                .update(|paths| {
                                                    if !paths.iter().any(|existing| existing == &candidate) {
                                                        paths.push(candidate.clone());
                                                    }
                                                });
                                        }
                                    }
                                >
                                    "Add exclude"
                                </button>
                            </div>
                            <p class="text-[11px] text-slate-600 dark:text-slate-300">
                                "Add a trailing '/' to match an entire directory."
                            </p>
                        </div>
                    </Show>
                    {move || {
                        if matches!(scope.get(), SymbolSearchScope::Custom) {
                            let paths = included_paths.get();
                            if paths.is_empty() {
                                Either::Left(view! { <></> })
                            } else {
                                let included_paths = included_paths.clone();
                                Either::Right(
                                    view! {
                                        <div class="flex flex-wrap items-center gap-2 text-xs">
                                            <span class="text-slate-500 dark:text-slate-300 uppercase tracking-wide">
                                                "Includes"
                                            </span>
                                            <For
                                                each=move || included_paths.get()
                                                key=|path| path.clone()
                                                children=move |path| {
                                                    let signal = included_paths.clone();
                                                    let display = path.clone();
                                                    view! {
                                                        <span class="inline-flex items-center gap-1 rounded-full bg-green-200/70 dark:bg-green-900/40 px-2 py-1 font-mono">
                                                            <span class="truncate max-w-[10rem]" title=display.clone()>
                                                                {display.clone()}
                                                            </span>
                                                            <button
                                                                class="text-xs text-slate-600 hover:text-slate-900 dark:text-slate-200 dark:hover:text-white"
                                                                on:click=move |ev| {
                                                                    ev.stop_propagation();
                                                                    let value = display.clone();
                                                                    signal
                                                                        .update(|paths| {
                                                                            if let Some(pos) = paths.iter().position(|p| p == &value) {
                                                                                paths.remove(pos);
                                                                            }
                                                                        });
                                                                }
                                                                aria-label="Remove included path"
                                                            >
                                                                "×"
                                                            </button>
                                                        </span>
                                                    }
                                                }
                                            />
                                        </div>
                                    },
                                )
                            }
                        } else {
                            Either::Left(view! { <></> })
                        }
                    }}
                    {move || {
                        if matches!(scope.get(), SymbolSearchScope::Custom) {
                            let paths = excluded_paths.get();
                            if paths.is_empty() {
                                Either::Left(view! { <></> })
                            } else {
                                let excluded_paths = excluded_paths.clone();
                                Either::Right(
                                    view! {
                                        <div class="flex flex-wrap items-center gap-2 text-xs">
                                            <span class="text-slate-500 dark:text-slate-300 uppercase tracking-wide">
                                                "Excludes"
                                            </span>
                                            <For
                                                each=move || excluded_paths.get()
                                                key=|path| path.clone()
                                                children=move |path| {
                                                    let signal = excluded_paths.clone();
                                                    let display = path.clone();
                                                    view! {
                                                        <span class="inline-flex items-center gap-1 rounded-full bg-gray-200 dark:bg-gray-700/70 px-2 py-1 font-mono">
                                                            <span class="truncate max-w-[10rem]" title=display.clone()>
                                                                {display.clone()}
                                                            </span>
                                                            <button
                                                                class="text-xs text-slate-600 hover:text-slate-900 dark:text-slate-200 dark:hover:text-white"
                                                                on:click=move |ev| {
                                                                    ev.stop_propagation();
                                                                    let value = display.clone();
                                                                    signal
                                                                        .update(|paths| {
                                                                            if let Some(pos) = paths.iter().position(|p| p == &value) {
                                                                                paths.remove(pos);
                                                                            }
                                                                        });
                                                                }
                                                                aria-label="Remove excluded path"
                                                            >
                                                                "×"
                                                            </button>
                                                        </span>
                                                    }
                                                }
                                            />
                                        </div>
                                    },
                                )
                            }
                        } else {
                            Either::Left(view! { <></> })
                        }
                    }}
                </div>
                <div class="mt-6">
                    <Show
                        when=move || selected_symbol.get().is_some()
                        fallback=move || {
                            view! {
                                <p class="text-sm text-slate-600 dark:text-slate-300">
                                    "Highlight a symbol in the editor to see definitions and references."
                                </p>
                            }
                        }
                    >
                        <Suspense fallback=move || {
                            view! {
                                <p class="text-sm text-slate-600 dark:text-slate-300">
                                    "Gathering symbol data..."
                                </p>
                            }
                        }>
                            {move || {
                                if selected_symbol.get().is_none() {
                                    return None;
                                }
                                let filter_text = snippet_filter.get();
                                let needle = filter_text.to_lowercase();
                                insights_resource
                                    .get()
                                    .map(|result| match result {
                                        Ok(Some(data)) => {
                                            if let Some(node) = insights_scroll_container
                                                .get_untracked()
                                            {
                                                node.set_scroll_top(0);
                                            }
                                            let SymbolInsightsResponse { commit, matches, .. } = data;
                                            let matches: Vec<_> = if needle.is_empty() {
                                                matches
                                            } else {
                                                matches
                                                    .into_iter()
                                                    .filter_map(|mut symbol_match| {
                                                        symbol_match
                                                            .references
                                                            .retain(|reference| {
                                                                snippet_matches_filter(reference, &needle)
                                                            });
                                                        if symbol_match.references.is_empty() {
                                                            None
                                                        } else {
                                                            Some(symbol_match)
                                                        }
                                                    })
                                                    .collect()
                                            };
                                            if matches.is_empty() {
                                                let message = if filter_text.is_empty() {
                                                    "No indexed symbols matched this selection.".to_string()
                                                } else {
                                                    "No snippets matched the local filter.".to_string()
                                                };

                                                view! {
                                                    <p class="text-sm text-slate-600 dark:text-slate-300">
                                                        {message}
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
                                                                let definition_file_path = definition.file_path.clone();
                                                                let definition_file_path_for_label = definition_file_path
                                                                    .clone();
                                                                let (definition_line, definition_link) = if let Some(
                                                                    line,
                                                                ) = definition.line
                                                                {
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
                                                                let display_path = definition_line
                                                                    .map(|line| {
                                                                        format!(
                                                                            "{}:{}",
                                                                            definition_file_path_for_label.clone(),
                                                                            line,
                                                                        )
                                                                    })
                                                                    .unwrap_or_else(|| definition_file_path_for_label.clone());
                                                                let display_title = display_path.clone();
                                                                let display_text = display_path.clone();
                                                                let reference_count = references.len();
                                                                let definition_repo = definition.repository.clone();
                                                                let grouped_references = {
                                                                    let mut groups: Vec<
                                                                        (String, String, String, Vec<SymbolReferenceWithSnippet>),
                                                                    > = Vec::new();
                                                                    for entry in references.into_iter() {
                                                                        let repo_name = entry.reference.repository.clone();
                                                                        let commit_sha = entry.reference.commit_sha.clone();
                                                                        let file_path = entry.reference.file_path.clone();
                                                                        if let Some((_, _, _, items)) = groups
                                                                            .iter_mut()
                                                                            .find(|(existing_repo, existing_commit, existing_path, _)| {
                                                                                existing_repo == &repo_name
                                                                                    && existing_commit == &commit_sha
                                                                                    && existing_path == &file_path
                                                                            })
                                                                        {
                                                                            items.push(entry);
                                                                        } else {
                                                                            groups
                                                                                .push((repo_name, commit_sha, file_path, vec![entry]));
                                                                        }
                                                                    }
                                                                    groups
                                                                };
                                                                let definition_file_path = definition.file_path.clone();

                                                                view! {
                                                                    <div class="rounded border border-slate-200 dark:border-slate-800 bg-white/90 dark:bg-slate-900/60 p-3 shadow-sm">
                                                                        <div class="flex items-center justify-between gap-2">
                                                                            {definition
                                                                                .namespace
                                                                                .as_ref()
                                                                                .map(|ns| {
                                                                                    view! {
                                                                                        <div class="text-xs text-slate-500 dark:text-slate-300">
                                                                                            {ns.clone()}
                                                                                        </div>
                                                                                    }
                                                                                })}
                                                                            <span class="text-xs text-slate-500 dark:text-slate-300 uppercase">
                                                                                {definition_language}
                                                                            </span>
                                                                        </div>
                                                                        <div class="mt-2 flex items-center gap-2 min-w-0">
                                                                            <A
                                                                                href=definition_link
                                                                                attr:class="text-sm text-blue-600 dark:text-blue-400 hover:underline font-mono"
                                                                                attr:title=display_title.clone()
                                                                            >
                                                                                <span class="inline-flex min-w-0 flex-1 text-ellipsis overflow-hidden break-all">
                                                                                    {display_text}
                                                                                </span>
                                                                            </A>
                                                                            <PathFilterActions
                                                                                path=definition_file_path.clone()
                                                                                included_paths=included_paths.clone()
                                                                                excluded_paths=excluded_paths.clone()
                                                                            />
                                                                        </div>
                                                                        {definition_line
                                                                            .map(|line| {
                                                                                view! {
                                                                                    <p class="text-xs text-slate-600 dark:text-slate-300 mt-1">
                                                                                        {format!("Line {}", line)}
                                                                                    </p>
                                                                                }
                                                                            })}
                                                                        {definition
                                                                            .kind
                                                                            .as_ref()
                                                                            .map(|kind| {
                                                                                view! {
                                                                                    <p class="text-xs text-slate-600 dark:text-slate-300 mt-1 uppercase">
                                                                                        {kind.clone()}
                                                                                    </p>
                                                                                }
                                                                            })}
                                                                        <p class="text-xs text-slate-600 dark:text-slate-300 mt-1">
                                                                            {format!("Score: {:.3}", definition.score)}
                                                                        </p>
                                                                        <div class="mt-4">
                                                                            <h3 class="text-xs font-semibold uppercase tracking-wide text-slate-600 dark:text-slate-300">
                                                                                {format!("References ({reference_count})")}
                                                                            </h3>
                                                                            {if grouped_references.is_empty() {
                                                                                Either::Left(
                                                                                    view! {
                                                                                        <p class="text-xs text-slate-600 dark:text-slate-300 mt-2">
                                                                                            "No references were indexed for this symbol."
                                                                                        </p>
                                                                                    },
                                                                                )
                                                                            } else {
                                                                                let groups = grouped_references;
                                                                                Either::Right(
                                                                                    view! {
                                                                                        <div class="mt-3 space-y-3">
                                                                                            {groups
                                                                                                .into_iter()
                                                                                                .map(|(repo_name, _commit_sha, file_path, entries)| {
                                                                                                    let file_reference_count = entries.len();
                                                                                                    let reference_label = if file_reference_count == 1 {
                                                                                                        "1 match".to_string()
                                                                                                    } else {
                                                                                                        format!("{file_reference_count} matches")
                                                                                                    };
                                                                                                    let summary_label = if repo_name == definition_repo {
                                                                                                        file_path.clone()
                                                                                                    } else {
                                                                                                        format!("{repo_name}/{file_path}")
                                                                                                    };
                                                                                                    let summary_label_title = summary_label.clone();
                                                                                                    let summary_label_text = summary_label.clone();

                                                                                                    view! {
                                                                                                        <details class="border border-slate-200 dark:border-slate-800 rounded bg-white/90 dark:bg-slate-950/40">
                                                                                                            <summary class="flex items-center justify-between gap-2 px-3 py-2 cursor-pointer select-none hover:bg-slate-100 dark:hover:bg-slate-800 rounded text-slate-900 dark:text-slate-100">
                                                                                                                <span
                                                                                                                    class="min-w-0 text-sm text-blue-600 dark:text-blue-400 text-ellipsis overflow-hidden whitespace-nowrap flex-1"
                                                                                                                    title=summary_label_title
                                                                                                                >
                                                                                                                    {summary_label_text}
                                                                                                                </span>
                                                                                                                <span class="text-xs text-slate-500 dark:text-slate-300">
                                                                                                                    {reference_label}
                                                                                                                </span>
                                                                                                            </summary>
                                                                                                            <div class="mt-2 space-y-2 px-3 pb-3">
                                                                                                                {entries
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
                                                                                                                        let reference_file_path = reference.file_path.clone();
                                                                                                                        let reference_title = reference_file_path.clone();
                                                                                                                        view! {
                                                                                                                            <div class="rounded border border-slate-200 dark:border-slate-800 bg-white/90 dark:bg-slate-950/40 transition-colors overflow-hidden">
                                                                                                                                <div class="flex items-center justify-between gap-2 px-3 py-2">
                                                                                                                                    <div class="min-w-0">
                                                                                                                                        <A
                                                                                                                                            href=reference_link.clone()
                                                                                                                                            attr:class="text-xs text-slate-500 dark:text-slate-300 hover:underline block"
                                                                                                                                            attr:title=reference_title.clone()
                                                                                                                                        >
                                                                                                                                            <span class="block text-ellipsis overflow-hidden whitespace-nowrap flex-1 min-w-0">
                                                                                                                                                {format!(
                                                                                                                                                    "Line {}  •  Column {}",
                                                                                                                                                    line_number,
                                                                                                                                                    reference.column,
                                                                                                                                                )}
                                                                                                                                            </span>
                                                                                                                                        </A>
                                                                                                                                    </div>
                                                                                                                                    <PathFilterActions
                                                                                                                                        path=reference_file_path.clone()
                                                                                                                                        included_paths=included_paths.clone()
                                                                                                                                        excluded_paths=excluded_paths.clone()
                                                                                                                                    />
                                                                                                                                </div>
                                                                                                                                {entry
                                                                                                                                    .snippet
                                                                                                                                    .map(|snippet| {
                                                                                                                                        let highlight_line = snippet.highlight_line;
                                                                                                                                        let start_line = snippet.start_line;
                                                                                                                                        view! {
                                                                                                                                            <div class="bg-slate-50/80 dark:bg-slate-900/60 border-t border-slate-200 dark:border-slate-800 px-3 py-2 text-xs font-mono text-slate-900 dark:text-slate-100 overflow-x-auto">
                                                                                                                                                {snippet
                                                                                                                                                    .lines
                                                                                                                                                    .into_iter()
                                                                                                                                                    .enumerate()
                                                                                                                                                    .map(|(idx, text)| {
                                                                                                                                                        let current_line = start_line + idx as u32;
                                                                                                                                                        let is_highlight = current_line == highlight_line;
                                                                                                                                                        let display_text = collapse_snippet_whitespace(&text);
                                                                                                                                                        let row_class = if is_highlight {
                                                                                                                                                            "flex gap-3 bg-blue-100/80 dark:bg-blue-900/40 rounded px-2 py-1"
                                                                                                                                                        } else {
                                                                                                                                                            "flex gap-3 px-2 py-1"
                                                                                                                                                        };
                                                                                                                                                        view! {
                                                                                                                                                            <div class=row_class>
                                                                                                                                                                <span class="w-12 text-right text-[10px] text-slate-500 dark:text-slate-300">
                                                                                                                                                                    {current_line}
                                                                                                                                                                </span>
                                                                                                                                                                <span class="flex-1 whitespace-nowrap min-w-max">
                                                                                                                                                                    {display_text}
                                                                                                                                                                </span>
                                                                                                                                                            </div>
                                                                                                                                                        }
                                                                                                                                                    })
                                                                                                                                                    .collect_view()}
                                                                                                                                            </div>
                                                                                                                                        }
                                                                                                                                    })}
                                                                                                                            </div>
                                                                                                                        }
                                                                                                                    })
                                                                                                                    .collect_view()}
                                                                                                            </div>
                                                                                                        </details>
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
                                                <p class="text-sm text-slate-600 dark:text-slate-300">
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

pub fn snippet_matches_filter(reference: &SymbolReferenceWithSnippet, needle: &str) -> bool {
    if needle.is_empty() {
        return true;
    }
    reference
        .snippet
        .as_ref()
        .map(|snippet| {
            snippet
                .lines
                .iter()
                .any(|line| line.to_lowercase().contains(needle))
        })
        .unwrap_or(false)
}

fn collapse_snippet_whitespace(value: &str) -> String {
    value.split_whitespace().collect::<Vec<&str>>().join(" ")
}
