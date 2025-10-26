use crate::db::models::{
    FacetCount, SearchResult, SearchResultsPage, SearchResultsStats, SearchSnippet,
};
use crate::dsl::DEFAULT_PAGE_SIZE;
use crate::services::search_service::search;
use leptos::either::{Either, EitherOf3};
use leptos::prelude::*;
use leptos_router::{
    NavigateOptions,
    hooks::{use_navigate, use_query},
    params::Params,
};
use urlencoding::encode;

#[derive(Params, PartialEq, Clone, Debug)]
pub struct SearchParams {
    pub q: Option<String>,
    pub page: Option<usize>,
}

#[component]
pub fn SearchPage() -> impl IntoView {
    let query = use_query::<SearchParams>();
    let navigate = use_navigate();

    let query_text = RwSignal::new(String::new());

    Effect::new({
        let query = query.clone();
        let query_text = query_text.clone();
        move |_| {
            if let Ok(params) = query.get() {
                query_text.set(params.q.clone().unwrap_or_default());
            }
        }
    });

    let search_results = Resource::new(query, |q| async move {
        match q {
            Ok(params) => {
                let page = params.page.unwrap_or(1).max(1);
                let search_text = params.q.clone().unwrap_or_default();
                if search_text.is_empty() {
                    return Ok(SearchResultsPage::empty(
                        search_text,
                        page as u32,
                        DEFAULT_PAGE_SIZE,
                    ));
                }
                search(search_text, page as u32).await
            }
            Err(_) => Ok(SearchResultsPage::empty(
                String::new(),
                1,
                DEFAULT_PAGE_SIZE,
            )),
        }
    });

    let repo_input = RwSignal::new(String::new());
    let path_input = RwSignal::new(String::new());
    let branch_input = RwSignal::new(String::new());
    let language_input = RwSignal::new(String::new());

    let navigate_for_chips = navigate.clone();
    let navigate_for_filters = navigate.clone();
    let navigate_for_pagination = navigate.clone();

    view! {
        <div class="w-full px-4 py-8 text-black dark:text-white">
            <div class="max-w-6xl mx-auto flex flex-col lg:flex-row gap-6">
                <aside class="w-full lg:w-72 flex-shrink-0 bg-white dark:bg-gray-900 border border-gray-200 dark:border-gray-700 rounded-lg p-4 space-y-4">
                    <h3 class="text-lg font-semibold text-gray-800 dark:text-gray-200">
                        "Filters"
                    </h3>
                    <FilterInput
                        title="Repository"
                        placeholder="my-org/my-repo"
                        signal=repo_input.clone()
                        query_text=query_text.clone()
                        navigate=navigate_for_filters.clone()
                        kind="repo"
                    />
                    <FilterInput
                        title="Path"
                        placeholder="src/app"
                        signal=path_input.clone()
                        query_text=query_text.clone()
                        navigate=navigate_for_filters.clone()
                        kind="path"
                    />
                    <FilterInput
                        title="Branch"
                        placeholder="main"
                        signal=branch_input.clone()
                        query_text=query_text.clone()
                        navigate=navigate_for_filters.clone()
                        kind="branch"
                    />
                    <FilterInput
                        title="Language"
                        placeholder="rust"
                        signal=language_input.clone()
                        query_text=query_text.clone()
                        navigate=navigate_for_filters.clone()
                        kind="lang"
                    />
                    <div class="border-t border-gray-200 dark:border-gray-700 pt-4">
                        <h4 class="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-2">
                            "Search Insights"
                        </h4>
                        <Suspense fallback=move || {
                            view! { <p class="text-xs text-gray-500">"Loading stats..."</p> }
                        }>
                            {move || match search_results.get() {
                                Some(Ok(results_page)) => {
                                    if results_page.results.is_empty() {
                                        view! {
                                            <p class="text-xs text-gray-500">
                                                "No results yet. Run a search to see stats."
                                            </p>
                                        }
                                            .into_any()
                                    } else if results_page.stats.common_directories.is_empty()
                                        && results_page.stats.top_repositories.is_empty()
                                        && results_page.stats.top_branches.is_empty()
                                    {
                                        view! {
                                            <p class="text-xs text-gray-500">
                                                "Not enough matches for insights."
                                            </p>
                                        }
                                            .into_any()
                                    } else {
                                        view! {
                                            <SearchStatsPanel
                                                stats=results_page.stats.clone()
                                                query_text=query_text.clone()
                                                navigate=navigate_for_filters.clone()
                                            />
                                        }
                                            .into_any()
                                    }
                                }
                                Some(Err(_)) => {
                                    view! {
                                        <p class="text-xs text-red-500">"Failed to load stats."</p>
                                    }
                                        .into_any()
                                }
                                None => {
                                    view! {
                                        <p class="text-xs text-gray-500">"Loading stats..."</p>
                                    }
                                        .into_any()
                                }
                            }}
                        </Suspense>
                    </div>
                </aside>
                <div class="flex-1 space-y-4">
                    <div class="flex flex-wrap gap-2">
                        {move || {
                            let chips = filter_chips(&query_text.get());
                            if chips.is_empty() {
                                view! { <p class="text-xs text-gray-500">"No filters applied."</p> }
                                    .into_any()
                            } else {
                                view! {
                                    <>
                                        {chips
                                            .into_iter()
                                            .map(|(label, token)| {
                                                let query_text = query_text.clone();
                                                let navigate = navigate_for_chips.clone();
                                                view! {
                                                    <span class="inline-flex items-center gap-1 rounded-full bg-blue-100 text-blue-700 dark:bg-blue-900/50 dark:text-blue-100 px-3 py-1 text-xs">
                                                        <span>{label.clone()}</span>
                                                        <button
                                                            class="hover:text-red-500"
                                                            on:click=move |_| {
                                                                let mut q = query_text.get();
                                                                q = remove_token(&q, &token);
                                                                query_text.set(q.clone());
                                                                submit_search(&navigate, &query_text, 1);
                                                            }
                                                        >
                                                            "×"
                                                        </button>
                                                    </span>
                                                }
                                            })
                                            .collect_view()}
                                    </>
                                }
                                    .into_any()
                            }
                        }}
                    </div>
                    <Suspense fallback=|| {
                        view! { <p class="text-center py-8">"Loading..."</p> }
                    }>
                        {move || {
                            search_results
                                .get()
                                .map(|res| match res {
                                    Ok(results_page) => {
                                        if results_page.results.is_empty() {
                                            EitherOf3::A(
                                                view! {
                                                    <p class="text-center py-8">"No results found."</p>
                                                },
                                            )
                                        } else {
                                            let page = results_page.page as usize;
                                            let has_more = results_page.has_more;
                                            let prev_page = page.saturating_sub(1).max(1);
                                            let next_page = page + 1;
                                            EitherOf3::B(
                                                view! {
                                                    <div class="space-y-4">
                                                        <p class="text-sm text-gray-600 dark:text-gray-400">
                                                            {format!(
                                                                "Showing page {} ({} results per page)",
                                                                page,
                                                                results_page.page_size,
                                                            )}
                                                        </p>
                                                        {results_page
                                                            .results
                                                            .into_iter()
                                                            .map(|result| view! { <SearchResultCard result=result /> })
                                                            .collect_view()}
                                                        <div class="flex items-center justify-between pt-4">
                                                            <button
                                                                class="px-4 py-2 rounded bg-gray-200 dark:bg-gray-700 hover:bg-gray-300 dark:hover:bg-gray-600 disabled:opacity-50 disabled:cursor-not-allowed"
                                                                disabled=move || page <= 1
                                                                on:click={
                                                                    let query_text = query_text.clone();
                                                                    let navigate = navigate_for_pagination.clone();
                                                                    move |_| {
                                                                        if page > 1 {
                                                                            submit_search(&navigate, &query_text, prev_page);
                                                                        }
                                                                    }
                                                                }
                                                            >
                                                                "Previous"
                                                            </button>
                                                            <span class="text-sm text-gray-600 dark:text-gray-400">
                                                                {format!("Page {}", page)}
                                                            </span>
                                                            <button
                                                                class="px-4 py-2 rounded bg-gray-200 dark:bg-gray-700 hover:bg-gray-300 dark:hover:bg-gray-600 disabled:opacity-50 disabled:cursor-not-allowed"
                                                                disabled=move || !has_more
                                                                on:click={
                                                                    let query_text = query_text.clone();
                                                                    let navigate = navigate_for_pagination.clone();
                                                                    move |_| {
                                                                        if has_more {
                                                                            submit_search(&navigate, &query_text, next_page);
                                                                        }
                                                                    }
                                                                }
                                                            >
                                                                "Next"
                                                            </button>
                                                        </div>
                                                    </div>
                                                },
                                            )
                                        }
                                    }
                                    Err(e) => {
                                        EitherOf3::C(
                                            view! {
                                                <p class="text-red-500 text-center py-8">
                                                    "Error: " {e.to_string()}
                                                </p>
                                            },
                                        )
                                    }
                                })
                        }}
                    </Suspense>
                </div>
            </div>
        </div>
    }
}

#[component]
fn FilterInput<F>(
    title: &'static str,
    placeholder: &'static str,
    signal: RwSignal<String>,
    query_text: RwSignal<String>,
    navigate: F,
    kind: &'static str,
) -> impl IntoView
where
    F: Fn(&str, NavigateOptions) + Clone + 'static,
{
    view! {
        <div class="flex flex-col gap-1">
            <label class="text-xs uppercase tracking-wide text-gray-500 dark:text-gray-400">
                {title}
            </label>
            <div class="flex gap-2">
                <input
                    class="input input-sm input-bordered flex-1 bg-white dark:bg-gray-900"
                    placeholder=placeholder
                    prop:value=move || signal.get()
                    on:input=move |ev| signal.set(event_target_value(&ev))
                    on:keydown={
                        let signal = signal.clone();
                        let query_text = query_text.clone();
                        let navigate = navigate.clone();
                        move |ev: leptos::ev::KeyboardEvent| {
                            if ev.key() == "Enter" {
                                let value = signal.get();
                                append_filter(&query_text, &navigate, kind, value.clone());
                                signal.set(String::new());
                            }
                        }
                    }
                />
                <button
                    class="btn btn-xs"
                    on:click={
                        let signal = signal.clone();
                        let query_text = query_text.clone();
                        let navigate = navigate.clone();
                        move |_| {
                            let value = signal.get();
                            append_filter(&query_text, &navigate, kind, value.clone());
                            signal.set(String::new());
                        }
                    }
                >
                    "Add"
                </button>
            </div>
        </div>
    }
}

#[component]
fn SearchStatsPanel<F>(
    stats: SearchResultsStats,
    query_text: RwSignal<String>,
    navigate: F,
) -> impl IntoView
where
    F: Fn(&str, NavigateOptions) + Clone + 'static,
{
    let SearchResultsStats {
        common_directories,
        top_repositories,
        top_branches,
    } = stats;

    fn section_header(title: &'static str) -> impl IntoView {
        view! {
            <h5 class="text-xs uppercase tracking-wide text-gray-500 dark:text-gray-400 mb-1">
                {title}
            </h5>
        }
    }

    fn empty_message(message: &'static str) -> impl IntoView {
        view! { <p class="text-xs text-gray-500">{message}</p> }
    }

    fn list_item<FInc, FExc>(
        facet: FacetCount,
        include_action: FInc,
        exclude_action: FExc,
    ) -> impl IntoView
    where
        FInc: Fn() + 'static,
        FExc: Fn() + 'static,
    {
        let FacetCount { value, count } = facet;
        let label = value.clone();
        view! {
            <li class="flex flex-col gap-1 rounded-md border border-gray-200 dark:border-gray-700 p-2 bg-gray-50 dark:bg-gray-900/30">
                <div class="flex items-center justify-between gap-2">
                    <span
                        class="text-sm font-medium text-gray-800 dark:text-gray-100 truncate"
                        title=label.clone()
                    >
                        {label.clone()}
                    </span>
                    <span class="text-xs text-gray-500 dark:text-gray-400">
                        {format!("{} hits", count)}
                    </span>
                </div>
                <div class="flex gap-2">
                    <button
                        class="btn btn-ghost btn-xs text-blue-600 dark:text-blue-400 px-2"
                        on:click=move |_| include_action()
                    >
                        "Include"
                    </button>
                    <button
                        class="btn btn-ghost btn-xs text-red-500 dark:text-red-400 px-2"
                        on:click=move |_| exclude_action()
                    >
                        "Exclude"
                    </button>
                </div>
            </li>
        }
    }

    let directories_view = if common_directories.is_empty() {
        Either::Left(empty_message("No common directories yet."))
    } else {
        let query_text = query_text.clone();
        let navigate = navigate.clone();
        Either::Right(
            common_directories
                .into_iter()
                .map(move |facet| {
                    let include_value = format!("{}/*", facet.value);
                    let exclude_value = include_value.clone();
                    let query_text_include = query_text.clone();
                    let navigate_include = navigate.clone();
                    let query_text_exclude = query_text.clone();
                    let navigate_exclude = navigate.clone();
                    list_item(
                        facet,
                        move || {
                            append_filter(
                                &query_text_include,
                                &navigate_include,
                                "path",
                                include_value.clone(),
                            )
                        },
                        move || {
                            append_negated_filter(
                                &query_text_exclude,
                                &navigate_exclude,
                                "path",
                                exclude_value.clone(),
                            )
                        },
                    )
                })
                .collect_view(),
        )
    };

    let repositories_view = if top_repositories.is_empty() {
        Either::Left(empty_message("No repository stats yet."))
    } else {
        let query_text = query_text.clone();
        let navigate = navigate.clone();
        Either::Right(
            top_repositories
                .into_iter()
                .map(move |facet| {
                    let include_value = facet.value.clone();
                    let exclude_value = include_value.clone();
                    let query_text_include = query_text.clone();
                    let navigate_include = navigate.clone();
                    let query_text_exclude = query_text.clone();
                    let navigate_exclude = navigate.clone();
                    list_item(
                        facet,
                        move || {
                            append_filter(
                                &query_text_include,
                                &navigate_include,
                                "repo",
                                include_value.clone(),
                            )
                        },
                        move || {
                            append_negated_filter(
                                &query_text_exclude,
                                &navigate_exclude,
                                "repo",
                                exclude_value.clone(),
                            )
                        },
                    )
                })
                .collect_view(),
        )
    };

    let branches_view = if top_branches.is_empty() {
        Either::Left(empty_message("No branch stats yet."))
    } else {
        let query_text = query_text.clone();
        let navigate = navigate.clone();
        Either::Right(
            top_branches
                .into_iter()
                .map(move |facet| {
                    let include_value = facet.value.clone();
                    let exclude_value = include_value.clone();
                    let query_text_include = query_text.clone();
                    let navigate_include = navigate.clone();
                    let query_text_exclude = query_text.clone();
                    let navigate_exclude = navigate.clone();
                    list_item(
                        facet,
                        move || {
                            append_filter(
                                &query_text_include,
                                &navigate_include,
                                "branch",
                                include_value.clone(),
                            )
                        },
                        move || {
                            append_negated_filter(
                                &query_text_exclude,
                                &navigate_exclude,
                                "branch",
                                exclude_value.clone(),
                            )
                        },
                    )
                })
                .collect_view(),
        )
    };

    view! {
        <div class="space-y-4">
            <div>
                {section_header("Common Paths")} <ul class="space-y-2">{directories_view}</ul>
            </div>
            <div>
                {section_header("Top Repositories")} <ul class="space-y-2">{repositories_view}</ul>
            </div>
            <div>{section_header("Top Branches")} <ul class="space-y-2">{branches_view}</ul></div>
        </div>
    }
}

fn remove_token(query: &str, token: &str) -> String {
    let mut parts: Vec<_> = query
        .split_whitespace()
        .filter(|part| *part != token)
        .map(|s| s.to_string())
        .collect();
    parts.dedup();
    parts.join(" ")
}

fn filter_chips(query: &str) -> Vec<(String, String)> {
    let mut chips = Vec::new();
    for token in query.split_whitespace() {
        if let Some((key, value)) = token.split_once(':') {
            let label = format!("{}: {}", key, value.trim_matches('"'));
            chips.push((label, token.to_string()));
        }
    }
    chips
}

fn submit_search<F>(navigate: &F, query_text: &RwSignal<String>, page: usize)
where
    F: Fn(&str, NavigateOptions),
{
    let q = query_text.get();
    let encoded = encode(&q);
    navigate(
        &format!("/search?q={}&page={}", encoded, page.max(1)),
        Default::default(),
    );
}

fn build_filter_token(kind: &str, value: &str, negate: bool) -> String {
    let quoted = if value.contains(' ') {
        format!("\"{}\"", value)
    } else {
        value.to_string()
    };
    if negate {
        format!("-{}:{}", kind, quoted)
    } else {
        format!("{}:{}", kind, quoted)
    }
}

fn append_token<F>(query_text: &RwSignal<String>, navigate: &F, token: String)
where
    F: Fn(&str, NavigateOptions),
{
    let mut current = query_text.get();
    if !current.split_whitespace().any(|existing| existing == token) {
        if !current.trim().is_empty() {
            current.push(' ');
        }
        current.push_str(&token);
        query_text.set(current);
    }

    submit_search(navigate, query_text, 1);
}

fn append_filter<F>(query_text: &RwSignal<String>, navigate: &F, kind: &str, value: String)
where
    F: Fn(&str, NavigateOptions),
{
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return;
    }
    let token = build_filter_token(kind, trimmed, false);
    append_token(query_text, navigate, token);
}

fn append_negated_filter<F>(query_text: &RwSignal<String>, navigate: &F, kind: &str, value: String)
where
    F: Fn(&str, NavigateOptions),
{
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return;
    }
    let token = build_filter_token(kind, trimmed, true);
    append_token(query_text, navigate, token);
}

#[component]
fn SearchResultCard(result: SearchResult) -> impl IntoView {
    let SearchResult {
        repository,
        commit_sha,
        file_path,
        start_line,
        end_line,
        match_line,
        content_text,
        branches,
        is_historical,
        snippets,
    } = result;

    let mut snippet_vec = snippets;
    let (primary_snippet, extra_snippets_vec) = if snippet_vec.is_empty() {
        (
            SearchSnippet {
                start_line,
                end_line,
                match_line,
                content_text,
            },
            Vec::new(),
        )
    } else {
        let primary_idx = snippet_vec
            .iter()
            .position(|snippet| {
                snippet.match_line == match_line && snippet.content_text == content_text
            })
            .unwrap_or(0);
        let primary = snippet_vec.swap_remove(primary_idx);
        (primary, snippet_vec)
    };
    let extra_snippets = extra_snippets_vec;
    let extra_count = extra_snippets.len();
    let expanded = RwSignal::new(false);

    let branch_badge = (!branches.is_empty()).then(|| {
        let label = format!("Branches: {}", branches.join(", "));
        view! {
            <span class="inline-flex items-center rounded-full bg-blue-100 text-blue-700 dark:bg-blue-900/50 dark:text-blue-100 px-2 py-0.5">
                {label}
            </span>
        }
    });

    let historical_badge = is_historical.then(|| view! {
        <span class="inline-flex items-center rounded-full bg-amber-200 text-amber-900 dark:bg-amber-900/60 dark:text-amber-100 px-2 py-0.5">
            "Historical"
        </span>
    });

    let short_commit: String = commit_sha.chars().take(7).collect();
    let primary_label = format!(
        "{}/{}:{}",
        repository, file_path, primary_snippet.match_line
    );
    let primary_link = format!(
        "/repo/{}/tree/{}/{}#L{}",
        repository, commit_sha, file_path, primary_snippet.match_line,
    );

    let extra_section = (extra_count > 0).then(|| {
        let repo = repository.clone();
        let commit = commit_sha.clone();
        let path = file_path.clone();
        let snippets = extra_snippets.clone();
        view! {
            <div class="mt-3 space-y-3">
                <button
                    class="text-sm text-blue-600 dark:text-blue-400 hover:underline"
                    on:click=move |_| {
                        expanded.update(|value| *value = !*value);
                    }
                >
                    {move || {
                        if expanded.get() {
                            match extra_count {
                                1 => "Hide 1 additional match".to_string(),
                                n => format!("Hide {} additional matches", n),
                            }
                        } else {
                            match extra_count {
                                1 => "Show 1 additional match".to_string(),
                                n => format!("Show {} additional matches", n),
                            }
                        }
                    }}
                </button>
                <Show when=move || expanded.get() fallback=move || view! { <></> }>
                    {
                        let repo = repo.clone();
                        let commit = commit.clone();
                        let path = path.clone();
                        let snippets = snippets.clone();
                        view! {
                            <div class="space-y-3">
                                {snippets
                                    .iter()
                                    .cloned()
                                    .map(|snippet| {
                                        let location_label = format!(
                                            "{}/{}:{}",
                                            repo,
                                            path,
                                            snippet.match_line,
                                        );
                                        let link = format!(
                                            "/repo/{}/tree/{}/{}#L{}",
                                            repo,
                                            commit,
                                            path,
                                            snippet.match_line,
                                        );
                                        view! {
                                            <div class="border border-dashed border-gray-300 dark:border-gray-700 rounded-md p-3 bg-gray-50 dark:bg-gray-900/40">
                                                <p class="font-mono text-xs break-all">
                                                    <a
                                                        href=link
                                                        class="hover:underline text-blue-600 dark:text-blue-400 break-all"
                                                    >
                                                        {location_label.clone()}
                                                    </a>
                                                </p>
                                                <pre class="bg-gray-100 dark:bg-gray-900 p-2 rounded-md mt-2 text-sm overflow-x-auto max-w-full">
                                                    <code>
                                                        {render_highlighted_snippet(snippet.content_text.clone())}
                                                    </code>
                                                </pre>
                                            </div>
                                        }
                                    })
                                    .collect_view()}
                            </div>
                        }
                    }
                </Show>
            </div>
        }
    });

    view! {
        <div class="mt-4 p-4 border border-gray-300 dark:border-gray-700 rounded-md bg-white dark:bg-gray-800 break-words max-w-full">
            <p class="font-mono text-sm break-all">
                <a
                    href=primary_link
                    class="hover:underline text-blue-600 dark:text-blue-400 break-all"
                >
                    {primary_label}
                </a>
            </p>
            <div class="flex flex-wrap items-center gap-2 mt-1 text-xs text-gray-600 dark:text-gray-400">
                <span>{format!("Commit {}", short_commit)}</span>
                {branch_badge}
                {historical_badge}
            </div>
            <pre class="bg-gray-100 dark:bg-gray-900 p-2 rounded-md mt-2 text-sm overflow-x-auto max-w-full">
                <code>{render_highlighted_snippet(primary_snippet.content_text.clone())}</code>
            </pre>
            {extra_section}
        </div>
    }
}

fn render_highlighted_snippet(text: String) -> impl IntoView {
    parse_highlight_segments(&text)
        .into_iter()
        .map(|(segment, highlighted)| {
            if highlighted {
                Either::Left(view! {
                    <span>
                        <mark>{segment}</mark>
                    </span>
                })
            } else {
                Either::Right(view! { <span>{segment}</span> })
            }
        })
        .collect_view()
}

fn parse_highlight_segments(input: &str) -> Vec<(String, bool)> {
    const OPEN: &str = "<mark>";
    const CLOSE: &str = "</mark>";

    let mut segments = Vec::new();
    let mut cursor = 0;
    while let Some(start_rel) = input[cursor..].find(OPEN) {
        let start_idx = cursor + start_rel;
        if start_idx > cursor {
            segments.push((input[cursor..start_idx].to_string(), false));
        }

        let highlight_start = start_idx + OPEN.len();
        if let Some(end_rel) = input[highlight_start..].find(CLOSE) {
            let highlight_end = highlight_start + end_rel;
            segments.push((input[highlight_start..highlight_end].to_string(), true));
            cursor = highlight_end + CLOSE.len();
        } else {
            segments.push((input[start_idx..].to_string(), false));
            cursor = input.len();
            break;
        }
    }

    if cursor < input.len() {
        segments.push((input[cursor..].to_string(), false));
    }

    segments.retain(|(segment, _)| !segment.is_empty());
    segments
}
