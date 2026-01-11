use leptos::either::Either;
use leptos::prelude::*;
use leptos_router::components::A;

#[component]
pub fn CopyPathButton(path: Signal<String>) -> impl IntoView {
    let trimmed_path = Memo::new(move |_| path.get().trim_matches('/').to_string());
    let copy_feedback = RwSignal::new(None::<String>);
    let copy_path = {
        let trimmed_path = trimmed_path.clone();
        let copy_feedback = copy_feedback.clone();
        move |_event: leptos::ev::MouseEvent| {
            let path_value = trimmed_path.get_untracked();
            if path_value.is_empty() {
                return;
            }

            if let Some(window) = web_sys::window() {
                let clipboard = window.navigator().clipboard();
                _ = clipboard.write_text(&path_value);
                copy_feedback.set(Some(path_value.clone()));
                let signal = copy_feedback.clone();
                set_timeout(
                    move || {
                        signal.set(None);
                    },
                    std::time::Duration::from_secs(2),
                );
            }
        }
    };

    view! {
        <Show when=move || !trimmed_path.get().is_empty() fallback=|| ()>
            <div class="flex flex-col gap-2 w-fit">
                <button
                    class="inline-flex items-center gap-2 text-xs font-semibold border border-slate-300 dark:border-slate-600 rounded-md px-3 py-1.5 bg-white/80 dark:bg-slate-900/50 text-slate-700 dark:text-slate-100 hover:bg-slate-100 dark:hover:bg-slate-800 transition-colors"
                    type="button"
                    on:click=copy_path
                    title="Copy file path"
                >
                    <svg
                        xmlns="http://www.w3.org/2000/svg"
                        viewBox="0 0 24 24"
                        fill="none"
                        stroke="currentColor"
                        stroke-width="1.5"
                        class="h-3.5 w-3.5"
                    >
                        <path
                            stroke-linecap="round"
                            stroke-linejoin="round"
                            d="M8 8h8a2 2 0 012 2v8a2 2 0 01-2 2H8a2 2 0 01-2-2v-8a2 2 0 012-2z"
                        ></path>
                        <path
                            stroke-linecap="round"
                            stroke-linejoin="round"
                            d="M16 4h-8a2 2 0 00-2 2v2h2V6h8v8h-2v2h2a2 2 0 002-2v-8a2 2 0 00-2-2z"
                        ></path>
                    </svg>
                    <span>"Copy path"</span>
                </button>
                <Show when=move || copy_feedback.get().is_some() fallback=|| ()>
                    <span class="badge badge-outline text-xs font-mono border-slate-300 dark:border-slate-600 text-slate-700 dark:text-slate-100 bg-white/80 dark:bg-slate-900/40">
                        {move || {
                            copy_feedback
                                .get()
                                .map(|value| format!("Copied {value}"))
                                .unwrap_or_default()
                        }}
                    </span>
                </Show>
            </div>
        </Show>
    }
}

#[component]
pub fn Breadcrumbs(
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
        <div class="flex flex-wrap items-center gap-3 mb-6 text-slate-700 dark:text-slate-300">
            <div class="text-sm breadcrumbs flex-1 min-w-0 text-inherit">
                <ul>
                    <li>
                        <A
                            href=move || format!("/repo/{}", repo())
                            attr:class="text-slate-700 dark:text-slate-200 hover:text-slate-900 dark:hover:text-white"
                        >
                            {move || repo()}
                        </A>
                    </li>
                    <li>
                        <A
                            href=move || format!("/repo/{}/tree/{}/", repo(), branch())
                            attr:class="text-slate-700 dark:text-slate-200 hover:text-slate-900 dark:hover:text-white"
                        >
                            {move || branch()}
                        </A>
                    </li>
                    <For
                        each=move || segments.get()
                        key=|(_, p, _)| p.clone()
                        children=move |(name, p, is_last)| {
                            let full_path = format!(
                                "/repo/{}/tree/{}/{}",
                                repo.get(),
                                branch.get(),
                                p,
                            );
                            view! {
                                <li>
                                    {if is_last {
                                        Either::Left(
                                            view! {
                                                <span class="truncate font-medium text-slate-900 dark:text-white">
                                                    {name}
                                                </span>
                                            },
                                        )
                                    } else {
                                        Either::Right(
                                            view! {
                                                <A
                                                    href=full_path
                                                    attr:class="text-slate-700 dark:text-slate-200 hover:text-slate-900 dark:hover:text-white"
                                                >
                                                    {name}
                                                </A>
                                            },
                                        )
                                    }}
                                </li>
                            }
                        }
                    />
                </ul>
            </div>
        </div>
    }
}

pub fn directory_prefix(path: &str) -> Option<String> {
    let trimmed = path.trim_matches('/');
    if trimmed.is_empty() {
        return None;
    }
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
