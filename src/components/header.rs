use crate::components::search_bar::SearchBar;
use leptos::leptos_dom::helpers::window_event_listener;
use leptos::tachys::dom::event_target_checked;
use leptos::{either::Either, prelude::*};
use leptos_darkmode::Darkmode;
use leptos_router::hooks::{use_query, use_url};
use std::rc::Rc;
use urlencoding::decode;
use web_sys::{self, HtmlElement, wasm_bindgen::JsCast};

#[component]
pub fn Header() -> impl IntoView {
    let mut darkmode = use_context::<Darkmode>();
    let route = use_url();
    let query_struct = use_query::<crate::pages::search::SearchParams>();
    let (show_search_overlay, set_show_search_overlay) = signal(false);

    let contextual_defaults = Memo::new(move |_| {
        let url = route.read();
        contextual_query_for_path(url.path())
    });
    let query = Memo::new(move |_| {
        query_struct
            .read()
            .as_ref()
            .ok()
            .and_then(|q| q.q.clone())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| contextual_defaults.get())
    });

    // Global "/" to open the search overlay, Esc to dismiss
    Effect::new({
        let show_search_overlay = show_search_overlay.clone();
        let set_show_search_overlay = set_show_search_overlay.clone();
        move |_| {
            let handle =
                window_event_listener(leptos::ev::keydown, move |ev: web_sys::KeyboardEvent| {
                    if ev.key() == "Escape" && show_search_overlay.get_untracked() {
                        ev.prevent_default();
                        set_show_search_overlay.set(false);
                        return;
                    }

                    if ev.key() != "/" || ev.ctrl_key() || ev.meta_key() || ev.alt_key() {
                        return;
                    }

                    if let Some(target) = ev.target() {
                        let is_form_field = target.dyn_ref::<web_sys::HtmlInputElement>().is_some()
                            || target.dyn_ref::<web_sys::HtmlTextAreaElement>().is_some()
                            || target.dyn_ref::<web_sys::HtmlSelectElement>().is_some();

                        if is_form_field {
                            return;
                        }

                        if let Some(el) = target.dyn_ref::<HtmlElement>() {
                            if el.is_content_editable() {
                                return;
                            }
                        }
                    }

                    ev.prevent_default();
                    set_show_search_overlay.set(true);
                });
            on_cleanup(move || handle.remove());
        }
    });

    view! {
        <header class="navbar flex justify-between w-full shadow-md border-b border-slate-200/70 dark:border-slate-800/70 bg-white/90 dark:bg-slate-950/80 text-slate-900 dark:text-white backdrop-blur">
            <div class="flex-none items-center justify-between mx-auto p-2">
                <a href="/" class="flex items-center gap-2">
                    <img class="hover:animate-spin w-14" src="/asterisk.svg" alt="Logo" />
                    <span class="text-xl font-semibold whitespace-nowrap text-slate-900 dark:text-white">
                        Pointer
                    </span>
                </a>
            </div>
            <div class="flex-1 flex justify-center">
                {move || {
                    if route.read().path() != "/" {
                        Either::Left(view! { <SearchBar initial_query=query.get() /> })
                    } else {
                        Either::Right(view! { <div /> })
                    }
                }}
            </div>
            <div class="flex-none text-slate-600 dark:text-white">
                <details class="dropdown dropdown-end">
                    <summary class="btn btn-ghost btn-circle">
                        <svg
                            xmlns="http://www.w3.org/2000/svg"
                            fill="none"
                            viewBox="0 0 24 24"
                            class="h-6 w-6 stroke-current"
                        >
                            <path
                                stroke-linecap="round"
                                stroke-linejoin="round"
                                stroke-width="2"
                                d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z"
                            />
                            <path
                                stroke-linecap="round"
                                stroke-linejoin="round"
                                stroke-width="2"
                                d="M15 12a3 3 0 11-6 0 3 3 0 016 0z"
                            />
                        </svg>
                    </summary>
                    <ul class="mt-3 p-2 shadow menu menu-sm dropdown-content rounded-box w-56 z-50 bg-white/95 dark:bg-slate-900 border border-slate-200 dark:border-slate-800 text-slate-900 dark:text-slate-100">
                        <li>
                            <div class="p-2">
                                <div class="form-control">
                                    <label class="cursor-pointer label">
                                        <span class="label-text text-slate-700 dark:text-slate-200">
                                            Dark Mode
                                        </span>
                                        <input
                                            type="checkbox"
                                            class="toggle toggle-primary"
                                            prop:checked={
                                                let darkmode = darkmode.clone();
                                                move || {
                                                    darkmode.clone().map(|v| v.get()).unwrap_or_default()
                                                }
                                            }
                                            on:change=move |ev| {
                                                let val = event_target_checked(&ev);
                                                darkmode.as_mut().and_then(|v| Some(v.set(val)));
                                            }
                                        />
                                    </label>
                                </div>
                            </div>
                        </li>
                    </ul>
                </details>
            </div>
        </header>
        {move || {
            if show_search_overlay.get() {
                let close_overlay = {
                    let set_show_search_overlay = set_show_search_overlay.clone();
                    move || set_show_search_overlay.set(false)
                };
                let close_overlay_cb: Rc<dyn Fn()> = Rc::new(close_overlay);
                view! {
                    <div
                        class="fixed inset-0 z-50 flex items-start justify-center bg-black/50 backdrop-blur-sm"
                        on:click=move |_| set_show_search_overlay.set(false)
                    >
                        <div
                            class="mt-16 w-full max-w-3xl px-4"
                            on:click=|ev| ev.stop_propagation()
                        >
                            <div class="flex justify-end mb-3">
                                <button
                                    class="text-sm text-slate-200 hover:text-white transition-colors"
                                    on:click=move |_| set_show_search_overlay.set(false)
                                >
                                    "Esc to close"
                                </button>
                            </div>
                            <SearchBar
                                initial_query=query.get()
                                auto_focus=true
                                on_complete=close_overlay_cb.clone()
                                open_in_new_tab=true
                            />
                        </div>
                    </div>
                }
                    .into_any()
            } else {
                view! { <div /> }.into_any()
            }
        }}
    }
}

fn contextual_query_for_path(path: &str) -> String {
    let trimmed = path.trim_start_matches('/');
    if trimmed.is_empty() {
        return String::new();
    }

    let has_trailing_slash = trimmed.ends_with('/');
    let segments: Vec<String> = trimmed
        .split('/')
        .filter(|segment| !segment.is_empty())
        .map(decode_segment)
        .collect();

    if segments.is_empty() || segments[0] != "repo" {
        return String::new();
    }

    let mut tokens = Vec::new();
    if let Some(repo) = segments.get(1) {
        if let Some(token) = build_contextual_token("repo", repo) {
            tokens.push(token);
        }
    } else {
        return String::new();
    }

    if segments.get(2).map(|s| s.as_str()) == Some("tree") {
        if let Some(branch) = segments.get(3) {
            if let Some(token) = build_contextual_token("branch", branch) {
                tokens.push(token);
            }
        }

        if segments.len() > 4 {
            let mut path_value = segments[4..].join("/");
            if has_trailing_slash && !path_value.ends_with('/') {
                path_value.push('/');
            }
            if let Some(token) = build_path_token(&path_value, has_trailing_slash) {
                tokens.push(token);
            }
        }
    }

    tokens.join(" ")
}

fn decode_segment(segment: &str) -> String {
    decode(segment)
        .map(|cow| cow.into_owned())
        .unwrap_or_else(|_| segment.to_string())
}

fn build_contextual_token(key: &str, value: &str) -> Option<String> {
    if value.is_empty() {
        return None;
    }
    let needs_quotes = value.chars().any(char::is_whitespace) || value.contains('"');
    let token = if needs_quotes {
        let escaped = value.replace('"', "\\\"");
        format!(r#"{key}:"{}""#, escaped)
    } else {
        format!("{key}:{value}")
    };
    Some(token)
}

fn build_path_token(path: &str, is_directory: bool) -> Option<String> {
    if path.is_empty() {
        return None;
    }
    let mut value = path.to_string();
    if is_directory {
        if !value.ends_with('/') {
            value.push('/');
        }
        if !value.ends_with('*') {
            value.push('*');
        }
    }
    build_contextual_token("path", &value)
}

#[cfg(test)]
mod tests {
    use super::contextual_query_for_path;

    #[test]
    fn contextual_query_repo_only() {
        assert_eq!(
            contextual_query_for_path("/repo/Pointer"),
            "repo:Pointer".to_string()
        );
    }

    #[test]
    fn contextual_query_repo_branch_path() {
        assert_eq!(
            contextual_query_for_path("/repo/foo/tree/main/src/lib.rs"),
            "repo:foo branch:main path:src/lib.rs".to_string()
        );
    }

    #[test]
    fn contextual_query_handles_encoded_segments_and_spaces() {
        assert_eq!(
            contextual_query_for_path("/repo/Foo%20Bar/tree/release%2F1.0/docs/My%20Guide/"),
            r#"repo:"Foo Bar" branch:release/1.0 path:"docs/My Guide/*""#.to_string()
        );
    }

    #[test]
    fn contextual_query_directory_adds_wildcard() {
        assert_eq!(
            contextual_query_for_path("/repo/foo/tree/main/docs"),
            "repo:foo branch:main path:docs".to_string()
        );
        assert_eq!(
            contextual_query_for_path("/repo/foo/tree/main/docs/"),
            "repo:foo branch:main path:docs/*".to_string()
        );
    }

    #[test]
    fn contextual_query_non_repo_path_returns_empty() {
        assert!(contextual_query_for_path("/search").is_empty());
    }
}
