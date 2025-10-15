use crate::components::search_bar::SearchBar;
use leptos::tachys::dom::event_target_checked;
use leptos::{either::Either, prelude::*};
use leptos_darkmode::Darkmode;
use leptos_router::hooks::{use_query, use_url};

#[component]
pub fn Header() -> impl IntoView {
    let mut darkmode = use_context::<Darkmode>();
    let route = use_url();
    let query_struct = use_query::<crate::pages::search::SearchParams>();
    let query = Memo::new(move |_| {
        query_struct
            .read()
            .as_ref()
            .ok()
            .and_then(|q| q.q.clone())
            .unwrap_or_default()
    });

    view! {
        <header class="navbar flex justify-between bg-base-100 shadow-md w-full">
            <div class="flex-none">
                <a href="/" class="text-xl font-bold">
                    Pointer
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
            <div class="flex-none">
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
                    <ul class="mt-3 p-2 shadow menu menu-sm dropdown-content bg-base-100 rounded-box w-56 z-50">
                        <li>
                            <div class="p-2">
                                <div class="form-control">
                                    <label class="cursor-pointer label">
                                        <span class="label-text">Dark Mode</span>
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
    }
}
