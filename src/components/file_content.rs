#[cfg(all(feature = "hydrate", target_arch = "wasm32"))]
use crate::scope_parser::visible_scope_chain;
use crate::scope_parser::{ScopeBreadcrumb, ScopeInfo, extract_scopes};
use leptos::html::{Code, Div};
use leptos::prelude::*;
use std::rc::Rc;
use web_sys::wasm_bindgen::JsCast;
use web_sys::wasm_bindgen::UnwrapThrowExt;

const SYMBOL_HIGHLIGHT_CLASS: &str = "selected-symbol-highlight";
const BREADCRUMB_BAR_ID: &str = "scope-breadcrumb-bar";
const CODE_SCROLL_CONTAINER_ID: &str = "code-scroll-container";
const STICKY_SCROLL_PADDING: f64 = 12.0;

#[component]
pub fn FileContent(
    html: String,
    line_count: usize,
    selected_symbol: RwSignal<Option<String>>,
    content: String,
    language: Option<String>,
) -> impl IntoView {
    let code_ref = NodeRef::<Code>::new();
    let scroll_container_ref = NodeRef::<Div>::new();
    let scopes = Rc::new(extract_scopes(&content, language.as_deref()));
    let has_scopes = !scopes.is_empty();
    let active_scopes = use_scope_visibility_tracker(
        code_ref.clone(),
        scroll_container_ref.clone(),
        scopes.clone(),
    );
    let scopes_collapsed = RwSignal::new(false);

    let code_ref = code_ref.clone();
    Effect::new(move |_| {
        use leptos::leptos_dom::helpers::window_event_listener;
        use web_sys::{HtmlElement, Node};
        let code_ref = code_ref.clone();
        let handle =
            window_event_listener(leptos::ev::keydown, move |ev: web_sys::KeyboardEvent| {
                let uses_modifier = ev.ctrl_key() || ev.meta_key();
                if !uses_modifier || !ev.key().eq_ignore_ascii_case("a") {
                    return;
                }

                if let Some(window) = web_sys::window() {
                    if let Some(document) = window.document() {
                        if let Some(active) = document.active_element() {
                            if let Some(element) = active.dyn_ref::<HtmlElement>() {
                                let tag = element.tag_name();
                                let skip = matches!(tag.as_str(), "INPUT" | "TEXTAREA" | "SELECT")
                                    || element.is_content_editable();
                                if skip {
                                    return;
                                }
                            }
                        }

                        ev.prevent_default();
                        if let Some(code_el) = code_ref.get() {
                            if let Ok(Some(selection)) = window.get_selection() {
                                let _ = selection.remove_all_ranges();
                                let node: Node = code_el.into();
                                let _ = selection.select_all_children(&node);
                            }
                        }
                    }
                }
            });

        on_cleanup(move || handle.remove());
    });

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

    {
        let code_ref = code_ref.clone();
        let selected_symbol = selected_symbol.clone();
        Effect::new(move |_| {
            let current_symbol = selected_symbol.get();
            if let Some(code_el) = code_ref.get() {
                if let Some(window) = web_sys::window() {
                    if let Some(document) = window.document() {
                        let element: web_sys::Element = code_el.unchecked_into();

                        // 1. Save selection
                        let selection = window.get_selection().unwrap_throw().unwrap_throw();
                        if selection.range_count() == 0 {
                            return;
                        }
                        let range = selection.get_range_at(0).unwrap_throw();

                        let start_marker = document
                            .create_element("span")
                            .unwrap_throw()
                            .dyn_into::<web_sys::HtmlElement>()
                            .unwrap_throw();
                        start_marker.set_id("selection-start-marker");

                        let end_marker = document
                            .create_element("span")
                            .unwrap_throw()
                            .dyn_into::<web_sys::HtmlElement>()
                            .unwrap_throw();
                        end_marker.set_id("selection-end-marker");

                        let end_range = range.clone_range();
                        end_range.collapse_with_to_start(false);
                        web_sys::Range::insert_node(&end_range, &end_marker).unwrap_throw();
                        let start_range = range.clone_range();
                        start_range.collapse_with_to_start(true);
                        web_sys::Range::insert_node(&start_range, &start_marker).unwrap_throw();

                        // 2. Clear existing highlights
                        clear_symbol_highlights(&document, &element);

                        // 3. Apply new highlights
                        if let Some(symbol) = current_symbol {
                            if !symbol.is_empty() {
                                apply_symbol_highlights(&document, &element, &symbol);
                            }
                        }

                        // 4. Restore selection
                        let start_node = document.get_element_by_id("selection-start-marker");
                        let end_node = document.get_element_by_id("selection-end-marker");

                        if let (Some(start_node), Some(end_node)) = (start_node, end_node) {
                            if let Ok(new_range) = document.create_range() {
                                let _ = new_range.set_start_after(&start_node);
                                let _ = new_range.set_end_before(&end_node);

                                let _ = selection.remove_all_ranges();
                                let _ = selection.add_range(&new_range);

                                // 5. Clean up markers
                                if let Some(parent) = start_node.parent_node() {
                                    let _ = parent.remove_child(&start_node);
                                }
                                if let Some(parent) = end_node.parent_node() {
                                    let _ = parent.remove_child(&end_node);
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    view! {
        <div class="relative flex flex-col gap-2">
            <Show when=move || has_scopes fallback=move || view! { <></> }>
                <ScopeBreadcrumbBar
                    current=active_scopes.clone()
                    collapsed=scopes_collapsed.clone()
                />
            </Show>
            <div
                id=CODE_SCROLL_CONTAINER_ID
                class="relative rounded-md"
                node_ref=scroll_container_ref
            >
                <div class="flex font-mono overflow-x-auto text-sm min-w-full">
                    <div class="text-right text-gray-500 pr-4 select-none">
                        {(1..=line_count)
                            .map(|n| {
                                let link_id = format!("line-number-{}", n);
                                view! {
                                    <a
                                        id=link_id
                                        href=format!("#L{n}")
                                        class="block hover:text-blue-400 scroll-mt-20"
                                    >
                                        {n}
                                    </a>
                                }
                            })
                            .collect_view()}
                    </div>
                    <div class="flex-grow" tabindex="0" on:mouseup=on_mouse_up>
                        <code id="code-content" inner_html=html node_ref=code_ref />
                    </div>
                </div>
            </div>
        </div>
    }
}

#[component]
pub fn ScopeBreadcrumbBar(
    current: RwSignal<Vec<ScopeBreadcrumb>>,
    collapsed: RwSignal<bool>,
) -> impl IntoView {
    view! {
        <div
            id=BREADCRUMB_BAR_ID
            class="sticky top-0 z-20 bg-white/95 dark:bg-gray-900/95 backdrop-blur border-b border-gray-200 dark:border-gray-700 shadow-sm"
        >
            <div class="flex items-center justify-between gap-3 text-xs px-3 py-2 text-gray-600 dark:text-gray-300">
                <div class="flex flex-wrap items-center gap-2 overflow-hidden min-h-[1.5rem]">
                    {move || {
                        let stack = current.get();
                        if stack.is_empty() {
                            view! {
                                <span class="text-gray-500 dark:text-gray-400">
                                    "No enclosing scope"
                                </span>
                            }
                                .into_any()
                        } else if collapsed.get() {
                            view! {
                                <span class="text-gray-500 dark:text-gray-400 italic">
                                    {format!(
                                        "{} scope{}",
                                        stack.len(),
                                        if stack.len() == 1 { "" } else { "s" },
                                    )} " hidden"
                                </span>
                            }
                                .into_any()
                        } else {
                            stack
                                .into_iter()
                                .map(|scope| {
                                    let line = scope.start_line;
                                    let label = scope.label.clone();
                                    view! {
                                        <button
                                            class="inline-flex items-center gap-2 rounded-full bg-gray-100 dark:bg-gray-800 px-2 py-0.5 text-gray-700 dark:text-gray-100 hover:bg-blue-100 dark:hover:bg-blue-900 transition"
                                            on:click=move |_| scroll_to_line(line)
                                        >
                                            <span class="truncate max-w-[16rem]">{label}</span>
                                            <span class="text-[10px] text-gray-500 dark:text-gray-400">
                                                {"#"}{line}
                                            </span>
                                        </button>
                                    }
                                })
                                .collect_view()
                                .into_any()
                        }
                    }}
                </div>
                <button
                    class="text-[11px] uppercase tracking-wide px-2 py-1 rounded border border-gray-300 dark:border-gray-600 text-gray-600 dark:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-800 transition"
                    on:click=move |_| collapsed.update(|value| *value = !*value)
                >
                    {move || if collapsed.get() { "Expand" } else { "Collapse" }}
                </button>
            </div>
        </div>
    }
}

pub fn scroll_to_line(line: usize) {
    if let Some(window) = web_sys::window() {
        if let Some(document) = window.document() {
            let target_id = format!("line-number-{}", line);
            if let Some(target) = document.get_element_by_id(&target_id) {
                scroll_with_sticky_offset(&target);
            }
        }
    }
}

pub fn use_scope_visibility_tracker(
    code_ref: NodeRef<Code>,
    scroll_container_ref: NodeRef<Div>,
    scopes: Rc<Vec<ScopeInfo>>,
) -> RwSignal<Vec<ScopeBreadcrumb>> {
    let active_scopes = RwSignal::new(Vec::new());

    #[cfg(not(all(feature = "hydrate", target_arch = "wasm32")))]
    {
        let _ = code_ref;
        let _ = scroll_container_ref;
        let _ = scopes;
    }

    #[cfg(all(feature = "hydrate", target_arch = "wasm32"))]
    {
        use std::{cell::RefCell, collections::BTreeSet};
        let code_ref = code_ref.clone();
        let scroll_container_ref = scroll_container_ref.clone();
        let scopes = scopes.clone();
        let active_scopes_handle = active_scopes.clone();
        Effect::new(move |_| {
            if scopes.is_empty() {
                return;
            }
            let Some(code_el) = code_ref.get() else {
                return;
            };
            let Some(window) = web_sys::window() else {
                return;
            };
            let Some(document) = window.document() else {
                return;
            };
            let node_list = match code_el.query_selector_all("[data-line]") {
                Ok(nodes) => nodes,
                Err(_) => return,
            };
            if node_list.length() == 0 {
                return;
            }

            let sticky_offset = sticky_breadcrumb_offset(&document);
            let options = web_sys::IntersectionObserverInit::new();
            options.set_root_margin(&format!("-{}px 0px 0px 0px", sticky_offset));
            if let Some(container) = scroll_container_ref.get() {
                if container.scroll_height() > container.client_height() + 1 {
                    let element: web_sys::Element = container.clone().unchecked_into();
                    options.set_root(Some(&element));
                }
            }

            let visible_lines = Rc::new(RefCell::new(BTreeSet::new()));
            let scopes_for_update = scopes.clone();
            let active_scopes = active_scopes_handle.clone();

            let callback_visible = Rc::clone(&visible_lines);
            let callback_scopes = scopes_for_update.clone();
            let callback_signal = active_scopes.clone();
            let callback = wasm_bindgen::closure::Closure::wrap(Box::new(
                move |entries: web_sys::js_sys::Array, _observer: web_sys::IntersectionObserver| {
                    if update_visible_lines(&entries, &callback_visible) {
                        apply_scope_update(&callback_visible, &callback_scopes, &callback_signal);
                    }
                },
            )
                as Box<dyn FnMut(web_sys::js_sys::Array, web_sys::IntersectionObserver)>);

            let observer = match web_sys::IntersectionObserver::new_with_options(
                callback.as_ref().unchecked_ref(),
                &options,
            ) {
                Ok(observer) => observer,
                Err(_) => return,
            };
            for idx in 0..node_list.length() {
                if let Some(node) = node_list.item(idx) {
                    if let Ok(element) = node.dyn_into::<web_sys::Element>() {
                        let _ = observer.observe(&element);
                    }
                }
            }

            let initial = observer.take_records();
            if update_visible_lines(&initial, &visible_lines) {
                apply_scope_update(&visible_lines, &scopes_for_update, &active_scopes);
            }

            let handle = IntersectionObserverHandle::new(observer, callback);
            on_cleanup(move || drop(handle));
        });
    }

    active_scopes
}

#[cfg(all(feature = "hydrate", target_arch = "wasm32"))]
fn update_visible_lines(
    entries: &web_sys::js_sys::Array,
    visible_lines: &Rc<std::cell::RefCell<std::collections::BTreeSet<usize>>>,
) -> bool {
    let mut changed = false;
    {
        let mut set = visible_lines.borrow_mut();
        for entry in entries.iter() {
            if let Ok(entry) = entry.dyn_into::<web_sys::IntersectionObserverEntry>() {
                if let Ok(target) = entry.target().dyn_into::<web_sys::Element>() {
                    if let Some(line_attr) = target.get_attribute("data-line") {
                        if let Ok(line_no) = line_attr.parse::<usize>() {
                            if entry.is_intersecting() && entry.intersection_ratio() > 0.0 {
                                if set.insert(line_no) {
                                    changed = true;
                                }
                            } else if set.remove(&line_no) {
                                changed = true;
                            }
                        }
                    }
                }
            }
        }
    }
    changed
}

#[cfg(all(feature = "hydrate", target_arch = "wasm32"))]
fn apply_scope_update(
    visible_lines: &Rc<std::cell::RefCell<std::collections::BTreeSet<usize>>>,
    scopes: &Vec<ScopeInfo>,
    active_scopes: &RwSignal<Vec<ScopeBreadcrumb>>,
) {
    let set = visible_lines.borrow();
    if set.is_empty() {
        return;
    }
    let first = *set.iter().next().unwrap();
    let last = *set.iter().next_back().unwrap_or(&first);
    drop(set);

    active_scopes.set(visible_scope_chain(scopes, first, last.max(first)));
}

#[cfg(all(feature = "hydrate", target_arch = "wasm32"))]
struct IntersectionObserverHandle {
    observer: web_sys::IntersectionObserver,
    _callback: wasm_bindgen::closure::Closure<
        dyn FnMut(web_sys::js_sys::Array, web_sys::IntersectionObserver),
    >,
}

#[cfg(all(feature = "hydrate", target_arch = "wasm32"))]
impl IntersectionObserverHandle {
    fn new(
        observer: web_sys::IntersectionObserver,
        callback: wasm_bindgen::closure::Closure<
            dyn FnMut(web_sys::js_sys::Array, web_sys::IntersectionObserver),
        >,
    ) -> Self {
        Self {
            observer,
            _callback: callback,
        }
    }
}

#[cfg(all(feature = "hydrate", target_arch = "wasm32"))]
impl Drop for IntersectionObserverHandle {
    fn drop(&mut self) {
        self.observer.disconnect();
    }
}

#[cfg(all(feature = "hydrate", target_arch = "wasm32"))]
unsafe impl Send for IntersectionObserverHandle {}
#[cfg(all(feature = "hydrate", target_arch = "wasm32"))]
unsafe impl Sync for IntersectionObserverHandle {}

fn sticky_breadcrumb_offset(document: &web_sys::Document) -> f64 {
    document
        .get_element_by_id(BREADCRUMB_BAR_ID)
        .and_then(|element| element.dyn_into::<web_sys::HtmlElement>().ok())
        .map(|element| element.offset_height() as f64 + STICKY_SCROLL_PADDING)
        .unwrap_or(0.0)
}

pub fn scroll_with_sticky_offset(target: &web_sys::Element) {
    if let Some(window) = web_sys::window() {
        if let Some(document) = window.document() {
            let offset = sticky_breadcrumb_offset(&document);
            if let Some(container) = document.get_element_by_id(CODE_SCROLL_CONTAINER_ID) {
                if let Some(container_el) = container.dyn_ref::<web_sys::HtmlElement>() {
                    let scroll_height = container_el.scroll_height();
                    let client_height = container_el.client_height();
                    if scroll_height > client_height + 1 {
                        let element_rect = target.get_bounding_client_rect();
                        let container_rect = container_el.get_bounding_client_rect();
                        let relative_top = element_rect.top() - container_rect.top();
                        let desired = relative_top + container_el.scroll_top() as f64 - offset;
                        container_el.set_scroll_top(desired.max(0.0).round() as i32);
                        return;
                    }
                }
            }

            let rect = target.get_bounding_client_rect();
            let delta = rect.top() - offset;
            window.scroll_by_with_x_and_y(0.0, delta);
        }
    }
}

pub fn clear_symbol_highlights(document: &web_sys::Document, root: &web_sys::Element) {
    let selector = format!(".{SYMBOL_HIGHLIGHT_CLASS}");
    if let Ok(nodes) = root.query_selector_all(&selector) {
        let len = nodes.length();
        for idx in 0..len {
            if let Some(node) = nodes.item(idx) {
                if let Ok(element) = node.dyn_into::<web_sys::Element>() {
                    if let Some(parent) = element.parent_node() {
                        let text_content = element.text_content().unwrap_or_default();
                        let text_node: web_sys::Node =
                            document.create_text_node(&text_content).into();
                        let _ = parent.replace_child(&text_node, &element);
                    }
                }
            }
        }
    }
    let root_node: web_sys::Node = root.clone().into();
    root_node.normalize();
}

fn apply_symbol_highlights(document: &web_sys::Document, root: &web_sys::Element, needle: &str) {
    fn highlight_text_nodes(document: &web_sys::Document, node: &web_sys::Node, needle: &str) {
        let mut child_opt = node.first_child();
        while let Some(child) = child_opt {
            let next = child.next_sibling();
            match child.node_type() {
                web_sys::Node::TEXT_NODE => highlight_text_node(document, &child, needle),
                web_sys::Node::ELEMENT_NODE => {
                    let skip = child
                        .dyn_ref::<web_sys::Element>()
                        .map(|el| el.class_list().contains(SYMBOL_HIGHLIGHT_CLASS))
                        .unwrap_or(false);
                    if !skip {
                        highlight_text_nodes(document, &child, needle);
                    }
                }
                _ => {}
            }
            child_opt = next;
        }
    }

    fn highlight_text_node(document: &web_sys::Document, text_node: &web_sys::Node, needle: &str) {
        if needle.is_empty() {
            return;
        }
        let value = match text_node.node_value() {
            Some(v) => v,
            None => return,
        };
        if value.len() < needle.len() || !value.contains(needle) {
            return;
        }
        let needle_len = needle.len();
        if let Some(parent) = text_node.parent_node() {
            let fragment = document.create_document_fragment();
            let text_ref = value.as_str();
            let mut cursor = 0;
            while let Some(rel_pos) = text_ref[cursor..].find(needle) {
                let start = cursor + rel_pos;
                let end = start + needle_len;
                if start > cursor {
                    let prefix = &text_ref[cursor..start];
                    if !prefix.is_empty() {
                        let node: web_sys::Node = document.create_text_node(prefix).into();
                        fragment.append_child(&node).unwrap_throw();
                    }
                }
                let matched = &text_ref[start..end];
                let element = document.create_element("mark").unwrap_throw();
                element.set_class_name(SYMBOL_HIGHLIGHT_CLASS);
                element.set_text_content(Some(matched));
                let node: web_sys::Node = element.into();
                fragment.append_child(&node).unwrap_throw();
                cursor = end;
            }
            let tail = &text_ref[cursor..];
            if !tail.is_empty() {
                let node: web_sys::Node = document.create_text_node(tail).into();
                fragment.append_child(&node).unwrap_throw();
            }
            let fragment_node: web_sys::Node = fragment.into();
            let _ = parent.replace_child(&fragment_node, text_node);
        }
    }

    let root_node: web_sys::Node = root.clone().into();
    highlight_text_nodes(document, &root_node, needle);
}
