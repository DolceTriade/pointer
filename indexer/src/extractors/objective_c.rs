use tree_sitter::{Node, Parser};

use super::ExtractedSymbol;

pub fn extract(source: &str) -> Vec<ExtractedSymbol> {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_objc::LANGUAGE.into())
        .expect("failed to load tree-sitter Objective-C grammar");

    let tree = match parser.parse(source, None) {
        Some(tree) => tree,
        None => return Vec::new(),
    };

    let root = tree.root_node();
    let source_bytes = source.as_bytes();
    let mut stack = vec![root];
    let mut symbols = Vec::new();

    while let Some(node) = stack.pop() {
        symbols.extend(collect_symbols(&node, source_bytes));

        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            stack.push(child);
        }
    }

    symbols
}

fn collect_symbols(node: &Node, source: &[u8]) -> Vec<ExtractedSymbol> {
    match node.kind() {
        "function_definition" => symbol_from_function(node, source),
        "method_definition" => symbol_from_method(node, source),
        "class_interface" | "class_implementation" => symbol_from_named(node, source, "class"),
        "category_interface" | "category_implementation" => {
            symbol_from_named(node, source, "category")
        }
        "protocol_declaration" => symbol_from_named(node, source, "protocol"),
        "property_declaration" => symbols_from_property(node, source),
        "ivar_declaration" | "instance_variable" => symbols_from_ivar(node, source),
        "declaration" => symbols_from_declaration(node, source),
        "preproc_function_def" => macro_symbol(node, source, true),
        "preproc_def" => macro_symbol(node, source, false),
        _ => Vec::new(),
    }
}

fn macro_symbol(node: &Node, source: &[u8], is_function_like: bool) -> Vec<ExtractedSymbol> {
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.kind() == "identifier" {
            if let Ok(text) = child.utf8_text(source) {
                return vec![ExtractedSymbol {
                    name: text.to_string(),
                    kind: if is_function_like { "function" } else { "var" }.to_string(),
                    namespace: None,
                }];
            }
        }
    }

    Vec::new()
}

fn find_identifier<'a>(node: &Node<'a>) -> Option<Node<'a>> {
    if let Some(name) = node.child_by_field_name("name") {
        return Some(name);
    }
    if let Some(ident) = node.child_by_field_name("identifier") {
        return Some(ident);
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.is_named() && child.kind() == "identifier" {
            return Some(child);
        }
    }

    None
}

fn identifier_text(node: &Node, source: &[u8]) -> Option<String> {
    node.utf8_text(source)
        .ok()
        .map(|text| text.trim().to_string())
}

fn symbol_from_named(node: &Node, source: &[u8], kind: &str) -> Vec<ExtractedSymbol> {
    let name_node = match find_identifier(node) {
        Some(name) => name,
        None => return Vec::new(),
    };

    let name = match identifier_text(&name_node, source) {
        Some(text) => text,
        None => return Vec::new(),
    };

    vec![ExtractedSymbol {
        name,
        kind: kind.to_string(),
        namespace: namespace_for_node(node, source),
    }]
}

fn symbol_from_function(node: &Node, source: &[u8]) -> Vec<ExtractedSymbol> {
    let declarator = match node.child_by_field_name("declarator") {
        Some(decl) => decl,
        None => return Vec::new(),
    };

    let name = identifier_from_declarator(&declarator, source);
    match name {
        Some(name) => vec![ExtractedSymbol {
            name,
            kind: "function".to_string(),
            namespace: namespace_for_node(node, source),
        }],
        None => Vec::new(),
    }
}

fn symbol_from_method(node: &Node, source: &[u8]) -> Vec<ExtractedSymbol> {
    let selector_node = node
        .child_by_field_name("selector")
        .or_else(|| find_identifier(node))
        .or_else(|| node.child(0))
        .filter(|n| n.is_named());

    let selector_node = match selector_node {
        Some(node) => node,
        None => return Vec::new(),
    };

    let name = match identifier_text(&selector_node, source) {
        Some(text) => text,
        None => return Vec::new(),
    };

    vec![ExtractedSymbol {
        name,
        kind: "method".to_string(),
        namespace: namespace_for_node(node, source),
    }]
}

fn symbols_from_property(node: &Node, source: &[u8]) -> Vec<ExtractedSymbol> {
    let mut results = Vec::new();
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.kind() == "struct_declaration" {
            let mut inner = child.walk();
            for declarator in child.children(&mut inner) {
                if declarator.kind() == "struct_declarator" {
                    if let Some(name) = identifier_from_declarator(&declarator, source) {
                        let namespace = namespace_for_node(node, source);
                        let is_function_like = declarator_contains_function(&declarator);
                        results.push(ExtractedSymbol {
                            name,
                            kind: if is_function_like {
                                "function"
                            } else {
                                "property"
                            }
                            .to_string(),
                            namespace,
                        });
                    }
                }
            }
        }
    }
    results
}

fn symbols_from_ivar(node: &Node, source: &[u8]) -> Vec<ExtractedSymbol> {
    let mut results = Vec::new();
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.kind() == "struct_declaration" {
            let mut inner = child.walk();
            for declarator in child.children(&mut inner) {
                if declarator.kind() == "struct_declarator" {
                    if let Some(name) = identifier_from_declarator(&declarator, source) {
                        let namespace = namespace_for_node(node, source);
                        let is_function_like = declarator_contains_function(&declarator);
                        results.push(ExtractedSymbol {
                            name,
                            kind: if is_function_like { "function" } else { "ivar" }.to_string(),
                            namespace,
                        });
                    }
                }
            }
        }
    }
    results
}

fn symbols_from_declaration(node: &Node, source: &[u8]) -> Vec<ExtractedSymbol> {
    let mut results = Vec::new();
    let mut cursor = node.walk();

    for child in node.children(&mut cursor) {
        if child.kind() == "init_declarator" {
            let declarator = child.child_by_field_name("declarator").unwrap_or(child);
            if let Some(name) = identifier_from_declarator(&declarator, source) {
                let namespace = namespace_for_node(node, source);
                let is_function_like = declarator_contains_function(&declarator);
                results.push(ExtractedSymbol {
                    name,
                    kind: if is_function_like { "function" } else { "var" }.to_string(),
                    namespace,
                });
            }
        } else if matches!(
            child.kind(),
            "declarator"
                | "pointer_declarator"
                | "reference_declarator"
                | "abstract_declarator"
                | "identifier"
        ) {
            if let Some(name) = identifier_from_declarator(&child, source) {
                let namespace = namespace_for_node(node, source);
                let is_function_like = declarator_contains_function(&child)
                    || identifier_in_function_declarator(&child);
                results.push(ExtractedSymbol {
                    name,
                    kind: if is_function_like { "function" } else { "var" }.to_string(),
                    namespace,
                });
            }
        } else if child.kind() == "function_declarator" {
            if let Some(name) = identifier_from_declarator(&child, source) {
                results.push(ExtractedSymbol {
                    name,
                    kind: "function".to_string(),
                    namespace: namespace_for_node(node, source),
                });
            }
        }
    }

    results
}

fn identifier_from_declarator(node: &Node, source: &[u8]) -> Option<String> {
    if matches!(node.kind(), "identifier" | "field_identifier") {
        return node.utf8_text(source).ok().map(|text| text.to_string());
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if let Some(name) = identifier_from_declarator(&child, source) {
            return Some(name);
        }
    }

    None
}

fn declarator_contains_function(node: &Node) -> bool {
    match node.kind() {
        "function_declarator" | "abstract_function_declarator" => return true,
        _ => {}
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if declarator_contains_function(&child) {
            return true;
        }
    }

    false
}

fn identifier_in_function_declarator(node: &Node) -> bool {
    let mut current = node.parent();
    while let Some(parent) = current {
        if parent.kind().ends_with("declarator") && declarator_contains_function(&parent) {
            return true;
        }
        current = parent.parent();
    }
    false
}

fn namespace_for_node(node: &Node, source: &[u8]) -> Option<String> {
    let mut segments = Vec::new();
    let mut current = node.parent();

    while let Some(parent) = current {
        match parent.kind() {
            "class_interface"
            | "class_implementation"
            | "category_interface"
            | "category_implementation"
            | "protocol_declaration" => {
                if let Some(name_node) = find_identifier(&parent) {
                    if let Some(text) = identifier_text(&name_node, source) {
                        segments.push(text);
                    }
                }
            }
            "method_definition" => {
                if let Some(selector_node) = parent
                    .child_by_field_name("selector")
                    .or_else(|| find_identifier(&parent))
                {
                    if let Some(text) = identifier_text(&selector_node, source) {
                        segments.push(text);
                    }
                }
            }
            "function_definition" => {
                if let Some(declarator) = parent.child_by_field_name("declarator") {
                    if let Some(name) = identifier_from_declarator(&declarator, source) {
                        segments.push(name);
                    }
                }
            }
            _ => {}
        }
        current = parent.parent();
    }

    if segments.is_empty() {
        None
    } else {
        segments.reverse();
        Some(segments.join("."))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_objective_c_symbols() {
        let source = r#"
            int solovar;
            static int (*global_handler)(int);
            @interface Demo : NSObject {
                int _count;
                int (*callback)(int);
            }
            @property(nonatomic) int value;
            @property(nonatomic) int (*onReady)(int);
            - (void)doThing;
            @end

            @implementation Demo
            - (void)doThing {
                int local = 0;
                int temp;
                int (*local_handler)(int);
            }
            @end

            void Helper(void) {
                int global = 1;
                int global_no_init;
                int (*helper_callback)(int);
            }
        "#;

        let mut symbols = extract(source);
        symbols.sort_by(|a, b| a.name.cmp(&b.name));

        assert!(symbols
            .iter()
            .any(|s| s.name == "Demo" && s.kind == "class"));
        assert!(symbols.iter().any(|s| {
            s.name == "doThing" && s.kind == "method" && s.namespace.as_deref() == Some("Demo")
        }));
        assert!(symbols
            .iter()
            .any(|s| s.name == "Helper" && s.kind == "function"));

        let fields: Vec<_> = symbols
            .iter()
            .filter(|s| matches!(s.kind.as_str(), "ivar" | "property"))
            .map(|s| (s.name.as_str(), s.kind.as_str()))
            .collect();
        assert!(fields.contains(&("_count", "ivar")));
        assert!(fields.contains(&("value", "property")));
        assert!(!fields
            .iter()
            .any(|(name, kind)| *name == "callback" && *kind == "ivar"));
        assert!(!fields
            .iter()
            .any(|(name, kind)| *name == "onReady" && *kind == "property"));

        let vars: Vec<_> = symbols
            .iter()
            .filter(|s| s.kind == "var")
            .map(|s| (s.name.as_str(), s.namespace.as_deref()))
            .collect();
        assert!(vars.contains(&("local", Some("Demo.doThing"))));
        assert!(vars.contains(&("temp", Some("Demo.doThing"))));
        assert!(vars.contains(&("global", Some("Helper"))));
        assert!(vars.contains(&("global_no_init", Some("Helper"))));
        assert!(vars.contains(&("solovar", None)));
        assert!(!vars.iter().any(|(name, _)| *name == "global_handler"));
        assert!(!vars.iter().any(|(name, _)| *name == "callback"));
        assert!(!vars.iter().any(|(name, _)| *name == "onReady"));
        assert!(!vars.iter().any(|(name, _)| *name == "helper_callback"));
        assert!(!vars.iter().any(|(name, _)| *name == "local_handler"));

        let fn_symbols: Vec<_> = symbols
            .iter()
            .filter(|s| s.kind == "function")
            .map(|s| (s.name.as_str(), s.namespace.as_deref()))
            .collect();
        assert!(fn_symbols.contains(&("Helper", None)));
        assert!(fn_symbols.contains(&("global_handler", None)));
        assert!(fn_symbols.contains(&("callback", Some("Demo"))));
        assert!(fn_symbols.contains(&("onReady", Some("Demo"))));
        assert!(fn_symbols.contains(&("helper_callback", Some("Helper"))));
        assert!(fn_symbols.contains(&("local_handler", Some("Demo.doThing"))));
    }
}
