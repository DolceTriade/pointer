use tree_sitter::{Node, Parser};

use super::ExtractedSymbol;

pub fn extract(source: &str) -> Vec<ExtractedSymbol> {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_cpp::language())
        .expect("failed to load tree-sitter C++ grammar");

    let tree = match parser.parse(source, None) {
        Some(tree) => tree,
        None => return Vec::new(),
    };

    let root = tree.root_node();
    let source_bytes = source.as_bytes();
    let mut stack = vec![root];
    let mut symbols = Vec::new();

    while let Some(node) = stack.pop() {
        let mut extracted = collect_symbols(&node, source_bytes);
        symbols.append(&mut extracted);

        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            stack.push(child);
        }
    }

    symbols
}

fn collect_symbols(node: &Node, source: &[u8]) -> Vec<ExtractedSymbol> {
    match node.kind() {
        "function_definition" => {
            let declarator = node.child_by_field_name("declarator");
            let name = declarator
                .as_ref()
                .and_then(|decl| identifier_from_declarator(decl, source));
            let namespace = declarator
                .as_ref()
                .and_then(|decl| namespace_from_qualified(decl, source))
                .or_else(|| namespace_for_node(node, source));
            match name {
                Some(name) => vec![ExtractedSymbol {
                    name,
                    kind: "fn".to_string(),
                    namespace,
                }],
                None => Vec::new(),
            }
        }
        "class_specifier" => symbols_from_class_like(node, source, "class"),
        "struct_specifier" => symbols_from_class_like(node, source, "struct"),
        "enum_specifier" => symbol_from_named(node, source, "enum"),
        "namespace_definition" => symbol_from_named(node, source, "namespace"),
        "declaration" | "simple_declaration" => symbols_from_simple_declaration(node, source),
        _ => Vec::new(),
    }
}

fn symbol_from_named(node: &Node, source: &[u8], kind: &str) -> Vec<ExtractedSymbol> {
    let name_node = match node.child_by_field_name("name") {
        Some(name) => name,
        None => return Vec::new(),
    };

    let name = match name_node.utf8_text(source) {
        Ok(text) => text.to_string(),
        Err(_) => return Vec::new(),
    };

    let namespace = namespace_for_node(node, source);

    vec![ExtractedSymbol {
        name,
        kind: kind.to_string(),
        namespace,
    }]
}

fn symbols_from_simple_declaration(node: &Node, source: &[u8]) -> Vec<ExtractedSymbol> {
    if is_typedef(node, source) {
        return Vec::new();
    }

    let mut vars = Vec::new();
    let mut cursor = node.walk();
    let namespace = namespace_for_node(node, source);

    for child in node.children(&mut cursor) {
        match child.kind() {
            "init_declarator" | "structured_binding_declarator" => {
                let declarator = child.child_by_field_name("declarator").unwrap_or(child);
                if let Some(name) = identifier_from_declarator(&declarator, source) {
                    let is_function_like = declarator_contains_function(&declarator);
                    vars.push(ExtractedSymbol {
                        name,
                        kind: if is_function_like { "fn" } else { "var" }.to_string(),
                        namespace: namespace.clone(),
                    });
                }
            }
            "declarator" | "pointer_declarator" | "reference_declarator" => {
                if let Some(name) = identifier_from_declarator(&child, source) {
                    let is_function_like = declarator_contains_function(&child);
                    vars.push(ExtractedSymbol {
                        name,
                        kind: if is_function_like { "fn" } else { "var" }.to_string(),
                        namespace: namespace.clone(),
                    });
                }
            }
            "function_declarator" => {
                if let Some(name) = identifier_from_declarator(&child, source) {
                    vars.push(ExtractedSymbol {
                        name,
                        kind: "fn".to_string(),
                        namespace: namespace.clone(),
                    });
                }
            }
            "identifier" | "field_identifier" => {
                if let Some(parent) = child.parent() {
                    if parent.kind() == "init_declarator" {
                        continue;
                    }
                }
                if let Ok(text) = child.utf8_text(source) {
                    let is_function_like = identifier_in_function_declarator(&child);
                    vars.push(ExtractedSymbol {
                        name: text.to_string(),
                        kind: if is_function_like { "fn" } else { "var" }.to_string(),
                        namespace: namespace.clone(),
                    });
                }
            }
            _ => {}
        }
    }

    vars
}

fn identifier_from_declarator(node: &Node, source: &[u8]) -> Option<String> {
    if matches!(node.kind(), "identifier" | "field_identifier") {
        return node.utf8_text(source).ok().map(|text| text.to_string());
    }

    let mut last = None;
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if let Some(name) = identifier_from_declarator(&child, source) {
            last = Some(name);
        }
    }

    last
}

fn namespace_for_node(node: &Node, source: &[u8]) -> Option<String> {
    let mut names = Vec::new();
    let mut current = node.parent();

    while let Some(parent) = current {
        match parent.kind() {
            "namespace_definition" | "class_specifier" | "struct_specifier" => {
                if let Some(name_node) = parent.child_by_field_name("name") {
                    if let Ok(text) = name_node.utf8_text(source) {
                        names.push(text.to_string());
                    }
                }
            }
            "function_definition" => {
                if let Some(declarator) = parent.child_by_field_name("declarator") {
                    if let Some(name) = identifier_from_declarator(&declarator, source) {
                        names.push(name);
                    }
                }
            }
            _ => {}
        }
        current = parent.parent();
    }

    if names.is_empty() {
        None
    } else {
        names.reverse();
        Some(names.join("::"))
    }
}

fn namespace_from_qualified(node: &Node, source: &[u8]) -> Option<String> {
    if node.kind() == "qualified_identifier" {
        if let Ok(text) = node.utf8_text(source) {
            if let Some(idx) = text.rfind("::") {
                let prefix = &text[..idx];
                if !prefix.is_empty() {
                    return Some(prefix.to_string());
                }
            }
        }
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if let Some(result) = namespace_from_qualified(&child, source) {
            return Some(result);
        }
    }

    None
}

fn is_typedef(node: &Node, source: &[u8]) -> bool {
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.kind() == "storage_class_specifier" {
            if let Ok(text) = child.utf8_text(source) {
                if text.trim() == "typedef" {
                    return true;
                }
            }
        }
    }
    false
}

fn declarator_contains_function(node: &Node) -> bool {
    if node.kind() == "function_declarator" {
        return true;
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if declarator_contains_function(&child) {
            return true;
        }
    }

    false
}

fn symbols_from_class_like(node: &Node, source: &[u8], kind: &str) -> Vec<ExtractedSymbol> {
    let mut symbols = symbol_from_named(node, source, kind);
    symbols.extend(function_pointer_members(node, source));
    symbols
}

fn function_pointer_members(node: &Node, source: &[u8]) -> Vec<ExtractedSymbol> {
    let mut results = Vec::new();
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.kind() == "field_declaration_list" {
            collect_function_declarators(&child, source, &mut results);
        }
    }

    results
}

fn collect_function_declarators(node: &Node, source: &[u8], out: &mut Vec<ExtractedSymbol>) {
    let mut stack = vec![*node];

    while let Some(current) = stack.pop() {
        if current.kind() == "function_declarator" {
            if let Some(name) = identifier_from_declarator(&current, source) {
                out.push(ExtractedSymbol {
                    name,
                    kind: "fn".to_string(),
                    namespace: namespace_for_node(&current, source),
                });
            }
        }

        let mut cursor = current.walk();
        for child in current.children(&mut cursor) {
            if child.kind() != ";" {
                stack.push(child);
            }
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_cpp_symbols_and_variables() {
        let source = r#"
            int (*global_handler)(int);
            cvar_t *r_maxPolyVerts;
            namespace foo {
                int (*foo_handler)(int);
                int foo_global = 2;
                class Bar {
                public:
                    int value;
                    int (*on_ready)(int);
                    int method() {
                        int local = value;
                        int (*local_callback)(int);
                        return local;
                    }
                };

                int run();
                int counter = 10;
            }

            int foo::run() {
                int result = counter;
                return result;
            }
        "#;

        let mut symbols = extract(source);
        symbols.sort_by(|a, b| a.name.cmp(&b.name));

        assert!(symbols.iter().any(|s| s.name == "Bar"
            && s.kind == "class"
            && s.namespace.as_deref() == Some("foo")));
        assert!(symbols
            .iter()
            .any(|s| s.name == "foo" && s.kind == "namespace"));
        assert!(symbols.iter().any(|s| s.name == "method"
            && s.kind == "fn"
            && s.namespace.as_deref() == Some("foo::Bar")));
        assert!(symbols
            .iter()
            .any(|s| s.name == "run" && s.kind == "fn" && s.namespace.as_deref() == Some("foo")));

        let var_names: Vec<_> = symbols
            .iter()
            .filter(|s| s.kind == "var")
            .map(|s| (s.name.as_str(), s.namespace.as_deref()))
            .collect();
        assert!(var_names.contains(&("counter", Some("foo"))));
        assert!(var_names.contains(&("r_maxPolyVerts", None)));
        assert!(var_names.contains(&("foo_global", Some("foo"))));
        assert!(var_names.iter().any(|(name, _)| *name == "local"));
        assert!(var_names.iter().any(|(name, _)| *name == "result"));
        assert!(!var_names.contains(&("global_handler", None)));
        assert!(!var_names.contains(&("foo_handler", Some("foo"))));
        assert!(!var_names.iter().any(|(name, ns)| *name == "on_ready" && *ns == Some("foo::Bar")));
        assert!(!var_names
            .iter()
            .any(|(name, ns)| *name == "local_callback" && *ns == Some("foo::Bar::method")));

        let fn_symbols: Vec<_> = symbols
            .iter()
            .filter(|s| s.kind == "fn")
            .map(|s| (s.name.as_str(), s.namespace.as_deref()))
            .collect();

        assert!(fn_symbols.contains(&("global_handler", None)));
        assert!(fn_symbols.contains(&("foo_handler", Some("foo"))));
        assert!(fn_symbols.contains(&("on_ready", Some("foo::Bar"))));
        assert!(fn_symbols.contains(&("local_callback", Some("foo::Bar::method"))));
    }
}
