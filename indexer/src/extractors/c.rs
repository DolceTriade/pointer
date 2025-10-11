use tree_sitter::{Node, Parser};

use super::ExtractedSymbol;

pub fn extract(source: &str) -> Vec<ExtractedSymbol> {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_c::LANGUAGE.into())
        .expect("failed to load tree-sitter C grammar");

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
            let name = node
                .child_by_field_name("declarator")
                .and_then(|decl| identifier_from_declarator(&decl, source));
            match name {
                Some(name) => vec![ExtractedSymbol {
                    name,
                    kind: "fn".to_string(),
                    namespace: None,
                }],
                None => Vec::new(),
            }
        }
        "struct_specifier" => symbols_from_struct(node, source, "struct"),
        "union_specifier" => symbols_from_struct(node, source, "union"),
        "enum_specifier" => symbol_from_named(node, source, "enum"),
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
                    kind: if is_function_like { "fn" } else { "var" }.to_string(),
                    namespace: None,
                }];
            }
        }
    }

    Vec::new()
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

    vec![ExtractedSymbol {
        name,
        kind: kind.to_string(),
        namespace: None,
    }]
}

fn symbols_from_declaration(node: &Node, source: &[u8]) -> Vec<ExtractedSymbol> {
    if is_typedef(node, source) {
        return Vec::new();
    }

    let mut vars = Vec::new();
    let mut cursor = node.walk();
    let namespace = namespace_for_node(node, source);

    for child in node.children(&mut cursor) {
        match child.kind() {
            "init_declarator" => {
                let declarator = child.child_by_field_name("declarator").unwrap_or(child);
                if declarator.kind() == "function_declarator" {
                    continue;
                }
                if let Some(name) = identifier_from_declarator(&declarator, source) {
                    let is_function_like = declarator_contains_function(&declarator);
                    vars.push(ExtractedSymbol {
                        name,
                        kind: if is_function_like { "fn" } else { "var" }.to_string(),
                        namespace: namespace.clone(),
                    });
                }
            }
            "declarator"
            | "pointer_declarator"
            | "reference_declarator"
            | "abstract_declarator"
            | "parenthesized_declarator" => {
                if child.kind() == "function_declarator" {
                    continue;
                }
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
            "identifier" => {
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

fn namespace_for_node(node: &Node, source: &[u8]) -> Option<String> {
    let mut parts = Vec::new();
    let mut current = node.parent();

    while let Some(parent) = current {
        match parent.kind() {
            "struct_specifier" | "union_specifier" => {
                if let Some(name_node) = parent.child_by_field_name("name") {
                    if let Ok(text) = name_node.utf8_text(source) {
                        parts.push(text.to_string());
                    }
                }
            }
            "function_definition" => {
                if let Some(declarator) = parent.child_by_field_name("declarator") {
                    if let Some(name) = identifier_from_declarator(&declarator, source) {
                        parts.push(name);
                    }
                }
            }
            _ => {}
        }
        current = parent.parent();
    }

    if parts.is_empty() {
        None
    } else {
        parts.reverse();
        Some(parts.join("::"))
    }
}

fn symbols_from_struct(node: &Node, source: &[u8], kind: &str) -> Vec<ExtractedSymbol> {
    let mut symbols = symbol_from_named(node, source, kind);
    symbols.extend(function_pointer_fields(node, source));
    symbols
}

fn function_pointer_fields(node: &Node, source: &[u8]) -> Vec<ExtractedSymbol> {
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
    fn extracts_functions_types_and_variables() {
        let source = r#"
            customType uninitialized;
            int (*global_handler)(int);
            struct Foo {
                int value;
            };

            struct Callbacks {
                int (*on_ready)(int);
                int count;
            };

            enum Bar {
                A,
                B,
            };

            int counter = 0;

            static int helper(void) {
                int local = 3;
                int (*local_callback)(int);
                return local;
            }

            int run(struct Foo foo) {
                int result = helper();
                return result;
            }
        "#;

        let mut symbols = extract(source);
        symbols.sort_by(|a, b| a.name.cmp(&b.name));

        assert!(
            symbols
                .iter()
                .any(|s| s.name == "Foo" && s.kind == "struct")
        );
        assert!(symbols.iter().any(|s| s.name == "Bar" && s.kind == "enum"));
        assert!(symbols.iter().any(|s| s.name == "helper" && s.kind == "fn"));
        assert!(symbols.iter().any(|s| s.name == "run" && s.kind == "fn"));

        let var_names: Vec<_> = symbols
            .iter()
            .filter(|s| s.kind == "var")
            .map(|s| s.name.as_str())
            .collect();

        assert!(var_names.contains(&"counter"));
        assert!(var_names.contains(&"local"));
        assert!(var_names.contains(&"result"));
        assert!(var_names.contains(&"uninitialized"));
        assert!(!var_names.contains(&"local_callback"));
        assert!(!var_names.contains(&"global_handler"));

        let fn_symbols: Vec<_> = symbols
            .iter()
            .filter(|s| s.kind == "fn")
            .map(|s| (s.name.as_str(), s.namespace.as_deref()))
            .collect();

        assert!(fn_symbols.contains(&("global_handler", None)));
        assert!(fn_symbols.contains(&("local_callback", Some("helper"))));
        assert!(fn_symbols.contains(&("on_ready", Some("Callbacks"))));
    }
}
