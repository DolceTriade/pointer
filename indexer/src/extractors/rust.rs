use tree_sitter::{Node, Parser};

use super::ExtractedSymbol;

pub fn extract(source: &str) -> Vec<ExtractedSymbol> {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_rust::language())
        .expect("failed to load tree-sitter Rust grammar");

    let tree = match parser.parse(source, None) {
        Some(tree) => tree,
        None => return Vec::new(),
    };

    let root = tree.root_node();
    let mut stack = vec![root];
    let mut symbols = Vec::new();
    let source_bytes = source.as_bytes();

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
        "function_item" | "method_item" => symbol_from_function(node, source),
        "struct_item" => symbol_from_named_item(node, source, "struct"),
        "enum_item" => symbol_from_named_item(node, source, "enum"),
        "trait_item" => symbol_from_named_item(node, source, "trait"),
        "mod_item" => symbol_from_named_item(node, source, "mod"),
        "const_item" => symbol_from_named_item(node, source, "const"),
        "static_item" => symbol_from_named_item(node, source, "static"),
        "type_item" => symbol_from_named_item(node, source, "type"),
        "field_declaration" => symbols_from_field(node, source),
        "tuple_field_declaration" => symbols_from_field(node, source),
        "let_declaration" => symbols_from_let(node, source),
        _ => Vec::new(),
    }
}

fn symbol_from_function(node: &Node, source: &[u8]) -> Vec<ExtractedSymbol> {
    let kind = if has_impl_ancestor(node) {
        "method"
    } else {
        "fn"
    };
    symbol_from_named_item(node, source, kind)
}

fn symbol_from_named_item(node: &Node, source: &[u8], kind: &str) -> Vec<ExtractedSymbol> {
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

fn symbols_from_field(node: &Node, source: &[u8]) -> Vec<ExtractedSymbol> {
    let name_node = match node.child_by_field_name("name") {
        Some(name) => name,
        None => return Vec::new(),
    };

    let name = match name_node.utf8_text(source) {
        Ok(text) => text.trim().to_string(),
        Err(_) => return Vec::new(),
    };

    let namespace = namespace_for_node(node, source);

    vec![ExtractedSymbol {
        name,
        kind: "field".to_string(),
        namespace,
    }]
}

fn has_impl_ancestor(node: &Node) -> bool {
    let mut current = node.parent();
    while let Some(parent) = current {
        if parent.kind() == "impl_item" {
            return true;
        }
        current = parent.parent();
    }
    false
}

fn symbols_from_let(node: &Node, source: &[u8]) -> Vec<ExtractedSymbol> {
    let pattern = match node.child_by_field_name("pattern") {
        Some(pattern) => pattern,
        None => return Vec::new(),
    };

    let mut names = Vec::new();
    collect_pattern_bindings(&pattern, source, &mut names);

    let namespace = namespace_for_node(node, source);

    names
        .into_iter()
        .map(|name| ExtractedSymbol {
            name,
            kind: "var".to_string(),
            namespace: namespace.clone(),
        })
        .collect()
}

fn collect_pattern_bindings(pattern: &Node, source: &[u8], out: &mut Vec<String>) {
    match pattern.kind() {
        "identifier" => {
            if let Ok(text) = pattern.utf8_text(source) {
                let trimmed = text.trim();
                if !trimmed.starts_with('_') {
                    out.push(trimmed.to_string());
                }
            }
        }
        "tuple_pattern"
        | "tuple_struct_pattern"
        | "slice_pattern"
        | "struct_pattern"
        | "struct_pattern_elements"
        | "struct_pattern_field"
        | "pattern_list" => {
            let mut cursor = pattern.walk();
            for child in pattern.children(&mut cursor) {
                collect_pattern_bindings(&child, source, out);
            }
        }
        _ => {
            let mut cursor = pattern.walk();
            for child in pattern.children(&mut cursor) {
                collect_pattern_bindings(&child, source, out);
            }
        }
    }
}

fn namespace_for_node(node: &Node, source: &[u8]) -> Option<String> {
    let mut names = Vec::new();
    let mut current = node.parent();
    while let Some(parent) = current {
        match parent.kind() {
            "mod_item" => {
                if let Some(name_node) = parent.child_by_field_name("name") {
                    if let Ok(name) = name_node.utf8_text(source) {
                        names.push(name.to_string());
                    }
                }
            }
            "impl_item" => {
                if let Some(name) = impl_target_name(&parent, source) {
                    names.push(name);
                }
            }
            "struct_item" | "enum_item" | "trait_item" => {
                if let Some(name_node) = parent.child_by_field_name("name") {
                    if let Ok(name) = name_node.utf8_text(source) {
                        names.push(name.to_string());
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

fn impl_target_name(node: &Node, source: &[u8]) -> Option<String> {
    if let Some(type_node) = node.child_by_field_name("type") {
        return type_node
            .utf8_text(source)
            .ok()
            .map(|t| t.trim().to_string());
    }

    if let Some(type_node) = node.child_by_field_name("trait") {
        if let Ok(text) = type_node.utf8_text(source) {
            return Some(text.trim().to_string());
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_public_symbols_with_namespaces_and_vars() {
        let source = r#"
            pub mod foo {
                pub struct Bar {
                    inner: i32,
                }
                pub(crate) fn helper() {}
                fn private_fn() {}

                impl Bar {
                    pub fn method(&self) {
                        let inner = 2;
                        let (left, right) = (1, 2);
                        let _ignored = 10;
                    }
                }
            }

            pub enum TopLevel {}
            pub fn top_fn() {}
            fn hidden() {}

            fn variable_owner() {
                let count = 1;
            }
        "#;

        let mut symbols = extract(source);
        symbols.sort_by(|a, b| a.name.cmp(&b.name));

        assert!(symbols.iter().any(|s| s.name == "foo" && s.kind == "mod"));
        assert!(symbols.iter().any(|s| s.name == "Bar"
            && s.kind == "struct"
            && s.namespace.as_deref() == Some("foo")));
        assert!(symbols.iter().any(|s| s.name == "helper"
            && s.kind == "fn"
            && s.namespace.as_deref() == Some("foo")));
        assert!(
            symbols
                .iter()
                .any(|s| s.name == "TopLevel" && s.kind == "enum")
        );
        assert!(symbols.iter().any(|s| s.name == "top_fn" && s.kind == "fn"));

        assert!(
            symbols
                .iter()
                .any(|s| s.name == "private_fn" && s.kind == "fn")
        );
        assert!(symbols.iter().any(|s| {
            s.name == "method" && s.kind == "method" && s.namespace.as_deref() == Some("foo::Bar")
        }));
        assert!(symbols.iter().any(|s| s.name == "hidden" && s.kind == "fn"));

        let var_names: Vec<_> = symbols
            .iter()
            .filter(|s| s.kind == "var")
            .map(|s| s.name.as_str())
            .collect();
        assert!(var_names.contains(&"count"));
        assert!(var_names.contains(&"inner"));
        assert!(var_names.contains(&"left"));
        assert!(var_names.contains(&"right"));
        assert!(!var_names.contains(&"_ignored"));

        assert!(symbols.iter().any(|s| {
            s.name == "inner" && s.kind == "field" && s.namespace.as_deref() == Some("foo::Bar")
        }));
    }
}
