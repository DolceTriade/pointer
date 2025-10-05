use tree_sitter::{Node, Parser};

use super::ExtractedSymbol;

pub fn extract(source: &str) -> Vec<ExtractedSymbol> {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_java::language())
        .expect("failed to load tree-sitter Java grammar");

    let tree = match parser.parse(source, None) {
        Some(tree) => tree,
        None => return Vec::new(),
    };

    let root = tree.root_node();
    let source_bytes = source.as_bytes();
    let package = package_name(&root, source_bytes);
    let mut stack = vec![root];
    let mut symbols = Vec::new();

    while let Some(node) = stack.pop() {
        let mut extracted = collect_symbols(&node, source_bytes, package.as_deref());
        symbols.append(&mut extracted);

        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            stack.push(child);
        }
    }

    symbols
}

fn collect_symbols(node: &Node, source: &[u8], package: Option<&str>) -> Vec<ExtractedSymbol> {
    match node.kind() {
        "class_declaration" => symbol_from_named(node, source, "class", package),
        "interface_declaration" => symbol_from_named(node, source, "interface", package),
        "enum_declaration" => symbol_from_named(node, source, "enum", package),
        "record_declaration" => symbol_from_named(node, source, "record", package),
        "annotation_type_declaration" => symbol_from_named(node, source, "annotation", package),
        "method_declaration" => symbol_from_named(node, source, "method", package),
        "constructor_declaration" => symbol_from_named(node, source, "constructor", package),
        "field_declaration" | "local_variable_declaration" => {
            symbols_from_variable_declaration(node, source, package)
        }
        _ => Vec::new(),
    }
}

fn symbol_from_named(
    node: &Node,
    source: &[u8],
    kind: &str,
    package: Option<&str>,
) -> Vec<ExtractedSymbol> {
    let name_node = match node.child_by_field_name("name") {
        Some(name) => name,
        None => return Vec::new(),
    };
    let name = match name_node.utf8_text(source) {
        Ok(text) => text.to_string(),
        Err(_) => return Vec::new(),
    };
    let namespace = namespace_for_node(node, source, package);

    vec![ExtractedSymbol {
        name,
        kind: kind.to_string(),
        namespace,
    }]
}

fn symbols_from_variable_declaration(
    node: &Node,
    source: &[u8],
    package: Option<&str>,
) -> Vec<ExtractedSymbol> {
    let namespace = variable_namespace(node, source, package);
    let mut vars = Vec::new();
    let mut cursor = node.walk();

    for child in node.children(&mut cursor) {
        if child.kind() == "variable_declarator" {
            if let Some(name_node) = child.child_by_field_name("name") {
                if let Ok(text) = name_node.utf8_text(source) {
                    vars.push(ExtractedSymbol {
                        name: text.to_string(),
                        kind: "var".to_string(),
                        namespace: namespace.clone(),
                    });
                }
            }
        }
    }

    vars
}

fn package_name(root: &Node, source: &[u8]) -> Option<String> {
    let mut cursor = root.walk();
    for child in root.children(&mut cursor) {
        if child.kind() == "package_declaration" {
            if let Some(name) = child
                .child_by_field_name("name")
                .or_else(|| child.named_child(0))
            {
                if let Ok(text) = name.utf8_text(source) {
                    return Some(text.trim().to_string());
                }
            }
        }
    }
    None
}

fn namespace_for_node(node: &Node, source: &[u8], package: Option<&str>) -> Option<String> {
    let mut names = Vec::new();
    let mut current = node.parent();

    while let Some(parent) = current {
        match parent.kind() {
            "class_declaration"
            | "interface_declaration"
            | "enum_declaration"
            | "record_declaration"
            | "annotation_type_declaration" => {
                if let Some(name_node) = parent.child_by_field_name("name") {
                    if let Ok(text) = name_node.utf8_text(source) {
                        names.push(text.to_string());
                    }
                }
            }
            _ => {}
        }
        current = parent.parent();
    }

    if names.is_empty() {
        package.map(|pkg| pkg.to_string())
    } else {
        names.reverse();
        if let Some(pkg) = package {
            let mut full = Vec::with_capacity(names.len() + 1);
            full.push(pkg.to_string());
            full.extend(names);
            Some(full.join("."))
        } else {
            Some(names.join("."))
        }
    }
}

fn variable_namespace(node: &Node, source: &[u8], package: Option<&str>) -> Option<String> {
    let mut scopes = Vec::new();
    let mut current = node.parent();

    while let Some(parent) = current {
        match parent.kind() {
            "method_declaration" | "constructor_declaration" => {
                if let Some(name_node) = parent.child_by_field_name("name") {
                    if let Ok(text) = name_node.utf8_text(source) {
                        scopes.push(text.to_string());
                    }
                }
            }
            "class_declaration"
            | "interface_declaration"
            | "enum_declaration"
            | "record_declaration"
            | "annotation_type_declaration" => {
                if let Some(name_node) = parent.child_by_field_name("name") {
                    if let Ok(text) = name_node.utf8_text(source) {
                        scopes.push(text.to_string());
                    }
                }
            }
            _ => {}
        }
        current = parent.parent();
    }

    let mut segments = Vec::new();
    if let Some(pkg) = package {
        segments.push(pkg.to_string());
    }

    if scopes.is_empty() {
        return if segments.is_empty() {
            None
        } else {
            Some(segments.join("."))
        };
    }

    scopes.reverse();
    segments.extend(scopes);

    Some(segments.join("."))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_public_symbols_and_variables() {
        let source = r#"
            package com.example.demo;

            public class Foo {
                private int value;

                public Foo() {
                    int created = 1;
                }

                public void doThing() {
                    int counter = 0;
                }

                void hidden() {}

                public static class Nested {
                    int nestedField = 3;
                }
            }

            interface Bar {}
        "#;

        let mut symbols = extract(source);
        symbols.sort_by(|a, b| a.name.cmp(&b.name));

        assert!(symbols.iter().any(|s| s.name == "Foo"
            && s.kind == "class"
            && s.namespace.as_deref() == Some("com.example.demo")));
        assert!(symbols.iter().any(|s| s.name == "Bar"
            && s.kind == "interface"
            && s.namespace.as_deref() == Some("com.example.demo")));
        assert!(symbols.iter().any(|s| s.name == "Foo"
            && s.kind == "constructor"
            && s.namespace.as_deref() == Some("com.example.demo.Foo")));
        assert!(symbols.iter().any(|s| s.name == "doThing"
            && s.kind == "method"
            && s.namespace.as_deref() == Some("com.example.demo.Foo")));
        assert!(symbols.iter().any(|s| s.name == "Nested"
            && s.kind == "class"
            && s.namespace.as_deref() == Some("com.example.demo.Foo")));
        assert!(symbols.iter().any(|s| {
            s.name == "hidden"
                && s.kind == "method"
                && s.namespace.as_deref() == Some("com.example.demo.Foo")
        }));

        let vars: Vec<_> = symbols
            .iter()
            .filter(|s| s.kind == "var")
            .map(|s| (s.name.as_str(), s.namespace.as_deref()))
            .collect();

        assert!(vars.contains(&("value", Some("com.example.demo.Foo"))));
        assert!(vars.contains(&("counter", Some("com.example.demo.Foo.doThing"))));
        assert!(vars.contains(&("created", Some("com.example.demo.Foo.Foo"))));
        assert!(vars.contains(&("nestedField", Some("com.example.demo.Foo.Nested"))));
    }
}
