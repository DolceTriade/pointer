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
        if let Some(symbol) = extract_symbol(&node, source_bytes, package.as_deref()) {
            symbols.push(symbol);
        }

        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            stack.push(child);
        }
    }

    symbols
}

fn extract_symbol(node: &Node, source: &[u8], package: Option<&str>) -> Option<ExtractedSymbol> {
    match node.kind() {
        "class_declaration" => symbol_from_named(node, source, "class", package),
        "interface_declaration" => symbol_from_named(node, source, "interface", package),
        "enum_declaration" => symbol_from_named(node, source, "enum", package),
        "record_declaration" => symbol_from_named(node, source, "record", package),
        "annotation_type_declaration" => symbol_from_named(node, source, "annotation", package),
        "method_declaration" => symbol_from_named_if_public(node, source, "method", package),
        "constructor_declaration" => {
            symbol_from_named_if_public(node, source, "constructor", package)
        }
        _ => None,
    }
}

fn symbol_from_named(
    node: &Node,
    source: &[u8],
    kind: &str,
    package: Option<&str>,
) -> Option<ExtractedSymbol> {
    let name_node = node.child_by_field_name("name")?;
    let name = name_node.utf8_text(source).ok()?.to_string();
    let namespace = namespace_for_node(node, source, package);

    Some(ExtractedSymbol {
        name,
        kind: kind.to_string(),
        namespace,
    })
}

fn symbol_from_named_if_public(
    node: &Node,
    source: &[u8],
    kind: &str,
    package: Option<&str>,
) -> Option<ExtractedSymbol> {
    let name_node = node.child_by_field_name("name")?;
    if !has_public_modifier(node, &name_node, source) {
        return None;
    }

    let name = name_node.utf8_text(source).ok()?.to_string();
    let namespace = namespace_for_node(node, source, package);

    Some(ExtractedSymbol {
        name,
        kind: kind.to_string(),
        namespace,
    })
}

fn has_public_modifier(node: &Node, name_node: &Node, source: &[u8]) -> bool {
    let start = node.start_byte();
    let end = name_node.start_byte();
    if start >= end || end > source.len() {
        return false;
    }

    std::str::from_utf8(&source[start..end])
        .map(|text| text.contains("public"))
        .unwrap_or(false)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_public_symbols() {
        let source = r#"
            package com.example.demo;

            public class Foo {
                private int value;

                public Foo() {}

                public void doThing() {}

                void hidden() {}

                public static class Nested {}
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
        assert!(symbols
            .iter()
            .any(|s| s.name == "Foo" && s.kind == "constructor"));
        assert!(symbols.iter().any(|s| s.name == "doThing"
            && s.kind == "method"
            && s.namespace.as_deref() == Some("com.example.demo.Foo")));
        assert!(symbols.iter().any(|s| s.name == "Nested"
            && s.kind == "class"
            && s.namespace.as_deref() == Some("com.example.demo.Foo")));
        assert!(symbols.iter().all(|s| s.name != "hidden"));
    }
}
