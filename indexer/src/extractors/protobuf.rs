use tree_sitter::{Node, Parser};

use super::ExtractedSymbol;

pub fn extract(source: &str) -> Vec<ExtractedSymbol> {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_proto::LANGUAGE.into())
        .expect("failed to load tree-sitter Protobuf grammar");

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
        "message" => symbol_from_named(node, source, "message", package),
        "enum" => symbol_from_named(node, source, "enum", package),
        "service" => symbol_from_named(node, source, "service", package),
        "rpc" => symbol_from_named(node, source, "rpc", package),
        _ => None,
    }
}

fn symbol_from_named(
    node: &Node,
    source: &[u8],
    kind: &str,
    package: Option<&str>,
) -> Option<ExtractedSymbol> {
    let name = node_name(node, source)?;
    let namespace = namespace_for_node(node, source, package);

    Some(ExtractedSymbol {
        name,
        kind: kind.to_string(),
        namespace,
    })
}

fn package_name(root: &Node, source: &[u8]) -> Option<String> {
    let mut cursor = root.walk();
    for child in root.children(&mut cursor) {
        if child.kind() == "package" {
            if let Some(name_node) = child
                .child_by_field_name("name")
                .or_else(|| child.named_child(0))
            {
                if let Ok(text) = name_node.utf8_text(source) {
                    return Some(text.to_string());
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
            "message" | "service" => {
                if let Some(text) = node_name(&parent, source) {
                    names.push(text);
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

fn node_name(node: &Node, source: &[u8]) -> Option<String> {
    for field in [
        "name",
        "message_name",
        "enum_name",
        "service_name",
        "rpc_name",
    ] {
        if let Some(child) = node.child_by_field_name(field) {
            return child.utf8_text(source).ok().map(|text| text.to_string());
        }
    }

    node.named_child(0)
        .and_then(|child| child.utf8_text(source).ok().map(|text| text.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_proto_symbols() {
        let source = r#"
            syntax = "proto3";

            package demo.api;

            message Foo {
                message Nested {}
            }

            enum Status {
                STATUS_UNKNOWN = 0;
            }

            service Demo {
                rpc Run (Foo) returns (Foo);
            }
        "#;

        let mut symbols = extract(source);
        symbols.sort_by(|a, b| a.name.cmp(&b.name));

        assert!(symbols.iter().any(|s| s.name == "Foo"
            && s.kind == "message"
            && s.namespace.as_deref() == Some("demo.api")));
        assert!(
            symbols
                .iter()
                .any(|s| s.name == "Nested" && s.namespace.as_deref() == Some("demo.api.Foo"))
        );
        assert!(
            symbols
                .iter()
                .any(|s| s.name == "Status" && s.kind == "enum")
        );
        assert!(
            symbols
                .iter()
                .any(|s| s.name == "Demo" && s.kind == "service")
        );
        assert!(symbols.iter().any(|s| s.name == "Run"
            && s.kind == "rpc"
            && s.namespace.as_deref() == Some("demo.api.Demo")));
    }
}
