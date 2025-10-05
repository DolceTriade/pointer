use tree_sitter::{Node, Parser};

use super::ExtractedSymbol;

pub fn extract(source: &str) -> Vec<ExtractedSymbol> {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_go::language())
        .expect("failed to load tree-sitter Go grammar");

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
        "function_declaration" => symbol_from_named(node, source, "fn", package),
        "method_declaration" => {
            let mut symbol = symbol_from_named(node, source, "method", package)?;
            if let Some(receiver) = node.child_by_field_name("receiver") {
                if let Some(receiver_type) = receiver_type(&receiver, source) {
                    symbol.namespace = merge_namespaces(package, Some(&receiver_type));
                }
            }
            Some(symbol)
        }
        "type_spec" => symbol_from_named(node, source, "type", package),
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

    Some(ExtractedSymbol {
        name,
        kind: kind.to_string(),
        namespace: package.map(|pkg| pkg.to_string()),
    })
}

fn receiver_type(node: &Node, source: &[u8]) -> Option<String> {
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.kind() == "parameter_declaration" {
            if let Some(ty) = child.child_by_field_name("type") {
                if let Ok(text) = ty.utf8_text(source) {
                    return Some(text.trim().to_string());
                }
            }
        }
    }
    None
}

fn merge_namespaces(package: Option<&str>, receiver: Option<&str>) -> Option<String> {
    match (package, receiver) {
        (Some(pkg), Some(rcv)) => Some(format!("{pkg}.{rcv}")),
        (Some(pkg), None) => Some(pkg.to_string()),
        (None, Some(rcv)) => Some(rcv.to_string()),
        (None, None) => None,
    }
}

fn package_name(root: &Node, source: &[u8]) -> Option<String> {
    let mut cursor = root.walk();
    for child in root.children(&mut cursor) {
        if child.kind() == "package_clause" {
            if let Some(name) = child
                .child_by_field_name("name")
                .or_else(|| child.named_child(0))
            {
                if let Ok(text) = name.utf8_text(source) {
                    return Some(text.to_string());
                }
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_go_symbols() {
        let source = r#"
            package demo

            type Foo struct {}
            type Bar interface {}

            func helper() {}

            func (f *Foo) Method() {}
        "#;

        let mut symbols = extract(source);
        symbols.sort_by(|a, b| a.name.cmp(&b.name));

        assert!(symbols.iter().any(|s| s.name == "Foo"
            && s.kind == "type"
            && s.namespace.as_deref() == Some("demo")));
        assert!(symbols.iter().any(|s| s.name == "Bar" && s.kind == "type"));
        assert!(symbols.iter().any(|s| s.name == "helper"
            && s.kind == "fn"
            && s.namespace.as_deref() == Some("demo")));
        assert!(symbols.iter().any(|s| s.name == "Method"
            && s.kind == "method"
            && s.namespace.as_deref() == Some("demo.*Foo")));
    }
}
