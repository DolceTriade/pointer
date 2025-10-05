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
        "function_declaration" => symbol_from_named(node, source, "fn", package),
        "method_declaration" => symbols_from_method(node, source, package),
        "type_spec" => symbol_from_named(node, source, "type", package),
        "short_var_declaration" => symbols_from_short_var(node, source, package),
        "var_spec" => symbols_from_spec(node, source, package, "var"),
        "const_spec" => symbols_from_spec(node, source, package, "const"),
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

    vec![ExtractedSymbol {
        name,
        kind: kind.to_string(),
        namespace: package.map(|pkg| pkg.to_string()),
    }]
}

fn symbols_from_method(node: &Node, source: &[u8], package: Option<&str>) -> Vec<ExtractedSymbol> {
    let mut symbol = symbol_from_named(node, source, "method", package);
    if let Some(receiver) = node.child_by_field_name("receiver") {
        if let Some(receiver_type) = receiver_type(&receiver, source) {
            if let Some(method) = symbol.first_mut() {
                method.namespace = merge_namespaces(package, Some(&receiver_type));
            }
        }
    }
    symbol
}

fn symbols_from_short_var(
    node: &Node,
    source: &[u8],
    package: Option<&str>,
) -> Vec<ExtractedSymbol> {
    let namespace = go_namespace(node, source, package);
    let mut vars = Vec::new();
    if let Some(left) = node.child_by_field_name("left") {
        let mut cursor = left.walk();
        for child in left.children(&mut cursor) {
            if child.kind() == "identifier" {
                if let Ok(name) = child.utf8_text(source) {
                    vars.push(ExtractedSymbol {
                        name: name.to_string(),
                        kind: "var".to_string(),
                        namespace: namespace.clone(),
                    });
                }
            }
        }
    }
    vars
}

fn symbols_from_spec(
    node: &Node,
    source: &[u8],
    package: Option<&str>,
    kind: &str,
) -> Vec<ExtractedSymbol> {
    let namespace = go_namespace(node, source, package);
    let mut vars = Vec::new();
    if let Some(names_node) = node.child_by_field_name("name") {
        match names_node.kind() {
            "identifier_list" => {
                let mut list_cursor = names_node.walk();
                for ident in names_node.children(&mut list_cursor) {
                    if ident.kind() == "identifier" {
                        if let Ok(name) = ident.utf8_text(source) {
                            vars.push(ExtractedSymbol {
                                name: name.to_string(),
                                kind: kind.to_string(),
                                namespace: namespace.clone(),
                            });
                        }
                    }
                }
            }
            "identifier" => {
                if let Ok(name) = names_node.utf8_text(source) {
                    vars.push(ExtractedSymbol {
                        name: name.to_string(),
                        kind: kind.to_string(),
                        namespace: namespace.clone(),
                    });
                }
            }
            _ => {}
        }
    }
    vars
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

fn go_namespace(node: &Node, source: &[u8], package: Option<&str>) -> Option<String> {
    let mut scopes = Vec::new();
    let mut current = node.parent();
    while let Some(parent) = current {
        match parent.kind() {
            "function_declaration" | "method_declaration" => {
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

    scopes.reverse();

    match (package, scopes.is_empty()) {
        (Some(pkg), true) => Some(pkg.to_string()),
        (Some(pkg), false) => Some(format!("{}.{}", pkg, scopes.join("."))),
        (None, true) => None,
        (None, false) => Some(scopes.join(".")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_go_symbols_and_variables() {
        let source = r#"
            package demo

            var top = 1

            type Foo struct {
                Value int
            }
            type Bar interface {}

            func helper() {
                local := 3
            }

            func (f *Foo) Method() {
                var counter int
            }
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

        let vars: Vec<_> = symbols
            .iter()
            .filter(|s| s.kind == "var" || s.kind == "const")
            .map(|s| (s.name.as_str(), s.namespace.as_deref()))
            .collect();

        assert!(vars.contains(&("top", Some("demo"))));
        assert!(vars.contains(&("local", Some("demo.helper"))));
        assert!(vars.contains(&("counter", Some("demo.Method"))));
    }
}
