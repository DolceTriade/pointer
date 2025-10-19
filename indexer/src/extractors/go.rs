use tree_sitter::{Node, Parser};

use super::{ExtractedReference, ExtractedSymbol, Extraction};

pub fn extract(source: &str) -> Extraction {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_go::LANGUAGE.into())
        .expect("failed to load tree-sitter Go grammar");

    let tree = match parser.parse(source, None) {
        Some(tree) => tree,
        None => return Extraction::default(),
    };

    let root = tree.root_node();
    let source_bytes = source.as_bytes();
    let package = package_name(&root, source_bytes);
    let mut stack = vec![root];
    let mut symbols = Vec::new();
    let mut references = Vec::new();

    while let Some(node) = stack.pop() {
        let mut extracted = collect_symbols(&node, source_bytes, package.as_deref());
        symbols.append(&mut extracted);

        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            stack.push(child);
        }
    }

    // Collect all identifiers as references (including those that are also symbols)
    // This allows for proper symbol-to-reference mapping where the same identifier can be both
    let mut stack = vec![root];
    while let Some(node) = stack.pop() {
        if node.kind() == "identifier" {
            if let Ok(text) = node.utf8_text(source_bytes) {
                let name = text.trim();
                if !name.is_empty() {
                    let pos = node.start_position();
                    references.push(ExtractedReference {
                        name: name.to_string(),
                        kind: Some("reference".to_string()),
                        namespace: None,
                        line: pos.row.saturating_add(1) as usize,
                        column: pos.column.saturating_add(1) as usize,
                    });
                }
            }
        }

        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            stack.push(child);
        }
    }

    Extraction {
        symbols,
        references,
    }
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
    let mut names = Vec::new();
    if let Some(left) = node.child_by_field_name("left") {
        collect_go_binding_names(&left, source, &mut names);
    }

    if names.is_empty() {
        return Vec::new();
    }

    let value_nodes = node
        .child_by_field_name("right")
        .map(flatten_go_expression_list)
        .unwrap_or_default();

    names
        .into_iter()
        .enumerate()
        .map(|(idx, name)| {
            let is_function_like = value_nodes
                .get(idx)
                .map(|expr| go_expression_is_function(expr))
                .unwrap_or(false);
            ExtractedSymbol {
                name,
                kind: if is_function_like { "fn" } else { "var" }.to_string(),
                namespace: namespace.clone(),
            }
        })
        .collect()
}

fn symbols_from_spec(
    node: &Node,
    source: &[u8],
    package: Option<&str>,
    kind: &str,
) -> Vec<ExtractedSymbol> {
    let namespace = go_namespace(node, source, package);
    let mut names = Vec::new();
    for i in 0..node.child_count() {
        if let Some(child) = node.child(i) {
            if node.field_name_for_child(i as u32) == Some("name") {
                collect_go_binding_names(&child, source, &mut names);
            }
        }
    }

    if names.is_empty() {
        return Vec::new();
    }

    let type_is_function = node
        .child_by_field_name("type")
        .map(|ty| go_type_is_function(&ty))
        .unwrap_or(false);

    let mut value_exprs = Vec::new();
    for i in 0..node.child_count() {
        if let Some(child) = node.child(i) {
            if node.field_name_for_child(i as u32) == Some("value") {
                value_exprs.extend(flatten_go_expression_list(child));
            }
        }
    }

    names
        .into_iter()
        .enumerate()
        .map(|(idx, name)| {
            let is_function_like = if type_is_function {
                true
            } else {
                value_exprs
                    .get(idx)
                    .map(|expr| go_expression_is_function(expr))
                    .unwrap_or(false)
            };
            ExtractedSymbol {
                name,
                kind: if is_function_like { "fn" } else { kind }.to_string(),
                namespace: namespace.clone(),
            }
        })
        .collect()
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

fn collect_go_binding_names(node: &Node, source: &[u8], out: &mut Vec<String>) {
    match node.kind() {
        "identifier" => {
            if let Ok(name) = node.utf8_text(source) {
                out.push(name.to_string());
            }
        }
        "identifier_list" | "expression_list" | "parenthesized_expression" => {
            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                if child.is_named() {
                    collect_go_binding_names(&child, source, out);
                }
            }
        }
        _ => {
            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                if child.is_named() {
                    collect_go_binding_names(&child, source, out);
                }
            }
        }
    }
}

fn flatten_go_expression_list(node: Node) -> Vec<Node> {
    if node.kind() == "expression_list" {
        let mut results = Vec::new();
        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            if child.is_named() {
                results.push(child);
            }
        }
        results
    } else {
        vec![node]
    }
}

fn go_expression_is_function(node: &Node) -> bool {
    if node.kind() == "func_literal" {
        return true;
    }

    if node.kind() == "parenthesized_expression" {
        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            if child.is_named() && go_expression_is_function(&child) {
                return true;
            }
        }
    }

    false
}

fn go_type_is_function(node: &Node) -> bool {
    if node.kind() == "function_type" {
        return true;
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if go_type_is_function(&child) {
            return true;
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_go_symbols_and_variables() {
        let source = r#"
            package demo

            var top = 1
            var handler func(int) int
            var typed1, typed2 func() int
            var withLiteral = func() int { return 0 }
            var mix1, mix2 = func() {}, 42

            type Foo struct {
                Value int
            }
            type Bar interface {}

            func helper() {
                local := 3
                localFn := func() int { return local }
            }

            func (f *Foo) Method() {
                var counter int
            }
        "#;

        let extraction = extract(source);
        let mut symbols = extraction.symbols;
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
        assert!(vars.contains(&("mix2", Some("demo"))));
        assert!(!vars.iter().any(|(name, _)| *name == "handler"));
        assert!(!vars.iter().any(|(name, _)| *name == "typed1"));
        assert!(!vars.iter().any(|(name, _)| *name == "typed2"));
        assert!(!vars.iter().any(|(name, _)| *name == "withLiteral"));
        assert!(!vars.iter().any(|(name, _)| *name == "mix1"));
        assert!(!vars.iter().any(|(name, _)| *name == "localFn"));

        let fn_symbols: Vec<_> = symbols
            .iter()
            .filter(|s| s.kind == "fn")
            .map(|s| (s.name.as_str(), s.namespace.as_deref()))
            .collect();

        assert!(fn_symbols.contains(&("helper", Some("demo"))));
        assert!(fn_symbols.contains(&("handler", Some("demo"))));
        assert!(fn_symbols.contains(&("typed1", Some("demo"))));
        assert!(fn_symbols.contains(&("typed2", Some("demo"))));
        assert!(fn_symbols.contains(&("withLiteral", Some("demo"))));
        assert!(fn_symbols.contains(&("mix1", Some("demo"))));
        assert!(fn_symbols.contains(&("localFn", Some("demo.helper"))));
    }
}
