use tree_sitter::{Node, Parser};

use super::ExtractedSymbol;

pub fn extract(source: &str) -> Vec<ExtractedSymbol> {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_typescript::language_typescript())
        .expect("failed to load tree-sitter JavaScript grammar");

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
        "function_declaration" | "generator_function_declaration" => {
            symbol_from_named(node, source, "function")
        }
        "class_declaration" => symbol_from_named(node, source, "class"),
        "method_definition" => symbols_from_method(node, source),
        "public_field_definition" | "property_definition" => symbols_from_field(node, source),
        "lexical_declaration" => {
            symbols_from_variable_declaration(node, source, lexical_kind(node, source))
        }
        "variable_declaration" => {
            symbols_from_variable_declaration(node, source, "var".to_string())
        }
        "assignment_expression" if is_destructuring_assignment(node) => {
            symbols_from_assignment(node, source)
        }
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

    vec![ExtractedSymbol {
        name,
        kind: kind.to_string(),
        namespace: namespace_for_node(node, source),
    }]
}

fn symbols_from_method(node: &Node, source: &[u8]) -> Vec<ExtractedSymbol> {
    let name_node = node
        .child_by_field_name("name")
        .or_else(|| node.child_by_field_name("property"))
        .or_else(|| node.child_by_field_name("_constructor"))
        .or_else(|| node.child(0))
        .filter(|n| n.is_named());

    let name_node = match name_node {
        Some(node) => node,
        None => return Vec::new(),
    };

    let name = match name_node.utf8_text(source) {
        Ok(text) => text.to_string(),
        Err(_) => return Vec::new(),
    };

    vec![ExtractedSymbol {
        name,
        kind: "method".to_string(),
        namespace: namespace_for_node(node, source),
    }]
}

fn symbols_from_field(node: &Node, source: &[u8]) -> Vec<ExtractedSymbol> {
    let name_node = node
        .child_by_field_name("name")
        .or_else(|| node.child_by_field_name("property"))
        .or_else(|| node.child(0))
        .filter(|n| n.is_named());

    let name_node = match name_node {
        Some(node) => node,
        None => return Vec::new(),
    };

    let name = match name_node.utf8_text(source) {
        Ok(text) => text.to_string(),
        Err(_) => return Vec::new(),
    };

    vec![ExtractedSymbol {
        name,
        kind: "field".to_string(),
        namespace: namespace_for_node(node, source),
    }]
}

fn symbols_from_variable_declaration(
    node: &Node,
    source: &[u8],
    kind: String,
) -> Vec<ExtractedSymbol> {
    let namespace = namespace_for_node(node, source);
    let mut results = Vec::new();
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if matches!(child.kind(), "variable_declarator" | "lexical_declarator") {
            if let Some(name_node) = child.child_by_field_name("name") {
                let mut names = Vec::new();
                collect_pattern_names(&name_node, source, &mut names);
                if names.is_empty() {
                    if let Ok(name) = name_node.utf8_text(source) {
                        names.push(name.to_string());
                    }
                }
                for name in names {
                    results.push(ExtractedSymbol {
                        name,
                        kind: kind.clone(),
                        namespace: namespace.clone(),
                    });
                }
            }
        }
    }
    results
}

fn symbols_from_assignment(node: &Node, source: &[u8]) -> Vec<ExtractedSymbol> {
    let namespace = namespace_for_node(node, source);
    let mut results = Vec::new();
    if let Some(left) = node.child_by_field_name("left") {
        let mut names = Vec::new();
        collect_pattern_names(&left, source, &mut names);
        for name in names {
            results.push(ExtractedSymbol {
                name,
                kind: "var".to_string(),
                namespace: namespace.clone(),
            });
        }
    }
    results
}

fn collect_pattern_names(node: &Node, source: &[u8], out: &mut Vec<String>) {
    match node.kind() {
        "identifier" | "property_identifier" => {
            if let Ok(name) = node.utf8_text(source) {
                out.push(name.to_string());
            }
        }
        "array_pattern" | "object_pattern" | "pair_pattern" | "parenthesized_pattern" => {
            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                collect_pattern_names(&child, source, out);
            }
        }
        _ => {}
    }
}

fn is_destructuring_assignment(node: &Node) -> bool {
    if let Some(left) = node.child_by_field_name("left") {
        matches!(left.kind(), "array_pattern" | "object_pattern")
    } else {
        false
    }
}

fn namespace_for_node(node: &Node, source: &[u8]) -> Option<String> {
    let mut segments = Vec::new();
    let mut current = node.parent();

    while let Some(parent) = current {
        match parent.kind() {
            "class_declaration" | "class_expression" => {
                if let Some(name_node) = parent.child_by_field_name("name") {
                    if let Ok(text) = name_node.utf8_text(source) {
                        segments.push(text.to_string());
                    }
                }
            }
            "function_declaration" | "generator_function_declaration" => {
                if let Some(name_node) = parent.child_by_field_name("name") {
                    if let Ok(text) = name_node.utf8_text(source) {
                        segments.push(text.to_string());
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

fn lexical_kind(node: &Node, source: &[u8]) -> String {
    if let Ok(text) = node.utf8_text(source) {
        let trimmed = text.trim_start();
        if trimmed.starts_with("const") {
            "const".to_string()
        } else if trimmed.starts_with("let") {
            "let".to_string()
        } else {
            "lexical".to_string()
        }
    } else {
        "lexical".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_function_class_and_variable_names() {
        let source = r#"
            export function foo() {}
            function hidden() {}
            class Widget {
                count = 1;
                method() {
                    const local = 1;
                }
            }
            const local = 1;
            const answer = 42, helper = () => {};
            let flag = true;
            var legacy = 0;
            let [a, b] = [1, 2];
            ({ x: alias } = obj);
        "#;

        let mut symbols = extract(source);
        symbols.sort_by(|a, b| a.name.cmp(&b.name));

        assert!(symbols
            .iter()
            .any(|s| s.name == "foo" && s.kind == "function"));
        assert!(symbols
            .iter()
            .any(|s| s.name == "hidden" && s.kind == "function"));
        assert!(symbols
            .iter()
            .any(|s| s.name == "Widget" && s.kind == "class"));
        assert!(symbols.iter().any(|s| {
            s.name == "method" && s.kind == "method" && s.namespace.as_deref() == Some("Widget")
        }));
        assert!(symbols.iter().any(|s| {
            s.name == "count" && s.kind == "field" && s.namespace.as_deref() == Some("Widget")
        }));

        let vars: Vec<_> = symbols
            .iter()
            .filter(|s| matches!(s.kind.as_str(), "var" | "let" | "const"))
            .map(|s| s.name.as_str())
            .collect();

        assert!(vars.contains(&"local"));
        assert!(vars.contains(&"answer"));
        assert!(vars.contains(&"helper"));
        assert!(vars.contains(&"flag"));
        assert!(vars.contains(&"legacy"));
        assert!(vars.contains(&"a"));
        assert!(vars.contains(&"b"));
        assert!(vars.contains(&"alias"));
    }
}
