use tree_sitter::{Node, Parser};

use super::ExtractedSymbol;

pub fn extract(source: &str) -> Vec<ExtractedSymbol> {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_python::LANGUAGE.into())
        .expect("failed to load tree-sitter Python grammar");

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
        "class_definition" => symbol_from_named(node, source, "class"),
        "function_definition" | "async_function_definition" => {
            let kind = function_kind(node, source);
            symbol_from_named(node, source, kind)
        }
        "assignment" => symbols_from_assignment(node, source),
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

fn symbols_from_assignment(node: &Node, source: &[u8]) -> Vec<ExtractedSymbol> {
    let namespace = namespace_for_node(node, source);
    let mut names = Vec::new();

    if let Some(left) = node.child_by_field_name("left") {
        collect_target_names(&left, source, &mut names);
    }

    let value_is_function = node
        .child_by_field_name("right")
        .map(|right| python_expression_is_function(&right))
        .unwrap_or(false);
    let classify_as_function = value_is_function && names.len() == 1;

    names
        .into_iter()
        .map(|name| ExtractedSymbol {
            name,
            kind: if classify_as_function { "fn" } else { "var" }.to_string(),
            namespace: namespace.clone(),
        })
        .collect()
}

fn collect_target_names(node: &Node, source: &[u8], out: &mut Vec<String>) {
    match node.kind() {
        "identifier" => {
            if let Ok(name) = node.utf8_text(source) {
                out.push(name.to_string());
            }
        }
        "tuple" | "list" | "pattern_list" | "parenthesized_expression" => {
            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                collect_target_names(&child, source, out);
            }
        }
        _ => {}
    }
}

fn python_expression_is_function(node: &Node) -> bool {
    match node.kind() {
        "lambda" => true,
        "parenthesized_expression" => {
            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                if child.is_named() && python_expression_is_function(&child) {
                    return true;
                }
            }
            false
        }
        _ => false,
    }
}

fn namespace_for_node(node: &Node, source: &[u8]) -> Option<String> {
    let mut names = Vec::new();
    let mut current = node.parent();

    while let Some(parent) = current {
        match parent.kind() {
            "class_definition" | "function_definition" | "async_function_definition" => {
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
        None
    } else {
        names.reverse();
        Some(names.join("."))
    }
}

fn function_kind<'a>(node: &'a Node, source: &[u8]) -> &'static str {
    if let Some(name_node) = node.child_by_field_name("name") {
        let start = node.start_byte();
        let end = name_node.start_byte();
        if start < end && end <= source.len() {
            if let Ok(text) = std::str::from_utf8(&source[start..end]) {
                if text.contains("async") {
                    return "async_fn";
                }
            }
        }
    }
    "fn"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_python_symbols_and_variables() {
        let source = r#"
            class Outer:
                class Inner:
                    async def nested_async(self):
                        pass

                def method(self):
                    def helper():
                        value = 42
                        return value
                    count = helper()
                    return count

            def top_level():
                answer = None
                return answer

            env = "prod"
            compute = lambda value: value
        "#;

        let mut symbols = extract(source);
        symbols.sort_by(|a, b| a.name.cmp(&b.name));

        assert!(
            symbols
                .iter()
                .any(|s| s.name == "Outer" && s.kind == "class")
        );
        assert!(
            symbols
                .iter()
                .any(|s| s.name == "Inner" && s.namespace.as_deref() == Some("Outer"))
        );
        assert!(symbols.iter().any(|s| s.name == "nested_async"
            && s.kind == "async_fn"
            && s.namespace.as_deref() == Some("Outer.Inner")));
        assert!(symbols.iter().any(|s| s.name == "method"
            && s.kind == "fn"
            && s.namespace.as_deref() == Some("Outer")));
        assert!(symbols.iter().any(|s| s.name == "helper"
            && s.kind == "fn"
            && s.namespace.as_deref() == Some("Outer.method")));
        assert!(
            symbols
                .iter()
                .any(|s| s.name == "top_level" && s.kind == "fn" && s.namespace.is_none())
        );

        let vars: Vec<_> = symbols
            .iter()
            .filter(|s| s.kind == "var")
            .map(|s| (s.name.as_str(), s.namespace.as_deref()))
            .collect();

        assert!(vars.contains(&("value", Some("Outer.method.helper"))));
        assert!(vars.contains(&("count", Some("Outer.method"))));
        assert!(vars.contains(&("answer", Some("top_level"))));
        assert!(vars.contains(&("env", None)));
        assert!(!vars.iter().any(|(name, _)| *name == "compute"));

        let fn_symbols: Vec<_> = symbols
            .iter()
            .filter(|s| s.kind == "fn")
            .map(|s| (s.name.as_str(), s.namespace.as_deref()))
            .collect();
        assert!(fn_symbols.contains(&("compute", None)));
    }
}
