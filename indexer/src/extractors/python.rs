use tree_sitter::{Node, Parser};

use super::ExtractedSymbol;

pub fn extract(source: &str) -> Vec<ExtractedSymbol> {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_python::language())
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
        if let Some(symbol) = extract_symbol(&node, source_bytes) {
            symbols.push(symbol);
        }

        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            stack.push(child);
        }
    }

    symbols
}

fn extract_symbol(node: &Node, source: &[u8]) -> Option<ExtractedSymbol> {
    match node.kind() {
        "class_definition" => symbol_from_named(node, source, "class"),
        "function_definition" => {
            let kind = function_kind(node, source);
            symbol_from_named(node, source, kind)
        }
        "async_function_definition" => symbol_from_named(node, source, "async_fn"),
        _ => None,
    }
}

fn symbol_from_named(node: &Node, source: &[u8], kind: &str) -> Option<ExtractedSymbol> {
    let name_node = node.child_by_field_name("name")?;
    let name = name_node.utf8_text(source).ok()?.to_string();
    let namespace = namespace_for_node(node, source);

    Some(ExtractedSymbol {
        name,
        kind: kind.to_string(),
        namespace,
    })
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

fn function_kind(node: &Node, source: &[u8]) -> &'static str {
    if let Some(name_node) = node.child_by_field_name("name") {
        if is_async(node, &name_node, source) {
            return "async_fn";
        }
    }
    "fn"
}

fn is_async(node: &Node, name_node: &Node, source: &[u8]) -> bool {
    let start = node.start_byte();
    let end = name_node.start_byte();
    if start >= end || end > source.len() {
        return false;
    }

    std::str::from_utf8(&source[start..end])
        .map(|text| text.contains("async"))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_python_symbols() {
        let source = r#"
            class Outer:
                class Inner:
                    async def nested_async(self):
                        pass

                def method(self):
                    def helper():
                        return 42
                    return helper()

            def top_level():
                return None
        "#;

        let mut symbols = extract(source);
        symbols.sort_by(|a, b| a.name.cmp(&b.name));

        assert!(symbols
            .iter()
            .any(|s| s.name == "Outer" && s.kind == "class"));
        assert!(symbols
            .iter()
            .any(|s| s.name == "Inner" && s.namespace.as_deref() == Some("Outer")));
        assert!(symbols.iter().any(|s| s.name == "nested_async"
            && s.kind == "async_fn"
            && s.namespace.as_deref() == Some("Outer.Inner")));
        assert!(symbols.iter().any(|s| s.name == "method"
            && s.kind == "fn"
            && s.namespace.as_deref() == Some("Outer")));
        assert!(symbols.iter().any(|s| s.name == "helper"
            && s.kind == "fn"
            && s.namespace.as_deref() == Some("Outer.method")));
        assert!(symbols
            .iter()
            .any(|s| s.name == "top_level" && s.kind == "fn" && s.namespace.is_none()));
    }
}
