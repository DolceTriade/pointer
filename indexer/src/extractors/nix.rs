use tree_sitter::{Node, Parser};

use super::ExtractedSymbol;

pub fn extract(source: &str) -> Vec<ExtractedSymbol> {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_nix::LANGUAGE.into())
        .expect("failed to load tree-sitter Nix grammar");

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
    if node.kind() != "binding" {
        return None;
    }

    let attr = node.child_by_field_name("attrpath")?;
    let mut segments = attrpath_segments(&attr, source)?;
    if segments.is_empty() {
        return None;
    }

    let name = segments.pop()?;
    let mut namespace_segments = ancestor_namespaces(node, source);
    namespace_segments.extend(segments);

    let namespace = if namespace_segments.is_empty() {
        None
    } else {
        Some(namespace_segments.join("."))
    };

    Some(ExtractedSymbol {
        name,
        kind: "attr".to_string(),
        namespace,
    })
}

fn attrpath_segments(node: &Node, source: &[u8]) -> Option<Vec<String>> {
    let text = node.utf8_text(source).ok()?.replace(' ', "");
    if text.is_empty() {
        return None;
    }

    let parts = text
        .split('.')
        .filter(|segment| !segment.is_empty())
        .map(|segment| segment.trim_matches('"').to_string())
        .collect::<Vec<_>>();

    if parts.is_empty() { None } else { Some(parts) }
}

fn ancestor_namespaces(node: &Node, source: &[u8]) -> Vec<String> {
    let mut segments = Vec::new();
    let mut current = node.parent();

    while let Some(parent) = current {
        if parent.kind() == "binding" {
            if let Some(path) = parent.child_by_field_name("attrpath") {
                if let Some(parts) = attrpath_segments(&path, source) {
                    segments.push(parts);
                }
            }
        }
        current = parent.parent();
    }

    segments.reverse();

    let mut flattened = Vec::new();
    for group in segments {
        for part in group {
            flattened.push(part);
        }
    }

    flattened
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_attr_bindings() {
        let source = r#"
            {
              foo.bar = 1;
              foo."baz".qux = 2;
              nested = {
                inner = 3;
              };
            }
        "#;

        let mut symbols = extract(source);
        symbols.sort_by(|a, b| a.name.cmp(&b.name));

        assert!(
            symbols.iter().any(|s| s.name == "bar"
                && s.kind == "attr"
                && s.namespace.as_deref() == Some("foo"))
        );
        assert!(
            symbols
                .iter()
                .any(|s| s.name == "qux" && s.namespace.as_deref() == Some("foo.baz"))
        );
        assert!(
            symbols
                .iter()
                .any(|s| s.name == "nested" && s.namespace.is_none())
        );
        assert!(
            symbols
                .iter()
                .any(|s| s.name == "inner" && s.namespace.as_deref() == Some("nested"))
        );
    }
}
