use tree_sitter::{Node, Parser};

use super::ExtractedSymbol;

pub fn extract(source: &str) -> Vec<ExtractedSymbol> {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_cpp::language())
        .expect("failed to load tree-sitter C++ grammar");

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
        "function_definition" => {
            let declarator = node.child_by_field_name("declarator")?;
            let name = identifier_from_declarator(&declarator, source)?;
            let namespace = namespace_for_node(node, source)
                .or_else(|| namespace_from_qualified(&declarator, source));
            Some(ExtractedSymbol {
                name,
                kind: "fn".to_string(),
                namespace,
            })
        }
        "class_specifier" => symbol_from_named(node, source, "class"),
        "struct_specifier" => symbol_from_named(node, source, "struct"),
        "enum_specifier" => symbol_from_named(node, source, "enum"),
        "namespace_definition" => symbol_from_named(node, source, "namespace"),
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

fn identifier_from_declarator(node: &Node, source: &[u8]) -> Option<String> {
    if matches!(node.kind(), "identifier" | "field_identifier") {
        return node.utf8_text(source).ok().map(|text| text.to_string());
    }

    let mut last = None;
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if let Some(name) = identifier_from_declarator(&child, source) {
            last = Some(name);
        }
    }

    last
}

fn namespace_for_node(node: &Node, source: &[u8]) -> Option<String> {
    let mut names = Vec::new();
    let mut current = node.parent();

    while let Some(parent) = current {
        match parent.kind() {
            "namespace_definition" | "class_specifier" | "struct_specifier" => {
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
        Some(names.join("::"))
    }
}

fn namespace_from_qualified(node: &Node, source: &[u8]) -> Option<String> {
    if node.kind() == "qualified_identifier" {
        if let Ok(text) = node.utf8_text(source) {
            if let Some(idx) = text.rfind("::") {
                let prefix = &text[..idx];
                if !prefix.is_empty() {
                    return Some(prefix.to_string());
                }
            }
        }
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if let Some(result) = namespace_from_qualified(&child, source) {
            return Some(result);
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_cpp_symbols() {
        let source = r#"
            namespace foo {
                class Bar {
                public:
                    int value;
                    int method() { return value; }
                };

                int run();
            }

            int foo::run() {
                return 0;
            }
        "#;

        let mut symbols = extract(source);
        symbols.sort_by(|a, b| a.name.cmp(&b.name));

        assert!(symbols.iter().any(|s| s.name == "Bar"
            && s.kind == "class"
            && s.namespace.as_deref() == Some("foo")));
        assert!(symbols
            .iter()
            .any(|s| s.name == "foo" && s.kind == "namespace"));
        assert!(symbols.iter().any(|s| s.name == "method"
            && s.kind == "fn"
            && s.namespace.as_deref() == Some("foo::Bar")));
        assert!(symbols
            .iter()
            .any(|s| s.name == "run" && s.kind == "fn" && s.namespace.as_deref() == Some("foo")));
    }
}
