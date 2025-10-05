use tree_sitter::{Node, Parser};

use super::ExtractedSymbol;

pub fn extract(source: &str) -> Vec<ExtractedSymbol> {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_rust::language())
        .expect("failed to load tree-sitter Rust grammar");

    let tree = match parser.parse(source, None) {
        Some(tree) => tree,
        None => return Vec::new(),
    };

    let root = tree.root_node();
    let mut stack = vec![root];
    let mut symbols = Vec::new();
    let source_bytes = source.as_bytes();

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
    if is_in_impl(node) {
        return None;
    }

    let kind = match node.kind() {
        "function_item" => "fn",
        "struct_item" => "struct",
        "enum_item" => "enum",
        "trait_item" => "trait",
        "mod_item" => "mod",
        "const_item" => "const",
        "static_item" => "static",
        "type_item" => "type",
        _ => return None,
    };

    symbol_from_named_item(node, source, kind)
}

fn symbol_from_named_item(node: &Node, source: &[u8], kind: &str) -> Option<ExtractedSymbol> {
    let name_node = node.child_by_field_name("name")?;

    if !is_public(node, &name_node, source) {
        return None;
    }

    let name = name_node.utf8_text(source).ok()?.to_string();
    let namespace = namespace_for_node(node, source);

    Some(ExtractedSymbol {
        name,
        kind: kind.to_string(),
        namespace,
    })
}

fn is_public(node: &Node, name_node: &Node, source: &[u8]) -> bool {
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.id() == name_node.id() {
            break;
        }

        if child.kind() == "visibility_modifier" {
            if let Ok(text) = child.utf8_text(source) {
                if text.starts_with("pub") {
                    return true;
                }
            }
        }
    }

    false
}

fn is_in_impl(node: &Node) -> bool {
    let mut current = node.parent();
    while let Some(parent) = current {
        if parent.kind() == "impl_item" {
            return true;
        }
        current = parent.parent();
    }
    false
}

fn namespace_for_node(node: &Node, source: &[u8]) -> Option<String> {
    let mut names = Vec::new();
    let mut current = node.parent();
    while let Some(parent) = current {
        match parent.kind() {
            "mod_item" => {
                if let Some(name_node) = parent.child_by_field_name("name") {
                    if let Ok(name) = name_node.utf8_text(source) {
                        names.push(name.to_string());
                    }
                }
            }
            "impl_item" => return None,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_public_symbols_with_namespaces() {
        let source = r#"
            pub mod foo {
                pub struct Bar;
                pub(crate) fn helper() {}
                fn private_fn() {}

                impl Bar {
                    pub fn method(&self) {}
                }
            }

            pub enum TopLevel {}
            pub fn top_fn() {}
            fn hidden() {}
        "#;

        let mut symbols = extract(source);
        symbols.sort_by(|a, b| a.name.cmp(&b.name));

        assert!(symbols.iter().any(|s| s.name == "foo" && s.kind == "mod"));
        assert!(symbols.iter().any(|s| s.name == "Bar"
            && s.kind == "struct"
            && s.namespace.as_deref() == Some("foo")));
        assert!(symbols.iter().any(|s| s.name == "helper"
            && s.kind == "fn"
            && s.namespace.as_deref() == Some("foo")));
        assert!(symbols
            .iter()
            .any(|s| s.name == "TopLevel" && s.kind == "enum"));
        assert!(symbols.iter().any(|s| s.name == "top_fn" && s.kind == "fn"));

        assert!(symbols.iter().all(|s| s.name != "private_fn"));
        assert!(symbols.iter().all(|s| s.name != "method"));
        assert!(symbols.iter().all(|s| s.name != "hidden"));
    }
}
