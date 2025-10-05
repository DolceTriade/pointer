use tree_sitter::{Node, Parser};

use super::ExtractedSymbol;

pub fn extract(source: &str) -> Vec<ExtractedSymbol> {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_c::language())
        .expect("failed to load tree-sitter C grammar");

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
            let name = node
                .child_by_field_name("declarator")
                .and_then(|decl| identifier_from_declarator(&decl, source))?;
            Some(ExtractedSymbol {
                name,
                kind: "fn".to_string(),
                namespace: None,
            })
        }
        "struct_specifier" => symbol_from_named(node, source, "struct"),
        "union_specifier" => symbol_from_named(node, source, "union"),
        "enum_specifier" => symbol_from_named(node, source, "enum"),
        _ => None,
    }
}

fn symbol_from_named(node: &Node, source: &[u8], kind: &str) -> Option<ExtractedSymbol> {
    let name_node = node.child_by_field_name("name")?;
    let name = name_node.utf8_text(source).ok()?.to_string();

    Some(ExtractedSymbol {
        name,
        kind: kind.to_string(),
        namespace: None,
    })
}

fn identifier_from_declarator(node: &Node, source: &[u8]) -> Option<String> {
    if node.kind() == "identifier" {
        return node.utf8_text(source).ok().map(|text| text.to_string());
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if let Some(name) = identifier_from_declarator(&child, source) {
            return Some(name);
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_functions_and_types() {
        let source = r#"
            struct Foo {
                int value;
            };

            enum Bar {
                A,
                B,
            };

            static int helper(void) {
                return 1;
            }

            int run(struct Foo foo) {
                return helper();
            }
        "#;

        let mut symbols = extract(source);
        symbols.sort_by(|a, b| a.name.cmp(&b.name));

        assert!(symbols
            .iter()
            .any(|s| s.name == "Foo" && s.kind == "struct"));
        assert!(symbols.iter().any(|s| s.name == "Bar" && s.kind == "enum"));
        assert!(symbols.iter().any(|s| s.name == "helper" && s.kind == "fn"));
        assert!(symbols.iter().any(|s| s.name == "run" && s.kind == "fn"));
    }
}
