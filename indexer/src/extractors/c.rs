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
        "function_definition" => {
            let name = node
                .child_by_field_name("declarator")
                .and_then(|decl| identifier_from_declarator(&decl, source));
            match name {
                Some(name) => vec![ExtractedSymbol {
                    name,
                    kind: "fn".to_string(),
                    namespace: None,
                }],
                None => Vec::new(),
            }
        }
        "struct_specifier" => symbol_from_named(node, source, "struct"),
        "union_specifier" => symbol_from_named(node, source, "union"),
        "enum_specifier" => symbol_from_named(node, source, "enum"),
        "declaration" => symbols_from_declaration(node, source),
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
        namespace: None,
    }]
}

fn symbols_from_declaration(node: &Node, source: &[u8]) -> Vec<ExtractedSymbol> {
    if is_typedef(node, source) {
        return Vec::new();
    }

    let mut vars = Vec::new();
    let mut cursor = node.walk();

    for child in node.children(&mut cursor) {
        match child.kind() {
            "init_declarator" => {
                let declarator = child.child_by_field_name("declarator").unwrap_or(child);
                if declarator.kind() == "function_declarator" {
                    continue;
                }
                if let Some(name) = identifier_from_declarator(&declarator, source) {
                    vars.push(ExtractedSymbol {
                        name,
                        kind: "var".to_string(),
                        namespace: None,
                    });
                }
            }
            "identifier" => {
                if let Some(parent) = child.parent() {
                    if parent.kind() == "init_declarator" {
                        continue;
                    }
                }
                if let Ok(text) = child.utf8_text(source) {
                    vars.push(ExtractedSymbol {
                        name: text.to_string(),
                        kind: "var".to_string(),
                        namespace: None,
                    });
                }
            }
            _ => {}
        }
    }

    vars
}

fn is_typedef(node: &Node, source: &[u8]) -> bool {
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.kind() == "storage_class_specifier" {
            if let Ok(text) = child.utf8_text(source) {
                if text.trim() == "typedef" {
                    return true;
                }
            }
        }
    }
    false
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
    fn extracts_functions_types_and_variables() {
        let source = r#"
            customType uninitialized;
            struct Foo {
                int value;
            };

            enum Bar {
                A,
                B,
            };

            int counter = 0;

            static int helper(void) {
                int local = 3;
                return local;
            }

            int run(struct Foo foo) {
                int result = helper();
                return result;
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

        let var_names: Vec<_> = symbols
            .iter()
            .filter(|s| s.kind == "var")
            .map(|s| s.name.as_str())
            .collect();

        assert!(var_names.contains(&"counter"));
        assert!(var_names.contains(&"local"));
        assert!(var_names.contains(&"result"));
        assert!(var_names.contains(&"uninitialized"));
    }
}
