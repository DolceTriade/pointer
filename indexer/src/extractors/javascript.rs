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
        if let Some(mut extracted) = extract_symbol(&node, source_bytes) {
            symbols.append(&mut extracted);
        }

        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            stack.push(child);
        }
    }

    symbols
}

fn extract_symbol(node: &Node, source: &[u8]) -> Option<Vec<ExtractedSymbol>> {
    match node.kind() {
        "function_declaration" | "generator_function_declaration" => {
            if !is_exported(node) {
                return None;
            }
            symbol_from_named(node, source, "function").map(|symbol| vec![symbol])
        }
        "class_declaration" => {
            if !is_exported(node) {
                return None;
            }
            symbol_from_named(node, source, "class").map(|symbol| vec![symbol])
        }
        "lexical_declaration" => {
            if !is_exported(node) {
                return None;
            }
            Some(symbols_from_variable_declaration(
                node,
                source,
                lexical_kind(node, source),
            ))
        }
        "variable_declaration" => {
            if !is_exported(node) {
                return None;
            }
            Some(symbols_from_variable_declaration(
                node,
                source,
                "var".to_string(),
            ))
        }
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

fn symbols_from_variable_declaration(
    node: &Node,
    source: &[u8],
    kind: String,
) -> Vec<ExtractedSymbol> {
    let mut results = Vec::new();
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        match child.kind() {
            "variable_declarator" | "lexical_declarator" => {
                if let Some(name_node) = child.child_by_field_name("name") {
                    if let Ok(name) = name_node.utf8_text(source) {
                        results.push(ExtractedSymbol {
                            name: name.to_string(),
                            kind: kind.clone(),
                            namespace: None,
                        });
                    }
                }
            }
            _ => {}
        }
    }
    results
}

fn is_exported(node: &Node) -> bool {
    let mut current = node.parent();
    while let Some(parent) = current {
        if parent.kind() == "export_statement" {
            return true;
        }
        if parent.kind() == "program" {
            break;
        }
        current = parent.parent();
    }
    false
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
    fn extracts_exported_symbols() {
        let source = r#"
            export function foo() {}
            function hidden() {}
            export class Widget {}
            export const answer = 42, helper = () => {};
            export let flag = true;
            export var legacy = 0;
            export default function () {}
        "#;

        let mut symbols = extract(source);
        symbols.sort_by(|a, b| a.name.cmp(&b.name));

        assert!(symbols
            .iter()
            .any(|s| s.name == "foo" && s.kind == "function"));
        assert!(symbols
            .iter()
            .any(|s| s.name == "Widget" && s.kind == "class"));
        assert!(symbols
            .iter()
            .any(|s| s.name == "answer" && s.kind == "const"));
        assert!(symbols
            .iter()
            .any(|s| s.name == "helper" && s.kind == "const"));
        assert!(symbols.iter().any(|s| s.name == "flag" && s.kind == "let"));
        assert!(symbols
            .iter()
            .any(|s| s.name == "legacy" && s.kind == "var"));
        assert!(symbols.iter().all(|s| s.name != "hidden"));
    }
}
