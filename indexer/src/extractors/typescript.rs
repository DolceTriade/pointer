use tree_sitter::{Node, Parser};

use super::ExtractedSymbol;

pub fn extract(source: &str) -> Vec<ExtractedSymbol> {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_typescript::language_typescript())
        .expect("failed to load tree-sitter TypeScript grammar");

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
            if !is_exported(node, source) {
                return None;
            }
            symbol_from_named(node, source, "function").map(|symbol| vec![symbol])
        }
        "class_declaration" => {
            if !is_exported(node, source) {
                return None;
            }
            symbol_from_named(node, source, "class").map(|symbol| vec![symbol])
        }
        "interface_declaration" => {
            if !is_exported(node, source) {
                return None;
            }
            symbol_from_named(node, source, "interface").map(|symbol| vec![symbol])
        }
        "type_alias_declaration" => {
            if !is_exported(node, source) {
                return None;
            }
            symbol_from_named(node, source, "type").map(|symbol| vec![symbol])
        }
        "enum_declaration" => {
            if !is_exported(node, source) {
                return None;
            }
            symbol_from_named(node, source, "enum").map(|symbol| vec![symbol])
        }
        "namespace_declaration" | "internal_module" => {
            if !is_exported(node, source) {
                return None;
            }
            symbol_from_named(node, source, "namespace").map(|symbol| vec![symbol])
        }
        "lexical_declaration" => {
            if !is_exported(node, source) {
                return None;
            }
            Some(symbols_from_variable_declaration(
                node,
                source,
                lexical_kind(node, source),
            ))
        }
        "variable_declaration" => {
            if !is_exported(node, source) {
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
    let namespace = namespace_for_node(node, source);

    Some(ExtractedSymbol {
        name,
        kind: kind.to_string(),
        namespace,
    })
}

fn symbols_from_variable_declaration(
    node: &Node,
    source: &[u8],
    kind: String,
) -> Vec<ExtractedSymbol> {
    let mut results = Vec::new();
    let namespace = namespace_for_node(node, source);
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        match child.kind() {
            "variable_declarator" | "lexical_declarator" => {
                if let Some(name_node) = child.child_by_field_name("name") {
                    if let Ok(name) = name_node.utf8_text(source) {
                        results.push(ExtractedSymbol {
                            name: name.to_string(),
                            kind: kind.clone(),
                            namespace: namespace.clone(),
                        });
                    }
                }
            }
            _ => {}
        }
    }
    results
}

fn has_export_modifier(node: &Node, source: &[u8]) -> bool {
    let start = node.start_byte();
    let end = node
        .child_by_field_name("name")
        .map(|name| name.start_byte())
        .unwrap_or(start);

    if start >= end || end > source.len() {
        return false;
    }

    std::str::from_utf8(&source[start..end])
        .map(|text| text.contains("export"))
        .unwrap_or(false)
}

fn node_has_export(node: &Node, source: &[u8]) -> bool {
    has_export_modifier(node, source)
        || node
            .parent()
            .map(|parent| parent.kind() == "export_statement")
            .unwrap_or(false)
}

fn ancestors_exported(node: &Node, source: &[u8]) -> bool {
    let mut current = node.parent();
    while let Some(parent) = current {
        if matches!(parent.kind(), "namespace_declaration" | "internal_module")
            && !node_has_export(&parent, source)
        {
            return false;
        }
        current = parent.parent();
    }
    true
}

fn is_exported(node: &Node, source: &[u8]) -> bool {
    if !ancestors_exported(node, source) {
        return false;
    }

    if node_has_export(node, source) {
        return true;
    }

    let mut current = node.parent();
    while let Some(parent) = current {
        if node_has_export(&parent, source) {
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
            "class_declaration"
            | "interface_declaration"
            | "enum_declaration"
            | "type_alias_declaration"
            | "namespace_declaration"
            | "internal_module" => {
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
    fn extracts_exported_typescript_symbols() {
        let source = r#"
            export interface Thing {}
            export type Alias = string;
            export enum Status { Ready }
            export function helper() {}
            export class Container {}
            export namespace Utils {
                export function inner() {}
            }
            namespace Internal {
                export function hidden() {}
            }
            export const value = 1;
            const internal = 0;
        "#;

        let mut symbols = extract(source);
        symbols.sort_by(|a, b| a.name.cmp(&b.name));

        assert!(symbols
            .iter()
            .any(|s| s.name == "Alias" && s.kind == "type"));
        assert!(symbols
            .iter()
            .any(|s| s.name == "Container" && s.kind == "class"));
        assert!(symbols
            .iter()
            .any(|s| s.name == "Thing" && s.kind == "interface"));
        assert!(symbols
            .iter()
            .any(|s| s.name == "Status" && s.kind == "enum"));
        assert!(symbols
            .iter()
            .any(|s| s.name == "helper" && s.kind == "function"));
        assert!(symbols
            .iter()
            .any(|s| s.name == "Utils" && s.kind == "namespace"));
        assert!(symbols.iter().any(|s| s.name == "inner"
            && s.kind == "function"
            && s.namespace.as_deref() == Some("Utils")));
        assert!(symbols
            .iter()
            .any(|s| s.name == "value" && s.kind == "const"));
        assert!(symbols.iter().all(|s| s.name != "internal"));
        assert!(symbols.iter().all(|s| s.name != "hidden"));
    }
}
