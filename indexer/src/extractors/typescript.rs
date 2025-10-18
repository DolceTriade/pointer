use tree_sitter::{Node, Parser};

use super::ExtractedSymbol;

pub fn extract(source: &str) -> Vec<ExtractedSymbol> {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into())
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
        "interface_declaration" => symbol_from_named(node, source, "interface"),
        "type_alias_declaration" => symbol_from_named(node, source, "type"),
        "enum_declaration" => symbol_from_named(node, source, "enum"),
        "namespace_declaration" | "internal_module" => symbol_from_named(node, source, "namespace"),
        "method_definition" | "method_signature" => symbols_from_method(node, source),
        "public_field_definition" | "property_declaration" | "property_signature" => {
            symbols_from_field(node, source)
        }
        "constructor" | "constructor_signature" => symbol_from_named(node, source, "constructor"),
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
                let single_binding = names.len() == 1;
                let type_is_function = child
                    .child_by_field_name("type")
                    .map(|ty| ts_type_is_function(&ty))
                    .unwrap_or(false);
                let value_is_function = child
                    .child_by_field_name("value")
                    .map(|value| ts_expression_is_function(&value))
                    .unwrap_or(false);
                let is_function_like = single_binding && (type_is_function || value_is_function);
                let symbol_kind = if is_function_like {
                    "function".to_string()
                } else {
                    kind.clone()
                };
                for name in names {
                    results.push(ExtractedSymbol {
                        name,
                        kind: symbol_kind.clone(),
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
        "array_pattern"
        | "object_pattern"
        | "binding_pattern"
        | "pair_pattern"
        | "parenthesized_pattern" => {
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
        matches!(
            left.kind(),
            "array_pattern" | "object_pattern" | "binding_pattern"
        )
    } else {
        false
    }
}

fn namespace_for_node(node: &Node, source: &[u8]) -> Option<String> {
    let mut segments = Vec::new();
    let mut current = node.parent();

    while let Some(parent) = current {
        match parent.kind() {
            "class_declaration"
            | "class_expression"
            | "interface_declaration"
            | "enum_declaration" => {
                if let Some(name_node) = parent.child_by_field_name("name") {
                    if let Ok(text) = name_node.utf8_text(source) {
                        segments.push(text.to_string());
                    }
                }
            }
            "namespace_declaration" | "internal_module" => {
                if let Some(name_node) = parent.child_by_field_name("name") {
                    if let Ok(text) = name_node.utf8_text(source) {
                        segments.push(text.to_string());
                    }
                }
            }
            "method_definition" | "method_signature" => {
                if let Some(name_node) = parent
                    .child_by_field_name("name")
                    .or_else(|| parent.child_by_field_name("property"))
                {
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

fn ts_expression_is_function(node: &Node) -> bool {
    match node.kind() {
        "function_expression" | "function" | "generator_function" | "arrow_function" => true,
        "parenthesized_expression" => {
            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                if child.is_named() && ts_expression_is_function(&child) {
                    return true;
                }
            }
            false
        }
        _ => false,
    }
}

fn ts_type_is_function(node: &Node) -> bool {
    if node.kind() == "function_type" {
        return true;
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if ts_type_is_function(&child) {
            return true;
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_typescript_symbols_and_variables() {
        let source = r#"
            interface Thing {
                field: string;
                method(): void;
            }
            type Alias = string;
            enum Status { Ready }
            function helper() {
                const inside = 1;
                let [a, b] = [1, 2];
            }
            class Container {
                value = 5;
                method() {
                    const local = 0;
                }
            }
            namespace Utils {
                export function inner() {
                    let local = 2;
                }
            }
            namespace Internal {
                function hidden() {
                    const secret = 0;
                }
            }
            const value = 1;
            let count = 0;
            const handler: (x: number) => number = (x) => x;
            let pointer: () => void;
            const arrow = () => {};
        "#;

        let mut symbols = extract(source);
        symbols.sort_by(|a, b| a.name.cmp(&b.name));

        assert!(
            symbols
                .iter()
                .any(|s| s.name == "Alias" && s.kind == "type")
        );
        assert!(
            symbols
                .iter()
                .any(|s| s.name == "Container" && s.kind == "class")
        );
        assert!(
            symbols
                .iter()
                .any(|s| s.name == "Thing" && s.kind == "interface")
        );
        assert!(
            symbols
                .iter()
                .any(|s| s.name == "Status" && s.kind == "enum")
        );
        assert!(
            symbols
                .iter()
                .any(|s| s.name == "helper" && s.kind == "function")
        );
        assert!(
            symbols
                .iter()
                .any(|s| s.name == "Utils" && s.kind == "namespace")
        );
        assert!(symbols.iter().any(|s| s.name == "inner"
            && s.kind == "function"
            && s.namespace.as_deref() == Some("Utils")));
        assert!(
            symbols
                .iter()
                .any(|s| s.name == "hidden" && s.kind == "function")
        );

        let fields: Vec<_> = symbols
            .iter()
            .filter(|s| s.kind == "field")
            .map(|s| (s.name.as_str(), s.namespace.as_deref()))
            .collect();
        assert!(fields.contains(&("field", Some("Thing"))));
        assert!(fields.contains(&("value", Some("Container"))));

        let vars: Vec<_> = symbols
            .iter()
            .filter(|s| matches!(s.kind.as_str(), "var" | "let" | "const"))
            .map(|s| (s.name.as_str(), s.namespace.as_deref()))
            .collect();

        assert!(vars.contains(&("inside", Some("helper"))));
        assert!(vars.contains(&("a", Some("helper"))));
        assert!(vars.contains(&("b", Some("helper"))));
        assert!(vars.contains(&("local", Some("Container.method"))));
        assert!(vars.contains(&("local", Some("Utils.inner"))));
        assert!(vars.contains(&("secret", Some("Internal.hidden"))));
        assert!(vars.contains(&("value", None)));
        assert!(vars.contains(&("count", None)));
        assert!(!vars.iter().any(|(name, _)| *name == "handler"));
        assert!(!vars.iter().any(|(name, _)| *name == "pointer"));
        assert!(!vars.iter().any(|(name, _)| *name == "arrow"));

        let fn_symbols: Vec<_> = symbols
            .iter()
            .filter(|s| s.kind == "function")
            .map(|s| (s.name.as_str(), s.namespace.as_deref()))
            .collect();
        assert!(fn_symbols.contains(&("handler", None)));
        assert!(fn_symbols.contains(&("pointer", None)));
        assert!(fn_symbols.contains(&("arrow", None)));
    }
}
