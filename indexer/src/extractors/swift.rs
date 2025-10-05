use tree_sitter::{Node, Parser};

use super::ExtractedSymbol;

pub fn extract(source: &str) -> Vec<ExtractedSymbol> {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_swift::LANGUAGE.into())
        .expect("failed to load tree-sitter Swift grammar");

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
        "class_declaration" => symbol_from_class_like(node, source),
        "struct_declaration" => symbol_from_named(node, source, "struct"),
        "enum_declaration" => symbol_from_named(node, source, "enum"),
        "protocol_declaration" => symbol_from_named(node, source, "protocol"),
        "extension_declaration" => symbol_from_named(node, source, "extension"),
        "function_declaration" => symbol_from_named(node, source, "function"),
        "initializer_declaration" => symbol_from_named(node, source, "init"),
        "deinitializer_declaration" => symbol_from_named(node, source, "deinit"),
        "property_declaration" => symbols_from_property_declaration(node, source),
        "variable_declaration" => symbols_from_variable_declaration(node, source),
        _ => Vec::new(),
    }
}

fn symbol_from_named(node: &Node, source: &[u8], kind: &str) -> Vec<ExtractedSymbol> {
    let name_node = node
        .child_by_field_name("name")
        .or_else(|| node.child_by_field_name("identifier"));
    let name_node = match name_node {
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

fn symbol_from_class_like(node: &Node, source: &[u8]) -> Vec<ExtractedSymbol> {
    let name_node = node
        .child_by_field_name("name")
        .or_else(|| node.child_by_field_name("identifier"));
    let name_node = match name_node {
        Some(name) => name,
        None => return Vec::new(),
    };

    let name = match name_node.utf8_text(source) {
        Ok(text) => text.to_string(),
        Err(_) => return Vec::new(),
    };

    let kind = swift_type_keyword(node, &name_node, source);

    vec![ExtractedSymbol {
        name,
        kind,
        namespace: namespace_for_node(node, source),
    }]
}

fn swift_type_keyword(node: &Node, name_node: &Node, source: &[u8]) -> String {
    let start = node.start_byte();
    let end = name_node.start_byte();
    if start >= end || end > source.len() {
        return "class".to_string();
    }

    let prefix = &source[start..end];
    let text = std::str::from_utf8(prefix).unwrap_or("").trim();
    if text.contains("struct") {
        "struct".to_string()
    } else if text.contains("enum") {
        "enum".to_string()
    } else if text.contains("protocol") {
        "protocol".to_string()
    } else if text.contains("extension") {
        "extension".to_string()
    } else {
        "class".to_string()
    }
}

fn symbols_from_variable_declaration(node: &Node, source: &[u8]) -> Vec<ExtractedSymbol> {
    let namespace = namespace_for_node(node, source);
    let mut base_names = Vec::new();
    if let Some(pattern) = node.child_by_field_name("pattern") {
        collect_pattern_names(&pattern, source, &mut base_names);
    }

    let mut results = Vec::new();
    let mut handled_initializers = false;

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.kind() == "pattern_initializer" {
            handled_initializers = true;
            if let Some(pattern) = child.child_by_field_name("pattern") {
                let mut names = Vec::new();
                collect_pattern_names(&pattern, source, &mut names);
                let pattern_is_function = swift_pattern_has_function_type(&pattern);
                let value_is_function = child
                    .child_by_field_name("value")
                    .map(|value| swift_expression_is_function(&value))
                    .unwrap_or(false);
                let is_function_like = pattern_is_function || value_is_function;

                for name in names {
                    results.push(ExtractedSymbol {
                        name,
                        kind: if is_function_like { "function" } else { "var" }.to_string(),
                        namespace: namespace.clone(),
                    });
                }
            }
        }
    }

    if !handled_initializers {
        let type_is_function = node
            .child_by_field_name("type_annotation")
            .map(|ty| swift_type_is_function(&ty))
            .unwrap_or(false)
            || swift_node_contains_function_type(node);

        for name in base_names {
            results.push(ExtractedSymbol {
                name,
                kind: if type_is_function { "function" } else { "var" }.to_string(),
                namespace: namespace.clone(),
            });
        }
    }

    results
}

fn symbols_from_property_declaration(node: &Node, source: &[u8]) -> Vec<ExtractedSymbol> {
    let namespace = namespace_for_node(node, source);
    let mut names = Vec::new();

    if let Some(name_node) = node.child_by_field_name("name") {
        collect_pattern_names(&name_node, source, &mut names);
    }

    let type_is_function = node
        .child_by_field_name("type_annotation")
        .map(|ty| swift_type_is_function(&ty))
        .unwrap_or(false)
        || swift_node_contains_function_type(node);
    let value_is_function = node
        .child_by_field_name("value")
        .map(|value| swift_expression_is_function(&value))
        .unwrap_or(false);
    let is_function_like = type_is_function || value_is_function;

    names
        .into_iter()
        .map(|name| ExtractedSymbol {
            name,
            kind: if is_function_like { "function" } else { "var" }.to_string(),
            namespace: namespace.clone(),
        })
        .collect()
}

fn collect_pattern_names(node: &Node, source: &[u8], out: &mut Vec<String>) {
    match node.kind() {
        "identifier_pattern" | "identifier" | "simple_identifier" | "bound_identifier" => {
            if let Ok(name) = node.utf8_text(source) {
                out.push(name.to_string());
            }
        }
        "pattern"
        | "tuple_pattern"
        | "wildcard_pattern"
        | "value_binding_pattern"
        | "pattern_tuple_element_list" => {
            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                collect_pattern_names(&child, source, out);
            }
        }
        _ => {}
    }
}

fn swift_pattern_has_function_type(node: &Node) -> bool {
    swift_node_contains_function_type(node)
}

fn swift_type_is_function(node: &Node) -> bool {
    swift_node_contains_function_type(node)
}

fn swift_node_contains_function_type(node: &Node) -> bool {
    match node.kind() {
        "function_type" | "lambda_function_type" => return true,
        _ => {}
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if swift_node_contains_function_type(&child) {
            return true;
        }
    }

    false
}

fn swift_expression_is_function(node: &Node) -> bool {
    if node.kind() == "lambda_literal" {
        return true;
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.is_named() && swift_expression_is_function(&child) {
            return true;
        }
    }

    false
}

fn namespace_for_node(node: &Node, source: &[u8]) -> Option<String> {
    let mut segments = Vec::new();
    let mut current = node.parent();

    while let Some(parent) = current {
        match parent.kind() {
            "class_declaration"
            | "struct_declaration"
            | "enum_declaration"
            | "protocol_declaration"
            | "extension_declaration" => {
                if let Some(name_node) = parent.child_by_field_name("name") {
                    if let Ok(text) = name_node.utf8_text(source) {
                        segments.push(text.to_string());
                    }
                }
            }
            "function_declaration" | "initializer_declaration" | "deinitializer_declaration" => {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_swift_symbols() {
        let source = r#"
            class Demo {
                var count = 0
                var handler: (Int) -> Int = { $0 }
                var callback: (Int) -> Int
                func doThing() {
                    let local = 1
                    let localHandler = { (x: Int) -> Int in x }
                }
            }

            struct Value {
                let inner: Int
            }

            func helper() {
                let answer = 42
            }
            let execute = { (x: Int) -> Int in x }
        "#;

        let mut symbols = extract(source);
        symbols.sort_by(|a, b| a.name.cmp(&b.name));

        assert!(symbols
            .iter()
            .any(|s| s.name == "Demo" && s.kind == "class"));
        assert!(symbols
            .iter()
            .any(|s| s.name == "Value" && s.kind == "struct"));
        assert!(symbols.iter().any(|s| {
            s.name == "doThing" && s.kind == "function" && s.namespace.as_deref() == Some("Demo")
        }));
        assert!(symbols
            .iter()
            .any(|s| s.name == "helper" && s.kind == "function"));

        let vars: Vec<_> = symbols
            .iter()
            .filter(|s| s.kind == "var")
            .map(|s| (s.name.as_str(), s.namespace.as_deref()))
            .collect();
        assert!(vars.contains(&("count", Some("Demo"))));
        assert!(vars.contains(&("local", Some("Demo.doThing"))));
        assert!(vars.contains(&("inner", Some("Value"))));
        assert!(vars.contains(&("answer", Some("helper"))));
        assert!(!vars.iter().any(|(name, _)| *name == "handler"));
        assert!(!vars.iter().any(|(name, _)| *name == "callback"));
        assert!(!vars.iter().any(|(name, _)| *name == "localHandler"));
        assert!(!vars.iter().any(|(name, _)| *name == "execute"));

        let fn_symbols: Vec<_> = symbols
            .iter()
            .filter(|s| s.kind == "function")
            .map(|s| (s.name.as_str(), s.namespace.as_deref()))
            .collect();
        assert!(fn_symbols.contains(&("handler", Some("Demo"))));
        assert!(fn_symbols.contains(&("callback", Some("Demo"))));
        assert!(fn_symbols.contains(&("localHandler", Some("Demo.doThing"))));
        assert!(fn_symbols.contains(&("execute", None)));
    }
}
