use tree_sitter::{Node, Parser};

use super::{ExtractedReference, ExtractedSymbol, Extraction};

pub fn extract(source: &str) -> Extraction {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_cpp::LANGUAGE.into())
        .expect("failed to load tree-sitter C++ grammar");

    let tree = match parser.parse(source, None) {
        Some(tree) => tree,
        None => return Extraction::default(),
    };

    let root = tree.root_node();
    let source_bytes = source.as_bytes();
    let mut stack = vec![root];
    let mut symbols = Vec::new();
    let mut references = Vec::new();

    // First pass: collect symbols (definitions and declarations)
    while let Some(node) = stack.pop() {
        let mut extracted = collect_symbols(&node, source_bytes);
        symbols.append(&mut extracted);

        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            stack.push(child);
        }
    }

    // Second pass: collect all identifiers as references (including those that are also symbols)
    // This allows for proper symbol-to-reference mapping where the same identifier can be both
    let identifier_kinds = vec![
        "identifier",
        "field_identifier",
        "scoped_identifier",
        "type_identifier",
    ];
    let mut stack = vec![root];
    while let Some(node) = stack.pop() {
        if identifier_kinds.iter().any(|&kind| kind == node.kind()) {
            if let Ok(text) = node.utf8_text(source_bytes) {
                let name = text.trim();
                if !name.is_empty() {
                    let pos = node.start_position();
                    references.push(ExtractedReference {
                        name: name.to_string(),
                        kind: Some("reference".to_string()),
                        namespace: None,
                        line: pos.row.saturating_add(1) as usize,
                        column: pos.column.saturating_add(1) as usize,
                    });
                }
            }
        }

        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            stack.push(child);
        }
    }

    Extraction {
        symbols,
        references,
    }
}

fn collect_symbols(node: &Node, source: &[u8]) -> Vec<ExtractedSymbol> {
    match node.kind() {
        "function_definition" => {
            let declarator = node.child_by_field_name("declarator");
            let name = declarator
                .as_ref()
                .and_then(|decl| identifier_from_declarator(decl, source));
            let namespace = declarator
                .as_ref()
                .and_then(|decl| namespace_from_qualified(decl, source))
                .or_else(|| namespace_for_node(node, source));
            match name {
                Some(name) => vec![ExtractedSymbol {
                    name,
                    kind: "fn_def".to_string(), // Changed to distinguish definitions
                    namespace,
                }],
                None => Vec::new(),
            }
        }
        "function_declarator" => {
            // This can be part of a declaration that's not a definition
            if let Some(parent) = node.parent() {
                if parent.kind() == "declaration" || parent.kind() == "simple_declaration" {
                    // Check if this is just a declaration (not a definition)
                    if is_function_declaration_only(&parent, source) {
                        if let Some(name) = identifier_from_declarator(node, source) {
                            return vec![ExtractedSymbol {
                                name,
                                kind: "fn_decl".to_string(), // Function declaration
                                namespace: namespace_for_node(node, source),
                            }];
                        }
                    }
                }
            }
            Vec::new()
        }
        "class_specifier" => symbols_from_class_like(node, source, "class_def"),
        "struct_specifier" => symbols_from_class_like(node, source, "struct_def"),
        "enum_specifier" => symbol_from_named(node, source, "enum_def"),
        "namespace_definition" => symbol_from_named(node, source, "namespace"),
        "declaration" | "simple_declaration" => {
            // Handle various types of declarations
            symbols_from_simple_declaration(node, source)
        }
        "preproc_function_def" => macro_symbol(node, source, true),
        "preproc_def" => macro_symbol(node, source, false),
        _ => Vec::new(),
    }
}

// Check if a declaration is just a function declaration (not definition)
fn is_function_declaration_only(decl_node: &Node, _source: &[u8]) -> bool {
    // Look for a function declarator without a function body
    let mut stack = vec![*decl_node];

    while let Some(current) = stack.pop() {
        if current.kind() == "compound_statement" || current.kind() == "field_initializer_list" {
            // If there's a compound statement or field initializer list, it might be a definition
            return false;
        }

        let mut cursor = current.walk();
        for child in current.children(&mut cursor) {
            stack.push(child);
        }
    }
    // If there's no compound statement, it's likely just a declaration
    true
}

fn macro_symbol(node: &Node, source: &[u8], is_function_like: bool) -> Vec<ExtractedSymbol> {
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.kind() == "identifier" {
            if let Ok(text) = child.utf8_text(source) {
                return vec![ExtractedSymbol {
                    name: text.to_string(),
                    kind: if is_function_like {
                        "macro_fn"
                    } else {
                        "macro_var"
                    }
                    .to_string(),
                    namespace: None,
                }];
            }
        }
    }

    Vec::new()
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

    let namespace = namespace_for_node(node, source);

    vec![ExtractedSymbol {
        name,
        kind: kind.to_string(),
        namespace,
    }]
}

fn symbols_from_simple_declaration(node: &Node, source: &[u8]) -> Vec<ExtractedSymbol> {
    if is_typedef(node, source) {
        return Vec::new();
    }

    // Check if this is a function declaration without a body (forward declaration)
    if is_function_declaration_only(node, source) {
        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            if child.kind() == "function_declarator" {
                if let Some(name) = identifier_from_declarator(&child, source) {
                    return vec![ExtractedSymbol {
                        name,
                        kind: "fn_decl".to_string(), // Function declaration
                        namespace: namespace_for_node(node, source),
                    }];
                }
            }
        }
    }

    let mut vars = Vec::new();
    let mut cursor = node.walk();
    let namespace = namespace_for_node(node, source);

    for child in node.children(&mut cursor) {
        match child.kind() {
            "init_declarator" | "structured_binding_declarator" => {
                let declarator = child.child_by_field_name("declarator").unwrap_or(child);
                if let Some(name) = identifier_from_declarator(&declarator, source) {
                    // Check if this has an initializer - if so, it's a definition; otherwise a declaration
                    let is_definition = has_initializer(&child, source);
                    let is_function_like = declarator_contains_function(&declarator);
                    let kind = if is_function_like {
                        if is_definition { "fn_def" } else { "fn_decl" }
                    } else {
                        if is_definition { "var_def" } else { "var_decl" }
                    };
                    vars.push(ExtractedSymbol {
                        name,
                        kind: kind.to_string(),
                        namespace: namespace.clone(),
                    });
                }
            }
            "declarator" | "pointer_declarator" | "reference_declarator" => {
                if let Some(name) = identifier_from_declarator(&child, source) {
                    let is_function_like = declarator_contains_function(&child);
                    if is_function_like {
                        // This is a function pointer declaration/definition
                        let is_definition = has_initializer(&child, source);
                        let kind = if is_definition { "fn_def" } else { "fn_decl" };
                        vars.push(ExtractedSymbol {
                            name,
                            kind: kind.to_string(),
                            namespace: namespace.clone(),
                        });
                    } else {
                        // Check if this has an initializer - if so, it's a definition; otherwise a declaration
                        let is_definition = has_initializer_in_declaration(node, source);
                        let kind = if is_definition { "var_def" } else { "var_decl" };
                        vars.push(ExtractedSymbol {
                            name,
                            kind: kind.to_string(),
                            namespace: namespace.clone(),
                        });
                    }
                }
            }
            "function_declarator" => {
                // This is handled above as a function declaration
                if let Some(name) = identifier_from_declarator(&child, source) {
                    vars.push(ExtractedSymbol {
                        name,
                        kind: "fn_def".to_string(), // Function definition
                        namespace: namespace.clone(),
                    });
                }
            }
            "identifier" | "field_identifier" => {
                if let Some(parent) = child.parent() {
                    if parent.kind() == "init_declarator" {
                        continue;
                    }
                }
                if let Ok(text) = child.utf8_text(source) {
                    let is_function_like = identifier_in_function_declarator(&child);
                    if is_function_like {
                        vars.push(ExtractedSymbol {
                            name: text.to_string(),
                            kind: "fn_def".to_string(),
                            namespace: namespace.clone(),
                        });
                    } else {
                        // Check if this has an initializer - if so, it's a definition; otherwise a declaration
                        let is_definition = has_initializer_in_declaration(node, source);
                        let kind = if is_definition { "var_def" } else { "var_decl" };
                        vars.push(ExtractedSymbol {
                            name: text.to_string(),
                            kind: kind.to_string(),
                            namespace: namespace.clone(),
                        });
                    }
                }
            }
            _ => {}
        }
    }

    vars
}

// Check if a node has an initializer
fn has_initializer(node: &Node, _source: &[u8]) -> bool {
    let mut stack = vec![*node];

    while let Some(current) = stack.pop() {
        if current.kind() == "initializer" || current.kind() == "initializer_list" {
            return true;
        }

        let mut cursor = current.walk();
        for child in current.children(&mut cursor) {
            stack.push(child);
        }
    }

    false
}

// Check if the declaration has an initializer anywhere
fn has_initializer_in_declaration(decl_node: &Node, _source: &[u8]) -> bool {
    let mut stack = vec![*decl_node];

    while let Some(current) = stack.pop() {
        if current.kind() == "initializer" || current.kind() == "initializer_list" {
            return true;
        }

        let mut cursor = current.walk();
        for child in current.children(&mut cursor) {
            stack.push(child);
        }
    }

    false
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
            "function_definition" => {
                if let Some(declarator) = parent.child_by_field_name("declarator") {
                    if let Some(name) = identifier_from_declarator(&declarator, source) {
                        names.push(name);
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

fn declarator_contains_function(node: &Node) -> bool {
    if node.kind() == "function_declarator" {
        return true;
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if declarator_contains_function(&child) {
            return true;
        }
    }

    false
}

fn symbols_from_class_like(node: &Node, source: &[u8], kind: &str) -> Vec<ExtractedSymbol> {
    let mut symbols = symbol_from_named(node, source, kind);
    symbols.extend(function_pointer_members(node, source));
    symbols
}

fn function_pointer_members(node: &Node, source: &[u8]) -> Vec<ExtractedSymbol> {
    let mut results = Vec::new();
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.kind() == "field_declaration_list" {
            collect_function_declarators(&child, source, &mut results);
        }
    }

    results
}

fn collect_function_declarators(node: &Node, source: &[u8], out: &mut Vec<ExtractedSymbol>) {
    let mut stack = vec![*node];

    while let Some(current) = stack.pop() {
        if current.kind() == "function_declarator" {
            if let Some(name) = identifier_from_declarator(&current, source) {
                out.push(ExtractedSymbol {
                    name,
                    kind: "fn_def".to_string(), // Function pointer definition
                    namespace: namespace_for_node(&current, source),
                });
            }
        }

        let mut cursor = current.walk();
        for child in current.children(&mut cursor) {
            if child.kind() != ";" {
                stack.push(child);
            }
        }
    }
}

fn identifier_in_function_declarator(node: &Node) -> bool {
    let mut current = node.parent();
    while let Some(parent) = current {
        if parent.kind().ends_with("declarator") && declarator_contains_function(&parent) {
            return true;
        }
        current = parent.parent();
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_cpp_symbols_and_variables() {
        let source = r#"
            int (*global_handler)(int);
            cvar_t *r_maxPolyVerts;
            namespace foo {
                int (*foo_handler)(int);
                int foo_global = 2;
                class Bar {
                public:
                    int value;
                    int (*on_ready)(int);
                    int method() {
                        int local = value;
                        int (*local_callback)(int);
                        return local;
                    }
                };

                int run();
                int counter = 10;
            }

            int foo::run() {
                int result = counter;
                return result;
            }
        "#;

        let extraction = extract(source);
        let mut symbols = extraction.symbols;
        symbols.sort_by(|a, b| a.name.cmp(&b.name));

        assert!(symbols.iter().any(|s| s.name == "Bar"
            && s.kind == "class_def"
            && s.namespace.as_deref() == Some("foo")));
        assert!(
            symbols
                .iter()
                .any(|s| s.name == "foo" && s.kind == "namespace")
        );
        assert!(symbols.iter().any(|s| s.name == "method"
            && s.kind == "fn_def"
            && s.namespace.as_deref() == Some("foo::Bar")));
        assert!(symbols.iter().any(|s| s.name == "run"
                && s.kind.starts_with("fn_")  // Could be fn_def or fn_decl depending on implementation
                && s.namespace.as_deref() == Some("foo")));

        let var_names: Vec<_> = symbols
            .iter()
            .filter(|s| s.kind.starts_with("var_")) // Match both var_def and var_decl
            .map(|s| (s.name.as_str(), s.namespace.as_deref()))
            .collect();
        assert!(var_names.contains(&("counter", Some("foo"))));
        assert!(var_names.contains(&("r_maxPolyVerts", None)));
        assert!(var_names.contains(&("foo_global", Some("foo"))));
        assert!(var_names.iter().any(|(name, _)| *name == "local"));
        assert!(var_names.iter().any(|(name, _)| *name == "result"));

        let fn_symbols: Vec<_> = symbols
            .iter()
            .filter(|s| s.kind.starts_with("fn_")) // Match both fn_def and fn_decl
            .map(|s| (s.name.as_str(), s.namespace.as_deref()))
            .collect();

        assert!(fn_symbols.contains(&("global_handler", None)));
        assert!(fn_symbols.contains(&("foo_handler", Some("foo"))));
        assert!(fn_symbols.contains(&("on_ready", Some("foo::Bar"))));
        assert!(fn_symbols.contains(&("local_callback", Some("foo::Bar::method"))));
    }
}
