use tree_sitter::{Node, Parser};

use super::ExtractedSymbol;

pub fn extract(source: &str) -> Vec<ExtractedSymbol> {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_objc::LANGUAGE.into())
        .expect("failed to load tree-sitter Objective-C grammar");

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
        "function_definition" => symbol_from_function(node, source),
        "method_definition" => symbol_from_method(node, source),
        "class_interface" | "class_implementation" => symbol_from_named(node, source, "class"),
        "category_interface" | "category_implementation" => {
            symbol_from_named(node, source, "category")
        }
        "protocol_declaration" => symbol_from_named(node, source, "protocol"),
        "property_declaration" => symbols_from_property(node, source),
        "ivar_declaration" | "instance_variable" => symbols_from_ivar(node, source),
        "declaration" => symbols_from_declaration(node, source),
        _ => Vec::new(),
    }
}

fn find_identifier<'a>(node: &Node<'a>) -> Option<Node<'a>> {
    if let Some(name) = node.child_by_field_name("name") {
        return Some(name);
    }
    if let Some(ident) = node.child_by_field_name("identifier") {
        return Some(ident);
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.is_named() && child.kind() == "identifier" {
            return Some(child);
        }
    }

    None
}

fn identifier_text(node: &Node, source: &[u8]) -> Option<String> {
    node.utf8_text(source)
        .ok()
        .map(|text| text.trim().to_string())
}

fn symbol_from_named(node: &Node, source: &[u8], kind: &str) -> Vec<ExtractedSymbol> {
    let name_node = match find_identifier(node) {
        Some(name) => name,
        None => return Vec::new(),
    };

    let name = match identifier_text(&name_node, source) {
        Some(text) => text,
        None => return Vec::new(),
    };

    vec![ExtractedSymbol {
        name,
        kind: kind.to_string(),
        namespace: namespace_for_node(node, source),
    }]
}

fn symbol_from_function(node: &Node, source: &[u8]) -> Vec<ExtractedSymbol> {
    let declarator = match node.child_by_field_name("declarator") {
        Some(decl) => decl,
        None => return Vec::new(),
    };

    let name = identifier_from_declarator(&declarator, source);
    match name {
        Some(name) => vec![ExtractedSymbol {
            name,
            kind: "function".to_string(),
            namespace: namespace_for_node(node, source),
        }],
        None => Vec::new(),
    }
}

fn symbol_from_method(node: &Node, source: &[u8]) -> Vec<ExtractedSymbol> {
    let selector_node = node
        .child_by_field_name("selector")
        .or_else(|| find_identifier(node))
        .or_else(|| node.child(0))
        .filter(|n| n.is_named());

    let selector_node = match selector_node {
        Some(node) => node,
        None => return Vec::new(),
    };

    let name = match identifier_text(&selector_node, source) {
        Some(text) => text,
        None => return Vec::new(),
    };

    vec![ExtractedSymbol {
        name,
        kind: "method".to_string(),
        namespace: namespace_for_node(node, source),
    }]
}

fn symbols_from_property(node: &Node, source: &[u8]) -> Vec<ExtractedSymbol> {
    let mut results = Vec::new();
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.kind() == "struct_declaration" {
            let mut inner = child.walk();
            for declarator in child.children(&mut inner) {
                if declarator.kind() == "struct_declarator" {
                    if let Some(name_node) = find_identifier(&declarator) {
                        if let Some(name) = identifier_text(&name_node, source) {
                            results.push(ExtractedSymbol {
                                name,
                                kind: "property".to_string(),
                                namespace: namespace_for_node(node, source),
                            });
                        }
                    }
                }
            }
        }
    }
    results
}

fn symbols_from_ivar(node: &Node, source: &[u8]) -> Vec<ExtractedSymbol> {
    let mut results = Vec::new();
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.kind() == "struct_declaration" {
            let mut inner = child.walk();
            for declarator in child.children(&mut inner) {
                if declarator.kind() == "struct_declarator" {
                    if let Some(name_node) = find_identifier(&declarator) {
                        if let Some(name) = identifier_text(&name_node, source) {
                            results.push(ExtractedSymbol {
                                name,
                                kind: "ivar".to_string(),
                                namespace: namespace_for_node(node, source),
                            });
                        }
                    }
                }
            }
        }
    }
    results
}

fn symbols_from_declaration(node: &Node, source: &[u8]) -> Vec<ExtractedSymbol> {
    let mut results = Vec::new();
    let mut cursor = node.walk();

    for child in node.children(&mut cursor) {
        if child.kind() == "init_declarator" {
            let declarator = child.child_by_field_name("declarator").unwrap_or(child);
            if let Some(name) = identifier_from_declarator(&declarator, source) {
                results.push(ExtractedSymbol {
                    name,
                    kind: "var".to_string(),
                    namespace: namespace_for_node(node, source),
                });
            }
        } else if matches!(
            child.kind(),
            "declarator"
                | "pointer_declarator"
                | "reference_declarator"
                | "abstract_declarator"
                | "identifier"
        ) {
            if let Some(name) = identifier_from_declarator(&child, source) {
                results.push(ExtractedSymbol {
                    name,
                    kind: "var".to_string(),
                    namespace: namespace_for_node(node, source),
                });
            }
        }
    }

    results
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

fn namespace_for_node(node: &Node, source: &[u8]) -> Option<String> {
    let mut segments = Vec::new();
    let mut current = node.parent();

    while let Some(parent) = current {
        match parent.kind() {
            "class_interface"
            | "class_implementation"
            | "category_interface"
            | "category_implementation"
            | "protocol_declaration" => {
                if let Some(name_node) = find_identifier(&parent) {
                    if let Some(text) = identifier_text(&name_node, source) {
                        segments.push(text);
                    }
                }
            }
            "method_definition" => {
                if let Some(selector_node) = parent
                    .child_by_field_name("selector")
                    .or_else(|| find_identifier(&parent))
                {
                    if let Some(text) = identifier_text(&selector_node, source) {
                        segments.push(text);
                    }
                }
            }
            "function_definition" => {
                if let Some(declarator) = parent.child_by_field_name("declarator") {
                    if let Some(name) = identifier_from_declarator(&declarator, source) {
                        segments.push(name);
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
    fn extracts_objective_c_symbols() {
        let source = r#"
            int solovar;
            @interface Demo : NSObject {
                int _count;
            }
            @property(nonatomic) int value;
            - (void)doThing;
            @end

            @implementation Demo
            - (void)doThing {
                int local = 0;
                int temp;
            }
            @end

            void Helper(void) {
                int global = 1;
                int global_no_init;
            }
        "#;

        let mut symbols = extract(source);
        symbols.sort_by(|a, b| a.name.cmp(&b.name));

        assert!(symbols
            .iter()
            .any(|s| s.name == "Demo" && s.kind == "class"));
        assert!(symbols.iter().any(|s| {
            s.name == "doThing" && s.kind == "method" && s.namespace.as_deref() == Some("Demo")
        }));
        assert!(symbols
            .iter()
            .any(|s| s.name == "Helper" && s.kind == "function"));

        let fields: Vec<_> = symbols
            .iter()
            .filter(|s| matches!(s.kind.as_str(), "ivar" | "property"))
            .map(|s| (s.name.as_str(), s.kind.as_str()))
            .collect();
        assert!(fields.contains(&("_count", "ivar")));
        assert!(fields.contains(&("value", "property")));

        let vars: Vec<_> = symbols
            .iter()
            .filter(|s| s.kind == "var")
            .map(|s| (s.name.as_str(), s.namespace.as_deref()))
            .collect();
        assert!(vars.contains(&("local", Some("Demo.doThing"))));
        assert!(vars.contains(&("temp", Some("Demo.doThing"))));
        assert!(vars.contains(&("global", Some("Helper"))));
        assert!(vars.contains(&("global_no_init", Some("Helper"))));
        assert!(vars.contains(&("solovar", None)));
    }
}
