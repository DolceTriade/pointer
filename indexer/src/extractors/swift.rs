use tree_sitter::{Node, Parser};

use super::{ExtractedReference, Extraction};

pub fn extract(source: &str) -> Extraction {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_swift::LANGUAGE.into())
        .expect("failed to load tree-sitter Swift grammar");

    let tree = match parser.parse(source, None) {
        Some(tree) => tree,
        None => return Extraction::default(),
    };

    let mut references = Vec::new();
    let source_bytes = source.as_bytes();
    collect_references(&tree.root_node(), source_bytes, &mut references, &[]);

    references.into()
}

fn collect_references(
    node: &Node,
    source: &[u8],
    references: &mut Vec<ExtractedReference>,
    namespace_stack: &[String],
) {
    let mut new_namespace_stack = namespace_stack.to_owned();

    match node.kind() {
        "class_declaration"
        | "struct_declaration"
        | "enum_declaration"
        | "protocol_declaration"
        | "extension_declaration"
        | "function_declaration"
        | "initializer_declaration"
        | "deinitializer_declaration" => {
            if let Some(name_node) = node.child_by_field_name("name") {
                if let Ok(name) = name_node.utf8_text(source) {
                    let pos = name_node.start_position();
                    references.push(ExtractedReference {
                        name: name.to_string(),
                        kind: Some("definition".to_string()),
                        namespace: if namespace_stack.is_empty() {
                            None
                        } else {
                            Some(namespace_stack.join("."))
                        },
                        line: pos.row + 1,
                        column: pos.column + 1,
                    });
                    new_namespace_stack.push(name.to_string());
                }
            }
        }
        "property_declaration" | "variable_declaration" => {
            let mut names = Vec::new();
            if let Some(pattern) = node.child_by_field_name("pattern") {
                collect_pattern_names(&pattern, source, &mut names);
            } else if let Some(name_node) = node.child_by_field_name("name") {
                collect_pattern_names(&name_node, source, &mut names);
            }

            for name in names {
                let pos = node.start_position();
                references.push(ExtractedReference {
                    name: name.to_string(),
                    kind: Some("definition".to_string()),
                    namespace: if namespace_stack.is_empty() {
                        None
                    } else {
                        Some(namespace_stack.join("."))
                    },
                    line: pos.row + 1,
                    column: pos.column + 1,
                });
            }
        }
        "identifier" | "simple_identifier" | "bound_identifier" | "identifier_pattern" => {
            if !is_part_of_definition_or_declaration(node) {
                if let Ok(name) = node.utf8_text(source) {
                    let pos = node.start_position();
                    references.push(ExtractedReference {
                        name: name.to_string(),
                        kind: Some("reference".to_string()),
                        namespace: if namespace_stack.is_empty() {
                            None
                        } else {
                            Some(namespace_stack.join("."))
                        },
                        line: pos.row + 1,
                        column: pos.column + 1,
                    });
                }
            }
        }
        _ => {}
    }

    for child in node.children(&mut node.walk()) {
        collect_references(&child, source, references, &new_namespace_stack);
    }
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

fn is_part_of_definition_or_declaration(node: &Node) -> bool {
    let mut current = node.parent();
    while let Some(parent) = current {
        match parent.kind() {
            "class_declaration"
            | "struct_declaration"
            | "enum_declaration"
            | "protocol_declaration"
            | "extension_declaration"
            | "function_declaration"
            | "initializer_declaration"
            | "deinitializer_declaration"
            | "property_declaration"
            | "variable_declaration" => {
                return true;
            }
            _ => {}
        }
        current = parent.parent();
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

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

        let extraction = extract(source);
        let references = extraction.references;

        let definitions: HashSet<_> = references
            .iter()
            .filter(|r| r.kind == Some("definition".to_string()))
            .map(|r| (r.name.as_str(), r.namespace.as_deref()))
            .collect();

        assert!(definitions.contains(&("Demo", None)));
        assert!(definitions.contains(&("count", Some("Demo"))));
        assert!(definitions.contains(&("handler", Some("Demo"))));
        assert!(definitions.contains(&("callback", Some("Demo"))));
        assert!(definitions.contains(&("doThing", Some("Demo"))));
        assert!(definitions.contains(&("local", Some("Demo.doThing"))));
        assert!(definitions.contains(&("localHandler", Some("Demo.doThing"))));
        assert!(definitions.contains(&("Value", None)));
        assert!(definitions.contains(&("inner", Some("Value"))));
        assert!(definitions.contains(&("helper", None)));
        assert!(definitions.contains(&("answer", Some("helper"))));
        assert!(definitions.contains(&("execute", None)));
    }
}
