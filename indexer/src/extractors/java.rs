use tree_sitter::{Node, Parser};

use super::{ExtractedReference, Extraction};

pub fn extract(source: &str) -> Extraction {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_java::LANGUAGE.into())
        .expect("failed to load tree-sitter Java grammar");

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
        | "interface_declaration"
        | "enum_declaration"
        | "record_declaration"
        | "annotation_type_declaration" => {
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
        "method_declaration" | "constructor_declaration" => {
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
        "field_declaration" | "local_variable_declaration" => {
            for child in node.children(&mut node.walk()) {
                if child.kind() == "variable_declarator" {
                    if let Some(name_node) = child.child_by_field_name("name") {
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
                        }
                    }
                }
            }
        }
        "package_declaration" => {
            if let Some(name_node) = node.child_by_field_name("name") {
                if let Ok(name) = name_node.utf8_text(source) {
                    new_namespace_stack.push(name.to_string());
                }
            }
        }
        "identifier" => {
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

fn is_part_of_definition_or_declaration(node: &Node) -> bool {
    let mut current = node.parent();
    while let Some(parent) = current {
        match parent.kind() {
            "class_declaration"
            | "interface_declaration"
            | "enum_declaration"
            | "record_declaration"
            | "annotation_type_declaration"
            | "method_declaration"
            | "constructor_declaration"
            | "field_declaration"
            | "local_variable_declaration"
            | "package_declaration" => {
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
    fn extracts_public_symbols_and_variables() {
        let source = r#"
            package com.example.demo;

            public class Foo {
                private int value;
                java.util.function.Function<Integer, Integer> handler = x -> x;

                public Foo() {
                    int created = 1;
                }

                public void doThing() {
                    int counter = 0;
                    Runnable localHandler = () -> {};
                }

                void hidden() {}

                public static class Nested {
                    int nestedField = 3;
                }
            }

            interface Bar {}
        "#;

        let extraction = extract(source);
        let references = extraction.references;

        let definitions: HashSet<_> = references
            .iter()
            .filter(|r| r.kind == Some("definition".to_string()))
            .map(|r| (r.name.as_str(), r.namespace.as_deref()))
            .collect();

        assert!(definitions.contains(&("com.example.demo", None)));
        assert!(definitions.contains(&("Foo", Some("com.example.demo"))));
        assert!(definitions.contains(&("value", Some("com.example.demo.Foo"))));
        assert!(definitions.contains(&("handler", Some("com.example.demo.Foo"))));
        assert!(definitions.contains(&("Foo", Some("com.example.demo.Foo")))); // Constructor
        assert!(definitions.contains(&("created", Some("com.example.demo.Foo.Foo"))));
        assert!(definitions.contains(&("doThing", Some("com.example.demo.Foo"))));
        assert!(definitions.contains(&("counter", Some("com.example.demo.Foo.doThing"))));
        assert!(definitions.contains(&("localHandler", Some("com.example.demo.Foo.doThing"))));
        assert!(definitions.contains(&("hidden", Some("com.example.demo.Foo"))));
        assert!(definitions.contains(&("Nested", Some("com.example.demo.Foo"))));
        assert!(definitions.contains(&("nestedField", Some("com.example.demo.Foo.Nested"))));
        assert!(definitions.contains(&("Bar", Some("com.example.demo"))));
    }
}
