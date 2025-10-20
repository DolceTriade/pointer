use tree_sitter::{Node, Parser};

use super::{ExtractedReference, Extraction};

pub fn extract(source: &str) -> Extraction {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_proto::LANGUAGE.into())
        .expect("failed to load tree-sitter Protobuf grammar");

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
        "message" | "enum" | "service" | "rpc" => {
            if let Some(name_node) = node_name_node(node) {
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
        "package" => {
            if let Some(name_node) = node.child_by_field_name("name") {
                if let Ok(name) = name_node.utf8_text(source) {
                    new_namespace_stack.push(name.to_string());
                }
            }
        }
        "identifier" | "field_identifier" => {
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

fn node_name_node<'a>(node: &Node<'a>) -> Option<Node<'a>> {
    for field in [
        "name",
        "message_name",
        "enum_name",
        "service_name",
        "rpc_name",
    ] {
        if let Some(child) = node.child_by_field_name(field) {
            return Some(child);
        }
    }
    node.named_child(0)
}

fn is_part_of_definition_or_declaration(node: &Node) -> bool {
    let mut current = node.parent();
    while let Some(parent) = current {
        match parent.kind() {
            "message" | "enum" | "service" | "rpc" | "package" => {
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
    fn extracts_proto_symbols() {
        let source = r#"
            syntax = "proto3";

            package demo.api;

            message Foo {
                message Nested {}
            }

            enum Status {
                STATUS_UNKNOWN = 0;
            }

            service Demo {
                rpc Run (Foo) returns (Foo);
            }
        "#;

        let extraction = extract(source);
        let references = extraction.references;

        let definitions: HashSet<_> = references
            .iter()
            .filter(|r| r.kind == Some("definition".to_string()))
            .map(|r| (r.name.as_str(), r.namespace.as_deref()))
            .collect();

        assert!(definitions.contains(&("Foo", Some("demo.api"))));
        assert!(definitions.contains(&("Nested", Some("demo.api.Foo"))));
        assert!(definitions.contains(&("Status", Some("demo.api"))));
        assert!(definitions.contains(&("Demo", Some("demo.api"))));
        assert!(definitions.contains(&("Run", Some("demo.api.Demo"))));
    }
}
