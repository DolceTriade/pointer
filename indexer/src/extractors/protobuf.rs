use std::collections::HashSet;
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
    let mut defined_nodes = HashSet::new();
    collect_references(
        &tree.root_node(),
        source_bytes,
        &mut references,
        &[],
        &mut defined_nodes,
    );

    references.into()
}

fn collect_references(
    node: &Node,
    source: &[u8],
    references: &mut Vec<ExtractedReference>,
    namespace_stack: &[String],
    defined_nodes: &mut HashSet<usize>,
) {
    let mut next_namespace = namespace_stack.to_vec();

    match node.kind() {
        "translation_unit" | "source_file" => {
            let mut cursor = node.walk();
            let mut children = Vec::new();
            for child in node.children(&mut cursor) {
                children.push(child);
            }

            let mut base_namespace = namespace_stack.to_vec();
            if let Some(pkg) = children.iter().find(|child| child.kind() == "package") {
                let name_node = pkg
                    .child_by_field_name("name")
                    .or_else(|| pkg.child_by_field_name("full_ident"))
                    .or_else(|| pkg.named_child(0));
                if let Some(name_node) = name_node {
                    if let Ok(name) = name_node.utf8_text(source) {
                        record_definition_node(
                            &name_node,
                            source,
                            references,
                            namespace_stack,
                            "definition",
                            defined_nodes,
                        );
                        base_namespace.push(name.to_string());
                    }
                }
            }

            for child in children {
                if child.kind() == "package" {
                    continue;
                }
                collect_references(&child, source, references, &base_namespace, defined_nodes);
            }
            return;
        }
        "message" | "enum" | "service" => {
            if let Some(name_node) =
                find_name_node(node, &["name", "message_name", "enum_name", "service_name"])
            {
                if let Some(name) = record_definition_node(
                    &name_node,
                    source,
                    references,
                    namespace_stack,
                    "definition",
                    defined_nodes,
                ) {
                    next_namespace = push_namespace(namespace_stack, &name);
                }
            }
        }
        "rpc" => {
            if let Some(name_node) = find_name_node(node, &["name", "rpc_name"]) {
                record_definition_node(
                    &name_node,
                    source,
                    references,
                    namespace_stack,
                    "definition",
                    defined_nodes,
                );
            }
        }
        "identifier" | "type_identifier" => {
            record_reference_node(node, source, references, namespace_stack, defined_nodes);
        }
        _ => {}
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        collect_references(&child, source, references, &next_namespace, defined_nodes);
    }
}

fn push_namespace(namespace_stack: &[String], segment: &str) -> Vec<String> {
    let mut next = namespace_stack.to_vec();
    next.push(segment.to_string());
    next
}

fn namespace_from_stack(namespace_stack: &[String]) -> Option<String> {
    if namespace_stack.is_empty() {
        None
    } else {
        Some(namespace_stack.join("."))
    }
}

fn record_definition_node(
    node: &Node,
    source: &[u8],
    references: &mut Vec<ExtractedReference>,
    namespace_stack: &[String],
    kind: &str,
    defined_nodes: &mut HashSet<usize>,
) -> Option<String> {
    if defined_nodes.contains(&node.id()) {
        return None;
    }

    if let Ok(raw) = node.utf8_text(source) {
        let name = raw.trim();
        if !name.is_empty() {
            let pos = node.start_position();
            references.push(ExtractedReference {
                name: name.to_string(),
                kind: Some(kind.to_string()),
                namespace: namespace_from_stack(namespace_stack),
                line: pos.row + 1,
                column: pos.column + 1,
            });
            defined_nodes.insert(node.id());
            return Some(name.to_string());
        }
    }
    None
}

fn record_reference_node(
    node: &Node,
    source: &[u8],
    references: &mut Vec<ExtractedReference>,
    namespace_stack: &[String],
    defined_nodes: &HashSet<usize>,
) {
    if defined_nodes.contains(&node.id()) {
        return;
    }

    if let Ok(raw) = node.utf8_text(source) {
        let name = raw.trim();
        if !name.is_empty() {
            let pos = node.start_position();
            references.push(ExtractedReference {
                name: name.to_string(),
                kind: Some("reference".to_string()),
                namespace: namespace_from_stack(namespace_stack),
                line: pos.row + 1,
                column: pos.column + 1,
            });
        }
    }
}

fn find_name_node<'a>(node: &Node<'a>, fields: &[&str]) -> Option<Node<'a>> {
    for field in fields {
        if let Some(child) = node.child_by_field_name(field) {
            if let Some(identifier) = extract_identifier(&child) {
                return Some(identifier);
            }
        }
    }
    extract_identifier(node)
}

fn extract_identifier<'a>(node: &Node<'a>) -> Option<Node<'a>> {
    match node.kind() {
        "identifier" | "type_identifier" => Some(*node),
        _ => {
            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                if let Some(id) = extract_identifier(&child) {
                    return Some(id);
                }
            }
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};

    fn bucket_kinds(
        references: &[ExtractedReference],
    ) -> (
        HashMap<(String, Option<String>), usize>,
        HashMap<(String, Option<String>), usize>,
    ) {
        let mut definitions = HashMap::new();
        let mut references_map = HashMap::new();
        for reference in references {
            let key = (reference.name.clone(), reference.namespace.clone());
            match reference.kind.as_deref() {
                Some("definition") | Some("declaration") => {
                    *definitions.entry(key).or_insert(0) += 1;
                }
                Some("reference") => {
                    *references_map.entry(key).or_insert(0) += 1;
                }
                other => panic!("unexpected kind: {:?}", other),
            }
        }
        (definitions, references_map)
    }

    #[test]
    fn extracts_proto_identifiers() {
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
        let (definitions, references_map) = bucket_kinds(&references);

        let expected_definitions = HashSet::from([
            ("demo.api".to_string(), None),
            ("Foo".to_string(), Some("demo.api".to_string())),
            ("Nested".to_string(), Some("demo.api.Foo".to_string())),
            ("Status".to_string(), Some("demo.api".to_string())),
            ("Demo".to_string(), Some("demo.api".to_string())),
            ("Run".to_string(), Some("demo.api.Demo".to_string())),
        ]);

        for key in &expected_definitions {
            assert!(
                definitions.contains_key(key),
                "missing definition for {:?}",
                key
            );
        }

        assert!(
            references_map.contains_key(&("Foo".to_string(), Some("demo.api.Demo".to_string())))
        );
    }
}
