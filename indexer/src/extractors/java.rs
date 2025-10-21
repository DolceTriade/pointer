use std::collections::HashSet;
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
        "program" => {
            let mut cursor = node.walk();
            let mut children = Vec::new();
            for child in node.children(&mut cursor) {
                children.push(child);
            }

            let mut base_namespace = namespace_stack.to_vec();
            if let Some(package_node) = children
                .iter()
                .find(|child| child.kind() == "package_declaration")
            {
                let mut name_cursor = package_node.walk();
                let name_node = package_node.child_by_field_name("name").or_else(|| {
                    package_node
                        .children(&mut name_cursor)
                        .find(|n| n.is_named() && n.kind() == "scoped_identifier")
                });
                if let Some(name_node) = name_node {
                    if let Ok(name_text) = name_node.utf8_text(source) {
                        record_definition_node(
                            &name_node,
                            source,
                            references,
                            namespace_stack,
                            "definition",
                            defined_nodes,
                        );
                        base_namespace.extend(name_text.split('.').map(|s| s.to_string()));
                    }
                }
            }

            for child in children {
                if child.kind() == "package_declaration" {
                    continue;
                }
                collect_references(&child, source, references, &base_namespace, defined_nodes);
            }
            return;
        }
        "class_declaration"
        | "interface_declaration"
        | "enum_declaration"
        | "record_declaration"
        | "annotation_type_declaration" => {
            if let Some(name_node) = node.child_by_field_name("name") {
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
        "constructor_declaration" | "method_declaration" => {
            if let Some(name_node) = node.child_by_field_name("name") {
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
        "field_declaration" | "local_variable_declaration" => {
            let mut names = Vec::new();
            collect_java_variable_names(node, &mut names);
            for identifier in names {
                record_definition_node(
                    &identifier,
                    source,
                    references,
                    namespace_stack,
                    "definition",
                    defined_nodes,
                );
            }
        }
        "catch_formal_parameter" => {
            if let Some(name_node) = node.child_by_field_name("name") {
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
        "enhanced_for_statement" => {
            if let Some(variable) = node.child_by_field_name("variable") {
                let mut names = Vec::new();
                collect_java_variable_names(&variable, &mut names);
                for identifier in names {
                    record_definition_node(
                        &identifier,
                        source,
                        references,
                        namespace_stack,
                        "definition",
                        defined_nodes,
                    );
                }
            }
            if let Some(name_node) = node.child_by_field_name("name") {
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
        "formal_parameter" | "spread_parameter" => {
            if let Some(name_node) = node.child_by_field_name("name") {
                record_definition_node(
                    &name_node,
                    source,
                    references,
                    namespace_stack,
                    "definition",
                    defined_nodes,
                );
            } else {
                let mut cursor = node.walk();
                for child in node.children(&mut cursor) {
                    if child.kind() == "identifier" {
                        record_definition_node(
                            &child,
                            source,
                            references,
                            namespace_stack,
                            "definition",
                            defined_nodes,
                        );
                    }
                }
            }
        }
        "enum_constant" => {
            if let Some(name_node) = node.child_by_field_name("name") {
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
        "identifier" => {
            record_reference_node(node, source, references, namespace_stack, defined_nodes);
        }
        _ => {}
    }

    walk_children(node, source, references, &next_namespace, defined_nodes);
}

fn walk_children(
    node: &Node,
    source: &[u8],
    references: &mut Vec<ExtractedReference>,
    namespace_stack: &[String],
    defined_nodes: &mut HashSet<usize>,
) {
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        collect_references(&child, source, references, namespace_stack, defined_nodes);
    }
}

fn collect_java_variable_names<'a>(node: &Node<'a>, out: &mut Vec<Node<'a>>) {
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        match child.kind() {
            "variable_declarator" | "variable_declarator_id" => {
                collect_java_variable_names(&child, out);
            }
            "identifier" => out.push(child),
            "parenthesized_expression" | "array_creation_expression" | "array_declarator" => {
                collect_java_variable_names(&child, out);
            }
            _ => {}
        }
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
    fn extracts_comprehensive_java_identifiers() {
        let source = r#"
            package com.example.demo;

            import java.util.List;

            public class Widget {
                private static int counter = 0;
                private final int value;

                public Widget(int value) {
                    this.value = value;
                }

                public int compute(int delta) {
                    int local = delta + value;
                    counter += delta;
                    return local;
                }

                private void consume(List<String> items) {
                    for (String item : items) {
                        System.out.println(item);
                    }
                }

                enum Mode {
                    OFF,
                    ON
                }
            }

            record RecordWidget(int value) {}
        "#;

        let extraction = extract(source);
        let references = extraction.references;
        let (definitions, references_map) = bucket_kinds(&references);

        let expected_definitions = HashSet::from([
            ("com.example.demo".to_string(), None),
            ("Widget".to_string(), Some("com.example.demo".to_string())),
            (
                "counter".to_string(),
                Some("com.example.demo.Widget".to_string()),
            ),
            (
                "value".to_string(),
                Some("com.example.demo.Widget".to_string()),
            ),
            (
                "Widget".to_string(),
                Some("com.example.demo.Widget".to_string()),
            ),
            (
                "value".to_string(),
                Some("com.example.demo.Widget.Widget".to_string()),
            ),
            (
                "compute".to_string(),
                Some("com.example.demo.Widget".to_string()),
            ),
            (
                "delta".to_string(),
                Some("com.example.demo.Widget.compute".to_string()),
            ),
            (
                "local".to_string(),
                Some("com.example.demo.Widget.compute".to_string()),
            ),
            (
                "consume".to_string(),
                Some("com.example.demo.Widget".to_string()),
            ),
            (
                "items".to_string(),
                Some("com.example.demo.Widget.consume".to_string()),
            ),
            (
                "item".to_string(),
                Some("com.example.demo.Widget.consume".to_string()),
            ),
            (
                "Mode".to_string(),
                Some("com.example.demo.Widget".to_string()),
            ),
            (
                "OFF".to_string(),
                Some("com.example.demo.Widget.Mode".to_string()),
            ),
            (
                "ON".to_string(),
                Some("com.example.demo.Widget.Mode".to_string()),
            ),
            (
                "RecordWidget".to_string(),
                Some("com.example.demo".to_string()),
            ),
            (
                "value".to_string(),
                Some("com.example.demo.RecordWidget".to_string()),
            ),
        ]);

        for key in &expected_definitions {
            assert!(
                definitions.contains_key(key),
                "missing definition for {:?}",
                key
            );
        }

        let expected_references = HashSet::from([
            (
                "value".to_string(),
                Some("com.example.demo.Widget.compute".to_string()),
            ),
            (
                "counter".to_string(),
                Some("com.example.demo.Widget.compute".to_string()),
            ),
            (
                "delta".to_string(),
                Some("com.example.demo.Widget.compute".to_string()),
            ),
            (
                "System".to_string(),
                Some("com.example.demo.Widget.consume".to_string()),
            ),
            (
                "out".to_string(),
                Some("com.example.demo.Widget.consume".to_string()),
            ),
            (
                "println".to_string(),
                Some("com.example.demo.Widget.consume".to_string()),
            ),
            (
                "items".to_string(),
                Some("com.example.demo.Widget.consume".to_string()),
            ),
            (
                "item".to_string(),
                Some("com.example.demo.Widget.consume".to_string()),
            ),
        ]);

        for key in &expected_references {
            assert!(
                references_map.contains_key(key),
                "missing reference for {:?}",
                key
            );
        }
    }
}
