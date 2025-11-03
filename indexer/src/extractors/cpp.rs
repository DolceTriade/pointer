use std::collections::HashSet;
use tree_sitter::{Node, Parser};

use super::{ExtractedReference, Extraction};

pub fn extract(source: &str) -> Extraction {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_cpp::LANGUAGE.into())
        .expect("failed to load tree-sitter C++ grammar");

    let tree = match parser.parse(source, None) {
        Some(tree) => tree,
        None => return Extraction::default(),
    };

    let mut references = Vec::new();
    let source_bytes = source.as_bytes();
    collect_references(&tree.root_node(), source_bytes, &mut references);

    references.into()
}

fn collect_references(root: &Node, source: &[u8], references: &mut Vec<ExtractedReference>) {
    let mut defined_nodes = HashSet::new();
    let mut stack: Vec<(Node, Vec<String>)> = Vec::new();
    stack.push((*root, Vec::new()));

    while let Some((node, namespace_stack)) = stack.pop() {
        let mut next_namespace = namespace_stack.clone();

        match node.kind() {
            "translation_unit" => {
                let mut cursor = node.walk();
                let children: Vec<Node> = node.children(&mut cursor).collect();
                for child in children.into_iter().rev() {
                    stack.push((child, namespace_stack.clone()));
                }
                continue;
            }
            "namespace_definition" => {
                if let Some(name_node) = node.child_by_field_name("name") {
                    if let Some(name) = record_definition_node(
                        &name_node,
                        source,
                        references,
                        &namespace_stack,
                        "definition",
                        &mut defined_nodes,
                    ) {
                        next_namespace = push_namespace(&namespace_stack, &name);
                    }
                }
            }
            "class_specifier" | "struct_specifier" | "enum_specifier" => {
                if let Some(name_node) = node.child_by_field_name("name") {
                    if let Some(name) = record_definition_node(
                        &name_node,
                        source,
                        references,
                        &namespace_stack,
                        "definition",
                        &mut defined_nodes,
                    ) {
                        next_namespace = push_namespace(&namespace_stack, &name);
                    }
                }
            }
            "type_alias_declaration" | "alias_declaration" => {
                if let Some(name_node) = node.child_by_field_name("name") {
                    record_definition_node(
                        &name_node,
                        source,
                        references,
                        &namespace_stack,
                        "definition",
                        &mut defined_nodes,
                    );
                }
            }
            "function_definition" => {
                if let Some(declarator) = node.child_by_field_name("declarator") {
                    if let Some(name_node) = find_identifier_in_declarator(&declarator) {
                        if let Some(name) = record_definition_node(
                            &name_node,
                            source,
                            references,
                            &namespace_stack,
                            "definition",
                            &mut defined_nodes,
                        ) {
                            next_namespace = push_namespace(&namespace_stack, &name);
                        }
                    }
                }
            }
            "declaration" | "simple_declaration" => {
                if has_function_declarator(&node) {
                    if let Some(declarator) = node
                        .children(&mut node.walk())
                        .find(|c| c.kind() == "function_declarator")
                    {
                        if let Some(name_node) = find_identifier_in_declarator(&declarator) {
                            record_definition_node(
                                &name_node,
                                source,
                                references,
                                &namespace_stack,
                                "declaration",
                                &mut defined_nodes,
                            );
                        }
                    }
                } else {
                    let mut names = Vec::new();
                    collect_declaration_identifiers(&node, &mut names);
                    for identifier in names {
                        record_definition_node(
                            &identifier,
                            source,
                            references,
                            &namespace_stack,
                            "definition",
                            &mut defined_nodes,
                        );
                    }
                }
            }
            "field_declaration" => {
                let mut names = Vec::new();
                collect_cpp_binding_names(&node, &mut names);
                for identifier in names {
                    record_definition_node(
                        &identifier,
                        source,
                        references,
                        &namespace_stack,
                        "definition",
                        &mut defined_nodes,
                    );
                }
            }
            "enumerator" => {
                if let Some(name_node) = node.child_by_field_name("name") {
                    record_definition_node(
                        &name_node,
                        source,
                        references,
                        &namespace_stack,
                        "definition",
                        &mut defined_nodes,
                    );
                }
            }
            "parameter_declaration" => {
                if let Some(declarator) = node.child_by_field_name("declarator") {
                    let mut names = Vec::new();
                    collect_cpp_binding_names(&declarator, &mut names);
                    for identifier in names {
                        record_definition_node(
                            &identifier,
                            source,
                            references,
                            &namespace_stack,
                            "definition",
                            &mut defined_nodes,
                        );
                    }
                } else if let Some(name_node) = node.child_by_field_name("name") {
                    record_definition_node(
                        &name_node,
                        source,
                        references,
                        &namespace_stack,
                        "definition",
                        &mut defined_nodes,
                    );
                }
            }
            "identifier" | "field_identifier" | "type_identifier" | "scoped_identifier" => {
                record_reference_node(&node, source, references, &namespace_stack, &defined_nodes);
            }
            "preproc_def" | "preproc_function_def" => {
                if let Some(name_node) = node
                    .child_by_field_name("name")
                    .or_else(|| node.named_child(0))
                {
                    record_definition_node(
                        &name_node,
                        source,
                        references,
                        &namespace_stack,
                        "definition",
                        &mut defined_nodes,
                    );
                }
            }
            _ => {}
        }
        let mut cursor = node.walk();
        let children: Vec<Node> = node.children(&mut cursor).collect();
        for child in children.into_iter().rev() {
            stack.push((child, next_namespace.clone()));
        }
    }
}

fn collect_declaration_identifiers<'a>(node: &Node<'a>, out: &mut Vec<Node<'a>>) {
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        match child.kind() {
            "init_declarator"
            | "init_declarator_list"
            | "function_declarator"
            | "pointer_declarator"
            | "reference_declarator"
            | "array_declarator"
            | "parenthesized_declarator"
            | "field_declarator"
            | "identifier" => collect_cpp_binding_names(&child, out),
            _ => {}
        }
    }
}

fn collect_cpp_binding_names<'a>(node: &Node<'a>, out: &mut Vec<Node<'a>>) {
    let mut stack = vec![*node];

    while let Some(current_node) = stack.pop() {
        match current_node.kind() {
            "identifier" | "field_identifier" => out.push(current_node),
            "init_declarator"
            | "field_declarator"
            | "parameter_declaration"
            | "reference_declarator" => {
                if let Some(child) = current_node.child_by_field_name("declarator") {
                    stack.push(child);
                } else if let Some(name) = current_node.child_by_field_name("name") {
                    stack.push(name);
                }
            }
            "pointer_declarator"
            | "function_declarator"
            | "array_declarator"
            | "parenthesized_declarator"
            | "qualified_identifier" => {
                if let Some(child) = current_node.child_by_field_name("declarator") {
                    stack.push(child);
                } else {
                    let mut cursor = current_node.walk();
                    for child in current_node.children(&mut cursor) {
                        if child.is_named() && !is_type_context(&child) {
                            stack.push(child);
                        }
                    }
                }
            }
            "parameter_list"
            | "field_declarator_list"
            | "initializer_list"
            | "argument_list"
            | "comma_expression"
            | "template_parameter_list" => {
                let mut cursor = current_node.walk();
                for child in current_node.children(&mut cursor) {
                    if child.is_named() && !is_type_context(&child) {
                        stack.push(child);
                    }
                }
            }
            _ => {
                let mut cursor = current_node.walk();
                for child in current_node.children(&mut cursor) {
                    if child.is_named() && !is_type_context(&child) {
                        stack.push(child);
                    }
                }
            }
        }
    }
}

fn is_type_context(node: &Node) -> bool {
    matches!(
        node.kind(),
        "primitive_type"
            | "type_identifier"
            | "scoped_identifier"
            | "qualified_identifier"
            | "struct_specifier"
            | "class_specifier"
            | "enum_specifier"
            | "union_specifier"
            | "type_qualifier"
            | "storage_class_specifier"
            | "template_argument_list"
    )
}

fn find_identifier_in_declarator<'a>(declarator: &Node<'a>) -> Option<Node<'a>> {
    let mut stack = vec![*declarator];

    while let Some(current) = stack.pop() {
        match current.kind() {
            "identifier" | "type_identifier" | "field_identifier" | "scoped_identifier" => {
                return Some(current);
            }
            "pointer_declarator"
            | "function_declarator"
            | "array_declarator"
            | "parenthesized_declarator"
            | "qualified_identifier"
            | "reference_declarator" => {
                if let Some(child) = current.child_by_field_name("declarator") {
                    stack.push(child);
                } else {
                    let mut cursor = current.walk();
                    for child in current.children(&mut cursor) {
                        stack.push(child);
                    }
                }
            }
            _ => {}
        }
    }

    None
}

fn has_function_declarator(node: &Node) -> bool {
    let mut cursor = node.walk();
    node.children(&mut cursor)
        .any(|child| child.kind() == "function_declarator")
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
        Some(namespace_stack.join("::"))
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
    fn extracts_comprehensive_cpp_identifiers() {
        let source = r#"
            namespace demo {
                class Widget {
                public:
                    Widget(int value);
                    int compute(int delta) {
                        int local = delta + value_;
                        counter += delta;
                        return local + counter;
                    }

                    static int counter;
                    int value_;
                };

                enum class Mode {
                    Off,
                    On,
                };

                using Alias = Widget;
            }

            int demo::Widget::counter = 0;

            int helper(int arg) {
                demo::Widget widget(arg);
                return widget.compute(arg);
            }
        "#;

        let extraction = extract(source);
        let references = extraction.references;
        let (definitions, references_map) = bucket_kinds(&references);

        let expected_definitions = HashSet::from([
            ("demo".to_string(), None),
            ("Widget".to_string(), Some("demo".to_string())),
            ("Widget".to_string(), Some("demo::Widget".to_string())),
            ("compute".to_string(), Some("demo::Widget".to_string())),
            (
                "local".to_string(),
                Some("demo::Widget::compute".to_string()),
            ),
            ("counter".to_string(), Some("demo::Widget".to_string())),
            ("value_".to_string(), Some("demo::Widget".to_string())),
            ("Mode".to_string(), Some("demo".to_string())),
            ("Off".to_string(), Some("demo::Mode".to_string())),
            ("On".to_string(), Some("demo::Mode".to_string())),
            ("Alias".to_string(), Some("demo".to_string())),
            ("counter".to_string(), Some("demo::Widget".to_string())),
            ("helper".to_string(), None),
            ("arg".to_string(), Some("helper".to_string())),
            ("widget".to_string(), Some("helper".to_string())),
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
                "value_".to_string(),
                Some("demo::Widget::compute".to_string()),
            ),
            (
                "counter".to_string(),
                Some("demo::Widget::compute".to_string()),
            ),
            (
                "delta".to_string(),
                Some("demo::Widget::compute".to_string()),
            ),
            ("Widget".to_string(), Some("helper".to_string())),
            ("compute".to_string(), Some("helper".to_string())),
            ("arg".to_string(), Some("helper".to_string())),
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
