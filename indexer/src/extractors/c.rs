use std::collections::HashSet;
use tree_sitter::{Node, Parser};

use super::{ExtractedReference, Extraction};

pub fn extract(source: &str) -> Extraction {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_c::LANGUAGE.into())
        .expect("failed to load tree-sitter C grammar");

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
        "translation_unit" => {
            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                collect_references(&child, source, references, namespace_stack, defined_nodes);
            }
            return;
        }
        "function_definition" => {
            if let Some(declarator) = node.child_by_field_name("declarator") {
                if let Some(name_node) = find_identifier_in_declarator(&declarator) {
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
        }
        "declaration" => {
            if has_function_declarator(node) {
                if let Some(declarator) = node
                    .children(&mut node.walk())
                    .find(|c| c.kind() == "function_declarator")
                {
                    if let Some(name_node) = find_identifier_in_declarator(&declarator) {
                        record_definition_node(
                            &name_node,
                            source,
                            references,
                            namespace_stack,
                            "declaration",
                            defined_nodes,
                        );
                    }
                }
            } else {
                let mut names = Vec::new();
                collect_declaration_identifiers(node, &mut names);
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
        }
        "type_definition" => {
            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                if child.kind() == "type_identifier" {
                    record_definition_node(
                        &child,
                        source,
                        references,
                        namespace_stack,
                        "definition",
                        defined_nodes,
                    );
                } else if child.kind() == "init_declarator" {
                    let mut names = Vec::new();
                    collect_c_binding_names(&child, &mut names);
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
            }
        }
        "struct_specifier" | "union_specifier" | "enum_specifier" => {
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
        "field_declaration" => {
            let mut names = Vec::new();
            collect_c_binding_names(node, &mut names);
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
        "enumerator" => {
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
        "parameter_declaration" => {
            if let Some(declarator) = node.child_by_field_name("declarator") {
                let mut names = Vec::new();
                collect_c_binding_names(&declarator, &mut names);
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
            } else if let Some(name_node) = node.child_by_field_name("name") {
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
        "identifier" | "type_identifier" | "field_identifier" => {
            record_reference_node(node, source, references, namespace_stack, defined_nodes);
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
                    namespace_stack,
                    "definition",
                    defined_nodes,
                );
            }
        }
        _ => {}
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        collect_references(&child, source, references, &next_namespace, defined_nodes);
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
            | "array_declarator"
            | "parenthesized_declarator"
            | "field_declarator"
            | "identifier" => collect_c_binding_names(&child, out),
            _ => {}
        }
    }
}

fn collect_c_binding_names<'a>(node: &Node<'a>, out: &mut Vec<Node<'a>>) {
    match node.kind() {
        "identifier" => out.push(*node),
        "field_identifier" => out.push(*node),
        "field_declarator" | "parameter_declaration" | "init_declarator" => {
            if let Some(child) = node.child_by_field_name("declarator") {
                collect_c_binding_names(&child, out);
            } else if let Some(name) = node.child_by_field_name("name") {
                collect_c_binding_names(&name, out);
            }
        }
        "pointer_declarator"
        | "function_declarator"
        | "array_declarator"
        | "parenthesized_declarator" => {
            if let Some(child) = node.child_by_field_name("declarator") {
                collect_c_binding_names(&child, out);
            } else {
                let mut cursor = node.walk();
                for child in node.children(&mut cursor) {
                    if child.is_named() && !is_type_context(&child) {
                        collect_c_binding_names(&child, out);
                    }
                }
            }
        }
        "parameter_list"
        | "field_declarator_list"
        | "argument_list"
        | "initializer_list"
        | "comma_expression"
        | "bitfield_clause" => {
            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                if child.is_named() && !is_type_context(&child) {
                    collect_c_binding_names(&child, out);
                }
            }
        }
        _ => {
            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                if child.is_named() && !is_type_context(&child) {
                    collect_c_binding_names(&child, out);
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
            | "struct_specifier"
            | "union_specifier"
            | "enum_specifier"
            | "sized_type_specifier"
            | "type_qualifier"
            | "storage_class_specifier"
    )
}

fn find_identifier_in_declarator<'a>(declarator: &Node<'a>) -> Option<Node<'a>> {
    match declarator.kind() {
        "identifier" => Some(*declarator),
        "pointer_declarator"
        | "function_declarator"
        | "array_declarator"
        | "parenthesized_declarator" => declarator
            .child_by_field_name("declarator")
            .and_then(|d| find_identifier_in_declarator(&d)),
        _ => None,
    }
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
    fn extracts_comprehensive_c_identifiers() {
        let source = r#"
            typedef int myint;

            struct Foo {
                int value;
                int (*on_ready)(int);
            };

            union Payload {
                int ival;
                float fval;
            };

            enum Kind {
                KindA,
                KindB,
            };

            static int counter = 0;

            int helper(int arg) {
                int local = arg + counter;
                return local;
            }

            int run(struct Foo foo) {
                myint result = helper(foo.value);
                return result;
            }
        "#;

        let extraction = extract(source);
        let references = extraction.references;
        let (definitions, references_map) = bucket_kinds(&references);

        let expected_definitions = HashSet::from([
            ("myint".to_string(), None),
            ("Foo".to_string(), None),
            ("value".to_string(), Some("Foo".to_string())),
            ("on_ready".to_string(), Some("Foo".to_string())),
            ("Payload".to_string(), None),
            ("ival".to_string(), Some("Payload".to_string())),
            ("fval".to_string(), Some("Payload".to_string())),
            ("Kind".to_string(), None),
            ("KindA".to_string(), Some("Kind".to_string())),
            ("KindB".to_string(), Some("Kind".to_string())),
            ("counter".to_string(), None),
            ("helper".to_string(), None),
            ("arg".to_string(), Some("helper".to_string())),
            ("local".to_string(), Some("helper".to_string())),
            ("run".to_string(), None),
            ("foo".to_string(), Some("run".to_string())),
            ("result".to_string(), Some("run".to_string())),
        ]);

        for key in &expected_definitions {
            assert!(
                definitions.contains_key(key),
                "missing definition for {:?}",
                key
            );
        }

        let expected_references = HashSet::from([
            ("counter".to_string(), Some("helper".to_string())),
            ("arg".to_string(), Some("helper".to_string())),
            ("helper".to_string(), Some("run".to_string())),
            ("foo".to_string(), Some("run".to_string())),
            ("value".to_string(), Some("run".to_string())),
            ("result".to_string(), Some("run".to_string())),
            ("myint".to_string(), Some("run".to_string())),
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
