use std::collections::HashSet;
use tree_sitter::{Node, Parser};

use super::{ExtractedReference, Extraction};

pub fn extract(source: &str) -> Extraction {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_objc::LANGUAGE.into())
        .expect("failed to load tree-sitter Objective-C grammar");

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
        "class_interface"
        | "class_implementation"
        | "category_interface"
        | "category_implementation"
        | "protocol_declaration" => {
            let mut name_cursor = node.walk();
            let name_node = node.child_by_field_name("name").or_else(|| {
                node.children(&mut name_cursor)
                    .find(|n| n.is_named() && n.kind() == "identifier")
            });
            if let Some(name_node) = name_node {
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
        "method_definition" | "method_declaration" => {
            let mut name_cursor = node.walk();
            let selector = node.child_by_field_name("selector").or_else(|| {
                node.children(&mut name_cursor)
                    .find(|n| n.is_named() && n.kind() == "identifier")
            });
            if let Some(selector) = selector {
                if let Some(name) = record_definition_node(
                    &selector,
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
        "instance_variable" => {
            let mut names = Vec::new();
            collect_declarator_identifiers(node, &mut names);
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
        "property_declaration" => {
            let mut names = Vec::new();
            collect_declarator_identifiers(node, &mut names);
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
        "ivar_declaration" | "declaration" => {
            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                if matches!(
                    child.kind(),
                    "struct_declarator" | "init_declarator" | "declarator" | "identifier"
                ) {
                    let mut names = Vec::new();
                    collect_declarator_identifiers(&child, &mut names);
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
        "parameter" | "parameter_declaration" | "pointer_declarator" => {
            let mut names = Vec::new();
            collect_declarator_identifiers(node, &mut names);
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
        "identifier" | "field_identifier" | "class_identifier" => {
            record_reference_node(node, source, references, namespace_stack, defined_nodes);
        }
        _ => {}
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        collect_references(&child, source, references, &next_namespace, defined_nodes);
    }
}

fn collect_declarator_identifiers<'a>(node: &Node<'a>, out: &mut Vec<Node<'a>>) {
    match node.kind() {
        "identifier" | "field_identifier" | "class_identifier" => out.push(*node),
        "pointer_declarator"
        | "function_declarator"
        | "array_declarator"
        | "parenthesized_declarator"
        | "struct_declarator"
        | "init_declarator" => {
            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                if child.is_named() {
                    collect_declarator_identifiers(&child, out);
                }
            }
        }
        "parameter_declaration" | "block_declarator" | "parameter" => {
            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                if child.is_named() {
                    collect_declarator_identifiers(&child, out);
                }
            }
        }
        _ => {
            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                if child.is_named() {
                    collect_declarator_identifiers(&child, out);
                }
            }
        }
    }
}

fn find_identifier_in_declarator<'a>(node: &Node<'a>) -> Option<Node<'a>> {
    match node.kind() {
        "identifier" | "field_identifier" | "class_identifier" => Some(*node),
        "pointer_declarator"
        | "function_declarator"
        | "array_declarator"
        | "parenthesized_declarator"
        | "struct_declarator"
        | "init_declarator" => {
            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                if let Some(found) = find_identifier_in_declarator(&child) {
                    return Some(found);
                }
            }
            None
        }
        _ => None,
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
    fn extracts_objective_c_identifiers() {
        let source = r#"
            int solovar;
            static int (*global_handler)(int);
            @interface Demo : NSObject {
                int _count;
                int (*callback)(int);
            }
            @property(nonatomic) int value;
            @property(nonatomic) int (*onReady)(int);
            - (void)doThing;
            @end

            @implementation Demo
            - (void)doThing {
                int local = 0;
                int temp;
                int (*local_handler)(int);
            }
            @end

            void Helper(void) {
                int global = 1;
                int global_no_init;
                int (*helper_callback)(int);
            }
        "#;

        let extraction = extract(source);
        let references = extraction.references;
        let (definitions, _) = bucket_kinds(&references);

        let expected_definitions = HashSet::from([
            ("solovar".to_string(), None),
            ("global_handler".to_string(), None),
            ("Demo".to_string(), None),
            ("_count".to_string(), Some("Demo".to_string())),
            ("callback".to_string(), Some("Demo".to_string())),
            ("value".to_string(), Some("Demo".to_string())),
            ("onReady".to_string(), Some("Demo".to_string())),
            ("doThing".to_string(), Some("Demo".to_string())),
            ("local".to_string(), Some("Demo.doThing".to_string())),
            ("temp".to_string(), Some("Demo.doThing".to_string())),
            (
                "local_handler".to_string(),
                Some("Demo.doThing".to_string()),
            ),
            ("Helper".to_string(), None),
            ("global".to_string(), Some("Helper".to_string())),
            ("global_no_init".to_string(), Some("Helper".to_string())),
            ("helper_callback".to_string(), Some("Helper".to_string())),
        ]);

        for key in &expected_definitions {
            assert!(
                definitions.contains_key(key),
                "missing definition for {:?}",
                key
            );
        }
    }
}
