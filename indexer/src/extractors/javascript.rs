use std::collections::HashSet;
use tree_sitter::{Node, Parser};

use super::{ExtractedReference, Extraction};

pub fn extract(source: &str) -> Extraction {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_typescript::LANGUAGE_TSX.into())
        .expect("failed to load tree-sitter JavaScript grammar");

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
        "program" | "source_file" => {
            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                collect_references(&child, source, references, namespace_stack, defined_nodes);
            }
            return;
        }
        "function_declaration" | "generator_function_declaration" => {
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
        "class_declaration" => {
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
        "method_definition" | "method_signature" => {
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
        "lexical_declaration" | "variable_declaration" => {
            for declarator in node.children(&mut node.walk()) {
                if declarator.kind() == "variable_declarator" {
                    let mut names = Vec::new();
                    collect_binding_names(&declarator, &mut names);
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
        "assignment_expression" => {
            if let Some(left) = node.child_by_field_name("left") {
                let mut names = Vec::new();
                collect_binding_names(&left, &mut names);
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
        "object_pattern" | "array_pattern" | "pair_pattern" | "parenthesized_pattern" => {
            let mut names = Vec::new();
            collect_binding_names(node, &mut names);
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
        "public_field_definition" | "property_definition" => {
            if let Some(name_node) = node.child_by_field_name("property") {
                record_definition_node(
                    &name_node,
                    source,
                    references,
                    namespace_stack,
                    "definition",
                    defined_nodes,
                );
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
        "required_parameter" | "optional_parameter" | "rest_parameter" => {
            if let Some(pattern) = node.child_by_field_name("pattern") {
                let mut names = Vec::new();
                collect_binding_names(&pattern, &mut names);
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
            } else {
                let mut names = Vec::new();
                collect_binding_names(node, &mut names);
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
        "identifier" | "property_identifier" | "shorthand_property_identifier" | "this" => {
            record_reference_node(node, source, references, namespace_stack, defined_nodes);
        }
        _ => {}
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        collect_references(&child, source, references, &next_namespace, defined_nodes);
    }
}

fn collect_binding_names<'a>(node: &Node<'a>, out: &mut Vec<Node<'a>>) {
    match node.kind() {
        "identifier" | "property_identifier" | "shorthand_property_identifier" => {
            out.push(*node);
        }
        "variable_declarator" => {
            if let Some(name) = node.child_by_field_name("name") {
                collect_binding_names(&name, out);
            }
        }
        "assignment_pattern"
        | "pattern"
        | "object_pattern"
        | "array_pattern"
        | "pair_pattern"
        | "parenthesized_pattern"
        | "object_assignment_pattern"
        | "array_assignment_pattern" => {
            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                if child.is_named() {
                    collect_binding_names(&child, out);
                }
            }
        }
        _ => {
            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                if child.is_named() {
                    collect_binding_names(&child, out);
                }
            }
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
    fn extracts_comprehensive_javascript_identifiers() {
        let source = r#"
            export function foo(value) {
                const local = value + 1;
                return local;
            }

            class Widget {
                count = 0;
                method(delta) {
                    let result = this.count + delta;
                    this.count = result;
                    return result;
                }
                static helper() {
                    return Widget;
                }
            }

            const [a, b] = [1, 2];
            ({ x: alias } = obj);
        "#;

        let extraction = extract(source);
        let references = extraction.references;
        let (definitions, references_map) = bucket_kinds(&references);

        let expected_definitions = HashSet::from([
            ("foo".to_string(), None),
            ("value".to_string(), Some("foo".to_string())),
            ("local".to_string(), Some("foo".to_string())),
            ("Widget".to_string(), None),
            ("count".to_string(), Some("Widget".to_string())),
            ("method".to_string(), Some("Widget".to_string())),
            ("delta".to_string(), Some("Widget.method".to_string())),
            ("result".to_string(), Some("Widget.method".to_string())),
            ("helper".to_string(), Some("Widget".to_string())),
            ("a".to_string(), None),
            ("b".to_string(), None),
            ("alias".to_string(), None),
        ]);

        for key in &expected_definitions {
            assert!(
                definitions.contains_key(key),
                "missing definition for {:?}",
                key
            );
        }

        let expected_references = HashSet::from([
            ("value".to_string(), Some("foo".to_string())),
            ("local".to_string(), Some("foo".to_string())),
            ("this".to_string(), Some("Widget.method".to_string())),
            ("count".to_string(), Some("Widget.method".to_string())),
            ("delta".to_string(), Some("Widget.method".to_string())),
            ("result".to_string(), Some("Widget.method".to_string())),
            ("Widget".to_string(), Some("Widget.helper".to_string())),
            ("obj".to_string(), None),
        ]);

        for key in &expected_references {
            assert!(
                references_map.contains_key(key),
                "missing reference for {:?}",
                key
            );
        }
    }

    #[test]
    fn extracts_jsx_expression_references() {
        let source = r#"
            const items = [
              <element key={'something'}>
                value
              </element>,
              <another
                ternery={foo.is('something') ? 'one' : undefined}
                click={() => {
                   doSoemthing();
                 }}
                 literal={`soemthing-${interpolation}`}
                >
                 {thing.value}
                </another>,
            ];
        "#;

        let extraction = extract(source);
        let references = extraction.references;

        let definitions: HashSet<_> = references
            .iter()
            .filter(|r| r.kind == Some("definition".to_string()))
            .map(|r| r.name.as_str())
            .collect();

        let refs: HashSet<_> = references
            .iter()
            .filter(|r| r.kind == Some("reference".to_string()))
            .map(|r| r.name.as_str())
            .collect();

        assert!(definitions.contains("items"));
        assert!(refs.contains("foo"));
        assert!(refs.contains("is"));
        assert!(refs.contains("doSoemthing"));
        assert!(refs.contains("interpolation"));
        assert!(refs.contains("thing"));
        assert!(refs.contains("value"));
    }

    #[test]
    fn extracts_jsx_array_enum_references() {
        let source = r#"
            const items = [
              <element
                args={[
                   Enum.ValA,
                    Enum.ValB,
                    Enum.ValC,
                 ]}
                >
              </element>,
            ];
        "#;

        let extraction = extract(source);
        let references = extraction.references;

        let refs: HashSet<_> = references
            .iter()
            .filter(|r| r.kind == Some("reference".to_string()))
            .map(|r| r.name.as_str())
            .collect();

        assert!(refs.contains("Enum"));
        assert!(refs.contains("ValA"));
        assert!(refs.contains("ValB"));
        assert!(refs.contains("ValC"));
    }

    #[test]
    fn extracts_array_literal_object_references() {
        let source = r#"
            const rows = [
              {
                id: "a",
                name: payload?.title || "",
                isActive: isActive,
              },
              {
                id: "b",
                label: "Worker",
                value: primaryUrl,
              },
            ];
        "#;

        let extraction = extract(source);
        let references = extraction.references;

        let refs: HashSet<_> = references
            .iter()
            .filter(|r| r.kind == Some("reference".to_string()))
            .map(|r| r.name.as_str())
            .collect();

        assert!(refs.contains("payload"));
        assert!(refs.contains("title"));
        assert!(refs.contains("isActive"));
        assert!(refs.contains("primaryUrl"));

        for reference in &references {
            assert!(
                !reference.name.contains('\n'),
                "unexpected multiline reference name: {:?}",
                reference.name
            );
        }
    }
}
