use std::collections::HashSet;
use tree_sitter::{Node, Parser};

use super::{ExtractedReference, Extraction};

pub fn extract(source: &str) -> Extraction {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_go::LANGUAGE.into())
        .expect("failed to load tree-sitter Go grammar");

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
    if node.kind() == "source_file" {
        let mut current_namespace = namespace_stack.to_vec();
        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            if child.kind() == "package_clause" {
                let mut pkg_cursor = child.walk();
                let mut name_node_opt = None;
                for grand in child.children(&mut pkg_cursor) {
                    if matches!(grand.kind(), "identifier" | "package_identifier") {
                        name_node_opt = Some(grand);
                        break;
                    }
                }
                if let Some(name_node) = name_node_opt {
                    if let Some(name) = record_definition_node(
                        &name_node,
                        source,
                        references,
                        namespace_stack,
                        "definition",
                        defined_nodes,
                    ) {
                        current_namespace = push_namespace(namespace_stack, &name);
                    }
                }
            } else {
                collect_references(
                    &child,
                    source,
                    references,
                    &current_namespace,
                    defined_nodes,
                );
            }
        }
        return;
    }

    let mut next_namespace = namespace_stack.to_vec();

    match node.kind() {
        "function_declaration" => {
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
        "method_declaration" => {
            let mut method_namespace = namespace_stack.to_vec();
            if let Some(receiver) = node.child_by_field_name("receiver") {
                let mut receiver_type_name = None;
                let mut cursor = receiver.walk();
                for param in receiver.children(&mut cursor) {
                    if param.kind() == "parameter_declaration" {
                        if let Some(receiver_type) = param.child_by_field_name("type") {
                            receiver_type_name = find_type_identifier(&receiver_type, source);
                            if receiver_type_name.is_some() {
                                break;
                            }
                        }
                    }
                }
                if let Some(receiver_name) = receiver_type_name {
                    if let Some(clean) = sanitize_identifier(&receiver_name) {
                        method_namespace = push_namespace(namespace_stack, &clean);
                    }
                }
            }

            if let Some(name_node) = node.child_by_field_name("name") {
                if let Some(name) = record_definition_node(
                    &name_node,
                    source,
                    references,
                    &method_namespace,
                    "definition",
                    defined_nodes,
                ) {
                    method_namespace = push_namespace(&method_namespace, &name);
                }
            }

            next_namespace = method_namespace;
        }
        "type_spec" => {
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
        "short_var_declaration" | "var_spec" | "const_spec" => {
            let mut names = Vec::new();
            if let Some(left) = node.child_by_field_name("left") {
                collect_go_binding_names(&left, source, &mut names);
            } else {
                collect_go_binding_names(node, source, &mut names);
            }

            for binding in names {
                record_definition_node(
                    &binding,
                    source,
                    references,
                    namespace_stack,
                    "definition",
                    defined_nodes,
                );
            }
        }
        "field_declaration" => {
            if let Some(name_node) = node.child_by_field_name("name") {
                let mut names = Vec::new();
                collect_go_binding_names(&name_node, source, &mut names);
                for binding in names {
                    record_definition_node(
                        &binding,
                        source,
                        references,
                        namespace_stack,
                        "definition",
                        defined_nodes,
                    );
                }
            } else {
                let mut names = Vec::new();
                collect_go_binding_names(node, source, &mut names);
                for binding in names {
                    record_definition_node(
                        &binding,
                        source,
                        references,
                        namespace_stack,
                        "definition",
                        defined_nodes,
                    );
                }
            }
        }
        "method_spec" => {
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
        "method_elem" => {
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
        "parameter_declaration" | "variadic_parameter" => {
            if let Some(name_node) = node.child_by_field_name("name") {
                let mut names = Vec::new();
                collect_go_binding_names(&name_node, source, &mut names);
                for binding in names {
                    record_definition_node(
                        &binding,
                        source,
                        references,
                        namespace_stack,
                        "definition",
                        defined_nodes,
                    );
                }
            }
        }
        "identifier" | "field_identifier" | "type_identifier" => {
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

fn sanitize_identifier(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed == "_" {
        None
    } else {
        Some(trimmed.to_string())
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
    if let Ok(raw) = node.utf8_text(source) {
        if let Some(name) = sanitize_identifier(raw) {
            let pos = node.start_position();
            references.push(ExtractedReference {
                name: name.clone(),
                kind: Some(kind.to_string()),
                namespace: namespace_from_stack(namespace_stack),
                line: pos.row + 1,
                column: pos.column + 1,
            });
            defined_nodes.insert(node.id());
            return Some(name);
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
        if let Some(name) = sanitize_identifier(raw) {
            let pos = node.start_position();
            references.push(ExtractedReference {
                name,
                kind: Some("reference".to_string()),
                namespace: namespace_from_stack(namespace_stack),
                line: pos.row + 1,
                column: pos.column + 1,
            });
        }
    }
}

fn collect_go_binding_names<'a>(node: &Node<'a>, source: &[u8], out: &mut Vec<Node<'a>>) {
    match node.kind() {
        "identifier" | "field_identifier" | "type_identifier" => {
            out.push(*node);
        }
        "identifier_list"
        | "field_identifier_list"
        | "expression_list"
        | "parameter_list"
        | "parenthesized_expression"
        | "parenthesized_identifier_list" => {
            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                if child.is_named() {
                    collect_go_binding_names(&child, source, out);
                }
            }
        }
        _ => {
            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                if child.is_named() {
                    collect_go_binding_names(&child, source, out);
                }
            }
        }
    }
}

fn find_type_identifier(node: &Node, source: &[u8]) -> Option<String> {
    match node.kind() {
        "type_identifier" | "identifier" => {
            node.utf8_text(source).ok().and_then(sanitize_identifier)
        }
        _ => {
            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                if let Some(name) = find_type_identifier(&child, source) {
                    return Some(name);
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

    fn collect_kinds(
        references: &[ExtractedReference],
    ) -> (
        HashMap<(String, Option<String>), usize>,
        HashMap<(String, Option<String>), usize>,
    ) {
        let mut definitions = HashMap::new();
        let mut references_map = HashMap::new();
        for r in references {
            let key = (r.name.clone(), r.namespace.clone());
            match r.kind.as_deref() {
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
    fn extracts_comprehensive_go_identifiers() {
        let source = r#"
            package demo

            import "fmt"

            const constantValue = 3

            type Foo struct {
                Value int
                Embedded
            }

            type Bar interface {
                DoThing(input int) (result int)
            }

            func helper(arg int) int {
                local := arg + constantValue
                return local
            }

            func (f *Foo) Method(extra int) {
                counter := 0
                f.Value = counter
                helper(counter)
            }

            func useInterface(b Bar) {
                fmt.Println(b)
            }
        "#;

        let extraction = extract(source);
        let references = extraction.references;
        let (definitions, references_map) = collect_kinds(&references);

        let expected_defs = HashSet::from([
            ("demo".to_string(), None),
            ("constantValue".to_string(), Some("demo".to_string())),
            ("Foo".to_string(), Some("demo".to_string())),
            ("Value".to_string(), Some("demo.Foo".to_string())),
            ("Embedded".to_string(), Some("demo.Foo".to_string())),
            ("Bar".to_string(), Some("demo".to_string())),
            ("DoThing".to_string(), Some("demo.Bar".to_string())),
            ("input".to_string(), Some("demo.Bar.DoThing".to_string())),
            ("result".to_string(), Some("demo.Bar.DoThing".to_string())),
            ("helper".to_string(), Some("demo".to_string())),
            ("arg".to_string(), Some("demo.helper".to_string())),
            ("local".to_string(), Some("demo.helper".to_string())),
            ("f".to_string(), Some("demo.Foo.Method".to_string())),
            ("Method".to_string(), Some("demo.Foo".to_string())),
            ("extra".to_string(), Some("demo.Foo.Method".to_string())),
            ("counter".to_string(), Some("demo.Foo.Method".to_string())),
            ("useInterface".to_string(), Some("demo".to_string())),
            ("b".to_string(), Some("demo.useInterface".to_string())),
        ]);

        for key in &expected_defs {
            assert!(
                definitions.contains_key(key),
                "missing definition for {:?}",
                key
            );
        }

        let expected_refs = HashSet::from([
            ("constantValue".to_string(), Some("demo.helper".to_string())),
            ("arg".to_string(), Some("demo.helper".to_string())),
            ("local".to_string(), Some("demo.helper".to_string())),
            ("f".to_string(), Some("demo.Foo.Method".to_string())),
            ("Value".to_string(), Some("demo.Foo.Method".to_string())),
            ("counter".to_string(), Some("demo.Foo.Method".to_string())),
            ("helper".to_string(), Some("demo.Foo.Method".to_string())),
            ("b".to_string(), Some("demo.useInterface".to_string())),
            ("fmt".to_string(), Some("demo.useInterface".to_string())),
            ("Println".to_string(), Some("demo.useInterface".to_string())),
        ]);

        for key in &expected_refs {
            assert!(
                references_map.contains_key(key),
                "missing reference for {:?}",
                key
            );
        }
    }
}
