use std::collections::HashSet;
use tree_sitter::{Node, Parser};

use super::{ExtractedReference, Extraction};

pub fn extract(source: &str) -> Extraction {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_rust::LANGUAGE.into())
        .expect("failed to load tree-sitter Rust grammar");

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
        "source_file" => {
            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                collect_references(&child, source, references, namespace_stack, defined_nodes);
            }
            return;
        }
        "mod_item" => {
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
        "struct_item" | "enum_item" | "trait_item" | "union_item" => {
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
        "impl_item" => {
            let mut impl_namespace = namespace_stack.to_vec();
            if let Some(target) = node.child_by_field_name("type") {
                if let Some(type_name) = find_type_identifier(&target, source) {
                    impl_namespace = push_namespace(namespace_stack, &type_name);
                }
            }
            next_namespace = impl_namespace;
        }
        "function_item" | "function_signature_item" => {
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
        "method_item" => {
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
        "const_item" | "static_item" | "type_item" | "macro_definition" => {
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
        "enum_variant" => {
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
        "field_declaration" | "tuple_field_declaration" => {
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
        "attribute_item" => {}
        "let_declaration" => {
            if let Some(pattern) = node.child_by_field_name("pattern") {
                let mut bindings = Vec::new();
                collect_pattern_bindings(&pattern, source, &mut bindings);
                for binding in bindings {
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
        "match_arm" => {
            if let Some(patterns) = node.child_by_field_name("pattern") {
                let mut bindings = Vec::new();
                collect_pattern_bindings(&patterns, source, &mut bindings);
                for binding in bindings {
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
        "parameter" => {
            if let Some(pattern) = node.child_by_field_name("pattern") {
                let mut bindings = Vec::new();
                collect_pattern_bindings(&pattern, source, &mut bindings);
                for binding in bindings {
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
        "closure_parameters" => {
            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                if child.kind() == "parameter" {
                    collect_references(&child, source, references, namespace_stack, defined_nodes);
                } else if child.kind() == "identifier" {
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
        "for_expression" => {
            if let Some(pattern) = node.child_by_field_name("pattern") {
                let mut bindings = Vec::new();
                collect_pattern_bindings(&pattern, source, &mut bindings);
                for binding in bindings {
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
        "let_condition" => {
            if let Some(pattern) = node.child_by_field_name("pattern") {
                let mut bindings = Vec::new();
                collect_pattern_bindings(&pattern, source, &mut bindings);
                for binding in bindings {
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
        "identifier" | "type_identifier" | "field_identifier" | "metavariable" => {
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
        Some(namespace_stack.join("::"))
    }
}

fn sanitize_identifier(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed == "_" || trimmed == "self" {
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
    if defined_nodes.contains(&node.id()) {
        return None;
    }
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

fn collect_pattern_bindings<'a>(pattern: &Node<'a>, source: &[u8], out: &mut Vec<Node<'a>>) {
    match pattern.kind() {
        "identifier" | "identifier_pattern" => {
            out.push(*pattern);
        }
        "tuple_pattern"
        | "tuple_struct_pattern"
        | "slice_pattern"
        | "struct_pattern"
        | "struct_pattern_elements"
        | "struct_pattern_field"
        | "pattern_list"
        | "mutable_pattern"
        | "reference_pattern"
        | "range_pattern"
        | "or_pattern"
        | "as_pattern" => {
            let mut cursor = pattern.walk();
            for child in pattern.children(&mut cursor) {
                if child.is_named() {
                    collect_pattern_bindings(&child, source, out);
                }
            }
        }
        _ => {
            let mut cursor = pattern.walk();
            for child in pattern.children(&mut cursor) {
                if child.is_named() {
                    collect_pattern_bindings(&child, source, out);
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
    fn extracts_comprehensive_rust_identifiers() {
        let source = r#"
            mod outer {
                pub struct Container {
                    pub value: i32,
                    pub pair: (i32, i32),
                }

                pub enum Choice<T> {
                    Unit,
                    Tuple(T, i32),
                    Struct { field: T },
                }

                pub trait Doer {
                    fn do_it(&self, amount: i32) -> i32;
                    const ID: u32;
                }

                impl<T> Container {
                    pub const CAPACITY: usize = 4;
                    pub fn new(value: T) -> Self {
                        let inner = value;
                        inner
                    }
                }

                impl Container {
                    pub fn build(value: i32) -> Self {
                        for (index, element) in [1, 2, 3].iter().enumerate() {
                            let squared = element * element;
                            println!("{} {}", index, squared);
                        }

                        if let Some(temp) = Some(value) {
                            let captured = temp;
                            let Choice::Tuple(first, second) = Choice::Tuple(temp, value);
                        }

                        let closure = |arg: i32| arg + value;
                        closure(value);

                        Self { value, pair: (value, value) }
                    }
                }
            }

            fn top_level(input: i32) -> i32 {
                match input {
                    0 => 0,
                    other @ 1..=10 => other,
                    _ => input,
                }
            }
        "#;

        let extraction = extract(source);
        let references = extraction.references;
        let (definitions, references_map) = bucket_kinds(&references);

        let expected_definitions = HashSet::from([
            ("outer".to_string(), None),
            ("Container".to_string(), Some("outer".to_string())),
            ("value".to_string(), Some("outer::Container".to_string())),
            ("pair".to_string(), Some("outer::Container".to_string())),
            ("Choice".to_string(), Some("outer".to_string())),
            ("Unit".to_string(), Some("outer::Choice".to_string())),
            ("Tuple".to_string(), Some("outer::Choice".to_string())),
            ("Struct".to_string(), Some("outer::Choice".to_string())),
            (
                "field".to_string(),
                Some("outer::Choice::Struct".to_string()),
            ),
            ("Doer".to_string(), Some("outer".to_string())),
            ("do_it".to_string(), Some("outer::Doer".to_string())),
            ("ID".to_string(), Some("outer::Doer".to_string())),
            ("CAPACITY".to_string(), Some("outer::Container".to_string())),
            ("new".to_string(), Some("outer::Container".to_string())),
            (
                "inner".to_string(),
                Some("outer::Container::new".to_string()),
            ),
            ("build".to_string(), Some("outer::Container".to_string())),
            (
                "value".to_string(),
                Some("outer::Container::build".to_string()),
            ),
            (
                "index".to_string(),
                Some("outer::Container::build".to_string()),
            ),
            (
                "element".to_string(),
                Some("outer::Container::build".to_string()),
            ),
            (
                "squared".to_string(),
                Some("outer::Container::build".to_string()),
            ),
            (
                "temp".to_string(),
                Some("outer::Container::build".to_string()),
            ),
            (
                "captured".to_string(),
                Some("outer::Container::build".to_string()),
            ),
            (
                "first".to_string(),
                Some("outer::Container::build".to_string()),
            ),
            (
                "second".to_string(),
                Some("outer::Container::build".to_string()),
            ),
            (
                "closure".to_string(),
                Some("outer::Container::build".to_string()),
            ),
            (
                "arg".to_string(),
                Some("outer::Container::build".to_string()),
            ),
            ("top_level".to_string(), None),
            ("input".to_string(), Some("top_level".to_string())),
            ("other".to_string(), Some("top_level".to_string())),
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
                "Self".to_string(),
                Some("outer::Container::new".to_string()),
            ),
            (
                "value".to_string(),
                Some("outer::Container::new".to_string()),
            ),
            (
                "value".to_string(),
                Some("outer::Container::build".to_string()),
            ),
            (
                "enumerate".to_string(),
                Some("outer::Container::build".to_string()),
            ),
            (
                "Some".to_string(),
                Some("outer::Container::build".to_string()),
            ),
            (
                "Choice".to_string(),
                Some("outer::Container::build".to_string()),
            ),
            (
                "Tuple".to_string(),
                Some("outer::Container::build".to_string()),
            ),
            (
                "println".to_string(),
                Some("outer::Container::build".to_string()),
            ),
            (
                "closure".to_string(),
                Some("outer::Container::build".to_string()),
            ),
            ("input".to_string(), Some("top_level".to_string())),
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
