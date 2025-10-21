use std::collections::HashSet;
use tree_sitter::{Node, Parser, Point};

use super::{ExtractedReference, Extraction};

pub fn extract(source: &str, namespace_hint: Option<&str>) -> Extraction {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_python::LANGUAGE.into())
        .expect("failed to load tree-sitter Python grammar");

    let tree = match parser.parse(source, None) {
        Some(tree) => tree,
        None => return Extraction::default(),
    };

    let mut references = Vec::new();
    let source_bytes = source.as_bytes();
    let mut definition_positions = HashSet::new();
    collect_references(
        &tree.root_node(),
        source_bytes,
        &mut references,
        &[],
        &mut definition_positions,
    );

    apply_namespace_hint(&mut references, namespace_hint);

    references.into()
}

fn collect_references(
    node: &Node,
    source: &[u8],
    references: &mut Vec<ExtractedReference>,
    namespace_stack: &[String],
    definition_positions: &mut HashSet<usize>,
) {
    let mut new_namespace_stack = namespace_stack.to_owned();

    match node.kind() {
        "class_definition" | "function_definition" | "async_function_definition" => {
            if let Some(name_node) = node.child_by_field_name("name") {
                if let Ok(name) = name_node.utf8_text(source) {
                    let pos = name_node.start_position();
                    definition_positions.insert(name_node.start_byte() as usize);
                    references.push(ExtractedReference {
                        name: name.to_string(),
                        kind: Some("definition".to_string()),
                        namespace: namespace_for_stack(namespace_stack),
                        line: pos.row + 1,
                        column: pos.column + 1,
                    });
                    new_namespace_stack.push(name.to_string());
                }
            }
        }
        "assignment" | "augmented_assignment" | "annotated_assignment" => {
            let target_field = if node.kind() == "annotated_assignment" {
                "target"
            } else {
                "left"
            };
            if let Some(target) = node.child_by_field_name(target_field) {
                let mut bindings = Vec::new();
                collect_binding_identifiers(&target, source, &mut bindings);
                for (name, pos, start_byte) in bindings {
                    definition_positions.insert(start_byte);
                    references.push(ExtractedReference {
                        name,
                        kind: Some("definition".to_string()),
                        namespace: namespace_for_stack(namespace_stack),
                        line: pos.row + 1,
                        column: pos.column + 1,
                    });
                }
            }
        }
        "for_statement" | "async_for_statement" | "for_in_clause" => {
            if let Some(target) = node.child_by_field_name("left") {
                let mut bindings = Vec::new();
                collect_binding_identifiers(&target, source, &mut bindings);
                for (name, pos, start_byte) in bindings {
                    definition_positions.insert(start_byte);
                    references.push(ExtractedReference {
                        name,
                        kind: Some("definition".to_string()),
                        namespace: namespace_for_stack(namespace_stack),
                        line: pos.row + 1,
                        column: pos.column + 1,
                    });
                }
            }
        }
        "with_item" => {
            if let Some(alias) = node.child_by_field_name("alias") {
                let mut bindings = Vec::new();
                collect_binding_identifiers(&alias, source, &mut bindings);
                for (name, pos, start_byte) in bindings {
                    definition_positions.insert(start_byte);
                    references.push(ExtractedReference {
                        name,
                        kind: Some("definition".to_string()),
                        namespace: namespace_for_stack(namespace_stack),
                        line: pos.row + 1,
                        column: pos.column + 1,
                    });
                }
            }
        }
        "except_clause" => {
            if let Some(alias) = node.child_by_field_name("alias") {
                let mut bindings = Vec::new();
                collect_binding_identifiers(&alias, source, &mut bindings);
                for (name, pos, start_byte) in bindings {
                    definition_positions.insert(start_byte);
                    references.push(ExtractedReference {
                        name,
                        kind: Some("definition".to_string()),
                        namespace: namespace_for_stack(namespace_stack),
                        line: pos.row + 1,
                        column: pos.column + 1,
                    });
                }
            }
        }
        "aliased_import" => {
            if let Some(alias) = node.child_by_field_name("alias") {
                if let Ok(name) = alias.utf8_text(source) {
                    let pos = alias.start_position();
                    definition_positions.insert(alias.start_byte() as usize);
                    references.push(ExtractedReference {
                        name: name.to_string(),
                        kind: Some("definition".to_string()),
                        namespace: namespace_for_stack(namespace_stack),
                        line: pos.row + 1,
                        column: pos.column + 1,
                    });
                }
            }
        }
        _ => {}
    }

    if node.kind() == "identifier" {
        let start_byte = node.start_byte() as usize;
        if !definition_positions.contains(&start_byte) {
            if let Ok(name) = node.utf8_text(source) {
                let pos = node.start_position();
                references.push(ExtractedReference {
                    name: name.to_string(),
                    kind: Some("reference".to_string()),
                    namespace: namespace_for_stack(namespace_stack),
                    line: pos.row + 1,
                    column: pos.column + 1,
                });
            }
        }
    }

    for child in node.children(&mut node.walk()) {
        collect_references(
            &child,
            source,
            references,
            &new_namespace_stack,
            definition_positions,
        );
    }
}

fn namespace_for_stack(namespace_stack: &[String]) -> Option<String> {
    if namespace_stack.is_empty() {
        None
    } else {
        Some(namespace_stack.join("::"))
    }
}

fn apply_namespace_hint(references: &mut [ExtractedReference], namespace_hint: Option<&str>) {
    let base = match namespace_hint {
        Some(hint) => {
            let trimmed = hint.trim();
            if trimmed.is_empty() {
                return;
            }
            trimmed
        }
        None => return,
    };

    let base_owned = base.to_string();
    let base_with_sep = if base.ends_with("::") {
        base_owned.clone()
    } else {
        format!("{}::", base)
    };

    for reference in references.iter_mut() {
        let existing = reference
            .namespace
            .take()
            .filter(|ns| !ns.is_empty())
            .unwrap_or_default();

        let merged = if existing.is_empty() {
            base_owned.clone()
        } else if existing.starts_with(base) {
            existing
        } else {
            format!("{}{}", base_with_sep, existing)
        };

        reference.namespace = Some(merged);
    }
}

fn collect_binding_identifiers(node: &Node, source: &[u8], out: &mut Vec<(String, Point, usize)>) {
    match node.kind() {
        "identifier" => {
            if let Ok(name) = node.utf8_text(source) {
                let trimmed = name.trim();
                if !trimmed.is_empty() && trimmed != "_" {
                    out.push((
                        trimmed.to_string(),
                        node.start_position(),
                        node.start_byte() as usize,
                    ));
                }
            }
        }
        "tuple"
        | "list"
        | "pattern_list"
        | "parenthesized_expression"
        | "tuple_pattern"
        | "list_pattern"
        | "pattern"
        | "as_pattern"
        | "expression_list" => {
            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                collect_binding_identifiers(&child, source, out);
            }
        }
        "default_parameter" | "typed_default_parameter" => {
            if let Some(name_node) = node.child_by_field_name("name") {
                collect_binding_identifiers(&name_node, source, out);
            }
        }
        "list_splat_pattern" | "dictionary_splat_pattern" => {
            if let Some(child) = node.child(1) {
                collect_binding_identifiers(&child, source, out);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn extracts_python_symbols_and_variables() {
        let source = r#"
            class Outer:
                class Inner:
                    async def nested_async(self):
                        pass

                def method(self):
                    def helper():
                        value = 42
                        return value
                    count = helper()
                    return count

            def top_level():
                answer = None
                return answer

            env = "prod"
            compute = lambda value: value
        "#;

        let extraction = extract(source, None);
        let references = extraction.references;

        let definitions: HashSet<_> = references
            .iter()
            .filter(|r| r.kind == Some("definition".to_string()))
            .map(|r| (r.name.as_str(), r.namespace.as_deref()))
            .collect();

        assert!(definitions.contains(&("Outer", None)));
        assert!(definitions.contains(&("Inner", Some("Outer"))));
        assert!(definitions.contains(&("nested_async", Some("Outer::Inner"))));
        assert!(definitions.contains(&("method", Some("Outer"))));
        assert!(definitions.contains(&("helper", Some("Outer::method"))));
        assert!(definitions.contains(&("value", Some("Outer::method::helper"))));
        assert!(definitions.contains(&("count", Some("Outer::method"))));
        assert!(definitions.contains(&("top_level", None)));
        assert!(definitions.contains(&("answer", Some("top_level"))));
        assert!(definitions.contains(&("env", None)));
        assert!(definitions.contains(&("compute", None)));

        let refs: HashSet<_> = references
            .iter()
            .filter(|r| r.kind == Some("reference".to_string()))
            .map(|r| r.name.as_str())
            .collect();

        assert!(refs.contains("value"));
        assert!(refs.contains("helper"));
        assert!(refs.contains("count"));
        assert!(refs.contains("answer"));
        assert!(refs.contains("value"));
    }

    #[test]
    fn applies_namespace_hint_to_python_scopes() {
        let source = r#"
            class Inner:
                def method(self):
                    value = 1

            top_level_var = 2
        "#;

        let extraction = extract(source, Some("pkg.module"));
        let mut collected = extraction
            .references
            .into_iter()
            .filter(|r| r.kind == Some("definition".to_string()))
            .map(|r| (r.name, r.namespace))
            .collect::<Vec<_>>();

        collected.sort_by(|a, b| a.0.cmp(&b.0));

        assert!(collected.contains(&("Inner".to_string(), Some("pkg.module".to_string()))));
        assert!(collected.contains(&("method".to_string(), Some("pkg.module::Inner".to_string()))));
        assert!(collected.contains(&(
            "value".to_string(),
            Some("pkg.module::Inner::method".to_string())
        )));
        assert!(collected.contains(&("top_level_var".to_string(), Some("pkg.module".to_string()))));
    }
}
