use tree_sitter::{Node, Parser};

use super::{ExtractedReference, Extraction};

pub fn extract(source: &str) -> Extraction {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into())
        .expect("failed to load tree-sitter JavaScript grammar");

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
        "function_declaration" | "generator_function_declaration" | "class_declaration" => {
            if let Some(name_node) = node.child_by_field_name("name") {
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
        "method_definition" => {
            if let Some(name_node) = node.child_by_field_name("name") {
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
        "public_field_definition" | "property_definition" => {
            if let Some(name_node) = node.child_by_field_name("property") {
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
                }
            }
        }
        "lexical_declaration" | "variable_declaration" => {
            for child in node.children(&mut node.walk()) {
                if child.kind() == "variable_declarator" {
                    if let Some(name_node) = child.child_by_field_name("name") {
                        let mut names = Vec::new();
                        collect_pattern_names(&name_node, source, &mut names);
                        for name in names {
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
                        }
                    }
                }
            }
        }
        "assignment_expression" => {
            if let Some(left) = node.child_by_field_name("left") {
                let mut names = Vec::new();
                collect_pattern_names(&left, source, &mut names);
                for name in names {
                    let pos = left.start_position();
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
                }
            }
        }
        "identifier" | "property_identifier" | "shorthand_property_identifier" => {
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

fn collect_pattern_names(node: &Node, source: &[u8], out: &mut Vec<String>) {
    match node.kind() {
        "identifier" | "property_identifier" => {
            if let Ok(name) = node.utf8_text(source) {
                out.push(name.to_string());
            }
        }
        "array_pattern" | "object_pattern" | "pair_pattern" | "parenthesized_pattern" => {
            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                collect_pattern_names(&child, source, out);
            }
        }
        _ => {}
    }
}

fn is_part_of_definition_or_declaration(node: &Node) -> bool {
    let mut current = node.parent();
    while let Some(parent) = current {
        match parent.kind() {
            "function_declaration"
            | "generator_function_declaration"
            | "class_declaration"
            | "method_definition"
            | "public_field_definition"
            | "property_definition"
            | "lexical_declaration"
            | "variable_declaration"
            | "assignment_expression" => {
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
    fn extracts_function_class_and_variable_names() {
        let source = r#"
            export function foo() {}
            function hidden() {}
            class Widget {
                count = 1;
                method() {
                    const local = 1;
                }
            }
            const local = 1;
            const answer = 42, helper = () => {};
            let flag = true;
            var legacy = 0;
            let [a, b] = [1, 2];
            ({ x: alias } = obj);
        "#;

        let extraction = extract(source);
        let references = extraction.references;

        let definitions: HashSet<_> = references
            .iter()
            .filter(|r| r.kind == Some("definition".to_string()))
            .map(|r| (r.name.as_str(), r.namespace.as_deref()))
            .collect();

        assert!(definitions.contains(&("foo", None)));
        assert!(definitions.contains(&("hidden", None)));
        assert!(definitions.contains(&("Widget", None)));
        assert!(definitions.contains(&("count", Some("Widget"))));
        assert!(definitions.contains(&("method", Some("Widget"))));
        assert!(definitions.contains(&("local", Some("Widget.method"))));
        assert!(definitions.contains(&("local", None)));
        assert!(definitions.contains(&("answer", None)));
        assert!(definitions.contains(&("helper", None)));
        assert!(definitions.contains(&("flag", None)));
        assert!(definitions.contains(&("legacy", None)));
        assert!(definitions.contains(&("a", None)));
        assert!(definitions.contains(&("b", None)));
        assert!(definitions.contains(&("alias", None)));
    }
}
