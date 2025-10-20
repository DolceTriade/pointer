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
        "function_definition" => {
            if let Some(declarator) = node.child_by_field_name("declarator") {
                if let Some(name_node) = find_identifier_in_declarator(&declarator) {
                    if let Ok(name) = name_node.utf8_text(source) {
                        let pos = name_node.start_position();
                        references.push(ExtractedReference {
                            name: name.to_string(),
                            kind: Some("definition".to_string()),
                            namespace: if namespace_stack.is_empty() {
                                None
                            } else {
                                Some(namespace_stack.join("::"))
                            },
                            line: pos.row + 1,
                            column: pos.column + 1,
                        });
                        new_namespace_stack.push(name.to_string());
                    }
                }
            }
        }
        "declaration" => {
            if is_function_declaration_only(node) {
                if let Some(declarator) = node
                    .children(&mut node.walk())
                    .find(|c| c.kind() == "function_declarator")
                {
                    if let Some(name_node) = find_identifier_in_declarator(&declarator) {
                        if let Ok(name) = name_node.utf8_text(source) {
                            let pos = name_node.start_position();
                            references.push(ExtractedReference {
                                name: name.to_string(),
                                kind: Some("declaration".to_string()),
                                namespace: if namespace_stack.is_empty() {
                                    None
                                } else {
                                    Some(namespace_stack.join("::"))
                                },
                                line: pos.row + 1,
                                column: pos.column + 1,
                            });
                        }
                    }
                }
            } else {
                for child in node.children(&mut node.walk()) {
                    if child.kind() == "init_declarator" {
                        if let Some(declarator) = child.child_by_field_name("declarator") {
                            if let Some(name_node) = find_identifier_in_declarator(&declarator) {
                                if let Ok(name) = name_node.utf8_text(source) {
                                    let pos = name_node.start_position();
                                    references.push(ExtractedReference {
                                        name: name.to_string(),
                                        kind: Some("definition".to_string()),
                                        namespace: if namespace_stack.is_empty() {
                                            None
                                        } else {
                                            Some(namespace_stack.join("::"))
                                        },
                                        line: pos.row + 1,
                                        column: pos.column + 1,
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }
        "struct_specifier" | "union_specifier" | "enum_specifier" => {
            if let Some(name_node) = node.child_by_field_name("name") {
                if let Ok(name) = name_node.utf8_text(source) {
                    let pos = name_node.start_position();
                    references.push(ExtractedReference {
                        name: name.to_string(),
                        kind: Some("definition".to_string()),
                        namespace: if namespace_stack.is_empty() {
                            None
                        } else {
                            Some(namespace_stack.join("::"))
                        },
                        line: pos.row + 1,
                        column: pos.column + 1,
                    });
                    new_namespace_stack.push(name.to_string());
                }
            }
        }
        "identifier" => {
            if !is_part_of_definition_or_declaration(node) {
                if let Ok(name) = node.utf8_text(source) {
                    let pos = node.start_position();
                    references.push(ExtractedReference {
                        name: name.to_string(),
                        kind: Some("reference".to_string()),
                        namespace: if namespace_stack.is_empty() {
                            None
                        } else {
                            Some(namespace_stack.join("::"))
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

fn is_function_declaration_only(node: &Node) -> bool {
    node.child_by_field_name("body").is_none()
}

fn is_part_of_definition_or_declaration(node: &Node) -> bool {
    let mut current = node.parent();
    while let Some(parent) = current {
        match parent.kind() {
            "function_definition"
            | "declaration"
            | "struct_specifier"
            | "union_specifier"
            | "enum_specifier" => {
                if let Some(name_node) = find_identifier_in_declarator(&parent) {
                    if name_node == *node {
                        return true;
                    }
                }
                if let Some(name_node) = parent.child_by_field_name("name") {
                    if name_node == *node {
                        return true;
                    }
                }
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
    fn extracts_functions_types_and_variables() {
        let source = r#"
            customType uninitialized;
            int (*global_handler)(int);
            struct Foo {
                int value;
            };

            struct Callbacks {
                int (*on_ready)(int);
                int count;
            };

            enum Bar {
                A,
                B,
            };

            int counter = 0;

            static int helper(void) {
                int local = 3;
                int (*local_callback)(int);
                return local;
            }

            int run(struct Foo foo) {
                int result = helper();
                return result;
            }
        "#;

        let extraction = extract(source);
        let references = extraction.references;

        let definitions: HashSet<_> = references
            .iter()
            .filter(|r| r.kind == Some("definition".to_string()))
            .map(|r| (r.name.as_str(), r.namespace.as_deref()))
            .collect();

        assert!(definitions.contains(&("Foo", None)));
        assert!(definitions.contains(&("Bar", None)));
        assert!(definitions.contains(&("helper", None)));
        assert!(definitions.contains(&("run", None)));
        assert!(definitions.contains(&("counter", None)));
        assert!(definitions.contains(&("local", Some("helper"))));
        assert!(definitions.contains(&("result", Some("run"))));
        assert!(definitions.contains(&("global_handler", None)));
        assert!(definitions.contains(&("local_callback", Some("helper"))));
        assert!(definitions.contains(&("on_ready", Some("Callbacks"))));

        let declarations: HashSet<_> = references
            .iter()
            .filter(|r| r.kind == Some("declaration".to_string()))
            .map(|r| (r.name.as_str(), r.namespace.as_deref()))
            .collect();

        assert!(declarations.contains(&("uninitialized", None)));

        let refs: HashSet<_> = references
            .iter()
            .filter(|r| r.kind == Some("reference".to_string()))
            .map(|r| r.name.as_str())
            .collect();

        assert!(refs.contains("customType"));
        assert!(refs.contains("local"));
        assert!(refs.contains("Foo"));
        assert!(refs.contains("helper"));
        assert!(refs.contains("result"));
    }
}
