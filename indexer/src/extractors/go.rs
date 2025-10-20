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
        "function_declaration" => {
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
        "method_declaration" => {
            if let Some(name_node) = node.child_by_field_name("name") {
                if let Ok(name) = name_node.utf8_text(source) {
                    let pos = name_node.start_position();
                    let mut namespace = if namespace_stack.is_empty() {
                        None
                    } else {
                        Some(namespace_stack.join("."))
                    };

                    if let Some(receiver) = node.child_by_field_name("receiver") {
                        if let Some(receiver_type) = receiver.child_by_field_name("type") {
                            if let Ok(receiver_type_name) = receiver_type.utf8_text(source) {
                                namespace = merge_namespaces(
                                    namespace.as_deref(),
                                    Some(receiver_type_name.trim_start_matches('*')),
                                );
                            }
                        }
                    }

                    references.push(ExtractedReference {
                        name: name.to_string(),
                        kind: Some("definition".to_string()),
                        namespace,
                        line: pos.row + 1,
                        column: pos.column + 1,
                    });
                    new_namespace_stack.push(name.to_string());
                }
            }
        }
        "type_spec" => {
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

            for name_node in names {
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
        "package_clause" => {
            if let Some(name_node) = node.child_by_field_name("name") {
                if let Ok(name) = name_node.utf8_text(source) {
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

fn collect_go_binding_names<'a>(node: &Node<'a>, source: &[u8], out: &mut Vec<Node<'a>>) {
    match node.kind() {
        "identifier" => {
            out.push(*node);
        }
        "identifier_list" | "expression_list" | "parenthesized_expression" => {
            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                if child.is_named() {
                    collect_go_binding_names(&child, source, out);
                }
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
            | "method_declaration"
            | "type_spec"
            | "short_var_declaration"
            | "var_spec"
            | "const_spec"
            | "package_clause" => {
                return true;
            }
            _ => {}
        }
        current = parent.parent();
    }
    false
}

fn merge_namespaces(package: Option<&str>, receiver: Option<&str>) -> Option<String> {
    match (package, receiver) {
        (Some(pkg), Some(rcv)) => Some(format!("{}.{}", pkg, rcv)),
        (Some(pkg), None) => Some(pkg.to_string()),
        (None, Some(rcv)) => Some(rcv.to_string()),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn extracts_go_symbols_and_variables() {
        let source = r#"
            package demo

            var top = 1
            var handler func(int) int
            var typed1, typed2 func() int
            var withLiteral = func() int { return 0 }
            var mix1, mix2 = func() {}, 42

            type Foo struct {
                Value int
            }
            type Bar interface {}

            func helper() {
                local := 3
                localFn := func() int { return local }
            }

            func (f *Foo) Method() {
                var counter int
            }
        "#;

        let extraction = extract(source);
        let references = extraction.references;

        let definitions: HashSet<_> = references
            .iter()
            .filter(|r| r.kind == Some("definition".to_string()))
            .map(|r| (r.name.as_str(), r.namespace.as_deref()))
            .collect();

        assert!(definitions.contains(&("demo", None)));
        assert!(definitions.contains(&("top", Some("demo"))));
        assert!(definitions.contains(&("handler", Some("demo"))));
        assert!(definitions.contains(&("typed1", Some("demo"))));
        assert!(definitions.contains(&("typed2", Some("demo"))));
        assert!(definitions.contains(&("withLiteral", Some("demo"))));
        assert!(definitions.contains(&("mix1", Some("demo"))));
        assert!(definitions.contains(&("mix2", Some("demo"))));
        assert!(definitions.contains(&("Foo", Some("demo"))));
        assert!(definitions.contains(&("Bar", Some("demo"))));
        assert!(definitions.contains(&("helper", Some("demo"))));
        assert!(definitions.contains(&("local", Some("demo.helper"))));
        assert!(definitions.contains(&("localFn", Some("demo.helper"))));
        assert!(definitions.contains(&("Method", Some("demo.Foo"))));
        assert!(definitions.contains(&("counter", Some("demo.Foo.Method"))));

        let refs: HashSet<_> = references
            .iter()
            .filter(|r| r.kind == Some("reference".to_string()))
            .map(|r| r.name.as_str())
            .collect();

        assert!(refs.contains("local"));
    }
}
