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
                if let Some(name_node) = find_identifier_in_declarator(declarator) {
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
        "declaration" | "simple_declaration" => {
            if is_function_declaration_only(node) {
                if let Some(declarator) = node
                    .children(&mut node.walk())
                    .find(|c| c.kind() == "function_declarator")
                {
                    if let Some(name_node) = find_identifier_in_declarator(declarator) {
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
                            if let Some(name_node) = find_identifier_in_declarator(declarator) {
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
        "class_specifier" | "struct_specifier" | "enum_specifier" | "namespace_definition" => {
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
        "identifier" | "field_identifier" | "scoped_identifier" | "type_identifier" => {
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

fn find_identifier_in_declarator<'a>(declarator: Node<'a>) -> Option<Node<'a>> {
    match declarator.kind() {
        "identifier" | "type_identifier" | "field_identifier" | "scoped_identifier" => {
            Some(declarator)
        }
        "pointer_declarator"
        | "function_declarator"
        | "array_declarator"
        | "parenthesized_declarator"
        | "qualified_identifier" => declarator
            .children(&mut declarator.walk())
            .find_map(|dec| find_identifier_in_declarator(dec)),
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
            | "simple_declaration"
            | "class_specifier"
            | "struct_specifier"
            | "enum_specifier"
            | "namespace_definition" => {
                if let Some(name_node) = find_identifier_in_declarator(parent) {
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
    fn extracts_cpp_symbols_and_variables() {
        let source = r#"
            int (*global_handler)(int);
            cvar_t *r_maxPolyVerts;
            namespace foo {
                int (*foo_handler)(int);
                int foo_global = 2;
                class Bar {
                public:
                    int value;
                    int (*on_ready)(int);
                    int method() {
                        int local = value;
                        int (*local_callback)(int);
                        return local;
                    }
                };

                int run();
                int counter = 10;
            }

            int foo::run() {
                int result = counter;
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

        assert!(definitions.contains(&("foo", None)));
        assert!(definitions.contains(&("Bar", Some("foo"))));
        assert!(definitions.contains(&("method", Some("foo::Bar"))));
        assert!(definitions.contains(&("run", Some("foo"))));
        assert!(definitions.contains(&("counter", Some("foo"))));
        assert!(definitions.contains(&("foo_global", Some("foo"))));
        assert!(definitions.contains(&("local", Some("foo::Bar::method"))));
        assert!(definitions.contains(&("result", Some("foo::run"))));
        assert!(definitions.contains(&("global_handler", None)));
        assert!(definitions.contains(&("r_maxPolyVerts", None)));
        assert!(definitions.contains(&("foo_handler", Some("foo"))));
        assert!(definitions.contains(&("on_ready", Some("foo::Bar"))));
        assert!(definitions.contains(&("local_callback", Some("foo::Bar::method"))));

        let declarations: HashSet<_> = references
            .iter()
            .filter(|r| r.kind == Some("declaration".to_string()))
            .map(|r| (r.name.as_str(), r.namespace.as_deref()))
            .collect();

        assert!(declarations.contains(&("run", Some("foo"))));

        let refs: HashSet<_> = references
            .iter()
            .filter(|r| r.kind == Some("reference".to_string()))
            .map(|r| r.name.as_str())
            .collect();

        assert!(refs.contains("cvar_t"));
        assert!(refs.contains("value"));
        assert!(refs.contains("local"));
        assert!(refs.contains("counter"));
        assert!(refs.contains("result"));
    }
}
