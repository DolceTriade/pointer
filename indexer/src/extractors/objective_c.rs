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
                                Some(namespace_stack.join("."))
                            },
                            line: pos.row + 1,
                            column: pos.column + 1,
                        });
                        new_namespace_stack.push(name.to_string());
                    }
                }
            }
        }
        "method_definition" => {
            if let Some(selector) = node.child_by_field_name("selector") {
                if let Ok(name) = selector.utf8_text(source) {
                    let pos = selector.start_position();
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
        "class_interface"
        | "class_implementation"
        | "category_interface"
        | "category_implementation"
        | "protocol_declaration" => {
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
        "property_declaration" | "ivar_declaration" | "declaration" => {
            for child in node.children(&mut node.walk()) {
                if child.kind() == "struct_declarator"
                    || child.kind() == "init_declarator"
                    || child.kind() == "declarator"
                {
                    if let Some(name_node) = find_identifier_in_declarator(child) {
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
            }
        }
        "identifier" | "field_identifier" => {
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

fn find_identifier_in_declarator<'a>(declarator: Node<'a>) -> Option<Node<'a>> {
    match declarator.kind() {
        "identifier" | "field_identifier" => Some(declarator),
        "pointer_declarator"
        | "function_declarator"
        | "array_declarator"
        | "parenthesized_declarator"
        | "struct_declarator"
        | "init_declarator" => declarator
            .children(&mut declarator.walk())
            .find_map(|dec| find_identifier_in_declarator(dec)),
        _ => None,
    }
}

fn is_part_of_definition_or_declaration(node: &Node) -> bool {
    let mut current = node.parent();
    while let Some(parent) = current {
        match parent.kind() {
            "function_definition"
            | "method_definition"
            | "class_interface"
            | "class_implementation"
            | "category_interface"
            | "category_implementation"
            | "protocol_declaration"
            | "property_declaration"
            | "ivar_declaration"
            | "declaration" => {
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
    fn extracts_objective_c_symbols() {
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

        let definitions: HashSet<_> = references
            .iter()
            .filter(|r| r.kind == Some("definition".to_string()))
            .map(|r| (r.name.as_str(), r.namespace.as_deref()))
            .collect();

        assert!(definitions.contains(&("solovar", None)));
        assert!(definitions.contains(&("global_handler", None)));
        assert!(definitions.contains(&("Demo", None)));
        assert!(definitions.contains(&("_count", Some("Demo"))));
        assert!(definitions.contains(&("callback", Some("Demo"))));
        assert!(definitions.contains(&("value", Some("Demo"))));
        assert!(definitions.contains(&("onReady", Some("Demo"))));
        assert!(definitions.contains(&("doThing", Some("Demo"))));
        assert!(definitions.contains(&("local", Some("Demo.doThing"))));
        assert!(definitions.contains(&("temp", Some("Demo.doThing"))));
        assert!(definitions.contains(&("local_handler", Some("Demo.doThing"))));
        assert!(definitions.contains(&("Helper", None)));
        assert!(definitions.contains(&("global", Some("Helper"))));
        assert!(definitions.contains(&("global_no_init", Some("Helper"))));
        assert!(definitions.contains(&("helper_callback", Some("Helper"))));
    }
}
