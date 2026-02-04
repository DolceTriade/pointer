use tree_sitter::{Node, Parser};

use super::{ExtractedReference, Extraction};

pub fn extract(source: &str) -> Extraction {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_typescript::LANGUAGE_TSX.into())
        .expect("failed to load tree-sitter TypeScript grammar");

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
        "function_declaration"
        | "generator_function_declaration"
        | "class_declaration"
        | "interface_declaration"
        | "type_alias_declaration"
        | "enum_declaration"
        | "namespace_declaration"
        | "internal_module" => {
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
        "method_definition"
        | "method_signature"
        | "public_field_definition"
        | "property_declaration"
        | "property_signature"
        | "constructor"
        | "constructor_signature" => {
            if let Some(name_node) = node
                .child_by_field_name("name")
                .or_else(|| node.child_by_field_name("property"))
            {
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
        "array_pattern"
        | "object_pattern"
        | "binding_pattern"
        | "pair_pattern"
        | "parenthesized_pattern" => {
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
            "variable_declarator" => {
                if is_within_field(node, &parent, "name") {
                    return true;
                }
            }
            "assignment_expression" => {
                if is_within_field(node, &parent, "left") {
                    return true;
                }
            }
            "function_declaration"
            | "generator_function_declaration"
            | "class_declaration"
            | "interface_declaration"
            | "type_alias_declaration"
            | "enum_declaration"
            | "namespace_declaration"
            | "internal_module" => {
                if is_within_field(node, &parent, "name") {
                    return true;
                }
            }
            "method_definition"
            | "method_signature"
            | "public_field_definition"
            | "property_declaration"
            | "property_signature" => {
                if is_within_field(node, &parent, "name")
                    || is_within_field(node, &parent, "property")
                {
                    return true;
                }
            }
            "constructor" | "constructor_signature" => {
                return true;
            }
            _ => {}
        }
        current = parent.parent();
    }
    false
}

fn is_within_field(node: &Node, ancestor: &Node, field_name: &str) -> bool {
    let Some(field_node) = ancestor.child_by_field_name(field_name) else {
        return false;
    };
    let mut current = Some(*node);
    while let Some(curr) = current {
        if curr.id() == field_node.id() {
            return true;
        }
        if curr.id() == ancestor.id() {
            break;
        }
        current = curr.parent();
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn extracts_typescript_symbols_and_variables() {
        let source = r#"
            interface Thing {
                field: string;
                method(): void;
            }
            type Alias = string;
            enum Status { Ready }
            function helper() {
                const inside = 1;
                let [a, b] = [1, 2];
            }
            class Container {
                value = 5;
                method() {
                    const local = 0;
                }
            }
            namespace Utils {
                export function inner() {
                    let local = 2;
                }
            }
            namespace Internal {
                function hidden() {
                    const secret = 0;
                }
            }
            const value = 1;
            let count = 0;
            const handler: (x: number) => number = (x) => x;
            let pointer: () => void;
            const arrow = () => {};
        "#;

        let extraction = extract(source);
        let references = extraction.references;

        let definitions: HashSet<_> = references
            .iter()
            .filter(|r| r.kind == Some("definition".to_string()))
            .map(|r| (r.name.as_str(), r.namespace.as_deref()))
            .collect();

        assert!(definitions.contains(&("Thing", None)));
        assert!(definitions.contains(&("field", Some("Thing"))));
        assert!(definitions.contains(&("method", Some("Thing"))));
        assert!(definitions.contains(&("Alias", None)));
        assert!(definitions.contains(&("Status", None)));
        assert!(definitions.contains(&("helper", None)));
        assert!(definitions.contains(&("inside", Some("helper"))));
        assert!(definitions.contains(&("a", Some("helper"))));
        assert!(definitions.contains(&("b", Some("helper"))));
        assert!(definitions.contains(&("Container", None)));
        assert!(definitions.contains(&("value", Some("Container"))));
        assert!(definitions.contains(&("method", Some("Container"))));
        assert!(definitions.contains(&("local", Some("Container.method"))));
        assert!(definitions.contains(&("Utils", None)));
        assert!(definitions.contains(&("inner", Some("Utils"))));
        assert!(definitions.contains(&("local", Some("Utils.inner"))));
        assert!(definitions.contains(&("Internal", None)));
        assert!(definitions.contains(&("hidden", Some("Internal"))));
        assert!(definitions.contains(&("secret", Some("Internal.hidden"))));
        assert!(definitions.contains(&("value", None)));
        assert!(definitions.contains(&("count", None)));
        assert!(definitions.contains(&("handler", None)));
        assert!(definitions.contains(&("pointer", None)));
        assert!(definitions.contains(&("arrow", None)));
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
