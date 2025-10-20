use tree_sitter::{Node, Parser};

use super::{ExtractedReference, Extraction};

pub fn extract(source: &str) -> Extraction {
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
        "class_definition" | "function_definition" | "async_function_definition" => {
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
        "assignment" => {
            if let Some(left) = node.child_by_field_name("left") {
                let mut names = Vec::new();
                collect_target_names(&left, source, &mut names);
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

fn collect_target_names(node: &Node, source: &[u8], out: &mut Vec<String>) {
    match node.kind() {
        "identifier" => {
            if let Ok(name) = node.utf8_text(source) {
                out.push(name.to_string());
            }
        }
        "tuple" | "list" | "pattern_list" | "parenthesized_expression" => {
            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                collect_target_names(&child, source, out);
            }
        }
        _ => {}
    }
}

fn is_part_of_definition_or_declaration(node: &Node) -> bool {
    let mut current = node.parent();
    while let Some(parent) = current {
        match parent.kind() {
            "class_definition"
            | "function_definition"
            | "async_function_definition"
            | "assignment" => {
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

        let extraction = extract(source);
        let references = extraction.references;

        let definitions: HashSet<_> = references
            .iter()
            .filter(|r| r.kind == Some("definition".to_string()))
            .map(|r| (r.name.as_str(), r.namespace.as_deref()))
            .collect();

        assert!(definitions.contains(&("Outer", None)));
        assert!(definitions.contains(&("Inner", Some("Outer"))));
        assert!(definitions.contains(&("nested_async", Some("Outer.Inner"))));
        assert!(definitions.contains(&("method", Some("Outer"))));
        assert!(definitions.contains(&("helper", Some("Outer.method"))));
        assert!(definitions.contains(&("value", Some("Outer.method.helper"))));
        assert!(definitions.contains(&("count", Some("Outer.method"))));
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
}
