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
        "function_item" | "method_item" | "struct_item" | "enum_item" | "trait_item"
        | "mod_item" | "const_item" | "static_item" | "type_item" => {
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
        "field_declaration" | "tuple_field_declaration" => {
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
                }
            }
        }
        "let_declaration" => {
            if let Some(pattern) = node.child_by_field_name("pattern") {
                let mut names = Vec::new();
                collect_pattern_bindings(&pattern, source, &mut names);
                for name in names {
                    let pos = pattern.start_position();
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
        "identifier" | "type_identifier" | "field_identifier" => {
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

fn collect_pattern_bindings(pattern: &Node, source: &[u8], out: &mut Vec<String>) {
    match pattern.kind() {
        "identifier" => {
            if let Ok(text) = pattern.utf8_text(source) {
                let trimmed = text.trim();
                if !trimmed.starts_with('_') {
                    out.push(trimmed.to_string());
                }
            }
        }
        "tuple_pattern"
        | "tuple_struct_pattern"
        | "slice_pattern"
        | "struct_pattern"
        | "struct_pattern_elements"
        | "struct_pattern_field"
        | "pattern_list" => {
            let mut cursor = pattern.walk();
            for child in pattern.children(&mut cursor) {
                collect_pattern_bindings(&child, source, out);
            }
        }
        _ => {
            let mut cursor = pattern.walk();
            for child in pattern.children(&mut cursor) {
                collect_pattern_bindings(&child, source, out);
            }
        }
    }
}

fn is_part_of_definition_or_declaration(node: &Node) -> bool {
    let mut current = node.parent();
    while let Some(parent) = current {
        match parent.kind() {
            "function_item"
            | "method_item"
            | "struct_item"
            | "enum_item"
            | "trait_item"
            | "mod_item"
            | "const_item"
            | "static_item"
            | "type_item"
            | "field_declaration"
            | "tuple_field_declaration"
            | "let_declaration" => {
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
    fn extracts_public_symbols_with_namespaces_and_vars() {
        let source = r#"
            pub mod foo {
                pub struct Bar {
                    inner: i32,
                }
                pub(crate) fn helper() {}
                fn private_fn() {}

                impl Bar {
                    pub fn method(&self) {
                        let inner = 2;
                        let (left, right) = (1, 2);
                        let _ignored = 10;
                    }
                }
            }

            pub enum TopLevel {}
            pub fn top_fn() {}
            fn hidden() {}

            fn variable_owner() {
                let count = 1;
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
        assert!(definitions.contains(&("inner", Some("foo.Bar"))));
        assert!(definitions.contains(&("helper", Some("foo"))));
        assert!(definitions.contains(&("private_fn", Some("foo"))));
        assert!(definitions.contains(&("method", Some("foo.Bar"))));
        assert!(definitions.contains(&("inner", Some("foo.Bar.method"))));
        assert!(definitions.contains(&("left", Some("foo.Bar.method"))));
        assert!(definitions.contains(&("right", Some("foo.Bar.method"))));
        assert!(definitions.contains(&("TopLevel", None)));
        assert!(definitions.contains(&("top_fn", None)));
        assert!(definitions.contains(&("hidden", None)));
        assert!(definitions.contains(&("variable_owner", None)));
        assert!(definitions.contains(&("count", Some("variable_owner"))));
    }
}
