use tree_sitter::{Node, Parser};

use super::{ExtractedReference, Extraction};

pub fn extract(source: &str) -> Extraction {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_nix::LANGUAGE.into())
        .expect("failed to load tree-sitter Nix grammar");

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
        "binding" => {
            if let Some(attr_node) = node.child_by_field_name("attrpath") {
                if let Some(mut segments) = attrpath_segments(&attr_node, source) {
                    if !segments.is_empty() {
                        let name = segments.pop().unwrap();
                        let pos = attr_node.start_position();
                        let mut ns = namespace_stack.to_owned();
                        ns.extend(segments);

                        references.push(ExtractedReference {
                            name: name.clone(),
                            kind: Some("definition".to_string()),
                            namespace: if ns.is_empty() {
                                None
                            } else {
                                Some(ns.join("."))
                            },
                            line: pos.row + 1,
                            column: pos.column + 1,
                        });
                        new_namespace_stack.push(name);
                    }
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

fn attrpath_segments(node: &Node, source: &[u8]) -> Option<Vec<String>> {
    let text = node.utf8_text(source).ok()?.replace(' ', "");
    if text.is_empty() {
        return None;
    }

    let parts = text
        .split('.')
        .filter(|segment| !segment.is_empty())
        .map(|segment| segment.trim_matches('"').to_string())
        .collect::<Vec<_>>();

    if parts.is_empty() { None } else { Some(parts) }
}

fn is_part_of_definition_or_declaration(node: &Node) -> bool {
    let mut current = node.parent();
    while let Some(parent) = current {
        if parent.kind() == "binding" {
            return true;
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
    fn extracts_attr_bindings() {
        let source = r#"
            {
              foo.bar = 1;
              foo."baz".qux = 2;
              nested = {
                inner = 3;
              };
            }
        "#;

        let extraction = extract(source);
        let references = extraction.references;

        let definitions: HashSet<_> = references
            .iter()
            .filter(|r| r.kind == Some("definition".to_string()))
            .map(|r| (r.name.as_str(), r.namespace.as_deref()))
            .collect();

        assert!(definitions.contains(&("bar", Some("foo"))));
        assert!(definitions.contains(&("qux", Some("foo.baz"))));
        assert!(definitions.contains(&("nested", None)));
        assert!(definitions.contains(&("inner", Some("nested"))));
    }
}
