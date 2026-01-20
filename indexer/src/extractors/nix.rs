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
                        if let Some(expr_node) = node.child_by_field_name("expression") {
                            if expr_node.kind() == "attrset_expression"
                                || expr_node.kind() == "function_expression"
                            {
                                new_namespace_stack.push(name);
                            }
                        }
                    }
                }
            }
        }
        "function_expression" => {
            if let Some(name_node) = node.child_by_field_name("universal") {
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
        "identifier" => {
            if !is_part_of_definition(node) {
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
    let mut segments = Vec::new();
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.kind() == "." {
            continue;
        }
        if let Ok(text) = child.utf8_text(source) {
            segments.push(text.trim_matches('"').to_string());
        }
    }
    if segments.is_empty() {
        None
    } else {
        Some(segments)
    }
}

fn is_part_of_definition(node: &Node) -> bool {
    let mut current = node.parent();
    while let Some(parent) = current {
        if parent.kind() == "attrpath" {
            if let Some(grandparent) = parent.parent() {
                if grandparent.kind() == "binding" {
                    // Check if the attrpath is the one for the binding definition
                    if grandparent.child_by_field_name("attrpath") == Some(parent) {
                        return true;
                    }
                }
            }
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

    #[test]
    fn extracts_complex_nix_expressions() {
        let source = r#"
            {
              isGccTuneSupported =
                tune:
                # for x86 -mtune= takes the same values as -march, plus two more:
                if targetPlatform.isx86 then
                  {
                    generic = true;
                    intel = true;
                  }
                  .${tune} or (isGccArchSupported tune)
                # on arm64, the -mtune= values are specific processors
                else if targetPlatform.isAarch64 then
                  (
                    if isGNU then
                      {
                        cortex-a53 = true;
                        cortex-a72 = true;
                        "cortex-a72.cortex-a53" = true;
                      }
                      .${tune} or false
                    else if isClang then
                      {
                        cortex-a53 = versionAtLeast ccVersion "3.9"; # llvm dfc5d1
                      }
                      .${tune} or false
                    else
                      false
                  )
                else if targetPlatform.isPower then
                  # powerpc does not support -march
                  true
                else if targetPlatform.isMips then
                  # for mips -mtune= takes the same values as -march
                  isGccArchSupported tune
                else
                  false;
            }
        "#;

        let extraction = extract(source);
        let references = extraction.references;

        let definitions: HashSet<_> = references
            .iter()
            .filter(|r| r.kind == Some("definition".to_string()))
            .map(|r| (r.name.as_str(), r.namespace.as_deref(), r.line))
            .collect();

        let refs: HashSet<_> = references
            .iter()
            .filter(|r| r.kind == Some("reference".to_string()))
            .map(|r| (r.name.as_str(), r.namespace.as_deref(), r.line))
            .collect();
        assert!(definitions.contains(&("isGccTuneSupported", None, 3)));
        assert!(definitions.contains(&("tune", Some("isGccTuneSupported"), 4)));
        assert!(definitions.contains(&("generic", Some("isGccTuneSupported"), 8)));
        assert!(definitions.contains(&("intel", Some("isGccTuneSupported"), 9)));
        assert!(definitions.contains(&("cortex-a53", Some("isGccTuneSupported"), 17)));
        assert!(definitions.contains(&("cortex-a72", Some("isGccTuneSupported"), 18)));
        assert!(definitions.contains(&("cortex-a72.cortex-a53", Some("isGccTuneSupported"), 19)));
        assert!(definitions.contains(&("cortex-a53", Some("isGccTuneSupported"), 24)));

        assert!(refs.contains(&("targetPlatform", Some("isGccTuneSupported"), 6)));
        assert!(refs.contains(&("isx86", Some("isGccTuneSupported"), 6)));
        assert!(refs.contains(&("tune", Some("isGccTuneSupported"), 11)));
        assert!(refs.contains(&("isGccArchSupported", Some("isGccTuneSupported"), 11)));
        assert!(refs.contains(&("tune", Some("isGccTuneSupported"), 11)));
        assert!(refs.contains(&("targetPlatform", Some("isGccTuneSupported"), 13)));
        assert!(refs.contains(&("isAarch64", Some("isGccTuneSupported"), 13)));
        assert!(refs.contains(&("isGNU", Some("isGccTuneSupported"), 15)));
        assert!(refs.contains(&("tune", Some("isGccTuneSupported"), 21)));
        assert!(refs.contains(&("isClang", Some("isGccTuneSupported"), 22)));
        assert!(refs.contains(&("versionAtLeast", Some("isGccTuneSupported"), 24)));
        assert!(refs.contains(&("ccVersion", Some("isGccTuneSupported"), 24)));
        assert!(refs.contains(&("tune", Some("isGccTuneSupported"), 26)));
        assert!(refs.contains(&("targetPlatform", Some("isGccTuneSupported"), 30)));
        assert!(refs.contains(&("isPower", Some("isGccTuneSupported"), 30)));
        assert!(refs.contains(&("targetPlatform", Some("isGccTuneSupported"), 33)));
        assert!(refs.contains(&("isMips", Some("isGccTuneSupported"), 33)));
        assert!(refs.contains(&("isGccArchSupported", Some("isGccTuneSupported"), 35)));
        assert!(refs.contains(&("tune", Some("isGccTuneSupported"), 35)));
    }

    #[test]
    fn extracts_string_interpolations() {
        let source = r#"
            {
              msg = "${if (hardeningDisable != [] || hardeningEnable != [] || isMusl) then "NIX_HARDENING_ENABLE" else null}";
              nested = "${if cond then "${inner}" else "fallback"}";
            }
        "#;

        let extraction = extract(source);
        let references = extraction.references;

        let refs: HashSet<_> = references
            .iter()
            .filter(|r| r.kind == Some("reference".to_string()))
            .map(|r| r.name.as_str())
            .collect();

        assert!(refs.contains("hardeningDisable"));
        assert!(refs.contains("hardeningEnable"));
        assert!(refs.contains("isMusl"));
        assert!(refs.contains("cond"));
        assert!(refs.contains("inner"));

        for reference in &references {
            assert!(
                !reference.name.contains("${")
                    && !reference.name.contains('}')
                    && !reference.name.contains('\n'),
                "unexpected interpolated symbol: {:?}",
                reference.name
            );
        }
    }
}
