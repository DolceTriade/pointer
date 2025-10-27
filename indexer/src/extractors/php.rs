use std::collections::HashSet;
use tree_sitter::{Node, Parser};

use super::{ExtractedReference, Extraction};

pub fn extract(source: &str) -> Extraction {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_php::LANGUAGE_PHP.into())
        .expect("failed to load tree-sitter PHP grammar");

    let tree = match parser.parse(source, None) {
        Some(tree) => tree,
        None => return Extraction::default(),
    };

    let mut references = Vec::new();
    let source_bytes = source.as_bytes();
    let mut defined_nodes = HashSet::new();
    let mut defined_variables = HashSet::new();
    collect_references(
        &tree.root_node(),
        source_bytes,
        &mut references,
        &mut Vec::new(),
        &mut defined_nodes,
        &mut defined_variables,
    );

    references.into()
}

fn collect_references(
    node: &Node,
    source: &[u8],
    references: &mut Vec<ExtractedReference>,
    current_namespace: &mut Vec<String>,
    defined_nodes: &mut HashSet<usize>,
    defined_variables: &mut HashSet<String>,
) {
    match node.kind() {
        "namespace_definition" => {
            if let Some(name_node) = node.child_by_field_name("name") {
                if let Some(name) = get_node_text(&name_node, source) {
                    *current_namespace = name.split("\\").map(|s| s.to_string()).collect();
                }
            }
        }
        "class_declaration" | "trait_declaration" | "interface_declaration" => {
            if let Some(name_node) = node.child_by_field_name("name") {
                let mut namespace_stack = current_namespace.clone();
                if let Some(name) = get_node_text(&name_node, source) {
                    record_definition_node(
                        &name_node,
                        source,
                        references,
                        &namespace_stack,
                        "definition",
                        defined_nodes,
                        defined_variables,
                    );
                    namespace_stack.push(name);
                }
                let mut cursor = node.walk();
                for child in node.children(&mut cursor) {
                    collect_references(
                        &child,
                        source,
                        references,
                        &mut namespace_stack,
                        defined_nodes,
                        &mut HashSet::new(),
                    );
                }
                return;
            }
        }
        "function_definition" | "method_declaration" => {
            if let Some(name_node) = node.child_by_field_name("name") {
                let mut namespace_stack = current_namespace.clone();
                if let Some(name) = get_node_text(&name_node, source) {
                    record_definition_node(
                        &name_node,
                        source,
                        references,
                        &namespace_stack,
                        "definition",
                        defined_nodes,
                        defined_variables,
                    );
                    namespace_stack.push(name);
                }
                let mut cursor = node.walk();
                for child in node.children(&mut cursor) {
                    collect_references(
                        &child,
                        source,
                        references,
                        &mut namespace_stack,
                        defined_nodes,
                        &mut HashSet::new(),
                    );
                }
                return;
            }
        }
        "assignment_expression" => {
            if let Some(left) = node.child_by_field_name("left") {
                if let Some(name) = get_node_text(&left, source) {
                    if !defined_variables.contains(&name) {
                        record_definition_node(
                            &left,
                            source,
                            references,
                            current_namespace,
                            "definition",
                            defined_nodes,
                            defined_variables,
                        );
                    } else {
                        record_reference_node(
                            &left,
                            source,
                            references,
                            current_namespace,
                            defined_nodes,
                        );
                    }
                }
            }
        }
        "simple_parameter" => {
            if let Some(name_node) = node.child_by_field_name("name") {
                record_definition_node(
                    &name_node,
                    source,
                    references,
                    current_namespace,
                    "definition",
                    defined_nodes,
                    defined_variables,
                );
            }
        }
        "property_element" | "const_element" => {
            if let Some(name_node) = node.child_by_field_name("name") {
                record_definition_node(
                    &name_node,
                    source,
                    references,
                    current_namespace,
                    "definition",
                    defined_nodes,
                    defined_variables,
                );
            }
        }
        "const_declaration" => {
            let mut cursor = node.walk();
            for child in node.children_by_field_name("declarator", &mut cursor) {
                if let Some(name_node) = child.child_by_field_name("name") {
                    record_definition_node(
                        &name_node,
                        source,
                        references,
                        current_namespace,
                        "definition",
                        defined_nodes,
                        defined_variables,
                    );
                }
            }
        }
        // Capture function calls - these are references to functions
        "function_call_expression" => {
            if let Some(callee) = node.child_by_field_name("function") {
                if callee.kind() != "member_access_expression"
                    && callee.kind() != "scoped_call_expression"
                {
                    record_reference_node(
                        &callee,
                        source,
                        references,
                        current_namespace,
                        defined_nodes,
                    );
                }
            }
        }
        // Capture method calls
        "member_call_expression" => {
            if let Some(method) = node.child_by_field_name("method") {
                record_reference_node(
                    &method,
                    source,
                    references,
                    current_namespace,
                    defined_nodes,
                );
            }
        }
        // Capture scoped calls (static method calls, class constants)
        "scoped_call_expression" => {
            if let Some(name) = node.child_by_field_name("name") {
                record_reference_node(&name, source, references, current_namespace, defined_nodes);
            }
        }
        // Capture class constant access
        "member_access_expression" => {
            if let Some(name) = node.child_by_field_name("member") {
                record_reference_node(&name, source, references, current_namespace, defined_nodes);
            }
        }

        // Capture class properties (fields)
        "property_declaration" => {
            let mut cursor = node.walk();
            for child in node.children_by_field_name("property", &mut cursor) {
                if let Some(name_node) = child.child_by_field_name("name") {
                    record_definition_node(
                        &name_node,
                        source,
                        references,
                        current_namespace,
                        "definition",
                        defined_nodes,
                        defined_variables,
                    );
                }
            }
        }
        "identifier" | "variable_name" | "name" => {
            record_reference_node(node, source, references, current_namespace, defined_nodes);
        }
        _ => {}
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        collect_references(
            &child,
            source,
            references,
            current_namespace,
            defined_nodes,
            defined_variables,
        );
    }
}

fn namespace_from_stack(namespace_stack: &[String]) -> Option<String> {
    if namespace_stack.is_empty() {
        None
    } else {
        Some(namespace_stack.join("\\"))
    }
}

fn sanitize_identifier(raw: &str) -> Option<String> {
    let trimmed = raw.trim().trim_start_matches("$");
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn record_definition_node(
    node: &Node,
    source: &[u8],
    references: &mut Vec<ExtractedReference>,
    namespace_stack: &[String],
    kind: &str,
    defined_nodes: &mut HashSet<usize>,
    defined_variables: &mut HashSet<String>,
) -> Option<String> {
    if defined_nodes.contains(&node.id()) {
        return None;
    }
    if let Ok(raw) = node.utf8_text(source) {
        if let Some(name) = sanitize_identifier(raw) {
            let pos = node.start_position();
            references.push(ExtractedReference {
                name: name.clone(),
                kind: Some(kind.to_string()),
                namespace: namespace_from_stack(namespace_stack),
                line: pos.row + 1,
                column: pos.column + 1,
            });
            defined_nodes.insert(node.id());
            defined_variables.insert(name.clone());
            return Some(name);
        }
    }
    None
}

fn record_reference_node(
    node: &Node,
    source: &[u8],
    references: &mut Vec<ExtractedReference>,
    namespace_stack: &[String],
    defined_nodes: &HashSet<usize>,
) {
    if defined_nodes.contains(&node.id()) {
        return;
    }
    if let Ok(raw) = node.utf8_text(source) {
        if let Some(name) = sanitize_identifier(raw) {
            let pos = node.start_position();
            references.push(ExtractedReference {
                name: name.to_string(),
                kind: Some("reference".to_string()),
                namespace: namespace_from_stack(namespace_stack),
                line: pos.row + 1,
                column: pos.column + 1,
            });
        }
    }
}

fn get_node_text(node: &Node, source: &[u8]) -> Option<String> {
    node.utf8_text(source).ok().map(|s| s.trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};

    fn bucket_kinds(
        references: &[ExtractedReference],
    ) -> (
        HashMap<(String, Option<String>), usize>,
        HashMap<(String, Option<String>), usize>,
    ) {
        let mut definitions = HashMap::new();
        let mut references_map = HashMap::new();
        for reference in references {
            let key = (reference.name.clone(), reference.namespace.clone());
            match reference.kind.as_deref() {
                Some("definition") | Some("declaration") => {
                    *definitions.entry(key).or_insert(0) += 1;
                }
                Some("reference") => {
                    *references_map.entry(key).or_insert(0) += 1;
                }
                other => panic!("unexpected kind: {:?}", other),
            }
        }
        (definitions, references_map)
    }

    #[test]
    fn extracts_php_identifiers() {
        let source = r#"<?php
namespace MyNamespace;

class MyClass {
    private $property;

    public function myMethod($param) {
        $local = $param + 1;
        return $local;
    }
}

function helperFunction($value) {
    $result = $value * 2;
    return $result;
}

$obj = new MyClass();
$test = helperFunction(5);
"#;

        let extraction = extract(source);
        let references = extraction.references;
        let (definitions, references_map) = bucket_kinds(&references);

        let expected_definitions = HashSet::from([
            ("MyClass".to_string(), Some("MyNamespace".to_string())),
            (
                "property".to_string(),
                Some(r"MyNamespace\MyClass".to_string()),
            ),
            (
                "myMethod".to_string(),
                Some(r"MyNamespace\MyClass".to_string()),
            ),
            (
                "param".to_string(),
                Some(r"MyNamespace\MyClass\myMethod".to_string()),
            ),
            (
                "local".to_string(),
                Some(r"MyNamespace\MyClass\myMethod".to_string()),
            ),
            (
                "helperFunction".to_string(),
                Some("MyNamespace".to_string()),
            ),
            (
                "value".to_string(),
                Some(r"MyNamespace\helperFunction".to_string()),
            ),
            (
                "result".to_string(),
                Some(r"MyNamespace\helperFunction".to_string()),
            ),
            ("obj".to_string(), Some("MyNamespace".to_string())),
            ("test".to_string(), Some("MyNamespace".to_string())),
        ]);

        for key in &expected_definitions {
            assert!(
                definitions.contains_key(key),
                "missing definition for {:?}",
                key
            );
        }

        let expected_references = HashSet::from([
            ("MyClass".to_string(), Some("MyNamespace".to_string())),
            (
                "helperFunction".to_string(),
                Some("MyNamespace".to_string()),
            ),
            (
                "param".to_string(),
                Some(r"MyNamespace\MyClass\myMethod".to_string()),
            ),
            (
                "local".to_string(),
                Some(r"MyNamespace\MyClass\myMethod".to_string()),
            ),
            (
                "value".to_string(),
                Some(r"MyNamespace\helperFunction".to_string()),
            ),
            (
                "result".to_string(),
                Some(r"MyNamespace\helperFunction".to_string()),
            ),
        ]);

        for key in &expected_references {
            assert!(
                references_map.contains_key(key),
                "missing reference for {:?}",
                key
            );
        }
    }

    #[test]
    fn extracts_php_function_calls() {
        let source = r#"<?php
namespace Test;

function test_function($param) {
    return $param;
}

$result = test_function("hello");
$another = test_function($result);
"#;

        let extraction = extract(source);
        let references = extraction.references;

        // Check that function calls are captured as references
        let has_test_function_ref = references
            .iter()
            .any(|r| r.name == "test_function" && r.kind.as_deref() == Some("reference"));
        assert!(
            has_test_function_ref,
            "Should capture function call as reference"
        );
    }

    #[test]
    fn extracts_php_class_properties() {
        let source = r#"<?php
namespace Test;

class TestClass {
    public $myProperty;

    public function testMethod() {
        $this->myProperty = 'value';
    }
}
"#;

        let extraction = extract(source);
        let references = extraction.references;

        // Check that properties are captured as both definitions and references
        let has_property_def = references
            .iter()
            .any(|r| r.name == "myProperty" && r.kind.as_deref() == Some("definition"));
        let has_property_ref = references
            .iter()
            .any(|r| r.name == "myProperty" && r.kind.as_deref() == Some("reference"));

        assert!(has_property_def, "Should capture property definition");
        assert!(has_property_ref, "Should capture property reference");
    }

    #[test]
    fn extracts_php_static_members() {
        let source = r#"<?php
namespace Test;

class TestClass {
    public static $staticProp = 'value';
    public static function staticMethod() {
        return self::$staticProp;
    }
}

$result = TestClass::$staticProp;
TestClass::staticMethod();
"#;

        let extraction = extract(source);
        let references = extraction.references;

        // Check that static properties and methods are captured
        let has_static_prop_ref = references.iter().any(|r| r.name == "staticProp");
        let has_static_method_ref = references.iter().any(|r| r.name == "staticMethod");

        assert!(
            has_static_prop_ref,
            "Should capture static property reference"
        );
        assert!(
            has_static_method_ref,
            "Should capture static method reference"
        );
    }

    #[test]
    fn extracts_php_traditional_constructs() {
        let source = r#"<?php
namespace Test;

interface TestInterface {
    public function interfaceMethod();
}

trait TestTrait {
    public function traitMethod() {
        return "trait";
    }
}

class BaseClass {
    public function baseMethod() {}
}

class TestClass extends BaseClass implements TestInterface {
    use TestTrait;

    public function interfaceMethod() {
        $this->baseMethod();
        $this->traitMethod();
    }
}
"#;

        let extraction = extract(source);
        let references = extraction.references;

        // Check that class relationships (extends, implements, use) create references
        let has_base_method_ref = references.iter().any(|r| r.name == "baseMethod");
        let has_trait_method_ref = references.iter().any(|r| r.name == "traitMethod");
        let has_interface_method_def = references
            .iter()
            .any(|r| r.name == "interfaceMethod" && r.kind.as_deref() == Some("definition"));

        assert!(
            has_base_method_ref,
            "Should capture inherited method reference"
        );
        assert!(
            has_trait_method_ref,
            "Should capture trait method reference"
        );
        assert!(
            has_interface_method_def,
            "Should capture interface method definition"
        );
    }
}
