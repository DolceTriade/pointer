use std::collections::HashSet;
use tree_sitter::{Node, Parser};

use super::{ExtractedReference, Extraction};

pub fn extract(source: &str) -> Extraction {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_glsl::LANGUAGE_GLSL.into())
        .expect("failed to load tree-sitter GLSL grammar");

    let tree = match parser.parse(source, None) {
        Some(tree) => tree,
        None => return Extraction::default(),
    };

    let mut references = Vec::new();
    let source_bytes = source.as_bytes();
    let mut defined_nodes = HashSet::new();
    collect_references(
        &tree.root_node(),
        source_bytes,
        &mut references,
        &[],
        &mut defined_nodes,
    );

    references.into()
}

fn collect_references(
    node: &Node,
    source: &[u8],
    references: &mut Vec<ExtractedReference>,
    namespace_stack: &[String],
    defined_nodes: &mut HashSet<usize>,
) {
    let mut next_namespace = namespace_stack.to_vec();

    match node.kind() {
        "translation_unit" => {
            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                collect_references(&child, source, references, namespace_stack, defined_nodes);
            }
            return;
        }
        "function_definition" => {
            if let Some(declarator) = node.child_by_field_name("declarator") {
                if let Some(name_node) = declarator.child_by_field_name("declarator") {
                    if let Some(name) = record_definition_node(
                        &name_node,
                        source,
                        references,
                        namespace_stack,
                        "definition",
                        defined_nodes,
                    ) {
                        next_namespace = push_namespace(namespace_stack, &name);
                    }
                }
            }
        }
        "parameter_declaration" => {
            if let Some(declarator) = node.child_by_field_name("declarator") {
                record_definition_node(
                    &declarator,
                    source,
                    references,
                    namespace_stack,
                    "definition",
                    defined_nodes,
                );
            }
        }
        "declaration" => {
            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                if child.kind() == "init_declarator" {
                    if let Some(declarator) = child.child_by_field_name("declarator") {
                        record_definition_node(
                            &declarator,
                            source,
                            references,
                            namespace_stack,
                            "definition",
                            defined_nodes,
                        );
                    }
                } else if child.kind() == "identifier" {
                    record_definition_node(
                        &child,
                        source,
                        references,
                        namespace_stack,
                        "definition",
                        defined_nodes,
                    );
                }
            }
        }
        "struct_specifier" => {
            if let Some(name_node) = node.child_by_field_name("name") {
                if let Some(name) = record_definition_node(
                    &name_node,
                    source,
                    references,
                    namespace_stack,
                    "definition",
                    defined_nodes,
                ) {
                    next_namespace = push_namespace(namespace_stack, &name);
                }
            }
        }
        "field_declaration" => {
            if let Some(field_name) = node.child_by_field_name("declarator") {
                record_definition_node(
                    &field_name,
                    source,
                    references,
                    namespace_stack,
                    "definition",
                    defined_nodes,
                );
            }
        }
        "preproc_def" => {
            if let Some(name_node) = node.child_by_field_name("name") {
                record_definition_node(
                    &name_node,
                    source,
                    references,
                    namespace_stack,
                    "definition",
                    defined_nodes,
                );
            }
        }
        "field_expression" => {
            // Handle field access like 'obj.field'
            // The argument (the object being accessed) and field will be processed as children
            // No special handling needed, just let the recursion process children normally
        }
        "identifier" => {
            record_reference_node(node, source, references, namespace_stack, defined_nodes);
        }
        "type_identifier" | "primitive_type" => {
            // Check if this is a built-in GLSL type and skip it
            if let Ok(type_text) = node.utf8_text(source) {
                // Check against common GLSL built-in types
                match type_text.trim() {
                    // Scalar types
                    "void" | "bool" | "int" | "uint" | "float" | "double" |
                    // Vector types
                    "vec2" | "vec3" | "vec4" | 
                    "ivec2" | "ivec3" | "ivec4" |
                    "uvec2" | "uvec3" | "uvec4" |
                    "bvec2" | "bvec3" | "bvec4" |
                    // Matrix types
                    "mat2" | "mat3" | "mat4" |
                    "mat2x2" | "mat2x3" | "mat2x4" |
                    "mat3x2" | "mat3x3" | "mat3x4" |
                    "mat4x2" | "mat4x3" | "mat4x4" |
                    // Sampler types
                    "sampler1D" | "sampler2D" | "sampler3D" | "samplerCube" |
                    "sampler1DShadow" | "sampler2DShadow" | "samplerCubeShadow" |
                    "sampler1DArray" | "sampler2DArray" | "sampler1DArrayShadow" | "sampler2DArrayShadow" |
                    "isampler1D" | "isampler2D" | "isampler3D" | "isamplerCube" |
                    "usampler1D" | "usampler2D" | "usampler3D" | "usamplerCube" |
                    "sampler2DRect" | "sampler2DRectShadow" | "samplerBuffer" |
                    "sampler2DMS" | "sampler2DMSArray" | "isampler2DMS" | "isampler2DMSArray" |
                    "usampler2DMS" | "usampler2DMSArray" |
                    // Image types
                    "image1D" | "iimage1D" | "uimage1D" |
                    "image2D" | "iimage2D" | "uimage2D" |
                    "image3D" | "iimage3D" | "uimage3D" |
                    "image2DRect" | "iimage2DRect" | "uimage2DRect" |
                    "imageCube" | "iimageCube" | "uimageCube" |
                    "imageBuffer" | "iimageBuffer" | "uimageBuffer" |
                    "image1DArray" | "iimage1DArray" | "uimage1DArray" |
                    "image2DArray" | "iimage2DArray" | "uimage2DArray" |
                    "imageCubeArray" | "iimageCubeArray" | "uimageCubeArray" |
                    "image2DMS" | "iimage2DMS" | "uimage2DMS" |
                    "image2DMSArray" | "iimage2DMSArray" | "uimage2DMSArray" => {
                        // Skip built-in GLSL types - don't record them as references
                    },
                    _ => {
                        // For non-built-in types (user-defined), record as reference
                        record_reference_node(node, source, references, namespace_stack, defined_nodes);
                    }
                }
            } else {
                // If we can't get text, just record as reference (safer fallback)
                record_reference_node(node, source, references, namespace_stack, defined_nodes);
            }
        }
        "field_identifier" => {
            record_reference_node(node, source, references, namespace_stack, defined_nodes);
        }
        _ => {}
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        collect_references(&child, source, references, &next_namespace, defined_nodes);
    }
}

fn push_namespace(namespace_stack: &[String], segment: &str) -> Vec<String> {
    let mut next = namespace_stack.to_vec();
    next.push(segment.to_string());
    next
}

fn namespace_from_stack(namespace_stack: &[String]) -> Option<String> {
    if namespace_stack.is_empty() {
        None
    } else {
        Some(namespace_stack.join("::"))
    }
}

fn sanitize_identifier(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
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
                name,
                kind: Some("reference".to_string()),
                namespace: namespace_from_stack(namespace_stack),
                line: pos.row + 1,
                column: pos.column + 1,
            });
        }
    }
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
    fn extracts_glsl_identifiers() {
        let source = r#"
#version 330 core

struct Light {
    vec3 position;
    vec3 color;
};

uniform Light light;
uniform vec3 viewPos;

in vec3 FragPos;
in vec3 Normal;

out vec4 color;

float calculateDistance(vec3 a, vec3 b) {
    vec3 diff = a - b;
    return length(diff);
}

void main() {
    float distance = calculateDistance(FragPos, light.position);
    color = vec4(light.color, 1.0);
}
"#;

        let extraction = extract(source);
        let references = extraction.references;
        let (_definitions, _references_map) = bucket_kinds(&references);
        let source = r#"
#version 330 core

struct Light {
    vec3 position;
    vec3 color;
};

uniform Light light;
uniform vec3 viewPos;

in vec3 FragPos;
in vec3 Normal;

out vec4 color;

float calculateDistance(vec3 a, vec3 b) {
    vec3 diff = a - b;
    return length(diff);
}

void main() {
    float distance = calculateDistance(FragPos, light.position);
    color = vec4(light.color, 1.0);
}
"#;

        let extraction = extract(source);
        let references = extraction.references;
        let (definitions, references_map) = bucket_kinds(&references);

        let expected_definitions = HashSet::from([
            ("Light".to_string(), None),
            ("position".to_string(), Some("Light".to_string())),
            ("color".to_string(), Some("Light".to_string())),
            ("calculateDistance".to_string(), None),
            ("a".to_string(), Some("calculateDistance".to_string())),
            ("b".to_string(), Some("calculateDistance".to_string())),
            ("diff".to_string(), Some("calculateDistance".to_string())),
            ("light".to_string(), None),
            ("viewPos".to_string(), None),
            ("FragPos".to_string(), None),
            ("Normal".to_string(), None),
            ("color".to_string(), None),
            ("main".to_string(), None),
            ("distance".to_string(), Some("main".to_string())),
        ]);

        for key in &expected_definitions {
            assert!(
                definitions.contains_key(key),
                "missing definition for {:?}",
                key
            );
        }

        let expected_references = HashSet::from([
            ("length".to_string(), Some("calculateDistance".to_string())),
            ("calculateDistance".to_string(), Some("main".to_string())),
            ("FragPos".to_string(), Some("main".to_string())),
            ("light".to_string(), Some("main".to_string())),
            ("position".to_string(), Some("main".to_string())), // This should be captured - it's a user-defined field in struct
            ("color".to_string(), Some("main".to_string())), // This should be captured - it's a user-defined field in struct
        ]);

        for key in &expected_references {
            assert!(
                references_map.contains_key(key),
                "missing reference for {:?}",
                key
            );
        }
    }
}
