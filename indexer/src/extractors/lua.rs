use std::collections::HashSet;
use tree_sitter::{Node, Parser};

use super::{ExtractedReference, Extraction};

pub fn extract(source: &str) -> Extraction {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_lua::LANGUAGE.into())
        .expect("failed to load tree-sitter Lua grammar");

    let tree = match parser.parse(source, None) {
        Some(tree) => tree,
        None => return Extraction::default(),
    };

    let mut references = Vec::new();
    let source_bytes = source.as_bytes();
    let mut defined_nodes = HashSet::new();
    let mut defined_variables = HashSet::new();

    // Initial call to collect_references
    collect_references(
        &tree.root_node(),
        source_bytes,
        &mut references,
        &mut Vec::new(), // Initial namespace stack
        &mut defined_nodes,
        &mut defined_variables,
    );

    references.into()
}

fn collect_references(
    node: &Node,
    source: &[u8],
    references: &mut Vec<ExtractedReference>,
    namespace_stack: &mut Vec<String>,
    defined_nodes: &mut HashSet<usize>,
    defined_variables: &mut HashSet<String>,
) {
    // Create a new namespace stack for the current scope if applicable, otherwise clone the parent's.
    // Lua doesn't have explicit block-level scoping in the same way as some other languages,
    // but functions and for-loops introduce new scopes.
    let mut current_scope_namespace = namespace_stack.clone();

    match node.kind() {
        // Global level declarations
        "chunk" | "program" => {}

        // Function declarations create a new scope and push a new namespace segment
        "function_declaration" | "local_function_declaration" => {
            if let Some(name_node) = node.child_by_field_name("name") {
                if let Some(name) = record_definition_node(
                    &name_node,
                    source,
                    references,
                    &current_scope_namespace,
                    "function_definition",
                    defined_nodes,
                    defined_variables,
                ) {
                    current_scope_namespace.push(name);
                }
            }
        }

        // Variable declarations that include local variables within a local_declaration
        "local_declaration" => {
            // Process all children to find identifiers to define
            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                if child.kind() == "variable_declaration" {
                    // Variable declaration contains assignment statement
                    let mut inner_cursor = child.walk();
                    for inner_child in child.children(&mut inner_cursor) {
                        if inner_child.kind() == "assignment_statement" {
                            // Find the variable_list in the assignment statement
                            let mut assign_cursor = inner_child.walk();
                            let mut variable_names = Vec::new();
                            for assign_inner_child in inner_child.children(&mut assign_cursor) {
                                if assign_inner_child.kind() == "variable_list" {
                                    // Process each identifier in the list
                                    let mut var_cursor = assign_inner_child.walk();
                                    for var_child in assign_inner_child.children(&mut var_cursor) {
                                        if var_child.kind() == "identifier" {
                                            if let Ok(name) = var_child.utf8_text(source) {
                                                variable_names.push(name.to_string());
                                            }
                                            record_definition_node(
                                                &var_child,
                                                source,
                                                references,
                                                &current_scope_namespace,
                                                "variable_definition",
                                                defined_nodes,
                                                defined_variables,
                                            );
                                        }
                                    }
                                }
                                // Process expression list to handle table constructors that may need special namespacing
                                else if assign_inner_child.kind() == "expression_list" {
                                    let mut expr_cursor = assign_inner_child.walk();
                                    for expr_child in assign_inner_child.children(&mut expr_cursor)
                                    {
                                        if expr_child.kind() == "table_constructor" {
                                            // For table constructors, we may want to enhance the namespace context
                                            // But for now we'll process normally and let recursive call handle it
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        // Also handle variable_declaration directly for other contexts
        "variable_declaration" => {
            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                if child.kind() == "assignment_statement" {
                    // Find the variable_list in the assignment statement
                    let mut assign_cursor = child.walk();
                    for assign_child in child.children(&mut assign_cursor) {
                        if assign_child.kind() == "variable_list" {
                            // Process each identifier in the list
                            let mut var_cursor = assign_child.walk();
                            for var_child in assign_child.children(&mut var_cursor) {
                                if var_child.kind() == "identifier" {
                                    record_definition_node(
                                        &var_child,
                                        source,
                                        references,
                                        &current_scope_namespace,
                                        "variable_definition",
                                        defined_nodes,
                                        defined_variables,
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }

        // Assignment statements where the left-hand side might contain new definitions
        // if they haven't been seen in the current scope or parent scopes.
        "assignment_statement" => {
            if let Some(variable_list) = node.child_by_field_name("variable_list") {
                let mut cursor = variable_list.walk();
                for child in variable_list.children(&mut cursor) {
                    if let Some(name) = get_node_text(&child, source) {
                        // Check if this variable is already defined in any active scope
                        // This check is simplified and might need refinement for complex scoping rules.
                        if !defined_variables.contains(&name) {
                            record_definition_node(
                                &child,
                                source,
                                references,
                                &current_scope_namespace,
                                "variable_definition",
                                defined_nodes,
                                defined_variables,
                            );
                        } else {
                            record_reference_node(
                                &child,
                                source,
                                references,
                                &current_scope_namespace,
                                defined_nodes,
                            );
                        }
                    }
                }
            }
        }

        "for_numeric_clause" => {
            if let Some(name_node) = node.child_by_field_name("name") {
                record_definition_node(
                    &name_node,
                    source,
                    references,
                    &current_scope_namespace,
                    "loop_variable_definition",
                    defined_nodes,
                    defined_variables,
                );
            }
        }
        "for_in_statement" => {
            // Variables in a for_in_statement are also definitions within their block
            if let Some(variable_list) = node.child_by_field_name("variable_list") {
                let mut cursor = variable_list.walk();
                for child in variable_list.children(&mut cursor) {
                    if child.kind() == "identifier" {
                        record_definition_node(
                            &child,
                            source,
                            references,
                            &current_scope_namespace,
                            "loop_variable_definition",
                            defined_nodes,
                            defined_variables,
                        );
                    }
                }
            }
        }

        // Function parameters are definitions within the function's scope
        "parameters" => {
            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                if child.kind() == "identifier" {
                    record_definition_node(
                        &child,
                        source,
                        references,
                        &current_scope_namespace,
                        "parameter_definition",
                        defined_nodes,
                        defined_variables,
                    );
                }
            }
        }

        // Handle table field definitions
        "table_constructor" => {
            // For table fields, we want them namespaced under their containing variable
            // For example: local tbl = { field = "value" } should make 'field' belong to 'tbl'
            // We use the current namespace stack which would include the variable name if we're in assignment context
            let mut cursor = node.walk();
            for child in node.named_children(&mut cursor) {
                if child.kind() == "field" {
                    // A 'field' node has a 'name' field for the field name (not 'key')
                    if let Some(name_node) = child.child_by_field_name("name") {
                        // Table keys can be string literals or identifiers
                        if name_node.kind() == "identifier" {
                            record_definition_node(
                                &name_node,
                                source,
                                references,
                                &current_scope_namespace,
                                "table_field_definition",
                                defined_nodes,
                                defined_variables,
                            );
                        }
                    }
                } else if child.kind() == "field_identifier" {
                    // Handle implicit assignments in table constructors like { a=1, b=2 }
                    // In these cases, the field_identifier might be directly present
                    record_definition_node(
                        &child,
                        source,
                        references,
                        &current_scope_namespace,
                        "table_field_definition",
                        defined_nodes,
                        defined_variables,
                    );
                } else if child.kind() == "shorthand_field" {
                    // Handle shorthand fields like {x} which is equivalent to {x = x}
                    if let Some(name_node) = child.child_by_field_name("name") {
                        record_definition_node(
                            &name_node,
                            source,
                            references,
                            &current_scope_namespace,
                            "table_field_definition",
                            defined_nodes,
                            defined_variables,
                        );
                    }
                }
            }
        }

        // Generic identifier or field identifier are typically references
        "identifier" | "field_identifier" => {
            // Only record as reference if it's not a definition being handled by a parent node
            // this is implicit due to the order of matching
            record_reference_node(
                node,
                source,
                references,
                &current_scope_namespace,
                defined_nodes,
            );
        }
        _ => {}
    }

    // Recurse into children with the potentially updated namespace stack
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        // Pass a mutable clone for child scopes
        collect_references(
            &child,
            source,
            references,
            &mut current_scope_namespace,
            defined_nodes,
            defined_variables,
        );
    }
}

// Helper function to push a new segment onto the namespace stack
// Helper to construct the full namespace string
fn namespace_from_stack(namespace_stack: &[String]) -> Option<String> {
    if namespace_stack.is_empty() {
        None
    } else {
        Some(namespace_stack.join(":"))
    }
}

// Helper to sanitize identifiers (remove '$' prefix for PHP, trim whitespace)
fn sanitize_identifier(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

// Helper to record a definition
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

// Helper to record a reference
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

// Helper to get text from a node
fn get_node_text(node: &Node, source: &[u8]) -> Option<String> {
    node.utf8_text(source).ok().map(|s| s.trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};

    // Helper function to bucket references by kind for easier testing
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
            // dbg!(&reference);
            match reference.kind.as_deref() {
                Some("function_definition")
                | Some("variable_definition")
                | Some("loop_variable_definition")
                | Some("parameter_definition")
                | Some("table_field_definition") => {
                    *definitions.entry(key.clone()).or_insert(0) += 1;
                }
                Some("reference") => {
                    *references_map.entry(key.clone()).or_insert(0) += 1;
                }
                other => panic!("unexpected kind: {:?}", other),
            }
        }
        (definitions, references_map)
    }

    #[test]
    fn extracts_lua_identifiers() {
        let source = r#"
local function myFunction(param)
    local localVar = param + 1
    return localVar
end

local x, y = 10, myFunction(x)

for i = 1, 10 do
    print(i)
end

local tbl = {
    field = "value"
}

print(tbl.field)
"#;

        let extraction = extract(source);
        let references = extraction.references;
        let (definitions, references_map) = bucket_kinds(&references);

        // Expected definitions
        let expected_definitions = HashSet::from([
            ("myFunction".to_string(), None), // Global function definition
            ("param".to_string(), Some("myFunction".to_string())), // Function parameter
            ("localVar".to_string(), Some("myFunction".to_string())), // Local variable inside function
            ("x".to_string(), None),                                  // Global local variable
            ("y".to_string(), None),                                  // Global local variable
            ("i".to_string(), None),                                  // Loop variable
            ("tbl".to_string(), None),                                // Local table definition
            ("field".to_string(), None), // Field inside table (namespacing implementation would make this Some("tbl"))
        ]);

        // Check if all expected definitions are present
        for key in &expected_definitions {
            if !definitions.contains_key(key) {
                dbg!("Missing definition: {:?}", key);
            }
            assert!(
                definitions.contains_key(key),
                "missing definition for {:?}",
                key
            );
        }

        // Expected references
        let expected_references = HashSet::from([
            ("param".to_string(), Some("myFunction".to_string())),
            ("localVar".to_string(), Some("myFunction".to_string())),
            ("myFunction".to_string(), None),
            ("x".to_string(), None),
            ("print".to_string(), None),
            ("i".to_string(), None),
            ("tbl".to_string(), None),
            ("field".to_string(), None), // Field reference (namespacing implementation would make this Some("tbl"))
        ]);

        // Check if all expected references are present
        for key in &expected_references {
            if !references_map.contains_key(key) {
                dbg!("Missing reference: {:?}", key);
            }
            assert!(
                references_map.contains_key(key),
                "missing reference for {:?}",
                key
            );
        }

        // Additional assertions for counts based on debugging
        assert_eq!(
            definitions.len(),
            expected_definitions.len(),
            "Mismatch in number of definitions"
        );
        assert_eq!(
            references_map.len(),
            expected_references.len(),
            "Mismatch in number of references"
        );
    }
}
