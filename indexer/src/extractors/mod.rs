use tree_sitter::{Node, Parser};

mod c;
mod cpp;
mod go;
mod java;
mod javascript;
mod nix;
mod objective_c;
mod protobuf;
mod python;
mod rust;
mod swift;
mod typescript;

#[derive(Debug, Clone)]
pub struct ExtractedSymbol {
    pub name: String,
    pub kind: String,
    pub namespace: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ExtractedReference {
    pub name: String,
    pub kind: Option<String>,
    pub namespace: Option<String>,
    pub line: usize,
    pub column: usize,
}

#[derive(Debug, Clone, Default)]
pub struct Extraction {
    pub symbols: Vec<ExtractedSymbol>,
    pub references: Vec<ExtractedReference>,
}

pub fn extract(language: &str, source: &str) -> Extraction {
    let symbols = match language {
        "c" => c::extract(source),
        "c++" | "cpp" => cpp::extract(source),
        "go" => go::extract(source),
        "js" | "javascript" => javascript::extract(source),
        "java" | "jvm" => java::extract(source),
        "nix" => nix::extract(source),
        "objc" | "objective-c" | "objectivec" => objective_c::extract(source),
        "proto" | "protobuf" => protobuf::extract(source),
        "py" | "python" => python::extract(source),
        "rust" => rust::extract(source),
        "swift" => swift::extract(source),
        "ts" | "typescript" => typescript::extract(source),
        _ => Vec::new(),
    };

    let references = collect_references(language, source);

    Extraction {
        symbols,
        references,
    }
}

fn collect_references(language: &str, source: &str) -> Vec<ExtractedReference> {
    let mut parser = match parser_for_language(language) {
        Some(parser) => parser,
        None => return Vec::new(),
    };

    let tree = match parser.parse(source, None) {
        Some(tree) => tree,
        None => return Vec::new(),
    };

    let root = tree.root_node();
    let mut refs = Vec::new();
    let mut stack = vec![root];
    let source_bytes = source.as_bytes();
    let identifier_kinds = identifier_kinds(language);

    while let Some(node) = stack.pop() {
        if identifier_kinds.iter().any(|&kind| kind == node.kind()) {
            if let Ok(text) = node.utf8_text(source_bytes) {
                let name = text.trim();
                if !name.is_empty() {
                    let pos = node.start_position();
                    let reference_kind = classify_reference_kind(language, &node);
                    refs.push(ExtractedReference {
                        name: name.to_string(),
                        kind: Some(reference_kind.to_string()),
                        namespace: None,
                        line: pos.row.saturating_add(1) as usize,
                        column: pos.column.saturating_add(1) as usize,
                    });
                }
            }
        }

        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            stack.push(child);
        }
    }

    refs
}

fn classify_reference_kind(language: &str, node: &Node) -> &'static str {
    let mut current = node.parent();
    let mut depth = 0;
    let definitions = definition_parent_kinds(language);
    let def_fields = definition_name_fields(language);

    while let Some(parent) = current {
        if definitions.iter().any(|kind| *kind == parent.kind()) {
            if let Some(field) = field_name_for_child(&parent, node) {
                if def_fields.iter().any(|f| *f == field) {
                    return "definition";
                }
            }
        }

        depth += 1;
        if depth >= 6 {
            break;
        }
        current = parent.parent();
    }

    "reference"
}

fn definition_parent_kinds(language: &str) -> &'static [&'static str] {
    match language {
        "rust" => &[
            "function_item",
            "method_item",
            "struct_item",
            "enum_item",
            "trait_item",
            "mod_item",
            "const_item",
            "static_item",
            "type_item",
            "field_declaration",
            "tuple_field_declaration",
            "let_declaration",
            "impl_item",
        ],
        "go" => &[
            "function_declaration",
            "method_declaration",
            "type_spec",
            "short_var_declaration",
            "var_spec",
            "const_spec",
        ],
        "js" | "javascript" => &[
            "function_declaration",
            "generator_function_declaration",
            "class_declaration",
            "method_definition",
            "public_field_definition",
            "property_definition",
            "lexical_declaration",
            "variable_declaration",
            "assignment_pattern",
        ],
        "ts" | "typescript" => &[
            "function_declaration",
            "generator_function_declaration",
            "class_declaration",
            "interface_declaration",
            "type_alias_declaration",
            "enum_declaration",
            "namespace_declaration",
            "internal_module",
            "method_signature",
            "method_definition",
            "property_signature",
            "property_declaration",
            "lexical_declaration",
            "variable_declaration",
        ],
        "python" | "py" => &[
            "function_definition",
            "class_definition",
            "assignment",
        ],
        "java" | "jvm" => &[
            "class_declaration",
            "interface_declaration",
            "enum_declaration",
            "record_declaration",
            "annotation_type_declaration",
            "method_declaration",
            "constructor_declaration",
            "field_declaration",
            "local_variable_declaration",
        ],
        "c" => &[
            "function_definition",
            "struct_specifier",
            "union_specifier",
            "enum_specifier",
            "declaration",
        ],
        "c++" | "cpp" => &[
            "function_definition",
            "struct_specifier",
            "union_specifier",
            "class_specifier",
            "namespace_definition",
            "field_declaration",
            "simple_declaration",
        ],
        "objc" | "objective-c" | "objectivec" => &[
            "class_interface",
            "class_implementation",
            "category_interface",
            "category_implementation",
            "protocol_declaration",
            "function_definition",
            "method_definition",
            "property_declaration",
            "declaration",
        ],
        "swift" => &[
            "class_declaration",
            "struct_declaration",
            "enum_declaration",
            "protocol_declaration",
            "extension_declaration",
            "function_declaration",
            "initializer_declaration",
            "deinitializer_declaration",
            "variable_declaration",
            "property_declaration",
        ],
        "proto" | "protobuf" => &[
            "message",
            "enum",
            "service",
            "rpc",
            "extend",
            "field",
        ],
        "nix" => &["binding"],
        _ => &[],
    }
}

fn definition_name_fields(language: &str) -> &'static [&'static str] {
    match language {
        "js" | "javascript" | "ts" | "typescript" => &["name", "property"],
        "swift" => &["name", "identifier"],
        "c" | "c++" | "cpp" => &["name", "identifier"],
        "rust" => &["name"],
        "go" => &["name"],
        "java" | "jvm" => &["name"],
        "objc" | "objective-c" | "objectivec" => &["name"],
        "proto" | "protobuf" => &["name"],
        "python" | "py" => &["name"],
        "nix" => &["name"],
        _ => &["name"],
    }
}

fn field_name_for_child(parent: &Node, child: &Node) -> Option<&'static str> {
    for i in 0..parent.child_count() {
        if let Some(candidate) = parent.child(i) {
            if candidate == *child {
                return parent.field_name_for_child(i as u32);
            }
        }
    }
    None
}

fn parser_for_language(language: &str) -> Option<Parser> {
    let mut parser = Parser::new();

    let language_result = match language {
        "c" => parser.set_language(&tree_sitter_c::LANGUAGE.into()),
        "c++" | "cpp" => parser.set_language(&tree_sitter_cpp::LANGUAGE.into()),
        "go" => parser.set_language(&tree_sitter_go::LANGUAGE.into()),
        "js" | "javascript" => {
            parser.set_language(&tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into())
        }
        "java" | "jvm" => parser.set_language(&tree_sitter_java::LANGUAGE.into()),
        "nix" => parser.set_language(&tree_sitter_nix::LANGUAGE.into()),
        "objc" | "objective-c" | "objectivec" => {
            parser.set_language(&tree_sitter_objc::LANGUAGE.into())
        }
        "proto" | "protobuf" => parser.set_language(&tree_sitter_proto::LANGUAGE.into()),
        "py" | "python" => parser.set_language(&tree_sitter_python::LANGUAGE.into()),
        "rust" => parser.set_language(&tree_sitter_rust::LANGUAGE.into()),
        "swift" => parser.set_language(&tree_sitter_swift::LANGUAGE.into()),
        "ts" | "typescript" => {
            parser.set_language(&tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into())
        }
        _ => return None,
    };

    language_result.ok()?;
    Some(parser)
}

fn identifier_kinds(language: &str) -> &'static [&'static str] {
    match language {
        "c" | "proto" | "protobuf" => &["identifier", "field_identifier"],
        "c++" | "cpp" => &[
            "identifier",
            "field_identifier",
            "scoped_identifier",
            "type_identifier",
        ],
        "go" => &["identifier"],
        "js" | "javascript" => &[
            "identifier",
            "property_identifier",
            "shorthand_property_identifier",
        ],
        "java" | "jvm" => &["identifier"],
        "nix" => &["identifier"],
        "objc" | "objective-c" | "objectivec" => &["identifier", "field_identifier"],
        "py" | "python" => &["identifier"],
        "rust" => &["identifier", "type_identifier", "field_identifier"],
        "swift" => &[
            "identifier",
            "simple_identifier",
            "bound_identifier",
            "identifier_pattern",
        ],
        "ts" | "typescript" => &[
            "identifier",
            "property_identifier",
            "shorthand_property_identifier",
        ],
        _ => &["identifier"],
    }
}
