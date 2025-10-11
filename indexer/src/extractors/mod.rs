use tree_sitter::Parser;

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
                    refs.push(ExtractedReference {
                        name: name.to_string(),
                        kind: None,
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

fn parser_for_language(language: &str) -> Option<Parser> {
    let mut parser = Parser::new();

    let language_result = match language {
        "c" => parser.set_language(&tree_sitter_c::LANGUAGE.into()),
        "c++" | "cpp" => parser.set_language(&tree_sitter_cpp::LANGUAGE.into()),
        "go" => parser.set_language(&tree_sitter_go::LANGUAGE.into()),
        "js" | "javascript" => parser.set_language(&tree_sitter_typescript::language_typescript()),
        "java" | "jvm" => parser.set_language(&tree_sitter_java::language()),
        "nix" => parser.set_language(&tree_sitter_nix::LANGUAGE.into()),
        "objc" | "objective-c" | "objectivec" => {
            parser.set_language(&tree_sitter_objc::LANGUAGE.into())
        }
        "proto" | "protobuf" => parser.set_language(&tree_sitter_proto::LANGUAGE.into()),
        "py" | "python" => parser.set_language(&tree_sitter_python::language()),
        "rust" => parser.set_language(&tree_sitter_rust::language()),
        "swift" => parser.set_language(&tree_sitter_swift::LANGUAGE.into()),
        "ts" | "typescript" => parser.set_language(&tree_sitter_typescript::language_typescript()),
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
        "rust" => &["identifier"],
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
