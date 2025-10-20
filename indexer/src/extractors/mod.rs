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
}

#[derive(Debug, Clone)]
pub struct ExtractedReference {
    pub name: String,
    pub kind: Option<String>, // e.g., "definition", "reference", "declaration"
    pub namespace: Option<String>,
    pub line: usize,
    pub column: usize,
}

#[derive(Debug, Clone, Default)]
pub struct Extraction {
    pub references: Vec<ExtractedReference>,
}

impl From<Vec<ExtractedReference>> for Extraction {
    fn from(references: Vec<ExtractedReference>) -> Self {
        Self { references }
    }
}

// Define the trait for language-specific indexing
pub trait LanguageIndexer {
    fn index(&self, source: &str) -> Extraction;
}

// Implement the trait for each language
pub struct CIndexer;
pub struct CppIndexer;
pub struct GoIndexer;
pub struct JavaIndexer;
pub struct JavaScriptIndexer;
pub struct NixIndexer;
pub struct ObjectiveCIndexer;
pub struct ProtobufIndexer;
pub struct PythonIndexer;
pub struct RustIndexer;
pub struct SwiftIndexer;
pub struct TypeScriptIndexer;

impl LanguageIndexer for CIndexer {
    fn index(&self, source: &str) -> Extraction {
        c::extract(source)
    }
}

impl LanguageIndexer for CppIndexer {
    fn index(&self, source: &str) -> Extraction {
        cpp::extract(source)
    }
}

impl LanguageIndexer for GoIndexer {
    fn index(&self, source: &str) -> Extraction {
        go::extract(source)
    }
}

impl LanguageIndexer for JavaIndexer {
    fn index(&self, source: &str) -> Extraction {
        java::extract(source)
    }
}

impl LanguageIndexer for JavaScriptIndexer {
    fn index(&self, source: &str) -> Extraction {
        javascript::extract(source)
    }
}

impl LanguageIndexer for NixIndexer {
    fn index(&self, source: &str) -> Extraction {
        nix::extract(source)
    }
}

impl LanguageIndexer for ObjectiveCIndexer {
    fn index(&self, source: &str) -> Extraction {
        objective_c::extract(source)
    }
}

impl LanguageIndexer for ProtobufIndexer {
    fn index(&self, source: &str) -> Extraction {
        protobuf::extract(source)
    }
}

impl LanguageIndexer for PythonIndexer {
    fn index(&self, source: &str) -> Extraction {
        python::extract(source)
    }
}

impl LanguageIndexer for RustIndexer {
    fn index(&self, source: &str) -> Extraction {
        rust::extract(source)
    }
}

impl LanguageIndexer for SwiftIndexer {
    fn index(&self, source: &str) -> Extraction {
        swift::extract(source)
    }
}

impl LanguageIndexer for TypeScriptIndexer {
    fn index(&self, source: &str) -> Extraction {
        typescript::extract(source)
    }
}

// Main extraction function using the new architecture
pub fn extract(language: &str, source: &str) -> Extraction {
    match language {
        "c" => CIndexer.index(source),
        "c++" | "cpp" => CppIndexer.index(source),
        "go" => GoIndexer.index(source),
        "js" | "javascript" => JavaScriptIndexer.index(source),
        "java" | "jvm" => JavaIndexer.index(source),
        "nix" => NixIndexer.index(source),
        "objc" | "objective-c" | "objectivec" => ObjectiveCIndexer.index(source),
        "proto" | "protobuf" => ProtobufIndexer.index(source),
        "py" | "python" => PythonIndexer.index(source),
        "rust" => RustIndexer.index(source),
        "swift" => SwiftIndexer.index(source),
        "ts" | "typescript" => TypeScriptIndexer.index(source),
        _ => Extraction::default(),
    }
}
