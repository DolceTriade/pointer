mod c;
mod cpp;
mod go;
mod java;
mod javascript;
mod nix;
mod protobuf;
mod python;
mod rust;
mod typescript;

#[derive(Debug, Clone)]
pub struct ExtractedSymbol {
    pub name: String,
    pub kind: String,
    pub namespace: Option<String>,
}

pub fn extract(language: &str, source: &str) -> Vec<ExtractedSymbol> {
    match language {
        "c" => c::extract(source),
        "c++" | "cpp" => cpp::extract(source),
        "go" => go::extract(source),
        "js" | "javascript" => javascript::extract(source),
        "java" => java::extract(source),
        "nix" => nix::extract(source),
        "proto" | "protobuf" => protobuf::extract(source),
        "py" | "python" => python::extract(source),
        "rust" => rust::extract(source),
        "ts" | "typescript" => typescript::extract(source),
        _ => Vec::new(),
    }
}
