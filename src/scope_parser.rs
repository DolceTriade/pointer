use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ScopeInfo {
    pub label: String,
    pub start_line: usize,
    pub end_line: usize,
    pub depth: usize,
    pub parent: Option<usize>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ScopeBreadcrumb {
    pub label: String,
    pub start_line: usize,
}

pub fn extract_scopes(source: &str, language: Option<&str>) -> Vec<ScopeInfo> {
    let lang = language
        .map(|l| l.to_lowercase())
        .unwrap_or_else(|| String::from(""));
    if is_indentation_lang(&lang) {
        parse_indentation_scopes(source)
    } else {
        parse_brace_scopes(source)
    }
}

fn is_indentation_lang(lang: &str) -> bool {
    matches!(
        lang,
        "python" | "py" | "yaml" | "yml" | "ruby" | "rb" | "haskell" | "hs"
    )
}

fn parse_brace_scopes(source: &str) -> Vec<ScopeInfo> {
    #[derive(Clone)]
    struct PendingScope {
        label: String,
        start_line: usize,
    }

    let mut scopes = Vec::new();
    let mut stack: Vec<Option<usize>> = Vec::new();
    let mut pending: Option<PendingScope> = None;
    let mut previous_line_continues = false;

    for (idx, line) in source.lines().enumerate() {
        let line_no = idx + 1;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            previous_line_continues = false;
            continue;
        }

        if let Some(label) = detect_scope_label(trimmed) {
            let should_override = pending.is_none()
                || !previous_line_continues
                || line_definitely_starts_scope(trimmed);
            if should_override {
                pending = Some(PendingScope {
                    label,
                    start_line: line_no,
                });
            }
        }

        let mut opened_scope_this_line = false;
        let mut chars = line.chars().peekable();
        let mut in_block_comment = false;
        let mut in_string: Option<char> = None;
        while let Some(ch) = chars.next() {
            if let Some(delim) = in_string {
                if ch == '\\' {
                    chars.next();
                    continue;
                }
                if ch == delim {
                    in_string = None;
                }
                continue;
            }
            if in_block_comment {
                if ch == '*' {
                    if matches!(chars.peek().copied(), Some('/')) {
                        chars.next();
                        in_block_comment = false;
                    }
                }
                continue;
            }
            match ch {
                '/' => {
                    if matches!(chars.peek().copied(), Some('/')) {
                        break;
                    }
                    if matches!(chars.peek().copied(), Some('*')) {
                        chars.next();
                        in_block_comment = true;
                        continue;
                    }
                }
                '"' | '\'' => {
                    in_string = Some(ch);
                    continue;
                }
                _ => {}
            }
            match ch {
                '{' => {
                    opened_scope_this_line = true;
                    if let Some(p) = pending.take() {
                        push_scope(&mut scopes, &stack, p.label, p.start_line);
                        stack.push(Some(scopes.len() - 1));
                    } else {
                        stack.push(None);
                    }
                }
                '}' => {
                    opened_scope_this_line = true;
                    if let Some(entry) = stack.pop() {
                        if let Some(idx) = entry {
                            let end_line = line_no.max(scopes[idx].start_line);
                            scopes[idx].end_line = end_line;
                        }
                    }
                }
                _ => {}
            }
        }

        let line_continues = line_continues_to_next(trimmed);
        if !opened_scope_this_line
            && !line_continues
            && pending.is_some()
            && line_ends_with_semicolon(trimmed)
        {
            pending = None;
        }

        previous_line_continues = line_continues;
    }

    while let Some(entry) = stack.pop() {
        if let Some(idx) = entry {
            scopes[idx].end_line = scopes[idx].start_line;
        }
    }

    scopes
}

fn parse_indentation_scopes(source: &str) -> Vec<ScopeInfo> {
    let mut scopes: Vec<ScopeInfo> = Vec::new();
    let mut stack: Vec<(usize, usize)> = Vec::new();
    let mut multiline_delimiter: Option<&'static str> = None;

    for (idx, line) in source.lines().enumerate() {
        let line_no = idx + 1;
        let trimmed = line.trim();
        let inside_multiline = multiline_delimiter.is_some();
        if trimmed.is_empty() {
            update_multiline_state(line, &mut multiline_delimiter);
            continue;
        }
        if inside_multiline {
            update_multiline_state(line, &mut multiline_delimiter);
            continue;
        }
        let indent = line
            .chars()
            .take_while(|c| *c == ' ' || *c == '\t')
            .map(|ch| if ch == '\t' { 4 } else { 1 })
            .sum();

        while let Some((prev_indent, scope_idx)) = stack.last().copied() {
            if indent <= prev_indent {
                stack.pop();
                scopes[scope_idx].end_line =
                    line_no.saturating_sub(1).max(scopes[scope_idx].start_line);
            } else {
                break;
            }
        }

        if trimmed.ends_with(':') {
            let label = trimmed.trim_end_matches(':').to_string();
            let parent = stack.last().map(|entry| entry.1);
            let depth = parent.map(|idx| scopes[idx].depth + 1).unwrap_or(0);
            let scope = ScopeInfo {
                label,
                start_line: line_no,
                end_line: line_no,
                depth,
                parent,
            };
            scopes.push(scope);
            stack.push((indent, scopes.len() - 1));
        }
        update_multiline_state(line, &mut multiline_delimiter);
    }

    let final_line = source.lines().count().max(1);
    while let Some((_, idx)) = stack.pop() {
        scopes[idx].end_line = final_line.max(scopes[idx].start_line);
    }

    scopes
}

fn push_scope(
    scopes: &mut Vec<ScopeInfo>,
    stack: &[Option<usize>],
    label: String,
    start_line: usize,
) {
    let parent = stack.iter().rev().find_map(|entry| *entry);
    let depth = parent.map(|idx| scopes[idx].depth + 1).unwrap_or(0);
    scopes.push(ScopeInfo {
        label,
        start_line,
        end_line: start_line,
        depth,
        parent,
    });
}

fn detect_scope_label(line: &str) -> Option<String> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return None;
    }

    let without_leading_closers = trimmed.trim_start_matches(|c| matches!(c, '}' | ')' | ';'));
    if without_leading_closers.is_empty() {
        return None;
    }

    if without_leading_closers.starts_with("//")
        || without_leading_closers.starts_with("/*")
        || without_leading_closers.starts_with('*')
        || without_leading_closers.starts_with('#')
        || without_leading_closers.starts_with("--")
    {
        return None;
    }

    if without_leading_closers == "{" || without_leading_closers == "}" {
        return None;
    }

    if without_leading_closers.ends_with(',') {
        return None;
    }

    let lower = without_leading_closers.to_ascii_lowercase();
    if lower.starts_with("type ") && !without_leading_closers.contains('{') {
        return None;
    }

    let before_brace = without_leading_closers
        .split('{')
        .next()
        .unwrap_or(without_leading_closers)
        .trim();

    if before_brace.is_empty() {
        return None;
    }

    let looks_like_scope = without_leading_closers.contains('{')
        || line_definitely_starts_scope(without_leading_closers)
        || line_looks_like_signature(before_brace);

    if !looks_like_scope {
        return None;
    }

    Some(before_brace.chars().take(120).collect())
}

fn has_assignment_operator(segment: &str) -> bool {
    let bytes = segment.as_bytes();
    let mut idx = 0;
    while idx < bytes.len() {
        match bytes[idx] {
            b'=' => {
                let prev = if idx > 0 { bytes[idx - 1] } else { 0 };
                let next = if idx + 1 < bytes.len() {
                    bytes[idx + 1]
                } else {
                    0
                };
                let part_of_comparison =
                    matches!(prev, b'<' | b'>' | b'=' | b'!') || matches!(next, b'=' | b'>');
                if !part_of_comparison {
                    return true;
                }
            }
            b':' => {
                if idx + 1 < bytes.len() && bytes[idx + 1] == b'=' {
                    return true;
                }
            }
            _ => {}
        }
        idx += 1;
    }
    false
}

fn line_looks_like_signature(segment: &str) -> bool {
    if let Some(idx) = segment.find('(') {
        let before = segment[..idx].trim();
        if before.is_empty() {
            return false;
        }
        if before.ends_with(',') {
            return false;
        }
        if has_assignment_operator(before) {
            return false;
        }
        true
    } else {
        false
    }
}

fn line_definitely_starts_scope(line: &str) -> bool {
    let stripped = line
        .trim_start_matches(|c| matches!(c, '}' | ')' | ';'))
        .to_ascii_lowercase();
    const KEYWORDS: [&str; 24] = [
        "if ",
        "if(",
        "else",
        "else if",
        "for ",
        "for(",
        "while",
        "switch",
        "case ",
        "fn ",
        "fn(",
        "func ",
        "function",
        "def ",
        "class ",
        "struct ",
        "enum ",
        "impl",
        "trait",
        "interface",
        "namespace",
        "module",
        "match",
        "loop",
    ];
    KEYWORDS.iter().any(|kw| stripped.starts_with(kw))
}

fn line_continues_to_next(line: &str) -> bool {
    let trimmed = line.trim_end();
    if trimmed.is_empty() {
        return false;
    }
    let trailing = trimmed.as_bytes();
    if trailing.ends_with(b"||") || trailing.ends_with(b"&&") {
        return true;
    }
    match trimmed.chars().last().unwrap_or_default() {
        ',' | '\\' | '+' | '-' | '*' | '/' | '%' | ':' | '?' | '(' | '.' | '&' | '|' => {
            return true;
        }
        '=' => return true,
        _ => {}
    }
    trimmed.ends_with("->") || trimmed.ends_with("=>")
}

fn line_ends_with_semicolon(line: &str) -> bool {
    matches!(last_code_char_before_comment(line), Some(';'))
}

fn last_code_char_before_comment(line: &str) -> Option<char> {
    let mut chars = line.chars().peekable();
    let mut last = None;
    let mut in_block_comment = false;
    let mut in_string: Option<char> = None;

    while let Some(ch) = chars.next() {
        if let Some(delim) = in_string {
            if ch == '\\' {
                chars.next();
                continue;
            }
            if ch == delim {
                in_string = None;
            }
            continue;
        }
        if in_block_comment {
            if ch == '*' {
                if matches!(chars.peek().copied(), Some('/')) {
                    chars.next();
                    in_block_comment = false;
                }
            }
            continue;
        }

        match ch {
            '/' => {
                if matches!(chars.peek().copied(), Some('/')) {
                    break;
                }
                if matches!(chars.peek().copied(), Some('*')) {
                    chars.next();
                    in_block_comment = true;
                    continue;
                }
            }
            '"' | '\'' => {
                in_string = Some(ch);
                continue;
            }
            _ => {}
        }

        if !ch.is_whitespace() {
            last = Some(ch);
        }
    }

    last
}

fn update_multiline_state(line: &str, state: &mut Option<&'static str>) {
    let bytes = line.as_bytes();
    let mut idx = 0;
    while idx + 2 < bytes.len() {
        if bytes[idx] == b'\\' {
            idx += 1;
            continue;
        }
        if &bytes[idx..idx + 3] == b"\"\"\"" {
            if matches!(state, Some(current) if *current == "\"\"\"") {
                *state = None;
            } else if state.is_none() {
                *state = Some("\"\"\"");
            }
            idx += 3;
            continue;
        }
        if &bytes[idx..idx + 3] == b"'''" {
            if matches!(state, Some(current) if *current == "'''") {
                *state = None;
            } else if state.is_none() {
                *state = Some("'''");
            }
            idx += 3;
            continue;
        }
        idx += 1;
    }
}

pub fn scope_chain_for_line(scopes: &[ScopeInfo], top_line: usize) -> Vec<ScopeBreadcrumb> {
    if scopes.is_empty() {
        return Vec::new();
    }
    let mut candidate: Option<usize> = None;
    for (idx, scope) in scopes.iter().enumerate() {
        if scope.start_line < top_line && scope.end_line >= top_line {
            candidate = match candidate {
                Some(current) => {
                    if scope.depth >= scopes[current].depth {
                        Some(idx)
                    } else {
                        Some(current)
                    }
                }
                None => Some(idx),
            };
        }
    }

    let mut chain = Vec::new();
    let mut current = candidate;
    while let Some(idx) = current {
        let scope = &scopes[idx];
        chain.push(ScopeBreadcrumb {
            label: scope.label.clone(),
            start_line: scope.start_line,
        });
        current = scope.parent;
    }
    chain.reverse();
    chain
}

pub fn visible_scope_chain(
    scopes: &[ScopeInfo],
    visible_start: usize,
    visible_end: usize,
) -> Vec<ScopeBreadcrumb> {
    if scopes.is_empty() || visible_start > visible_end {
        return Vec::new();
    }

    for line in visible_start..=visible_end {
        let mut chain = scope_chain_for_line(scopes, line);
        if chain.is_empty() {
            continue;
        }
        chain.retain(|crumb| crumb.start_line < visible_start);
        if !chain.is_empty() {
            return chain;
        }
    }

    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    const GO_SNIPPET: &str = r#"// ManifestFile specifies the location of the runfile manifest file.  You can
// pass this as an option to New.  If unset or empty, use the value of the
// environmental variable RUNFILES_MANIFEST_FILE.
type ManifestFile string

func (f ManifestFile) new(sourceRepo SourceRepo) (*Runfiles, error) {
    m, err := f.parse()
    if err != nil {
        return nil, err
    }
    env := []string{
        manifestFileVar + "=" + string(f),
    }
    // Certain tools (e.g., Java tools) may need the runfiles directory, so try to find it even if
    // running with a manifest file.
    if strings.HasSuffix(string(f), ".runfiles_manifest") ||
        strings.HasSuffix(string(f), "/MANIFEST") ||
        strings.HasSuffix(string(f), "\\MANIFEST") {
        // Cut off either "_manifest" or "/MANIFEST" or "\\MANIFEST", all of length 9, from the end
        // of the path to obtain the runfiles directory.
        d := string(f)[:len(string(f))-len("_manifest")]
        env = append(env,
            directoryVar+"="+d,
            legacyDirectoryVar+"="+d)
    }
    r := &Runfiles{
        impl:       &m,
        env:        env,
        sourceRepo: string(sourceRepo),
    }
    err = r.loadRepoMapping()
    return r, err
}
"#;

    const PYTHON_SNIPPET: &str = r#"TTS_92 = """
tts:
  - platform: google_translate
    service_name: google_say
"""


class ConfigErrorTranslationKey(StrEnum):
    """Config error translation keys for config errors."""

    # translation keys with a generated config related message text
    CONFIG_VALIDATION_ERR = "config_validation_err"
    PLATFORM_CONFIG_VALIDATION_ERR = "platform_config_validation_err"

    # translation keys with a general static message text
    COMPONENT_IMPORT_ERR = "component_import_err"
    CONFIG_PLATFORM_IMPORT_ERR = "config_platform_import_err"
    CONFIG_VALIDATOR_UNKNOWN_ERR = "config_validator_unknown_err"
    CONFIG_SCHEMA_UNKNOWN_ERR = "config_schema_unknown_err"
    PLATFORM_COMPONENT_LOAD_ERR = "platform_component_load_err"
    PLATFORM_COMPONENT_LOAD_EXC = "platform_component_load_exc"
    PLATFORM_SCHEMA_VALIDATOR_ERR = "platform_schema_validator_err"

    # translation key in case multiple errors occurred
    MULTIPLE_INTEGRATION_CONFIG_ERRORS = "multiple_integration_config_errors"


_CONFIG_LOG_SHOW_STACK_TRACE: dict[ConfigErrorTranslationKey, bool] = {
    ConfigErrorTranslationKey.COMPONENT_IMPORT_ERR: False,
    ConfigErrorTranslationKey.CONFIG_PLATFORM_IMPORT_ERR: False,
    ConfigErrorTranslationKey.CONFIG_VALIDATOR_UNKNOWN_ERR: True,
    ConfigErrorTranslationKey.CONFIG_SCHEMA_UNKNOWN_ERR: True,
    ConfigErrorTranslationKey.PLATFORM_COMPONENT_LOAD_ERR: False,
    ConfigErrorTranslationKey.PLATFORM_COMPONENT_LOAD_EXC: True,
    ConfigErrorTranslationKey.PLATFORM_SCHEMA_VALIDATOR_ERR: True,
}


@dataclass
class ConfigExceptionInfo:
    """Configuration exception info class."""

    exception: Exception
    translation_key: ConfigErrorTranslationKey
    platform_path: str
    config: ConfigType
    integration_link: str | None


@dataclass
class IntegrationConfigInfo:
    """Configuration for an integration and exception information."""

    config: ConfigType | None
    exception_info_list: list[ConfigExceptionInfo]


def get_default_config_dir() -> str:
    """Put together the default configuration directory based on the OS."""
    data_dir = os.path.expanduser("~")
    return os.path.join(data_dir, CONFIG_DIR_NAME)
"#;

    fn cpp_scope_labels(source: &str) -> Vec<String> {
        extract_scopes(source, Some("cpp"))
            .into_iter()
            .map(|scope| scope.label)
            .collect()
    }

    #[test]
    fn detects_nested_brace_scopes() {
        let source = r#"
        struct Foo {
            fn bar() {
                if (baz) {
                }
            }
        }
        "#;
        let scopes = extract_scopes(source, Some("rust"));
        assert_eq!(scopes.len(), 3);
        let chain = scope_chain_for_line(&scopes, 5);
        assert_eq!(
            chain,
            vec![
                ScopeBreadcrumb {
                    label: "struct Foo".to_string(),
                    start_line: 2
                },
                ScopeBreadcrumb {
                    label: "fn bar()".to_string(),
                    start_line: 3
                },
                ScopeBreadcrumb {
                    label: "if (baz)".to_string(),
                    start_line: 4
                },
            ]
        );
    }

    #[test]
    fn detects_python_scopes() {
        let source = r#"
def foo():
    for x in xs:
        if x > 0:
            print(x)
        "#;
        let scopes = extract_scopes(source, Some("python"));
        assert_eq!(scopes.len(), 3);
        let chain = scope_chain_for_line(&scopes, 4);
        assert_eq!(chain.len(), 2);
        assert_eq!(chain[0].label, "def foo()");
    }

    #[test]
    fn chain_empty_outside_scope() {
        let source = "fn foo() {}\n";
        let scopes = extract_scopes(source, Some("rust"));
        let chain = scope_chain_for_line(&scopes, 1);
        assert!(chain.is_empty());
    }

    #[test]
    fn handles_pending_scope_before_brace() {
        let source = r#"
class Foo
{
public:
    Foo() {}
};
"#;
        let scopes = extract_scopes(source, Some("cpp"));
        assert!(!scopes.is_empty());
        assert_eq!(scopes[0].label.starts_with("class Foo"), true);
    }

    #[test]
    fn indentation_multiple_levels() {
        let source = r#"
def outer():
    if check:
        for value in values:
            print(value)
"#;
        let scopes = extract_scopes(source, Some("python"));
        assert_eq!(scopes.len(), 3);
        let chain = scope_chain_for_line(&scopes, 5);
        assert_eq!(chain.len(), 3);
        assert_eq!(chain[0].label, "def outer()");
        assert_eq!(chain[1].label.trim(), "if check");
        assert!(chain[2].label.contains("for value"));
    }

    #[test]
    fn visible_scope_returns_hidden_scope() {
        let source = r#"fn foo() {
    let x = 1;
}

fn bar() {}
"#;
        let scopes = extract_scopes(source, Some("rust"));
        let chain = visible_scope_chain(&scopes, 3, 4);
        assert_eq!(
            chain,
            vec![ScopeBreadcrumb {
                label: "fn foo()".to_string(),
                start_line: 1
            }]
        );
    }

    #[test]
    fn visible_scope_requires_hidden_definition() {
        let source = r#"fn foo() {
    let x = 1;
}
"#;
        let scopes = extract_scopes(source, Some("rust"));
        let chain = visible_scope_chain(&scopes, 1, 3);
        assert!(chain.is_empty());
    }

    #[test]
    fn visible_scope_handles_nested_scopes() {
        let source = r#"fn outer() {
    if ready {
        println!("inner");
    }
}
"#;
        let scopes = extract_scopes(source, Some("rust"));
        let chain = visible_scope_chain(&scopes, 3, 4);
        assert_eq!(
            chain,
            vec![
                ScopeBreadcrumb {
                    label: "fn outer()".to_string(),
                    start_line: 1
                },
                ScopeBreadcrumb {
                    label: "if ready".to_string(),
                    start_line: 2
                }
            ]
        );

        let chain = visible_scope_chain(&scopes, 2, 4);
        assert_eq!(
            chain,
            vec![ScopeBreadcrumb {
                label: "fn outer()".to_string(),
                start_line: 1
            }]
        );
    }

    #[test]
    fn brace_parser_discards_semicolon_statements_with_trailing_comments() {
        let source = r#"
void Example() {
    if (condition) {
        glCullFace(GL_FRONT); // comment mentioning {
    } else {
        glCullFace(GL_BACK);
    }
}
"#;
        let labels = cpp_scope_labels(source);
        assert!(
            labels.iter().any(|label| label.contains("Example")),
            "function scope missing in {:?}",
            labels
        );
        assert!(
            !labels.iter().any(|label| label.contains("glCullFace")),
            "function calls were incorrectly promoted to scopes: {:?}",
            labels
        );
    }

    #[test]
    fn brace_parser_keeps_pending_label_for_multiline_signatures() {
        let source = r#"
static
void ComplexFunction(
    int a,
    int b
)
{
    if (a > b) {
        return;
    }
}
"#;
        let labels = cpp_scope_labels(source);
        assert!(
            labels.iter().any(|label| label.contains("ComplexFunction")),
            "multiline signature did not produce a scope: {:?}",
            labels
        );
    }

    #[test]
    fn semicolon_detection_ignores_trailing_comments() {
        let line = "    glCullFace( GL_FRONT ); // comment mentioning {";
        assert!(
            line_ends_with_semicolon(line),
            "line should be treated as semicolon-terminated"
        );
    }

    #[test]
    fn go_scope_visibility_cases() {
        let scopes = extract_scopes(GO_SNIPPET, Some("go"));
        assert!(
            scopes
                .iter()
                .any(|scope| scope.label.starts_with("func (f ManifestFile) new"))
        );

        // When the function signature is visible, no breadcrumbs should render.
        let chain = visible_scope_chain(&scopes, 6, 8);
        assert!(chain.is_empty());

        // Scrolling past the signature should show the enclosing function.
        let chain = visible_scope_chain(&scopes, 10, 14);
        assert_eq!(
            chain,
            vec![
                ScopeBreadcrumb {
                    label: "func (f ManifestFile) new(sourceRepo SourceRepo) (*Runfiles, error)"
                        .to_string(),
                    start_line: 6
                },
                ScopeBreadcrumb {
                    label: "if err != nil".to_string(),
                    start_line: 8
                }
            ]
        );

        // Inside the env literal the stack should include the function and the literal.
        let chain = visible_scope_chain(&scopes, 12, 13);
        assert_eq!(
            chain,
            vec![
                ScopeBreadcrumb {
                    label: "func (f ManifestFile) new(sourceRepo SourceRepo) (*Runfiles, error)"
                        .to_string(),
                    start_line: 6
                },
                ScopeBreadcrumb {
                    label: "env := []string".to_string(),
                    start_line: 11
                }
            ]
        );

        // Within the HasSuffix condition, the function and if block should be present.
        let chain = visible_scope_chain(&scopes, 19, 23);
        assert_eq!(
            chain,
            vec![
                ScopeBreadcrumb {
                    label: "func (f ManifestFile) new(sourceRepo SourceRepo) (*Runfiles, error)"
                        .to_string(),
                    start_line: 6
                },
                ScopeBreadcrumb {
                    label: "if strings.HasSuffix(string(f), \".runfiles_manifest\") ||".to_string(),
                    start_line: 16
                }
            ]
        );

        // Within the struct literal, the breadcrumb should include the literal scope.
        let chain = visible_scope_chain(&scopes, 27, 31);
        assert_eq!(
            chain,
            vec![
                ScopeBreadcrumb {
                    label: "func (f ManifestFile) new(sourceRepo SourceRepo) (*Runfiles, error)"
                        .to_string(),
                    start_line: 6
                },
                ScopeBreadcrumb {
                    label: "r := &Runfiles".to_string(),
                    start_line: 26
                }
            ]
        );

        // After the struct literal closes, only the function should remain.
        let chain = visible_scope_chain(&scopes, 31, 32);
        assert_eq!(
            chain,
            vec![ScopeBreadcrumb {
                label: "func (f ManifestFile) new(sourceRepo SourceRepo) (*Runfiles, error)"
                    .to_string(),
                start_line: 6
            }]
        );
    }

    #[test]
    fn python_scope_visibility_cases() {
        let scopes = extract_scopes(PYTHON_SNIPPET, Some("python"));
        assert!(
            scopes
                .iter()
                .any(|scope| scope.label.starts_with("class ConfigErrorTranslationKey"))
        );
        assert!(scopes.iter().all(|scope| !scope.label.starts_with("tts")));

        // When the class definition is visible, breadcrumbs stay empty.
        let chain = visible_scope_chain(&scopes, 8, 9);
        assert!(chain.is_empty());

        // Scrolling past the class header shows the class scope.
        let chain = visible_scope_chain(&scopes, 12, 18);
        assert_eq!(
            chain,
            vec![ScopeBreadcrumb {
                label: "class ConfigErrorTranslationKey(StrEnum)".to_string(),
                start_line: 8
            }]
        );

        // Inside the dataclass, we should see the enclosing class.
        let chain = visible_scope_chain(&scopes, 44, 47);
        assert_eq!(
            chain,
            vec![ScopeBreadcrumb {
                label: "class ConfigExceptionInfo".to_string(),
                start_line: 40
            }]
        );

        // Similarly for the integration config class.
        let chain = visible_scope_chain(&scopes, 54, 55);
        assert_eq!(
            chain,
            vec![ScopeBreadcrumb {
                label: "class IntegrationConfigInfo".to_string(),
                start_line: 51
            }]
        );

        // The function should appear when its signature is off screen.
        let chain = visible_scope_chain(&scopes, 59, 60);
        assert_eq!(
            chain,
            vec![ScopeBreadcrumb {
                label: "def get_default_config_dir() -> str".to_string(),
                start_line: 58
            }]
        );
    }
}
