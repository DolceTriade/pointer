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
    let mut pending: Vec<PendingScope> = Vec::new();

    for (idx, line) in source.lines().enumerate() {
        let line_no = idx + 1;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        if let Some(label) = detect_scope_label(trimmed) {
            if trimmed.contains('{') {
                push_scope(&mut scopes, &stack, label, line_no);
                stack.push(Some(scopes.len() - 1));
            } else {
                pending.push(PendingScope {
                    label,
                    start_line: line_no,
                });
            }
        }

        for ch in line.chars() {
            match ch {
                '{' => {
                    if let Some(p) = pending.pop() {
                        push_scope(&mut scopes, &stack, p.label, p.start_line);
                        stack.push(Some(scopes.len() - 1));
                    } else {
                        stack.push(None);
                    }
                }
                '}' => {
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

    for (idx, line) in source.lines().enumerate() {
        let line_no = idx + 1;
        if line.trim().is_empty() {
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

        if line.trim_end().ends_with(':') {
            let label = line.trim().trim_end_matches(':').to_string();
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
    let before_brace = line.split('{').next().unwrap_or(line).trim();
    if before_brace.is_empty() {
        None
    } else {
        Some(before_brace.chars().take(120).collect())
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
