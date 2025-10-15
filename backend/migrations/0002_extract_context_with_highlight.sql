CREATE OR REPLACE FUNCTION extract_context_with_highlight(
    p_text TEXT,
    p_substring TEXT,
    p_context_lines INT,
    p_case_sensitive BOOLEAN DEFAULT FALSE
)
RETURNS TABLE (
    match_line_number INT,
    context_snippet TEXT
)
LANGUAGE sql
AS $$
WITH
-- 1. Split the text once into a virtual table of lines and assign line numbers (ORDINALITY).
lines AS (
    SELECT
        line_num,
        line_content
    FROM
        unnest(regexp_split_to_array(p_text, E'\n')) WITH ORDINALITY AS t(line_content, line_num)
),
-- 2. Identify all lines that contain the substring match.
matches AS (
    SELECT
        line_num AS matched_line_num
    FROM
        lines
    WHERE (
        CASE
            WHEN p_case_sensitive THEN line_content ~ p_substring
            ELSE line_content ~* p_substring
        END
    )
)
-- 3. For each match, generate the context window and aggregate the lines.
SELECT
    m.matched_line_num,
    -- Aggregate the context lines back into a single string, preserving order.
    string_agg(
        CASE
            WHEN l_context.line_num = m.matched_line_num
            THEN regexp_replace(
                l_context.line_content,
                '(' || p_substring || ')',
                '<mark>\1</mark>',
                CASE
                    WHEN p_case_sensitive THEN 'g'
                    ELSE 'gi'
                END
            )
            ELSE l_context.line_content
        END, E'\n' ORDER BY context_line_num
    ) AS context_snippet
FROM
    matches m
-- LATERAL joins allow us to reference columns from the 'matches' table (m.matched_line_num)
-- to calculate dynamic boundaries for the context window.
CROSS JOIN LATERAL (
    -- Calculate the boundary limits for the context window: [matched_line_num - N, matched_line_num + N]
    SELECT
        GREATEST(1, m.matched_line_num - p_context_lines) AS start_line,
        LEAST((SELECT MAX(line_num) FROM lines), m.matched_line_num + p_context_lines) AS end_line
) AS bounds
-- Generate all line numbers (indices) within the calculated bounds for the context.
CROSS JOIN LATERAL generate_series(bounds.start_line, bounds.end_line) AS context_line_num
-- 4. Join back to the original 'lines' table to get the text content for the context lines.
JOIN
    lines l_context ON l_context.line_num = context_line_num
GROUP BY
    m.matched_line_num
ORDER BY
    m.matched_line_num;
$$;
