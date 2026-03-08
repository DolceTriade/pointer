DROP FUNCTION IF EXISTS extract_context_with_highlight(TEXT, TEXT, INT, BOOLEAN);

CREATE OR REPLACE FUNCTION extract_context_with_highlight(
    p_text TEXT,
    p_substring TEXT,
    p_context_lines INT,
    p_case_sensitive BOOLEAN DEFAULT FALSE
)
RETURNS TABLE (
    match_line_number INT,
    snippet_start_line_number INT,
    context_snippet TEXT,
    match_spans JSONB
)
LANGUAGE plpgsql
AS $$
DECLARE
    lines TEXT[];
    max_line_num INT;
    current_line_num INT;
    line_content TEXT;
    end_line_num INT;
    previous_context TEXT;
    flags TEXT;
    search_start INT;
    match_start_char INT;
    match_end_char INT;
    line_offset_bytes INT;
BEGIN
    lines := regexp_split_to_array(COALESCE(p_text, ''), E'\n');
    max_line_num := COALESCE(array_length(lines, 1), 0);
    IF max_line_num = 0 THEN
        RETURN;
    END IF;

    flags := CASE WHEN p_case_sensitive THEN '' ELSE 'i' END;

    FOR current_line_num IN 1..max_line_num LOOP
        line_content := lines[current_line_num];
        IF (
            (p_case_sensitive AND line_content ~ p_substring)
            OR ((NOT p_case_sensitive) AND line_content ~* p_substring)
        ) THEN
            match_line_number := current_line_num;
            snippet_start_line_number := GREATEST(1, current_line_num - p_context_lines);
            end_line_num := LEAST(max_line_num, current_line_num + p_context_lines);
            context_snippet := array_to_string(lines[snippet_start_line_number:end_line_num], E'\n');

            previous_context := CASE
                WHEN current_line_num > snippet_start_line_number
                    THEN array_to_string(lines[snippet_start_line_number:current_line_num - 1], E'\n')
                ELSE ''
            END;
            line_offset_bytes := octet_length(previous_context);
            IF current_line_num > snippet_start_line_number THEN
                line_offset_bytes := line_offset_bytes + 1;
            END IF;

            match_spans := '[]'::jsonb;
            search_start := 1;

            LOOP
                match_start_char := regexp_instr(line_content, p_substring, search_start, 1, 0, flags);
                EXIT WHEN match_start_char = 0;

                match_end_char := regexp_instr(line_content, p_substring, search_start, 1, 1, flags);
                IF match_end_char = 0 THEN
                    match_end_char := char_length(line_content) + 1;
                END IF;

                match_spans := match_spans || jsonb_build_array(
                    jsonb_build_object(
                        'start', line_offset_bytes + octet_length(left(line_content, match_start_char - 1)),
                        'end', line_offset_bytes + octet_length(left(line_content, match_end_char - 1))
                    )
                );

                IF match_end_char <= match_start_char THEN
                    search_start := match_start_char + 1;
                ELSE
                    search_start := match_end_char;
                END IF;
            END LOOP;

            RETURN NEXT;
        END IF;
    END LOOP;

    RETURN;
END;
$$;
