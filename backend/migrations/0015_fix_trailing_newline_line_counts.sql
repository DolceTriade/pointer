UPDATE content_blob_chunks cbc
SET chunk_line_count = CASE
    WHEN chunks.text_content = '' THEN 0
    ELSE (
        length(chunks.text_content) - length(replace(chunks.text_content, E'\n', ''))
        + CASE
            WHEN right(chunks.text_content, 1) = E'\n' THEN 0
            ELSE 1
        END
    )
END
FROM chunks
WHERE chunks.chunk_hash = cbc.chunk_hash
  AND cbc.chunk_line_count IS DISTINCT FROM CASE
        WHEN chunks.text_content = '' THEN 0
        ELSE (
            length(chunks.text_content) - length(replace(chunks.text_content, E'\n', ''))
            + CASE
                WHEN right(chunks.text_content, 1) = E'\n' THEN 0
                ELSE 1
            END
        )
    END;

WITH blob_counts AS (
    SELECT
        cbc.content_hash AS hash,
        COALESCE(SUM(cbc.chunk_line_count), 0) AS corrected_line_count
    FROM content_blob_chunks cbc
    GROUP BY cbc.content_hash
)
UPDATE content_blobs cb
SET line_count = blob_counts.corrected_line_count
FROM blob_counts
WHERE blob_counts.hash = cb.hash
  AND cb.line_count IS DISTINCT FROM blob_counts.corrected_line_count;
