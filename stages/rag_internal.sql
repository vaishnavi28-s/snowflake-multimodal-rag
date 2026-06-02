
-- STAGE: RAG_INTERNAL
-- Stores raw PDFs and paginated outputs


CREATE STAGE IF NOT EXISTS RAG.PUBLIC.RAG_INTERNAL
    DIRECTORY  = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    COMMENT    = 'Raw PDFs and paginated pipeline outputs';
