
-- BUILD: Multimodal RAG Pipeline
-- Deploys the full production pipeline to Snowflake
-- Usage: snowsql -c ml -f sql/build.sql



-- 1. Load environment
!source sql/environment.sql

-- 2. Set context
USE ROLE      IDENTIFIER($ML_ROLE);
USE WAREHOUSE IDENTIFIER($ML_WH);
USE DATABASE  IDENTIFIER($DB);
USE SCHEMA    PUBLIC;

-- 3. Create stage
!source sql/stages/rag_internal.sql

-- 4. Create tables
!source sql/tables/ingestion_log.sql
!source sql/tables/image_corpus.sql
!source sql/tables/vm3_vectors.sql
!source sql/tables/pdf_corpus.sql
!source sql/tables/parse_doc.sql
!source sql/tables/joined_data.sql

-- 5. Deploy pipeline stored procedure
!source sql/procedures/rag_pipeline.sql

-- 6. Deploy automation (stream + task)
!source sql/procedures/rag_automation.sql

-- 7. Verify deployment
SHOW PROCEDURES LIKE 'RUN_RAG_PIPELINE'  IN SCHEMA RAG.PUBLIC;
SHOW TASKS      LIKE 'RAG_PIPELINE_TASK' IN SCHEMA RAG.PUBLIC;
SHOW STREAMS    LIKE 'RAG_STAGE_STREAM'  IN SCHEMA RAG.PUBLIC;
SHOW TABLES     LIKE '%DEMO_SEC%'        IN SCHEMA RAG.PUBLIC;
SHOW STAGES     LIKE 'RAG_INTERNAL'      IN SCHEMA RAG.PUBLIC;
