
-- PROCEDURE: RUN_RAG_PIPELINE
-- Full multimodal RAG pipeline runs entirely inside Snowflake
--
-- Routing logic:
--   .txt / .md     -> read directly -> EMBED_TEXT_768 (cheapest)
--   .pdf scanned   -> extracted_text < 50 chars -> OCR -> EMBED_TEXT_768
--   .pdf digital   -> text only, no image refs -> EMBED_TEXT_768 (cheap)
--   .pdf mixed     -> markdown image refs detected ->
--                     text -> EMBED_TEXT_768
--                     images -> put_stream to stage -> AI_EMBED
-- Schema  : RAG.PUBLIC
-- Manual call: CALL RAG.PUBLIC.RUN_RAG_PIPELINE();


DROP PROCEDURE IF EXISTS RAG.PUBLIC.RUN_RAG_PIPELINE();

CREATE OR REPLACE PROCEDURE RAG.PUBLIC.RUN_RAG_PIPELINE()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS CALLER
AS $$
import base64
import io
import json
import re

STAGE           = '@RAG.PUBLIC.RAG_INTERNAL'
STAGE_NAME      = 'RAG.PUBLIC.RAG_INTERNAL'
TMP_IMG_PATH    = 'tmp_images'
SUPPORTED_EXTS  = ('.pdf', '.txt', '.md')
SCANNED_THRESH  = 50
MIN_IMAGE_BYTES = 5000

IMAGE_REF_RE = re.compile(r'!\[.*?\]\(.*?\)')


def sql_safe(text, max_len=2000):
    return str(text).replace("\\", "\\\\").replace("'", "''")[:max_len]


def parse_result(raw):
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    try:
        return json.loads(str(raw))
    except Exception:
        return {}


def classify_layout_content(layout_content):
    extracted_text = ''
    has_images = False

    if isinstance(layout_content, list):
        for block in layout_content:
            if not isinstance(block, dict):
                continue
            btype = str(block.get('type', '')).lower()
            if btype == 'image':
                has_images = True
            else:
                extracted_text += str(
                    block.get('text', '') or block.get('content', '')
                )

    elif isinstance(layout_content, str):
        extracted_text = layout_content
        has_images = bool(IMAGE_REF_RE.search(layout_content))

    return extracted_text, has_images


def insert_parse_doc(session, fname, doc_page_key, page_number, stage_prefix, text, doc_type):
    """
    Parameterised insert for large document text.
    Avoids SQL compiler limits on inline string literals.
    """
    truncated = text[:50000]
    session.sql(
        """
        INSERT INTO DEMO_SEC_PARSE_DOC
            (FILE_NAME, DOC_PAGE_KEY, PAGE_NUMBER, STAGE_PREFIX,
             PARSE_DOC_OUTPUT, DOC_TYPE)
        SELECT ?, ?, ?, ?, TO_VARIANT(?), ?
        WHERE NOT EXISTS (
            SELECT 1 FROM DEMO_SEC_PARSE_DOC WHERE FILE_NAME = ?
        )
        """,
        params=[fname, doc_page_key, page_number, stage_prefix,
                truncated, doc_type, fname]
    ).collect()


def insert_image_vector(session, fname, doc_page_key, page_number, idx, stage_prefix, img_vector):
    """
    Insert image embedding vector into VM3_VECTORS.

    img_vector is a Python list of floats returned by AI_EMBED via Snowpark.
    We JSON-serialise it and pass as a bind parameter, then cast to
    VECTOR(FLOAT, 1024) inside SQL -- avoids f-string injection of huge arrays.
    """
    vector_json = json.dumps(img_vector)
    session.sql(
        """
        INSERT INTO DEMO_SEC_VM3_VECTORS
            (FILE_NAME, DOC_PAGE_KEY, PAGE_NUMBER,
             IMAGE_INDEX, STAGE_PREFIX, IMAGE_VECTOR)
        SELECT ?, ?, ?, ?, ?,
            PARSE_JSON(?)::VECTOR(FLOAT, 1024)
        WHERE NOT EXISTS (
            SELECT 1 FROM DEMO_SEC_VM3_VECTORS
            WHERE FILE_NAME = ?
            AND IMAGE_INDEX = ?
        )
        """,
        params=[fname, doc_page_key, page_number, idx, stage_prefix,
                vector_json, fname, idx]
    ).collect()


def run(session) -> str:
    log = []

  
    # Step 1: Discover new files in stage
 
    rows = session.sql(f"LIST {STAGE}").collect()

    new_files = []
    for row in rows:
        full_path = str(row[0])
        fname     = full_path.split('/')[-1]

        if any(x in full_path for x in ['/paged_pdf/', '/paged_image/', '/tmp_images/']):
            continue
        if '_page_' in fname:
            continue
        if not any(fname.lower().endswith(ext) for ext in SUPPORTED_EXTS):
            continue

        existing = session.sql(
            f"SELECT STATUS FROM INGESTION_LOG WHERE FILE_NAME = '{fname}'"
        ).collect()
        if existing and existing[0][0] not in ('ERROR',):
            continue

        size = int(row[1]) if len(row) > 1 else 0

        session.sql(f"""
            MERGE INTO INGESTION_LOG t
            USING (SELECT '{fname}' AS fn) s ON t.FILE_NAME = s.fn
            WHEN MATCHED THEN UPDATE SET
                STATUS = 'PENDING', ERROR_MSG = NULL,
                FILE_SIZE = {size}, UPDATED_AT = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT
                (FILE_NAME, FILE_SIZE, SOURCE_TYPE, STATUS)
                VALUES ('{fname}', {size}, 'stage', 'PENDING')
        """).collect()

        new_files.append(fname)

    log.append(f"[1/6] {len(new_files)} new file(s) queued")

    if not new_files:
        log.append("[2/6] No new files -- skipping")
        log.append("[3/6] Skipping catalog")
    else:

        # Step 2: Mark files as ready

        for fname in new_files:
            session.sql(f"""
                UPDATE INGESTION_LOG
                SET STATUS = 'PAGINATED', UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE FILE_NAME = '{fname}'
            """).collect()

        log.append(f"[2/6] {len(new_files)} file(s) ready")


        # Step 3: Refresh stage + populate PDF_CORPUS

        session.sql(f"ALTER STAGE {STAGE_NAME} REFRESH").collect()

        session.sql(f"""
            INSERT INTO DEMO_SEC_PDF_CORPUS
                (FILE_NAME, DOC_PAGE_KEY, PAGE_NUMBER, STAGE_PREFIX)
            SELECT
                SPLIT_PART(METADATA$FILENAME, '/', -1) AS FILE_NAME,
                REGEXP_REPLACE(
                    SPLIT_PART(METADATA$FILENAME, '/', -1),
                    '\\.(pdf|txt|md)$', ''
                ) AS DOC_PAGE_KEY,
                1 AS PAGE_NUMBER,
                '@RAG.PUBLIC.RAG_INTERNAL' AS STAGE_PREFIX
            FROM {STAGE}
            WHERE METADATA$FILENAME NOT LIKE '%/paged_pdf/%'
            AND METADATA$FILENAME NOT LIKE '%/paged_image/%'
            AND METADATA$FILENAME NOT LIKE '%/tmp_images/%'
            AND SPLIT_PART(METADATA$FILENAME, '/', -1) NOT LIKE '%_page_%'
            AND (
                SPLIT_PART(METADATA$FILENAME, '/', -1) ILIKE '%.pdf'
                OR SPLIT_PART(METADATA$FILENAME, '/', -1) ILIKE '%.txt'
                OR SPLIT_PART(METADATA$FILENAME, '/', -1) ILIKE '%.md'
            )
            AND SPLIT_PART(METADATA$FILENAME, '/', -1)
                NOT IN (SELECT FILE_NAME FROM DEMO_SEC_PDF_CORPUS)
            GROUP BY 1,2,3,4
        """).collect()

        log.append("[3/6] Catalog updated")

    # Step 4a: .txt and .md

    txt_md_files = session.sql("""
        SELECT pc.FILE_NAME, pc.DOC_PAGE_KEY, pc.PAGE_NUMBER, pc.STAGE_PREFIX
        FROM DEMO_SEC_PDF_CORPUS pc
        WHERE pc.FILE_NAME NOT IN (SELECT FILE_NAME FROM DEMO_SEC_PARSE_DOC)
        AND (pc.FILE_NAME ILIKE '%.txt' OR pc.FILE_NAME ILIKE '%.md')
    """).collect()

    txt_count = 0
    for row in txt_md_files:
        fname        = row[0]
        doc_page_key = row[1]
        page_number  = row[2]
        stage_prefix = row[3]

        try:
            from snowflake.snowpark.files import SnowflakeFile
            with SnowflakeFile.open(
                f"{STAGE}/{fname}", 'r', require_scoped_url=False
            ) as f:
                text_content = f.read()

            insert_parse_doc(session, fname, doc_page_key, page_number,
                             stage_prefix, text_content, 'TEXT')
            txt_count += 1

        except Exception as exc:
            err = sql_safe(str(exc))
            session.sql(f"""
                UPDATE INGESTION_LOG SET STATUS = 'ERROR',
                ERROR_MSG = '{err}', UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE FILE_NAME = '{fname}'
            """).collect()
            log.append(f"  ERROR reading {fname}: {err}")

    if txt_count > 0:
        log.append(f"[4a/6] {txt_count} text/markdown file(s) read directly")


    # Step 4b: PDFs

    pdf_files = session.sql("""
        SELECT pc.FILE_NAME, pc.DOC_PAGE_KEY, pc.PAGE_NUMBER, pc.STAGE_PREFIX
        FROM DEMO_SEC_PDF_CORPUS pc
        WHERE pc.FILE_NAME NOT IN (SELECT FILE_NAME FROM DEMO_SEC_PARSE_DOC)
        AND pc.FILE_NAME ILIKE '%.pdf'
        AND pc.FILE_NAME NOT LIKE '%_page_%'
    """).collect()

    scanned_count = 0
    digital_count = 0
    mixed_count   = 0

    for row in pdf_files:
        fname        = row[0]
        doc_page_key = row[1]
        page_number  = row[2]
        stage_prefix = row[3]

        try:

            # Single LAYOUT call

            layout_rows = session.sql(f"""
                SELECT
                    PARSE_JSON(TO_VARCHAR(
                        SNOWFLAKE.CORTEX.PARSE_DOCUMENT(
                            '{STAGE}', '{fname}', {{'mode': 'LAYOUT'}}
                        )
                    )) AS layout_result
            """).collect()

            raw_layout     = layout_rows[0][0] if layout_rows else None
            layout_result  = parse_result(raw_layout)
            layout_content = layout_result.get('content', [])

            extracted_text, has_images = classify_layout_content(layout_content)
            text_len = len(extracted_text.strip())

            log.append(f"  {fname}: text_len={text_len}, has_images={has_images}")


            # Branch: SCANNED

            if text_len < SCANNED_THRESH:
                ocr_rows = session.sql(f"""
                    SELECT PARSE_JSON(TO_VARCHAR(
                        SNOWFLAKE.CORTEX.PARSE_DOCUMENT(
                            '{STAGE}', '{fname}', {{'mode': 'OCR'}}
                        )
                    )):content AS ocr_content
                """).collect()

                raw_ocr     = ocr_rows[0][0] if ocr_rows else None
                ocr_content = str(raw_ocr) if raw_ocr is not None else ''

                insert_parse_doc(session, fname, doc_page_key, page_number,
                                 stage_prefix, ocr_content, 'SCANNED')
                scanned_count += 1

            # Branch: DIGITAL
            elif not has_images:
                store_text = extracted_text if extracted_text else str(layout_content)

                insert_parse_doc(session, fname, doc_page_key, page_number,
                                 stage_prefix, store_text, 'DIGITAL')
                digital_count += 1

            # Branch: MIXED
            else:
                mixed_rows = session.sql(f"""
                    SELECT PARSE_JSON(TO_VARCHAR(
                        SNOWFLAKE.CORTEX.PARSE_DOCUMENT(
                            '{STAGE}', '{fname}',
                            {{'mode': 'LAYOUT', 'extract_images': true}}
                        )
                    )) AS full_result
                """).collect()

                raw_full     = mixed_rows[0][0] if mixed_rows else None
                full_result  = parse_result(raw_full)
                full_content = full_result.get('content', [])
                full_text, _ = classify_layout_content(full_content)
                store_text   = full_text if full_text else str(full_content)

                insert_parse_doc(session, fname, doc_page_key, page_number,
                                 stage_prefix, store_text, 'MIXED')

                # Process images
                images = full_result.get('images', [])
                stored_img_count = 0

                for idx, img in enumerate(images):
                    try:
                        if not isinstance(img, dict):
                            try:
                                img = json.loads(str(img))
                            except Exception:
                                continue

                        img_b64 = img.get('image_base64', '')
                        if not img_b64:
                            continue

                        clean_b64 = re.sub(r'^data:image/[^;]+;base64,', '', img_b64)
                        img_bytes = base64.b64decode(clean_b64)

                        if len(img_bytes) < MIN_IMAGE_BYTES:
                            continue

                        img_stage_path = f"{TMP_IMG_PATH}/{doc_page_key}_img_{idx}.png"
                        session.file.put_stream(
                            io.BytesIO(img_bytes),
                            f"{STAGE}/{img_stage_path}",
                            auto_compress=False,
                            overwrite=True
                        )

                        embed_rows = session.sql(f"""
                            SELECT AI_EMBED(
                                'voyage-multimodal-3',
                                TO_FILE('{STAGE}', '{img_stage_path}')
                            ) AS img_vector
                        """).collect()

                        if not (embed_rows and embed_rows[0][0]):
                            continue

                        img_vector = embed_rows[0][0]

                        # img_vector is a Python list of floats
                        # JSON-serialise and pass as bind param,
                        # cast to VECTOR(FLOAT, 1024) inside SQL

                        insert_image_vector(
                            session, fname, doc_page_key, page_number,
                            idx, stage_prefix, img_vector
                        )

                        stored_img_count += 1

                    except Exception as img_exc:
                        log.append(
                            f"  WARNING: image {idx} in {fname} skipped: "
                            f"{str(img_exc)[:150]}"
                        )
                        continue

                log.append(f"  {fname}: {stored_img_count} image vector(s) stored")
                mixed_count += 1

        except Exception as exc:
            err = sql_safe(str(exc))
            session.sql(f"""
                UPDATE INGESTION_LOG SET STATUS = 'ERROR',
                ERROR_MSG = '{err}', UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE FILE_NAME = '{fname}'
            """).collect()
            log.append(f"  ERROR processing {fname}: {err}")

    if scanned_count > 0:
        log.append(f"[4b/6] {scanned_count} scanned PDF(s) processed with OCR")
    if digital_count > 0:
        log.append(f"[4c/6] {digital_count} plain text PDF(s) parsed with LAYOUT")
    if mixed_count > 0:
        log.append(f"[4d/6] {mixed_count} mixed PDF(s) processed with extract_images + AI_EMBED")

    log.append("[4/6] All files classified and parsed")

  
    # Step 5: Build JOINED_DATA

    session.sql(f"""
        INSERT INTO DEMO_SEC_JOINED_DATA
            (FILE_NAME, PAGE_NUMBER, VECTOR_MAIN, TEXT,
             IMAGE_FILEPATH, SOURCE_DOC, UPDATED_AT)
        SELECT
            p.FILE_NAME,
            p.PAGE_NUMBER,
            SNOWFLAKE.CORTEX.EMBED_TEXT_768(
                'snowflake-arctic-embed-m-v1.5',
                LEFT(TO_VARCHAR(p.PARSE_DOC_OUTPUT), 8000)
            ) AS VECTOR_MAIN,
            p.PARSE_DOC_OUTPUT AS TEXT,
            p.FILE_NAME AS IMAGE_FILEPATH,
            REGEXP_REPLACE(p.FILE_NAME, '\\.(pdf|txt|md)$', '') AS SOURCE_DOC,
            CURRENT_TIMESTAMP()
        FROM DEMO_SEC_PARSE_DOC p
        WHERE p.FILE_NAME NOT IN (SELECT FILE_NAME FROM DEMO_SEC_JOINED_DATA)
        AND TO_VARCHAR(p.PARSE_DOC_OUTPUT) IS NOT NULL
        AND LENGTH(TO_VARCHAR(p.PARSE_DOC_OUTPUT)) > 0
    """).collect()

    session.sql("""
        UPDATE INGESTION_LOG SET STATUS = 'EMBEDDED',
        UPDATED_AT = CURRENT_TIMESTAMP()
        WHERE STATUS = 'PAGINATED'
    """).collect()

    log.append("[5/6] JOINED_DATA built -- embeddings complete")


    # Step 6: Recreate Cortex Search Service

    session.sql("""
        CREATE OR REPLACE CORTEX SEARCH SERVICE RAG.PUBLIC.DEMO_SEC_CORTEX_SEARCH_SERVICE
            TEXT INDEXES TEXT
            VECTOR INDEXES VECTOR_MAIN(query_model = 'snowflake-arctic-embed-m-v1.5')
            WAREHOUSE  = 'ML_WH'
            TARGET_LAG = '1 day'
        AS (
            SELECT
                TO_VARCHAR(TEXT) AS TEXT,
                PAGE_NUMBER,
                VECTOR_MAIN,
                IMAGE_FILEPATH,
                SOURCE_DOC
            FROM DEMO_SEC_JOINED_DATA
            WHERE TEXT IS NOT NULL
        )
    """).collect()

    session.sql("""
        UPDATE INGESTION_LOG SET STATUS = 'INDEXED',
        UPDATED_AT = CURRENT_TIMESTAMP()
        WHERE STATUS = 'EMBEDDED'
    """).collect()

    log.append("[6/6] Cortex Search Service recreated -- agent updated")

    return '\n'.join(log)
$$;