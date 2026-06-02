# Snowflake Multimodal RAG Pipeline

End-to-end document ingestion and search pipeline running entirely inside Snowflake. Supports PDFs (digital, scanned, mixed), markdown, and text files. Automatically classifies each document and routes it through the correct parsing and embedding path.

## How It Works

Files dropped into an internal stage are picked up by a stored procedure that classifies and processes each one:

- **TXT / MD** — read directly, embedded with `EMBED_TEXT_768`
- **Digital PDF** — parsed with `PARSE_DOCUMENT LAYOUT`, embedded with `EMBED_TEXT_768`
- **Scanned PDF** — `PARSE_DOCUMENT LAYOUT` detects no text, falls back to `OCR`, embedded with `EMBED_TEXT_768`
- **Mixed PDF** — `PARSE_DOCUMENT LAYOUT` detects embedded images via markdown image refs, re-parsed with `extract_images:true`, text embedded with `EMBED_TEXT_768`, images embedded with `AI_EMBED voyage-multimodal-3` and stored separately in a vector table

Text embeddings feed into a Cortex Search Service for hybrid keyword + semantic search. Image embeddings are stored in `DEMO_SEC_VM3_VECTORS` for multimodal retrieval.

## Cost Per Document Type

Rates from the [Snowflake Service Consumption Table](https://www.snowflake.com/legal-files/CreditConsumptionTable.pdf) (effective May 12, 2026). 1 credit = $2.00 USD.

| Doc Type | What Gets Called | Credits | USD |
|---|---|---|---|
| TXT / MD | `EMBED_TEXT_768` only | ~0.00003 | ~$0.00006 |
| Digital PDF (10 pages) | 1× `PARSE_DOCUMENT LAYOUT` + `EMBED_TEXT_768` | ~0.033 | ~$0.067 |
| Scanned PDF (10 pages) | 1× `PARSE_DOCUMENT LAYOUT` + 1× `PARSE_DOCUMENT OCR` + `EMBED_TEXT_768` | ~0.038 | ~$0.077 |
| Mixed PDF (94 pages, 31 images) | 2× `PARSE_DOCUMENT LAYOUT` + 31× `AI_EMBED` + `EMBED_TEXT_768` | ~0.314 | ~$0.628 |

`extract_images:true` on the second LAYOUT call has no additional cost per Snowflake documentation. Page count is the dominant cost driver — image count adds very little.

## Running the Pipeline

Upload a file to the stage and call the procedure:

```bash
snowsql -c <connection> -q "PUT file://path/to/file.pdf @RAG.PUBLIC.RAG_INTERNAL AUTO_COMPRESS=FALSE OVERWRITE=TRUE;"
snowsql -c <connection> -q "CALL RAG.PUBLIC.RUN_RAG_PIPELINE();"
```

The procedure returns a structured log showing classification and processing results for each file.

## Checking Costs

```sql
SELECT
    FUNCTION_NAME,
    MODEL_NAME,
    SUM(TOKEN_CREDITS)                  AS CREDITS,
    ROUND(SUM(TOKEN_CREDITS) * 2.00, 4) AS COST_USD,
    SUM(TOKENS)                         AS TOTAL_TOKENS
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FUNCTIONS_USAGE_HISTORY
WHERE START_TIME >= DATEADD('day', -1, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY CREDITS DESC;
```

Note: `ACCOUNT_USAGE` views have up to a 3-hour lag. For live spend, go to Snowsight → Admin → Cost Management → Consumption → Service Type: AI Services.

## Setup

Requires Snowflake with Cortex AI enabled, an internal stage at `RAG.PUBLIC.RAG_INTERNAL`, and a warehouse named `ML_WH`. Deploy the procedure from `sql/procedures/rag_pipeline.sql`:

```bash
snowsql -c <connection> -f sql/procedures/rag_pipeline.sql
```

Table definitions are in `sql/stages/build.sql`.

## Stack

Snowflake Cortex — `PARSE_DOCUMENT`, `EMBED_TEXT_768`, `AI_EMBED` · Voyage AI `voyage-multimodal-3` · Snowflake Arctic Embed `snowflake-arctic-embed-m-v1.5` · Cortex Search · Snowpark Python
