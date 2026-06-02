# Snowflake Multimodal RAG Pipeline

End-to-end document ingestion and search pipeline running entirely inside Snowflake. Supports PDFs (digital, scanned, mixed), markdown, and text files. Automatically classifies each document and routes it through the correct parsing and embedding path.

## Pipeline

```
┌──────────────────────────────────────────────────────┐
│                   Internal Stage                     │
│                  (RAG_INTERNAL)                      │
└───────────────────────┬──────────────────────────────┘
                        │
                        ▼
┌──────────────────────────────────────────────────────┐
│               RUN_RAG_PIPELINE()                     │
│           File Discovery + Classification            │
└───────┬───────────────┬──────────────┬───────────────┘
        │               │              │               │
        ▼               ▼              ▼               ▼
     TXT/MD        Digital PDF    Scanned PDF      Mixed PDF
     Read          1x LAYOUT      1x LAYOUT        2x LAYOUT
     directly      reused         + 1x OCR         + AI_EMBED
                                                   per image
        │               │              │               │
        └───────────────┴──────────────┴───────────────┘
                                │
                                ▼
                        EMBED_TEXT_768
                   (snowflake-arctic-embed-m-v1.5)
                                │
                   ┌────────────┴────────────┐
                   ▼                         ▼
         DEMO_SEC_JOINED_DATA       DEMO_SEC_VM3_VECTORS
         (text + 768-dim vectors)   (images + 1024-dim vectors)
                   │
                   ▼
         Cortex Search Service
       (hybrid keyword + semantic)
```

## How It Works

- **TXT / MD** — read directly, embedded with `EMBED_TEXT_768`
- **Digital PDF** — parsed with `PARSE_DOCUMENT LAYOUT`, embedded with `EMBED_TEXT_768`
- **Scanned PDF** — `PARSE_DOCUMENT LAYOUT` detects no text, falls back to `OCR`, embedded with `EMBED_TEXT_768`
- **Mixed PDF** — `PARSE_DOCUMENT LAYOUT` detects embedded images via markdown image refs, re-parsed with `extract_images:true`, text embedded with `EMBED_TEXT_768`, images embedded with `AI_EMBED voyage-multimodal-3` and stored separately in `DEMO_SEC_VM3_VECTORS`

`extract_images:true` on the second LAYOUT call has no additional cost per Snowflake documentation.

## Cost Per Document Type

Rates from the [Snowflake Service Consumption Table](https://www.snowflake.com/legal-files/CreditConsumptionTable.pdf) (effective May 12, 2026). 1 credit = $2.00 USD.

| Doc Type | What Gets Called | Credits | USD |
|---|---|---|---|
| TXT / MD | `EMBED_TEXT_768` only | ~0.00003 | ~$0.00006 |
| Digital PDF (10 pages) | 1× `PARSE_DOCUMENT LAYOUT` + `EMBED_TEXT_768` | ~0.033 | ~$0.067 |
| Scanned PDF (10 pages) | 1× `PARSE_DOCUMENT LAYOUT` + 1× `PARSE_DOCUMENT OCR` + `EMBED_TEXT_768` | ~0.038 | ~$0.077 |
| Mixed PDF (94 pages, 31 images) | 2× `PARSE_DOCUMENT LAYOUT` + 31× `AI_EMBED` + `EMBED_TEXT_768` | ~0.314 | ~$0.628 |

Page count is the dominant cost driver. Image count adds very little due to the low `AI_EMBED` rate.

## Running the Pipeline

Upload a file to the stage and call the procedure:

```bash
snowsql -c <connection> -q "PUT file://path/to/file.pdf @RAG.PUBLIC.RAG_INTERNAL AUTO_COMPRESS=FALSE OVERWRITE=TRUE;"
snowsql -c <connection> -q "CALL RAG.PUBLIC.RUN_RAG_PIPELINE();"
```

The procedure returns a structured log showing classification and processing results for each file:

```
[1/6] 1 new file(s) queued
[2/6] 1 file(s) ready
[3/6] Catalog updated
  document.pdf: text_len=220033, has_images=True
  document.pdf: 31 image vector(s) stored
[4d/6] 1 mixed PDF(s) processed with extract_images + AI_EMBED
[4/6] All files classified and parsed
[5/6] JOINED_DATA built -- embeddings complete
[6/6] Cortex Search Service recreated -- agent updated
```

## Setup

Requires Snowflake with Cortex AI enabled, an internal stage at `RAG.PUBLIC.RAG_INTERNAL`, and a warehouse named `ML_WH`. Create all tables first using `build.sql`, then deploy the procedure:

```bash
snowsql -c <connection> -f build.sql
snowsql -c <connection> -f procedures/rag_pipeline.sql
```

## Stack

Snowflake Cortex — `PARSE_DOCUMENT`, `EMBED_TEXT_768`, `AI_EMBED` · Voyage AI `voyage-multimodal-3` · Snowflake Arctic Embed `snowflake-arctic-embed-m-v1.5` · Cortex Search · Snowpark Python
