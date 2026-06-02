# Snowflake Multimodal RAG Pipeline

End-to-end document ingestion and search pipeline running entirely inside Snowflake. Supports PDFs (digital, scanned, mixed), markdown, and text files. Automatically classifies each document and routes it through the correct preprocessing path. The search service is exposed as an agent connected to Snowflake Intelligence for natural language querying.

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
                   │
                   ▼
            Cortex Agent
                   │
                   ▼
       Snowflake Intelligence
     (natural language querying)
```

## How It Works

- **TXT / MD**: read directly, embedded with `EMBED_TEXT_768`
- **Digital PDF**: parsed with `AI_PARSE_DOCUMENT LAYOUT`, embedded with `EMBED_TEXT_768`
- **Scanned PDF**: `AI_PARSE_DOCUMENT LAYOUT` detects no text, falls back to `OCR`, embedded with `EMBED_TEXT_768`
- **Mixed PDF**: `AI_PARSE_DOCUMENT LAYOUT` detects embedded images via markdown image refs, re-parsed with `extract_images:true`, text embedded with `EMBED_TEXT_768`, images embedded with `AI_EMBED voyage-multimodal-3` and stored separately in `DEMO_SEC_VM3_VECTORS`
- **Cortex Search Service**: rebuilt after every pipeline run, serves hybrid keyword and semantic search over all ingested documents
- **Cortex Agent**: wraps the search service and exposes it as a tool
- **Snowflake Intelligence**: connects to the agent for natural language querying over the document corpus

## Cost Breakdown

Every document type goes through two stages: **parsing** (extracting content from the file) and **embedding** (converting that content into vectors for search). The Snowflake features used at each stage depend on what the document contains.

### Why each feature is used

**`AI_PARSE_DOCUMENT LAYOUT`** is called on every PDF. It reads the document structure and extracts text. For digital PDFs this is the only parse call needed. For mixed PDFs it is called twice, once to detect that images are present, and again with `extract_images:true` to retrieve the actual image bytes. The second call has no additional cost.

**`AI_PARSE_DOCUMENT OCR`** is only called on scanned PDFs. When LAYOUT returns no meaningful text (below 50 characters), the pipeline concludes the document is image-based and falls back to OCR to extract the text.

**`AI_EMBED voyage-multimodal-3`** is only called on mixed PDFs, once per image that passes the minimum size filter. It generates a 1024-dimension vector from each image so that image content becomes searchable.

**`EMBED_TEXT_768 snowflake-arctic-embed-m-v1.5`** is called once for every document regardless of type. It converts the extracted text into a 768-dimension vector that feeds the Cortex Search Service.

**TXT and MD files** skip parsing entirely. The file is read directly and only the embedding step runs.

### Functions called per document type

Rates from Table 6(a) and 6(g) of the [Snowflake Service Consumption Table](https://www.snowflake.com/legal-files/CreditConsumptionTable.pdf) (effective May 12, 2026). All rates are in AI Credits.

| Function | TXT / MD | Digital PDF | Scanned PDF | Mixed PDF | Official Rate |
|---|---|---|---|---|---|
| `AI_PARSE_DOCUMENT` LAYOUT | 0× | 1× | 1× | 2× | 3.66 AI Credits / 1,000 pages |
| `AI_PARSE_DOCUMENT` OCR | 0× | 0× | 1× | 0× | 0.68 AI Credits / 1,000 pages |
| `AI_PARSE_DOCUMENT` + `extract_images:true` | 0× | 0× | 0× | included in 2nd LAYOUT | No additional cost |
| `AI_EMBED` `snowflake-arctic-embed-m-v1.5` | 1× | 1× | 1× | 1× | 0.03 AI Credits / 1M tokens |
| `AI_EMBED` `voyage-multimodal-3` | 0× | 0× | 0× | 1× per image | 0.06 AI Credits / 1M tokens |

1 AI Credit = $2.00 USD (Global, On Demand). Each document page = 970 tokens for token-based billing.

### Cost summary per document type

**TXT / MD**
Only the embedding step runs. No parsing, no page-based billing. Cost is determined purely by the length of the text, billed at 0.03 AI Credits per 1M tokens. For typical documents this is a fraction of a cent.

**Digital PDF**
One LAYOUT parse call plus one embedding. The dominant cost is the LAYOUT call at 3.66 AI Credits per 1,000 pages. A 10-page report costs 0.0366 AI Credits (~$0.07).

**Scanned PDF**
Two parse calls. LAYOUT to detect the document is scanned, then OCR to extract the text plus one embedding. Combined parse cost is (3.66 + 0.68) = 4.34 AI Credits per 1,000 pages. A 10-page scanned document costs 0.0434 AI Credits (~$0.09).

**Mixed PDF**
Two LAYOUT calls plus one embedding for text, and one `AI_EMBED voyage-multimodal-3` call per image for image vectors. The LAYOUT calls cost 2 × 3.66 = 7.32 AI Credits per 1,000 pages. Image embedding adds 0.06 AI Credits per 1M tokens per image, which is minimal. Page count is the dominant cost driver. A 94-page document with 31 images costs approximately 0.69 AI Credits (~$1.38).

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

Snowflake Cortex — `AI_PARSE_DOCUMENT`, `EMBED_TEXT_768`, `AI_EMBED` · Voyage AI `voyage-multimodal-3` · Snowflake Arctic Embed `snowflake-arctic-embed-m-v1.5` · Cortex Search · Cortex Agent · Snowflake Intelligence · Snowpark Python
