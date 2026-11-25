<!-- Adapted from databricks-solutions/genie-code-skills-demo skills/sentiment-analysis/pipeline-templates.md (DB license, see
     THIRD_PARTY_NOTICES.md). Changed: skill names, layer-as-schema naming, product reviews example. -->

# Sentiment pipeline templates

Reusable bronze / silver / gold SQL for `@de-sentiment-analysis`. Substitute catalog, schema, and source table names.

## Bronze -- raw text only (no AI calls)

```sql
CREATE OR REFRESH STREAMING TABLE bronze.product_reviews
COMMENT "Raw customer reviews ingested from <catalog>.<schema>.product_reviews"
TBLPROPERTIES (
  "quality" = "bronze",
  "domain" = "ecommerce"
)
CLUSTER BY AUTO
AS SELECT
  review_id,
  customer_id,
  product_id,
  review_text,
  review_date,
  current_timestamp() AS audit_timestamp,
  'reviews_service' AS source_system
FROM STREAM(<catalog>.<schema>.product_reviews)
WHERE review_id IS NOT NULL
  AND review_text IS NOT NULL;
```

## Silver -- AI functions

```sql
CREATE OR REFRESH MATERIALIZED VIEW silver.review_sentiment(
  CONSTRAINT valid_review_id EXPECT (review_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_sentiment EXPECT (sentiment_label IN ('positive','negative','neutral','mixed') OR sentiment_label IS NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_topic EXPECT (
    topic_label IN ('product_quality','shipping','price','customer_service','other')
    OR topic_label IS NULL
  ) ON VIOLATION DROP ROW
)
COMMENT "Customer reviews with AI-derived sentiment, topic and entities from bronze.product_reviews"
TBLPROPERTIES (
  "quality" = "silver",
  "domain" = "ecommerce",
  "delta.enableChangeDataFeed" = "true",
  "delta.enableRowTracking" = "true"
)
AS SELECT
  review_id,
  customer_id,
  product_id,
  review_text AS source_text,
  CASE
    WHEN LENGTH(TRIM(review_text)) < 5 THEN NULL
    ELSE ai_analyze_sentiment(review_text)
  END AS sentiment_label,
  CASE
    WHEN LENGTH(TRIM(review_text)) < 5 THEN NULL
    ELSE ai_classify(
      review_text,
      ARRAY('product_quality', 'shipping', 'price', 'customer_service', 'other')
    )
  END AS topic_label,
  CASE
    WHEN LENGTH(TRIM(review_text)) < 5 THEN NULL
    ELSE ai_extract(review_text, ARRAY('product_name', 'competitor_name'))
  END AS extracted_entities,
  CASE
    WHEN review_text IS NULL OR LENGTH(TRIM(review_text)) < 5 THEN 'MISSING_TEXT'
    WHEN ai_analyze_sentiment(review_text) IS NULL THEN 'AI_NULL_RESPONSE'
    ELSE 'CLEAN'
  END AS data_quality_flag,
  current_timestamp() AS audit_timestamp,
  'reviews_service' AS source_system
FROM bronze.product_reviews;

ALTER TABLE silver.review_sentiment
  ALTER COLUMN review_id COMMENT 'Unique review identifier from the reviews service',
  ALTER COLUMN source_text COMMENT 'Raw review text passed to the AI functions; retained for audit and re-runs',
  ALTER COLUMN sentiment_label COMMENT 'Output of ai_analyze_sentiment: positive | negative | neutral | mixed',
  ALTER COLUMN topic_label COMMENT 'Output of ai_classify against the customer-voice taxonomy',
  ALTER COLUMN extracted_entities COMMENT 'Output of ai_extract: STRUCT<product_name:STRING, competitor_name:STRING>',
  ALTER COLUMN data_quality_flag COMMENT 'Row-level DQ status: CLEAN | MISSING_TEXT | AI_NULL_RESPONSE';

ALTER TABLE silver.review_sentiment
  SET TAGS ('quality' = 'silver', 'domain' = 'ecommerce', 'data_classification' = 'internal', 'ai_generated' = 'true');
```

## Gold -- aggregates only (no source_text, no AI calls)

```sql
CREATE OR REFRESH MATERIALIZED VIEW gold.review_sentiment_by_product
COMMENT "Sentiment distribution by product category and topic, sourced from silver.review_sentiment"
TBLPROPERTIES (
  "quality" = "gold",
  "domain" = "ecommerce",
  "delta.enableChangeDataFeed" = "true"
)
AS SELECT
  p.category,
  s.topic_label,
  s.sentiment_label,
  COUNT(*) AS review_count,
  ROUND(
    COUNT_IF(s.sentiment_label = 'positive') * 100.0 / NULLIF(COUNT(*), 0),
    1
  ) AS positive_pct,
  ROUND(
    COUNT_IF(s.sentiment_label = 'negative') * 100.0 / NULLIF(COUNT(*), 0),
    1
  ) AS negative_pct,
  current_timestamp() AS audit_timestamp,
  'gold_aggregation' AS source_system
FROM silver.review_sentiment s
JOIN silver.products_scd1 p
  ON s.product_id = p.product_id
WHERE s.data_quality_flag = 'CLEAN'
GROUP BY p.category, s.topic_label, s.sentiment_label;
```
