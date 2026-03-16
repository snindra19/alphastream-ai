WITH staged AS (
    SELECT * FROM {{ ref('stg_news_headlines') }}
),

daily_metrics AS (
    SELECT
        ticker,
        DATE(published_at)                          AS news_date,
        COUNT(*)                                    AS article_count,
        COUNTIF(LOWER(title) LIKE '%surge%'
            OR LOWER(title) LIKE '%soar%'
            OR LOWER(title) LIKE '%rally%'
            OR LOWER(title) LIKE '%gain%'
            OR LOWER(title) LIKE '%profit%'
            OR LOWER(title) LIKE '%record%')        AS bullish_signals,
        COUNTIF(LOWER(title) LIKE '%crash%'
            OR LOWER(title) LIKE '%drop%'
            OR LOWER(title) LIKE '%fall%'
            OR LOWER(title) LIKE '%loss%'
            OR LOWER(title) LIKE '%lawsuit%'
            OR LOWER(title) LIKE '%decline%')       AS bearish_signals,
        STRING_AGG(title, ' | '
            ORDER BY published_at DESC
            LIMIT 5)                                AS latest_headlines,
        MAX(ingested_at)                            AS last_updated
    FROM staged
    GROUP BY ticker, DATE(published_at)
),

with_signal AS (
    SELECT
        *,
        CASE
            WHEN bullish_signals > bearish_signals THEN 'BULLISH'
            WHEN bearish_signals > bullish_signals THEN 'BEARISH'
            ELSE 'NEUTRAL'
        END AS sentiment_signal
    FROM daily_metrics
)

SELECT * FROM with_signal