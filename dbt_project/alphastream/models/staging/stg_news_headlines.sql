WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_news') }}
),

cleaned AS (
    SELECT
        ticker,
        TRIM(title)                                    AS title,
        COALESCE(TRIM(description), '')                AS description,
        url,
        SAFE_CAST(published_at AS TIMESTAMP)           AS published_at,
        source,
        ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY ticker, title
            ORDER BY ingested_at DESC
        )                                              AS row_num
    FROM source
    WHERE
        title IS NOT NULL
        AND REGEXP_CONTAINS(title, r'[a-zA-Z]')
)

SELECT
    ticker,
    title,
    description,
    url,
    published_at,
    source,
    ingested_at
FROM cleaned
WHERE row_num = 1