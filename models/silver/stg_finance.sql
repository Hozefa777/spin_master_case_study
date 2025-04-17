SELECT
    CAST(year AS INT64) AS year,
    CAST(month AS INT64) AS month,
    LOWER(version) AS version,
    LOWER(region) AS region,
    LOWER(brand) AS brand,
    LOWER(node) AS node,
    SAFE_CAST(amount AS FLOAT64) AS amount
FROM {{ ref('raw_fin_data') }}
