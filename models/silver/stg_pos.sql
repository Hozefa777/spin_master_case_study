SELECT
    CAST(year AS INT64) AS year,
    CAST(month AS INT64) AS month,
    CAST(week AS INT64) AS week,
    DATE_FROM_ISOWEEK(CAST(year AS INT64), CAST(week AS INT64)) AS week_date,
    LOWER(region) AS region,
    retailer_code,
    LOWER(retailer_name) AS retailer_name,
    brand_code,
    LOWER(brand) AS brand,
    SAFE_CAST(pos_units AS INT64) AS pos_units,
    SAFE_CAST(pos_amount AS FLOAT64) AS pos_amount,
    SAFE_CAST(inv_units AS INT64) AS inv_units,
    SAFE_CAST(inv_amount AS FLOAT64) AS inv_amount
FROM {{ ref('raw_pos') }}