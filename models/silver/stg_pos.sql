SELECT
    retailer_code,
    brand_code,
    CAST(year AS INT64) AS year,
    EXTRACT(MONTH FROM PARSE_DATE('%G-%V', CAST(year AS STRING) || '-' || LPAD(CAST(week AS STRING), 2, '0'))) AS month,
    CAST(week AS INT64) AS week,
    PARSE_DATE(
        '%G-%V',
        CAST(year AS STRING) || '-' || LPAD(CAST(week AS STRING), 2, '0')
    ) AS week_date,
    LOWER(region) AS region,
    LOWER(retailer_name) AS retailer_name,
    LOWER(brand) AS brand,
    SAFE_CAST(pos_units AS INT64) AS pos_units,
    SAFE_CAST(pos_amount AS FLOAT64) AS pos_amount,
    SAFE_CAST(inv_units AS INT64) AS inv_units,
    SAFE_CAST(inv_amount AS FLOAT64) AS inv_amount
FROM {{ ref('raw_pos') }}
