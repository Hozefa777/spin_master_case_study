WITH base AS (
    SELECT
        year,
        month,
        LOWER(version) AS type,
        LOWER(region) AS region,
        LOWER(brand) AS brand,
        LOWER(node) AS node,
        SAFE_CAST(amount AS FLOAT64) AS amount
    FROM {{ ref('stg_finance') }}
),

metrics AS (
    SELECT
        *,
        MAX(CASE WHEN node = 'gps' THEN amount ELSE 0 END) OVER (PARTITION BY year, month, type, region, brand) AS gps_amount,
        MAX(CASE WHEN node = 'allowance' THEN amount ELSE 0 END) OVER (PARTITION BY year, month, type, region, brand) AS allowance_amount,
        MAX(CASE WHEN node = 'other revenue' THEN amount ELSE 0 END) OVER (PARTITION BY year, month, type, region, brand) AS other_revenue_amount,
        MAX(CASE WHEN node = 'cogs' THEN amount ELSE 0 END) OVER (PARTITION BY year, month, type, region, brand) AS cogs_amount
    FROM base
)

SELECT
    year,
    month,
    type,
    region,
    brand,
    node,
    amount,
    gps_amount,
    gps_amount - allowance_amount AS nps_amount,
    gps_amount - allowance_amount + other_revenue_amount AS revenue,
    gps_amount - allowance_amount + other_revenue_amount - cogs_amount AS gm
FROM metrics
