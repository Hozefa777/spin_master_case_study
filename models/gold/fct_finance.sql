WITH finance_clean AS (
    SELECT *
    FROM {{ ref('stg_finance') }}
),


pivoted AS (
    SELECT
        year,
        month,
        version,
        region,
        brand,
        SUM(CASE WHEN node = 'gps' THEN amount ELSE 0 END) AS gps,
        SUM(CASE WHEN node = 'allowance' THEN amount ELSE 0 END) AS allowance,
        SUM(CASE WHEN node = 'other revenue' THEN amount ELSE 0 END) AS other_revenue,
        SUM(CASE WHEN node = 'cogs' THEN amount ELSE 0 END) AS cogs
    FROM finance_clean
    GROUP BY year, month, version, region, brand
)

SELECT
    *,
    gps - allowance AS nps,
    (gps - allowance + other_revenue) AS revenue,
    (gps - allowance + other_revenue - cogs) AS gm
FROM pivoted
