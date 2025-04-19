WITH pos AS (
    SELECT
        week_date,
        retailer_code,
        brand,
        SUM(pos_units) AS pos_units,
        SUM(pos_amount) AS pos_amount,
        SUM(inv_units) AS inv_units,
        SUM(inv_amount) AS inv_amount
    FROM {{ ref('stg_pos') }}
    GROUP BY week_date, retailer_code, brand
),

shipment AS (
    SELECT
        landing_date AS week_date,
        retailer_code,
        brand,
        SUM(shp_units) AS landed_units,
        SUM(shp_amount) AS landed_amount
    FROM  {{ ref('stg_shipment') }}
    GROUP BY week_date, retailer_code, brand
),



anchor AS (
    SELECT DISTINCT pos.week_date, pos.retailer_code, pos.brand FROM pos
    UNION DISTINCT
    SELECT DISTINCT week_date, retailer_code, brand FROM shipment
),

combined AS (
    SELECT
        a.week_date,
        a.retailer_code,
        a.brand,
        p.pos_units,
        p.pos_amount,
        p.inv_units,
        p.inv_amount,
        s.landed_units,
        s.landed_amount
    FROM anchor a
    LEFT JOIN pos p USING (week_date, retailer_code, brand)
    LEFT JOIN shipment s USING (week_date, retailer_code, brand)
),

last_pos_week AS (
    SELECT retailer_code, brand, MAX(week_date) AS last_pos_week_date
    FROM pos
    GROUP BY retailer_code, brand
),

with_projection AS (
    SELECT
        c.*,
        last_pos_week_date,
        MAX(inv_units) OVER (
            PARTITION BY c.retailer_code, c.brand
            ORDER BY c.week_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS last_known_inventory,

        SUM(
            CASE
                WHEN c.week_date > lp.last_pos_week_date THEN IFNULL(landed_units, 0)
                ELSE 0
            END
        ) OVER (
            PARTITION BY c.retailer_code, c.brand
            ORDER BY week_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_landed_after_pos

    FROM combined c
    LEFT JOIN last_pos_week lp
      ON c.retailer_code = lp.retailer_code AND c.brand = lp.brand
),

final AS (
    SELECT *,
        CASE
            WHEN inv_units IS NOT NULL THEN inv_units
            WHEN week_date > last_pos_week_date THEN
                last_known_inventory + cumulative_landed_after_pos
            ELSE NULL
        END AS projected_inventory_units
    FROM with_projection
),

final_with_kpis AS (
    SELECT
        week_date,
        retailer_code,
        brand,
        IFNULL(pos_units, 0) AS pos_units,
        IFNULL(pos_amount, 0) AS pos_amount,
        IFNULL(inv_units, 0) AS inv_units,
        IFNULL(inv_amount, 0) AS inv_amount,
        IFNULL(landed_units, 0) AS landed_units,
        IFNULL(landed_amount, 0) AS landed_amount,
        IFNULL(projected_inventory_units, 0) AS projected_inventory_units,
        EXTRACT(YEAR FROM week_date) AS year,
        EXTRACT(MONTH FROM week_date) AS month,

        -- Month-end Inventory
        MAX(IFNULL(projected_inventory_units, 0)) OVER (
            PARTITION BY retailer_code, brand, EXTRACT(YEAR FROM week_date), EXTRACT(MONTH FROM week_date)
        ) AS month_end_inventory,

        -- Inventory Coverage
        SAFE_DIVIDE(IFNULL(projected_inventory_units, 0), NULLIF(pos_units, 0)) AS inventory_coverage,

        -- Sell-through Rate
        SAFE_DIVIDE(
            pos_units,
            NULLIF(pos_units + IFNULL(projected_inventory_units, 0) + IFNULL(landed_units, 0), 0)
        ) AS sell_through_rate

    FROM final
)

SELECT * FROM final_with_kpis