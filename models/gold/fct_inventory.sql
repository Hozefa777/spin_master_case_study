WITH
pos AS (
    SELECT
        week_date,
        retailer_code,
        brand,
        SUM(pos_units) AS pos_units
    FROM
        {{ ref('stg_pos') }}
    GROUP BY
        week_date,
        retailer_code,
        brand
),

shipment AS (
    SELECT
        landing_date AS week_date,
        retailer_code,
        brand,
        SUM(shp_units) AS landed_units
    FROM
        {{ ref('stg_shipment') }}
    GROUP BY
        week_date,
        retailer_code,
        brand
),

anchor AS (
    SELECT DISTINCT
        week_date,
        retailer_code,
        brand
    FROM
        pos
    UNION ALL
    SELECT DISTINCT
        week_date,
        retailer_code,
        brand
    FROM
        shipment
),

base AS (
    SELECT
        ar.week_date,
        ar.retailer_code,
        ar.brand,
        COALESCE(ps.pos_units, 0) AS pos_units,
        COALESCE(st.landed_units, 0) AS landed_units
    FROM
        anchor AS ar
    LEFT JOIN
        pos AS ps
        ON
            ar.week_date = ps.week_date
            AND ar.retailer_code = ps.retailer_code
            AND ar.brand = ps.brand
    LEFT JOIN
        shipment AS st
        ON
            ar.week_date = st.week_date
            AND ar.retailer_code = st.retailer_code
            AND ar.brand = st.brand
),

projected_inventory AS (
    SELECT
        *,
        SUM(landed_units - pos_units)
            OVER (
                PARTITION BY retailer_code, brand
                ORDER BY
                    week_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
            AS ending_inventory
    FROM
        base
)

SELECT *
FROM
    projected_inventory
