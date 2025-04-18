WITH
pos AS (
    SELECT
        week_date,
        retailer_code,
        brand,
        SUM(pos_units) AS pos_units,
        SUM(inv_units) AS inv_units,
        SUM(pos_amount) AS pos_amount,
        SUM(inv_amount) AS inv_amount
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
        SUM(shp_units) AS landed_units,
        SUM(shp_amount) AS landed_amount
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
        COALESCE(ps.inv_units, 0) AS inv_units,
        COALESCE(pos_amount,0) AS pos_amount,
        COALESCE(inv_amount,0) AS inv_amount,
    
            LAST_VALUE(ps.inv_units IGNORE NULLS) OVER (
        PARTITION BY ar.retailer_code, ar.brand
        ORDER BY ar.week_date
    ) AS last_inventory_units,
                LAST_VALUE(ps.inv_amount IGNORE NULLS) OVER (
        PARTITION BY ar.retailer_code, ar.brand
        ORDER BY ar.week_date
    ) AS last_inventory_amount,


        COALESCE(st.landed_units, 0) AS landed_units,
        COALESCE(st.landed_amount, 0) AS landed_amount,
        SUM(ifnull(st.landed_units,0)) over (        PARTITION BY ar.retailer_code, ar.brand
        ORDER BY ar.week_date) AS Cummulative_landed_units,
        SUM(ifnull(st.landed_units,0)) over (        PARTITION BY ar.retailer_code, ar.brand
        ORDER BY ar.week_date) AS Cummulative_landed_amount
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
weekending_inventory AS
(
SELECT *,
ifnull(Cummulative_landed_units,0)+ifnull(last_inventory_units,0) AS week_ending_invnetory_units,
ifnull(Cummulative_landed_amount,0)+ifnull(last_inventory_amount,0) AS week_ending_invnetory_amount
FROM
    base
),
month_ending_invnentory AS

(

SELECT *,

  LAST_VALUE(week_ending_invnetory_units) OVER (
    PARTITION BY retailer_code, brand, EXTRACT(YEAR FROM week_date), EXTRACT(MONTH FROM week_date)
    ORDER BY week_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS month_end_inventory_pos_units,

    LAST_VALUE(week_ending_invnetory_amount) OVER (
    PARTITION BY retailer_code, brand, EXTRACT(YEAR FROM week_date), EXTRACT(MONTH FROM week_date)
    ORDER BY week_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS month_end_inventory_pos_amount
  

FROM weekending_inventory
)

SELECT * FROM month_ending_invnentory
