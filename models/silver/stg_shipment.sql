SELECT
    retailer_code,
    brand_code,
    CAST(year AS INT64) AS year,
    EXTRACT(MONTH FROM PARSE_DATE('%G-%V', CAST(year AS STRING) || '-' || LPAD(CAST(week AS STRING), 2, '0'))) AS month,
    CAST(week AS INT64) AS week,
    PARSE_DATE(
        '%G-%V',
        CAST(year AS STRING) || '-' || LPAD(CAST(week AS STRING), 2, '0')
    ) AS shipment_date,
    LOWER(region) AS region,
    LOWER(shipment_type) AS shipment_type,
    LOWER(retailer_name) AS retailer_name,
    LOWER(brand) AS brand,
    SAFE_CAST(shp_units AS INT64) AS shp_units,
    SAFE_CAST(shp_amount AS FLOAT64) AS shp_amount,
    CASE
        WHEN
            LOWER(shipment_type) = 'fob'
            THEN
                DATE_ADD(
                    PARSE_DATE(
                        '%G-%V',
                        CAST(year AS STRING)
                        || '-'
                        || LPAD(CAST(week AS STRING), 2, '0')
                    ),
                    INTERVAL 8 WEEK
                )
        WHEN
            LOWER(shipment_type) = 'dom'
            THEN
                DATE_ADD(
                    PARSE_DATE(
                        '%G-%V',
                        CAST(year AS STRING)
                        || '-'
                        || LPAD(CAST(week AS STRING), 2, '0')
                    ),
                    INTERVAL 4 WEEK
                )
    END AS landing_date
FROM {{ ref('raw_shipment') }}
