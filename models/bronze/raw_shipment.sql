SELECT
    year,
    month,
    week,
    region,
    shipment_type,
    retailer_code,
    retailer_name,
    brand_code,
    brand,
    shp_units,
    shp_amount
FROM
    {{ source("raw", "shipment") }}
