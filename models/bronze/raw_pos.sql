SELECT
    year,
    month,
    week,
    region,
    retailer_code,
    retailer_name,
    brand_code,
    brand,
    pos_units,
    pos_amount,
    inv_units,
    inv_amount
FROM
    {{ source("raw", "pos") }}
