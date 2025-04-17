

select
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
from
    {{ source("raw", "pos") }}
