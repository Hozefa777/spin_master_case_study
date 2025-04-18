SELECT
    node,
    amount,
    month,
    year,
    region,
    version,
    brand
FROM
    {{ source("raw", "fin_data_sheet_1") }}
