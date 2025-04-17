select
    node as node,
    amount as amount,
    month as month,
    year as year,
    region as region,
    version as version,
    brand as brand,
from {{ source("raw", "fin_data_sheet_1") }}
