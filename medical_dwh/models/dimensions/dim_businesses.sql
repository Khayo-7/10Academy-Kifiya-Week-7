SELECT
    DISTINCT group_id,
    business_name,
    phone_number,
    address
FROM {{ ref('stg_telegram') }}
WHERE business_name IS NOT NULL;
