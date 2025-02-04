WITH raw_data AS (
    SELECT 
        group_id,
        ARRAY_AGG(message_id) AS message_ids,
        MAX(message_text) AS message_text,
        ARRAY_AGG(media_path) AS media_paths,
        ARRAY_AGG(link) AS links,
        business_name,
        phone_number,
        address,
        scraped_date
    FROM raw_data
    GROUP BY group_id, business_name, phone_number, address, scraped_date
)
SELECT * FROM raw_data
