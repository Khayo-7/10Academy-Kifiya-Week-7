WITH raw_data AS (
    SELECT
        group_id,
        ARRAY_AGG(message_id ORDER BY scraped_date) AS message_ids,
        STRING_AGG(message_text, ' || ') AS message_text,
        ARRAY_AGG(media_path) AS media_paths,
        ARRAY_AGG(unnest(regexp_matches(message_text, 'https?://[^ ]+', 'g'))) AS links,
        business_name,
        phone_number,
        address,
        scraped_date
    FROM raw_data_table
    GROUP BY group_id, business_name, phone_number, address, scraped_date
)
SELECT * FROM raw_data;
