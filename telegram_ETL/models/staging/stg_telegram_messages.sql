-- Staging Model
WITH raw_data AS (
    SELECT
        _id AS raw_id,
        group_id,
        message_ids,
        message_text,
        sender,
        scraped_date,
        media_paths,
        ARRAY(SELECT unnest(regexp_matches(message_text, 'https?://[^\s]+', 'g'))) AS links
    FROM {{ source('telegram', 'raw_data') }}
)
SELECT * FROM raw_data;
