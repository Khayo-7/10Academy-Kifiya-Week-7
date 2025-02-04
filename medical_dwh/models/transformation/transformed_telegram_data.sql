-- Transformation Model
WITH grouped_messages AS (
    SELECT
        group_id,
        ARRAY_AGG(DISTINCT message_ids) AS message_ids,
        MAX(message_text) AS message_text, -- Ensuring only the first text in group is kept
        sender,
        MAX(scraped_date) AS scraped_date,
        ARRAY_AGG(DISTINCT media_paths) AS media_paths,
        ARRAY_AGG(DISTINCT links) AS links
    FROM {{ ref('stg_telegram_data') }}
    GROUP BY group_id, sender
)
SELECT * FROM grouped_messages