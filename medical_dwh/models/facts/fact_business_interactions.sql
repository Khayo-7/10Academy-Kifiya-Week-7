WITH cleaned_data AS (
    SELECT
        group_id,
        message_ids,
        message_text,
        media_paths,
        links,
        scraped_date
    FROM {{ ref('stg_telegram') }}
)
SELECT
    group_id,
    UNNEST(message_ids) AS message_id,
    message_text,
    UNNEST(media_paths) AS media_path,
    UNNEST(links) AS extracted_link,
    scraped_date
FROM cleaned_data;
