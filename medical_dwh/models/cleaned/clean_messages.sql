WITH cleaned AS (
    SELECT 
        message_id,
        sender_id,
        TRIM(message_text) AS message_text,
        message_date,
        COALESCE(media_path, 'No Media') AS media_path,
        channel
    FROM stg_telegram_messages
    WHERE message_text IS NOT NULL
)
SELECT * FROM cleaned