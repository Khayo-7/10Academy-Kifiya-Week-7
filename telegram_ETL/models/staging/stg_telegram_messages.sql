WITH raw_messages AS (
    SELECT 
        id AS message_id,
        sender_id,
        text AS message_text,
        date AS message_date,
        media_path,
        channel
    FROM raw_telegram_data
)
SELECT * FROM raw_messages;
