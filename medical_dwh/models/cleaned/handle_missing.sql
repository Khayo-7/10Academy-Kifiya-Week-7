WITH filled AS (
    SELECT 
        message_id,
        COALESCE(sender_id, 'Unknown Sender') AS sender_id,
        COALESCE(message_text, 'No Text') AS message_text,
        message_date,
        COALESCE(media_path, 'No Media') AS media_path,
        channel
    FROM deduplicated
)
SELECT * FROM filled
