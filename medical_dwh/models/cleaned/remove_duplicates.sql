WITH deduplicated AS (
    SELECT DISTINCT ON (message_id) 
        message_id, sender_id, message_text, message_date, media_path, channel
    FROM cleaned_messages
    ORDER BY message_id, message_date DESC
)
SELECT * FROM deduplicated
