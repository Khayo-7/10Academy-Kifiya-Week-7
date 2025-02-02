WITH standardized AS (
    SELECT 
        message_id,
        sender_id,
        LOWER(TRIM(message_text)) AS message_text,
        TO_TIMESTAMP(message_date, 'YYYY-MM-DD HH24:MI:SS') AS formatted_date,
        media_path,
        channel
    FROM filled
)
SELECT * FROM standardized;
