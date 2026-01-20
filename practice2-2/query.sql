CREATE STREAM messages_stream (
    user_id STRING,
    recipient_id STRING,
    message STRING,
    timestamp BIGINT
) WITH (
    KAFKA_TOPIC='messages',
    VALUE_FORMAT='JSON',
    PARTITIONS=3
);


CREATE TABLE count_all AS
SELECT user_id, COUNT(*) as count_all
FROM messages_stream
GROUP BY user_id
EMIT CHANGES;

CREATE TABLE count_unique_recipient AS
SELECT user_id, COUNT_DISTINCT(recipient_id) as count_unique_recipient
FROM messages_stream
GROUP BY user_id
    EMIT CHANGES;

CREATE TABLE user_statistics
    WITH (
             KAFKA_TOPIC = 'user_statistics',
             VALUE_FORMAT = 'JSON'
) AS
SELECT
    a.user_id AS user_id,
    a.count_all AS count_messages,
    b.count_unique_recipient AS count_unique_recipients
FROM count_all a
         JOIN count_unique_recipient b
              ON a.user_id = b.user_id
    EMIT CHANGES;