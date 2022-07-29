SELECT sentiment, count(*)
 FROM model
 INNER JOIN tweet
 ON model.tweet_id = tweet.tweet_id
 WHERE created_at > sysdate - :P5_LAST_DAYS
  AND
 (
     CASE
        WHEN :P5_TIME IS NULL THEN 1
        WHEN :P5_Time = 'Morning' AND EXTRACT(HOUR FROM created_at) BETWEEN 5 AND 12 THEN 1
        WHEN :P5_Time = 'Afternoon' AND EXTRACT(HOUR FROM created_at) BETWEEN 13 AND 20 THEN 1
        WHEN :P5_TIME = 'Night' AND (EXTRACT(HOUR FROM created_at) BETWEEN 21 AND 23 OR EXTRACT(HOUR FROM created_at) BETWEEN 0 AND 4) THEN 1
        ELSE 0
        END = 1
        )
 GROUP by sentiment
 ORDER BY COUNT(*) DESC 