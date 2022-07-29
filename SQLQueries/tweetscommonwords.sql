WITH t2 AS
(
 SELECT REGEXP_SUBSTR(LOWER(cleantweet),'[^[:space:]]+',1,level) AS word, sentiment as sentiment
   FROM model
   INNER JOIN tweet
   ON model.tweet_id = tweet.tweet_id
CONNECT BY level <= REGEXP_COUNT(LOWER(cleantweet),'[:space:]') + 1
    AND PRIOR SYS_GUID() IS NOT NULL
    AND PRIOR model_id = model_id
    AND created_at > sysdate - :P5_LAST_DAYS
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
)    
SELECT word, COUNT(*), sentiment AS count
  FROM t2
 WHERE word IS NOT NULL
 AND LENGTH(word) > 3
 GROUP BY word, sentiment
 ORDER BY COUNT(*) DESC, sentiment
 FETCH FIRST 30 ROWS ONLY