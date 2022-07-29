WITH top_buttom_aux AS(
    SELECT tweet_text, created_at, 
    (CASE WHEN EXTRACT(HOUR FROM created_at) BETWEEN 5 AND 12 THEN 'Morning'
    WHEN EXTRACT(HOUR FROM created_at) BETWEEN 13 AND 20 THEN 'Afternoon'
    WHEN EXTRACT(HOUR FROM created_at) BETWEEN 21 AND 23 OR EXTRACT(HOUR FROM created_at) BETWEEN 0 AND 4 THEN 'Night'
    END
    ) AS daytime
FROM tweet
WHERE TRUNC(created_at) = TRUNC(TO_DATE(:P2_DATE))),

sentiment_aux AS
(
 SELECT REGEXP_SUBSTR(LOWER(TRANSLATE(clean_tag, 'áéíóú', 'aeiou')),'[^[:space:]]+',1,level) AS hashtag, sentiment as sentiment
   FROM model
   INNER JOIN tweet
   ON model.tweet_id = tweet.tweet_id
   WHERE TRUNC(created_at) = TRUNC(TO_DATE(:P2_DATE))
CONNECT BY level <= REGEXP_COUNT(LOWER(TRANSLATE(clean_tag, 'áéíóú', 'aeiou')),'[:space:]') + 1
    AND PRIOR SYS_GUID() IS NOT NULL
    AND PRIOR model_id = model_id
),
negative AS
(
    SELECT 'The most common hashtag with a negative sentiment was #' || hashtag AS insight, count(*) || ' tweets' AS tweet_count, hashtag
FROM sentiment_aux
WHERE hashtag IS NOT NULL
AND hashtag NOT IN ('izcalli', 'cuautitlanizcalli', 'izcalli', 'cuautitlan', 'edomex')
AND sentiment = 'NEG'
GROUP BY hashtag, sentiment
ORDER BY COUNT(*) DESC
FETCH FIRST 1 ROW ONLY
),
neutral AS
(
    SELECT 'The most common hashtag with a neutral sentiment was #' || hashtag AS insight,  count(*) || ' tweets' AS tweet_count, hashtag
FROM sentiment_aux
WHERE hashtag IS NOT NULL
AND hashtag NOT IN ('izcalli', 'cuautitlanizcalli', 'izcalli', 'cuautitlan', 'edomex')
AND sentiment = 'NEU'
GROUP BY hashtag, sentiment
ORDER BY COUNT(*) DESC
FETCH FIRST 1 ROW ONLY
),
positive AS
(
    SELECT 'The most common hashtag with a positive sentiment was #' || hashtag AS insight,  count(*) || ' tweets' AS tweet_count, hashtag
FROM sentiment_aux
WHERE hashtag IS NOT NULL
AND hashtag NOT IN ('izcalli', 'cuautitlanizcalli', 'izcalli', 'cuautitlan', 'edomex')
AND sentiment = 'POS'
GROUP BY hashtag, sentiment
ORDER BY COUNT(*) DESC
FETCH FIRST 1 ROW ONLY
),
top_3_aux AS
(
 SELECT REGEXP_SUBSTR(LOWER(TRANSLATE(clean_tag, 'áéíóú', 'aeiou')),'[^[:space:]]+',1,level) AS hashtag, sentiment as sentiment
   FROM model
   INNER JOIN tweet
   ON model.tweet_id = tweet.tweet_id
   WHERE TRUNC(created_at) = TRUNC(TO_DATE(:P2_DATE))
CONNECT BY level <= REGEXP_COUNT(LOWER(TRANSLATE(clean_tag, 'áéíóú', 'aeiou')),'[:space:]') + 1
    AND PRIOR SYS_GUID() IS NOT NULL
    AND PRIOR model_id = model_id
),
top_3 AS
(
    SELECT CASE
WHEN ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) = 1 THEN
    'Most common hashtag: #' || hashtag
WHEN ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) = 2 THEN
    'Second most common hashtag: #' || hashtag
WHEN ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) = 3 THEN
    'Third most common hashtag: #' || hashtag
END as insight,
count(*) || ' tweets ', hashtag
FROM top_3_aux
WHERE hashtag IS NOT NULL
AND hashtag NOT IN ('izcalli', 'cuautitlanizcalli', 'izcalli', 'cuautitlan', 'edomex')
GROUP BY hashtag, sentiment
ORDER BY COUNT(*) DESC
FETCH FIRST 3 ROW ONLY
),
emocion_aux AS
(
     SELECT REGEXP_SUBSTR(LOWER(TRANSLATE(clean_tag, 'áéíóú', 'aeiou')),'[^[:space:]]+',1,level) AS hashtag, emocion as emocion
   FROM model
   INNER JOIN tweet
   ON model.tweet_id = tweet.tweet_id
   WHERE TRUNC(created_at) = TRUNC(TO_DATE(:P2_DATE))
CONNECT BY level <= REGEXP_COUNT(LOWER(TRANSLATE(clean_tag, 'áéíóú', 'aeiou')),'[:space:]') + 1
    AND PRIOR SYS_GUID() IS NOT NULL
    AND PRIOR model_id = model_id
),
hashtag_angry AS
(
    SELECT 'The most common hashtag for with an expression of anger was #' || hashtag, count(*) || ' tweets', hashtag
FROM emocion_aux
WHERE hashtag IS NOT NULL
AND hashtag NOT IN ('izcalli', 'cuautitlanizcalli', 'izcalli', 'cuautitlan', 'edomex')
AND emocion = 'anger'
GROUP BY hashtag, emocion
ORDER BY COUNT(*) DESC
FETCH FIRST 1 ROW ONLY
),
hashtag_joy AS
(
    SELECT 'The most common hashtag for with an expression of joy was #' || hashtag, count(*) || ' tweets', hashtag
FROM emocion_aux
WHERE hashtag IS NOT NULL
AND hashtag NOT IN ('izcalli', 'cuautitlanizcalli', 'izcalli', 'cuautitlan', 'edomex')
AND emocion = 'joy'
GROUP BY hashtag, emocion
ORDER BY COUNT(*) DESC
FETCH FIRST 1 ROW ONLY
)

SELECT 'The total amount of tweets were ' AS insight, COUNT(*) || ' tweets' AS tweet_count, NULL as hashtag
FROM tweet
WHERE TRUNC(created_at) = TRUNC(TO_DATE(:P2_DATE))
UNION ALL
SELECT CASE
    WHEN daytime = 'Morning' THEN 
        'Most of the tweets were created on the morning'
    WHEN daytime = 'Afternoon' THEN 
        'Most of the tweets were created on the afternoon'
    WHEN daytime = 'Night' THEN 
        'Most of the tweets were created on the night'
    ELSE 'There is no data'
    END AS insight,
    CASE
    WHEN daytime = 'Morning' THEN 
        (SELECT MAX(COUNT(*)) FROM top_buttom_aux GROUP BY daytime) || ' tweets'
    WHEN daytime = 'Afternoon' THEN 
        (SELECT MAX(COUNT(*)) FROM top_buttom_aux GROUP BY daytime) || ' tweets'
    WHEN daytime = 'Night' THEN 
        (SELECT MAX(COUNT(*)) FROM top_buttom_aux GROUP BY daytime) || ' tweets' 
    ELSE '0 tweets'
        END
 AS tweet_count, NULL as hashtag
FROM top_buttom_aux
GROUP BY daytime
HAVING COUNT(*) IN (SELECT MAX(COUNT(*)) FROM top_buttom_aux GROUP BY daytime)
UNION ALL
SELECT CASE
    WHEN daytime = 'Morning' THEN 
        'The least amount of the tweets were created on the morning'
    WHEN daytime = 'Afternoon' THEN 
        'The least amount of the tweets were created on the afternoon'
    WHEN daytime = 'Night' THEN 
        'The least amount of the tweets were created on the night'
    ELSE 'There is no data'
    END AS insight,
    CASE
    WHEN daytime = 'Morning' THEN 
        (SELECT MIN(COUNT(*)) FROM top_buttom_aux GROUP BY daytime) || ' tweets'
    WHEN daytime = 'Afternoon' THEN 
        (SELECT MIN(COUNT(*)) FROM top_buttom_aux GROUP BY daytime) || ' tweets'
    WHEN daytime = 'Night' THEN 
        (SELECT MIN(COUNT(*)) FROM top_buttom_aux GROUP BY daytime) || ' tweets'
    ELSE '0 tweets'
    END AS tweet_count, NULL as hashtag
FROM top_buttom_aux
GROUP BY daytime
HAVING COUNT(*) IN (SELECT MIN(COUNT(*)) FROM top_buttom_aux GROUP BY daytime)
UNION ALL
SELECT * FROM negative
UNION ALL
SELECT * FROM neutral
UNION ALL
SELECT * FROM positive
UNION ALL
SELECT * FROM top_3
UNION ALL
SELECT * FROM hashtag_angry
UNION ALL
SELECT * FROM hashtag_joy