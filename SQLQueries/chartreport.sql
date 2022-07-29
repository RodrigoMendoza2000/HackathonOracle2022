SELECT model.tweet_text AS TweetText, model.sentiment, model.tipo, model.emocion,  model.clean_tag AS hashtags, tweet.created_at AS TweetDate
FROM model
INNER JOIN tweet
ON model.tweet_id = tweet.tweet_id
WHERE (:P6_EMOCION IS NULL OR model.emocion = :P6_EMOCION)
AND (:P6_SENTIMENT IS NULL OR model.sentiment = :P6_SENTIMENT)
AND (:P6_TIPO IS NULL OR model.tipo = :P6_TIPO)
AND (:P6_HASHTAG IS NULL OR REGEXP_LIKE(LOWER(TRANSLATE(clean_tag, 'áéíóú', 'aeiou')), '.*' || :P6_HASHTAG || '.*'))
AND (:P6_DATE IS NULL OR TRUNC(created_at) = TRUNC(TO_DATE(:P6_DATE)))