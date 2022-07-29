WITH t2 AS(
    select place.PLACE_ID,
       FULL_NAME,
       COUNTRY,
       LATITUDE1,
       LONGITUDE1,
       LATITUDE2,
       LONGITUDE2,
       LATITUDE3,
       LONGITUDE3,
       LATITUDE4,
       LONGITUDE4,
       COUNT(*) as times
  from PLACE
  INNER JOIN tweet
  ON place.place_id = tweet.place_id
  WHERE created_at > sysdate - :P4_LAST_DAYS
    AND
 (
     CASE
        WHEN :P4_TIME IS NULL THEN 1
        WHEN :P4_Time = 'Morning' AND EXTRACT(HOUR FROM created_at) BETWEEN 5 AND 12 THEN 1
        WHEN :P4_Time = 'Afternoon' AND EXTRACT(HOUR FROM created_at) BETWEEN 13 AND 20 THEN 1
        WHEN :P4_TIME = 'Night' AND (EXTRACT(HOUR FROM created_at) BETWEEN 21 AND 23 OR EXTRACT(HOUR FROM created_at) BETWEEN 0 AND 4) THEN 1
        ELSE 0
        END = 1
        )
  GROUP BY place.PLACE_ID,
       FULL_NAME,
       COUNTRY,
       LATITUDE1,
       LONGITUDE1,
       LATITUDE2,
       LONGITUDE2,
       LATITUDE3,
       LONGITUDE3,
       LATITUDE4,
       LONGITUDE4
)

select 
  t2.PLACE_ID,
       FULL_NAME,
       COUNTRY,
       LATITUDE1,
       LONGITUDE1,
       LATITUDE2,
       LONGITUDE2,
       LATITUDE3,
       LONGITUDE3,
       LATITUDE4,
       LONGITUDE4,
       tweet_text,
       created_at,
       times
from (
  select tweet_id, created_at, tweet_text, place_id,
    row_number() over(partition by place_id order by created_at desc) as rn
  from tweet
) x
inner join t2
on x.place_id = t2.place_id
where rn = 1