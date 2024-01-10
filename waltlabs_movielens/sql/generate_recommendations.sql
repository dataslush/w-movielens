CREATE OR REPLACE TABLE `{project-id}.{dataset-id}.recommendations`
OPTIONS() AS
SELECT
  *
FROM
  ML.RECOMMEND(MODEL `{project-id}.{dataset-id}.recommender`,
  (
    SELECT
    DISTINCT  userId
    FROM
      `{project-id}.{dataset-id}.ratings`
    LIMIT 1000
  ))