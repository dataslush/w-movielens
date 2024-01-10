CREATE OR REPLACE MODEL `{project-id}.{dataset-id}.recommender`
OPTIONS
  (model_type='matrix_factorization',
   user_col='userId',
   item_col='movieId',
   l2_reg=9.83,
   num_factors=34) AS
SELECT
  userId,
  movieId,
  rating
FROM `{project-id}.{dataset-id}.ratings`