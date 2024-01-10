SELECT
  userId,
  ARRAY_AGG(STRUCT(title,
      genres,
      predicted_rating)
  ORDER BY
    predicted_rating DESC
  LIMIT
    5)
FROM (
  SELECT
    userId,
    recommendations.movieId,
    predicted_rating,
    title,
    genres
  FROM
    `{project-id}.{dataset-id}.recommendations` recommendations
  JOIN
    `{project-id}.{dataset-id}.movies` movies
  ON
    movies.movieId = recommendations.movieId)
GROUP BY
  userId