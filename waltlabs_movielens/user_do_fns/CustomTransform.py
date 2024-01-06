import apache_beam as beam

# Custom Transform to parse movies.csv records
class ParseMoviesCSV(beam.DoFn):
    def process(self, element):
        movie_id, title, genres = element.split(',')
        yield {
            'movieId': int(movie_id),
            'title': title,
            'genres': genres.split('|')
        }

# Custom Transform to parse ratings.csv records
class ParseRatingsCSV(beam.DoFn):
    def process(self, element):
        user_id, movie_id, rating, timestamp = element.split(',')
        yield {
            'userId': int(user_id),
            'movieId': int(movie_id),
            'rating': float(rating),
            'timestamp': int(timestamp)
        }