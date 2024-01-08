import io, csv
import apache_beam as beam
from apache_beam.io.gcp.gcsio import GcsIO
# Custom Transform to parse movies.csv records
class ReadParseMoviesCSV(beam.DoFn):
    def process(self, element):    
        if 'movies.csv' in element:
            with GcsIO().open(element,'rb') as file:
                csv_reader = csv.reader(io.TextIOWrapper(file, encoding='utf-8'))
                header_skipped = False
                for line in csv_reader:
                    if not header_skipped:
                        header_skipped = True
                        continue  # Skip the header line

                movie_id, title, genres = line[0], line[1], line[2]
                yield {
                    'movieId': int(movie_id),
                    'title': title,
                    'genres': genres.split('|')
                }

# Custom Transform to parse ratings.csv records
class ReadParseRatingsCSV(beam.DoFn):
    def process(self, element):
        if 'ratings.csv' in element:
            with GcsIO().open(element,'rb') as file:
                csv_reader = csv.reader(io.TextIOWrapper(file, encoding='utf-8'))
                header_skipped = False
                for line in csv_reader:
                    if not header_skipped:
                        header_skipped = True
                        continue  # Skip the header line

                    user_id, movie_id, rating, timestamp = line[0], line[1], line[2], line[3]
                    yield {
                        'userId': int(user_id),
                        'movieId': int(movie_id),
                        'rating': float(rating),
                        'timestamp': int(timestamp)
                    }
                    