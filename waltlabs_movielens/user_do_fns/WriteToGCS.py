import apache_beam as beam
import logging
from apache_beam.io.gcp.gcsio import GcsIO

class WriteToGCS(beam.DoFn):
    def __init__(self, gcs_bucket, gcs_output_prefix):
        self.gcs_bucket = gcs_bucket
        self.gcs_output_prefix = gcs_output_prefix

    def process(self, element):
        filename, content = element
        gcs_path = f'gs://{self.gcs_bucket}/{self.gcs_output_prefix}/{filename}'
        try:
            with GcsIO().open(gcs_path, 'w') as gcs_file:
                gcs_file.write(content)
            yield gcs_path
        except Exception as e:
            logging.error(f"Error writing to GCS: {e}")